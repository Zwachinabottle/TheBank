from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from datetime import timedelta
import time
from threading import Lock

app = Flask(__name__, static_folder='images')
app.secret_key = "your_secret_key_here"

# ---------- CACHING SYSTEM ----------
class SheetCache:
    """In-memory cache with TTL to minimize Google Sheets API calls"""
    def __init__(self, ttl=45):
        self.cache = {}
        self.ttl = ttl  # Time to live in seconds (45s default)
        self.lock = Lock()
    
    def get(self, key):
        """Get cached value if not expired"""
        with self.lock:
            if key in self.cache:
                value, timestamp = self.cache[key]
                if time.time() - timestamp < self.ttl:
                    return value
                else:
                    # Expired, remove it
                    del self.cache[key]
            return None
    
    def set(self, key, value):
        """Set cached value with current timestamp"""
        with self.lock:
            self.cache[key] = (value, time.time())
    
    def invalidate(self, *keys):
        """Invalidate specific cache keys or patterns"""
        with self.lock:
            if not keys:
                # Clear all cache
                self.cache.clear()
            else:
                for key in keys:
                    if key in self.cache:
                        del self.cache[key]
    
    def invalidate_pattern(self, pattern):
        """Invalidate all keys containing pattern"""
        with self.lock:
            keys_to_delete = [k for k in self.cache.keys() if pattern in k]
            for key in keys_to_delete:
                del self.cache[key]

# Global cache instance
cache = SheetCache(ttl=45)

def retry_with_backoff(func, max_retries=3, initial_delay=1):
    """Exponential backoff retry for 429 errors"""
    for attempt in range(max_retries):
        try:
            return func()
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:  # Quota exceeded
                if attempt < max_retries - 1:
                    delay = initial_delay * (2 ** attempt)  # Exponential backoff
                    print(f"API quota exceeded. Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    raise Exception("Google Sheets API quota exceeded. Please try again later.")
            else:
                raise
        except Exception as e:
            raise

# ---------- GOOGLE SHEETS SETUP ----------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
client = gspread.authorize(credentials)

sheet = client.open("Bank-Info")
users_sheet = sheet.worksheet("Users")
transactions_sheet = sheet.worksheet("Transactions")
fed_sheet = sheet.worksheet("Reserve")
loans_sheet = sheet.worksheet("Loans")
# Ensure sheet has required columns
header = users_sheet.row_values(1)

required_headers = ["Username", "Password", "Balance", "Frozen", "Role"]

if header != required_headers:
    # Reset header row (preserves data if columns existed)
    users_sheet.delete_rows(1)
    users_sheet.insert_row(required_headers, 1)
    



# ---------- HELPERS ----------
def get_interest_rate():
    """
    Calculate interest rate based on bank's available lending capacity.
    Uses fractional reserve banking principles:
    - Bank can lend from its own reserves + 90% of customer deposits
    - Low available funds = HIGH interest rates (0.5% to 5% per week)
    - This mimics real Federal Reserve rate adjustments
    """
    data = get_federal_reserve_stats()

    try:
        # Get bank account balance
        bank_account = get_bank_account()
        bank_balance = float(bank_account.get("Balance", 0))
        
        # Get total customer deposits (Student + Teacher funds)
        student_money = float(data.get("Student", 0))
        teacher_money = float(data.get("Teacher", 0))
        total_deposits = student_money + teacher_money
        
        # Calculate lendable funds:
        # Bank's own money + 90% of customer deposits (keeping 10% reserve)
        lendable_funds = bank_balance + (total_deposits * 0.9)
        
        # Get current outstanding loans
        total_loaned = float(data.get("Loaned", 0))
        
        # Calculate available lending capacity
        available_capacity = lendable_funds - total_loaned
        
        # If we have negative capacity or very low funds, max out interest rate
        if available_capacity <= 0:
            return 0.05  # 5% - maximum rate when bank is tapped out
        
        # Calculate capacity ratio (0 = empty, 1 = full capacity)
        if lendable_funds > 0:
            capacity_ratio = available_capacity / lendable_funds
        else:
            return 0.05  # Max rate if no lendable funds
        
        # Interest rate formula: INVERSE relationship
        # Low capacity (0) → 5% rate
        # High capacity (1) → 0.5% rate
        # Linear interpolation: rate = 0.05 - (capacity_ratio * 0.045)
        interest_rate = 0.05 - (capacity_ratio * 0.045)
        
        # Clamp between 0.5% and 5%
        interest_rate = max(0.005, min(0.05, interest_rate))
        
        return round(interest_rate, 4)

    except (ValueError, KeyError, TypeError):
        return 0.025  # Default 2.5% if calculation fails

def get_all_users():
    """Get all users with caching"""
    cache_key = "all_users"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    def fetch():
        return users_sheet.get_all_records()
    
    users = retry_with_backoff(fetch)
    cache.set(cache_key, users)
    return users

def get_all_users_with_balances():
    """Alias for get_all_users (already returns balance data)"""
    return get_all_users()

def update_balance(username, new_balance):
    """Update balance and invalidate relevant caches"""
    def update():
        cell = users_sheet.find(username)
        users_sheet.update_cell(cell.row, 3, new_balance)
    
    retry_with_backoff(update)
    
    # Invalidate caches that depend on user data
    cache.invalidate("all_users", f"user_balance_{username}", f"user_data_{username}")

def add_transaction(sender, receiver, amount, comment=""):
    """Add transaction and invalidate transaction cache"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not comment:
        comment = "No comment"
    
    def append():
        transactions_sheet.append_row([sender, receiver, amount, now, comment])
    
    retry_with_backoff(append)
    
    # Invalidate transaction caches for both users
    cache.invalidate(f"transactions_{sender}", f"transactions_{receiver}")

def get_user_balance(username):
    """Get user balance with caching"""
    cache_key = f"user_balance_{username}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    # Try to get from all_users cache first to avoid extra API call
    all_users = get_all_users()
    user = next((u for u in all_users if u["Username"] == username), None)
    if user:
        balance = float(user["Balance"])
        cache.set(cache_key, balance)
        return balance
    
    # Fallback to direct lookup if user not found in cache
    def fetch():
        cell = users_sheet.find(username)
        return float(users_sheet.cell(cell.row, 3).value)
    
    balance = retry_with_backoff(fetch)
    cache.set(cache_key, balance)
    return balance

def get_user_transactions(username):
    """Get user transactions with caching"""
    cache_key = f"transactions_{username}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    def fetch():
        return transactions_sheet.get_all_records()
    
    rows = retry_with_backoff(fetch)
    formatted = []

    for t in rows:
        if t["Sender"] == username or t["Receiver"] == username:
            formatted.append({
                "Sender": t["Sender"],
                "Receiver": t["Receiver"],
                "Amount": float(t["Amount"]),
                "Date": t["Date"],
                "Comment": t.get("Comment", "No comment")
            })

    formatted.sort(key=lambda x: x["Date"], reverse=True)
    cache.set(cache_key, formatted)
    return formatted


def transfer_money(sender, receiver, amount, comment):
    """Transfer money between accounts with optimized API calls"""
    # Use cached user data to avoid multiple finds
    all_users = get_all_users()
    sender_user = next((u for u in all_users if u["Username"] == sender), None)
    receiver_user = next((u for u in all_users if u["Username"] == receiver), None)
    
    if not sender_user or not receiver_user:
        return False
    
    sender_balance = float(sender_user["Balance"])
    receiver_balance = float(receiver_user["Balance"])

    if sender_balance < amount:
        return False

    # Find cells and update (unavoidable API calls, but batched)
    def batch_update():
        sender_cell = users_sheet.find(sender)
        receiver_cell = users_sheet.find(receiver)
        users_sheet.update_cell(sender_cell.row, 3, sender_balance - amount)
        users_sheet.update_cell(receiver_cell.row, 3, receiver_balance + amount)
    
    retry_with_backoff(batch_update)
    add_transaction(sender, receiver, amount, comment)
    
    # Invalidate relevant caches
    cache.invalidate(
        "all_users",
        f"user_balance_{sender}",
        f"user_balance_{receiver}",
        f"user_data_{sender}",
        f"user_data_{receiver}"
    )
    
    return True

def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if "role" not in session or session["role"] not in roles:
                flash("You don't have permission to access this page.", "error")
                return redirect(url_for("account"))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def freeze_account(username):
    """Freeze account and invalidate cache"""
    def update():
        cell = users_sheet.find(username)
        header = users_sheet.row_values(1)
        frozen_col = header.index("Frozen") + 1
        users_sheet.update_cell(cell.row, frozen_col, "Yes")
    
    retry_with_backoff(update)
    cache.invalidate("all_users", f"user_data_{username}", f"frozen_{username}")

def unfreeze_account(username):
    """Unfreeze account and invalidate cache"""
    def update():
        cell = users_sheet.find(username)
        header = users_sheet.row_values(1)
        frozen_col = header.index("Frozen") + 1
        users_sheet.update_cell(cell.row, frozen_col, "No")
    
    retry_with_backoff(update)
    cache.invalidate("all_users", f"user_data_{username}", f"frozen_{username}")

def create_account(username, password):
    """Create account and invalidate user cache"""
    def append():
        users_sheet.append_row([username, password, 0, "No", "Student"])
    
    retry_with_backoff(append)
    cache.invalidate("all_users")

def normalize_roles_column():
    # Make sure the sheet has a Role column. If not, create it.
    header = users_sheet.row_values(1)

    if "Role" not in header:
        users_sheet.update_cell(1, len(header) + 1, "Role")
        header.append("Role")

    # Get all data starting from row 2
    all_data = users_sheet.get_all_records()

    for idx, row in enumerate(all_data, start=2):  # row 2 onward
        role = row.get("Role", "").strip()
        if role == "":
            users_sheet.update_cell(idx, header.index("Role") + 1, "Student")

def is_frozen(username):
    """Check if account is frozen with caching"""
    cache_key = f"frozen_{username}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    # Try to get from all_users cache first
    all_users = get_all_users()
    user = next((u for u in all_users if u["Username"] == username), None)
    if user:
        is_frozen_status = str(user.get("Frozen", "No")).strip().lower() == "yes"
        cache.set(cache_key, is_frozen_status)
        return is_frozen_status
    
    # Fallback to direct lookup
    def fetch():
        cell = users_sheet.find(username)
        header = users_sheet.row_values(1)
        frozen_col = header.index("Frozen") + 1
        value = users_sheet.cell(cell.row, frozen_col).value
        return str(value).strip().lower() == "yes"
    
    is_frozen_status = retry_with_backoff(fetch)
    cache.set(cache_key, is_frozen_status)
    return is_frozen_status

def ensure_fed_sheet():
    required_headers = [
        "Total",
        "Reserves",
        "Student",
        "Teacher",
        "Liquidity",
        "Cash",
        "Loaned",
        "Last Updated",
        "ProjectEndDate"  # Date when all loans must be paid off (format: YYYY-MM-DD)
    ]

    existing_headers = fed_sheet.row_values(1)

    # If header row is empty, write full header
    if not existing_headers:
        fed_sheet.insert_row(required_headers, 1)
        fed_sheet.insert_row([""] * len(required_headers), 2)
        return

    # Add missing headers (append to the right)
    for header in required_headers:
        if header not in existing_headers:
            fed_sheet.update_cell(
                1,
                len(existing_headers) + 1,
                header
            )
            existing_headers.append(header)

    # Ensure row 2 exists
    if len(fed_sheet.get_all_values()) < 2:
        fed_sheet.insert_row([""] * len(existing_headers), 2)
ensure_fed_sheet()

def get_fed_columns():
    """Get federal reserve column mapping with caching"""
    cache_key = "fed_columns"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    def fetch():
        header = fed_sheet.row_values(1)
        return {name: idx + 1 for idx, name in enumerate(header)}
    
    columns = retry_with_backoff(fetch)
    cache.set(cache_key, columns)
    return columns

def set_fed_value(label, value):
    """Set federal reserve value and invalidate cache"""
    cols = get_fed_columns()
    if label not in cols:
        return
    
    def update():
        fed_sheet.update_cell(2, cols[label], value)
    
    retry_with_backoff(update)
    cache.invalidate("fed_stats")

def get_federal_reserve_stats():
    """Get federal reserve stats with caching"""
    cache_key = "fed_stats"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    def fetch():
        cols = get_fed_columns()
        values = fed_sheet.row_values(2)
        
        data = {}
        for key, col in cols.items():
            if col - 1 < len(values):
                data[key] = values[col - 1]
            else:
                data[key] = ""
        return data
    
    stats = retry_with_backoff(fetch)
    cache.set(cache_key, stats)
    return stats



def recalculate_federal_reserve():
    """Recalculate federal reserve stats with proper loan and cash burn tracking"""
    users = get_all_users_with_balances()
    
    # Calculate total money in all accounts
    total_in_accounts = 0
    student_money = 0
    teacher_money = 0

    for u in users:
        balance = float(u["Balance"])
        total_in_accounts += balance

        if u.get("Role") == "Student":
            student_money += balance
        elif u.get("Role") in ["Teacher", "Banker"]:
            teacher_money += balance
    
    # Calculate total money LOANED OUT (approved loans still being paid back)
    total_loaned_out = 0
    total_loan_repaid = 0
    
    try:
        def fetch_loans():
            return loans_sheet.get_all_records()
        
        loans = retry_with_backoff(fetch_loans)
        
        for loan in loans:
            if loan.get("Status") == "Approved":
                original_amount = float(loan.get("Amount", 0))
                total_weeks = int(loan.get("Weeks", 0))
                weeks_remaining = int(loan.get("WeeksRemaining", 0))
                
                # Calculate how much was originally loaned
                total_loaned_out += original_amount
                
                # Calculate how much has been repaid (weeks passed * weekly payment)
                if total_weeks > 0:
                    weeks_paid = total_weeks - weeks_remaining
                    weekly_payment = float(loan.get("Weekly", 0))
                    total_loan_repaid += weeks_paid * weekly_payment
    except:
        pass  # If loans sheet doesn't exist or error, default to 0
    
    # Calculate total CASH BURNED (approved cash burns that removed money from economy)
    total_cash_burned = 0
    
    try:
        def fetch_cashburns():
            cashburns_sheet = sheet.worksheet("CashBurns")
            return cashburns_sheet.get_all_records()
        
        cashburns = retry_with_backoff(fetch_cashburns)
        
        for burn in cashburns:
            if burn.get("Status") == "Approved":
                total_cash_burned += float(burn.get("Amount", 0))
    except:
        pass  # If CashBurns sheet doesn't exist or error, default to 0
    
    # CALCULATION LOGIC:
    # Total Money Created = Money in Accounts + Money Loaned Out (still owed) + Cash Burned + Loan Repayments
    # This represents ALL money that has ever entered the economy
    
    total_money_created = total_in_accounts + (total_loaned_out - total_loan_repaid) + total_cash_burned
    
    # Cash = Money that was loaned out but not yet in circulation (original loan amounts minus repayments)
    # This is money the Federal Reserve "created" via loans
    cash_outstanding = total_loaned_out - total_loan_repaid
    
    # Reserves = Total money created - (Student money + Teacher money + Cash outstanding)
    # This represents the "federal reserve" balance
    reserves = round(total_money_created - (student_money + teacher_money + cash_outstanding), 2)
    
    # Liquidity = Bank's lendable funds using fractional reserve banking
    # Bank balance + 90% of customer deposits (keeping 10% reserve)
    try:
        bank_account = get_bank_account()
        bank_balance = float(bank_account.get("Balance", 0))
        lendable_funds = bank_balance + ((student_money + teacher_money) * 0.9)
        liquidity = round(lendable_funds, 2)
    except:
        # Fallback to old calculation if bank account doesn't exist
        liquidity = round(total_in_accounts - ((student_money * 0.1) + (teacher_money * 0.1)), 2)
    
    # Loaned = Total amount currently loaned out (not yet repaid)
    loaned = round(cash_outstanding, 2)
    
    # Cash = Total cash burned (money removed from economy)
    cash = round(total_cash_burned, 2)

    # Update Federal Reserve sheet
    set_fed_value("Total", round(total_money_created, 2))
    set_fed_value("Reserves", reserves)
    set_fed_value("Student", round(student_money, 2))
    set_fed_value("Teacher", round(teacher_money, 2))
    set_fed_value("Liquidity", liquidity)
    set_fed_value("Cash", cash)
    set_fed_value("Loaned", loaned)
    set_fed_value("Last Updated", datetime.now().strftime("%Y-%m-%d %H:%M"))
    
    # Invalidate fed stats cache after update
    cache.invalidate("fed_stats")

def get_project_end_date():
    """Get the project end date from Federal Reserve sheet"""
    cache_key = "project_end_date"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    def fetch():
        cols = get_fed_columns()
        if "ProjectEndDate" not in cols:
            return None
        values = fed_sheet.row_values(2)
        col_idx = cols["ProjectEndDate"] - 1
        if col_idx < len(values) and values[col_idx]:
            return values[col_idx]
        return None
    
    end_date = retry_with_backoff(fetch)
    cache.set(cache_key, end_date)
    return end_date

def set_project_end_date(date_string):
    """Set project end date (format: YYYY-MM-DD)"""
    set_fed_value("ProjectEndDate", date_string)
    cache.invalidate("project_end_date", "weeks_until_project_end")

def get_weeks_until_project_end():
    """Calculate weeks remaining until project end date"""
    cache_key = "weeks_until_project_end"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    end_date_str = get_project_end_date()
    if not end_date_str:
        # Default to 9 weeks if not set
        return 9
    
    try:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        today = datetime.now()
        days_remaining = (end_date - today).days
        weeks_remaining = max(1, (days_remaining + 6) // 7)  # Round up, minimum 1 week
        cache.set(cache_key, weeks_remaining)
        return weeks_remaining
    except:
        return 9

def process_loan_payments():
    """Process all loan payments that are due (automated weekly deductions)"""
    from datetime import datetime, timedelta
    
    def get_loan_data():
        loans = loans_sheet.get_all_records()
        header = loans_sheet.row_values(1)
        return loans, header
    
    loans, header = retry_with_backoff(get_loan_data)
    col_index = {name: idx + 1 for idx, name in enumerate(header)}
    
    today = datetime.now().date()
    payments_processed = 0
    
    for idx, loan in enumerate(loans, start=2):  # Start at row 2 (after header)
        if loan.get("Status") != "Active":
            continue
        
        next_payment_str = loan.get("NextPaymentDate", "")
        if not next_payment_str:
            continue
        
        try:
            next_payment = datetime.strptime(next_payment_str, "%Y-%m-%d").date()
        except:
            continue
        
        # If payment is due (today or past)
        if next_payment <= today:
            requester = loan.get("Requester")
            weekly_payment = float(loan.get("Weekly", 0))
            weeks_remaining = int(loan.get("WeeksRemaining", 0))
            total_paid = float(loan.get("TotalPaid", 0))
            
            # Get current balance (will go negative if insufficient funds)
            current_balance = get_user_balance(requester)
            
            # ALWAYS deduct payment, even if it makes balance negative
            new_balance = current_balance - weekly_payment
            update_balance(requester, new_balance)
            
            # CREDIT the bank's account (loan repayment goes back to bank)
            bank_account = get_bank_account()
            bank_balance = float(bank_account.get("Balance", 0))
            new_bank_balance = bank_balance + weekly_payment
            update_bank_balance(new_bank_balance)
            
            # Add transaction
            week_number = int(loan.get('Weeks', 0)) - weeks_remaining + 1
            add_transaction(requester, "Bank", weekly_payment, f"Loan payment (week {week_number})")
            
            # Update loan record
            weeks_remaining -= 1
            total_paid += weekly_payment
            
            def update_loan():
                # Update weeks remaining
                loans_sheet.update_cell(idx, col_index["WeeksRemaining"], weeks_remaining)
                
                # Update total paid
                loans_sheet.update_cell(idx, col_index["TotalPaid"], total_paid)
                
                if weeks_remaining <= 0:
                    # Loan fully paid
                    loans_sheet.update_cell(idx, col_index["Status"], "Paid")
                    loans_sheet.update_cell(idx, col_index["NextPaymentDate"], "")
                else:
                    # Set next payment date (7 days from now)
                    next_date = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
                    loans_sheet.update_cell(idx, col_index["NextPaymentDate"], next_date)
            
            retry_with_backoff(update_loan)
            payments_processed += 1
            
            # Log the payment (include warning if balance went negative)
            if new_balance < 0:
                log_action("System", f"Auto-deducted ${weekly_payment} loan payment from {requester} (balance now NEGATIVE: ${new_balance:.2f}) → Bank: ${new_bank_balance:.2f}", weekly_payment, "Loan Payment")
            else:
                log_action("System", f"Auto-deducted ${weekly_payment} loan payment from {requester} (balance: ${new_balance:.2f}) → Bank: ${new_bank_balance:.2f}", weekly_payment, "Loan Payment")
    
    # Invalidate caches
    cache.invalidate("all_loans")
    cache.invalidate_pattern("user_loans_")
    
    return payments_processed

def loan_money(sender, reason, amount, weeks):
    """Submit loan request and invalidate cache"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    interest_rate = get_interest_rate()
    total_amount = amount * (1 + interest_rate * weeks)
    weekly_payment = round(total_amount / weeks, 2)
    
    def append():
        loans_sheet.append_row([
            sender,           # Requester
            reason,           # Reason
            amount,           # Amount
            weeks,            # Weeks
            weekly_payment,   # Weekly
            "Pending",        # Status
            now,              # Date
            "",               # WeeksRemaining (empty until approved)
            "",               # NextPaymentDate (empty until approved)
            0                 # TotalPaid
        ])
    
    retry_with_backoff(append)
    cache.invalidate("pending_loans", "all_loans", f"user_loans_{sender}")

def approve_loan(loan_row_index):
    """Approve a loan and set up payment schedule"""
    from datetime import timedelta
    
    def get_loan():
        loan = loans_sheet.row_values(loan_row_index)
        header = loans_sheet.row_values(1)
        return loan, header
    
    loan, header = retry_with_backoff(get_loan)
    col_index = {name: idx + 1 for idx, name in enumerate(header)}
    
    requester = loan[col_index["Requester"] - 1]
    amount = float(loan[col_index["Amount"] - 1])
    weeks = int(loan[col_index["Weeks"] - 1])
    
    # DEDUCT loan amount from bank's account (fractional reserve lending)
    bank_account = get_bank_account()
    bank_balance = float(bank_account.get("Balance", 0))
    
    # Check if bank has enough capacity (with fractional reserves)
    data = get_federal_reserve_stats()
    student_money = float(data.get("Student", 0))
    teacher_money = float(data.get("Teacher", 0))
    total_deposits = student_money + teacher_money
    lendable_funds = bank_balance + (total_deposits * 0.9)
    
    if amount > lendable_funds:
        # Not enough funds to approve - would need to deny or create money
        log_action(session.get("user", "System"), 
                  f"Cannot approve loan for {requester}: Insufficient bank capacity (${lendable_funds:.2f} available, ${amount:.2f} requested)", 
                  amount, "Denied - Insufficient Funds")
        # Still deny the loan in the sheet
        loans_sheet.update_cell(loan_row_index, col_index["Status"], "Denied")
        cache.invalidate("pending_loans", "all_loans", f"user_loans_{requester}")
        return
    
    # Deduct from bank's account
    new_bank_balance = bank_balance - amount
    update_bank_balance(new_bank_balance)
    
    # Add money to user's account
    current_balance = get_user_balance(requester)
    update_balance(requester, current_balance + amount)
    
    # Add transaction
    add_transaction("Bank", requester, amount, "Loan disbursement")
    
    # Update loan status
    def update_loan():
        loans_sheet.update_cell(loan_row_index, col_index["Status"], "Active")
        loans_sheet.update_cell(loan_row_index, col_index["WeeksRemaining"], weeks)
        
        # Set next payment date (7 days from now)
        next_payment = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        loans_sheet.update_cell(loan_row_index, col_index["NextPaymentDate"], next_payment)
    
    retry_with_backoff(update_loan)
    
    # Log action
    log_action(session.get("user", "System"), 
              f"Approved loan for {requester}: ${amount} (Bank balance: ${bank_balance:.2f} → ${new_bank_balance:.2f})", 
              amount, "Approved")
    
    # Invalidate loan caches
    cache.invalidate("pending_loans", "all_loans", f"user_loans_{requester}")

def deny_loan(loan_row_index):
    """Deny a loan application"""
    def get_loan():
        loan = loans_sheet.row_values(loan_row_index)
        header = loans_sheet.row_values(1)
        return loan, header
    
    loan, header = retry_with_backoff(get_loan)
    col_index = {name: idx + 1 for idx, name in enumerate(header)}
    
    requester = loan[col_index["Requester"] - 1]
    
    def update():
        loans_sheet.update_cell(loan_row_index, col_index["Status"], "Denied")
    
    retry_with_backoff(update)
    log_action(session.get("user", "System"), f"Denied loan for {requester}", None, "Denied")
    
    # Invalidate loan caches
    cache.invalidate("pending_loans", "all_loans", f"user_loans_{requester}")

def get_pending_deletions():
    """Get all pending account deletion requests with caching"""
    cache_key = "pending_deletions"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    try:
        def fetch():
            deletions_sheet = sheet.worksheet("Deletions")
            return deletions_sheet.get_all_records()
        
        rows = retry_with_backoff(fetch)
        pending = []
        for idx, row in enumerate(rows, start=2):
            if row.get("Status") == "Pending":
                pending.append({
                    "row": idx,
                    "Username": row.get("Username", ""),
                    "Requester": row.get("Requester", ""),
                    "Reason": row.get("Reason", ""),
                    "Date": row.get("Date", "")
                })
        cache.set(cache_key, pending)
        return pending
    except:
        return []

def get_pending_cash_burns():
    """Get all pending cash burn requests with caching"""
    cache_key = "pending_cashburns"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    try:
        def fetch():
            cashburns_sheet = sheet.worksheet("CashBurns")
            return cashburns_sheet.get_all_records()
        
        rows = retry_with_backoff(fetch)
        pending = []
        for idx, row in enumerate(rows, start=2):
            if row.get("Status") == "Pending":
                pending.append({
                    "row": idx,
                    "Requester": row.get("Requester", ""),
                    "Amount": float(row.get("Amount", 0)),
                    "Reason": row.get("Reason", ""),
                    "Date": row.get("Date", "")
                })
        cache.set(cache_key, pending)
        return pending
    except:
        return []

def get_pending_teacher_requests():
    """Get all pending teacher account requests with caching"""
    cache_key = "pending_teacher_requests"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    try:
        def fetch():
            teacher_requests_sheet = sheet.worksheet("TeacherRequests")
            return teacher_requests_sheet.get_all_records()
        
        rows = retry_with_backoff(fetch)
        pending = []
        for idx, row in enumerate(rows, start=2):
            if row.get("Status") == "Pending":
                pending.append({
                    "row": idx,
                    "Username": row.get("Username", ""),
                    "Password": row.get("Password", ""),
                    "Date": row.get("Request Date", "")
                })
        cache.set(cache_key, pending)
        return pending
    except:
        return []
    
def get_pending_loans():
    """Get all pending loan applications with caching"""
    cache_key = "pending_loans"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    def fetch():
        return loans_sheet.get_all_records()
    
    rows = retry_with_backoff(fetch)
    pending = []
    for idx, loan in enumerate(rows, start=2):
        if loan.get("Status") == "Pending":
            pending.append({
                "row": idx,
                "Requester": loan.get("Requester", ""),
                "Amount": float(loan.get("Amount", 0)),
                "Weeks": int(loan.get("Weeks", 0)),
                "Weekly": float(loan.get("Weekly", 0)),
                "Reason": loan.get("Reason", ""),
                "Date": loan.get("Date", "")
            })
    cache.set(cache_key, pending)
    return pending 

def approve_deletion(deletion_row_index):
    """Approve and execute account deletion"""
    def get_deletion():
        deletions_sheet = sheet.worksheet("Deletions")
        header = deletions_sheet.row_values(1)
        deletion = deletions_sheet.row_values(deletion_row_index)
        return deletions_sheet, header, deletion
    
    deletions_sheet, header, deletion = retry_with_backoff(get_deletion)
    col_index = {name: idx + 1 for idx, name in enumerate(header)}
    
    username = deletion[col_index["Username"] - 1]
    
    # Delete user from Users sheet
    try:
        def delete_user():
            cell = users_sheet.find(username)
            users_sheet.delete_rows(cell.row)
            # Update deletion status
            deletions_sheet.update_cell(deletion_row_index, col_index["Status"], "Approved")
        
        retry_with_backoff(delete_user)
        
        # Log action
        log_action(session["user"], f"Approved deletion of {username}", None, "Approved")
        
        # Invalidate caches
        cache.invalidate("all_users", "pending_deletions", f"user_data_{username}")
    except Exception as e:
        print(f"Error deleting user: {e}")

def deny_deletion(deletion_row_index):
    """Deny account deletion request"""
    def get_and_update():
        deletions_sheet = sheet.worksheet("Deletions")
        header = deletions_sheet.row_values(1)
        col_index = {name: idx + 1 for idx, name in enumerate(header)}
        deletions_sheet.update_cell(deletion_row_index, col_index["Status"], "Denied")
    
    retry_with_backoff(get_and_update)
    log_action(session["user"], "Denied deletion request", None, "Denied")
    cache.invalidate("pending_deletions")

def approve_cash_burn(burn_row_index):
    """Approve cash burn and remove money"""
    def get_burn():
        cashburns_sheet = sheet.worksheet("CashBurns")
        header = cashburns_sheet.row_values(1)
        burn = cashburns_sheet.row_values(burn_row_index)
        return cashburns_sheet, header, burn
    
    cashburns_sheet, header, burn = retry_with_backoff(get_burn)
    col_index = {name: idx + 1 for idx, name in enumerate(header)}
    
    requester = burn[col_index["Requester"] - 1]
    amount = float(burn[col_index["Amount"] - 1])
    
    # Remove money from account
    current_balance = get_user_balance(requester)
    update_balance(requester, current_balance - amount)
    
    # Add transaction
    add_transaction(requester, "Cash Burn", amount, "Cash burn approved")
    
    # Update status
    def update():
        cashburns_sheet.update_cell(burn_row_index, col_index["Status"], "Approved")
    
    retry_with_backoff(update)
    
    # Log action
    log_action(session["user"], f"Approved cash burn for {requester}: ${amount}", amount, "Approved")
    cache.invalidate("pending_cashburns")

def deny_cash_burn(burn_row_index):
    """Deny cash burn request"""
    def get_and_update():
        cashburns_sheet = sheet.worksheet("CashBurns")
        header = cashburns_sheet.row_values(1)
        col_index = {name: idx + 1 for idx, name in enumerate(header)}
        cashburns_sheet.update_cell(burn_row_index, col_index["Status"], "Denied")
    
    retry_with_backoff(get_and_update)
    log_action(session["user"], "Denied cash burn request", None, "Denied")
    cache.invalidate("pending_cashburns")
    
def get_user_loans(username):
    """Get user-specific loans with caching and countdown info"""
    cache_key = f"user_loans_{username}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    def fetch():
        return loans_sheet.get_all_records()
    
    rows = retry_with_backoff(fetch)
    user_loans = []
    today = datetime.now().date()
    
    for loan in rows:
        if loan["Requester"] == username:
            # Helper to safely convert to float (handles empty strings)
            def safe_float(value, default=0):
                if value == '' or value is None:
                    return default
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return default
            
            # Helper to safely convert to int (handles empty strings)
            def safe_int(value, default=0):
                if value == '' or value is None:
                    return default
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return default
            
            loan_data = {
                "Reason": loan.get("Reason", ""),
                "Amount": safe_float(loan.get("Amount", 0)),
                "Weeks": safe_int(loan.get("Weeks", 0)),
                "Weekly": safe_float(loan.get("Weekly", 0)),
                "Status": loan.get("Status", "Pending"),
                "Date": loan.get("Date", ""),
                "WeeksRemaining": safe_int(loan.get("WeeksRemaining", ""), None) if loan.get("WeeksRemaining") else None,
                "NextPaymentDate": loan.get("NextPaymentDate", ""),
                "TotalPaid": safe_float(loan.get("TotalPaid", 0)),
            }
            
            # Calculate days until next payment
            if loan_data["NextPaymentDate"] and loan_data["Status"] == "Active":
                try:
                    next_payment = datetime.strptime(loan_data["NextPaymentDate"], "%Y-%m-%d").date()
                    days_until = (next_payment - today).days
                    loan_data["DaysUntilPayment"] = days_until
                except:
                    loan_data["DaysUntilPayment"] = None
            else:
                loan_data["DaysUntilPayment"] = None
            
            # Calculate total amount (with interest)
            total_amount = loan_data["Weekly"] * loan_data["Weeks"]
            loan_data["TotalAmount"] = round(total_amount, 2)
            loan_data["RemainingAmount"] = round(total_amount - loan_data["TotalPaid"], 2)
            
            user_loans.append(loan_data)
    
    cache.set(cache_key, user_loans)
    return user_loans

def get_all_loans():
    """Fetch all loans from sheet with caching and countdown info"""
    cache_key = "all_loans"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    def fetch():
        return loans_sheet.get_all_records()
    
    rows = retry_with_backoff(fetch)
    loans = []
    today = datetime.now().date()
    
    # Helper to safely convert to float (handles empty strings)
    def safe_float(value, default=0):
        if value == '' or value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    # Helper to safely convert to int (handles empty strings)
    def safe_int(value, default=0):
        if value == '' or value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    for loan in rows:
        loan_data = {
            "Requester": loan.get("Requester", ""),
            "Reason": loan.get("Reason", ""),
            "Amount": safe_float(loan.get("Amount", 0)),
            "Weeks": safe_int(loan.get("Weeks", 0)),
            "Weekly": safe_float(loan.get("Weekly", 0)),
            "Status": loan.get("Status", "Pending"),
            "Date": loan.get("Date", ""),
            "WeeksRemaining": safe_int(loan.get("WeeksRemaining", ""), None) if loan.get("WeeksRemaining") else None,
            "NextPaymentDate": loan.get("NextPaymentDate", ""),
            "TotalPaid": safe_float(loan.get("TotalPaid", 0)),
        }
        
        # Calculate days until next payment
        if loan_data["NextPaymentDate"] and loan_data["Status"] == "Active":
            try:
                next_payment = datetime.strptime(loan_data["NextPaymentDate"], "%Y-%m-%d").date()
                days_until = (next_payment - today).days
                loan_data["DaysUntilPayment"] = days_until
            except:
                loan_data["DaysUntilPayment"] = None
        else:
            loan_data["DaysUntilPayment"] = None
        
        # Calculate total amount (with interest)
        total_amount = loan_data["Weekly"] * loan_data["Weeks"]
        loan_data["TotalAmount"] = round(total_amount, 2)
        loan_data["RemainingAmount"] = round(total_amount - loan_data["TotalPaid"], 2)
        
        loans.append(loan_data)
    
    # Sort: Active first, then Pending, then completed
    loans.sort(key=lambda x: (
        0 if x["Status"] == "Active" else (1 if x["Status"] == "Pending" else 2),
        x["Date"]
    ), reverse=True)
    cache.set(cache_key, loans)
    return loans

# ---------- ROUTES ----------
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        users = get_all_users()
        user = next((u for u in users if u["Username"] == username and u["Password"] == password), None)

        if user:
            session["user"] = username
            session["role"] = user.get("Role", "Student")  # Default to Student if Role is missing

            # set default theme
            if "theme" not in session:
                session["theme"] = "dark"

            return redirect(url_for("account"))

        return render_template("login.html", error="Invalid credentials")

    return render_template("login.html")

@app.route("/process_loans", methods=["POST"])
@role_required("Banker")
def process_loans():
    """Run this daily or weekly to process loan payments"""
    from datetime import timedelta
    
    today = datetime.now()
    
    def get_loan_data():
        header = loans_sheet.row_values(1)
        loans = loans_sheet.get_all_records()
        return header, loans
    
    # Get column indices
    header, loans = retry_with_backoff(get_loan_data)
    col_index = {name: idx + 1 for idx, name in enumerate(header)}
    
    processed_count = 0
    
    for idx, loan in enumerate(loans, start=2):  # Start at row 2 (after header)
        if loan.get("Status") != "Active":
            continue
        
        # Check if NextPaymentDate exists and is valid
        next_payment_str = loan.get("NextPaymentDate", "")
        if not next_payment_str:
            continue
            
        try:
            next_payment = datetime.strptime(next_payment_str, "%Y-%m-%d")
        except ValueError:
            continue
        
        if today >= next_payment:
            # Deduct payment
            username = loan["Requester"]
            weekly_payment = float(loan["Weekly"])
            
            try:
                current_balance = get_user_balance(username)
                
                if current_balance >= weekly_payment:
                    update_balance(username, current_balance - weekly_payment)
                    add_transaction(username, "Bank", weekly_payment, "Automatic loan payment")
                    
                    # Update loan
                    weeks_remaining = int(loan.get("WeeksRemaining", 0)) - 1
                    total_paid = float(loan.get("TotalPaid", 0)) + weekly_payment
                    
                    def update_loan():
                        loans_sheet.update_cell(idx, col_index["WeeksRemaining"], weeks_remaining)
                        loans_sheet.update_cell(idx, col_index["TotalPaid"], total_paid)
                        
                        if weeks_remaining <= 0:
                            loans_sheet.update_cell(idx, col_index["Status"], "Completed")
                        else:
                            new_date = (next_payment + timedelta(days=7)).strftime("%Y-%m-%d")
                            loans_sheet.update_cell(idx, col_index["NextPaymentDate"], new_date)
                    
                    retry_with_backoff(update_loan)
                    processed_count += 1
                else:
                    # Insufficient funds - mark as late?
                    log_action("System", f"Insufficient funds for {username} loan payment", weekly_payment, "Failed")
                    
            except Exception as e:
                print(f"Error processing loan for {username}: {e}")
                continue
    
    # Invalidate loan caches after processing
    cache.invalidate("all_loans", "pending_loans")
    cache.invalidate_pattern("user_loans_")
    
    flash(f"Processed {processed_count} loan payments")
    return redirect(url_for("federal_reserve"))

@app.route("/account")
def account():
    if "user" not in session:
        return redirect(url_for("login"))

    username = session["user"]
    balance = get_user_balance(username)
    transactions = get_user_transactions(username)
    loans = get_user_loans(username)  

    return render_template("account.html",
                           username=username,
                           balance=balance,
                           transactions=transactions,
                           loans=loans) 


@app.route("/transfer", methods=["POST"])
def transfer():
    if "user" not in session:
        return redirect(url_for("login"))

    sender = session["user"]
    receiver = request.form["receiver"]
    amount = float(request.form["amount"])
    comment = request.form.get("comment", "No comment")

    # Validate amount is positive
    if amount <= 0:
        flash("Amount must be greater than zero", "error")
        return redirect(url_for("account"))

    # Check if sender or receiver is frozen
    sender_frozen = is_frozen(sender)
    receiver_frozen = is_frozen(receiver)

    if sender_frozen:
        flash("Your account is frozen", "error")
        flash("Sending and Receiving money is disabled", "error")
        return redirect(url_for("account"))

    if receiver_frozen:
        # No separate "Your account" message here because it’s receiver’s account frozen
        flash("Sending and Receiving money is disabled", "error")
        return redirect(url_for("account"))

    if sender==receiver:
        flash("Sending money to oneself is disabled", "error")
        return redirect(url_for("account"))
            # Proceed with normal transfer if neither frozen
    if transfer_money(sender, receiver, amount, comment):
        flash("Transfer successful!")
    else:
        flash("Insufficient balance!")

    return redirect(url_for("account"))




@app.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("login"))


@app.route("/loan", methods=["GET", "POST"])
def loan():
    if "user" not in session:
        return redirect(url_for("login"))
    interest_rate = get_interest_rate()
    
    # Get weeks until project end
    max_weeks = get_weeks_until_project_end()
    project_end_date = get_project_end_date()

    if request.method == "POST":
        sender = session["user"]
        reason = request.form["reason"]
        amount = float(request.form["amount"])
        weeks = int(request.form["weeks"])
        
        # Validate amount and weeks are positive
        if amount <= 0:
            flash("Loan amount must be greater than zero", "error")
            return redirect(url_for("loan"))
        if weeks <= 0 or weeks > max_weeks:
            flash(f"Loan duration must be between 1 and {max_weeks} weeks", "error")
            return redirect(url_for("loan"))
        
        loan_money(sender, reason, amount, weeks)
        flash("Loan Application Received!")
        return redirect(url_for("account"))

    return render_template("loan.html", username=session["user"], irate=interest_rate, 
                         max_weeks=max_weeks, project_end_date=project_end_date)


@app.route("/teachertoolslogin", methods=["GET", "POST"])
def teacher_tools_login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        users = get_all_users()
        user = next((u for u in users if u["Username"] == username and u["Password"] == password), None)

        if user:
            session["user"] = username
            session["role"] = user.get("Role", "Student")  # Default to Student if Role is missing
            
            if session["role"] in ["Teacher", "Banker"]:

                # set default theme
                if "theme" not in session:
                    session["theme"] = "dark"

                return redirect(url_for("teacher_tools"))

        return render_template("teachertoolslogin.html", error="Invalid credentials")

    return render_template("teachertoolslogin.html")

@app.post("/freeze_account")
@role_required("Teacher", "Banker")
def freeze_account_route():
    username = request.form["username"]
    freeze_account(username)
    flash(f"{username} has been frozen.")
    return redirect(url_for("teacher_tools"))

@app.post("/unfreeze_account")
@role_required("Teacher", "Banker")
def unfreeze_account_route():
    username = request.form["username"]
    unfreeze_account(username)
    flash(f"{username} has been unfrozen.")
    return redirect(url_for("teacher_tools"))

@app.post("/create_account")
@role_required("Teacher", "Banker")
def create_account_route():
    username = request.form["new_username"]
    password = request.form["new_password"]

    # Prevent duplicates
    users = get_all_users()
    if any(u["Username"] == username for u in users):
        flash("Username already exists.")
        return redirect(url_for("teacher_tools"))

    create_account(username, password)
    flash("Account created successfully!")
    return redirect(url_for("teacher_tools"))

@app.route("/create_student_account", methods=["POST"])
def create_student_account():
    username = request.form["new_username"]
    password = request.form["new_password"]
    confirm = request.form["confirm_password"]

    # Server-side password confirmation check
    if password != confirm:
        flash("Passwords do not match!", "error")
        return redirect(url_for("login"))

    # Check for duplicate usernames
    users = get_all_users()
    if any(u["Username"] == username for u in users):
        flash("Username already exists!", "error")
        return redirect(url_for("login"))

    # Create account with $0 balance and Student role
    create_account(username, password)
    flash("Account created successfully! You can now log in.", "success")
    return redirect(url_for("login"))

@app.route("/request_teacher_account", methods=["POST"])
def request_teacher_account():
    username = request.form["new_username"]
    password = request.form["new_password"]
    confirm = request.form["confirm_password"]

    # Server-side password confirmation check
    if password != confirm:
        flash("Passwords do not match!", "error")
        return redirect(url_for("teacher_tools_login"))

    # Check for duplicate usernames
    users = get_all_users()
    if any(u["Username"] == username for u in users):
        flash("Username already exists!", "error")
        return redirect(url_for("teacher_tools_login"))

    # Add to teacher requests sheet for banker approval
    def add_request():
        try:
            teacher_requests_sheet = sheet.worksheet("TeacherRequests")
        except:
            # Create sheet if it doesn't exist
            teacher_requests_sheet = sheet.add_worksheet("TeacherRequests", rows=100, cols=5)
            teacher_requests_sheet.append_row(["Username", "Password", "Request Date", "Status", "Approved By"])
        
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        teacher_requests_sheet.append_row([username, password, now, "Pending", ""])
    
    retry_with_backoff(add_request)
    cache.invalidate_pattern("teacher")
    
    flash("Teacher account request sent! A banker will review it soon.", "success")
    return redirect(url_for("teacher_tools_login"))

@app.route("/delete_account", methods=["POST"])
@role_required("Teacher", "Banker")
def delete_account():
    username = request.form["username"]
    reason = request.form["reason"]
    
    # Log action with pending status
    log_action(session["user"], f"Requested deletion of {username}: {reason}", None, "Pending")
    
    flash(f"Deletion request for {username} is pending approval")
    return redirect(url_for("teacher_tools"))

def log_action(user, action, amount, acceptance):
    """Log actions to Logs sheet and invalidate cache"""
    def append():
        logs_sheet = sheet.worksheet("Logs")
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logs_sheet.append_row([user, action, amount or "", acceptance, now])
    
    retry_with_backoff(append)
    cache.invalidate("logs")
    
@app.route("/add_money", methods=["POST"])
@role_required("Teacher", "Banker")
def add_money():
    username = request.form["username"]
    amount = float(request.form["amount"])
    
    # Validate amount is positive
    if amount <= 0:
        flash("Amount must be greater than zero", "error")
        return redirect(url_for("teacher_tools"))
    
    # Update balance
    current_balance = get_user_balance(username)
    update_balance(username, current_balance + amount)
    
    # Log action
    log_action(session["user"], f"Added ${amount} to {username}", amount, "Approved")
    
    flash(f"Added ${amount} to {username}")
    return redirect(url_for("teacher_tools"))

@app.route("/subtract_money", methods=["POST"])
@role_required("Teacher", "Banker")
def subtract_money():
    username = request.form["username"]
    amount = float(request.form["amount"])
    
    # Validate amount is positive
    if amount <= 0:
        flash("Amount must be greater than zero", "error")
        return redirect(url_for("teacher_tools"))
    
    # Update balance
    current_balance = get_user_balance(username)
    update_balance(username, current_balance - amount)
    
    # Log action
    log_action(session["user"], f"Subtracted ${amount} from {username}", amount, "Approved")
    
    flash(f"Subtracted ${amount} from {username}")
    return redirect(url_for("teacher_tools"))

@app.route("/approve_loan/<int:row_index>", methods=["POST"])
@role_required("Banker")
def approve_loan_route(row_index):
    approve_loan(row_index)
    flash("Loan approved!")
    return redirect(url_for("federal_reserve"))

@app.route("/deny_loan/<int:row_index>", methods=["POST"])
@role_required("Banker")
def deny_loan_route(row_index):
    deny_loan(row_index)
    flash("Loan denied!")
    return redirect(url_for("federal_reserve"))

@app.route("/process_loan_payments", methods=["POST"])
@role_required("Banker")
def process_loan_payments_route():
    """Manually trigger automatic loan payments"""
    count = process_loan_payments()
    if count > 0:
        flash(f"Processed {count} loan payment(s) successfully!", "success")
    else:
        flash("No loan payments were due at this time.", "info")
    return redirect(url_for("federal_reserve"))

@app.route("/set_project_end_date", methods=["POST"])
@role_required("Banker")
def set_project_end_date_route():
    """Set the project end date for loan timing"""
    date_string = request.form.get("end_date")
    if date_string:
        try:
            # Validate date format
            datetime.strptime(date_string, "%Y-%m-%d")
            set_project_end_date(date_string)
            flash(f"Project end date set to {date_string}", "success")
        except ValueError:
            flash("Invalid date format. Use YYYY-MM-DD", "error")
    else:
        flash("No date provided", "error")
    return redirect(url_for("federal_reserve"))

@app.route("/approve_deletion/<int:row_index>", methods=["POST"])
@role_required("Banker")
def approve_deletion_route(row_index):
    def get_deletion():
        deletions_sheet = sheet.worksheet("Deletions")
        deletion = deletions_sheet.row_values(row_index)
        header = deletions_sheet.row_values(1)
        return deletions_sheet, deletion, header
    
    deletions_sheet, deletion, header = retry_with_backoff(get_deletion)
    col_index = {name: idx + 1 for idx, name in enumerate(header)}
    
    username = deletion[col_index["Username"] - 1]
    
    # Delete user from Users sheet
    try:
        def delete():
            cell = users_sheet.find(username)
            users_sheet.delete_rows(cell.row)
            # Update deletion status
            deletions_sheet.update_cell(row_index, col_index["Status"], "Approved")
        
        retry_with_backoff(delete)
        
        log_action(session["user"], f"Approved deletion of {username}", None, "Approved")
        flash(f"Account {username} has been deleted!")
        
        # Invalidate caches
        cache.invalidate("all_users", "pending_deletions", f"user_data_{username}")
    except Exception as e:
        flash(f"Error deleting account {username}")
        print(f"Deletion error: {e}")
    
    return redirect(url_for("federal_reserve"))

@app.route("/deny_deletion/<int:row_index>", methods=["POST"])
@role_required("Banker")
def deny_deletion_route(row_index):
    def get_and_update():
        deletions_sheet = sheet.worksheet("Deletions")
        header = deletions_sheet.row_values(1)
        col_index = {name: idx + 1 for idx, name in enumerate(header)}
        deletions_sheet.update_cell(row_index, col_index["Status"], "Denied")
    
    retry_with_backoff(get_and_update)
    flash("Deletion request denied!")
    cache.invalidate("pending_deletions")
    return redirect(url_for("federal_reserve"))

@app.route("/approve_cashburn/<int:row_index>", methods=["POST"])
@role_required("Banker")
def approve_cashburn_route(row_index):
    def get_burn():
        cashburns_sheet = sheet.worksheet("CashBurns")
        cashburn = cashburns_sheet.row_values(row_index)
        header = cashburns_sheet.row_values(1)
        return cashburns_sheet, cashburn, header
    
    cashburns_sheet, cashburn, header = retry_with_backoff(get_burn)
    col_index = {name: idx + 1 for idx, name in enumerate(header)}
    
    requester = cashburn[col_index["Requester"] - 1]
    amount = float(cashburn[col_index["Amount"] - 1])
    
    # Deduct from user's balance
    current_balance = get_user_balance(requester)
    update_balance(requester, current_balance - amount)
    
    # Add transaction
    add_transaction(requester, "Cash Burn", amount, "Cash burn approved")
    
    # Update status
    def update():
        cashburns_sheet.update_cell(row_index, col_index["Status"], "Approved")
    
    retry_with_backoff(update)
    
    log_action(session["user"], f"Approved cash burn for {requester}: ${amount}", amount, "Approved")
    flash("Cash burn approved!")
    cache.invalidate("pending_cashburns")
    return redirect(url_for("federal_reserve"))

@app.route("/deny_cashburn/<int:row_index>", methods=["POST"])
@role_required("Banker")
def deny_cashburn_route(row_index):
    def get_and_update():
        cashburns_sheet = sheet.worksheet("CashBurns")
        header = cashburns_sheet.row_values(1)
        col_index = {name: idx + 1 for idx, name in enumerate(header)}
        cashburns_sheet.update_cell(row_index, col_index["Status"], "Denied")
    
    retry_with_backoff(get_and_update)
    flash("Cash burn request denied!")
    cache.invalidate("pending_cashburns")
    return redirect(url_for("federal_reserve"))

@app.route("/approve_teacher_request/<int:row_index>", methods=["POST"])
@role_required("Banker")
def approve_teacher_request(row_index):
    def get_request_and_create():
        teacher_requests_sheet = sheet.worksheet("TeacherRequests")
        row = teacher_requests_sheet.row_values(row_index)
        
        if len(row) >= 3:
            username = row[0]
            password = row[1]
            
            # Create teacher account
            users_sheet.append_row([username, password, "0", "No", "Teacher"])
            
            # Update request status
            header = teacher_requests_sheet.row_values(1)
            col_index = {name: idx + 1 for idx, name in enumerate(header)}
            teacher_requests_sheet.update_cell(row_index, col_index["Status"], "Approved")
            teacher_requests_sheet.update_cell(row_index, col_index["Approved By"], session["user"])
            
            return username
        return None
    
    username = retry_with_backoff(get_request_and_create)
    
    if username:
        log_action(session["user"], f"Approved teacher account for {username}", None, "Approved")
        flash(f"Teacher account created for {username}!", "success")
    else:
        flash("Error approving teacher request", "error")
    
    cache.invalidate("pending_teacher_requests")
    cache.invalidate_pattern("users")
    return redirect(url_for("federal_reserve"))

@app.route("/deny_teacher_request/<int:row_index>", methods=["POST"])
@role_required("Banker")
def deny_teacher_request(row_index):
    def get_and_update():
        teacher_requests_sheet = sheet.worksheet("TeacherRequests")
        row = teacher_requests_sheet.row_values(row_index)
        username = row[0] if len(row) > 0 else "Unknown"
        
        header = teacher_requests_sheet.row_values(1)
        col_index = {name: idx + 1 for idx, name in enumerate(header)}
        teacher_requests_sheet.update_cell(row_index, col_index["Status"], "Denied")
        teacher_requests_sheet.update_cell(row_index, col_index["Approved By"], session["user"])
        
        return username
    
    username = retry_with_backoff(get_and_update)
    log_action(session["user"], f"Denied teacher account request for {username}", None, "Denied")
    flash(f"Teacher request for {username} denied", "info")
    cache.invalidate("pending_teacher_requests")
    return redirect(url_for("federal_reserve"))

def get_logs():
    """Get all logs from Logs sheet with caching"""
    cache_key = "logs"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    try:
        def fetch():
            logs_sheet = sheet.worksheet("Logs")
            return logs_sheet.get_all_records()
        
        rows = retry_with_backoff(fetch)
        logs = []
        for log in rows:
            logs.append({
                "User": log.get("User", ""),
                "Action": log.get("Action", ""),
                "Amount": log.get("Amount", ""),
                "Acceptance": log.get("Acceptance", ""),
                "Date": log.get("Date", "")
            })
        # Sort by date, newest first
        logs.sort(key=lambda x: x["Date"], reverse=True)
        cache.set(cache_key, logs)
        return logs
    except:
        return []

@app.route("/federalreserve")
@role_required("Banker")
def federal_reserve():
    recalculate_federal_reserve()
    data = get_federal_reserve_stats()
    users = get_all_users_with_balances()
    
    pending_loans = get_pending_loans()
    all_loans = get_all_loans()
    pending_deletions = get_pending_deletions()
    pending_cashburns = get_pending_cash_burns()
    pending_teacher_requests = get_pending_teacher_requests()
    logs = get_logs()
    
    # Get bank account info
    bank_account = get_bank_account()
    
    # Get project timing info
    project_end_date = get_project_end_date()
    weeks_remaining = get_weeks_until_project_end()
    
    return render_template("federalreserve.html", 
                         data=data, 
                         users=users,
                         pending_loans=pending_loans,
                         all_loans=all_loans,
                         pending_deletions=pending_deletions,
                         pending_cashburns=pending_cashburns,
                         pending_teacher_requests=pending_teacher_requests,
                         logs=logs,
                         bank_account=bank_account,
                         project_end_date=project_end_date,
                         weeks_remaining=weeks_remaining)

@app.route("/verify_bank_password", methods=["POST"])
@role_required("Banker")
def verify_bank_password():
    """Verify the bank administration password"""
    password = request.form.get("password", "")
    # You can change this password to whatever you want
    BANK_ADMIN_PASSWORD = "Banker67Admin"
    
    if password == BANK_ADMIN_PASSWORD:
        return jsonify({"success": True})
    else:
        return jsonify({"success": False, "message": "Incorrect password"})

@app.route("/create_bank_money", methods=["POST"])
@role_required("Banker")
def create_bank_money():
    """Create money and add it to the bank account"""
    password = request.form.get("password", "")
    amount = float(request.form.get("amount", 0))
    reason = request.form.get("reason", "")
    
    BANK_ADMIN_PASSWORD = "Banker67Admin"
    
    if password != BANK_ADMIN_PASSWORD:
        flash("Invalid bank administration password", "error")
        return redirect(url_for("federal_reserve"))
    
    if amount <= 0:
        flash("Amount must be greater than zero", "error")
        return redirect(url_for("federal_reserve"))
    
    # Get or create bank account
    bank_account = get_bank_account()
    current_balance = float(bank_account.get("Balance", 0))
    new_balance = current_balance + amount
    
    # Update bank balance
    update_bank_balance(new_balance)
    
    # Log the action
    log_action(session["user"], f"Created ${amount} for bank: {reason}", amount, "Approved")
    
    flash(f"Successfully created ${amount:.2f} for the bank!", "success")
    cache.invalidate_pattern("bank")
    cache.invalidate("fed_stats")
    return redirect(url_for("federal_reserve"))

@app.route("/transfer_from_bank", methods=["POST"])
@role_required("Banker")
def transfer_from_bank():
    """Transfer money from bank to a user account"""
    password = request.form.get("password", "")
    recipient = request.form.get("recipient", "")
    amount = float(request.form.get("amount", 0))
    reason = request.form.get("reason", "")
    
    BANK_ADMIN_PASSWORD = "Banker67Admin"
    
    if password != BANK_ADMIN_PASSWORD:
        flash("Invalid bank administration password", "error")
        return redirect(url_for("federal_reserve"))
    
    if amount <= 0:
        flash("Amount must be greater than zero", "error")
        return redirect(url_for("federal_reserve"))
    
    # Check if recipient exists
    users = get_all_users()
    if not any(u["Username"] == recipient for u in users):
        flash(f"User '{recipient}' not found", "error")
        return redirect(url_for("federal_reserve"))
    
    # Get bank balance
    bank_account = get_bank_account()
    bank_balance = float(bank_account.get("Balance", 0))
    
    if bank_balance < amount:
        flash(f"Insufficient bank funds. Bank has ${bank_balance:.2f}", "error")
        return redirect(url_for("federal_reserve"))
    
    # Deduct from bank
    update_bank_balance(bank_balance - amount)
    
    # Add to recipient
    recipient_balance = get_user_balance(recipient)
    update_balance(recipient, recipient_balance + amount)
    
    # Add transaction
    add_transaction(recipient, "Bank Transfer", amount, f"From bank: {reason}")
    
    # Log the action
    log_action(session["user"], f"Transferred ${amount} from bank to {recipient}: {reason}", amount, "Approved")
    
    flash(f"Successfully transferred ${amount:.2f} to {recipient}!", "success")
    cache.invalidate_pattern("bank")
    cache.invalidate_pattern("users")
    return redirect(url_for("federal_reserve"))

def get_bank_account():
    """Get the bank account info with caching"""
    cache_key = "bank_account"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    try:
        # Check if Bank user exists
        users = get_all_users()
        bank_user = next((u for u in users if u["Username"] == "Bank"), None)
        
        if not bank_user:
            # Create bank account if it doesn't exist
            def create_bank():
                users_sheet.append_row(["Bank", "BankPassword", "0", "No", "System"])
            retry_with_backoff(create_bank)
            cache.invalidate_pattern("users")
            bank_account = {"Username": "Bank", "Balance": "0", "Role": "System"}
        else:
            bank_account = bank_user
        
        cache.set(cache_key, bank_account)
        return bank_account
    except:
        return {"Username": "Bank", "Balance": "0", "Role": "System"}

def update_bank_balance(new_balance):
    """Update the bank account balance"""
    def update():
        cell = users_sheet.find("Bank")
        if cell:
            users_sheet.update_cell(cell.row, 3, new_balance)
        else:
            # Create bank account if not found
            users_sheet.append_row(["Bank", "BankPassword", str(new_balance), "No", "System"])
    
    retry_with_backoff(update)
    cache.invalidate("bank_account")
    cache.invalidate_pattern("users")


@app.route("/request_cashburn", methods=["POST"])
def request_cashburn():
    if "user" not in session:
        return redirect(url_for("login"))
    
    requester = session["user"]
    amount = float(request.form["amount"])
    reason = request.form["reason"]
    
    # Validate amount is positive
    if amount <= 0:
        flash("Amount must be greater than zero", "error")
        return redirect(url_for("teacher_tools"))
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def append():
        cashburns_sheet = sheet.worksheet("CashBurns")
        cashburns_sheet.append_row([requester, amount, reason, "Pending", now])
    
    retry_with_backoff(append)
    cache.invalidate("pending_cashburns")
    flash("Cash burn request submitted!")
    return redirect(url_for("teacher_tools"))

@app.route("/api/transactions/<username>")
@role_required("Banker", "Teacher")
def get_transactions_api(username):
    """API endpoint to get transactions for a specific user"""
    transactions = get_user_transactions(username)
    return jsonify(transactions)

@app.route("/teachertools")
@role_required("Teacher", "Banker")
def teacher_tools():
    if "user" not in session:
        return redirect(url_for("teacher_tools_login"))

    username = session["user"]
    users = get_all_users_with_balances()
    loans = get_all_loans()  # Add this
    
    normalize_roles_column()
    
    return render_template(
        "teachertools.html",
        users=users,
        username=username,
        loans=loans  # Pass loans
    )

@app.route("/adjust_money", methods=["POST"])
@role_required("Teacher", "Banker")
def adjust_money():
    username = request.form["username"]
    amount = float(request.form["amount"])
    action = request.form["action"]
    
    # Validate amount is positive
    if amount <= 0:
        flash("Amount must be greater than zero", "error")
        return redirect(url_for("teacher_tools"))
    
    current_balance = get_user_balance(username)
    
    if action == "add":
        new_balance = current_balance + amount
        log_action(session["user"], f"Added ${amount} to {username}", amount, "Approved")
    else:
        new_balance = current_balance - amount
        log_action(session["user"], f"Subtracted ${amount} from {username}", amount, "Approved")
    
    update_balance(username, new_balance)
    flash(f"Balance adjusted for {username}")
    return redirect(url_for("teacher_tools"))

@app.route("/set_money", methods=["POST"])
@role_required("Teacher", "Banker")
def set_money():
    username = request.form["username"]
    amount = float(request.form["amount"])
    
    # Validate amount is not negative (can be zero to clear balance)
    if amount < 0:
        flash("Balance cannot be negative", "error")
        return redirect(url_for("teacher_tools"))
    
    update_balance(username, amount)
    log_action(session["user"], f"Set balance to ${amount} for {username}", amount, "Approved")
    
    flash(f"Balance set to ${amount} for {username}")
    return redirect(url_for("teacher_tools"))


# ---------- THEME TOGGLE API ----------
@app.post("/toggle-theme")
def toggle_theme():
    # allow theme toggle for both logged-in AND logged-out users
    current = session.get("theme", "dark")
    new = "light" if current == "dark" else "dark"
    session["theme"] = new
    return jsonify({"theme": new})

if __name__ == "__main__":
    app.run(debug=True)
