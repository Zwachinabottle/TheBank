from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from datetime import timedelta
import time
import re
from threading import Lock

app = Flask(__name__, static_folder='images')
app.secret_key = "your_secret_key_here"
#https://tinyurl.com/KchingBanking
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

# Ensure Transactions sheet has a proper header row
trans_required_headers = ["Sender", "Receiver", "Amount", "Date", "Comment"]
trans_header = transactions_sheet.row_values(1)
if not trans_header or trans_header[0] != "Sender":
    if not trans_header:
        # Sheet is empty — write headers to row 1
        transactions_sheet.update('A1:E1', [trans_required_headers])
    else:
        # Sheet has data rows but no header — insert header row at the top
        transactions_sheet.insert_row(trans_required_headers, 1)

# Ensure sheet has required columns (but never delete existing headers!)
header = users_sheet.row_values(1)

required_headers = ["Username", "Password", "Balance", "Frozen", "Role", "Email", "AccountType", "CardNumber", "PIN", "WeeklyPayment"]

# Only add missing columns, never delete or reset headers
if len(header) < len(required_headers):
    # Add missing columns at the end
    for i in range(len(header), len(required_headers)):
        users_sheet.update_cell(1, i + 1, required_headers[i])
elif len(header) > 0:
    # Fix any incorrect column names in existing positions (without deleting row)
    for i, req_header in enumerate(required_headers):
        if i < len(header) and header[i] != req_header:
            users_sheet.update_cell(1, i + 1, req_header)
    



# ---------- HELPERS ----------
def get_exchange_rate():
    """Return the current time-era exchange rate multiplier (default 1.0)"""
    data = get_federal_reserve_stats()
    try:
        return float(data.get("ExchangeRate") or 1.0)
    except (ValueError, TypeError):
        return 1.0


def get_time_period():
    """Return the current time-era label (default empty string)"""
    data = get_federal_reserve_stats()
    return data.get("TimePeriod") or ""


def set_exchange_rate(rate, label=""):
    """Persist exchange rate multiplier and time-era label to the Reserve sheet"""
    set_fed_value("ExchangeRate", rate)
    set_fed_value("TimePeriod", label)


def get_personal_to_company_rate():
    """Return the current personal-to-company currency conversion rate (default 1.0)"""
    data = get_federal_reserve_stats()
    try:
        return float(data.get("PersonalToCompanyRate") or 1.0)
    except (ValueError, TypeError):
        return 1.0


def set_personal_to_company_rate(rate):
    """Persist the personal-to-company conversion rate to the Reserve sheet"""
    set_fed_value("PersonalToCompanyRate", rate)


def get_teacher_pin():
    """Read the teacher PIN from the SystemConfig row in the Reserve sheet (column C).
    Falls back to '4444' if not set."""
    cache_key = "teacher_pin"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        def fetch():
            r = sheet.worksheet("Reserve")
            config_cell = r.find("SystemConfig", in_column=1)
            if config_cell:
                row = r.row_values(config_cell.row)
                # Column C (index 2) = teacher_pin
                return row[2] if len(row) > 2 and row[2] else "4444"
            return "4444"
        pin = retry_with_backoff(fetch)
    except Exception:
        pin = "4444"
    cache.set(cache_key, pin)
    return pin


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

def get_display_name_from_email(email):
    """Extract display name from email (characters before the first dot)"""
    if not email or not isinstance(email, str):
        return ""
    
    # For company accounts with multiple emails, use the first one
    if ',' in email:
        email = email.split(',')[0].strip()
    
    # Extract the part before @ 
    if '@' in email:
        local_part = email.split('@')[0]
        # Get everything before the first dot
        if '.' in local_part:
            return local_part.split('.')[0].capitalize()
        return local_part.capitalize()
    
    return ""

def get_all_users():
    """Get all users with caching"""
    cache_key = "all_users"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    def fetch():
        # Use expected_headers to ensure correct column mapping
        expected = ["Username", "Password", "Balance", "Frozen", "Role", "Email", "AccountType", "CardNumber", "PIN", "WeeklyPayment"]
        return users_sheet.get_all_records(expected_headers=expected)
    
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
    """Get user transactions with caching — merges Transactions sheet + teacher adjustments from Logs"""
    cache_key = f"transactions_{username}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    def fetch_transactions():
        return transactions_sheet.get_all_records()

    def fetch_logs():
        try:
            logs = sheet.worksheet("Logs")
            return logs.get_all_records()
        except Exception:
            return []

    rows = retry_with_backoff(fetch_transactions)
    formatted = []

    # Pull from Transactions sheet
    for t in rows:
        if t.get("Sender") == username or t.get("Receiver") == username:
            try:
                amount = float(t["Amount"])
            except (ValueError, TypeError):
                continue
            formatted.append({
                "Sender": t.get("Sender", ""),
                "Receiver": t.get("Receiver", ""),
                "Amount": amount,
                "Date": t.get("Date", ""),
                "Comment": t.get("Comment", "No comment")
            })

    # Also pull teacher adjustments from Logs that reference this student
    log_rows = retry_with_backoff(fetch_logs)
    for log in log_rows:
        action = log.get("Action", "")
        teacher = log.get("User", "")
        timestamp = log.get("Timestamp", "")
        try:
            amount = float(log.get("Amount", 0))
        except (ValueError, TypeError):
            continue

        # Match "Added $X to <username>"
        if re.search(rf"\bAdded\b.*\bto {re.escape(username)}\b", action, re.IGNORECASE):
            formatted.append({
                "Sender": teacher,
                "Receiver": username,
                "Amount": amount,
                "Date": timestamp,
                "Comment": f"Teacher credit by {teacher}"
            })
        # Match "Subtracted $X from <username>"
        elif re.search(rf"\bSubtracted\b.*\bfrom {re.escape(username)}\b", action, re.IGNORECASE):
            formatted.append({
                "Sender": teacher,
                "Receiver": username,
                "Amount": amount,
                "Date": timestamp,
                "Comment": f"Teacher deduction by {teacher}"
            })
        # Match "Set balance to $X for <username>"
        elif re.search(rf"\bSet balance\b.*\bfor {re.escape(username)}\b", action, re.IGNORECASE):
            formatted.append({
                "Sender": teacher,
                "Receiver": username,
                "Amount": amount,
                "Date": timestamp,
                "Comment": f"Balance set by {teacher}"
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
    
    if not sender_user:
        return "sender_not_found"
    
    if not receiver_user:
        return "receiver_not_found"
    
    sender_balance = float(sender_user["Balance"])
    receiver_balance = float(receiver_user["Balance"])

    if sender_balance < amount:
        return "insufficient_balance"

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
    
    return "success"

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

def generate_card_number():
    """Generate a random 12-digit card number starting with 67"""
    import random
    # Format: 67XX-XXXX-XXXX (starts with 67 for Vex Bank, 10 random digits)
    remaining_digits = ''.join([str(random.randint(0, 9)) for _ in range(10)])
    # Format as 67XX-XXXX-XXXX for readability
    card_number = f"67{remaining_digits[:2]}-{remaining_digits[2:6]}-{remaining_digits[6:10]}"
    return card_number

def generate_pin():
    """Generate a random 4-digit PIN"""
    import random
    return ''.join([str(random.randint(0, 9)) for _ in range(4)])

def validate_username(username):
    """Validate username format and length"""
    if not username or len(username) < 3:
        return False, "Username must be at least 3 characters long"
    if len(username) > 30:
        return False, "Username must be less than 30 characters"
    return True, ""

def validate_password(password):
    """Validate password length"""
    if not password or len(password) < 4:
        return False, "Password must be at least 4 characters long"
    if len(password) > 30:
        return False, "Password must be less than 30 characters"
    return True, ""

def validate_email(email):
    """Validate email ends with @mypisd.net"""
    if not email:
        return False, "Email is required"
    if not email.lower().endswith("@mypisd.net"):
        return False, "Email must end with @mypisd.net"
    return True, ""

def create_account(username, password, email="", account_type="Personal"):
    """Create account and invalidate user cache"""
    card_number = generate_card_number()
    pin = generate_pin()
    
    def append():
        users_sheet.append_row([username, password, 0, "No", "Student", email, account_type, card_number, pin])
    
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
        "ProjectEndDate",  # Date when all loans must be paid off (format: YYYY-MM-DD)
        "TimePeriod",             # Current time era label (e.g. "Ancient Rome")
        "ExchangeRate",           # Multiplier applied to base weekly payments (default 1.0)
        "PersonalToCompanyRate"   # Exchange: 1 personal $ → N company $ (default 1.0)
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

def ensure_logs_sheet():
    """Ensure Logs sheet has proper headers"""
    try:
        logs_sheet = sheet.worksheet("Logs")
    except:
        # Create Logs sheet if it doesn't exist
        logs_sheet = sheet.add_worksheet("Logs", rows=1000, cols=5)
    
    required_headers = ["User", "Action", "Amount", "Acceptance", "Timestamp"]
    existing_headers = logs_sheet.row_values(1)
    
    # If header row is empty, write full header
    if not existing_headers or existing_headers == ['', '', '', '', '']:
        logs_sheet.update('A1:E1', [required_headers])
        return
    
    # Fix headers if they exist but are wrong
    if existing_headers[:5] != required_headers:
        logs_sheet.update('A1:E1', [required_headers])

def ensure_deletions_sheet():
    """Ensure Deletions sheet exists with proper headers"""
    try:
        deletions_sheet = sheet.worksheet("Deletions")
    except:
        deletions_sheet = sheet.add_worksheet("Deletions", rows=1000, cols=5)

    required_headers = ["Username", "Requester", "Reason", "Date", "Status"]
    existing_headers = deletions_sheet.row_values(1)

    if not existing_headers or existing_headers == ['', '', '', '', '']:
        deletions_sheet.update('A1:E1', [required_headers])

ensure_fed_sheet()
ensure_logs_sheet()
ensure_deletions_sheet()

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
        try:
            # Safely convert balance, skip if invalid
            balance_val = u.get("Balance", 0)
            if balance_val == "" or balance_val is None:
                balance = 0.0
            else:
                balance = float(balance_val)
            
            total_in_accounts += balance

            if u.get("Role") == "Student":
                student_money += balance
            elif u.get("Role") in ["Teacher", "Banker"]:
                teacher_money += balance
        except (ValueError, TypeError) as e:
            # Skip users with invalid balance data
            print(f"Warning: Skipping user {u.get('Username', 'Unknown')} due to invalid balance: {u.get('Balance', 'N/A')}")
            continue
    
    # Calculate total money LOANED OUT (approved loans still being paid back)
    total_loaned_out = 0
    total_loan_repaid = 0
    
    try:
        def fetch_loans():
            return loans_sheet.get_all_records(expected_headers=loans_sheet.row_values(1))
        
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
        balance_val = bank_account.get("Balance", 0)
        # Safely convert balance
        if balance_val == "" or balance_val is None:
            bank_balance = 0.0
        else:
            bank_balance = float(balance_val)
        lendable_funds = bank_balance + ((student_money + teacher_money) * 0.9)
        liquidity = round(lendable_funds, 2)
    except (ValueError, TypeError) as e:
        print(f"Warning: Error calculating liquidity, using fallback: {e}")
        # Fallback to old calculation if bank account doesn't exist or has invalid data
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
        header = loans_sheet.row_values(1)
        loans = loans_sheet.get_all_records(expected_headers=header)
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

def process_weekly_personal_payments():
    """Process weekly payments for Personal accounts only, scaled by the current exchange rate.
    
    Uses batched writes to stay well within Google Sheets API rate limits:
      - 1 call to read all user rows (for row indices)
      - 1 batch_update call for all balance changes
      - 1 append_rows call for all transaction records
      - 1 append_rows call for all log entries
    """
    all_users = get_all_users()
    exchange_rate = get_exchange_rate()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    balance_updates   = []   # (username, new_balance)
    transaction_rows  = []   # rows for Transactions sheet
    log_rows          = []   # rows for Logs sheet

    # ── Phase 1: collect all updates in memory (no API calls) ──────────────
    for user in all_users:
        if user.get("AccountType") != "Personal" or user.get("Role") != "Student":
            continue

        weekly_payment = user.get("WeeklyPayment", "")
        if not weekly_payment or weekly_payment == "":
            continue

        try:
            base_amount = float(weekly_payment)
            if base_amount <= 0:
                continue

            amount       = round(base_amount * exchange_rate, 2)
            username     = user.get("Username")
            new_balance  = round(float(user.get("Balance", 0)) + amount, 2)
            note         = (f"Automated weekly payment "
                            f"(base ${base_amount:.2f} × rate {exchange_rate:.4f})")

            balance_updates.append((username, new_balance))
            transaction_rows.append(["Weekly Payment", username, amount, now, note])
            log_rows.append(["System",
                             f"Automated weekly payment of ${amount} to {username} " + note,
                             amount, "Weekly Payment", now])
        except (ValueError, TypeError) as e:
            print(f"Error collecting weekly payment for "
                  f"{user.get('Username', 'Unknown')}: {e}")

    if not balance_updates:
        return 0

    payments_processed = len(balance_updates)

    # ── Phase 2: batch-update all balances (2 API calls total) ─────────────
    try:
        def do_balance_batch():
            all_rows = users_sheet.get_all_values()
            # Build username → spreadsheet row number map (rows are 1-indexed)
            username_to_row = {
                row[0]: idx + 1
                for idx, row in enumerate(all_rows)
                if idx > 0 and len(row) > 0
            }
            cell_updates = []
            for uname, new_bal in balance_updates:
                row_num = username_to_row.get(uname)
                if row_num:
                    cell_updates.append({"range": f"C{row_num}", "values": [[new_bal]]})
            if cell_updates:
                users_sheet.batch_update(cell_updates)

        retry_with_backoff(do_balance_batch)
        cache.invalidate("all_users")
        for uname, _ in balance_updates:
            cache.invalidate(f"user_balance_{uname}", f"user_data_{uname}")
    except Exception as e:
        print(f"Error batch-updating balances during weekly payments: {e}")
        payments_processed = 0   # nothing committed, report 0

    # ── Phase 3: batch-append transactions (1 API call) ────────────────────
    if transaction_rows:
        try:
            def append_transactions():
                transactions_sheet.append_rows(transaction_rows,
                                               value_input_option="USER_ENTERED")
            retry_with_backoff(append_transactions)
        except Exception as e:
            print(f"Error batch-appending weekly-payment transactions: {e}")

    # ── Phase 4: batch-append log entries (1 API call) ─────────────────────
    if log_rows:
        try:
            def append_logs():
                ls = sheet.worksheet("Logs")
                ls.append_rows(log_rows, value_input_option="USER_ENTERED")
            retry_with_backoff(append_logs)
            cache.invalidate("logs")
        except Exception as e:
            print(f"Error batch-appending weekly-payment logs: {e}")

    return payments_processed

def set_weekly_payment(username, amount):
    """Set weekly payment amount for a Personal account"""
    def update():
        cell = users_sheet.find(username)
        header = users_sheet.row_values(1)
        payment_col = header.index("WeeklyPayment") + 1
        users_sheet.update_cell(cell.row, payment_col, amount)
    
    retry_with_backoff(update)
    cache.invalidate("all_users", f"user_data_{username}")

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
            # Use expected_headers to ensure correct column mapping
            expected = ["Username", "Password", "Email", "Status", "ApprovedBy", "Requested Date"]
            return teacher_requests_sheet.get_all_records(expected_headers=expected)
        
        rows = retry_with_backoff(fetch)
        pending = []
        for idx, row in enumerate(rows, start=2):
            if row.get("Status") == "Pending":
                pending.append({
                    "row": idx,
                    "Username": row.get("Username", ""),
                    "Password": row.get("Password", ""),
                    "Email": row.get("Email", ""),
                    "Date": row.get("Requested Date", "")
                })
        cache.set(cache_key, pending)
        return pending
    except:
        return []

def get_pending_role_change_requests():
    """Get all pending role change requests with caching"""
    cache_key = "pending_role_change_requests"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    try:
        def fetch():
            role_requests_sheet = sheet.worksheet("RoleChangeRequests")
            return role_requests_sheet.get_all_records(expected_headers=role_requests_sheet.row_values(1))
        
        rows = retry_with_backoff(fetch)
        pending = []
        for idx, row in enumerate(rows, start=2):
            if row.get("Status") == "Pending":
                pending.append({
                    "row": idx,
                    "Username": row.get("Username", ""),
                    "CurrentRole": row.get("Current Role", ""),
                    "RequestedRole": row.get("Requested Role", ""),
                    "Reason": row.get("Reason", ""),
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
        return loans_sheet.get_all_records(expected_headers=loans_sheet.row_values(1))
    
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
        return loans_sheet.get_all_records(expected_headers=loans_sheet.row_values(1))
    
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
        return loans_sheet.get_all_records(expected_headers=loans_sheet.row_values(1))
    
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
        loans = loans_sheet.get_all_records(expected_headers=header)
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
    
    # Get user's card number and PIN
    all_users = get_all_users()
    user_data = next((u for u in all_users if u["Username"] == username), None)
    card_number = user_data.get("CardNumber", "N/A") if user_data else "N/A"
    pin = user_data.get("PIN", "N/A") if user_data else "N/A"

    return render_template("account.html",
                           username=username,
                           balance=balance,
                           transactions=transactions,
                           loans=loans,
                           card_number=card_number,
                           pin=pin) 


@app.route("/check_username", methods=["POST"])
def check_username():
    """AJAX endpoint to check if a username exists"""
    if "user" not in session:
        return jsonify({"exists": False, "error": "Not logged in"}), 401
    
    username = request.json.get("username", "").strip()
    
    if not username:
        return jsonify({"exists": False})
    
    all_users = get_all_users()
    user_exists = any(u["Username"] == username for u in all_users)
    
    # Check if frozen
    is_frozen_status = False
    if user_exists:
        is_frozen_status = is_frozen(username)
    
    return jsonify({
        "exists": user_exists,
        "frozen": is_frozen_status
    })

@app.route("/get_transactions", methods=["GET"])
def get_transactions():
    """AJAX endpoint to get user's recent transactions"""
    if "user" not in session:
        return jsonify({"error": "Not logged in"}), 401
    
    username = session["user"]
    transactions = get_user_transactions(username)
    
    return jsonify({
        "transactions": transactions,
        "username": username
    })

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

    # Check if sender and receiver exist
    all_users = get_all_users()
    sender_exists = any(u["Username"] == sender for u in all_users)
    receiver_exists = any(u["Username"] == receiver for u in all_users)
    
    if not sender_exists:
        flash("Your account was not found", "error")
        return redirect(url_for("account"))
    
    if not receiver_exists:
        flash(f"Account '{receiver}' does not exist", "error")
        return redirect(url_for("account"))

    # Get account types
    sender_user = next((u for u in all_users if u["Username"] == sender), None)
    receiver_user = next((u for u in all_users if u["Username"] == receiver), None)
    
    sender_account_type = sender_user.get("AccountType", "Personal") if sender_user else "Personal"
    receiver_account_type = receiver_user.get("AccountType", "Personal") if receiver_user else "Personal"
    
    # Block transfers between Personal and Company accounts
    if (sender_account_type == "Personal" and receiver_account_type == "Company") or \
       (sender_account_type == "Company" and receiver_account_type == "Personal"):
        error_msg = "Transfers between Personal and Company accounts are not allowed"
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"success": False, "message": error_msg}), 403
        flash(error_msg, "error")
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
    try:
        result = transfer_money(sender, receiver, amount, comment)
        
        if result == "success":
            success_msg = f"Successfully sent ${amount:.2f} to {receiver}!"
            # Get updated balance for AJAX response
            new_balance = get_user_balance(sender)
            
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({
                    "success": True, 
                    "message": success_msg,
                    "new_balance": new_balance
                })
            flash(success_msg, "success")
        elif result == "insufficient_balance":
            error_msg = "Insufficient balance!"
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({"success": False, "message": error_msg}), 400
            flash(error_msg, "error")
        elif result == "receiver_not_found":
            error_msg = f"Account '{receiver}' does not exist"
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({"success": False, "message": error_msg}), 400
            flash(error_msg, "error")
        elif result == "sender_not_found":
            error_msg = "Your account was not found"
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({"success": False, "message": error_msg}), 400
            flash(error_msg, "error")
        else:
            error_msg = "Transfer failed. Please try again."
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({"success": False, "message": error_msg}), 500
            flash(error_msg, "error")
    except Exception as e:
        error_msg = f"Error: {str(e)}"
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"success": False, "message": error_msg}), 500
        flash(error_msg, "error")

    return redirect(url_for("account"))


@app.route("/change_username", methods=["POST"])
def change_username():
    """Allow users to change their username"""
    if "user" not in session:
        flash("Please log in first", "error")
        return redirect(url_for("login"))
    
    current_username = session["user"]
    new_username = request.form.get("new_username", "").strip()
    password = request.form.get("password", "")
    
    # Validate inputs
    if not new_username or not password:
        flash("Please provide both a new username and password", "error")
        return redirect(url_for("account"))
    
    # Validate username format
    valid, error_msg = validate_username(new_username)
    if not valid:
        flash(error_msg, "error")
        return redirect(url_for("account"))
    
    if new_username == current_username:
        flash("New username is the same as current username", "error")
        return redirect(url_for("account"))
    
    # Check if new username already exists
    all_users = get_all_users()
    if any(u["Username"] == new_username for u in all_users):
        flash("Username already taken", "error")
        return redirect(url_for("account"))
    
    # Verify password
    current_user = next((u for u in all_users if u["Username"] == current_username), None)
    if not current_user or current_user.get("Password") != password:
        flash("Incorrect password", "error")
        return redirect(url_for("account"))
    
    try:
        # Update username in Users sheet
        def update_users():
            header = users_sheet.row_values(1)
            all_rows = users_sheet.get_all_values()
            username_col = header.index("Username") + 1
            
            for row_idx, row in enumerate(all_rows[1:], start=2):
                if row[0] == current_username:
                    users_sheet.update_cell(row_idx, username_col, new_username)
                    break
        
        retry_with_backoff(update_users)
        
        # Update username in Transactions sheet (both Sender and Receiver columns)
        def update_transactions():
            transactions_sheet = sheet.worksheet("Transactions")
            header = transactions_sheet.row_values(1)
            all_rows = transactions_sheet.get_all_values()
            
            sender_col = header.index("Sender") + 1
            receiver_col = header.index("Receiver") + 1
            
            for row_idx, row in enumerate(all_rows[1:], start=2):
                if len(row) >= max(sender_col, receiver_col):
                    if row[sender_col - 1] == current_username:
                        transactions_sheet.update_cell(row_idx, sender_col, new_username)
                    if row[receiver_col - 1] == current_username:
                        transactions_sheet.update_cell(row_idx, receiver_col, new_username)
        
        retry_with_backoff(update_transactions)
        
        # Update username in Loans sheet
        def update_loans():
            header = loans_sheet.row_values(1)
            all_rows = loans_sheet.get_all_values()
            
            if "Username" in header:
                username_col = header.index("Username") + 1
                for row_idx, row in enumerate(all_rows[1:], start=2):
                    if len(row) >= username_col and row[username_col - 1] == current_username:
                        loans_sheet.update_cell(row_idx, username_col, new_username)
        
        retry_with_backoff(update_loans)
        
        # Update username in Logs sheet if it exists
        try:
            def update_logs():
                logs_sheet = sheet.worksheet("Logs")
                header = logs_sheet.row_values(1)
                all_rows = logs_sheet.get_all_values()
                
                if "User" in header:
                    user_col = header.index("User") + 1
                    for row_idx, row in enumerate(all_rows[1:], start=2):
                        if len(row) >= user_col and row[user_col - 1] == current_username:
                            logs_sheet.update_cell(row_idx, user_col, new_username)
            
            retry_with_backoff(update_logs)
        except:
            pass  # Logs sheet might not exist or might not have records
        
        # Update username in CashBurns sheet if it exists
        try:
            def update_cashburns():
                cashburns_sheet = sheet.worksheet("CashBurns")
                header = cashburns_sheet.row_values(1)
                all_rows = cashburns_sheet.get_all_values()
                
                if "Requester" in header:
                    requester_col = header.index("Requester") + 1
                    for row_idx, row in enumerate(all_rows[1:], start=2):
                        if len(row) >= requester_col and row[requester_col - 1] == current_username:
                            cashburns_sheet.update_cell(row_idx, requester_col, new_username)
            
            retry_with_backoff(update_cashburns)
        except:
            pass
        
        # Update username in Deletions sheet if it exists
        try:
            def update_deletions():
                deletions_sheet = sheet.worksheet("Deletions")
                header = deletions_sheet.row_values(1)
                all_rows = deletions_sheet.get_all_values()
                
                if "Username" in header:
                    username_col = header.index("Username") + 1
                    for row_idx, row in enumerate(all_rows[1:], start=2):
                        if len(row) >= username_col and row[username_col - 1] == current_username:
                            deletions_sheet.update_cell(row_idx, username_col, new_username)
            
            retry_with_backoff(update_deletions)
        except:
            pass
        
        # Update username in RoleChangeRequests sheet if it exists
        try:
            def update_role_requests():
                role_requests_sheet = sheet.worksheet("RoleChangeRequests")
                header = role_requests_sheet.row_values(1)
                all_rows = role_requests_sheet.get_all_values()
                
                if "Username" in header:
                    username_col = header.index("Username") + 1
                    for row_idx, row in enumerate(all_rows[1:], start=2):
                        if len(row) >= username_col and row[username_col - 1] == current_username:
                            role_requests_sheet.update_cell(row_idx, username_col, new_username)
            
            retry_with_backoff(update_role_requests)
        except:
            pass
        
        # Update session with new username
        session["user"] = new_username
        
        # Invalidate all relevant caches
        cache.invalidate_pattern("user")
        cache.invalidate("all_users")
        cache.invalidate("transactions")
        cache.invalidate("loans")
        cache.invalidate("logs")
        
        # Log the action
        log_action(new_username, f"Changed username from {current_username} to {new_username}", None, "Approved")
        
        flash(f"Username successfully changed to {new_username}!", "success")
    except Exception as e:
        flash(f"Error changing username: {str(e)}", "error")
    
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

        return render_template("teachertoolslogin.html", error="Invalid credentials",
                               teacher_pin=get_teacher_pin())

    return render_template("teachertoolslogin.html", teacher_pin=get_teacher_pin())

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
    username = request.form["new_username"].strip()
    password = request.form["new_password"]
    email = request.form.get("email", "").strip()
    account_type = request.form.get("account_type", "Personal")

    # Validate username
    valid, error_msg = validate_username(username)
    if not valid:
        flash(error_msg, "error")
        return redirect(url_for("teacher_tools"))
    
    # Validate password
    valid, error_msg = validate_password(password)
    if not valid:
        flash(error_msg, "error")
        return redirect(url_for("teacher_tools"))
    
    # Validate email if provided
    if email:
        if account_type == "Personal":
            valid, error_msg = validate_email(email)
            if not valid:
                flash(error_msg, "error")
                return redirect(url_for("teacher_tools"))
        elif account_type == "Company":
            # Validate all emails in comma-separated list
            email_list = [e.strip() for e in email.split(',')]
            for single_email in email_list:
                valid, error_msg = validate_email(single_email)
                if not valid:
                    flash(f"{error_msg} (for {single_email})", "error")
                    return redirect(url_for("teacher_tools"))

    # Prevent duplicates
    users = get_all_users()
    if any(u["Username"] == username for u in users):
        flash("Username already exists.", "error")
        return redirect(url_for("teacher_tools"))

    create_account(username, password, email, account_type)
    flash("Account created successfully!", "success")
    return redirect(url_for("teacher_tools"))

@app.route("/create_student_account", methods=["POST"])
def create_student_account():
    username = request.form["new_username"].strip()
    password = request.form["new_password"]
    confirm = request.form["confirm_password"]
    email = request.form.get("email", "").strip()
    account_type = request.form.get("account_type", "Personal")

    # Validate username
    valid, error_msg = validate_username(username)
    if not valid:
        flash(error_msg, "error")
        return redirect(url_for("login"))
    
    # Validate password
    valid, error_msg = validate_password(password)
    if not valid:
        flash(error_msg, "error")
        return redirect(url_for("login"))

    # Server-side password confirmation check
    if password != confirm:
        flash("Passwords do not match!", "error")
        return redirect(url_for("login"))

    # Check for duplicate usernames
    users = get_all_users()
    if any(u["Username"] == username for u in users):
        flash("Username already exists!", "error")
        return redirect(url_for("login"))
    
    # Check for duplicate emails (only for Personal accounts)
    if account_type == "Personal":
        if not email:
            flash("Email is required for personal accounts!", "error")
            return redirect(url_for("login"))
        
        # Validate email domain
        valid, error_msg = validate_email(email)
        if not valid:
            flash(error_msg, "error")
            return redirect(url_for("login"))
        
        # Check if email already exists
        if any(u.get("Email", "").strip().lower() == email.lower() for u in users):
            flash("This email is already registered!", "error")
            return redirect(url_for("login"))
    
    # For Company accounts, email can contain multiple emails (comma-separated)
    if account_type == "Company":
        if not email:
            flash("At least one email is required for company accounts!", "error")
            return redirect(url_for("login"))
        
        # Validate all emails in comma-separated list
        email_list = [e.strip() for e in email.split(',')]
        for single_email in email_list:
            valid, error_msg = validate_email(single_email)
            if not valid:
                flash(f"{error_msg} (for {single_email})", "error")
                return redirect(url_for("login"))

    # Create account with $0 balance and Student role
    create_account(username, password, email, account_type)
    flash("Account created successfully! You can now log in.", "success")
    return redirect(url_for("login"))

@app.route("/request_teacher_account", methods=["POST"])
def request_teacher_account():
    username = request.form["new_username"].strip()
    password = request.form["new_password"]
    confirm = request.form["confirm_password"]
    email = request.form.get("email", "").strip()

    # Validate username
    valid, error_msg = validate_username(username)
    if not valid:
        flash(error_msg, "error")
        return redirect(url_for("teacher_tools_login"))
    
    # Validate password
    valid, error_msg = validate_password(password)
    if not valid:
        flash(error_msg, "error")
        return redirect(url_for("teacher_tools_login"))

    # Server-side password confirmation check
    if password != confirm:
        flash("Passwords do not match!", "error")
        return redirect(url_for("teacher_tools_login"))

    # Check for duplicate usernames
    users = get_all_users()
    if any(u["Username"] == username for u in users):
        flash("Username already exists!", "error")
        return redirect(url_for("teacher_tools_login"))
    
    # Validate email
    if not email:
        flash("Email is required for teacher accounts!", "error")
        return redirect(url_for("teacher_tools_login"))
    
    # Validate email domain
    valid, error_msg = validate_email(email)
    if not valid:
        flash(error_msg, "error")
        return redirect(url_for("teacher_tools_login"))

    # Add to teacher requests sheet for banker approval
    def add_request():
        try:
            teacher_requests_sheet = sheet.worksheet("TeacherRequests")
        except:
            # Create sheet if it doesn't exist
            teacher_requests_sheet = sheet.add_worksheet("TeacherRequests", rows=100, cols=6)
            teacher_requests_sheet.append_row(["Username", "Password", "Email", "Status", "ApprovedBy", "Requested Date"])
        
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        teacher_requests_sheet.append_row([username, password, email, "Pending", "", now])
    
    retry_with_backoff(add_request)
    cache.invalidate_pattern("teacher")
    
    flash("Teacher account request sent! A banker will review it soon.", "success")
    return redirect(url_for("teacher_tools_login"))

@app.route("/request_role_change", methods=["POST"])
def request_role_change():
    if "user" not in session:
        flash("You must be logged in to request a role change", "error")
        return redirect(url_for("login"))
    
    username = session["user"]
    current_role = session.get("role", "Student")
    requested_role = request.form.get("requested_role", "")
    reason = request.form.get("reason", "")
    
    if not requested_role or not reason:
        flash("Please provide both a role and reason for the request", "error")
        return redirect(url_for("account"))
    
    # Don't allow requesting same role
    if requested_role == current_role:
        flash("You already have this role!", "error")
        return redirect(url_for("account"))
    
    # Add to role change requests sheet
    def add_request():
        try:
            role_requests_sheet = sheet.worksheet("RoleChangeRequests")
        except:
            # Create sheet if it doesn't exist
            role_requests_sheet = sheet.add_worksheet("RoleChangeRequests", rows=100, cols=6)
            role_requests_sheet.append_row(["Username", "Current Role", "Requested Role", "Reason", "Request Date", "Status"])
        
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        role_requests_sheet.append_row([username, current_role, requested_role, reason, now, "Pending"])
    
    retry_with_backoff(add_request)
    cache.invalidate_pattern("role_change")
    
    flash("Role change request submitted! A banker will review it soon.", "success")
    return redirect(url_for("account"))

@app.route("/delete_account", methods=["POST"])
@role_required("Teacher", "Banker")
def delete_account():
    username = request.form["username"]
    reason = request.form["reason"]
    
    # Write deletion request to Deletions sheet
    def write_deletion():
        deletions_sheet = sheet.worksheet("Deletions")
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        deletions_sheet.append_row([username, session["user"], reason, now, "Pending"])

    try:
        retry_with_backoff(write_deletion)
        cache.invalidate("pending_deletions")
    except Exception as e:
        print(f"Error writing deletion request: {e}")
    
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
    cache.invalidate(f"transactions_{username}")
    
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
    cache.invalidate(f"transactions_{username}")
    
    flash(f"Subtracted ${amount} from {username}")
    return redirect(url_for("teacher_tools"))

@app.route("/approve_loan/<int:row_index>", methods=["POST"])
@role_required("Banker")
def approve_loan_route(row_index):
    try:
        approve_loan(row_index)
        flash("Loan approved!")
        
        # Return JSON for AJAX requests
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"success": True, "message": "Loan approved!"})
        return redirect(url_for("federal_reserve"))
    except Exception as e:
        error_msg = "API quota exceeded. Please wait a moment and try again." if "quota" in str(e).lower() else f"Error: {str(e)}"
        flash(error_msg, "error")
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"success": False, "message": error_msg}), 429 if "quota" in str(e).lower() else 500
        return redirect(url_for("federal_reserve"))

@app.route("/deny_loan/<int:row_index>", methods=["POST"])
@role_required("Banker")
def deny_loan_route(row_index):
    try:
        deny_loan(row_index)
        flash("Loan denied!")
        
        # Return JSON for AJAX requests
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"success": True, "message": "Loan denied!"})
        return redirect(url_for("federal_reserve"))
    except Exception as e:
        error_msg = "API quota exceeded. Please wait a moment and try again." if "quota" in str(e).lower() else f"Error: {str(e)}"
        flash(error_msg, "error")
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"success": False, "message": error_msg}), 429 if "quota" in str(e).lower() else 500
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

@app.route("/process_weekly_payments", methods=["POST"])
@role_required("Banker")
def process_weekly_payments_route():
    """Manually trigger weekly payments for Personal accounts"""
    try:
        payments_processed = process_weekly_personal_payments()
        flash(f"Processed {payments_processed} weekly payments for Personal accounts!")
        return redirect(url_for("teacher_tools"))
    except Exception as e:
        flash(f"Error processing weekly payments: {str(e)}", "error")
        return redirect(url_for("teacher_tools"))

@app.route("/set_exchange_rate", methods=["POST"])
@role_required("Teacher", "Banker")
def set_exchange_rate_route():
    """Set the time-era label and exchange rate multiplier"""
    label = request.form.get("time_period", "").strip()
    rate_str = request.form.get("exchange_rate", "1").strip()
    try:
        rate = float(rate_str)
        if rate <= 0:
            flash("Exchange rate must be greater than zero", "error")
            return redirect(url_for("teacher_tools"))
        set_exchange_rate(rate, label)
        cache.invalidate("fed_stats")
        era_display = f'"{label}" ' if label else ""
        log_action(session["user"],
                   f"Set time period {era_display}exchange rate to {rate:.4f}",
                   rate, "Exchange Rate")
        flash(f"Time period set to \"{label}\" with exchange rate {rate:.4f}x")
    except ValueError:
        flash("Invalid exchange rate", "error")
    return redirect(url_for("teacher_tools"))


@app.route("/convert_personal_to_company", methods=["POST"])
@role_required("Banker")
def convert_personal_to_company_route():
    """Convert an amount from a personal account to a company account using the current rate."""
    personal_username = request.form.get("personal_username", "").strip()
    company_username = request.form.get("company_username", "").strip()
    amount_str = request.form.get("amount", "0").strip()

    if not personal_username or not company_username or not amount_str:
        flash("All fields are required", "error")
        return redirect(url_for("federal_reserve"))

    try:
        amount = float(amount_str)
        if amount <= 0:
            flash("Amount must be greater than zero", "error")
            return redirect(url_for("federal_reserve"))
    except ValueError:
        flash("Invalid amount", "error")
        return redirect(url_for("federal_reserve"))

    all_users = get_all_users()
    personal_user = next((u for u in all_users if u["Username"] == personal_username), None)
    company_user  = next((u for u in all_users if u["Username"] == company_username), None)

    if not personal_user:
        flash(f"Personal account '{personal_username}' not found", "error")
        return redirect(url_for("federal_reserve"))
    if not company_user:
        flash(f"Company account '{company_username}' not found", "error")
        return redirect(url_for("federal_reserve"))
    if personal_user.get("AccountType") != "Personal":
        flash(f"'{personal_username}' is not a Personal account", "error")
        return redirect(url_for("federal_reserve"))
    if company_user.get("AccountType") != "Company":
        flash(f"'{company_username}' is not a Company account", "error")
        return redirect(url_for("federal_reserve"))

    personal_balance = float(personal_user.get("Balance", 0))
    if personal_balance < amount:
        flash(f"Insufficient personal balance (${personal_balance:.2f} available)", "error")
        return redirect(url_for("federal_reserve"))

    rate = get_personal_to_company_rate()
    company_received = round(amount * rate, 2)

    # Deduct from personal, credit to company
    update_balance(personal_username, personal_balance - amount)
    company_balance = float(company_user.get("Balance", 0))
    update_balance(company_username, company_balance + company_received)

    note = (f"Currency conversion: ${amount:.2f} personal → ${company_received:.2f} company "
            f"(rate {rate:.4f})")
    add_transaction("Currency Conversion", personal_username, -amount, note)
    add_transaction("Currency Conversion", company_username, company_received, note)
    log_action(session["user"], note, amount, "Currency Conversion")

    flash(f"Converted ${amount:.2f} from {personal_username} → ${company_received:.2f} deposited to {company_username} (rate {rate:.4f}x)")
    return redirect(url_for("federal_reserve"))


@app.route("/set_weekly_payment", methods=["POST"])
@role_required("Teacher", "Banker")
def set_weekly_payment_route():
    """Set weekly payment amount for a Personal account"""
    username = request.form["username"]
    amount = request.form.get("weekly_amount", "0")
    
    # Verify user exists and is a Personal account
    all_users = get_all_users()
    user = next((u for u in all_users if u["Username"] == username), None)
    
    if not user:
        flash(f"User {username} not found", "error")
        return redirect(url_for("teacher_tools"))
    
    if user.get("AccountType") != "Personal":
        flash(f"{username} is not a Personal account", "error")
        return redirect(url_for("teacher_tools"))
    
    try:
        amount_float = float(amount) if amount else 0
        if amount_float < 0:
            flash("Weekly payment cannot be negative", "error")
            return redirect(url_for("teacher_tools"))
        
        set_weekly_payment(username, amount_float)
        log_action(session["user"], f"Set weekly payment for {username} to ${amount_float}", amount_float, "Set Payment")
        flash(f"Set weekly payment for {username} to ${amount_float:.2f}")
        return redirect(url_for("teacher_tools"))
    except ValueError:
        flash("Invalid amount", "error")
        return redirect(url_for("teacher_tools"))

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
    try:
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
        
        # Return JSON for AJAX requests
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"success": True, "message": "Cash burn approved!"})
        return redirect(url_for("federal_reserve"))
    except Exception as e:
        error_msg = "API quota exceeded. Please wait a moment and try again." if "quota" in str(e).lower() else f"Error: {str(e)}"
        flash(error_msg, "error")
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"success": False, "message": error_msg}), 429 if "quota" in str(e).lower() else 500
        return redirect(url_for("federal_reserve"))

@app.route("/deny_cashburn/<int:row_index>", methods=["POST"])
@role_required("Banker")
def deny_cashburn_route(row_index):
    try:
        def get_and_update():
            cashburns_sheet = sheet.worksheet("CashBurns")
            header = cashburns_sheet.row_values(1)
            col_index = {name: idx + 1 for idx, name in enumerate(header)}
            cashburns_sheet.update_cell(row_index, col_index["Status"], "Denied")
        
        retry_with_backoff(get_and_update)
        flash("Cash burn request denied!")
        cache.invalidate("pending_cashburns")
        
        # Return JSON for AJAX requests
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"success": True, "message": "Cash burn denied!"})
        return redirect(url_for("federal_reserve"))
    except Exception as e:
        error_msg = "API quota exceeded. Please wait a moment and try again." if "quota" in str(e).lower() else f"Error: {str(e)}"
        flash(error_msg, "error")
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"success": False, "message": error_msg}), 429 if "quota" in str(e).lower() else 500
        return redirect(url_for("federal_reserve"))

@app.route("/approve_teacher_request/<int:row_index>", methods=["POST"])
@role_required("Banker")
def approve_teacher_request(row_index):
    try:
        def get_request_and_create():
            teacher_requests_sheet = sheet.worksheet("TeacherRequests")
            header = teacher_requests_sheet.row_values(1)
            row = teacher_requests_sheet.row_values(row_index)
            
            # Expected header: Username, Password, Email, Status, ApprovedBy, Requested Date
            if len(row) >= 3:
                username = row[0]  # Username
                password = row[1]  # Password
                email = row[2] if len(row) > 2 else ""  # Email
                
                # Generate card number and PIN for teacher
                card_number = generate_card_number()
                pin = generate_pin()
                
                # Create teacher account with email
                users_sheet.append_row([username, password, "0", "No", "Teacher", email, "Personal", card_number, pin])
                
                # Update request status
                col_index = {name: idx + 1 for idx, name in enumerate(header)}
                teacher_requests_sheet.update_cell(row_index, col_index.get("Status", 4), "Approved")
                teacher_requests_sheet.update_cell(row_index, col_index.get("ApprovedBy", 5), session["user"])
                
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
        
        # Return JSON for AJAX requests
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            if username:
                return jsonify({"success": True, "message": f"Teacher account created for {username}!"})
            else:
                return jsonify({"success": False, "message": "Error approving teacher request"})
        return redirect(url_for("federal_reserve"))
    except Exception as e:
        error_msg = "API quota exceeded. Please wait a moment and try again." if "quota" in str(e).lower() else f"Error: {str(e)}"
        flash(error_msg, "error")
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"success": False, "message": error_msg}), 429 if "quota" in str(e).lower() else 500
        return redirect(url_for("federal_reserve"))

@app.route("/deny_teacher_request/<int:row_index>", methods=["POST"])
@role_required("Banker")
def deny_teacher_request(row_index):
    try:
        def get_and_update():
            teacher_requests_sheet = sheet.worksheet("TeacherRequests")
            row = teacher_requests_sheet.row_values(row_index)
            username = row[0] if len(row) > 0 else "Unknown"
            
            header = teacher_requests_sheet.row_values(1)
            col_index = {name: idx + 1 for idx, name in enumerate(header)}
            teacher_requests_sheet.update_cell(row_index, col_index.get("Status", 4), "Denied")
            teacher_requests_sheet.update_cell(row_index, col_index.get("ApprovedBy", 5), session["user"])
            
            return username
        
        username = retry_with_backoff(get_and_update)
        log_action(session["user"], f"Denied teacher account request for {username}", None, "Denied")
        flash(f"Teacher request for {username} denied", "info")
        cache.invalidate("pending_teacher_requests")
        
        # Return JSON for AJAX requests
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"success": True, "message": f"Teacher request for {username} denied"})
        return redirect(url_for("federal_reserve"))
    except Exception as e:
        error_msg = "API quota exceeded. Please wait a moment and try again." if "quota" in str(e).lower() else f"Error: {str(e)}"
        flash(error_msg, "error")
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"success": False, "message": error_msg}), 429 if "quota" in str(e).lower() else 500
        return redirect(url_for("federal_reserve"))

@app.route("/approve_role_change/<int:row_index>", methods=["POST"])
@role_required("Banker")
def approve_role_change(row_index):
    def get_request_and_update():
        role_requests_sheet = sheet.worksheet("RoleChangeRequests")
        row = role_requests_sheet.row_values(row_index)
        
        if len(row) >= 3:
            username = row[0]
            requested_role = row[2]
            
            # Update user's role in Users sheet
            all_users = get_all_users()
            user = next((u for u in all_users if u["Username"] == username), None)
            
            if user:
                cell = users_sheet.find(username)
                if cell:
                    users_sheet.update_cell(cell.row, 5, requested_role)  # Column 5 is Role
            
            # Update request status
            header = role_requests_sheet.row_values(1)
            col_index = {name: idx + 1 for idx, name in enumerate(header)}
            role_requests_sheet.update_cell(row_index, col_index["Status"], "Approved")
            
            return username, requested_role
        return None, None
    
    username, new_role = retry_with_backoff(get_request_and_update)
    
    if username:
        log_action(session["user"], f"Approved role change for {username} to {new_role}", None, "Approved")
        flash(f"Role changed! {username} is now a {new_role}.", "success")
        cache.invalidate_pattern("users")
        cache.invalidate("pending_role_change_requests")
    else:
        flash("Error approving role change request", "error")
    
    return redirect(url_for("federal_reserve"))

@app.route("/deny_role_change/<int:row_index>", methods=["POST"])
@role_required("Banker")
def deny_role_change(row_index):
    def get_and_update():
        role_requests_sheet = sheet.worksheet("RoleChangeRequests")
        row = role_requests_sheet.row_values(row_index)
        username = row[0] if len(row) > 0 else "Unknown"
        
        header = role_requests_sheet.row_values(1)
        col_index = {name: idx + 1 for idx, name in enumerate(header)}
        role_requests_sheet.update_cell(row_index, col_index["Status"], "Denied")
        
        return username
    
    username = retry_with_backoff(get_and_update)
    log_action(session["user"], f"Denied role change request for {username}", None, "Denied")
    flash(f"Role change request for {username} denied", "info")
    cache.invalidate("pending_role_change_requests")
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
                "Date": log.get("Timestamp", "")  # Using Timestamp column
            })
        # Sort by date, newest first
        logs.sort(key=lambda x: x.get("Date", ""), reverse=True)
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
    
    # Sanitize user data - ensure Balance is a float
    sanitized_users = []
    for u in users:
        try:
            # Create a copy of the user dict
            user_copy = u.copy()
            # Safely convert balance to float
            balance_val = u.get("Balance", 0)
            if balance_val == "" or balance_val is None:
                user_copy["Balance"] = 0.0
            else:
                user_copy["Balance"] = float(balance_val)
            sanitized_users.append(user_copy)
        except (ValueError, TypeError):
            # If conversion fails, log and skip this user or set balance to 0
            print(f"Warning: Invalid balance for user {u.get('Username', 'Unknown')}: {u.get('Balance', 'N/A')}")
            user_copy = u.copy()
            user_copy["Balance"] = 0.0
            sanitized_users.append(user_copy)
    
    pending_loans = get_pending_loans()
    all_loans = get_all_loans()
    pending_deletions = get_pending_deletions()
    pending_cashburns = get_pending_cash_burns()
    pending_teacher_requests = get_pending_teacher_requests()
    pending_role_changes = get_pending_role_change_requests()
    logs = get_logs()
    
    # Get bank account info
    bank_account = get_bank_account()
    
    # Get project timing info
    project_end_date = get_project_end_date()
    weeks_remaining = get_weeks_until_project_end()
    personal_to_company_rate = get_personal_to_company_rate()
    teacher_pin = get_teacher_pin()

    return render_template("federalreserve.html", 
                         data=data, 
                         users=sanitized_users,
                         pending_loans=pending_loans,
                         all_loans=all_loans,
                         pending_deletions=pending_deletions,
                         pending_cashburns=pending_cashburns,
                         pending_teacher_requests=pending_teacher_requests,
                         pending_role_changes=pending_role_changes,
                         logs=logs,
                         bank_account=bank_account,
                         project_end_date=project_end_date,
                         weeks_remaining=weeks_remaining,
                         personal_to_company_rate=personal_to_company_rate,
                         teacher_pin=teacher_pin)

@app.route("/repair_user_data")
@role_required("Banker")
def repair_user_data():
    """Repair corrupted user data where Balance column has role names"""
    try:
        header = users_sheet.row_values(1)
        all_rows = users_sheet.get_all_values()
        
        # Find column indices
        balance_col = header.index("Balance") + 1  # Column 3
        role_col = header.index("Role") + 1  # Column 5
        
        repaired_count = 0
        errors = []
        
        for idx, row in enumerate(all_rows[1:], start=2):  # Skip header, start at row 2
            if len(row) >= max(balance_col, role_col):
                balance_val = row[balance_col - 1] if balance_col <= len(row) else ""
                role_val = row[role_col - 1] if role_col <= len(row) else ""
                username = row[0] if len(row) > 0 else f"Row {idx}"
                
                # Check if balance contains a role name (Student/Teacher/Banker)
                if balance_val in ["Student", "Teacher", "Banker"]:
                    # Balance is corrupted - set to 0 and move value to Role column
                    users_sheet.update_cell(idx, balance_col, 0)
                    
                    # If role column is empty or wrong, set it to the value from balance
                    if not role_val or role_val not in ["Student", "Teacher", "Banker"]:
                        users_sheet.update_cell(idx, role_col, balance_val)
                    
                    repaired_count += 1
                    print(f"Repaired {username}: Balance '{balance_val}' → 0, Role → '{balance_val}'")
                
                # Also check if role column has numeric values (swapped data)
                elif role_val and role_val.replace('.', '').replace('-', '').isdigit():
                    # Role column has a number - might be swapped
                    try:
                        numeric_val = float(role_val)
                        if balance_val in ["Student", "Teacher", "Banker"]:
                            # Swap: balance has role, role has number
                            users_sheet.update_cell(idx, balance_col, numeric_val)
                            users_sheet.update_cell(idx, role_col, balance_val)
                            repaired_count += 1
                            print(f"Swapped {username}: Balance ← {numeric_val}, Role ← '{balance_val}'")
                    except ValueError:
                        pass
        
        # Invalidate cache after repairs
        cache.invalidate("all_users")
        cache.invalidate_pattern("users")
        cache.invalidate_pattern("user_")
        
        flash(f"Repaired {repaired_count} user records", "success")
        return redirect(url_for("federal_reserve"))
        
    except Exception as e:
        flash(f"Error repairing data: {str(e)}", "error")
        return redirect(url_for("federal_reserve"))

@app.route("/generate_missing_cards")
@role_required("Banker")
def generate_missing_cards():
    """Generate card numbers and PINs for accounts that don't have them"""
    try:
        header = users_sheet.row_values(1)
        all_rows = users_sheet.get_all_values()
        
        # Find column indices
        card_col = header.index("CardNumber") + 1 if "CardNumber" in header else None
        pin_col = header.index("PIN") + 1 if "PIN" in header else None
        
        if not card_col or not pin_col:
            flash("CardNumber or PIN column not found in Users sheet", "error")
            return redirect(url_for("federal_reserve"))
        
        updated_count = 0
        
        # Start from row 2 (skip header)
        for row_idx in range(2, len(all_rows) + 1):
            row = all_rows[row_idx - 1]
            
            # Check if this row needs card number or PIN
            needs_card = len(row) < card_col or not row[card_col - 1]
            needs_pin = len(row) < pin_col or not row[pin_col - 1]
            
            if needs_card or needs_pin:
                # Generate card and PIN
                new_card = generate_card_number()
                new_pin = generate_pin()
                
                # Update the cells
                if needs_card:
                    users_sheet.update_cell(row_idx, card_col, new_card)
                if needs_pin:
                    users_sheet.update_cell(row_idx, pin_col, new_pin)
                
                updated_count += 1
        
        cache.invalidate("all_users")
        flash(f"Successfully generated card numbers and PINs for {updated_count} accounts!", "success")
    except Exception as e:
        flash(f"Error generating cards: {str(e)}", "error")
    
    return redirect(url_for("federal_reserve"))

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

@app.route("/save_system_setting", methods=["POST"])
@role_required("Banker")
def save_system_setting():
    """Save system configuration settings"""
    setting_type = request.form.get("setting_type", "")
    value = request.form.get("value", "")
    
    if not setting_type or not value:
        return jsonify({"success": False, "message": "Missing parameters"})
    
    try:
        # Get the Reserve sheet (for storing config)
        fed_sheet = sheet.worksheet("Reserve")
        
        # Get current config or create structure
        try:
            config_row = fed_sheet.find("SystemConfig", in_column=1)
            if config_row:
                row_num = config_row.row
            else:
                # Create config row
                fed_sheet.append_row(["SystemConfig", "", "", "", "", "", ""])
                row_num = len(fed_sheet.get_all_values())
        except:
            # If not found, append new row
            fed_sheet.append_row(["SystemConfig", "", "", "", "", "", ""])
            row_num = len(fed_sheet.get_all_values())
        
        # Map settings to columns
        # Column mapping: A=Label, B=BankerPassword, C=TeacherPIN, D=CardPrefix, E=MaxInterest, F=MinInterest, G=ReserveReq
        column_map = {
            "banker_password": 2,  # Column B
            "teacher_pin": 3,       # Column C
            "card_prefix": 4,       # Column D
            "max_interest": 5,      # Column E
            "min_interest": 6,      # Column F
            "reserve_requirement": 7 # Column G
        }
        
        if setting_type == "project_end_date":
            # Project end date uses the existing mechanism
            fed_sheet.update_cell(2, 1, value)  # Update A2 with the date
            cache.invalidate_pattern("federal")
            return jsonify({"success": True, "message": "Project end date updated"})

        if setting_type == "currency_conversion_rate":
            # Personal-to-company conversion rate stored directly in Reserve row 2
            try:
                rate = float(value)
                if rate <= 0:
                    return jsonify({"success": False, "message": "Rate must be greater than zero"})
                set_personal_to_company_rate(rate)
                cache.invalidate("fed_stats")
                return jsonify({"success": True, "message": f"Personal→Company rate set to {rate:.4f}"})
            except ValueError:
                return jsonify({"success": False, "message": "Invalid rate value"})
        
        if setting_type not in column_map:
            return jsonify({"success": False, "message": "Invalid setting type"})
        
        # Update the specific setting
        col_num = column_map[setting_type]
        fed_sheet.update_cell(row_num, col_num, value)
        
        # Invalidate cache
        cache.invalidate_pattern("federal")
        cache.invalidate_pattern("system_config")
        if setting_type == "teacher_pin":
            cache.invalidate("teacher_pin")
        
        return jsonify({"success": True, "message": f"{setting_type} updated successfully"})
        
    except Exception as e:
        print(f"Error saving system setting: {e}")
        return jsonify({"success": False, "message": str(e)})

@app.route("/change_user_role", methods=["POST"])
@role_required("Banker")
def change_user_role():
    """Change a user's role"""
    username = request.form.get("username", "")
    new_role = request.form.get("new_role", "")
    
    if not username or not new_role:
        return jsonify({"success": False, "message": "Missing parameters"})
    
    # Validate role
    valid_roles = ["Student", "Teacher", "Banker"]
    if new_role not in valid_roles:
        return jsonify({"success": False, "message": "Invalid role"})
    
    # Prevent changing Bank account
    if username == "Bank":
        return jsonify({"success": False, "message": "Cannot change Bank account role"})
    
    try:
        # Find the user in the Users sheet
        user_cell = users_sheet.find(username, in_column=1)
        if not user_cell:
            return jsonify({"success": False, "message": f"User {username} not found"})
        
        row_num = user_cell.row
        
        # Get header to find Role column index
        # Expected: Username, Password, Balance, Frozen, Role, Email, AccountType, CardNumber, PIN
        header = users_sheet.row_values(1)
        try:
            role_col_index = header.index("Role") + 1  # +1 because gspread uses 1-based indexing
        except ValueError:
            return jsonify({"success": False, "message": "Role column not found in sheet"})
        
        # Update the role (Column 5 - Role is at index 4, so column 5)
        users_sheet.update_cell(row_num, role_col_index, new_role)
        
        # Log the action
        log_action(session["user"], f"Changed {username}'s role to {new_role}", None, "Approved")
        
        # Invalidate cache
        cache.invalidate_pattern("users")
        cache.invalidate("all_users", f"user_data_{username}")
        
        return jsonify({"success": True, "message": f"Role changed to {new_role}"})
        
    except Exception as e:
        print(f"Error changing user role: {e}")
        return jsonify({"success": False, "message": str(e)})

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
                users_sheet.append_row(["Bank", "BankPassword", "0", "No", "System", "", "System", "0000-0000-0000-0000", "0000"])
            retry_with_backoff(create_bank)
            cache.invalidate_pattern("users")
            bank_account = {"Username": "Bank", "Balance": "0", "Role": "System", "Email": "", "AccountType": "System", "CardNumber": "0000-0000-0000-0000", "PIN": "0000"}
        else:
            bank_account = bank_user
        
        cache.set(cache_key, bank_account)
        return bank_account
    except:
        return {"Username": "Bank", "Balance": "0", "Role": "System", "Email": "", "AccountType": "System", "CardNumber": "0000-0000-0000-0000", "PIN": "0000"}

def update_bank_balance(new_balance):
    """Update the bank account balance"""
    def update():
        cell = users_sheet.find("Bank")
        if cell:
            users_sheet.update_cell(cell.row, 3, new_balance)
        else:
            # Create bank account if not found
            users_sheet.append_row(["Bank", "BankPassword", str(new_balance), "No", "System", "", "System", "0000-0000-0000-0000", "0000"])
    
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
    
    # Add display names to all users based on their email
    for user in users:
        user["DisplayName"] = get_display_name_from_email(user.get("Email", ""))
    
    # Separate users into Personal and Company accounts
    personal_students = [u for u in users if u.get("Role") == "Student" and u.get("AccountType") == "Personal"]
    company_students = [u for u in users if u.get("Role") == "Student" and u.get("AccountType") == "Company"]
    teachers = [u for u in users if u.get("Role") in ["Teacher", "Banker"]]
    
    normalize_roles_column()
    
    exchange_rate = get_exchange_rate()
    time_period = get_time_period()

    return render_template(
        "teachertools.html",
        users=users,
        personal_students=personal_students,
        company_students=company_students,
        teachers=teachers,
        username=username,
        loans=loans,
        exchange_rate=exchange_rate,
        time_period=time_period
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
    cache.invalidate(f"transactions_{username}")
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
    cache.invalidate(f"transactions_{username}")
    
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
