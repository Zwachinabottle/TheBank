from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from datetime import timedelta
import time
import re
from threading import Lock, Thread

app = Flask(__name__, static_folder='images')
app.secret_key = "your_secret_key_here"
#https://tinyurl.com/KchingBanking
# ---------- CACHING SYSTEM ----------
class SheetCache:
    """In-memory cache with TTL to minimize Google Sheets API calls.

    Per-key TTL overrides let high-churn data (user balances) expire quickly
    while structural config (column maps, teacher PIN) stays warm for many
    minutes — reducing total API calls dramatically.

    TTL constants (seconds):
        LONG_TTL   = 600   structural / config (headers, teacher PIN)
        MEDIUM_TTL = 180   shared raw sheet dumps (transactions, loans)
        SHORT_TTL  = 90    per-user derived views
        DEFAULT_TTL = 60   general default
    """
    LONG_TTL   = 600
    MEDIUM_TTL = 180
    SHORT_TTL  = 90
    DEFAULT_TTL = 60

    def __init__(self, ttl=60):
        self.cache = {}
        self.ttl = ttl  # instance default TTL in seconds
        self.lock = Lock()

    def get(self, key):
        """Return cached value for *key*, or None if missing/expired."""
        with self.lock:
            if key in self.cache:
                value, timestamp, key_ttl = self.cache[key]
                if time.time() - timestamp < key_ttl:
                    return value
                del self.cache[key]
            return None

    def set(self, key, value, ttl=None):
        """Store *value* under *key*.

        *ttl* overrides the instance default for this entry only.  Use the
        SheetCache.LONG_TTL / MEDIUM_TTL / SHORT_TTL class constants.
        """
        with self.lock:
            effective_ttl = ttl if ttl is not None else self.ttl
            self.cache[key] = (value, time.time(), effective_ttl)
    
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

# Global cache instance — default 60 s per entry; high-churn keys use
# per-call ttl= overrides (see SheetCache constants above).
cache = SheetCache()  # ttl=60 default

# In-memory override for the Investment Floor's active week.
# Set immediately by set_investment_week — no Google Sheets round-trip lag.
# Falls back to the sheet's CurrentWeek row if empty (e.g. after server restart).
_investment_week_override: str = ""

# Per-user transfer locks — prevent double-spend race conditions on concurrent requests
_transfer_locks: dict = {}
_transfer_locks_meta = Lock()

def get_transfer_lock(username: str):
    """Get or create a per-user Lock for transfer operations."""
    with _transfer_locks_meta:
        if username not in _transfer_locks:
            _transfer_locks[username] = Lock()
        return _transfer_locks[username]

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

# Pre-load the Logs worksheet once at startup so log_action never pays the
# cost of a runtime sheet.worksheet() API lookup on every teacher action.
try:
    logs_sheet = sheet.worksheet("Logs")
except Exception:
    logs_sheet = sheet.add_worksheet("Logs", rows=1000, cols=5)

# Pre-load the CashBurns worksheet once at startup for the same reason.
try:
    cashburns_sheet = sheet.worksheet("CashBurns")
except Exception:
    cashburns_sheet = sheet.add_worksheet("CashBurns", rows=1000, cols=5)

# Pre-load the Ads worksheet for the ad management system.
try:
    ads_sheet = sheet.worksheet("Ads")
except Exception:
    ads_sheet = sheet.add_worksheet("Ads", rows=1000, cols=9)
    ads_sheet.update([["ID", "Title", "ImageURL", "LinkURL", "Pages", "Schedule", "Priority", "Interval", "Active"]], 'A1:I1')

# Pre-load the Lottery tickets sheet.
try:
    lottery_sheet = sheet.worksheet("Lottery")
except Exception:
    lottery_sheet = sheet.add_worksheet("Lottery", rows=5000, cols=6)
    lottery_sheet.update([["TicketID", "Username", "Number1", "VexBall", "PurchaseDate", "Drawing"]], 'A1:F1')

# Pre-load the LotteryLogs sheet (purchase records & win/loss events).
try:
    lottery_logs_sheet = sheet.worksheet("LotteryLogs")
except Exception:
    lottery_logs_sheet = sheet.add_worksheet("LotteryLogs", rows=5000, cols=5)
    lottery_logs_sheet.update([["Username", "Type", "Amount", "Date", "Description"]], 'A1:E1')

# Load the Investments sheet (teacher-maintained company net-worth table).
try:
    investments_sheet = sheet.worksheet("Investments")
except Exception:
    investments_sheet = None  # page will show a friendly error if missing

# Pre-load the StockHoldings sheet (per-user investment tracking).
# Schema: Username | Company | InvestedAmount | NetWorthAtInvestment
try:
    stock_holdings_sheet = sheet.worksheet("StockHoldings")
    # Migrate old schema (Ticker/Shares/AvgCostBasis) → new schema if needed
    _sh_header = stock_holdings_sheet.row_values(1)
    if _sh_header and _sh_header[1:3] == ["Ticker", "Shares"]:
        stock_holdings_sheet.clear()
        stock_holdings_sheet.update([["Username", "Company", "InvestedAmount", "NetWorthAtInvestment"]], 'A1:D1')
except Exception:
    stock_holdings_sheet = sheet.add_worksheet("StockHoldings", rows=2000, cols=4)
    stock_holdings_sheet.update([["Username", "Company", "InvestedAmount", "NetWorthAtInvestment"]], 'A1:D1')

# Pre-load FundRequests sheet (pending investment fund requests from students).
try:
    fund_requests_sheet = sheet.worksheet("FundRequests")
except Exception:
    fund_requests_sheet = sheet.add_worksheet("FundRequests", rows=2000, cols=4)
    fund_requests_sheet.update([["Username", "Amount", "Status", "RequestedAt"]], 'A1:D1')

# Pre-load InvestFunds sheet (approved investment fund balances per user).
try:
    invest_funds_sheet = sheet.worksheet("InvestFunds")
except Exception:
    invest_funds_sheet = sheet.add_worksheet("InvestFunds", rows=1000, cols=2)
    invest_funds_sheet.update([["Username", "Balance"]], 'A1:B1')

# Ensure Transactions sheet has a proper header row
trans_required_headers = ["Sender", "Receiver", "Amount", "Date", "Comment"]
trans_header = transactions_sheet.row_values(1)
if not trans_header or trans_header[0] != "Sender":
    if not trans_header:
        # Sheet is empty — write headers to row 1
        transactions_sheet.update([trans_required_headers], 'A1:E1')
    else:
        # Sheet has data rows but no header — insert header row at the top
        transactions_sheet.insert_row(trans_required_headers, 1)

# ---------- STATIC COLUMN-INDEX MAPS (computed once, no extra API calls) ----------
# These map column name → 1-based column number and never change during a session.
_users_header = users_sheet.row_values(1)
_USERS_COLS: dict = {name: idx + 1 for idx, name in enumerate(_users_header)}

_loans_header = loans_sheet.row_values(1) if loans_sheet else []
_LOANS_HEADERS: list = _loans_header  # used as expected_headers in get_all_records

# Ensure sheet has required columns (but never delete existing headers!)
header = _users_header

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
    cache.set(cache_key, pin, ttl=SheetCache.LONG_TTL)
    return pin


def get_interest_rate():
    """
    Calculate interest rate based purely on the bank's own account balance.
    - High bank balance = LOW interest rates (0.5%)
    - Low/empty bank balance = HIGH interest rates (5%)
    Uses a cap of $100,000 and steps in $1,000 increments.
    """
    BALANCE_CAP = 100000.0  # Balance considered "full" (min rate)
    STEP = 1000.0            # Rate changes every $1,000

    try:
        bank_account = get_bank_account()
        bank_balance = float(bank_account.get("Balance", 0))

        # If bank is empty or negative, charge max rate
        if bank_balance <= 0:
            return 0.05  # 5%

        # Snap balance down to nearest $1,000 increment
        stepped_balance = (bank_balance // STEP) * STEP

        # Capacity ratio based on stepped balance
        capacity_ratio = min(stepped_balance / BALANCE_CAP, 1.0)

        # Inverse relationship: more money → lower rate
        # ratio=1.0 ($100k+) → 0.5%, ratio=0.0 ($0) → 5%
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
    cache.set(cache_key, users, ttl=120)  # 2-min freshness for user list
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
    
    # Invalidate transaction caches for both users (also bust shared raw caches)
    cache.invalidate(f"transactions_{sender}", f"transactions_{receiver}",
                     "all_transactions_raw", "all_logs_raw")


def add_lottery_log(username, log_type, amount, description=""):
    """Write a lottery event to the LotteryLogs sheet instead of transactions."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not description:
        description = log_type

    def append():
        lottery_logs_sheet.append_row([username, log_type, amount, now, description])

    retry_with_backoff(append)
    cache.invalidate(f"lottery_logs_{username}", "all_lottery_logs_raw")


def get_all_lottery_logs_raw():
    """Return all rows from the LotteryLogs sheet, shared across callers."""
    cache_key = "all_lottery_logs_raw"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    def fetch():
        return lottery_logs_sheet.get_all_records()
    rows = retry_with_backoff(fetch)
    cache.set(cache_key, rows, ttl=SheetCache.MEDIUM_TTL)
    return rows


def get_user_lottery_logs(username):
    """Return lottery log entries for a user, newest-first."""
    cache_key = f"lottery_logs_{username}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    rows = get_all_lottery_logs_raw()
    logs = []
    for r in rows:
        if r.get("Username") == username:
            try:
                amount = float(r["Amount"])
            except (ValueError, TypeError):
                amount = 0.0
            logs.append({
                "Type":        r.get("Type", ""),
                "Amount":      amount,
                "Date":        r.get("Date", ""),
                "Description": r.get("Description", ""),
            })
    logs.sort(key=lambda x: x["Date"], reverse=True)
    cache.set(cache_key, logs, ttl=SheetCache.SHORT_TTL)
    return logs

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

# ---------- SHARED RAW-DATA HELPERS ----------
# Each of these fetches the *entire* sheet once and caches it at MEDIUM_TTL
# (3 min).  Per-user functions (get_user_transactions, get_user_loans, etc.)
# call these helpers instead of hitting the API themselves, so N concurrent
# users cause exactly ONE network call per TTL window instead of N.

def get_all_transactions_raw():
    """Return all rows from the Transactions sheet, shared across callers."""
    cache_key = "all_transactions_raw"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    def fetch():
        return transactions_sheet.get_all_records()
    rows = retry_with_backoff(fetch)
    cache.set(cache_key, rows, ttl=SheetCache.MEDIUM_TTL)
    return rows

def get_all_logs_raw():
    """Return all rows from the Logs sheet, shared across callers."""
    cache_key = "all_logs_raw"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    def fetch():
        try:
            return logs_sheet.get_all_records()
        except Exception:
            return []
    rows = retry_with_backoff(fetch)
    cache.set(cache_key, rows, ttl=SheetCache.MEDIUM_TTL)
    return rows

def get_all_loans_raw():
    """Return all rows from the Loans sheet, shared across callers."""
    cache_key = "all_loans_raw"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    def fetch():
        hdrs = _LOANS_HEADERS if _LOANS_HEADERS else None
        return loans_sheet.get_all_records(expected_headers=hdrs) if hdrs else loans_sheet.get_all_records()
    rows = retry_with_backoff(fetch)
    cache.set(cache_key, rows, ttl=SheetCache.MEDIUM_TTL)
    return rows


def get_user_transactions(username):
    """Get user transactions with caching — merges Transactions sheet + teacher adjustments from Logs"""
    cache_key = f"transactions_{username}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    # Use shared raw caches — one API call per TTL window regardless of how
    # many users request their transactions simultaneously.
    rows = get_all_transactions_raw()
    formatted = []

    # Pull from Transactions sheet (exclude lottery-related entries — those live in LotteryLogs)
    _lottery_accounts = {"LotteryPrize", "LotteryReserve", "LotteryEmployment"}
    for t in rows:
        sender   = t.get("Sender", "")
        receiver = t.get("Receiver", "")
        # Skip any row involving an internal lottery pool account
        if sender in _lottery_accounts or receiver in _lottery_accounts:
            continue
        if sender == username or receiver == username:
            try:
                amount = float(t["Amount"])
            except (ValueError, TypeError):
                continue
            formatted.append({
                "Sender":   sender,
                "Receiver": receiver,
                "Amount":   amount,
                "Date":     t.get("Date", ""),
                "Comment":  t.get("Comment", "No comment")
            })

    # Also pull teacher adjustments from Logs that reference this student
    log_rows = get_all_logs_raw()
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
    cache.set(cache_key, formatted, ttl=SheetCache.SHORT_TTL)
    return formatted


def transfer_money(sender, receiver, amount, comment):
    """Transfer money between accounts with concurrent request safety.
    
    A per-sender Lock ensures that two simultaneous requests from the same
    user cannot both pass the balance check and both deduct funds (double-spend).
    The cache is invalidated inside the lock so the balance read is always fresh.
    """
    with get_transfer_lock(sender):
        # Invalidate inside the lock so we read the latest balance, not a stale one
        cache.invalidate("all_users", f"user_balance_{sender}", f"user_data_{sender}")
        all_users = get_all_users()

        sender_user = next((u for u in all_users if u["Username"] == sender), None)
        receiver_user = next((u for u in all_users if u["Username"] == receiver), None)

        if not sender_user:
            return "sender_not_found"

        if not receiver_user:
            return "receiver_not_found"

        sender_balance = float(sender_user["Balance"] or 0)
        receiver_balance = float(receiver_user["Balance"] or 0)

        if sender_balance < amount:
            return "insufficient_balance"

        def batch_update():
            sender_cell = users_sheet.find(sender)
            receiver_cell = users_sheet.find(receiver)
            users_sheet.update_cell(sender_cell.row, 3, sender_balance - amount)
            users_sheet.update_cell(receiver_cell.row, 3, receiver_balance + amount)

        retry_with_backoff(batch_update)
        add_transaction(sender, receiver, amount, comment)

        # 1% transaction fee — created as new money added to the bank account
        fee = round(amount * 0.01, 2)
        if fee > 0:
            bank_account = get_bank_account()
            bank_balance = float(bank_account.get("Balance", 0))
            update_bank_balance(bank_balance + fee)
            add_transaction("System", "Bank", fee, f"1% transaction fee on transfer from {sender} to {receiver}")

        cache.invalidate(
            "all_users",
            f"user_balance_{sender}",
            f"user_balance_{receiver}",
            f"user_data_{sender}",
            f"user_data_{receiver}"
        )
        cache.invalidate("bank_account")

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
    frozen_col = _USERS_COLS.get("Frozen", 4)  # column pre-computed at startup
    def update():
        cell = users_sheet.find(username)
        users_sheet.update_cell(cell.row, frozen_col, "Yes")
    
    retry_with_backoff(update)
    cache.invalidate("all_users", f"user_data_{username}", f"frozen_{username}")

def unfreeze_account(username):
    """Unfreeze account and invalidate cache"""
    frozen_col = _USERS_COLS.get("Frozen", 4)  # column pre-computed at startup
    def update():
        cell = users_sheet.find(username)
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
        logs_sheet.update([required_headers], 'A1:E1')
        return
    
    # Fix headers if they exist but are wrong
    if existing_headers[:5] != required_headers:
        logs_sheet.update([required_headers], 'A1:E1')

def ensure_deletions_sheet():
    """Ensure Deletions sheet exists with proper headers"""
    try:
        deletions_sheet = sheet.worksheet("Deletions")
    except:
        deletions_sheet = sheet.add_worksheet("Deletions", rows=1000, cols=5)

    required_headers = ["Username", "Requester", "Reason", "Date", "Status"]
    existing_headers = deletions_sheet.row_values(1)

    if not existing_headers or existing_headers == ['', '', '', '', '']:
        deletions_sheet.update([required_headers], 'A1:E1')

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
    cache.set(cache_key, columns, ttl=SheetCache.LONG_TTL)  # column headers never change at runtime
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
    cache.set(cache_key, stats, ttl=90)  # fed stats: fresh within 90 s
    return stats


# Throttle for recalculate_federal_reserve: minimum interval between full
# recalculations to prevent hammering the API when multiple users reload the
# Federal Reserve page in quick succession.
_last_fed_recalc: float = 0.0
_FED_RECALC_MIN_INTERVAL = 120  # seconds between recalculations


def recalculate_federal_reserve():
    """Recalculate federal reserve stats with proper loan and cash burn tracking.

    Throttled: if called within _FED_RECALC_MIN_INTERVAL seconds of the last
    full recalculation the call is silently skipped — the dashboard will still
    show the cached (recently-fresh) values.
    """
    global _last_fed_recalc
    now = time.time()
    if now - _last_fed_recalc < _FED_RECALC_MIN_INTERVAL:
        return  # too soon — skip to save API quota
    _last_fed_recalc = now

    users = get_all_users_with_balances()
    
    # Calculate total money in all accounts
    total_in_accounts = 0
    student_money = 0
    teacher_money = 0

    for u in users:
        try:
            # Skip lottery pool accounts — they are internal fund buckets,
            # not real money in the economy
            if u.get("AccountType") == "LotteryFund":
                continue

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
                
                # Calculate how much has been repaid (days passed * daily payment)
                if total_weeks > 0:
                    weeks_paid = total_weeks - weeks_remaining
                    weekly_payment = float(loan.get("Weekly", 0))
                    total_loan_repaid += weeks_paid * weekly_payment
    except:
        pass  # If loans sheet doesn't exist or error, default to 0
    
    # CALCULATION LOGIC:
    # Total Money Created = Money in Accounts + Money Loaned Out (still owed) + Loan Repayments
    # This represents ALL money that has ever entered the economy
    
    total_money_created = total_in_accounts + (total_loaned_out - total_loan_repaid)
    
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

    # Update Federal Reserve sheet
    set_fed_value("Total", round(total_money_created, 2))
    set_fed_value("Reserves", reserves)
    set_fed_value("Student", round(student_money, 2))
    set_fed_value("Teacher", round(teacher_money, 2))
    set_fed_value("Liquidity", liquidity)
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
    cache.set(cache_key, end_date, ttl=300)  # project end date changes rarely
    return end_date

def set_project_end_date(date_string):
    """Set project end date (format: YYYY-MM-DD)"""
    set_fed_value("ProjectEndDate", date_string)
    cache.invalidate("project_end_date", "days_until_project_end")

def get_days_until_project_end():
    """Calculate days remaining until project end date"""
    cache_key = "days_until_project_end"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    end_date_str = get_project_end_date()
    if not end_date_str:
        # Default to 63 days (9 weeks) if not set
        return 63
    
    try:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        today = datetime.now()
        days_remaining = max(1, (end_date - today).days)  # minimum 1 day
        cache.set(cache_key, days_remaining)
        return days_remaining
    except:
        return 63

def get_weeks_until_project_end():
    """Calculate weeks remaining until project end date"""
    days = get_days_until_project_end()
    return max(1, round(days / 7))

def process_loan_payments():
    """Process all loan payments that are due (automated daily deductions)"""
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
            day_number = int(loan.get('Weeks', 0)) - weeks_remaining + 1
            add_transaction(requester, "Bank", weekly_payment, f"Loan payment (day {day_number})")
            
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
                    # Set next payment date (1 day from now)
                    next_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
                    loans_sheet.update_cell(idx, col_index["NextPaymentDate"], next_date)
            
            retry_with_backoff(update_loan)
            payments_processed += 1
            
            # Log the payment (include warning if balance went negative)
            if new_balance < 0:
                log_action("System", f"Auto-deducted ${weekly_payment} loan payment from {requester} (balance now NEGATIVE: ${new_balance:.2f}) → Bank: ${new_bank_balance:.2f}", weekly_payment, "Loan Payment")
            else:
                log_action("System", f"Auto-deducted ${weekly_payment} loan payment from {requester} (balance: ${new_balance:.2f}) → Bank: ${new_bank_balance:.2f}", weekly_payment, "Loan Payment")
    
    # Invalidate caches
    cache.invalidate("all_loans", "all_loans_raw")
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
        if user.get("AccountType") != "Personal":
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
                logs_sheet.append_rows(log_rows, value_input_option="USER_ENTERED")
            retry_with_backoff(append_logs)
            cache.invalidate("logs")
        except Exception as e:
            print(f"Error batch-appending weekly-payment logs: {e}")

    return payments_processed

def set_weekly_payment(username, amount):
    """Set weekly payment amount for a Personal account"""
    payment_col = _USERS_COLS.get("WeeklyPayment", 10)  # column pre-computed at startup
    def update():
        cell = users_sheet.find(username)
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
    cache.invalidate("pending_loans", "all_loans", "all_loans_raw", f"user_loans_{sender}")

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
        cache.invalidate("pending_loans", "all_loans", "all_loans_raw", f"user_loans_{requester}")
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
    cache.invalidate("pending_loans", "all_loans", "all_loans_raw", f"user_loans_{requester}")

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
    cache.invalidate("pending_loans", "all_loans", "all_loans_raw", f"user_loans_{requester}")

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
            # get_all_records uses the sheet's own header row by default — no extra row_values(1) needed
            return role_requests_sheet.get_all_records()
        
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
    
    # Use shared raw cache — one API call per MEDIUM_TTL window for all callers
    rows = get_all_loans_raw()
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
        header = cashburns_sheet.row_values(1)
        burn = cashburns_sheet.row_values(burn_row_index)
        return header, burn
    
    header, burn = retry_with_backoff(get_burn)
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
    
    # Use shared raw cache — avoids a per-user API call
    rows = get_all_loans_raw()
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
    
    cache.set(cache_key, user_loans, ttl=SheetCache.SHORT_TTL)
    return user_loans

def get_all_loans():
    """Fetch all loans from sheet with caching and countdown info"""
    cache_key = "all_loans"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    # Use shared raw cache — same loan data re-used by pending/user/all queries
    rows = get_all_loans_raw()
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
    cache.set(cache_key, loans, ttl=120)
    return loans


# ---------- STOCK FLOOR HELPERS ----------

def _parse_nw(raw):
    """Convert a raw cell value like '$1,234.56' or '1234.56' to float, or 0.0."""
    try:
        return float(str(raw).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return 0.0


def get_investments_data():
    """
    Parse the Investments sheet and return structured company + meta data.

    Sheet layout (1-based rows):
      Row 1  → week column headers  (col A = label, col B+ = week names)
      Row 2  → inflation rates      (col A = "Inflation", col B+ = values)
      Rows 3+ → companies           (col A = company name, col B+ = net worth per week)
    """
    global _investment_week_override
    cache_key = "investments_data"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    if investments_sheet is None:
        return {"companies": [], "inflation": "", "currentWeek": "", "allWeeks": []}

    def fetch():
        return investments_sheet.get_all_values()

    raw = retry_with_backoff(fetch)

    if not raw:
        return {"companies": [], "inflation": "", "currentWeek": "", "allWeeks": []}

    # --- row indices (0-based) ---
    weeks_row     = raw[0] if len(raw) > 0 else []   # row 1: week headers
    inflation_row = raw[1] if len(raw) > 1 else []   # row 2: inflation values

    # All week labels in col B onward (index 1+)
    all_weeks = [str(weeks_row[i]) for i in range(1, len(weeks_row)) if str(weeks_row[i]).strip()]

    company_rows = raw[2:] if len(raw) > 2 else []

    # Determine current week column:
    # 1. In-memory override (set_investment_week stores it here instantly — no API lag)
    # 2. CurrentWeek row in the sheet (survives server restarts)
    # 3. Fallback: rightmost labelled week column
    current_col = 1

    def _find_col_by_label(label):
        label_l = label.strip().lower()
        for i in range(1, len(weeks_row)):
            if str(weeks_row[i]).strip().lower() == label_l:
                return i
        return None

    if _investment_week_override:
        col = _find_col_by_label(_investment_week_override)
        if col is not None:
            current_col = col
    else:
        found_cw = False
        for row in raw:
            if row and str(row[0]).strip().lower().replace(" ", "") == "currentweek":
                stored = str(row[1]).strip() if len(row) > 1 else ""
                col = _find_col_by_label(stored)
                if col is not None:
                    current_col = col
                    # Warm the in-memory override so future calls don't re-scan the sheet
                    _investment_week_override = str(weeks_row[col]).strip()
                elif stored:
                    # Legacy fallback: stored value might be a numeric column index
                    try:
                        target_col = int(stored)
                        max_col = max((i for i in range(1, len(weeks_row)) if str(weeks_row[i]).strip()), default=1)
                        current_col = max(1, min(target_col, max_col))
                        _investment_week_override = str(weeks_row[current_col]).strip()
                    except (ValueError, IndexError):
                        pass
                found_cw = True
                break
        if not found_cw:
            # No CurrentWeek row — use rightmost labelled week
            for i in range(1, len(weeks_row)):
                if str(weeks_row[i]).strip():
                    current_col = i

    current_week_label = str(weeks_row[current_col]) if current_col < len(weeks_row) else ""

    # Inflation for current week
    current_inflation = ""
    if current_col < len(inflation_row) and str(inflation_row[current_col]).strip():
        current_inflation = str(inflation_row[current_col]).strip()

    # Parse companies: every row from row 3 onward that has a name in col A
    companies = []
    for row in company_rows:
        name = str(row[0]).strip() if row else ""
        if not name or name.lower() in ("inflation", "currentweek"):
            continue

        # Build history for every labelled week
        history = []
        for i in range(1, len(weeks_row)):
            if not str(weeks_row[i]).strip():
                continue
            val = _parse_nw(row[i]) if i < len(row) else 0.0
            history.append({"week": str(weeks_row[i]), "netWorth": val})

        current_nw = _parse_nw(row[current_col]) if current_col < len(row) else 0.0

        # Previous week value for change %
        prev_nw = 0.0
        if current_col > 1:
            prev_nw = _parse_nw(row[current_col - 1]) if (current_col - 1) < len(row) else 0.0

        change_pct = round((current_nw - prev_nw) / prev_nw * 100, 2) if prev_nw > 0 else 0.0

        companies.append({
            "name":         name,
            "netWorth":     current_nw,
            "prevNetWorth": prev_nw,
            "changePct":    change_pct,
            "history":      history,
        })

    result = {
        "companies":   companies,
        "inflation":   current_inflation,
        "currentWeek": current_week_label,
        "allWeeks":    all_weeks,
    }
    # Only cache if we actually got companies — prevents stale empty results
    if companies:
        cache.set(cache_key, result)
    return result


def get_user_investment_holdings(username):
    """Return a list of {Company, InvestedAmount, NetWorthAtInvestment} for a user."""
    cache_key = f"holdings_{username}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    def fetch():
        return stock_holdings_sheet.get_all_records()

    all_rows = retry_with_backoff(fetch)
    holdings = [r for r in all_rows if r.get("Username") == username]
    for h in holdings:
        try:
            h["InvestedAmount"]       = float(h.get("InvestedAmount", 0) or 0)
            h["NetWorthAtInvestment"] = float(h.get("NetWorthAtInvestment", 0) or 0)
        except (ValueError, TypeError):
            pass
    cache.set(cache_key, holdings)
    return holdings


def get_investment_fund_balance(username):
    """Return the current approved investment fund balance for a user."""
    cache_key = f"inv_fund_{username}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        def fetch():
            return invest_funds_sheet.get_all_records()
        rows = retry_with_backoff(fetch)
        for row in rows:
            if row.get("Username") == username:
                bal = float(row.get("Balance", 0) or 0)
                cache.set(cache_key, bal)
                return bal
    except Exception:
        pass
    cache.set(cache_key, 0.0)
    return 0.0


def update_investment_fund_balance(username, delta):
    """Add delta (positive to credit, negative to deduct) to a user's investment fund."""
    def _update():
        rows = invest_funds_sheet.get_all_values()
        for idx, row in enumerate(rows[1:], start=2):
            if row and row[0] == username:
                current = float(row[1]) if len(row) > 1 and row[1] else 0.0
                new_bal = max(0.0, round(current + delta, 2))
                invest_funds_sheet.update_cell(idx, 2, new_bal)
                return
        # New user — append row
        new_bal = max(0.0, round(delta, 2))
        invest_funds_sheet.append_row([username, new_bal])
    retry_with_backoff(_update)
    cache.invalidate(f"inv_fund_{username}")


def get_pending_fund_requests():
    """Return list of pending investment fund requests for the Federal Reserve."""
    cache_key = "pending_fund_requests"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        def fetch():
            return fund_requests_sheet.get_all_records()
        rows = retry_with_backoff(fetch)
        result = [
            {"row": i + 2, **row}
            for i, row in enumerate(rows)
            if row.get("Status") == "Pending"
        ]
        cache.set(cache_key, result)
        return result
    except Exception:
        return []


def invest_in_company(username, company_name, amount):
    """
    Invest `amount` dollars from `username`'s balance into `company_name`.
    Returns: 'success' | 'company_not_found' | 'insufficient_balance'
             | 'invalid_amount' | 'no_net_worth'
    """
    if amount <= 0:
        return "invalid_amount"

    with get_transfer_lock(username):
        cache.invalidate("all_users", f"user_balance_{username}", "investments_data")

        data = get_investments_data()
        company = next((c for c in data["companies"] if c["name"] == company_name), None)
        if not company:
            return "company_not_found"
        if company["netWorth"] <= 0:
            return "no_net_worth"

        fund_balance = get_investment_fund_balance(username)
        if fund_balance <= 0:
            return "no_fund"
        if amount > fund_balance:
            return "insufficient_balance"

        # Deduct entirely from the investment fund
        update_investment_fund_balance(username, -round(amount, 2))
        add_transaction(username, "INVESTMENTS", amount,
                        f"Invested ${amount:.2f} in {company_name} (from Investment Fund)")

        # Update holdings (weighted average if already invested)
        def update_holdings():
            all_rows = stock_holdings_sheet.get_all_records()
            for idx, row in enumerate(all_rows, start=2):
                if row.get("Username") == username and row.get("Company") == company_name:
                    existing_inv  = float(row.get("InvestedAmount", 0) or 0)
                    existing_nw   = float(row.get("NetWorthAtInvestment", 0) or 0)
                    new_inv       = existing_inv + amount
                    # Weighted average of entry net worth
                    new_entry_nw  = round(
                        (existing_nw * existing_inv + company["netWorth"] * amount) / new_inv, 4
                    )
                    stock_holdings_sheet.update_cell(idx, 3, round(new_inv, 4))
                    stock_holdings_sheet.update_cell(idx, 4, new_entry_nw)
                    return
            stock_holdings_sheet.append_row(
                [username, company_name, round(amount, 4), round(company["netWorth"], 4)]
            )

        retry_with_backoff(update_holdings)
        cache.invalidate(f"holdings_{username}", f"user_balance_{username}", "all_users")
        return "success"


def divest_from_company(username, company_name, withdraw_amount):
    """
    Withdraw `withdraw_amount` dollars (current value) from `username`'s investment
    in `company_name`.  The actual proceeds = withdraw_amount (we pay out at current value).
    Returns: 'success' | 'company_not_found' | 'not_enough_investment'
             | 'invalid_amount' | 'no_net_worth'
    """
    if withdraw_amount <= 0:
        return "invalid_amount"

    with get_transfer_lock(username):
        cache.invalidate("all_users", f"user_balance_{username}", "investments_data",
                         f"holdings_{username}")

        data = get_investments_data()
        company = next((c for c in data["companies"] if c["name"] == company_name), None)
        if not company:
            return "company_not_found"
        if company["netWorth"] <= 0:
            return "no_net_worth"

        holdings = get_user_investment_holdings(username)
        holding  = next((h for h in holdings if h.get("Company") == company_name), None)

        if not holding:
            return "not_enough_investment"

        # Current value of their entire stake
        entry_nw = holding["NetWorthAtInvestment"]
        invested = holding["InvestedAmount"]
        current_value = round(
            invested * (company["netWorth"] / entry_nw) if entry_nw > 0 else 0.0, 2
        )

        if withdraw_amount > current_value + 0.005:  # small float tolerance
            return "not_enough_investment"

        # Fraction of stake being withdrawn
        fraction = withdraw_amount / current_value if current_value > 0 else 1.0
        remaining_invested = round(invested * (1 - fraction), 4)

        def update_holdings():
            all_rows = stock_holdings_sheet.get_all_records()
            for idx, row in enumerate(all_rows, start=2):
                if row.get("Username") == username and row.get("Company") == company_name:
                    if remaining_invested <= 0.001:
                        stock_holdings_sheet.delete_rows(idx)
                    else:
                        stock_holdings_sheet.update_cell(idx, 3, remaining_invested)
                    return

        retry_with_backoff(update_holdings)

        # Credit user balance
        balance = get_user_balance(username)
        update_balance(username, round(balance + withdraw_amount, 2))
        add_transaction("INVESTMENTS", username, withdraw_amount,
                        f"Withdrew ${withdraw_amount:.2f} from {company_name}")

        cache.invalidate(f"holdings_{username}", f"user_balance_{username}", "all_users")
        return "success"


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
    cache.invalidate("all_loans", "all_loans_raw", "pending_loans")
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

    lottery_logs = get_user_lottery_logs(username)

    return render_template("account.html",
                           username=username,
                           balance=balance,
                           transactions=transactions,
                           loans=loans,
                           lottery_logs=lottery_logs,
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
    
    # Get days until project end
    max_days = get_days_until_project_end()
    project_end_date = get_project_end_date()

    if request.method == "POST":
        sender = session["user"]
        reason = request.form["reason"]
        amount = float(request.form["amount"])
        weeks = int(request.form["weeks"])
        
        # Validate amount and days are positive
        if amount <= 0:
            flash("Loan amount must be greater than zero", "error")
            return redirect(url_for("loan"))
        if weeks <= 0 or weeks > 7:
            flash("Loan duration must be between 1 and 7 days", "error")
            return redirect(url_for("loan"))
        
        loan_money(sender, reason, amount, weeks)
        flash("Loan Application Received!")
        return redirect(url_for("account"))

    return render_template("loan.html", username=session["user"], irate=interest_rate, 
                         max_days=max_days, project_end_date=project_end_date)


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
    """Log actions to Logs sheet and invalidate cache.
    Uses the module-level logs_sheet handle to avoid a runtime worksheet() API lookup.
    """
    def append():
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
            cashburn = cashburns_sheet.row_values(row_index)
            header = cashburns_sheet.row_values(1)
            return cashburn, header
        
        cashburn, header = retry_with_backoff(get_burn)
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

# ---------- ADS HELPERS ----------
def get_all_ads():
    """Return every ad row (active and inactive) — used by the Ad Manager."""
    cache_key = "ads_all"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        ads = ads_sheet.get_all_records()
        cache.set(cache_key, ads)
        return ads
    except Exception as e:
        print(f"Error fetching ads: {e}")
        return []


def get_ads(page=None):
    """Return active ads, optionally filtered by page name.
    Sorted by Priority descending (highest first = weighted rotation)."""
    ads = get_all_ads()

    # Filter by page if specified
    if page:
        filtered = []
        for ad in ads:
            pages_str = str(ad.get("Pages", "")).lower()
            if "all" in pages_str or page.lower() in pages_str:
                filtered.append(ad)
        ads = filtered

    # Only active ads
    ads = [a for a in ads if str(a.get("Active", "")).upper() == "TRUE"]

    # Sort by priority descending
    try:
        ads.sort(key=lambda a: int(a.get("Priority", 1) or 1), reverse=True)
    except Exception:
        pass

    return ads


@app.route("/federalreserve")
@role_required("Banker")
def federal_reserve():
    # Trigger recalculation in the background so the page loads immediately
    # from cache while stats are updated asynchronously.
    Thread(target=recalculate_federal_reserve, daemon=True).start()
    data = get_federal_reserve_stats()
    users = get_all_users_with_balances()

    # Sanitize user balances for display
    sanitized_users = []
    for u in users:
        user_copy = u.copy()
        try:
            balance_val = u.get("Balance", 0)
            user_copy["Balance"] = 0.0 if (balance_val == "" or balance_val is None) else float(balance_val)
        except (ValueError, TypeError):
            user_copy["Balance"] = 0.0
        sanitized_users.append(user_copy)

    pending_loans = get_pending_loans()
    all_loans = get_all_loans()
    pending_deletions = get_pending_deletions()
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
    all_ads = get_all_ads()

    # Investment week info for the settings panel
    inv_data = get_investments_data()
    inv_all_weeks = inv_data["allWeeks"]
    inv_current_week = inv_data["currentWeek"]
    pending_fund_requests = get_pending_fund_requests()

    return render_template("federalreserve.html", 
                         data=data, 
                         users=sanitized_users,
                         pending_loans=pending_loans,
                         all_loans=all_loans,
                         pending_deletions=pending_deletions,
                         pending_teacher_requests=pending_teacher_requests,
                         pending_role_changes=pending_role_changes,
                         logs=logs,
                         bank_account=bank_account,
                         project_end_date=project_end_date,
                         weeks_remaining=weeks_remaining,
                         personal_to_company_rate=personal_to_company_rate,
                         teacher_pin=teacher_pin,
                         all_ads=all_ads,
                         inv_all_weeks=inv_all_weeks,
                         inv_current_week=inv_current_week,
                         pending_fund_requests=pending_fund_requests)

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

@app.route("/backfill_transaction_fees", methods=["GET", "POST"], strict_slashes=False)
@role_required("Banker")
def backfill_transaction_fees():
    """Retroactively credit the bank 1% for all past transactions that haven't been fee'd yet."""
    if request.method == "GET":
        return redirect(url_for("federal_reserve"))
    try:
        all_txns = get_all_transactions_raw()

        # Sum amounts for all real transfers — skip existing fee entries to avoid double-counting
        total_fee = 0.0
        skipped = 0
        counted = 0
        for txn in all_txns:
            sender   = str(txn.get("Sender", ""))
            receiver = str(txn.get("Receiver", ""))
            comment  = str(txn.get("Comment", ""))
            # Skip entries that are already fee transactions
            if sender == "System" and receiver == "Bank" and "1% transaction fee" in comment:
                skipped += 1
                continue
            # Skip bank-internal transfers (Bank→user, user→Bank Transfer)
            if sender == "Bank" or receiver == "Bank" or sender == "System":
                skipped += 1
                continue
            try:
                amt = float(txn.get("Amount", 0))
            except (ValueError, TypeError):
                continue
            if amt > 0:
                total_fee += round(amt * 0.01, 2)
                counted += 1

        total_fee = round(total_fee, 2)

        if total_fee <= 0:
            flash("No eligible past transactions found to backfill.", "info")
            return redirect(url_for("federal_reserve"))

        # Add the total fee to the bank balance as created money
        bank_account = get_bank_account()
        bank_balance = float(bank_account.get("Balance", 0))
        update_bank_balance(bank_balance + total_fee)
        add_transaction("System", "Bank", total_fee,
                        f"Historical 1% fee backfill — {counted} past transactions")

        log_action(session["user"],
                   f"Backfilled historical 1% fees: ${total_fee:.2f} from {counted} transactions",
                   total_fee, "Approved")

        cache.invalidate("bank_account", "all_transactions_raw", "fed_stats")
        cache.invalidate_pattern("bank")

        flash(f"Successfully added ${total_fee:.2f} to the bank (1% of {counted} past transactions).", "success")
    except Exception as e:
        flash(f"Error during backfill: {e}", "error")

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
    loans = [l for l in get_all_loans() if str(l.get("Status", "")).strip().lower() != "declined"]
    
    # Add display names to all users based on their email
    for user in users:
        user["DisplayName"] = get_display_name_from_email(user.get("Email", ""))
    
    # Separate users into Personal and Company accounts
    personal_students = [u for u in users if u.get("Role") in ("Student", "Banker") and u.get("AccountType") == "Personal"]
    company_students = [u for u in users if u.get("Role") in ("Student", "Banker") and u.get("AccountType") == "Company"]
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
    comment = request.form.get("comment", "").strip()
    
    # Validate amount is positive
    if amount <= 0:
        flash("Amount must be greater than zero", "error")
        return redirect(url_for("teacher_tools"))
    
    current_balance = get_user_balance(username)
    
    if action == "add":
        new_balance = current_balance + amount
        log_msg = f"Added ${amount} to {username}" + (f" — {comment}" if comment else "")
        log_action(session["user"], log_msg, amount, "Approved")
        add_transaction(session["user"], username, amount, comment or "Teacher adjustment")
    else:
        new_balance = current_balance - amount
        log_msg = f"Subtracted ${amount} from {username}" + (f" — {comment}" if comment else "")
        log_action(session["user"], log_msg, amount, "Approved")
        add_transaction(f"DEDUCT:{session['user']}", username, amount, comment or "Teacher deduction")
    
    update_balance(username, new_balance)
    cache.invalidate(f"transactions_{username}")
    flash(f"Balance adjusted for {username}")
    return redirect(url_for("teacher_tools"))

@app.route("/set_money", methods=["POST"])
@role_required("Teacher", "Banker")
def set_money():
    username = request.form["username"]
    amount = float(request.form["amount"])
    comment = request.form.get("comment", "").strip()
    
    # Validate amount is not negative (can be zero to clear balance)
    if amount < 0:
        flash("Balance cannot be negative", "error")
        return redirect(url_for("teacher_tools"))
    
    update_balance(username, amount)
    log_msg = f"Set balance to ${amount} for {username}" + (f" — {comment}" if comment else "")
    log_action(session["user"], log_msg, amount, "Approved")
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


# ---------- AD MANAGEMENT ROUTES ----------

@app.route("/api/ads")
def api_ads():
    """Public endpoint — returns ads for a given page, filtered by day schedule."""
    page = request.args.get("page", "")
    today = datetime.now().strftime("%a")  # Mon, Tue, Wed, Thu, Fri, Sat, Sun
    ads = get_ads(page)

    # Filter by optional day schedule
    result = []
    for ad in ads:
        schedule = str(ad.get("Schedule", "")).strip()
        if not schedule:
            result.append(ad)
        elif today in [s.strip() for s in schedule.split(",")]:
            result.append(ad)

    return jsonify(result)


@app.route("/ads/add", methods=["POST"])
@role_required("Banker")
def ads_add():
    """Create a new ad row in the Ads sheet."""
    data = request.get_json()
    all_ads = get_all_ads()
    next_id = max([int(a.get("ID", 0) or 0) for a in all_ads], default=0) + 1

    row = [
        next_id,
        data.get("title", ""),
        data.get("image_url", ""),
        data.get("link_url", ""),
        data.get("pages", "all"),
        data.get("schedule", ""),
        int(data.get("priority", 1)),
        int(data.get("interval", 5)),
        "TRUE" if data.get("active", True) else "FALSE"
    ]
    ads_sheet.append_row(row)
    cache.invalidate("ads_all")
    return jsonify({"success": True, "id": next_id})


@app.route("/ads/update/<int:ad_id>", methods=["POST"])
@role_required("Banker")
def ads_update(ad_id):
    """Update an existing ad row by ID."""
    data = request.get_json()
    all_rows = ads_sheet.get_all_values()

    for idx, row in enumerate(all_rows[1:], start=2):
        if len(row) > 0 and str(row[0]) == str(ad_id):
            new_row = [
                ad_id,
                data.get("title", row[1] if len(row) > 1 else ""),
                data.get("image_url", row[2] if len(row) > 2 else ""),
                data.get("link_url", row[3] if len(row) > 3 else ""),
                data.get("pages", row[4] if len(row) > 4 else "all"),
                data.get("schedule", row[5] if len(row) > 5 else ""),
                int(data.get("priority", row[6] if len(row) > 6 else 1)),
                int(data.get("interval", row[7] if len(row) > 7 else 5)),
                "TRUE" if data.get("active", True) else "FALSE"
            ]
            ads_sheet.update([new_row], f"A{idx}:I{idx}")
            cache.invalidate("ads_all")
            return jsonify({"success": True})

    return jsonify({"success": False, "error": "Ad not found"}), 404


@app.route("/ads/toggle/<int:ad_id>", methods=["POST"])
@role_required("Banker")
def ads_toggle(ad_id):
    """Flip the Active flag of an ad."""
    all_rows = ads_sheet.get_all_values()
    for idx, row in enumerate(all_rows[1:], start=2):
        if len(row) > 0 and str(row[0]) == str(ad_id):
            current = str(row[8]).upper() if len(row) > 8 else "FALSE"
            new_val = "FALSE" if current == "TRUE" else "TRUE"
            ads_sheet.update_cell(idx, 9, new_val)
            cache.invalidate("ads_all")
            return jsonify({"success": True, "active": new_val == "TRUE"})
    return jsonify({"success": False, "error": "Ad not found"}), 404


@app.route("/ads/delete/<int:ad_id>", methods=["POST"])
@role_required("Banker")
def ads_delete(ad_id):
    """Delete an ad row by ID."""
    all_rows = ads_sheet.get_all_values()
    for idx, row in enumerate(all_rows[1:], start=2):
        if len(row) > 0 and str(row[0]) == str(ad_id):
            ads_sheet.delete_rows(idx)
            cache.invalidate("ads_all")
            return jsonify({"success": True})
    return jsonify({"success": False, "error": "Ad not found"}), 404


# ---------- LOTTERY HELPERS ----------

def ensure_lottery_pools():
    """Ensure lottery pool accounts exist in the users sheet."""
    pool_accounts = [
        ("LotteryPrize",      "System", "LotteryFund"),
        ("LotteryReserve",    "System", "LotteryFund"),
        ("LotteryEmployment", "System", "LotteryFund"),
    ]
    users = get_all_users()
    existing = {u["Username"] for u in users}
    created = False
    for uname, role, actype in pool_accounts:
        if uname not in existing:
            def _create(un=uname, r=role, at=actype):
                users_sheet.append_row([un, "LotteryInternal", "0",
                                        "No", r, "", at,
                                        "0000-0000-0000-0000", "0000", "0"])
            retry_with_backoff(_create)
            created = True
    if created:
        cache.invalidate("all_users")


def get_lottery_pool_balances():
    """Return {prize, reserve, employment} pool balances."""
    users = get_all_users()
    mapping = {
        "LotteryPrize":      "prize",
        "LotteryReserve":    "reserve",
        "LotteryEmployment": "employment",
    }
    result = {"prize": 0.0, "reserve": 0.0, "employment": 0.0}
    for u in users:
        key = mapping.get(u["Username"])
        if key:
            try:
                result[key] = float(u.get("Balance") or 0)
            except (ValueError, TypeError):
                result[key] = 0.0
    return result


def get_user_lottery_tickets(username):
    """Return all lottery tickets for a user."""
    cache_key = f"lottery_tickets_{username}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    def fetch():
        return lottery_sheet.get_all_records()
    rows = retry_with_backoff(fetch)
    tickets = [r for r in rows if r.get("Username") == username]
    cache.set(cache_key, tickets)
    return tickets


def invalidate_lottery_caches(username=None):
    cache.invalidate("all_users", "bank_account", "all_lottery_logs_raw")
    if username:
        cache.invalidate(
            f"lottery_tickets_{username}",
            f"lottery_logs_{username}",
            f"user_balance_{username}",
            f"user_data_{username}",
        )


def get_lottery_winning():
    """Return the latest lottery winning-numbers dict, or None if never drawn.
    Stored as extra columns on the Reserve sheet row 2:
    LotteryNum1 … LotteryNum4, LotteryVex, LotteryDrawDate, LotteryDrawName.
    """
    cache_key = "lottery_winning"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached if cached.get("drawn") else None
    try:
        def fetch():
            header = fed_sheet.row_values(1)
            if "LotteryNum1" not in header:
                return None
            values = fed_sheet.row_values(2)
            col = {name: idx for idx, name in enumerate(header)}
            def gv(k):
                i = col.get(k)
                return values[i] if i is not None and i < len(values) else ""
            n1 = gv("LotteryNum1")
            if not n1:
                return None
            return {
                "drawn": True,
                "numbers": [gv("LotteryNum1"), gv("LotteryNum2"),
                            gv("LotteryNum3"), gv("LotteryNum4")],
                "vex":       gv("LotteryVex"),
                "draw_date": gv("LotteryDrawDate"),
                "draw_name": gv("LotteryDrawName"),
            }
        result = retry_with_backoff(fetch)
    except Exception:
        result = None
    cache.set(cache_key, result or {"drawn": False}, ttl=120)
    return result


def set_lottery_winning(numbers, vex, draw_name=""):
    """Persist winning numbers to the Reserve sheet (append columns if needed)."""
    needed = ["LotteryNum1", "LotteryNum2", "LotteryNum3", "LotteryNum4",
              "LotteryVex", "LotteryDrawDate", "LotteryDrawName"]

    def _ensure_cols():
        header = fed_sheet.row_values(1)
        for col_name in needed:
            if col_name not in header:
                next_col = len(header) + 1
                fed_sheet.update_cell(1, next_col, col_name)
                header.append(col_name)

    retry_with_backoff(_ensure_cols)
    cache.invalidate("fed_columns")

    now_str = datetime.now().strftime("%Y-%m-%d")
    updates = {
        "LotteryNum1":    str(numbers[0]),
        "LotteryNum2":    str(numbers[1]),
        "LotteryNum3":    str(numbers[2]),
        "LotteryNum4":    str(numbers[3]),
        "LotteryVex":     str(vex),
        "LotteryDrawDate": now_str,
        "LotteryDrawName": draw_name or now_str,
    }
    for k, v in updates.items():
        set_fed_value(k, v)

    cache.invalidate("lottery_winning", "fed_stats")


# ---------- LOTTERY ROUTES ----------

@app.route("/lottery")
def lottery():
    if "user" not in session:
        return redirect(url_for("login"))
    ensure_lottery_pools()
    username  = session["user"]
    pools        = get_lottery_pool_balances()
    user_tickets = get_user_lottery_tickets(username)
    user_balance = get_user_balance(username)
    winning      = get_lottery_winning()
    is_banker    = session.get("role") in ("Banker", "Teacher")
    return render_template(
        "lottery.html",
        pools=pools,
        user_tickets=user_tickets,
        user_balance=user_balance,
        winning=winning,
        is_banker=is_banker,
    )


@app.route("/lottery/buy", methods=["POST"])
def lottery_buy():
    import random as _random
    if "user" not in session:
        return redirect(url_for("login"))

    username = session["user"]

    if is_frozen(username):
        flash("Your account is frozen. Contact your teacher.", "error")
        return redirect(url_for("lottery"))

    # ── Quantity ──────────────────────────────────────────────
    try:
        quantity = int(request.form.get("quantity", 0))
        if quantity <= 0:
            raise ValueError("non-positive")
    except (ValueError, TypeError):
        flash("Invalid ticket quantity.", "error")
        return redirect(url_for("lottery"))

    if quantity > 1000:
        flash("Maximum 1,000 tickets per purchase.", "error")
        return redirect(url_for("lottery"))

    total_cost = round(quantity * 2.0, 2)   # $2 per ticket

    # ── Balance check ─────────────────────────────────────────
    user_balance = get_user_balance(username)
    if user_balance < total_cost:
        flash(
            f"Insufficient balance. Need ${total_cost:.2f}, have ${user_balance:.2f}.",
            "error",
        )
        return redirect(url_for("lottery"))

    # ── Number selection ──────────────────────────────────────
    import json as _json
    pick_mode    = request.form.get("pick_mode", "auto")
    tickets_json = request.form.get("tickets_json", "").strip()

    if pick_mode == "manual":
        # Per-ticket JSON: [[n1,n2,n3,n4,vex], ...]
        if tickets_json:
            try:
                parsed = _json.loads(tickets_json)
                if not isinstance(parsed, list) or len(parsed) != quantity:
                    raise ValueError("length mismatch")
                tickets_data = []
                for entry in parsed:
                    if len(entry) != 5:
                        raise ValueError("expected 5 values per ticket")
                    nums = [int(entry[j]) for j in range(4)]
                    vex  = int(entry[4])
                    for n in nums:
                        if not (1 <= n <= 8):
                            raise ValueError("main number out of range")
                    if len(set(nums)) != 4:
                        raise ValueError("numbers must be unique")
                    if not (1 <= vex <= 10):
                        raise ValueError("vex out of range")
                    tickets_data.append((sorted(nums), vex))
            except (ValueError, TypeError, KeyError, IndexError) as exc:
                flash(f"Invalid ticket numbers: {exc}. Main: 4 unique from 1-8, Vex Ball: 1-10.", "error")
                return redirect(url_for("lottery"))
        else:
            flash("Please use the Pick Numbers form to enter your numbers.", "error")
            return redirect(url_for("lottery"))
    else:
        tickets_data = [
            (sorted(_random.sample(range(1, 9), 4)), _random.randint(1, 10))
            for _ in range(quantity)
        ]

    # ── Deduct from user ──────────────────────────────────────
    new_user_bal = round(user_balance - total_cost, 2)
    update_balance(username, new_user_bal)

    # ── Distribute to pools (70 / 20 / 10) ───────────────────
    prize_cut      = round(total_cost * 0.50, 2)
    employment_cut = round(total_cost * 0.20, 2)
    reserve_cut    = round(total_cost - prize_cut - employment_cut, 2)  # absorbs rounding

    pools = get_lottery_pool_balances()
    update_balance("LotteryPrize",      round(pools["prize"]      + prize_cut,      2))
    update_balance("LotteryReserve",    round(pools["reserve"]    + reserve_cut,    2))
    update_balance("LotteryEmployment", round(pools["employment"] + employment_cut, 2))

    # ── Save tickets to sheet ─────────────────────────────────
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _get_next_id():
        return len(lottery_sheet.get_all_values())   # header + rows = next sequential id

    start_id = retry_with_backoff(_get_next_id)

    new_rows = [
        [f"TKT-{start_id + i:06d}", username,
         ",".join(str(n) for n in nums),   # e.g. "2,4,6,8"
         vex, now_str, "Active"]
        for i, (nums, vex) in enumerate(tickets_data)
    ]

    def _append():
        lottery_sheet.append_rows(new_rows, value_input_option="RAW")

    retry_with_backoff(_append)

    # ── Log to LotteryLogs (not general transactions) ─────────
    add_lottery_log(
        username, "Purchase", total_cost,
        f"Bought {quantity} ticket{'s' if quantity != 1 else ''} — ${total_cost:.2f} spent",
    )

    invalidate_lottery_caches(username)

    flash(
        f"🎟️ Purchased {quantity} ticket{'s' if quantity != 1 else ''}! "
        f"First ticket: [{', '.join(str(n) for n in tickets_data[0][0])}] + Vex {tickets_data[0][1]}. Good luck!",
        "success",
    )
    return redirect(url_for("lottery"))


@app.route("/lottery/draw", methods=["POST"])
def lottery_draw():
    """Banker/Teacher only: set winning numbers and optionally run the drawing."""
    if "user" not in session:
        return redirect(url_for("login"))
    if session.get("role") not in ("Banker", "Teacher"):
        flash("You don't have permission to do that.", "error")
        return redirect(url_for("lottery"))

    try:
        nums = [
            int(request.form.get("n1", 0)),
            int(request.form.get("n2", 0)),
            int(request.form.get("n3", 0)),
            int(request.form.get("n4", 0)),
        ]
        vex       = int(request.form.get("vex", 0))
        draw_name = request.form.get("draw_name", "").strip()
        run       = request.form.get("run_drawing") == "1"
    except (ValueError, TypeError):
        flash("Invalid number input.", "error")
        return redirect(url_for("lottery"))

    # Validate
    for n in nums:
        if not (1 <= n <= 8):
            flash("All 4 main numbers must be between 1 and 8.", "error")
            return redirect(url_for("lottery"))
    if len(set(nums)) != 4:
        flash("All 4 numbers must be unique.", "error")
        return redirect(url_for("lottery"))
    if not (1 <= vex <= 10):
        flash("Vex Ball must be between 1 and 10.", "error")
        return redirect(url_for("lottery"))

    set_lottery_winning(nums, vex, draw_name)

    if run:
        winning_set = set(nums)
        winners_jackpot = []
        winners_match   = []
        winners_vex     = []

        def _get_tickets():
            return lottery_sheet.get_all_records()

        tickets = retry_with_backoff(_get_tickets)
        pools   = get_lottery_pool_balances()

        for t in tickets:
            if t.get("Drawing") != "Active":
                continue
            numstr = str(t.get("Number1", ""))
            try:
                ticket_nums = set(int(x.strip()) for x in numstr.split(","))
            except (ValueError, TypeError):
                continue
            try:
                ticket_vex = int(t.get("VexBall", 0))
            except (ValueError, TypeError):
                ticket_vex = 0

            match_nums = ticket_nums == winning_set
            match_vex  = ticket_vex == vex

            if match_nums and match_vex:
                winners_jackpot.append(t["Username"])
            elif match_nums:
                winners_match.append(t["Username"])
            elif match_vex:
                winners_vex.append(t["Username"])

        # ── Award jackpot (split if multiple winners) ────────────
        jackpot_amount = pools["prize"]
        if winners_jackpot:
            unique_jackpot = list(set(winners_jackpot))
            share = round(jackpot_amount / len(winners_jackpot), 2)
            for uname in unique_jackpot:
                count = winners_jackpot.count(uname)
                award = round(share * count, 2)
                bal = get_user_balance(uname)
                update_balance(uname, round(bal + award, 2))
                add_lottery_log(uname, "Jackpot Win", award,
                                f"Jackpot winner! Drawing: {draw_name or 'Draw'}")
            update_balance("LotteryPrize", 0.0)

        # ── Award $50 for 4-number match (no Vex) ───────────────
        for uname in winners_match:
            bal = get_user_balance(uname)
            update_balance(uname, round(bal + 50.0, 2))
            add_lottery_log(uname, "4-Number Win", 50.0,
                            f"Matched all 4 numbers! Drawing: {draw_name or 'Draw'}")
            pools = get_lottery_pool_balances()
            update_balance("LotteryPrize", max(0.0, round(pools["prize"] - 50.0, 2)))

        # ── Award $2 refund for Vex Ball only ────────────────────
        for uname in winners_vex:
            bal = get_user_balance(uname)
            update_balance(uname, round(bal + 2.0, 2))
            add_lottery_log(uname, "Vex Ball Win", 2.0,
                            f"Vex Ball match refund. Drawing: {draw_name or 'Draw'}")
            pools = get_lottery_pool_balances()
            update_balance("LotteryPrize", max(0.0, round(pools["prize"] - 2.0, 2)))

        # ── Mark all Active tickets as drawn ─────────────────────
        label = draw_name or datetime.now().strftime("%Y-%m-%d")

        def _mark_done():
            all_vals = lottery_sheet.get_all_values()
            if not all_vals:
                return
            header = all_vals[0]
            if "Drawing" not in header:
                return
            drawing_col = header.index("Drawing")  # 0-based
            batch = []
            for row_idx, row in enumerate(all_vals[1:], start=2):
                if len(row) > drawing_col and row[drawing_col] == "Active":
                    col_letter = chr(65 + drawing_col)
                    batch.append({
                        "range":  f"{col_letter}{row_idx}",
                        "values": [[label]],
                    })
            if batch:
                lottery_sheet.batch_update(batch)

        retry_with_backoff(_mark_done)

        # Bust all caches
        cache.invalidate("all_users")
        for u in get_all_users():
            cache.invalidate(
                f"lottery_tickets_{u['Username']}",
                f"user_balance_{u['Username']}",
                f"user_data_{u['Username']}",
            )

        flash(
            f"✅ Drawing complete! "
            f"Jackpot winners: {len(set(winners_jackpot))} | "
            f"4-number matches: {len(set(winners_match))} | "
            f"Vex-only: {len(set(winners_vex))}",
            "success",
        )
    else:
        flash(
            f"Winning numbers saved: "
            f"{nums[0]}-{nums[1]}-{nums[2]}-{nums[3]} + Vex {vex}",
            "success",
        )

    invalidate_lottery_caches()
    return redirect(url_for("lottery"))


@app.route("/api/lottery/status")
def api_lottery_status():
    if "user" not in session:
        return jsonify({"error": "Not logged in"}), 401
    ensure_lottery_pools()
    pools    = get_lottery_pool_balances()
    username = session["user"]
    tickets  = get_user_lottery_tickets(username)
    return jsonify({
        "pools":            pools,
        "jackpot":          pools["prize"],
        "user_ticket_count": len(tickets),
    })


# ---------- STOCK FLOOR ROUTES ----------

@app.route("/stocks")
def stocks():
    """Investment Floor — view companies from Investments sheet and the user's portfolio."""
    if "user" not in session:
        return redirect(url_for("login"))
    username = session["user"]
    inv_data  = get_investments_data()
    # If cache returned empty companies, bust and re-fetch once
    if not inv_data["companies"]:
        cache.invalidate("investments_data")
        inv_data = get_investments_data()
    holdings     = get_user_investment_holdings(username)
    balance      = get_user_balance(username)
    fund_balance = get_investment_fund_balance(username)

    # Map company name → holding for template lookup
    holdings_map = {h["Company"]: h for h in holdings}

    # Compute current portfolio value (each stake grows with net worth)
    portfolio_value = 0.0
    for h in holdings:
        company = next((c for c in inv_data["companies"] if c["name"] == h["Company"]), None)
        if company and h["NetWorthAtInvestment"] > 0:
            portfolio_value += h["InvestedAmount"] * (company["netWorth"] / h["NetWorthAtInvestment"])

    return render_template(
        "stocks.html",
        companies=inv_data["companies"],
        inflation=inv_data["inflation"],
        current_week=inv_data["currentWeek"],
        all_weeks=inv_data["allWeeks"],
        holdings_map=holdings_map,
        portfolio_value=round(portfolio_value, 2),
        balance=balance,
        fund_balance=fund_balance,
    )


@app.route("/stocks/buy", methods=["POST"])
def stocks_buy():
    """Handle an invest form submission."""
    if "user" not in session:
        return redirect(url_for("login"))
    username     = session["user"]
    company_name = request.form.get("company", "").strip()
    try:
        amount = round(float(request.form.get("amount", 0)), 2)
    except ValueError:
        flash("Invalid amount.", "error")
        return redirect(url_for("stocks"))

    result = invest_in_company(username, company_name, amount)
    if result == "success":
        flash(f"Successfully invested ${amount:.2f} in {company_name}!", "success")
    elif result == "no_fund":
        flash("You don't have an investment fund. Request funds from your banker first.", "error")
    elif result == "insufficient_balance":
        flash("Amount exceeds your available investment fund balance.", "error")
    elif result == "company_not_found":
        flash("Company not found.", "error")
    elif result == "no_net_worth":
        flash("This company has no net worth data yet.", "error")
    elif result == "invalid_amount":
        flash("Amount must be greater than zero.", "error")
    else:
        flash("Transaction failed. Please try again.", "error")

    return redirect(url_for("stocks"))


@app.route("/stocks/sell", methods=["POST"])
def stocks_sell():
    """Handle a withdraw/divest form submission."""
    if "user" not in session:
        return redirect(url_for("login"))
    username     = session["user"]
    company_name = request.form.get("company", "").strip()
    try:
        amount = round(float(request.form.get("amount", 0)), 2)
    except ValueError:
        flash("Invalid amount.", "error")
        return redirect(url_for("stocks"))

    result = divest_from_company(username, company_name, amount)
    if result == "success":
        flash(f"Successfully withdrew ${amount:.2f} from {company_name}!", "success")
    elif result == "not_enough_investment":
        flash("You don't have enough invested to withdraw that amount.", "error")
    elif result == "company_not_found":
        flash("Company not found.", "error")
    elif result == "no_net_worth":
        flash("This company has no net worth data yet.", "error")
    elif result == "invalid_amount":
        flash("Amount must be greater than zero.", "error")
    else:
        flash("Transaction failed. Please try again.", "error")

    return redirect(url_for("stocks"))


@app.route("/stocks/request_fund", methods=["POST"])
def stocks_request_fund():
    """Student submits an investment fund request for banker approval.
    No money moves until the banker approves — then it is added to their
    investment fund balance which stacks across multiple approvals."""
    if "user" not in session:
        return redirect(url_for("login"))
    username = session["user"]
    try:
        amount = round(float(request.form.get("amount", 0)), 2)
    except ValueError:
        flash("Invalid amount.", "error")
        return redirect(url_for("stocks"))
    if amount < 1 or amount > 1000:
        flash("Fund request must be between $1 and $1000.", "error")
        return redirect(url_for("stocks"))
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    def append():
        fund_requests_sheet.append_row([username, amount, "Pending", now])
    retry_with_backoff(append)
    cache.invalidate("pending_fund_requests")
    flash(f"Investment fund request of ${amount:.2f} sent to your banker 🪙 You’ll be able to invest once it’s approved!", "info")
    return redirect(url_for("stocks"))


@app.route("/approve_fund_request/<int:row_index>", methods=["POST"])
@role_required("Banker")
def approve_fund_request(row_index):
    """Approve an investment fund request — add the amount to the student’s fund balance."""
    try:
        def get_and_approve():
            row = fund_requests_sheet.row_values(row_index)
            uname  = row[0] if len(row) > 0 else ""
            amt    = float(row[1]) if len(row) > 1 and row[1] else 0.0
            fund_requests_sheet.update_cell(row_index, 3, "Approved")
            return uname, amt
        username, amount = retry_with_backoff(get_and_approve)
        if username and amount > 0:
            update_investment_fund_balance(username, amount)
            new_total = get_investment_fund_balance(username)
            log_action(session["user"], f"Approved investment fund of ${amount:.2f} for {username} (total fund: ${new_total:.2f})", amount, "Approved")
            flash(f"Approved ${amount:.2f} investment fund for {username}. Their fund total is now ${new_total:.2f}.", "success")
        cache.invalidate("pending_fund_requests")
    except Exception as e:
        flash(f"Error approving fund request: {str(e)}", "error")
    return redirect(url_for("federal_reserve"))


@app.route("/deny_fund_request/<int:row_index>", methods=["POST"])
@role_required("Banker")
def deny_fund_request(row_index):
    """Deny an investment fund request."""
    try:
        def get_and_deny():
            row = fund_requests_sheet.row_values(row_index)
            uname = row[0] if len(row) > 0 else "Unknown"
            fund_requests_sheet.update_cell(row_index, 3, "Denied")
            return uname
        username = retry_with_backoff(get_and_deny)
        cache.invalidate("pending_fund_requests")
        flash(f"Denied investment fund request for {username}.", "info")
    except Exception as e:
        flash(f"Error: {str(e)}", "error")
    return redirect(url_for("federal_reserve"))


@app.route("/api/stocks")
def api_stocks():
    """JSON endpoint — returns live company data + user holdings."""
    if "user" not in session:
        return jsonify({"error": "Not logged in"}), 401
    username = session["user"]
    inv_data = get_investments_data()
    holdings = get_user_investment_holdings(username)
    holdings_map = {h["Company"]: h for h in holdings}
    result = []
    for c in inv_data["companies"]:
        h = holdings_map.get(c["name"], {})
        inv    = h.get("InvestedAmount", 0)
        nw_in  = h.get("NetWorthAtInvestment", 0)
        cur_val = round(inv * (c["netWorth"] / nw_in), 2) if nw_in > 0 else 0.0
        result.append({
            "name":          c["name"],
            "netWorth":      c["netWorth"],
            "changePct":     c["changePct"],
            "history":       c["history"],
            "userInvested":  inv,
            "currentValue":  cur_val,
            "pl":            round(cur_val - inv, 2),
        })
    return jsonify({
        "companies":   result,
        "inflation":   inv_data["inflation"],
        "currentWeek": inv_data["currentWeek"],
    })


@app.route("/set_investment_week", methods=["POST"])
@role_required("Banker")
def set_investment_week():
    """Set the active Investment Floor week. Stored in memory immediately
    (instant effect) and persisted to the sheet for server-restart survival."""
    global _investment_week_override
    week_label = request.form.get("week_label", "").strip()
    if not week_label:
        flash("Invalid week selection.", "error")
        return redirect(url_for("federal_reserve"))

    # Set in-memory first — takes effect on the very next request, no API lag
    _investment_week_override = week_label
    cache.invalidate("investments_data")

    # Also persist to sheet so the setting survives a server restart
    if investments_sheet is not None:
        def write_week():
            raw = investments_sheet.get_all_values()
            for i, row in enumerate(raw):
                if row and str(row[0]).strip().lower().replace(" ", "") == "currentweek":
                    investments_sheet.update_cell(i + 1, 2, week_label)
                    return
            investments_sheet.append_row(["CurrentWeek", week_label])
        try:
            retry_with_backoff(write_week)
        except Exception:
            pass  # in-memory override is already set; sheet write failure is non-fatal

    flash(f"Investment current week set to {week_label}.", "success")
    return redirect(url_for("federal_reserve"))


@app.route("/debug/investments")
@role_required("Banker")
def debug_investments():
    """Temporary debug route — shows raw Investments sheet rows and parsed output."""
    if investments_sheet is None:
        return jsonify({"error": "investments_sheet is None — tab not found"})
    try:
        raw = investments_sheet.get_all_values()
    except Exception as e:
        return jsonify({"error": str(e)})

    cache.invalidate("investments_data")  # force fresh parse
    parsed = get_investments_data()

    return jsonify({
        "raw_row_count": len(raw),
        "row1_weeks":          raw[0] if len(raw) > 0 else [],
        "row2_inflation":      raw[1] if len(raw) > 1 else [],
        "row3_first_company":  raw[2] if len(raw) > 2 else [],
        "row4_second_company": raw[3] if len(raw) > 3 else [],
        "parsed_company_count": len(parsed["companies"]),
        "parsed_companies": parsed["companies"],
        "current_week": parsed["currentWeek"],
        "inflation": parsed["inflation"],
    })


if __name__ == "__main__":
    app.run(debug=True)
