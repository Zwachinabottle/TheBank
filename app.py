from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

app = Flask(__name__, static_folder='images')
app.secret_key = "your_secret_key_here"

# ---------- GOOGLE SHEETS SETUP ----------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name("cred.json", scope)
client = gspread.authorize(credentials)

sheet = client.open("Bank-Info")
users_sheet = sheet.worksheet("Users")
loans_sheet = sheet.worksheet("Loans")
transactions_sheet = sheet.worksheet("Transactions")
# interest_sheet = sheet.worksheet("LoanRates")
# interest_cell = interest_sheet.find("Rates")
# if interest_cell:
#     interest_rate = float(interest_cell.value)
# else:
#     interest_rate = 0.01 # default interest rate if not found
# 
interest_rate = 0.01

# ---------- HELPERS ----------
def get_all_users():
    return users_sheet.get_all_records()

def get_all_users_with_balances():
    return users_sheet.get_all_records()


def update_balance(username, new_balance):
    cell = users_sheet.find(username)
    users_sheet.update_cell(cell.row, 3, new_balance)


def add_transaction(sender, receiver, amount, comment=""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not comment:
        comment = "No comment"
    transactions_sheet.append_row([sender, receiver, amount, now, comment])


def get_user_balance(username):
    cell = users_sheet.find(username)
    return float(users_sheet.cell(cell.row, 3).value)


def get_user_transactions(username):
    rows = transactions_sheet.get_all_records()
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
    return formatted


def transfer_money(sender, receiver, amount, comment):
    sender_cell = users_sheet.find(sender)
    receiver_cell = users_sheet.find(receiver)

    sender_balance = float(users_sheet.cell(sender_cell.row, 3).value)
    receiver_balance = float(users_sheet.cell(receiver_cell.row, 3).value)

    if sender_balance < amount:
        return False

    users_sheet.update_cell(sender_cell.row, 3, sender_balance - amount)
    users_sheet.update_cell(receiver_cell.row, 3, receiver_balance + amount)

    add_transaction(sender, receiver, amount, comment)
    return True

def loan_money(sender, reason, amount, weeks):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_amount = amount * (1 + float(interest_rate) * weeks) 
    weekly_payment = round(total_amount / weeks, 2)
    loans_sheet.append_row([sender, reason, amount, weeks, weekly_payment, "Pending", now])


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


@app.route("/account")
def account():
    if "user" not in session:
        return redirect(url_for("login"))

    username = session["user"]
    balance = get_user_balance(username)
    transactions = get_user_transactions(username)

    return render_template("account.html",
                           username=username,
                           balance=balance,
                           transactions=transactions)


@app.route("/transfer", methods=["POST"])
def transfer():
    if "user" not in session:
        return redirect(url_for("login"))

    sender = session["user"]
    receiver = request.form["receiver"]
    amount = float(request.form["amount"])
    comment = request.form.get("comment", "No comment")

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

    if request.method == "POST":
        sender = session["user"]
        reason = request.form["reason"]
        amount = float(request.form["amount"])
        weeks = int(request.form["weeks"])
        loan_money(sender, reason, amount, weeks)
        flash("Loan Application Received!")
        return redirect(url_for("account"))

    return render_template("loan.html",username=session["user"],irate=interest_rate)

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

@app.route("/teachertools")
@role_required("Teacher", "Banker")
def teacher_tools():
    if "user" not in session:
        return redirect(url_for("teacher_tools_login"))

    username = session["user"]
    users = get_all_users_with_balances()

    normalize_roles_column()
    
    return render_template(
    "teachertools.html",
    users=users,
    username=username,
    pending_loans=pending_loans,
    active_loans=active_loans
)

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
