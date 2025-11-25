from flask import Flask, render_template, request, redirect, url_for, session, flash
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from waitress import serve

app = Flask(__name__)

app.secret_key = "your_secret_key_here"  # Required for session management

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name("cred.json", scope)
client = gspread.authorize(credentials)
sheet = client.open("Bank-Info")
users_sheet = sheet.worksheet("Users")
transactions_sheet = sheet.worksheet("Transactions")

def get_all_users():
    return users_sheet.get_all_records()

def update_balance(username, new_balance):
    cell = users_sheet.find(username)
    users_sheet.update_cell(cell.row, 3, new_balance)  # Column 3 is Balance
    from datetime import datetime

def add_transaction(sender, receiver, amount):
    transactions_sheet.append_row([sender, receiver, amount, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])

def get_user_balance(username):
    cell = users_sheet.find(username)
    return float(users_sheet.cell(cell.row, 3).value)

def get_user_transactions(username):
    all_transactions = transactions_sheet.get_all_records()
    return [t for t in all_transactions if t["Sender"] == username or t["Receiver"] == username]


def transfer_money(sender, receiver, amount):
    # Find sender and receiver rows
    sender_cell = users_sheet.find(sender)
    receiver_cell = users_sheet.find(receiver)

    # Get current balances
    sender_balance = float(users_sheet.cell(sender_cell.row, 3).value)
    receiver_balance = float(users_sheet.cell(receiver_cell.row, 3).value)

    # Check if sender has enough balance
    if sender_balance >= amount:
        # Update balances
        sender_new_balance = sender_balance - amount
        receiver_new_balance = receiver_balance + amount

        users_sheet.update_cell(sender_cell.row, 3, sender_new_balance)
        users_sheet.update_cell(receiver_cell.row, 3, receiver_new_balance)

        # Record transaction
        add_transaction(sender, receiver, amount)
        return True
    else:
        return False


users = {
    "zachary": {"password": "123", "balance": 1000},
    "test": {"password": "test123", "balance": 500} 
}

@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        # Fetch all users from Google Sheets
        users = get_all_users()

        # Check credentials
        user = next((u for u in users if u["Username"] == username and u["Password"] == password), None)
        if user:
            session["user"] = username
            return redirect(url_for("account"))
        else:
            return render_template("login.html", error="Invalid credentials")
    return render_template("login.html")


@app.route("/account")
def account():
    if "user" not in session:
        return redirect(url_for("login"))

    username = session["user"]
    balance = get_user_balance(username)
    transactions = get_user_transactions(username)

    return render_template("account.html", username=username, balance=balance, transactions=transactions)

@app.route("/transfer", methods=["GET", "POST"])
def transfer():
    if "user" not in session:
        return redirect(url_for("login"))

    if request.method == "POST":
        sender = session["user"]
        receiver = request.form["receiver"]
        amount = float(request.form["amount"])

        if transfer_money(sender, receiver, amount):
            flash("Transfer successful!")
        else:
            flash("Insufficient balance!")

        return redirect(url_for("account"))

    return render_template("transfer.html")


@app.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("login"))

if __name__ == "__main__":
    app.run(debug=True)