from flask import Flask, render_template, request, redirect, url_for, session
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from waitress import serve

app = Flask(__name__)

app.secret_key = "your_secret_key_here"  # Required for session management

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name("cred.json", scope)
client = gspread.authorize(credentials)
sheet = client.open("Bank-Info").sheet1

users = {
    "zachary": {"password": "password123", "balance": 1000},
    "test": {"password": "test123", "balance": 500} 
}

@app.route("/")
def login():
    """ if request.method == 'POST':
        username = request.form["username"]
        password = request.form["password"]
        if username in users and users[username]["password"] == password:
            session["user"] = username
            return redirect(url_for("account"))
        else:
            return render_template("login.html") """
    return render_template("login.html")

if __name__ == "__main__":
    app.run(debug=True)