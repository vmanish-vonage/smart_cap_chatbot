from flask import Flask, render_template, request, redirect, session, jsonify
from flask_session import Session
from llm_client import call_llm_api
from scheduler import start_scheduler

from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env


app = Flask(__name__)
app.secret_key = "super_secret"
app.config["SESSION_TYPE"] = "filesystem"
Session(app)


@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        api_key = request.form["customer_api_key"]
        password = request.form["password"]
        if password == "1234":
            session["api_key"] = api_key
            return redirect("/chat")
        return "Invalid password", 401
    return render_template("login.html")


@app.route("/chat", methods=["GET"])
def chat():
    if "api_key" not in session:
        return redirect("/")
    return render_template("chat.html")


@app.route("/chat/message", methods=["POST"])
def chat_message():
    if "api_key" not in session:
        return jsonify({"error": "Not authenticated"}), 403
    data = request.get_json()
    messages = data.get("messages", [])

    # Fix messages: ensure each message content is a list of dicts with key "text"
    fixed_messages = []
    for msg in messages:
        role = msg.get("role")
        content = msg.get("content")

        # If content is a string, wrap it as a list of one dict {"text": ...}
        if isinstance(content, str):
            content = [{"text": content}]
        # If content is already a list, leave as is
        elif not isinstance(content, list):
            # If malformed or missing, default to empty list
            content = []

        fixed_messages.append({
            "role": role,
            "content": content
        })

    response = call_llm_api(session["api_key"], fixed_messages)
    return jsonify({"reply": response})


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")


if __name__ == "__main__":
    start_scheduler()
    app.run(debug=True)
