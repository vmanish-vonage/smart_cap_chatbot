import duckdb
import io
from flask import Flask, render_template, request, redirect, session, jsonify, g
from lp_solver import allocate_customer_capacity

from flask_session import Session
from llm_client import call_llm_api
from scheduler import start_refresh_signature_scheduler, start_preprocess_scheduler
from dotenv import load_dotenv

load_dotenv()

preprocessed_data = None
app = Flask(__name__)
app.secret_key = "super_secret"
app.config["SESSION_TYPE"] = "filesystem"
Session(app)

def get_db():
    if "db" not in g:
        g.db = duckdb.connect("traffic_data.duckdb")
    return g.db

@app.teardown_appcontext
def close_db(exception):
    db = g.pop("db", None)
    if db is not None:
        db.close()

# -------- GLOBAL ROUTE GUARD -------- #
@app.before_request
def restrict_protected_routes():
    path = request.path
    if path.startswith("/chat") and "api_key" not in session:
        return redirect("/")
    if path.startswith("/admin/dashboard") and not session.get("admin_authenticated"):
        return redirect("/admin")

# --------- ROUTES --------- #
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

@app.route("/chat")
def chat():
    return render_template("chat.html")

@app.route("/chat/message", methods=["POST"])
def chat_message():
    if "api_key" not in session:
        return jsonify({"error": "Not authenticated"}), 403

    data = request.get_json()
    messages = data.get("messages", [])

    fixed_messages = []
    for msg in messages:
        role = msg.get("role")
        content = msg.get("content")

        if isinstance(content, str):
            content = [{"text": content}]
        elif not isinstance(content, list):
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

@app.route("/admin", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        password = request.form["password"]
        if password == "123456":
            session["admin_authenticated"] = True
            return redirect("/admin/dashboard")
        return "Invalid admin password", 401
    return render_template("admin_login.html")

@app.route("/admin/dashboard")
def admin_dashboard():
    conn = get_db()
    df = conn.execute("SELECT * FROM allocations").fetchdf()

    import plotly.express as px
    status_counts = df["allocation_status"].value_counts().reset_index()
    status_counts.columns = ["status", "count"]
    fig = px.bar(status_counts, x="status", y="count", title="Allocation Status Overview")

    buffer = io.StringIO()
    fig.write_html(buffer, include_plotlyjs='cdn')
    graph_html = buffer.getvalue()

    return render_template("admin_dashboard.html", graph_html=graph_html)

@app.route("/api/allocate", methods=["POST"])
def allocate_capacity():
    data = request.get_json()

    required_keys = {"requested_tps", "destinations", "traffic_volume", "peak_window", "peak_tps"}
    if not required_keys.issubset(data):
        return jsonify({"error": "Missing required fields"}), 400

    result = allocate_customer_capacity(session.get("api_key"), data)

    if result["status"] == "failure" or result["status"] == "error":
        return jsonify({"status": "failure", "message": result.get("message", "No feasible allocation found")}), 400

    return jsonify({
        "status": "success",
        "allocation": result["allocations"],
        "total_allocated": result["total_allocated_tps"]
    })


    # import json
    # allocation_description = json.dumps({
    #     "allocation": allocations,
    #     "status": "success",
    #     "total_allocated": round(float(sum(result.x)), 2)
    # })

    # OR allocation_description = json.dumps(allocations)
    # con.execute("""
    #     INSERT INTO allocations (
    #         customer_api_key,
    #         requested_tps,
    #         requested_destinations,
    #         requested_volume,
    #         requested_peak_traffic_time,
    #         allocation_status,
    #         allocation_description
    #     ) VALUES (?, ?, ?, ?, ?, ?, ?)
    # """, (
    #     session.get("api_key")
    #     int(request['requested_tps']),
    #     str(request['destinations']),
    #     int(request.get('traffic_volume', 0)),
    #     request.get('peak_window', '0-23'),
    #     allocation_description
    # ))

if __name__ == "__main__":
    start_refresh_signature_scheduler()
    start_preprocess_scheduler()
    app.run(debug=False, threaded=False)
