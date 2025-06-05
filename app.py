import duckdb
import io
import re
import json
from flask import Flask, render_template, request, redirect, session, jsonify, g
from lp_solver import allocate_customer_capacity

from flask_session import Session
from llm_client import call_llm_api, generate_formatted_summary
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

    response_text = call_llm_api(session["api_key"], fixed_messages)
    trigger, allocation_data = extract_allocation_data(response_text)
    if trigger == "WE_ARE_READY_TO_ALLOCATE":
        if not allocation_data:
            return jsonify(
                {"reply": "⚠️ I received the allocation trigger but couldn't parse the data. Please try again."})

        # Call allocator
        result = allocate_capacity_helper(session["api_key"], allocation_data, get_db())

        if result["status"] != "success":
            return jsonify({"reply": f"❌ Allocation failed: {result.get('message', 'Unknown error')}"})

        # Optionally insert allocation record into DB here

        # Build a reply summary
        summary = f"""✅ Allocation successful!
    - API Key of Customer: {session.get("api_key")}    
    - TPS Allocated: {result['total_allocated_tps']}
    - Destinations: {", ".join(allocation_data["destinations"])}
    - Peak Window: {allocation_data.get("peak_window", "N/A")} in 24 hours format
    - Peak TPS: {allocation_data.get("peak_tps", "N/A")}
    - Weekly Volume: {allocation_data.get("traffic_volume", "N/A")}
    ###"""

        llm_generated_summary = generate_formatted_summary(session.get("api_key"), summary)

        return jsonify({"reply": llm_generated_summary})

    return jsonify({"reply": response_text})

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

def allocate_capacity_helper(api_key, allocation_data, db_connection=None):
    required_keys = {"requested_tps", "destinations", "peak_window", "peak_tps"}

    if not required_keys.issubset(allocation_data):
        return {
            "status": "failure",
            "message": "Missing required fields"
        }

    result = allocate_customer_capacity(api_key, allocation_data)

    if result["status"] in {"failure", "error"}:
        # Optional: Save failure record as well
        if db_connection:
            save_allocation_record(db_connection, api_key, allocation_data, [{}], status="failure")
        return {
            "status": "failure",
            "message": result.get("message", "No feasible allocation found")
        }

    # Save success record if DB connection is provided
    if db_connection:
        with db_connection:
            save_allocation_record(db_connection, api_key, allocation_data, result["allocations"], status="success")
            update_allocated_tps_for_customer(api_key, allocation_data.get('requested_tps'))

    return {
        "status": "success",
        "allocations": result["allocations"],
        "total_allocated_tps": result["total_allocated_tps"]
    }

@app.route("/api/allocate", methods=["POST"])
def allocate_capacity():
    data = request.get_json()

    required_keys = {"requested_tps", "destinations", "peak_window", "peak_tps"}
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

def extract_allocation_data(llm_response: str):
    # Check if trigger exists
    if "WE_ARE_READY_TO_ALLOCATE" not in llm_response:
        return None, None

    # Extract JSON block using regex
    try:
        json_block = re.search(r"\{.*?\}", llm_response, re.DOTALL).group(0)
        allocation_data = json.loads(json_block)
        return "WE_ARE_READY_TO_ALLOCATE", allocation_data
    except Exception as e:
        print(f"Failed to parse allocation JSON: {e}")
        return "WE_ARE_READY_TO_ALLOCATE", None


def save_allocation_record(db_connection, api_key, allocation_data, allocations, status="success"):
    """
    Save the allocation record into the 'allocations' table.

    Parameters:
        db_connection: sqlite3.Connection object
        api_key: str, customer API key
        allocation_data: dict, contains requested_tps, destinations, traffic_volume, peak_window, peak_tps
        allocations: dict or list, the actual allocation details to be saved as JSON
        status: str, allocation status, e.g. "success" or "failure"
    """

    allocation_description = json.dumps(allocations)
    traffic_volume = int(allocation_data.get('traffic_volume') or 0)

    with db_connection:
        db_connection.execute("""
            INSERT INTO allocations (
                customer_api_key,
                requested_tps,
                requested_destinations,
                requested_volume,
                requested_peak_traffic_time,
                allocation_status,
                allocation_description
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            api_key,
            int(allocation_data.get('requested_tps', 0)),
            json.dumps(allocation_data.get('destinations', [])),  # store as JSON string
            traffic_volume,
            allocation_data.get('peak_window', '0-23'),
            status,
            allocation_description
        ))

def update_allocated_tps_for_customer(api_key, tps_assigned):
    """
    Increment the allocated_tps for a customer in the customer_info table.

    Parameters:
=        api_key: str, customer API key
        tps_assigned: int or float, TPS to add to the customer's allocated_tps
    """
    db_connection = duckdb.connect("traffic_data.duckdb")
    if not isinstance(tps_assigned, (int, float)):
        raise ValueError("tps_assigned must be a number")

    with db_connection:
        db_connection.execute("""
            UPDATE customer_info
            SET allocated_tps = COALESCE(allocated_tps, 0) + ?
            WHERE customer_api_key = ?
        """, (tps_assigned, api_key))

    # import json

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
