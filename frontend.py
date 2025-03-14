from flask import Flask, jsonify, request
import psycopg2
import os
import sys

app = Flask(__name__)

# Cloud SQL Connection Details
DB_USER = "postgres"
DB_NAME = "chicago_business_intelligence"
DB_PASSWORD = "root"
DB_HOST = "/cloudsql/chicago-business-intel:us-central1:mypostgres"  # Cloud SQL Connection
DB_PORT = "5432"

# Function to connect to the database
def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"❌ Error connecting to database: {e}", file=sys.stderr)
        return None

# 📢 Debugging: Log all incoming requests
@app.before_request
def log_request_info():
    print(f"📢 Received request: {request.method} {request.path}", file=sys.stderr)

# ✅ Define the API endpoint correctly
@app.route("/api/taxi_trips", methods=["GET"])
def get_taxi_trips():
    conn = get_db_connection()
    if not conn:
        print("❌ Database connection failed", file=sys.stderr)
        return jsonify({"error": "Database connection failed"}), 500

    try:
        print("🚀 Fetching data from database...", file=sys.stderr)
        cur = conn.cursor()
        cur.execute("SELECT * FROM public.taxi_trips LIMIT 5;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        print("✅ Database connection closed", file=sys.stderr)

        # Convert result to JSON format
        taxi_trips = [
            {
                "trip_id": row[0],  # Adjust column indexes as per DB structure
                "trip_start_timestamp": row[1],
                "trip_end_timestamp": row[2],
                "pickup_latitude": row[3],
                "pickup_longitude": row[4],
                "dropoff_latitude": row[5],
                "dropoff_longitude": row[6],
                "pickup_zip_code": row[7],
                "dropoff_zip_code": row[8],
                "pickup_airport": row[9]
            } for row in rows
        ]
        print("✅ Data fetched successfully", file=sys.stderr)
        return jsonify(taxi_trips)
    except Exception as e:
        print(f"❌ Failed to fetch data: {e}", file=sys.stderr)
        return jsonify({"error": f"Failed to fetch data: {str(e)}"}), 500

# ✅ Ensure Flask listens on the correct PORT (Cloud Run requires this)
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8082))  # Default to 8082
    print(f"🚀 Starting Flask on PORT {port}...", file=sys.stderr)
    app.run(host="0.0.0.0", port=port, debug=True)
