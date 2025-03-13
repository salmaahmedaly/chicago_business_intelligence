from flask import Flask, jsonify
import psycopg2
import os

app = Flask(__name__)

# Database connection details
DB_USER = "postgres"
DB_NAME = "chicago_business_intelligence"
DB_PASSWORD = "root"
DB_HOST = "/cloudsql/chicago-business-intel:us-central1:mypostgres"
DB_PORT = "5432"

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("✅ Successfully connected to database")
        return conn
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        return None

@app.route("/api/taxi_trips", methods=["GET"])
def get_taxi_trips():
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM public.taxi_trips LIMIT 5;")
        rows = cur.fetchall()
        cur.close()
        conn.close()

        taxi_trips = [
            {
                "trip_id": row[1],
                "trip_start_timestamp": row[2],
                "trip_end_timestamp": row[3],
                "pickup_latitude": row[4],
                "pickup_longitude": row[5],
                "dropoff_latitude": row[6],
                "dropoff_longitude": row[7],
                "pickup_zip_code": row[8],
                "dropoff_zip_code": row[9],
                "pickup_airport": row[10]
            } for row in rows
        ]
        print("✅ Successfully fetched taxi trips")
        return jsonify(taxi_trips)
    except Exception as e:
        return jsonify({"error": f"Failed to fetch data: {str(e)}"}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8082))  # Read the PORT environment variable
    print(f"Starting server on port {port}")
    app.run(host="0.0.0.0", port=port, debug=True)

