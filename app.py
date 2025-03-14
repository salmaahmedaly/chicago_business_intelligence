import os
import psycopg2
from flask import Flask
import pandas as pd
app = Flask(__name__)

# Example: environment variables for your DB connection
# DB_HOST = os.environ.get("DB_HOST", "localhost")
# DB_NAME = os.environ.get("DB_NAME", "postgres")
# DB_USER = os.environ.get("DB_USER", "postgres")
# DB_PASS = os.environ.get("DB_PASS", "secret")

@app.route("/")
def hello():
    return "Hello from the Flask app!"

# db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=/cloudsql/chicago-business-intel:us-central1:mypostgres sslmode=disable port = 5432"


@app.route("/taxi-trip-first")
def taxi_trip_first():
    # Connect to your Postgres
    conn = psycopg2.connect(
        host='/cloudsql/chicago-business-intel:us-central1:mypostgres',
        dbname='chicago_business_intelligence',
        user='postgres',
        password='root',
        port=5432,
        sslmode='disable'
    )

    # if connection is successful, print connection successful
    if conn:
        print("Connection successful", conn)
    else:
        print("Connection unsuccessful")

    try:
        with conn.cursor() as cur:
            # Just fetch 1 row from the taxi_trip table
            # cur.execute("SELECT * FROM taxi_trips;")
            # row = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM taxi_trips;")
            count = cur.fetchone()
            print(count)


            # row is a tuple of columns e.g. (col1, col2, col3, ...)
            # For example, you can return it as plain text or JSON:
            if count:
                return f" count: {count}"
            else:
                return "No rows found in taxi_trip."
    finally:
        conn.close()


#create a forcasting graph for taxi trips by zipcode For streetscaping investment and planning, 
# the city of Chicago is interested to forecast daily, weekly, and monthly traffic
#  patterns utilizing the taxi trips for the different zip codes.

@app.route("/taxi-trip-forecast")
def taxi_trip_forecast():
    # Connect to your Postgres
    conn = psycopg2.connect(
        host='/cloudsql/chicago-business-intel:us-central1:mypostgres',
        dbname='chicago_business_intelligence',
        user='postgres',
        password='root',
        port=5432,
        sslmode='disable'
    )

    # if connection is successful, print connection successful
    if conn:
        print("Connection successful", conn)
    else:
        print("Connection unsuccessful")

    try:
        with conn.cursor() as cur:
            # Just fetch 1 row from the taxi_trip table
            # cur.execute("SELECT * FROM taxi_trips;")
            # row = cur.fetchone()
            cur.execute("SELECT * FROM taxi_trips;")
            count = cur.fetchone()
            # read data as pandas dataframe
            df = pd.read_sql("SELECT * FROM taxi_trips;", conn)
            print(df.head())
            print(count)


            # row is a tuple of columns e.g. (col1, col2, col3, ...)
            # For example, you can return it as plain text or JSON:
            if count:
                return f" count: {count}"
            else:
                return "No rows found in taxi_trip."
    finally:
        conn.close()


# def forcast_weekly_traffic():
    


if __name__ == "__main__":
    # Might run on port 8082, or a typical Flask default port 5000
    app.run(host="0.0.0.0", port=8082)
