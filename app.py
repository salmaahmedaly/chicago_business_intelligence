from flask import Flask

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World! This is our Flask frontend."

if __name__ == "__main__":
    # By default, Flask runs on port 5000, but let's match your Dockerfile below with 8082.
    app.run(host="0.0.0.0", port=8082)
