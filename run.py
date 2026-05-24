from app import create_app
from config import Config

# Starting up the gunicorn server:
# gunicorn -c gunicorn.conf.py run:app

# Running experiment for a test case (e.g. test case: 3, repeats: 3):
# python3 tools/locust/run_locust_benchmarks.py --host http://127.0.0.1:8080 --test-cases T1 --repeats 3

# Build the Flask app by calling the factory with our Config class
app = create_app(Config)

if __name__ == "__main__":
    # Local entrypoint
    app.run(host="0.0.0.0", port=8080, debug=app.config.get("DEBUG", True))
