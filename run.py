from app import create_app
from config import Config

# Starting up the gunicorn server:
# gunicorn -c gunicorn.conf.py run:app

# Build the Flask app by calling the factory with our Config class
app = create_app(Config)

if __name__ == "__main__":
    # Local entrypoint
    app.run(host="0.0.0.0", port=8080, debug=app.config.get("DEBUG", True))
