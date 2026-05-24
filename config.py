import os


class Config:
    # Primary DB connection string. DATABASE_URL overrides the local default.
    SQLALCHEMY_DATABASE_URI = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://postgres:postgres@localhost:5432/cookie_jar_masters",
    )

    # Disable Flask-SQLAlchemy event system overhead
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Configuration rationale:
    # - pool_size=8 with max_overflow=0 creates a hard DB connection budget that
    #   matches the app's active-processing cap (8 threads), so queueing is 
    #   attributable to controlled app capacity (not hidden pool bursting).
    # - This fixed budget improves interpretability across T1-T7 by keeping the
    #   baseline stable and comparable run-to-run.
    # - With workers=1, threads=8, and an app-level cap of 8, a larger pool would
    #   remain underutilized.
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_size": 8,         # Keep up to 8 persistent DB connections in the pool
        "max_overflow": 0,      # Allow no temporary extra connections above pool_size
        "pool_pre_ping": True,  # Check connection liveness before use; recycle stale ones
    }

    # Controls Flask debug mode when run.py starts the app directly (on by default)
    DEBUG = os.getenv("FLASK_DEBUG", "1") == "1"
