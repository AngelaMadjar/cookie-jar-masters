import os


class Config:
    # Cloud SQL connection parts
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "")
    db_name = os.getenv("DB_NAME", "postgres")
    cloud_sql_connection_name = os.getenv(
        "CLOUD_SQL_CONNECTION_NAME",
        "dept-dinl-angela:europe-west1:cookie-jar-masters",
    )

    # Primary DB connection string built from the variables above
    SQLALCHEMY_DATABASE_URI = os.getenv(
        "DATABASE_URL",
        f"postgresql+psycopg2://{db_user}:{db_password}@/{db_name}"
        f"?host=/cloudsql/{cloud_sql_connection_name}",
    )

    # Disable Flask-SQLAlchemy event overhead.
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Shared benchmark control variables (same as E1) for fair E1/E2 comparison:
    # - pool_size=8 with max_overflow=0 keeps an explicit connection budget
    # - aligns DB-side parallelism envelope with app-side concurrency design
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_size": 8,         # Keep up to 8 persistent DB connections in the pool.
        "max_overflow": 0,      # Allow no temporary extra connections above pool_size.
        "pool_pre_ping": True,  # Validate connection liveness before checkout.
    }

    # Controls Flask debug mode when run.py starts the app directly.
    DEBUG = os.getenv("FLASK_DEBUG", "1") == "1"
