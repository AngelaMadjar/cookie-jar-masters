import os


class Config:
    # Previous local default (kept for reference):
    # SQLALCHEMY_DATABASE_URI = os.getenv(
    #     "DATABASE_URL",
    #     "postgresql+psycopg2://postgres:postgres@localhost:5432/cookie_jar_masters",
    # )
    
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "")
    db_name = os.getenv("DB_NAME", "cookie-jar-masters")
    cloud_sql_connection_name = os.getenv(
        "CLOUD_SQL_CONNECTION_NAME",
        "dept-dinl-angela:europe-west1:cookie-jar-masters",
    )

    SQLALCHEMY_DATABASE_URI = os.getenv(
        "DATABASE_URL",
        f"postgresql+psycopg2://{db_user}:{db_password}@/{db_name}"
        f"?host=/cloudsql/{cloud_sql_connection_name}",
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Keeping the same db configuration to preserve benchmark control variables and make the comparison of E1 and E2 fair
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_size": 8,
        "max_overflow": 0,
        "pool_pre_ping": True,
    }
    DEBUG = os.getenv("FLASK_DEBUG", "1") == "1"
