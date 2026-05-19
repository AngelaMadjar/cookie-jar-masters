import os


class Config:
    SQLALCHEMY_DATABASE_URI = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://postgres:postgres@localhost:5432/cookie_jar_masters",
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_size": 8,
        "max_overflow": 0,
        "pool_pre_ping": True,
    }
    DEBUG = os.getenv("FLASK_DEBUG", "1") == "1"
