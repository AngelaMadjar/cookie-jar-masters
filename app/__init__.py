from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

db = SQLAlchemy()
migrate = Migrate()


def create_app(config_class):
    # Application factory pattern: creates a new Flask app instance using
    # the provided config class (e.g. dev/test/prod can pass different configs).
    app = Flask(__name__)
    app.config.from_object(config_class)

    # Register HTTP route groups (blueprints).
    from app.routes.database.database_routes import bp as db_bp
    from app.routes.scan.tracker_ingestion_routes import bp as tracker_ingestion_bp
    app.register_blueprint(db_bp)
    app.register_blueprint(tracker_ingestion_bp)

    # Bind extensions to this app instance.
    db.init_app(app)
    migrate.init_app(app, db)

    # Import models so SQLAlchemy metadata is fully registered before migrations/ORM operations run
    from app import models

    return app
