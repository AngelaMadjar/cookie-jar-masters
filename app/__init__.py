from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

db = SQLAlchemy()
migrate = Migrate()


def create_app(config_class):
    app = Flask(__name__)
    app.config.from_object(config_class)

    from app.routes.database.database_routes import bp as db_bp
    from app.routes.scan.tracker_ingestion_routes import bp as tracker_ingestion_bp
    app.register_blueprint(db_bp)
    app.register_blueprint(tracker_ingestion_bp)

    db.init_app(app)
    migrate.init_app(app, db)

    from app import models  

    return app
