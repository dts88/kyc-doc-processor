"""Flask app factory for the KYC settings web interface."""

import os
from pathlib import Path

from flask import Flask

from database.connection import DatabaseManager


def create_app(db_path: str | None = None) -> Flask:
    """Create and configure the Flask application."""
    app = Flask(
        __name__,
        template_folder=str(Path(__file__).parent / "templates"),
    )
    app.secret_key = os.urandom(24)

    # Store database path for use in routes
    if db_path is None:
        project_root = Path(__file__).parent.parent
        db_path = str(project_root / "data" / "kyc.db")
    app.config["DB_PATH"] = db_path

    # Register blueprints
    from web.routes import register_blueprints
    register_blueprints(app)

    return app


def get_db_from_app(app: Flask) -> DatabaseManager:
    """Get a DatabaseManager instance from the Flask app config."""
    return DatabaseManager(app.config["DB_PATH"])
