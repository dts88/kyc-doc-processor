"""Flask app factory for the KYC settings web interface."""

import os
from pathlib import Path

from flask import Flask
from flask_wtf.csrf import CSRFProtect

from database.connection import DatabaseManager

csrf = CSRFProtect()

# Project root for path validation
PROJECT_ROOT = Path(__file__).parent.parent
ALLOWED_DATA_DIR = PROJECT_ROOT / "data"


def create_app(db_path: str | None = None) -> Flask:
    """Create and configure the Flask application."""
    app = Flask(
        __name__,
        template_folder=str(Path(__file__).parent / "templates"),
    )
    app.secret_key = os.urandom(24)

    # CSRF protection
    csrf.init_app(app)

    # Store database path and allowed data directory for use in routes
    if db_path is None:
        db_path = str(ALLOWED_DATA_DIR / "kyc.db")
    app.config["DB_PATH"] = db_path
    app.config["ALLOWED_DATA_DIR"] = str(ALLOWED_DATA_DIR.resolve())

    # Register blueprints
    from web.routes import register_blueprints
    register_blueprints(app)

    return app


def get_db_from_app(app: Flask) -> DatabaseManager:
    """Get a DatabaseManager instance from the Flask app config."""
    return DatabaseManager(app.config["DB_PATH"])


def is_safe_path(app: Flask, file_path: str | Path) -> bool:
    """Check if a file path is within the allowed data directory."""
    try:
        resolved = Path(file_path).resolve()
        allowed = Path(app.config["ALLOWED_DATA_DIR"])
        return str(resolved).startswith(str(allowed))
    except (ValueError, OSError):
        return False
