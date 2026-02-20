"""Blueprint registration for the web interface."""

from flask import Flask


def register_blueprints(app: Flask) -> None:
    from web.routes.dashboard import bp as dashboard_bp
    from web.routes.email_config import bp as email_bp
    from web.routes.vp_emails import bp as vp_emails_bp
    from web.routes.vp_signatures import bp as vp_signatures_bp
    from web.routes.kyc_team import bp as kyc_team_bp

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(email_bp)
    app.register_blueprint(vp_emails_bp)
    app.register_blueprint(vp_signatures_bp)
    app.register_blueprint(kyc_team_bp)
