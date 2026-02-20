"""Dashboard route — settings overview."""

from flask import Blueprint, render_template

from web import get_db_from_app
from flask import current_app

bp = Blueprint("dashboard", __name__)


@bp.route("/")
def index():
    db = get_db_from_app(current_app)
    try:
        from web.services.settings_service import is_smtp_configured, get_kyc_team
        from web.services.vp_service import get_vp_emails, get_active_vp_names

        smtp_ok = is_smtp_configured(db)
        vp_emails = get_vp_emails(db)
        vp_names = get_active_vp_names(db)
        kyc_members = get_kyc_team(db)

        return render_template(
            "dashboard.html",
            smtp_configured=smtp_ok,
            vp_email_count=len(vp_emails),
            vp_signature_count=len(vp_names),
            kyc_team_count=len(kyc_members),
        )
    finally:
        db.close()
