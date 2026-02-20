"""Email/SMTP configuration routes."""

from flask import Blueprint, render_template, request, flash, redirect, url_for, current_app

from web import get_db_from_app
from web.services.settings_service import get_smtp_config, save_smtp_config, get_kyc_team_emails
from web.services.email_test import send_test_email

bp = Blueprint("email_config", __name__)


@bp.route("/email", methods=["GET", "POST"])
def email_settings():
    db = get_db_from_app(current_app)
    try:
        if request.method == "POST":
            action = request.form.get("action")

            if action == "save":
                save_smtp_config(db, {
                    "smtp_host": request.form.get("smtp_host", "").strip(),
                    "smtp_port": request.form.get("smtp_port", "587").strip(),
                    "smtp_user": request.form.get("smtp_user", "").strip(),
                    "smtp_password": request.form.get("smtp_password", "").strip(),
                    "smtp_from_address": request.form.get("smtp_from_address", "").strip(),
                })
                flash("SMTP settings saved.", "success")
                return redirect(url_for("email_config.email_settings"))

            elif action == "test":
                smtp = get_smtp_config(db)
                recipients = get_kyc_team_emails(db)
                if not recipients:
                    flash("No KYC team members configured. Add at least one before testing.", "error")
                else:
                    ok, msg = send_test_email(smtp, recipients[0])
                    flash(msg, "success" if ok else "error")
                return redirect(url_for("email_config.email_settings"))

            elif action == "import_yaml":
                _import_from_yaml(db)
                flash("Imported SMTP settings from config.yaml.", "success")
                return redirect(url_for("email_config.email_settings"))

        smtp = get_smtp_config(db)
        return render_template("email_config.html", smtp=smtp)
    finally:
        db.close()


def _import_from_yaml(db):
    """Import SMTP settings from config.yaml into the database."""
    from pathlib import Path
    import yaml

    config_path = Path(__file__).parent.parent.parent / "config.yaml"
    if not config_path.exists():
        return
    with open(config_path) as f:
        config = yaml.safe_load(f)

    notif = config.get("notification", {})
    mapping = {
        "smtp_host": notif.get("smtp_host", ""),
        "smtp_port": str(notif.get("smtp_port", 587)),
        "smtp_user": notif.get("smtp_user", ""),
        "smtp_password": notif.get("smtp_password", ""),
        "smtp_from_address": notif.get("from_address", ""),
    }
    save_smtp_config(db, mapping)
