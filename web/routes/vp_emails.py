"""VP email whitelist management routes."""

from flask import Blueprint, render_template, request, flash, redirect, url_for, current_app

from web import get_db_from_app
from web.services.vp_service import get_vp_emails, add_vp_email, delete_vp_email

bp = Blueprint("vp_emails", __name__)


@bp.route("/vp-emails", methods=["GET", "POST"])
def vp_email_list():
    db = get_db_from_app(current_app)
    try:
        if request.method == "POST":
            action = request.form.get("action")

            if action == "add":
                vp_name = request.form.get("vp_name", "").strip()
                email = request.form.get("email", "").strip()
                if not vp_name or not email:
                    flash("VP name and email are required.", "error")
                else:
                    try:
                        add_vp_email(db, vp_name, email)
                        flash(f"Added VP: {vp_name} ({email})", "success")
                    except Exception:
                        flash(f"Email {email} already exists.", "error")

            elif action == "delete":
                vp_id = request.form.get("vp_id", type=int)
                if vp_id:
                    delete_vp_email(db, vp_id)
                    flash("VP email removed.", "success")

            return redirect(url_for("vp_emails.vp_email_list"))

        emails = get_vp_emails(db)
        return render_template("vp_emails.html", vp_emails=emails)
    finally:
        db.close()
