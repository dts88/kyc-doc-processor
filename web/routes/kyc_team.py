"""KYC team email management routes."""

from flask import Blueprint, render_template, request, flash, redirect, url_for, current_app

from web import get_db_from_app
from web.services.settings_service import get_kyc_team, add_kyc_member, delete_kyc_member

bp = Blueprint("kyc_team", __name__)


@bp.route("/kyc-team", methods=["GET", "POST"])
def kyc_team_list():
    db = get_db_from_app(current_app)
    try:
        if request.method == "POST":
            action = request.form.get("action")

            if action == "add":
                name = request.form.get("name", "").strip()
                email = request.form.get("email", "").strip()
                role = request.form.get("role", "kyc_reviewer").strip()
                if not name or not email:
                    flash("Name and email are required.", "error")
                else:
                    try:
                        add_kyc_member(db, name, email, role)
                        flash(f"Added KYC team member: {name} ({email})", "success")
                    except Exception:
                        flash(f"Email {email} already exists.", "error")

            elif action == "delete":
                member_id = request.form.get("member_id", type=int)
                if member_id:
                    delete_kyc_member(db, member_id)
                    flash("KYC team member removed.", "success")

            return redirect(url_for("kyc_team.kyc_team_list"))

        members = get_kyc_team(db)
        return render_template("kyc_team.html", members=members)
    finally:
        db.close()
