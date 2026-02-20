"""VP signature management routes."""

from flask import Blueprint, render_template, request, flash, redirect, url_for, current_app

from web import get_db_from_app
from web.services.vp_service import (
    get_vp_signatures, add_vp_signature, deactivate_vp_signature,
    get_signed_onboarding_forms, get_active_vp_names,
)

bp = Blueprint("vp_signatures", __name__)


@bp.route("/vp-signatures", methods=["GET", "POST"])
def vp_signature_list():
    db = get_db_from_app(current_app)
    try:
        if request.method == "POST":
            action = request.form.get("action")

            if action == "record":
                file_id = request.form.get("file_id", type=int)
                if file_id:
                    # Find the VP name from the onboarding form
                    forms = get_signed_onboarding_forms(db)
                    for form in forms:
                        if form["file_id"] == file_id:
                            vp_name = form["vp_name"]
                            if vp_name:
                                add_vp_signature(
                                    db, vp_name,
                                    source_file_id=file_id,
                                    source_description=f"{form['filename']} ({form['company']})",
                                )
                                flash(f"Recorded VP signature: {vp_name}", "success")
                            else:
                                flash("VP name not found in form extraction data.", "error")
                            break
                    else:
                        flash("Selected form not found.", "error")

            elif action == "manual_add":
                vp_name = request.form.get("vp_name", "").strip()
                if not vp_name:
                    flash("VP name is required.", "error")
                else:
                    add_vp_signature(db, vp_name, source_description="Manually added")
                    flash(f"Added VP signature: {vp_name}", "success")

            elif action == "deactivate":
                sig_id = request.form.get("sig_id", type=int)
                if sig_id:
                    deactivate_vp_signature(db, sig_id)
                    flash("VP signature deactivated.", "success")

            return redirect(url_for("vp_signatures.vp_signature_list"))

        signatures = get_vp_signatures(db)
        signed_forms = get_signed_onboarding_forms(db)
        active_names = get_active_vp_names(db)

        return render_template(
            "vp_signatures.html",
            signatures=signatures,
            signed_forms=signed_forms,
            active_vp_count=len(active_names),
        )
    finally:
        db.close()
