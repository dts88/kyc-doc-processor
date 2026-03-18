"""File management routes — view files, assign classifications, merge counterparties."""

from pathlib import Path

from flask import (
    Blueprint, render_template, request, flash, redirect,
    url_for, current_app, send_file, abort,
)

from classification.doc_types import DOC_TYPES
from web import get_db_from_app, is_safe_path
from web.services.file_service import (
    assign_file_classification,
    get_all_counterparties,
    get_all_files,
    get_counterparty_progress,
    get_file_detail,
    merge_counterparties,
)

bp = Blueprint("file_mgmt", __name__)


@bp.route("/files", methods=["GET"])
def file_list():
    db = get_db_from_app(current_app)
    try:
        status_filter = request.args.get("status", "all")
        files = get_all_files(db, status_filter)
        counterparties = get_all_counterparties(db)
        progress = get_counterparty_progress(db)

        # Compute stats
        all_files = get_all_files(db, "all")
        stats = {
            "total": len(all_files),
            "needs_review": sum(1 for f in all_files if f["status"] == "needs_review"),
            "error": sum(1 for f in all_files if f["status"] == "error"),
            "classified": sum(1 for f in all_files if f["status"] == "classified"),
            "extraction_done": sum(1 for f in all_files if f["status"] == "extraction_done"),
            "packaged": sum(1 for f in all_files if f["status"] == "packaged"),
        }

        # Build file_id -> list of assigned doc type codes
        file_doc_types = {}
        dc_rows = db.execute(
            """SELECT dc.file_id, dt.code
               FROM document_classifications dc
               JOIN kyc_doc_types dt ON dc.doc_type_id = dt.id
               ORDER BY dc.file_id, dc.is_primary DESC"""
        )
        for row in dc_rows:
            file_doc_types.setdefault(row["file_id"], []).append(row["code"])

        return render_template(
            "file_management.html",
            files=files,
            stats=stats,
            status_filter=status_filter,
            counterparties=counterparties,
            progress=progress,
            doc_types=DOC_TYPES,
            file_doc_types=file_doc_types,
        )
    finally:
        db.close()


@bp.route("/files", methods=["POST"])
def file_action():
    db = get_db_from_app(current_app)
    try:
        action = request.form.get("action")

        if action == "assign":
            file_id = request.form.get("file_id", type=int)
            doc_types_raw = request.form.getlist("doc_types")
            counterparty = request.form.get("counterparty", "").strip()
            fuzzy_threshold = request.form.get("fuzzy_threshold", 85, type=int)

            if not file_id or not doc_types_raw:
                flash("File ID and at least one document type are required.", "error")
                return redirect(url_for("file_mgmt.file_list"))

            result = assign_file_classification(
                db, file_id, doc_types_raw, counterparty, fuzzy_threshold
            )

            if "error" in result:
                flash(result["error"], "error")
            else:
                types_str = ", ".join(result["doc_types"])
                flash(
                    f"Assigned file #{result['file_id']} ({result['filename']}) "
                    f"as [{types_str}] to {result['counterparty_name']} (#{result['counterparty_id']})"
                    f" — extraction running in background",
                    "success",
                )

        elif action == "delete":
            file_id = request.form.get("file_id", type=int)
            if file_id:
                from web.services.file_service import delete_file
                result = delete_file(db, file_id)
                if "error" in result:
                    flash(result["error"], "error")
                else:
                    flash(f"Deleted file: {result['filename']}", "success")

        elif action == "bulk_delete":
            file_ids = request.form.getlist("file_ids", type=int)
            if file_ids:
                from web.services.file_service import delete_file
                deleted = 0
                for fid in file_ids:
                    result = delete_file(db, fid)
                    if "error" not in result:
                        deleted += 1
                    else:
                        flash(f"File #{fid}: {result['error']}", "error")
                if deleted:
                    flash(f"Deleted {deleted} file(s).", "success")

        elif action == "bulk_assign":
            file_ids = request.form.getlist("file_ids", type=int)
            doc_types_raw = request.form.getlist("doc_types")
            counterparty = request.form.get("counterparty", "").strip()
            fuzzy_threshold = request.form.get("fuzzy_threshold", 85, type=int)

            if not file_ids or not doc_types_raw:
                flash("Select files and at least one document type.", "error")
                return redirect(url_for("file_mgmt.file_list"))

            success_count = 0
            for fid in file_ids:
                result = assign_file_classification(
                    db, fid, doc_types_raw, counterparty, fuzzy_threshold
                )
                if "error" in result:
                    flash(f"File #{fid}: {result['error']}", "error")
                else:
                    success_count += 1

            if success_count:
                types_str = ", ".join(doc_types_raw)
                flash(
                    f"Bulk assigned {success_count} file(s) as [{types_str}] to {counterparty or 'existing'}",
                    "success",
                )

        elif action == "mark_delivered":
            cp_id = request.form.get("counterparty_id", type=int)
            if cp_id:
                from web.services.file_service import mark_counterparty_delivered
                result = mark_counterparty_delivered(db, cp_id)
                if "error" in result:
                    flash(result["error"], "error")
                else:
                    flash(f"Marked {result['name']} as delivered.", "success")

        elif action == "merge":
            target_id = request.form.get("target_id", type=int)
            source_ids = request.form.getlist("source_ids", type=int)

            if not target_id or not source_ids:
                flash("Target and at least one source counterparty are required.", "error")
                return redirect(url_for("file_mgmt.file_list"))

            for source_id in source_ids:
                result = merge_counterparties(db, target_id, source_id)
                if "error" in result:
                    flash(result["error"], "error")
                else:
                    flash(
                        f"Merged #{result['source_id']} ({result['source_name']}) "
                        f"into #{result['target_id']} ({result['target_name']})",
                        "success",
                    )

        return redirect(url_for("file_mgmt.file_list"))
    finally:
        db.close()


@bp.route("/files/<int:file_id>/download")
def file_download(file_id):
    """Download a file."""
    db = get_db_from_app(current_app)
    try:
        detail = get_file_detail(db, file_id)
        if not detail:
            abort(404)

        file_path = Path(detail["file_path"])
        if not is_safe_path(current_app, file_path):
            abort(403)
        if not file_path.exists():
            flash(f"File not found on disk: {file_path}", "error")
            return redirect(url_for("file_mgmt.file_list"))

        return send_file(
            file_path,
            as_attachment=True,
            download_name=detail["original_filename"],
        )
    finally:
        db.close()


@bp.route("/files/<int:file_id>/view")
def file_view(file_id):
    """View a file inline in the browser (PDF, images)."""
    db = get_db_from_app(current_app)
    try:
        detail = get_file_detail(db, file_id)
        if not detail:
            abort(404)

        file_path = Path(detail["file_path"])
        if not is_safe_path(current_app, file_path):
            abort(403)
        if not file_path.exists():
            flash(f"File not found on disk: {file_path}", "error")
            return redirect(url_for("file_mgmt.file_list"))

        return send_file(file_path, as_attachment=False)
    finally:
        db.close()
