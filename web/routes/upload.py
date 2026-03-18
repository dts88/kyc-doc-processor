"""File upload routes — trader-facing page for submitting KYC documents."""

import json
import logging
from pathlib import Path

from flask import (
    Blueprint, render_template, request, flash, redirect,
    url_for, current_app,
)

from web import get_db_from_app

logger = logging.getLogger(__name__)

bp = Blueprint("upload", __name__)

SUPPORTED_EXTENSIONS = {
    ".pdf", ".docx", ".xlsx",
    ".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp",
}

MAX_FILE_SIZE_MB = 50


@bp.route("/upload", methods=["GET"])
def upload_page():
    return render_template("upload.html", supported_ext=sorted(SUPPORTED_EXTENSIONS))


@bp.route("/upload", methods=["POST"])
def upload_files():
    uploader_name = request.form.get("uploader_name", "").strip()
    if not uploader_name:
        flash("Please enter your name before uploading.", "error")
        return redirect(url_for("upload.upload_page"))

    files = request.files.getlist("files")
    if not files or all(f.filename == "" for f in files):
        flash("No files selected.", "error")
        return redirect(url_for("upload.upload_page"))

    # Resolve inbox path from config
    import yaml
    project_root = Path(__file__).parent.parent.parent
    config_path = project_root / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    inbox_dir = project_root / config["paths"]["inbox"]
    inbox_dir.mkdir(parents=True, exist_ok=True)

    saved = []
    skipped = []

    for f in files:
        if not f.filename:
            continue

        # Check extension
        suffix = Path(f.filename).suffix.lower()
        if suffix not in SUPPORTED_EXTENSIONS:
            skipped.append(f"{f.filename} (unsupported type: {suffix})")
            continue

        # Check file size
        f.seek(0, 2)  # seek to end
        size_mb = f.tell() / (1024 * 1024)
        f.seek(0)  # reset
        if size_mb > MAX_FILE_SIZE_MB:
            skipped.append(f"{f.filename} (too large: {size_mb:.1f}MB, max {MAX_FILE_SIZE_MB}MB)")
            continue

        # Save to inbox — handle name collisions
        dest = inbox_dir / f.filename
        if dest.exists():
            stem = Path(f.filename).stem
            counter = 1
            while dest.exists():
                dest = inbox_dir / f"{stem}_{counter}{suffix}"
                counter += 1

        f.save(str(dest))
        saved.append(f.filename)
        logger.info("Upload by '%s': %s -> %s", uploader_name, f.filename, dest)

    # Audit log
    db = get_db_from_app(current_app)
    try:
        db.execute_insert(
            """INSERT INTO processing_log (stage, action, details)
               VALUES (?, ?, ?)""",
            ("intake", "upload_web", json.dumps({
                "uploader": uploader_name,
                "saved": saved,
                "skipped": skipped,
            }, ensure_ascii=False)),
        )
    finally:
        db.close()

    if saved:
        flash(f"Uploaded {len(saved)} file(s) by {uploader_name}: {', '.join(saved)}", "success")
    if skipped:
        flash(f"Skipped {len(skipped)} file(s): {', '.join(skipped)}", "error")

    return redirect(url_for("upload.upload_page"))
