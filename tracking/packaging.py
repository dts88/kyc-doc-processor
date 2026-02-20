"""Package completed counterparty KYC documents for delivery."""

import json
import logging
import shutil
from datetime import datetime
from pathlib import Path

from database.connection import DatabaseManager

logger = logging.getLogger(__name__)


def package_counterparty(
    db: DatabaseManager,
    counterparty_id: int,
    classified_dir: Path,
    completed_dir: Path,
) -> str | None:
    """Create a zip package of all KYC documents for a completed counterparty.

    Returns the package file path, or None if not ready.
    """
    # Get counterparty info
    cp = db.execute("SELECT * FROM counterparties WHERE id = ?", (counterparty_id,))
    if not cp:
        logger.error("Counterparty #%d not found", counterparty_id)
        return None

    cp = cp[0]
    slug = cp["slug"]

    # Get all received files
    files = db.execute(
        """SELECT sf.file_path, sf.original_filename, dt.code, dt.name_en
           FROM counterparty_checklist cl
           JOIN submitted_files sf ON cl.file_id = sf.id
           JOIN kyc_doc_types dt ON cl.doc_type_id = dt.id
           WHERE cl.counterparty_id = ? AND cl.status IN ('received', 'verified')
           ORDER BY dt.sort_order""",
        (counterparty_id,),
    )

    if not files:
        logger.warning("No files to package for counterparty #%d", counterparty_id)
        return None

    # Create package directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    package_name = f"{slug}_{timestamp}"
    package_dir = completed_dir / package_name
    package_dir.mkdir(parents=True, exist_ok=True)

    total_size = 0
    file_count = 0

    # Copy files into package
    for f in files:
        src = Path(f["file_path"])
        if not src.exists():
            logger.warning("Source file missing: %s", src)
            continue

        # Create subfolder by document type
        doc_folder = package_dir / f["code"]
        doc_folder.mkdir(exist_ok=True)

        dest = doc_folder / f["original_filename"]
        shutil.copy2(str(src), str(dest))
        total_size += dest.stat().st_size
        file_count += 1

    # Create manifest
    manifest = {
        "counterparty": cp["name"],
        "counterparty_id": counterparty_id,
        "packaged_at": datetime.now().isoformat(),
        "file_count": file_count,
        "documents": [
            {"type": f["code"], "name": f["name_en"], "file": f["original_filename"]}
            for f in files
        ],
    }
    manifest_path = package_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))

    # Create zip archive
    zip_path = shutil.make_archive(str(package_dir), "zip", str(package_dir))

    # Record in database
    db.execute_insert(
        """INSERT INTO completed_packages
           (counterparty_id, package_path, file_count, total_size)
           VALUES (?, ?, ?, ?)""",
        (counterparty_id, zip_path, file_count, total_size),
    )

    # Update counterparty status
    db.execute(
        "UPDATE counterparties SET status = 'completed', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (counterparty_id,),
    )

    # Log
    db.execute_insert(
        """INSERT INTO processing_log (counterparty_id, stage, action, details)
           VALUES (?, 'packaging', 'packaged', ?)""",
        (counterparty_id, json.dumps({"zip_path": zip_path, "files": file_count})),
    )

    logger.info(
        "Packaged counterparty '%s': %d files, %s",
        cp["name"], file_count, zip_path,
    )

    # Clean up unzipped package dir
    shutil.rmtree(str(package_dir))

    return zip_path
