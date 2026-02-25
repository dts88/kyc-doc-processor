"""Package completed counterparty KYC documents for delivery."""

import json
import logging
import re
import shutil
from datetime import datetime
from pathlib import Path

from database.connection import DatabaseManager

logger = logging.getLogger(__name__)

# Pattern matching conversion artifacts (e.g. "file_page_1.png")
_PAGE_IMAGE_RE = re.compile(r"_page_\d+\.png$", re.IGNORECASE)


def package_counterparty(
    db: DatabaseManager,
    counterparty_id: int,
    classified_dir: Path,
    completed_dir: Path,
) -> str | None:
    """Create a zip package of all KYC documents for a completed counterparty.

    Packages the entire classified/<slug>/ directory, preserving the folder
    structure (01_bizfile, 02_incorporation, etc.) and including ALL files
    per doc type.  Conversion artifacts (*_page_N.png) are excluded.

    Returns the package file path, or None if not ready.
    """
    cp = db.execute("SELECT * FROM counterparties WHERE id = ?", (counterparty_id,))
    if not cp:
        logger.error("Counterparty #%d not found", counterparty_id)
        return None

    cp = cp[0]
    slug = cp["slug"]
    source_dir = classified_dir / slug

    if not source_dir.exists():
        logger.warning("Classified directory not found: %s", source_dir)
        return None

    # Collect files, excluding conversion artifacts
    source_files = [
        f for f in source_dir.rglob("*")
        if f.is_file() and not _PAGE_IMAGE_RE.search(f.name)
    ]

    if not source_files:
        logger.warning("No files to package for counterparty #%d", counterparty_id)
        return None

    # Create package directory, mirroring classified structure
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    package_name = f"{slug}_{timestamp}"
    package_dir = completed_dir / package_name
    package_dir.mkdir(parents=True, exist_ok=True)

    total_size = 0
    file_count = 0
    doc_list = []

    for src in sorted(source_files):
        rel = src.relative_to(source_dir)
        dest = package_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src), str(dest))
        total_size += dest.stat().st_size
        file_count += 1
        doc_list.append({
            "folder": str(rel.parent),
            "file": src.name,
        })

    # Create manifest
    manifest = {
        "counterparty": cp["name"],
        "counterparty_id": counterparty_id,
        "slug": slug,
        "packaged_at": datetime.now().isoformat(),
        "file_count": file_count,
        "documents": doc_list,
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

    # Update file statuses to 'packaged' for all files belonging to this counterparty
    db.execute(
        """UPDATE submitted_files SET status = 'packaged', updated_at = CURRENT_TIMESTAMP
           WHERE id IN (
               SELECT DISTINCT dc.file_id FROM document_classifications dc
               WHERE dc.counterparty_id = ?
           )""",
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
