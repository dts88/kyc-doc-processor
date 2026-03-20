"""Package completed counterparty KYC documents for delivery."""

import json
import logging
import re
import shutil
from datetime import datetime
from pathlib import Path

from classification.doc_types import DOC_TYPES, ID_TO_CODE
from database.connection import DatabaseManager

logger = logging.getLogger(__name__)

# Pattern matching conversion artifacts (e.g. "file_page_1.png")
_PAGE_IMAGE_RE = re.compile(r"_page_\d+\.png$", re.IGNORECASE)


def _build_type_prefix(doc_type_codes: list[str]) -> str:
    """Build a numeric prefix from document type codes.

    Each doc type has a sort_order (01-99). If a file belongs to multiple
    types, all numbers are concatenated, e.g. "010203".

    Returns the prefix string (e.g. "01", "010203").
    """
    sort_orders = []
    for code in doc_type_codes:
        dt = DOC_TYPES.get(code)
        if dt:
            sort_orders.append(dt.sort_order)
    sort_orders.sort()
    return "".join(f"{n:02d}" for n in sort_orders)


def _get_file_type_map(db: DatabaseManager, counterparty_id: int) -> dict[int, list[str]]:
    """Query all doc type codes for each file belonging to this counterparty.

    Returns {file_id: [doc_type_code, ...]} sorted by sort_order.
    """
    rows = db.execute(
        """SELECT dc.file_id, dt.code, dt.sort_order
           FROM document_classifications dc
           JOIN kyc_doc_types dt ON dc.doc_type_id = dt.id
           WHERE dc.counterparty_id = ?
           ORDER BY dc.file_id, dt.sort_order""",
        (counterparty_id,),
    )
    file_types: dict[int, list[str]] = {}
    for r in rows:
        file_types.setdefault(r["file_id"], []).append(r["code"])
    return file_types


def _get_file_paths(db: DatabaseManager, counterparty_id: int) -> dict[int, dict]:
    """Get file path and original filename for each file of this counterparty."""
    rows = db.execute(
        """SELECT DISTINCT sf.id, sf.file_path, sf.original_filename
           FROM submitted_files sf
           JOIN document_classifications dc ON sf.id = dc.file_id
           WHERE dc.counterparty_id = ?""",
        (counterparty_id,),
    )
    return {r["id"]: r for r in rows}


def package_counterparty(
    db: DatabaseManager,
    counterparty_id: int,
    classified_dir: Path,
    completed_dir: Path,
) -> str | None:
    """Create a zip package of all KYC documents for a completed counterparty.

    Files are placed flat (no sub-folders) with a numeric prefix derived from
    the document type sort_order.  If a file belongs to multiple types, all
    numbers are concatenated, e.g. ``010203_original_name.pdf``.

    Conversion artifacts (*_page_N.png) are excluded.

    Returns the package file path, or None if not ready.
    """
    cp = db.execute("SELECT * FROM counterparties WHERE id = ?", (counterparty_id,))
    if not cp:
        logger.error("Counterparty #%d not found", counterparty_id)
        return None

    cp = cp[0]
    slug = cp["slug"]

    # Build mapping: file_id -> [doc_type_codes]
    file_type_map = _get_file_type_map(db, counterparty_id)
    file_info_map = _get_file_paths(db, counterparty_id)

    if not file_info_map:
        logger.warning("No files to package for counterparty #%d", counterparty_id)
        return None

    # Create flat package directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    package_name = f"{slug}_{timestamp}"
    package_dir = completed_dir / package_name
    package_dir.mkdir(parents=True, exist_ok=True)

    total_size = 0
    file_count = 0
    doc_list = []
    seen_names: set[str] = set()  # guard against duplicate filenames after rename

    for file_id, info in sorted(file_info_map.items()):
        src = Path(info["file_path"])

        # Skip conversion artifacts and missing files
        if _PAGE_IMAGE_RE.search(src.name) or not src.exists():
            continue

        # Build prefix from doc type sort_orders
        type_codes = file_type_map.get(file_id, [])
        prefix = _build_type_prefix(type_codes)

        # Compose new filename: prefix + "_" + original filename
        original_name = info["original_filename"]
        new_name = f"{prefix}_{original_name}" if prefix else original_name

        # Handle potential duplicate names
        base_name = new_name
        counter = 1
        while new_name in seen_names:
            stem = Path(base_name).stem
            suffix = Path(base_name).suffix
            new_name = f"{stem}_{counter}{suffix}"
            counter += 1
        seen_names.add(new_name)

        dest = package_dir / new_name
        shutil.copy2(str(src), str(dest))
        total_size += dest.stat().st_size
        file_count += 1
        doc_list.append({
            "original_filename": original_name,
            "packaged_filename": new_name,
            "doc_types": type_codes,
        })

    if file_count == 0:
        logger.warning("No files to package for counterparty #%d", counterparty_id)
        shutil.rmtree(str(package_dir))
        return None

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
        """UPDATE submitted_files SET status = 'packaged', error_message = NULL, updated_at = CURRENT_TIMESTAMP
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
