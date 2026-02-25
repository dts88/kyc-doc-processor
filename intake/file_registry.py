"""File registry with SHA-256 deduplication."""

import hashlib
import logging
import shutil
from pathlib import Path

from database.connection import DatabaseManager

logger = logging.getLogger(__name__)


def compute_file_hash(file_path: str | Path) -> str:
    """Compute SHA-256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def is_duplicate(db: DatabaseManager, file_hash: str) -> bool:
    """Check if a file with this hash has already been submitted."""
    rows = db.execute(
        "SELECT id FROM submitted_files WHERE file_hash = ? AND status != 'error'",
        (file_hash,),
    )
    return len(rows) > 0


def register_file(
    db: DatabaseManager,
    file_path: Path,
    processing_dir: Path,
) -> int | None:
    """Register a new file: compute hash, check duplicate, move to processing, insert record.

    Returns the submitted_file ID, or None if duplicate.
    """
    file_hash = compute_file_hash(file_path)

    if is_duplicate(db, file_hash):
        logger.info("Duplicate file detected (hash=%s): %s — deleting", file_hash[:12], file_path.name)
        file_path.unlink(missing_ok=True)
        return None

    # Detect MIME type
    mime_type = _detect_mime_type(file_path)

    # Move to processing directory
    processing_dir.mkdir(parents=True, exist_ok=True)
    dest = processing_dir / file_path.name
    # Handle name collisions
    if dest.exists():
        stem = file_path.stem
        suffix = file_path.suffix
        counter = 1
        while dest.exists():
            dest = processing_dir / f"{stem}_{counter}{suffix}"
            counter += 1

    shutil.move(str(file_path), str(dest))
    logger.info("Moved %s to processing: %s", file_path.name, dest)

    # Insert into database
    file_id = db.execute_insert(
        """INSERT INTO submitted_files
           (original_filename, file_path, file_size, file_hash, mime_type, status)
           VALUES (?, ?, ?, ?, ?, 'pending')""",
        (file_path.name, str(dest), dest.stat().st_size, file_hash, mime_type),
    )

    # Log intake
    db.execute_insert(
        """INSERT INTO processing_log (file_id, stage, action, details)
           VALUES (?, 'intake', 'registered', ?)""",
        (file_id, f'{{"original_path": "{file_path}", "hash": "{file_hash}"}}'),
    )

    logger.info("Registered file #%d: %s (hash=%s)", file_id, file_path.name, file_hash[:12])
    return file_id


def _detect_mime_type(file_path: Path) -> str:
    """Detect MIME type using python-magic if available, else fallback."""
    try:
        import magic
        return magic.from_file(str(file_path), mime=True)
    except ImportError:
        # Fallback based on extension
        ext_map = {
            ".pdf": "application/pdf",
            ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".tiff": "image/tiff",
            ".tif": "image/tiff",
            ".bmp": "image/bmp",
        }
        return ext_map.get(file_path.suffix.lower(), "application/octet-stream")
