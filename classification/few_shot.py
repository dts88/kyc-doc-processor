"""Build few-shot examples from classification corrections."""

import logging
from pathlib import Path

from database.connection import DatabaseManager

logger = logging.getLogger(__name__)

# Max chars of document content to store per correction
EXCERPT_MAX_CHARS = 1000


def get_few_shot_examples(db: DatabaseManager) -> str:
    """Return a formatted few-shot block from stored corrections.

    Each example includes a content excerpt so Claude can learn from actual
    document content patterns rather than just filenames.

    Returns an empty string when there are no corrections.
    """
    rows = db.execute(
        """SELECT original_filename, content_excerpt, machine_doc_type, correct_doc_type
           FROM classification_corrections
           ORDER BY created_at DESC
           LIMIT 20""",
    )

    if not rows:
        return ""

    examples: list[str] = []
    for row in rows:
        machine = row["machine_doc_type"] or "unknown"
        correct = row["correct_doc_type"]

        if machine == correct:
            continue

        entry = f'- File: "{row["original_filename"]}" was classified as [{machine}], correct type is [{correct}].'
        if row["content_excerpt"]:
            entry += f'\n  Content excerpt: """{row["content_excerpt"]}"""'
        examples.append(entry)

    if not examples:
        return ""

    header = "## Previous Classification Corrections (learn from these)\n"
    body = "\n".join(examples)
    return f"{header}{body}\n\n"


def record_correction(
    db: DatabaseManager,
    original_filename: str,
    machine_doc_type: str | None,
    correct_doc_type: str,
    file_path: Path | str | None = None,
):
    """Record a classification correction for future few-shot learning.

    If file_path is provided, converts the file to extract a content excerpt.
    Duplicate corrections (same filename + same correct type) are skipped.
    """
    existing = db.execute(
        """SELECT id FROM classification_corrections
           WHERE original_filename = ? AND correct_doc_type = ?""",
        (original_filename, correct_doc_type),
    )
    if existing:
        return

    # Extract content excerpt from the actual file
    content_excerpt = None
    if file_path:
        content_excerpt = _extract_excerpt(Path(file_path))

    db.execute_insert(
        """INSERT INTO classification_corrections
           (original_filename, content_excerpt, machine_doc_type, correct_doc_type)
           VALUES (?, ?, ?, ?)""",
        (original_filename, content_excerpt, machine_doc_type, correct_doc_type),
    )
    logger.info(
        "Recorded correction: '%s' [%s] -> [%s] (excerpt: %d chars)",
        original_filename, machine_doc_type or "unknown", correct_doc_type,
        len(content_excerpt) if content_excerpt else 0,
    )


def _extract_excerpt(file_path: Path) -> str | None:
    """Convert file and return a truncated text excerpt for learning."""
    if not file_path.exists():
        return None

    try:
        from processing.file_converter import convert_file
        result = convert_file(file_path)

        if result.text_content:
            excerpt = result.text_content[:EXCERPT_MAX_CHARS].strip()
            # Clean up conversion artifacts (images generated during conversion)
            if result.image_paths:
                for img in result.image_paths:
                    try:
                        if img.exists() and img.resolve() != file_path.resolve():
                            img.unlink()
                    except Exception:
                        pass
            return excerpt if excerpt else None

        return None
    except Exception as e:
        logger.warning("Could not extract content for few-shot: %s", e)
        return None
