"""Counterparty tracking and fuzzy name matching."""

import json
import logging
import re

from rapidfuzz import fuzz

from database.connection import DatabaseManager

logger = logging.getLogger(__name__)


def slugify(name: str) -> str:
    """Convert company name to a filesystem-safe slug."""
    slug = name.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_-]+", "_", slug)
    return slug.strip("_")[:80]


def find_or_create_counterparty(
    db: DatabaseManager,
    company_name: str,
    fuzzy_threshold: int = 85,
) -> int:
    """Find existing counterparty by fuzzy name match, or create new one.

    Returns counterparty ID.
    """
    if not company_name or company_name.lower() == "unknown":
        # Create/find a special "unknown" counterparty
        rows = db.execute("SELECT id FROM counterparties WHERE slug = 'unknown'")
        if rows:
            return rows[0]["id"]
        return db.execute_insert(
            """INSERT INTO counterparties (name, slug, aliases, status)
               VALUES ('Unknown', 'unknown', '[]', 'in_progress')""",
        )

    # Search existing counterparties
    all_counterparties = db.execute(
        "SELECT id, name, slug, aliases FROM counterparties"
    )

    best_match_id = None
    best_score = 0

    for cp in all_counterparties:
        # Check main name
        score = fuzz.token_sort_ratio(company_name.lower(), cp["name"].lower())
        if score > best_score:
            best_score = score
            best_match_id = cp["id"]

        # Check aliases
        aliases = json.loads(cp["aliases"]) if cp["aliases"] else []
        for alias in aliases:
            alias_score = fuzz.token_sort_ratio(company_name.lower(), alias.lower())
            if alias_score > best_score:
                best_score = alias_score
                best_match_id = cp["id"]

    if best_score >= fuzzy_threshold and best_match_id is not None:
        logger.info(
            "Matched '%s' to existing counterparty #%d (score=%d)",
            company_name, best_match_id, best_score,
        )
        # Add as alias if it's a new variant
        _add_alias(db, best_match_id, company_name)
        return best_match_id

    # Create new counterparty
    slug = slugify(company_name)
    # Ensure unique slug
    existing = db.execute("SELECT id FROM counterparties WHERE slug = ?", (slug,))
    if existing:
        slug = f"{slug}_{len(all_counterparties) + 1}"

    cp_id = db.execute_insert(
        """INSERT INTO counterparties (name, slug, aliases, status)
           VALUES (?, ?, ?, 'in_progress')""",
        (company_name, slug, json.dumps([company_name])),
    )

    # Initialize checklist for this counterparty
    _init_checklist(db, cp_id)

    logger.info("Created new counterparty #%d: %s (slug=%s)", cp_id, company_name, slug)
    return cp_id


def _add_alias(db: DatabaseManager, counterparty_id: int, name: str):
    """Add a name variant as alias if not already present."""
    rows = db.execute(
        "SELECT aliases FROM counterparties WHERE id = ?", (counterparty_id,)
    )
    if not rows:
        return
    aliases = json.loads(rows[0]["aliases"]) if rows[0]["aliases"] else []
    # Check if already similar to an existing alias
    for alias in aliases:
        if fuzz.token_sort_ratio(name.lower(), alias.lower()) >= 95:
            return
    aliases.append(name)
    db.execute(
        "UPDATE counterparties SET aliases = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (json.dumps(aliases), counterparty_id),
    )


def _init_checklist(db: DatabaseManager, counterparty_id: int):
    """Initialize the document checklist for a new counterparty."""
    doc_types = db.execute("SELECT id FROM kyc_doc_types")
    for dt in doc_types:
        db.execute_insert(
            """INSERT OR IGNORE INTO counterparty_checklist
               (counterparty_id, doc_type_id, status)
               VALUES (?, ?, 'missing')""",
            (counterparty_id, dt["id"]),
        )


def update_checklist(
    db: DatabaseManager,
    counterparty_id: int,
    doc_type_id: int,
    file_id: int,
):
    """Mark a document as received in the counterparty's checklist."""
    db.execute(
        """UPDATE counterparty_checklist
           SET status = 'received', file_id = ?, updated_at = CURRENT_TIMESTAMP
           WHERE counterparty_id = ? AND doc_type_id = ?""",
        (file_id, counterparty_id, doc_type_id),
    )
    logger.info(
        "Updated checklist: counterparty #%d, doc_type #%d -> received (file #%d)",
        counterparty_id, doc_type_id, file_id,
    )


def get_counterparty_status(db: DatabaseManager, counterparty_id: int) -> dict:
    """Get the full status of a counterparty's KYC documents."""
    rows = db.execute(
        """SELECT cl.status as cl_status, dt.code, dt.name_en, dt.required,
                  sf.original_filename
           FROM counterparty_checklist cl
           JOIN kyc_doc_types dt ON cl.doc_type_id = dt.id
           LEFT JOIN submitted_files sf ON cl.file_id = sf.id
           WHERE cl.counterparty_id = ?
           ORDER BY dt.sort_order""",
        (counterparty_id,),
    )

    cp = db.execute("SELECT * FROM counterparties WHERE id = ?", (counterparty_id,))
    if not cp:
        return {}

    docs = []
    received = 0
    required_total = 0
    required_received = 0

    # Find files that serve multiple doc types (shared files)
    shared_files = {}  # file_id -> list of doc_type codes
    for row in rows:
        if row["cl_status"] in ("received", "verified") and row["original_filename"]:
            fname = row["original_filename"]
            shared_files.setdefault(fname, []).append(row["code"])

    for row in rows:
        # Check if this file also serves other types
        also_serves = []
        if row["original_filename"] and row["original_filename"] in shared_files:
            also_serves = [
                c for c in shared_files[row["original_filename"]]
                if c != row["code"]
            ]

        doc = {
            "code": row["code"],
            "name": row["name_en"],
            "required": bool(row["required"]),
            "status": row["cl_status"],
            "file": row["original_filename"],
            "also_serves": also_serves,
        }
        docs.append(doc)
        if row["cl_status"] in ("received", "verified"):
            received += 1
            if row["required"]:
                required_received += 1
        if row["required"]:
            required_total += 1

    return {
        "counterparty_id": counterparty_id,
        "name": cp[0]["name"],
        "slug": cp[0]["slug"],
        "status": cp[0]["status"],
        "documents": docs,
        "progress": f"{required_received}/{required_total} required",
        "total_received": received,
        "is_complete": required_received >= required_total,
    }
