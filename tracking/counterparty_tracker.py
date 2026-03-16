"""Counterparty tracking and fuzzy name matching."""

import json
import logging
import re
import unicodedata

from rapidfuzz import fuzz

from database.connection import DatabaseManager

logger = logging.getLogger(__name__)

# Full-width to half-width punctuation mapping for CJK normalization
_FULLWIDTH_MAP = str.maketrans({
    "（": "(", "）": ")", "【": "[", "】": "]", "｛": "{", "｝": "}",
    "，": ",", "。": ".", "；": ";", "：": ":", "\u201c": '"', "\u201d": '"',
    "\u2018": "'", "\u2019": "'", "！": "!", "？": "?", "、": "/",
})


def _normalize_cjk(text: str) -> str:
    """Normalize CJK text for better fuzzy matching.

    - Full-width punctuation → half-width
    - NFKC unicode normalization (e.g. ﬁ → fi, ２ → 2)
    - Collapse whitespace
    """
    text = unicodedata.normalize("NFKC", text)
    text = text.translate(_FULLWIDTH_MAP)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _split_cjk_latin(text: str) -> tuple[str, str]:
    """Split text into CJK portion and Latin/ASCII portion.

    Returns (cjk_part, latin_part). Either may be empty.
    """
    cjk_chars = []
    latin_chars = []
    for ch in text:
        if '\u4e00' <= ch <= '\u9fff' or '\u3400' <= ch <= '\u4dbf' or '\uf900' <= ch <= '\ufaff':
            cjk_chars.append(ch)
        elif ch.isascii() or ch in " -_.":
            latin_chars.append(ch)
        else:
            # Other scripts (keep in both for safety)
            cjk_chars.append(ch)
            latin_chars.append(ch)
    return "".join(cjk_chars).strip(), "".join(latin_chars).strip()


def _fuzzy_score(name_a: str, name_b: str) -> float:
    """Compute fuzzy match score with CJK awareness.

    For mixed CJK/Latin names, computes scores on each portion separately
    and returns the higher score.
    """
    a_norm = _normalize_cjk(name_a.lower())
    b_norm = _normalize_cjk(name_b.lower())

    # Standard token_sort_ratio on full normalized text
    full_score = fuzz.token_sort_ratio(a_norm, b_norm)

    # If both contain CJK characters, also try matching CJK portions separately
    a_cjk, a_latin = _split_cjk_latin(a_norm)
    b_cjk, b_latin = _split_cjk_latin(b_norm)

    partial_scores = [full_score]

    if a_cjk and b_cjk:
        partial_scores.append(fuzz.token_sort_ratio(a_cjk, b_cjk))
    if a_latin and b_latin and len(a_latin) >= 3 and len(b_latin) >= 3:
        partial_scores.append(fuzz.token_sort_ratio(a_latin, b_latin))

    return max(partial_scores)


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
        raise ValueError("Cannot create counterparty with unknown company name — file should be flagged for review")

    # Search existing counterparties
    all_counterparties = db.execute(
        "SELECT id, name, slug, aliases FROM counterparties"
    )

    # Exact slug match — catches cases where the input is a slug-style name
    input_slug = slugify(company_name)
    for cp in all_counterparties:
        if cp["slug"] == input_slug:
            _add_alias(db, cp["id"], company_name)
            _ensure_checklist(db, cp["id"])
            return cp["id"]

    best_match_id = None
    best_score = 0

    for cp in all_counterparties:
        # Check main name (CJK-aware fuzzy matching)
        score = _fuzzy_score(company_name, cp["name"])
        if score > best_score:
            best_score = score
            best_match_id = cp["id"]

        # Check aliases
        aliases = json.loads(cp["aliases"]) if cp["aliases"] else []
        for alias in aliases:
            alias_score = _fuzzy_score(company_name, alias)
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
        _ensure_checklist(db, best_match_id)
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
    # Check if already similar to an existing alias (CJK-aware)
    for alias in aliases:
        if _fuzzy_score(name, alias) >= 95:
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


def _ensure_checklist(db: DatabaseManager, counterparty_id: int):
    """Ensure checklist rows exist for an existing counterparty.

    Uses INSERT OR IGNORE so it's safe to call repeatedly — only missing
    rows are created.
    """
    rows = db.execute(
        "SELECT COUNT(*) AS cnt FROM counterparty_checklist WHERE counterparty_id = ?",
        (counterparty_id,),
    )
    if rows[0]["cnt"] == 0:
        _init_checklist(db, counterparty_id)
        logger.info("Re-initialized checklist for counterparty #%d", counterparty_id)


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
        "is_complete": required_total > 0 and required_received >= required_total,
    }
