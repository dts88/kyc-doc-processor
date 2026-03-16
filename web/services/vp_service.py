"""CRUD operations for VP emails and VP signatures."""

import json
import logging

from database.connection import DatabaseManager

logger = logging.getLogger(__name__)


def _audit_log(db: DatabaseManager, action: str, details: dict) -> None:
    """Write an audit entry to processing_log for VP changes."""
    try:
        db.execute_insert(
            """INSERT INTO processing_log (stage, action, details)
               VALUES (?, ?, ?)""",
            ("settings", action, json.dumps(details, ensure_ascii=False)),
        )
    except Exception as e:
        logger.warning("Failed to write audit log: %s", e)


# --- VP Emails ---

def get_vp_emails(db: DatabaseManager) -> list[dict]:
    return db.execute("SELECT * FROM vp_emails ORDER BY added_at")


def add_vp_email(db: DatabaseManager, vp_name: str, email: str) -> int:
    vp_id = db.execute_insert(
        "INSERT INTO vp_emails (vp_name, email) VALUES (?, ?)",
        (vp_name, email),
    )
    _audit_log(db, "add_vp_email", {"vp_name": vp_name, "email": email})
    return vp_id


def delete_vp_email(db: DatabaseManager, vp_email_id: int) -> None:
    rows = db.execute("SELECT vp_name, email FROM vp_emails WHERE id = ?", (vp_email_id,))
    with db.get_cursor() as cur:
        cur.execute("DELETE FROM vp_emails WHERE id = ?", (vp_email_id,))
    if rows:
        _audit_log(db, "delete_vp_email", {"vp_name": rows[0]["vp_name"], "email": rows[0]["email"]})


# --- VP Signatures ---

def get_vp_signatures(db: DatabaseManager, active_only: bool = False) -> list[dict]:
    sql = "SELECT * FROM vp_signatures"
    if active_only:
        sql += " WHERE is_active = 1"
    sql += " ORDER BY verified_at DESC"
    return db.execute(sql)


def get_active_vp_names(db: DatabaseManager) -> list[str]:
    rows = db.execute("SELECT DISTINCT vp_name FROM vp_signatures WHERE is_active = 1")
    return [r["vp_name"] for r in rows]


def add_vp_signature(db: DatabaseManager, vp_name: str,
                     source_file_id: int | None = None,
                     source_description: str | None = None) -> int:
    sig_id = db.execute_insert(
        """INSERT INTO vp_signatures (vp_name, source_file_id, source_description)
           VALUES (?, ?, ?)""",
        (vp_name, source_file_id, source_description),
    )
    _audit_log(db, "add_vp_signature", {"vp_name": vp_name, "source": source_description})
    return sig_id


def deactivate_vp_signature(db: DatabaseManager, sig_id: int) -> None:
    rows = db.execute("SELECT vp_name FROM vp_signatures WHERE id = ?", (sig_id,))
    with db.get_cursor() as cur:
        cur.execute(
            "UPDATE vp_signatures SET is_active = 0 WHERE id = ?",
            (sig_id,),
        )
    if rows:
        _audit_log(db, "deactivate_vp_signature", {"vp_name": rows[0]["vp_name"], "sig_id": sig_id})


def get_signed_onboarding_forms(db: DatabaseManager) -> list[dict]:
    """Get onboarding forms that have vp_signed=true in extraction data."""
    rows = db.execute(
        """SELECT sf.id as file_id, sf.original_filename,
                  er.extracted_data, dc.detected_company_name
           FROM extraction_results er
           JOIN document_classifications dc ON er.classification_id = dc.id
           JOIN submitted_files sf ON er.file_id = sf.id
           JOIN kyc_doc_types dt ON dc.doc_type_id = dt.id
           WHERE dt.code = 'onboarding_form'
             AND er.validation_passed = 1
           ORDER BY sf.id DESC"""
    )
    import json
    result = []
    for r in rows:
        try:
            data = json.loads(r["extracted_data"])
        except (json.JSONDecodeError, TypeError):
            continue
        if data.get("vp_signed"):
            result.append({
                "file_id": r["file_id"],
                "filename": r["original_filename"],
                "company": r["detected_company_name"],
                "vp_name": data.get("vp_signature_name", ""),
            })
    return result
