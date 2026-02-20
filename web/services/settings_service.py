"""CRUD operations for app_settings table (SMTP config etc.)."""

from database.connection import DatabaseManager

SMTP_KEYS = ("smtp_host", "smtp_port", "smtp_user", "smtp_password", "smtp_from_address")


def get_setting(db: DatabaseManager, key: str) -> str | None:
    rows = db.execute("SELECT value FROM app_settings WHERE key = ?", (key,))
    return rows[0]["value"] if rows else None


def set_setting(db: DatabaseManager, key: str, value: str) -> None:
    with db.get_cursor() as cur:
        cur.execute(
            """INSERT INTO app_settings (key, value, updated_at)
               VALUES (?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP""",
            (key, value),
        )


def get_smtp_config(db: DatabaseManager) -> dict:
    """Return SMTP settings as a dict. Missing keys become empty strings."""
    result = {}
    for key in SMTP_KEYS:
        result[key] = get_setting(db, key) or ""
    return result


def save_smtp_config(db: DatabaseManager, data: dict) -> None:
    """Save SMTP settings from a form submission."""
    for key in SMTP_KEYS:
        if key in data:
            set_setting(db, key, data[key])


def is_smtp_configured(db: DatabaseManager) -> bool:
    host = get_setting(db, "smtp_host")
    return bool(host and host.strip())


# --- KYC Team CRUD ---

def get_kyc_team(db: DatabaseManager) -> list[dict]:
    return db.execute("SELECT * FROM kyc_team ORDER BY added_at")


def add_kyc_member(db: DatabaseManager, name: str, email: str, role: str = "kyc_reviewer") -> int:
    return db.execute_insert(
        "INSERT INTO kyc_team (name, email, role) VALUES (?, ?, ?)",
        (name, email, role),
    )


def delete_kyc_member(db: DatabaseManager, member_id: int) -> None:
    with db.get_cursor() as cur:
        cur.execute("DELETE FROM kyc_team WHERE id = ?", (member_id,))


def get_kyc_team_emails(db: DatabaseManager) -> list[str]:
    rows = db.execute("SELECT email FROM kyc_team ORDER BY id")
    return [r["email"] for r in rows]
