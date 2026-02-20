"""Email notification for completed KYC packages and review alerts."""

import json
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from database.connection import DatabaseManager

logger = logging.getLogger(__name__)


def _get_smtp_config_from_db(db: DatabaseManager) -> dict | None:
    """Try to load SMTP settings from app_settings table."""
    try:
        from web.services.settings_service import get_smtp_config, is_smtp_configured
        if is_smtp_configured(db):
            cfg = get_smtp_config(db)
            return {
                "host": cfg["smtp_host"],
                "port": int(cfg.get("smtp_port") or 587),
                "user": cfg["smtp_user"],
                "password": cfg["smtp_password"],
                "from_address": cfg["smtp_from_address"],
            }
    except Exception:
        pass
    return None


def _get_recipients_from_db(db: DatabaseManager) -> list[str]:
    """Get recipient list from kyc_team table."""
    try:
        from web.services.settings_service import get_kyc_team_emails
        emails = get_kyc_team_emails(db)
        if emails:
            return emails
    except Exception:
        pass
    return []


def _resolve_smtp_and_recipients(db: DatabaseManager, smtp_config: dict) -> tuple[dict, list[str]]:
    """Resolve SMTP config and recipients, preferring DB over config.yaml.

    Returns (smtp_dict, recipients_list).
    """
    # Try DB first
    db_smtp = _get_smtp_config_from_db(db)
    db_recipients = _get_recipients_from_db(db)

    if db_smtp and db_smtp.get("host"):
        smtp = db_smtp
    else:
        smtp = {
            "host": smtp_config.get("host", ""),
            "port": int(smtp_config.get("port", 587)),
            "user": smtp_config.get("user", ""),
            "password": smtp_config.get("password", ""),
            "from_address": smtp_config.get("from_address", ""),
        }

    recipients = db_recipients if db_recipients else smtp_config.get("recipients", [])
    # Filter empty strings
    recipients = [r for r in recipients if r and r.strip()]

    return smtp, recipients


def send_completion_notification(
    db: DatabaseManager,
    counterparty_id: int,
    package_path: str,
    smtp_config: dict,
) -> bool:
    """Send email notification that a counterparty's KYC package is ready.

    smtp_config should contain: host, port, user, password, from_address, recipients
    Returns True if sent successfully.
    """
    cp = db.execute("SELECT * FROM counterparties WHERE id = ?", (counterparty_id,))
    if not cp:
        return False
    cp = cp[0]

    smtp, recipients = _resolve_smtp_and_recipients(db, smtp_config)

    if not smtp.get("host"):
        logger.warning("SMTP not configured, skipping email notification")
        _log_notification(db, counterparty_id, package_path, sent=False, reason="SMTP not configured")
        return False

    if not recipients:
        logger.warning("No recipients configured, skipping email notification")
        _log_notification(db, counterparty_id, package_path, sent=False, reason="No recipients")
        return False

    subject = f"KYC Package Ready: {cp['name']}"
    body = f"""
    <html>
    <body>
    <h2>KYC Document Package Ready for Review</h2>
    <p>The KYC document package for <strong>{cp['name']}</strong> is complete and ready for review.</p>
    <p><strong>Package location:</strong> {package_path}</p>
    <p>All required KYC documents have been collected, classified, and packaged.</p>
    <p>Please review the package and proceed with the compliance assessment.</p>
    <hr>
    <p style="color: #999; font-size: 12px;">This is an automated notification from the KYC Document Processor.</p>
    </body>
    </html>
    """

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = smtp["from_address"]
        msg["To"] = ", ".join(recipients)
        msg.attach(MIMEText(body, "html"))

        with smtplib.SMTP(smtp["host"], smtp["port"]) as server:
            server.starttls()
            server.login(smtp["user"], smtp["password"])
            server.send_message(msg)

        logger.info("Sent completion notification for counterparty '%s'", cp["name"])
        _log_notification(db, counterparty_id, package_path, sent=True)
        return True

    except Exception as e:
        logger.error("Failed to send notification: %s", e)
        _log_notification(db, counterparty_id, package_path, sent=False, reason=str(e))
        return False


def send_review_notification(
    db: DatabaseManager,
    file_id: int,
    filename: str,
    reason: str,
    smtp_config: dict | None = None,
) -> bool:
    """Send notification to KYC team when a file needs manual review.

    Returns True if sent successfully.
    """
    smtp, recipients = _resolve_smtp_and_recipients(db, smtp_config or {})

    if not smtp.get("host") or not recipients:
        logger.info("Review notification skipped (SMTP or recipients not configured)")
        return False

    subject = f"KYC Review Required: {filename}"
    body = f"""
    <html>
    <body>
    <h2>File Requires Manual Review</h2>
    <p>A submitted file needs human verification:</p>
    <table style="border-collapse: collapse; margin: 15px 0;">
        <tr><td style="padding: 6px 12px; font-weight: bold;">File ID:</td><td style="padding: 6px 12px;">#{file_id}</td></tr>
        <tr><td style="padding: 6px 12px; font-weight: bold;">Filename:</td><td style="padding: 6px 12px;">{filename}</td></tr>
        <tr><td style="padding: 6px 12px; font-weight: bold;">Reason:</td><td style="padding: 6px 12px;">{reason}</td></tr>
    </table>
    <p><strong>Actions:</strong></p>
    <ul>
        <li>Use <code>python main.py assign {file_id} &lt;doc_type&gt; --counterparty &lt;name&gt;</code> to manually classify</li>
        <li>Use <code>python main.py reprocess {file_id}</code> to retry with Claude</li>
        <li>Or manage via the Web settings interface</li>
    </ul>
    <hr>
    <p style="color: #999; font-size: 12px;">This is an automated notification from the KYC Document Processor.</p>
    </body>
    </html>
    """

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = smtp["from_address"]
        msg["To"] = ", ".join(recipients)
        msg.attach(MIMEText(body, "html"))

        with smtplib.SMTP(smtp["host"], smtp["port"]) as server:
            server.starttls()
            server.login(smtp["user"], smtp["password"])
            server.send_message(msg)

        logger.info("Sent review notification for file #%d", file_id)
        _log_notification_generic(db, file_id, "review_notification", sent=True)
        return True

    except Exception as e:
        logger.error("Failed to send review notification: %s", e)
        _log_notification_generic(db, file_id, "review_notification", sent=False, reason=str(e))
        return False


def _log_notification(db: DatabaseManager, counterparty_id: int, package_path: str,
                      sent: bool, reason: str = ""):
    """Log notification attempt."""
    db.execute_insert(
        """INSERT INTO processing_log (counterparty_id, stage, action, details)
           VALUES (?, 'notification', ?, ?)""",
        (
            counterparty_id,
            "sent" if sent else "failed",
            json.dumps({"package": package_path, "reason": reason}),
        ),
    )


def _log_notification_generic(db: DatabaseManager, file_id: int, action: str,
                              sent: bool, reason: str = ""):
    """Log a generic notification attempt."""
    db.execute_insert(
        """INSERT INTO processing_log (file_id, stage, action, details)
           VALUES (?, 'notification', ?, ?)""",
        (
            file_id,
            action if sent else f"{action}_failed",
            json.dumps({"sent": sent, "reason": reason}),
        ),
    )
