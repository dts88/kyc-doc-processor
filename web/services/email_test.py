"""Test email sending via SMTP settings from the database."""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_test_email(smtp_config: dict, recipient: str) -> tuple[bool, str]:
    """Send a test email to verify SMTP settings.

    Returns (success, message).
    """
    if not smtp_config.get("smtp_host"):
        return False, "SMTP host not configured"

    subject = "KYC System - SMTP Test Email"
    body = """
    <html>
    <body>
    <h2>SMTP Configuration Test</h2>
    <p>This is a test email from the KYC Document Processor.</p>
    <p>If you received this email, your SMTP settings are configured correctly.</p>
    <hr>
    <p style="color: #999; font-size: 12px;">KYC Document Processor - Settings Verification</p>
    </body>
    </html>
    """

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = smtp_config.get("smtp_from_address", smtp_config.get("smtp_user", ""))
        msg["To"] = recipient
        msg.attach(MIMEText(body, "html"))

        with smtplib.SMTP(smtp_config["smtp_host"], int(smtp_config.get("smtp_port", 587))) as server:
            server.starttls()
            server.login(smtp_config["smtp_user"], smtp_config["smtp_password"])
            server.send_message(msg)

        return True, f"Test email sent to {recipient}"

    except Exception as e:
        return False, f"Failed to send: {e}"
