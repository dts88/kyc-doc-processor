"""Weekly report generation in HTML format."""

import json
import logging
from datetime import datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from database.connection import DatabaseManager
from tracking.completion_checker import get_all_counterparty_statuses

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent / "templates"


def generate_weekly_report(db: DatabaseManager, reports_dir: Path) -> str:
    """Generate a weekly KYC status report as HTML.

    Returns the path to the generated report file.
    """
    reports_dir.mkdir(parents=True, exist_ok=True)

    # Gather data
    statuses = get_all_counterparty_statuses(db)

    # Count stats
    total = len(statuses)
    completed = sum(1 for s in statuses if s["status"] in ("completed", "delivered"))
    in_progress = sum(1 for s in statuses if s["status"] == "in_progress")

    # Files submitted this week
    files_this_week_rows = db.execute(
        """SELECT COUNT(*) as cnt FROM submitted_files
           WHERE created_at >= datetime('now', '-7 days')
           AND status != 'duplicate'"""
    )
    files_this_week = files_this_week_rows[0]["cnt"] if files_this_week_rows else 0

    # Files needing review
    review_rows = db.execute(
        """SELECT id, original_filename, error_message, created_at
           FROM submitted_files
           WHERE status IN ('needs_review', 'error')
           ORDER BY created_at DESC"""
    )
    needs_review = [
        {
            "id": r["id"],
            "filename": r["original_filename"],
            "error": r["error_message"] or "Needs manual review",
            "created_at": r["created_at"],
        }
        for r in review_rows
    ]

    # Recent packages
    pkg_rows = db.execute(
        """SELECT cp.name, p.file_count, p.created_at, p.delivered
           FROM completed_packages p
           JOIN counterparties cp ON p.counterparty_id = cp.id
           ORDER BY p.created_at DESC LIMIT 20"""
    )
    recent_packages = [
        {
            "name": r["name"],
            "file_count": r["file_count"],
            "created_at": r["created_at"],
            "delivered": bool(r["delivered"]),
        }
        for r in pkg_rows
    ]

    # Document type short labels for table header (must match kyc_doc_types sort_order)
    doc_types = [
        {"short": "BF", "name": "Bizfile"},
        {"short": "IC", "name": "Incorporation"},
        {"short": "MA", "name": "M&A"},
        {"short": "SD", "name": "Shareholders & Directors"},
        {"short": "OS", "name": "Ownership Structure"},
        {"short": "FR", "name": "Financial Reports"},
        {"short": "BR", "name": "Bank Reference"},
        {"short": "KQ", "name": "KYC Questionnaire"},
        {"short": "OB", "name": "Onboarding Form"},
        {"short": "SF", "name": "Source of Funds"},
    ]

    # Render HTML
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    template = env.get_template("weekly_report.html")

    report_date = datetime.now().strftime("%Y-%m-%d")
    html = template.render(
        report_date=report_date,
        total_counterparties=total,
        completed_count=completed,
        in_progress_count=in_progress,
        files_this_week=files_this_week,
        counterparties=statuses,
        doc_types=doc_types,
        needs_review=needs_review,
        recent_packages=recent_packages,
        generated_at=datetime.now().isoformat(),
    )

    # Write report file
    report_filename = f"kyc_report_{report_date}.html"
    report_path = reports_dir / report_filename
    report_path.write_text(html, encoding="utf-8")

    # Record in database
    summary = {
        "total_counterparties": total,
        "completed": completed,
        "in_progress": in_progress,
        "files_this_week": files_this_week,
        "needs_review": len(needs_review),
    }
    db.execute_insert(
        """INSERT INTO weekly_reports (report_date, report_path, summary)
           VALUES (?, ?, ?)""",
        (report_date, str(report_path), json.dumps(summary)),
    )

    logger.info("Generated weekly report: %s", report_path)
    return str(report_path)
