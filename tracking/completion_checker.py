"""Check counterparty KYC document completeness."""

import logging

from database.connection import DatabaseManager
from tracking.counterparty_tracker import get_counterparty_status

logger = logging.getLogger(__name__)


def check_completion(db: DatabaseManager, counterparty_id: int) -> bool:
    """Check if a counterparty has all required KYC documents.

    Returns True if all 9 required documents are received/verified.
    """
    status = get_counterparty_status(db, counterparty_id)
    if not status:
        return False
    return status["is_complete"]


def get_all_counterparty_statuses(db: DatabaseManager) -> list[dict]:
    """Get status summaries for all counterparties."""
    counterparties = db.execute(
        "SELECT id FROM counterparties ORDER BY name"
    )
    return [get_counterparty_status(db, cp["id"]) for cp in counterparties]


def check_and_mark_pending_review(db: DatabaseManager) -> list[int]:
    """Find counterparties with all required docs and mark as pending_review.

    Only checks counterparties in 'in_progress' status.
    Returns list of counterparty IDs that were newly marked.
    """
    counterparties = db.execute(
        "SELECT id FROM counterparties WHERE status = 'in_progress'"
    )

    marked = []
    for cp in counterparties:
        if check_completion(db, cp["id"]):
            db.execute(
                "UPDATE counterparties SET status = 'pending_review', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (cp["id"],),
            )
            marked.append(cp["id"])
            logger.info("Counterparty #%d marked as pending_review (9/9 required docs)", cp["id"])

    return marked


def get_newly_completed(db: DatabaseManager) -> list[int]:
    """Deprecated — use check_and_mark_pending_review instead.

    Kept for backward compatibility. Now just calls check_and_mark_pending_review.
    """
    return check_and_mark_pending_review(db)
