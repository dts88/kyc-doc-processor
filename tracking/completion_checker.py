"""Check counterparty KYC document completeness."""

import logging

from database.connection import DatabaseManager
from tracking.counterparty_tracker import get_counterparty_status

logger = logging.getLogger(__name__)


def check_completion(db: DatabaseManager, counterparty_id: int) -> bool:
    """Check if a counterparty has all required KYC documents.

    Returns True if all 8 required documents are received/verified.
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


def get_newly_completed(db: DatabaseManager) -> list[int]:
    """Find counterparties that are newly completed (all required docs received)
    but not yet packaged.

    Returns list of counterparty IDs.
    """
    # Find counterparties in 'in_progress' status where all required docs are received
    counterparties = db.execute(
        "SELECT id FROM counterparties WHERE status = 'in_progress'"
    )

    newly_completed = []
    for cp in counterparties:
        if check_completion(db, cp["id"]):
            newly_completed.append(cp["id"])
            logger.info("Counterparty #%d is now complete!", cp["id"])

    return newly_completed
