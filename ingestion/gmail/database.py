"""
database.py — Gmail ingestion storage
Echo Personal Memory System

Thin layer between the Gmail poller and the storage engine.

    initialize_database()       is PostgreSQL reachable and the Gmail schema present?
    store_in_postgresql(data)   store one parsed email; True only when it is new

All writes go through ste.storage_engine.store_gmail_message, which stores the
memory item, gmail_metadata, engagement and attachment rows in ONE transaction.
This file writes no SQL of its own.

Callers (all sync, all off the event loop):
    router.poll_once (asyncio.to_thread)  initialize_database
    gmail_api.fetch_and_store_new_emails  store_in_postgresql
"""

import logging
import threading

from ste import postgresql_manager
from ste.storage_engine import store_gmail_message

logger = logging.getLogger(__name__)

# Last result of initialize_database, so its log fires only when the state
# changes. router.poll_once calls it every 30 s; logging each call would spam.
_db_ready: bool | None = None
_db_ready_lock = threading.Lock()


def _record_db_state(ready: bool, detail: str) -> None:
    global _db_ready
    with _db_ready_lock:
        changed = ready != _db_ready
        _db_ready = ready
    if not changed:
        return
    if ready:
        logger.info("Gmail storage ready: PostgreSQL reachable, gmail_metadata present")
    else:
        logger.error("Gmail storage unavailable: %s", detail)


def initialize_database() -> bool:
    """Return True when PostgreSQL answers and the gmail_metadata table exists.

    Read-only check: it creates nothing (scripts/setup_db.py owns the schema).
    """
    try:
        found = postgresql_manager.scalar(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'gmail_metadata'
            LIMIT 1
            """
        )
    except Exception as exc:
        _record_db_state(False, f"PostgreSQL connection error: {exc}")
        return False

    if found is None:
        # Reachable, but the schema is missing — storing would fail on every email.
        _record_db_state(False, "gmail_metadata table not found (run scripts/setup_db.py)")
        return False

    _record_db_state(True, "")
    return True


def store_in_postgresql(data: dict) -> bool:
    """Store one parsed email. Returns True only when it was newly inserted.

    False means either already stored (normal: every poll re-sees recent
    emails) or a storage error, which is logged with its traceback. The caller
    counts True results as "processed".
    """
    try:
        _memory_id, inserted = store_gmail_message(data)
    except Exception:
        # No body in the log — the email id is enough to find it.
        logger.exception("Gmail storage failed: email_id=%s", data.get("source_item_id"))
        return False
    # store_gmail_message already logs the insert / the update.
    return inserted
