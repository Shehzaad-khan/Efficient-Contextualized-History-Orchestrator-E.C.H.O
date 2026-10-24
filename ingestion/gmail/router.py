"""
router.py — Gmail connector API and background poller
Echo Personal Memory System

    poll_forever()            background task started by backend/main.py ("gmail-poller")
    GET  /gmail/health        liveness check
    POST /gmail/poll          run one poll now (manual trigger / testing)
    POST /gmail/engagement    the Gmail content script reports an opened email + dwell

Threads: every blocking call (Gmail API, PostgreSQL, the settings read from
Redis) happens inside poll_once or update_gmail_engagement, and both run in a
worker thread via asyncio.to_thread. Nothing here blocks the event loop.
Two polls (the loop and POST /poll) can still overlap in two threads;
gmail_api's _poll_lock makes the second wait for the first.

Shutdown: backend/main.py cancels poll_forever. A poll already running in its
worker thread cannot be cancelled and finishes on its own first.
"""

from __future__ import annotations

import asyncio
import logging
import threading

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ste import settings_store
from ste.storage_engine import update_gmail_engagement
from .config import CHECK_INTERVAL
from .database import initialize_database
from .gmail_api import GmailAuthError, authenticate_gmail, fetch_and_store_new_emails

router = APIRouter(prefix="/gmail", tags=["Gmail Connector"])
logger = logging.getLogger(__name__)

# One Gmail API service for the whole process, built on first use.
_service = None
_service_lock = threading.Lock()
_gmail_auth_warning_logged = False


def _get_service():
    """Return the shared Gmail service, authenticating on first use.

    The lock stops the poll loop and POST /poll from building the service at
    the same time. If authentication raises, _service stays None and the next
    poll retries.
    """
    global _service
    with _service_lock:
        if _service is None:
            _service = authenticate_gmail()
        return _service


def poll_once() -> dict:
    """One Gmail poll. Blocking — always call it through asyncio.to_thread."""
    global _gmail_auth_warning_logged
    if not settings_store.is_source_enabled("gmail"):
        return {"status": "skipped", "reason": "gmail_capture_disabled", "processed": 0}
    if not initialize_database():
        # database.py logs the reason once; without this check every email
        # in the poll would fail to store, one by one.
        return {"status": "skipped", "reason": "database_unavailable", "processed": 0}
    try:
        service = _get_service()
    except GmailAuthError as exc:
        with _service_lock:
            if _gmail_auth_warning_logged:
                logger.debug("Gmail polling skipped until sign-in completes: %s", exc)
            else:
                logger.warning("Gmail polling skipped until sign-in completes: %s", exc)
                _gmail_auth_warning_logged = True
        return {"status": "skipped", "reason": "gmail_not_signed_in", "processed": 0}

    with _service_lock:
        # Signed in again: the next sign-in failure must be logged at WARNING again.
        _gmail_auth_warning_logged = False
    processed = fetch_and_store_new_emails(service)
    return {"status": "ok", "processed": processed}


async def poll_forever() -> None:
    """Poll, then wait CHECK_INTERVAL seconds, forever (the wait starts after
    each poll ends, so a slow poll never overlaps the next one)."""
    logger.info("Gmail poller started (every %d s after each poll ends)", CHECK_INTERVAL)
    try:
        while True:
            try:
                result = await asyncio.to_thread(poll_once)
                # gmail_api already logs an INFO summary of each real poll.
                logger.debug("Gmail poll result: %s", result)
            except Exception:
                logger.exception("Gmail poll failed")
            await asyncio.sleep(CHECK_INTERVAL)
    except asyncio.CancelledError:
        logger.info("Gmail poller stopped")
        raise


@router.get("/health")
def gmail_health():
    return {"status": "ok", "module": "gmail"}


@router.post("/poll")
async def trigger_gmail_poll():
    logger.info("Manual Gmail poll requested")
    return await asyncio.to_thread(poll_once)


class EmailOpenedEvent(BaseModel):
    email_id: str = Field(..., min_length=1, max_length=255)
    dwell_seconds: int = Field(..., ge=0)


@router.post("/engagement")
async def record_email_engagement(event: EmailOpenedEvent):
    updated = await asyncio.to_thread(
        update_gmail_engagement, event.email_id, event.dwell_seconds
    )
    if not updated:
        # Normal for emails we never stored (excluded senders, drafts).
        logger.debug("Gmail engagement for an unknown email: %s", event.email_id)
        raise HTTPException(status_code=404, detail="Email not found in memory")
    return {"status": "ok", "email_id": event.email_id}
