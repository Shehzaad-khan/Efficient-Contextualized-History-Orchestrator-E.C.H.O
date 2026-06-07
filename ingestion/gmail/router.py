from __future__ import annotations

import asyncio
import logging
import os

from fastapi import APIRouter

from .config import CHECK_INTERVAL
from .database import initialize_database
from .gmail_api import authenticate_gmail, fetch_and_store_new_emails

router = APIRouter(prefix="/gmail", tags=["Gmail Connector"])
logger = logging.getLogger(__name__)

_service = None


def _get_service():
    global _service
    if _service is None:
        _service = authenticate_gmail()
    return _service


def poll_once() -> dict:
    initialize_database()
    service = _get_service()
    processed = fetch_and_store_new_emails(service)
    return {"status": "ok", "processed": processed}


async def poll_forever() -> None:
    if os.getenv("ENABLE_GMAIL_POLLING", "true").lower() not in {"1", "true", "yes"}:
        logger.info("Gmail background polling disabled")
        return
    while True:
        try:
            result = await asyncio.to_thread(poll_once)
            logger.info("Gmail poll completed: %s", result)
        except Exception as exc:
            logger.exception("Gmail poll failed: %s", exc)
        await asyncio.sleep(CHECK_INTERVAL)


@router.get("/health")
def gmail_health():
    return {"status": "ok", "module": "gmail"}


@router.post("/poll")
async def trigger_gmail_poll():
    return await asyncio.to_thread(poll_once)
