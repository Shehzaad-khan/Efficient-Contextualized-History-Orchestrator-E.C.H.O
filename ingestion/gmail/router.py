from __future__ import annotations

import asyncio
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ste.storage_engine import update_gmail_engagement
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


class EmailOpenedEvent(BaseModel):
    email_id: str = Field(..., min_length=1, max_length=255)
    dwell_seconds: int = Field(..., ge=0)


@router.post("/engagement")
async def record_email_engagement(event: EmailOpenedEvent):
    updated = await asyncio.to_thread(
        update_gmail_engagement, event.email_id, event.dwell_seconds
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Email not found in memory")
    return {"status": "ok", "email_id": event.email_id}
