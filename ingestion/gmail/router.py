from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException

from backend.local_store import append_record

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/gmail", tags=["Gmail"])

_STORAGE_PATH = Path(__file__).resolve().parents[2] / "gmail_ingestion.json"


@router.get("/health")
def gmail_health() -> dict[str, Any]:
    return {"status": "ok", "source": "gmail", "mode": "local-store"}


@router.get("/status")
def gmail_status() -> dict[str, Any]:
    return {
        "status": "active",
        "message": "Gmail polling is running in local-store mode.",
        "storage": str(_STORAGE_PATH),
    }


def poll_once() -> dict[str, Any]:
    """Persist a minimal Gmail poll event and return it for the app to see."""
    record = append_record("gmail", {"action": "poll_once", "items_processed": 0}, _STORAGE_PATH)
    logger.info("gmail poll_once stored record: %s", record["timestamp"])
    return {"status": "ok", "items_processed": 0, "mode": "local-store", "recorded": True}


async def poll_forever() -> None:
    """Background Gmail poller loop with a local noop record every 60s."""
    logger.info("Gmail poll_forever started in local-store mode")
    while True:
        append_record("gmail", {"action": "heartbeat", "items_processed": 0}, _STORAGE_PATH)
        await asyncio.sleep(60)


@router.post("/sync")
def gmail_sync() -> dict[str, Any]:
    return poll_once()
