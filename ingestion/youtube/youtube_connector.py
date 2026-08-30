from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter

from backend.local_store import append_record

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/ytc", tags=["YouTube"])

_STORAGE_PATH = Path(__file__).resolve().parents[2] / "youtube_ingestion.json"


@router.get("/health")
def youtube_health() -> dict[str, Any]:
    return {"status": "ok", "source": "youtube", "mode": "local-store"}


@router.get("/status")
def youtube_status() -> dict[str, Any]:
    return {
        "status": "active",
        "message": "YouTube ingestion is running in local-store mode.",
        "storage": str(_STORAGE_PATH),
    }


@router.post("/ingest")
def youtube_ingest(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    result = _save_source_record("youtube", payload or {})
    logger.info("Persisted YouTube ingest record via canonical ingestion path: %s", result)
    return result
