from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter

from backend.local_store import append_record

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/chrome", tags=["Chrome"])

_STORAGE_PATH = Path(__file__).resolve().parents[2] / "chrome_ingestion.json"


@router.get("/health")
def chrome_health() -> dict[str, Any]:
    return {"status": "ok", "source": "chrome", "mode": "local-store"}


@router.get("/status")
def chrome_status() -> dict[str, Any]:
    return {
        "status": "active",
        "message": "Chrome capture is running in local-store mode.",
        "storage": str(_STORAGE_PATH),
    }


@router.post("/ingest")
def chrome_ingest(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    result = _save_source_record("chrome", payload or {})
    logger.info("Persisted Chrome ingest record via canonical ingestion path: %s", result)
    return result
