from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from backend.local_store import append_record, list_records, search_records

router = APIRouter(prefix="/intake", tags=["Intake API"])


def _canonical_record(source: str, payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = payload or {}
    canonical = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": source,
        "payload": payload,
    }
    if source == "chrome":
        canonical["payload"].setdefault("domain", payload.get("domain") or "unknown")
        canonical["payload"].setdefault("url", payload.get("url") or "")
        canonical["payload"].setdefault("title", payload.get("title") or payload.get("url") or "Untitled page")
        canonical["payload"].setdefault("canonical_url", payload.get("canonical_url") or payload.get("url") or "")
    elif source == "youtube":
        canonical["payload"].setdefault("video_id", payload.get("video_id") or payload.get("id") or "unknown")
        canonical["payload"].setdefault("title", payload.get("title") or "Untitled video")
    elif source == "gmail":
        canonical["payload"].setdefault("subject", payload.get("subject") or payload.get("title") or "No subject")
        canonical["payload"].setdefault("sender", payload.get("sender") or payload.get("from") or "unknown")
    return canonical


def _save_source_record(source: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    record = _canonical_record(source, payload)
    saved = append_record(source, record["payload"], None)
    return {"status": "ok", "source": source, "record": saved}


@router.post("/chrome")
def intake_chrome(payload: dict[str, Any] | None = None):
    return _save_source_record("chrome", payload)


@router.post("/youtube")
def intake_youtube(payload: dict[str, Any] | None = None):
    return _save_source_record("youtube", payload)


@router.post("/gmail")
def intake_gmail(payload: dict[str, Any] | None = None):
    return _save_source_record("gmail", payload)


@router.get("/records")
def get_records(source: str | None = Query(None), limit: int = Query(20, ge=1, le=200)):
    return {"records": list_records(source=source, limit=limit)}


@router.get("/search")
def search_records_api(query: str = Query(..., min_length=1), source: str | None = Query(None)):
    if not query.strip():
        raise HTTPException(status_code=400, detail="query is required")
    return {"records": search_records(query, source=source)}
