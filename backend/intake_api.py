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
    canonical_payload = record["payload"]

    try:
        from datetime import datetime, timezone

        from backend.storage_engine import SYSTEM_GROUP_IDS, store_memory_item

        created_at_value = canonical_payload.get("timestamp") or canonical_payload.get("created_at")
        if not created_at_value:
            created_at_value = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        if source == "gmail":
            source_id = canonical_payload.get("email_id") or canonical_payload.get("id") or canonical_payload.get("thread_id") or canonical_payload.get("subject") or "gmail-message"
            title = canonical_payload.get("subject") or canonical_payload.get("title") or "Gmail message"
            raw_text = canonical_payload.get("body") or canonical_payload.get("content") or canonical_payload.get("snippet") or ""
            source_metadata = {
                "from": canonical_payload.get("sender") or canonical_payload.get("from"),
                "to": canonical_payload.get("recipients") or canonical_payload.get("to") or [],
                "thread_id": canonical_payload.get("thread_id"),
                "subject": title,
                "labels": canonical_payload.get("labels") or [],
                "has_attachments": bool(canonical_payload.get("has_attachments")),
                "is_sent": bool(canonical_payload.get("is_sent")),
            }
            engagement = {"dwell_time_seconds": int(canonical_payload.get("dwell_time_seconds") or 0), "play_sessions_count": 0}
        elif source == "chrome":
            source_id = canonical_payload.get("canonical_url") or canonical_payload.get("url") or canonical_payload.get("id") or "chrome-page"
            title = canonical_payload.get("title") or canonical_payload.get("url") or "Web page"
            raw_text = canonical_payload.get("text") or canonical_payload.get("content") or canonical_payload.get("snippet") or ""
            source_metadata = {
                "url": canonical_payload.get("url"),
                "canonical_url": canonical_payload.get("canonical_url") or canonical_payload.get("url"),
                "domain": canonical_payload.get("domain"),
                "referrer": canonical_payload.get("referrer"),
                "scroll_depth": canonical_payload.get("scroll_depth", 0.0),
                "interaction_count": canonical_payload.get("interaction_count", 0),
                "revisit_count": canonical_payload.get("revisit_count", 0),
                "word_count": canonical_payload.get("word_count"),
            }
            engagement = {
                "dwell_time_seconds": int(canonical_payload.get("dwell_time_seconds") or 0),
                "play_sessions_count": int(canonical_payload.get("play_sessions_count") or 1),
            }
        elif source == "youtube":
            source_id = canonical_payload.get("video_id") or canonical_payload.get("id") or canonical_payload.get("title") or "youtube-video"
            title = canonical_payload.get("title") or "YouTube video"
            raw_text = canonical_payload.get("transcript_text") or canonical_payload.get("description") or canonical_payload.get("snippet") or ""
            source_metadata = {
                "channel_name": canonical_payload.get("channel_name"),
                "channel_id": canonical_payload.get("channel_id"),
                "duration_seconds": canonical_payload.get("duration_seconds"),
                "is_short": bool(canonical_payload.get("is_short")),
                "transcript_text": canonical_payload.get("transcript_text"),
                "youtube_category_id": canonical_payload.get("youtube_category_id"),
            }
            engagement = {
                "dwell_time_seconds": int(canonical_payload.get("watch_time_seconds") or canonical_payload.get("dwell_time_seconds") or 0),
                "watch_time_seconds": int(canonical_payload.get("watch_time_seconds") or 0),
                "play_sessions_count": int(canonical_payload.get("play_sessions_count") or 1),
            }
        else:
            raise ValueError(f"Unsupported source: {source}")

        memory_id, inserted = store_memory_item(
            source_type=source,
            source_id=str(source_id),
            system_group_id=SYSTEM_GROUP_IDS.get("misc", 5),
            title=str(title),
            raw_text=str(raw_text) if raw_text is not None else None,
            created_at=datetime.fromisoformat(str(created_at_value).replace("Z", "+00:00")).replace(tzinfo=None),
            source_metadata=source_metadata,
            engagement=engagement,
        )
        return {
            "status": "ok",
            "source": source,
            "storage": "postgresql",
            "record": {
                "memory_id": memory_id,
                "inserted": inserted,
                "source": source,
                "payload": canonical_payload,
            },
        }
    except Exception as exc:
        saved = append_record(source, canonical_payload, None)
        return {
            "status": "ok",
            "source": source,
            "storage": "local-json",
            "fallback": True,
            "record": saved,
            "warning": str(exc),
        }


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
