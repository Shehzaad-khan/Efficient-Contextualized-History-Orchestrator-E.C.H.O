"""
youtube_connector.py — YouTube capture (architecture §7, YTC module)
Echo Personal Memory System

Routes (mounted by backend/main.py under /ytc), all sent by
extension/content/youtube_tracker.js through background.js:

    POST /ytc/video-detected   once per video, when the extension's intent gate passes
    POST /ytc/revisit-check    once per video after 5 s of active watch time
    POST /ytc/heartbeat        every 5 s after that: the session's running watch time
    POST /ytc/video-closed     on leave/close: the session's final watch time

Flow of /video-detected:
    1. capture setting off       -> discard
    2. bad video_id              -> discard
    3. intent gate (§7.3)        -> discard if it fails
    4. record this visit in Redis for the next 24 h
    5. store the row (empty title, NULL raw_text) and answer the extension
    6. after the response: fetch Data API metadata + transcript, fill the row
       (the ENP worker waits for that fill before embedding)

The server repeats the extension's gate as a safety net (defense in depth): the
backend must not trust whatever a client posts.
Option C (revisit) is the one gate option a client cannot prove by its own
numbers. The early /revisit-check route is where the server decides it: on a
true answer it remembers "this video is a confirmed revisit" for
REVISIT_CONFIRM_SECONDS, and /video-detected accepts triggered_by="revisit" once,
only against that memory. The Redis key alone is not enough: /revisit-check
itself writes it on every first visit.

Threading: the routes are async, so every blocking call (Postgres via
ste.storage_engine, the sync Redis read in settings_store) goes through
asyncio.to_thread. The revisit check uses the async Redis client directly.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from ste import settings_store
from ste.redis_manager import REVISIT_TTL_SECONDS, check_and_record_revisit_async
from ste.storage_engine import (
    store_youtube_detection,
    update_youtube_metadata,
    update_youtube_watch_time,
)
from .video_classifier import classify_video_type
from .youtube_api_client import fetch_video_metadata, is_valid_video_id

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ytc", tags=["YouTube Connector"])


class VideoDetectedEvent(BaseModel):
    url: str
    video_id: str
    is_short: bool  # the extension's own view; the server re-derives it from the URL
    watch_time_seconds: int = Field(ge=0)
    triggered_by: str  # "watch_time" | "completion_rate" | "manual_interaction" | "revisit"
    interaction_type: Optional[str] = None  # "pause" | "seek" | "speed_change"
    duration_seconds: Optional[int] = Field(default=None, ge=0)
    timestamp: datetime


class WatchTimeHeartbeat(BaseModel):
    video_id: str
    watch_time_seconds: int = Field(ge=0)  # running total for THIS session
    timestamp: datetime


class VideoClosedEvent(BaseModel):
    video_id: str
    final_watch_time_seconds: int = Field(ge=0)
    timestamp: datetime


class VideoRevisitCheck(BaseModel):
    video_id: str


# Architecture §7.3 Option A. These MUST match the extension's thresholds in
# extension/content/youtube_tracker.js: the extension sends /ytc/video-detected
# exactly once (intentFired latches), so a stricter server gate silently
# discards the video with no retry ever happening.
SHORT_MIN_WATCH_SECONDS = 15
REGULAR_MIN_WATCH_SECONDS = 20
COMPLETION_RATE_THRESHOLD = 0.5


def passes_intent_gate(event: VideoDetectedEvent, is_short: bool, is_revisit: bool) -> bool:
    """§7.3 gate: any ONE option passes. is_short comes from the server's own
    URL check, so a client can't claim "short" to get the lower bar."""
    # Option A — watch time (Shorts have a lower bar than regular videos)
    min_watch = SHORT_MIN_WATCH_SECONDS if is_short else REGULAR_MIN_WATCH_SECONDS
    if event.watch_time_seconds >= min_watch:
        return True
    # Option B — completion rate (only computable when client reports duration)
    if event.duration_seconds and event.duration_seconds > 0:
        completion_rate = event.watch_time_seconds / event.duration_seconds
        if completion_rate >= COMPLETION_RATE_THRESHOLD:
            return True
    # Option C — revisit: same video in the last 24 h (sliding window in Redis;
    # the doc says "earlier today")
    if is_revisit:
        return True
    # Extra (beyond arch spec) — explicit manual interaction (pause/seek/speed)
    if event.triggered_by == "manual_interaction" and event.interaction_type:
        return True
    return False


def _discard(reason: str, video_id: str) -> dict:
    """Log why a request was dropped and build the response the extension gets."""
    # %r + truncation: an invalid id is client input and could hold newlines.
    logger.info("YouTube event discarded: reason=%s video_id=%r", reason, video_id[:20])
    return {"status": "discarded", "reason": reason}


async def _youtube_enabled() -> bool:
    # is_source_enabled is sync: a cache refresh (every 30 s) is a blocking
    # Redis GET, up to the socket timeout when Redis is down. Keep it off the loop.
    return await asyncio.to_thread(settings_store.is_source_enabled, "youtube")


async def check_revisit(video_id: str) -> bool:
    """True if this video was seen in the last 24 h; also records this visit.
    Redis down -> False (the video can still pass on Options A/B/manual)."""
    try:
        return await check_and_record_revisit_async("youtube", video_id, ttl_seconds=REVISIT_TTL_SECONDS)
    except Exception:
        logger.warning("YouTube revisit check failed for video_id=%s; treating as first visit", video_id, exc_info=True)
        return False


# Videos /revisit-check has just confirmed as revisits: video_id -> expiry on the
# time.monotonic() clock. In memory on purpose (it lives seconds, and the routes
# run on one event loop, so no lock is needed); a restart inside that window only
# drops one revisit, and the next watch of the video is a revisit again.
REVISIT_CONFIRM_SECONDS = 60
_confirmed_revisits: dict[str, float] = {}


def _confirm_revisit(video_id: str) -> None:
    now = time.monotonic()
    for stale_id in [vid for vid, expiry in _confirmed_revisits.items() if expiry <= now]:
        del _confirmed_revisits[stale_id]
    _confirmed_revisits[video_id] = now + REVISIT_CONFIRM_SECONDS


def _consume_confirmed_revisit(video_id: str) -> bool:
    """True once per confirmation: the entry is removed, so a client cannot replay it."""
    expiry = _confirmed_revisits.pop(video_id, None)
    return expiry is not None and expiry > time.monotonic()


@router.post("/revisit-check")
async def handle_revisit_check(event: VideoRevisitCheck):
    """Record the early visit and let a returning video pass Option C."""
    if not await _youtube_enabled():
        return {"is_revisit": False}
    if not is_valid_video_id(event.video_id):
        return {"is_revisit": False}

    is_revisit = await check_revisit(event.video_id)
    if is_revisit:
        _confirm_revisit(event.video_id)
    return {"is_revisit": is_revisit}


async def _fill_metadata(memory_id: str, video_id: str) -> None:
    """Runs after the response is sent (FastAPI BackgroundTasks). Failures are
    logged, never raised: the row stays with an empty title until a re-watch."""
    try:
        metadata = await fetch_video_metadata(video_id)
        if not metadata:
            # fetch_video_metadata already logged why (auth, quota, 404, ...).
            return
        await asyncio.to_thread(update_youtube_metadata, memory_id, metadata)
    except Exception:
        logger.exception("YouTube metadata fill failed: memory_id=%s video_id=%s", memory_id, video_id)


@router.post("/video-detected")
async def handle_video_detected(event: VideoDetectedEvent, background_tasks: BackgroundTasks):
    if not await _youtube_enabled():
        return _discard("youtube_capture_disabled", event.video_id)

    if not is_valid_video_id(event.video_id):
        return _discard("invalid_video_id", event.video_id)

    is_short = classify_video_type(event.url) == "short"
    is_revisit = event.triggered_by == "revisit" and _consume_confirmed_revisit(event.video_id)
    if not passes_intent_gate(event, is_short, is_revisit):
        return _discard("intent_gate_failed", event.video_id)
    await check_revisit(event.video_id)  # record this visit; the answer is not needed

    try:
        memory_id = await asyncio.to_thread(store_youtube_detection, event.video_id, is_short, event.timestamp)
    except Exception:
        logger.exception("YouTube detection store failed: video_id=%s", event.video_id)
        raise HTTPException(status_code=500, detail="Failed to save video memory")

    # BackgroundTasks keeps a reference to the job and runs it after the
    # response; a bare asyncio.create_task can be garbage-collected mid-run.
    background_tasks.add_task(_fill_metadata, memory_id, event.video_id)

    return {
        "status": "saved",
        "memory_id": memory_id,
        "video_id": event.video_id,
        "is_short": is_short,
        "triggered_by": event.triggered_by,
    }


async def _save_watch_time(video_id: str, watch_time_seconds: int) -> None:
    # Stores GREATEST(stored, sent): the longest single session, not the sum of
    # re-watches (decision A2 / tracker C6, on hold).
    try:
        await asyncio.to_thread(update_youtube_watch_time, video_id, watch_time_seconds)
    except Exception:
        logger.exception("YouTube watch-time update failed: video_id=%s", video_id)
        raise


@router.post("/heartbeat")
async def handle_heartbeat(event: WatchTimeHeartbeat):
    # Capture switched off mid-video -> stop updating the stored row too.
    if not await _youtube_enabled():
        return _discard("youtube_capture_disabled", event.video_id)
    if not is_valid_video_id(event.video_id):
        return _discard("invalid_video_id", event.video_id)

    await _save_watch_time(event.video_id, event.watch_time_seconds)
    return {"status": "ok"}


@router.post("/video-closed")
async def handle_video_closed(event: VideoClosedEvent):
    if not await _youtube_enabled():
        return _discard("youtube_capture_disabled", event.video_id)
    if not is_valid_video_id(event.video_id):
        return _discard("invalid_video_id", event.video_id)

    await _save_watch_time(event.video_id, event.final_watch_time_seconds)
    logger.info("YouTube video closed: video_id=%s final_watch_time=%ss", event.video_id, event.final_watch_time_seconds)
    return {"status": "ok"}
