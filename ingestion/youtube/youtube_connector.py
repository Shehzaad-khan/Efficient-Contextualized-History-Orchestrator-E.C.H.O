"""
youtube_connector.py - YTC Module
Echo Personal Memory System
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ste.redis_manager import REVISIT_TTL_SECONDS, check_and_record_revisit_async
from ste.storage_engine import (
    store_youtube_detection,
    update_youtube_metadata,
    update_youtube_watch_time,
)
from .youtube_api_client import is_valid_video_id
from .video_classifier import classify_video_type

load_dotenv()
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ytc", tags=["YouTube Connector"])

executor = ThreadPoolExecutor(max_workers=4)
redis_client: Optional[object] = None


class VideoDetectedEvent(BaseModel):
    url: str
    video_id: str
    is_short: bool
    watch_time_seconds: int
    triggered_by: str
    interaction_type: Optional[str] = None
    duration_seconds: Optional[int] = None
    timestamp: datetime


class WatchTimeHeartbeat(BaseModel):
    video_id: str
    watch_time_seconds: int
    timestamp: datetime


class VideoClosedEvent(BaseModel):
    video_id: str
    final_watch_time_seconds: int
    timestamp: datetime


# Architecture §7.3 Option A. These MUST match the extension's thresholds in
# extension/content/youtube_tracker.js: the extension sends /ytc/video-detected
# exactly once (intentFired latches), so a stricter server gate silently
# discards the video with no retry ever happening.
SHORT_MIN_WATCH_SECONDS = 15
REGULAR_MIN_WATCH_SECONDS = 20
COMPLETION_RATE_THRESHOLD = 0.5


def passes_intent_gate(event: VideoDetectedEvent, is_revisit: bool) -> bool:
    # Option A — watch time (Shorts have a lower bar than regular videos)
    min_watch = SHORT_MIN_WATCH_SECONDS if event.is_short else REGULAR_MIN_WATCH_SECONDS
    if event.watch_time_seconds >= min_watch:
        return True
    # Option B — completion rate (only computable when client reports duration)
    if event.duration_seconds and event.duration_seconds > 0:
        completion_rate = event.watch_time_seconds / event.duration_seconds
        if completion_rate >= COMPLETION_RATE_THRESHOLD:
            return True
    # Option C — revisit signal (same video earlier today, via Redis)
    if is_revisit:
        return True
    # Extra (beyond arch spec) — explicit manual interaction
    if event.triggered_by == "manual_interaction" and event.interaction_type:
        return True
    return False


async def check_revisit(video_id: str) -> bool:
    try:
        return await check_and_record_revisit_async("youtube", video_id, ttl_seconds=REVISIT_TTL_SECONDS)
    except Exception as exc:
        logger.error("Redis error: %s", exc)
        return False


@router.post("/video-detected")
async def handle_video_detected(event: VideoDetectedEvent):
    import asyncio
    from . import youtube_api_client
    from ste import settings_store

    if not settings_store.is_source_enabled("youtube"):
        return {"status": "discarded", "reason": "youtube_capture_disabled"}

    if not is_valid_video_id(event.video_id):
        return {"status": "discarded", "reason": "invalid_video_id"}

    is_revisit = await check_revisit(event.video_id)
    if not passes_intent_gate(event, is_revisit):
        return {"status": "discarded", "reason": "intent_gate_failed"}

    is_short = classify_video_type(event.url) == "short"
    loop = asyncio.get_event_loop()

    try:
        memory_id = await loop.run_in_executor(
            executor,
            store_youtube_detection,
            event.video_id,
            is_short,
            event.timestamp,
        )
    except Exception as exc:
        logger.error("DB write failed: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to save video memory")

    async def fetch_and_update():
        try:
            metadata = await youtube_api_client.fetch_video_metadata(event.video_id)
            if metadata:
                await loop.run_in_executor(executor, update_youtube_metadata, memory_id, metadata)
        except Exception as exc:
            logger.error("Metadata fetch failed: %s", exc)

    asyncio.create_task(fetch_and_update())

    return {
        "status": "saved",
        "memory_id": memory_id,
        "video_id": event.video_id,
        "is_short": is_short,
        "triggered_by": event.triggered_by,
    }


@router.post("/heartbeat")
async def handle_heartbeat(event: WatchTimeHeartbeat):
    import asyncio

    if not is_valid_video_id(event.video_id):
        return {"status": "discarded", "reason": "invalid_video_id"}

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, update_youtube_watch_time, event.video_id, event.watch_time_seconds)
    return {"status": "ok"}


@router.post("/video-closed")
async def handle_video_closed(event: VideoClosedEvent):
    import asyncio

    if not is_valid_video_id(event.video_id):
        return {"status": "discarded", "reason": "invalid_video_id"}

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, update_youtube_watch_time, event.video_id, event.final_watch_time_seconds)
    logger.info("video-closed - video_id=%s final_watch_time=%ss", event.video_id, event.final_watch_time_seconds)
    return {"status": "ok"}
