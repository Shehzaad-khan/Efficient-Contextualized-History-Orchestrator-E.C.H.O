"""
youtube_connector.py - YTC Module
Echo Personal Memory System
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.storage_engine import (
    store_youtube_detection,
    update_youtube_metadata,
    update_youtube_watch_time,
)
from .video_classifier import classify_video_type

load_dotenv()
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ytc", tags=["YouTube Connector"])

executor = ThreadPoolExecutor(max_workers=4)

redis_client: Optional[aioredis.Redis] = None
REVISIT_TTL_SECONDS = 86400


class VideoDetectedEvent(BaseModel):
    url: str
    video_id: str
    is_short: bool
    watch_time_seconds: int
    triggered_by: str
    interaction_type: Optional[str] = None
    timestamp: datetime


class WatchTimeHeartbeat(BaseModel):
    video_id: str
    watch_time_seconds: int
    timestamp: datetime


class VideoClosedEvent(BaseModel):
    video_id: str
    final_watch_time_seconds: int
    timestamp: datetime


def passes_intent_gate(event: VideoDetectedEvent, is_revisit: bool) -> bool:
    if event.watch_time_seconds >= 20:
        return True
    if event.triggered_by == "manual_interaction" and event.interaction_type:
        return True
    return is_revisit


async def check_revisit(video_id: str) -> bool:
    if not redis_client:
        logger.warning("Redis not connected - revisit check skipped")
        return False
    key = f"ytc:revisit:{video_id}"
    try:
        exists = await redis_client.exists(key)
        await redis_client.setex(key, REVISIT_TTL_SECONDS, "1")
        return bool(exists)
    except Exception as exc:
        logger.error("Redis error: %s", exc)
        return False


@router.post("/video-detected")
async def handle_video_detected(event: VideoDetectedEvent):
    import asyncio
    from . import youtube_api_client

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

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, update_youtube_watch_time, event.video_id, event.watch_time_seconds)
    return {"status": "ok"}


@router.post("/video-closed")
async def handle_video_closed(event: VideoClosedEvent):
    import asyncio

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, update_youtube_watch_time, event.video_id, event.final_watch_time_seconds)
    logger.info("video-closed - video_id=%s final_watch_time=%ss", event.video_id, event.final_watch_time_seconds)
    return {"status": "ok"}
