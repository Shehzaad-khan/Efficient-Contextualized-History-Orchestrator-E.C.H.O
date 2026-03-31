"""
youtube_connector.py — YTC Module
Echo Personal Memory System

FastAPI backend handler for YouTube video events sent from the Chrome Extension.

Responsibilities:
    - Receive video events from playback_tracker.js
    - Run intent gate (3 conditions from architecture)
    - Write raw record to PostgreSQL on intent pass (hot path — instant, no blocking)
    - Fetch YouTube metadata from Data API after save
    - Update watch_time_seconds in real time as extension sends heartbeats
    - Redis revisit detection (24-hour window)

Hot path writes:
    memory_items        → source_type='youtube', source_id=video_id
    youtube_metadata    → video_id, is_short (metadata fetched immediately after)
    memory_engagement   → watch_time_seconds=0 (updated via heartbeats)
"""

import logging
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

import psycopg2
import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .video_classifier import classify_video_type

load_dotenv()
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ytc", tags=["YouTube Connector"])

executor = ThreadPoolExecutor(max_workers=4)

redis_client: Optional[aioredis.Redis] = None
REVISIT_TTL_SECONDS = 86400


def get_connection():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise ValueError("DATABASE_URL not set in .env")
    return psycopg2.connect(url)


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
        logger.debug(f"Intent gate PASS (A) — watch_time={event.watch_time_seconds}s")
        return True
    if event.triggered_by == "manual_interaction" and event.interaction_type:
        logger.debug(f"Intent gate PASS (B) — interaction={event.interaction_type}")
        return True
    if is_revisit:
        logger.debug("Intent gate PASS (C) — revisit")
        return True
    logger.debug(f"Intent gate FAIL — watch_time={event.watch_time_seconds}s")
    return False


async def check_revisit(video_id: str) -> bool:
    if not redis_client:
        logger.warning("Redis not connected — revisit check skipped")
        return False
    key = f"ytc:revisit:{video_id}"
    try:
        exists = await redis_client.exists(key)
        await redis_client.setex(key, REVISIT_TTL_SECONDS, "1")
        return bool(exists)
    except Exception as e:
        logger.error(f"Redis error: {e}")
        return False


def _db_insert_video(video_id: str, is_short: bool, detected_at: datetime) -> str:
    memory_id = str(uuid.uuid4())
    system_group_id = 3 if is_short else 5

    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO memory_items
                (memory_id, system_group_id, source_type, source_id,
                 title, raw_text, preprocessed, classified_by, created_at, first_ingested_at)
            VALUES
                (%s, %s, 'youtube', %s, '', '', FALSE, 'pending', %s, NOW())
            ON CONFLICT (source_type, source_id) DO NOTHING
        """, (memory_id, system_group_id, video_id, detected_at))

        if cur.rowcount == 0:
            cur.execute("""
                SELECT memory_id FROM memory_items
                WHERE source_type = 'youtube' AND source_id = %s
            """, (video_id,))
            row = cur.fetchone()
            if row:
                memory_id = str(row[0])
            conn.rollback()
            logger.info(f"Video already exists — memory_id={memory_id}")
            return memory_id

        cur.execute("""
            INSERT INTO youtube_metadata
                (memory_id, video_id, channel_name, channel_id,
                 duration_seconds, is_short, transcript_text)
            VALUES
                (%s, %s, NULL, NULL, NULL, %s, NULL)
            ON CONFLICT DO NOTHING
        """, (memory_id, video_id, is_short))

        cur.execute("""
            INSERT INTO memory_engagement
                (memory_id, watch_time_seconds, first_opened_at,
                 last_accessed_at, play_sessions_count)
            VALUES
                (%s, 0, NOW(), NOW(), 1)
            ON CONFLICT DO NOTHING
        """, (memory_id,))

        conn.commit()
        cur.close()
        logger.info(f"DB INSERT success — memory_id={memory_id} video_id={video_id}")
        return memory_id

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def _db_update_watch_time(video_id: str, watch_time_seconds: int) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE memory_engagement me
            SET watch_time_seconds = %s,
                last_accessed_at = NOW(),
                completion_rate = CASE
                    WHEN ym.duration_seconds > 0
                    THEN LEAST(1.0, %s::float / ym.duration_seconds)
                    ELSE NULL
                END
            FROM memory_items mi
            LEFT JOIN youtube_metadata ym ON mi.memory_id = ym.memory_id
            WHERE me.memory_id = mi.memory_id
              AND mi.source_type = 'youtube'
              AND mi.source_id = %s
        """, (watch_time_seconds, watch_time_seconds, video_id))
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        logger.error(f"watch_time update failed: {e}")
    finally:
        conn.close()


def _db_update_metadata(memory_id: str, metadata: dict) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            UPDATE youtube_metadata SET
                channel_name        = %s,
                channel_id          = %s,
                duration_seconds    = %s,
                youtube_category_id = %s
            WHERE memory_id = %s
        """, (
            metadata.get("channel_name"),
            metadata.get("channel_id"),
            metadata.get("duration_seconds"),
            metadata.get("category_id"),
            memory_id,
        ))

        raw_text = f"{metadata.get('description', '')} {' '.join(metadata.get('tags', []))}".strip()
        cur.execute("""
            UPDATE memory_items SET
                title           = %s,
                raw_text        = %s,
                last_updated_at = NOW()
            WHERE memory_id = %s
        """, (metadata.get("title", ""), raw_text, memory_id))

        conn.commit()
        cur.close()
        logger.info(f"Metadata updated — memory_id={memory_id} title='{metadata.get('title')}'")
    except Exception as e:
        conn.rollback()
        logger.error(f"Metadata update failed: {e}")
    finally:
        conn.close()


@router.post("/video-detected")
async def handle_video_detected(event: VideoDetectedEvent):
    import asyncio
    from . import youtube_api_client

    logger.info(
        f"video-detected — video_id={event.video_id} "
        f"watch_time={event.watch_time_seconds}s "
        f"triggered_by={event.triggered_by}"
    )

    is_revisit = await check_revisit(event.video_id)

    if not passes_intent_gate(event, is_revisit):
        return {"status": "discarded", "reason": "intent_gate_failed"}

    video_type = classify_video_type(event.url)
    is_short = video_type == "short"

    loop = asyncio.get_event_loop()
    try:
        memory_id = await loop.run_in_executor(
            executor, _db_insert_video, event.video_id, is_short, event.timestamp
        )
    except Exception as e:
        logger.error(f"DB write failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to save video memory")

    async def fetch_and_update():
        try:
            metadata = await youtube_api_client.fetch_video_metadata(event.video_id)
            if metadata:
                await loop.run_in_executor(executor, _db_update_metadata, memory_id, metadata)
        except Exception as e:
            logger.error(f"Metadata fetch failed: {e}")

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
    await loop.run_in_executor(executor, _db_update_watch_time, event.video_id, event.watch_time_seconds)
    return {"status": "ok"}


@router.post("/video-closed")
async def handle_video_closed(event: VideoClosedEvent):
    import asyncio
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, _db_update_watch_time, event.video_id, event.final_watch_time_seconds)
    logger.info(f"video-closed — video_id={event.video_id} final_watch_time={event.final_watch_time_seconds}s")
    return {"status": "ok"}
