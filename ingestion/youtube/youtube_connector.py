"""
youtube_connector.py — YTC Module
Echo Personal Memory System

FastAPI backend handler for YouTube video events sent from the Chrome Extension.

Responsibilities:
    - Receive video events from playback_tracker.js
    - Run intent gate (3 conditions from architecture)
    - Write raw record to PostgreSQL on intent pass (hot path — instant, no blocking)
    - Trigger background metadata fetch via youtube_api_client
    - Update watch_time_seconds in real time as extension sends heartbeats
    - Redis revisit detection (24-hour window)

Hot path writes:
    memory_items        → source_type='youtube', source_id=video_id
    youtube_metadata    → video_id, is_short (metadata pending ENP)
    memory_engagement   → watch_time_seconds=0 (updated via heartbeats)

Background (ENP — not this file):
    YouTube Data API metadata fetch
    Transcript fetch
    Embedding generation
    FAISS indexing
    preprocessed=TRUE
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

import redis.asyncio as aioredis
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .video_classifier import classify_video_type, extract_video_id, is_youtube_url

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ytc", tags=["YouTube Connector"])

# ---------------------------------------------------------------------------
# Redis client — injected at app startup via app.state
# Used for 24-hour revisit detection (same pattern as Chrome CHC module)
# ---------------------------------------------------------------------------
redis_client: Optional[aioredis.Redis] = None

REVISIT_TTL_SECONDS = 86400  # 24 hours


# ---------------------------------------------------------------------------
# Pydantic models — define the contract the extension must send
# ---------------------------------------------------------------------------

class VideoDetectedEvent(BaseModel):
    """
    Sent by extension when a YouTube URL is detected.
    Triggers intent gate evaluation.
    """
    url: str
    video_id: str
    is_short: bool
    watch_time_seconds: int      # foreground watch time at time of event
    triggered_by: str            # 'watch_time' | 'manual_interaction' | 'revisit'
    interaction_type: Optional[str] = None  # 'pause' | 'seek' | 'speed_change' | None
    timestamp: datetime


class WatchTimeHeartbeat(BaseModel):
    """
    Sent every 5 seconds by extension while video is playing in foreground.
    Updates memory_engagement.watch_time_seconds incrementally.
    Used for real-time effort score tracking.
    """
    video_id: str
    watch_time_seconds: int      # cumulative total for this session
    timestamp: datetime


class VideoClosedEvent(BaseModel):
    """
    Sent when user navigates away from video or closes tab.
    Finalizes watch_time_seconds for the session.
    """
    video_id: str
    final_watch_time_seconds: int
    timestamp: datetime


# ---------------------------------------------------------------------------
# Intent Gate
# ---------------------------------------------------------------------------

def passes_intent_gate(event: VideoDetectedEvent, is_revisit: bool) -> bool:
    """
    Architecture-specified intent gate — ANY ONE of three conditions must pass.

    Option A: Sustained watch time >= 20 seconds (foreground, playing)
    Option B: Manual interaction (pause/resume, seek, speed change)
    Option C: Revisit — same video_id watched earlier today (Redis 24h)

    Args:
        event:      The VideoDetectedEvent from the extension
        is_revisit: Whether Redis found this video_id in the 24h window

    Returns:
        True if video should be saved to memory
    """
    # Option A — sustained watch time
    if event.watch_time_seconds >= 20:
        logger.debug(f"Intent gate PASS (Option A) — watch_time={event.watch_time_seconds}s")
        return True

    # Option B — manual interaction
    if event.triggered_by == "manual_interaction" and event.interaction_type:
        logger.debug(f"Intent gate PASS (Option B) — interaction={event.interaction_type}")
        return True

    # Option C — revisit
    if is_revisit:
        logger.debug(f"Intent gate PASS (Option C) — revisit detected")
        return True

    logger.debug(
        f"Intent gate FAIL — watch_time={event.watch_time_seconds}s, "
        f"triggered_by={event.triggered_by}, revisit={is_revisit}"
    )
    return False


async def check_revisit(video_id: str) -> bool:
    """
    Check Redis for a prior visit to this video_id within 24 hours.
    Sets the key if not present (first visit marker).

    Redis key: ytc:revisit:{video_id}
    TTL: 86400 seconds (24 hours)
    """
    if not redis_client:
        logger.warning("Redis not connected — revisit check skipped")
        return False

    key = f"ytc:revisit:{video_id}"
    try:
        exists = await redis_client.exists(key)
        # Always refresh/set the key to mark this visit
        await redis_client.setex(key, REVISIT_TTL_SECONDS, "1")
        return bool(exists)
    except Exception as e:
        logger.error(f"Redis error during revisit check: {e}")
        return False


# ---------------------------------------------------------------------------
# DB write helpers
# These are stubs until the STE (Storage Engine) module is ready.
# Replace the body of each with real SQLAlchemy/asyncpg calls when DB is live.
# ---------------------------------------------------------------------------

async def insert_memory_item(video_id: str, is_short: bool, detected_at: datetime) -> str:
    """
    INSERT INTO memory_items — hot path, instant write.

    source_type = 'youtube'
    source_id   = video_id
    title       = '' (populated by ENP after API fetch)
    raw_text    = '' (populated by ENP after API fetch)
    preprocessed = FALSE

    Returns:
        memory_id (UUID v4) of the newly created row
    """
    memory_id = str(uuid.uuid4())

    # TODO: replace with real DB call when STE is ready
    # await db.execute("""
    #     INSERT INTO memory_items
    #         (memory_id, system_group_id, source_type, source_id,
    #          title, raw_text, preprocessed, classified_by, created_at, first_ingested_at)
    #     VALUES
    #         ($1, 5, 'youtube', $2, '', '', FALSE, 'pending', $3, NOW())
    #     ON CONFLICT (source_type, source_id) DO NOTHING
    # """, memory_id, video_id, detected_at)

    logger.info(f"[STUB] INSERT memory_items — memory_id={memory_id} video_id={video_id}")
    return memory_id


async def insert_youtube_metadata(memory_id: str, video_id: str, is_short: bool) -> None:
    """
    INSERT INTO youtube_metadata — hot path, instant write.
    Metadata fields (channel_name, duration_seconds, transcript_text)
    are empty here — populated by ENP background worker after API fetch.
    """
    # TODO: replace with real DB call when STE is ready
    # await db.execute("""
    #     INSERT INTO youtube_metadata
    #         (memory_id, video_id, channel_name, duration_seconds, is_short, transcript_text)
    #     VALUES
    #         ($1, $2, NULL, NULL, $3, NULL)
    #     ON CONFLICT DO NOTHING
    # """, memory_id, video_id, is_short)

    logger.info(f"[STUB] INSERT youtube_metadata — memory_id={memory_id} is_short={is_short}")


async def insert_memory_engagement(memory_id: str) -> None:
    """
    INSERT INTO memory_engagement — hot path, instant write.
    watch_time_seconds starts at 0, updated via heartbeats.
    """
    # TODO: replace with real DB call when STE is ready
    # await db.execute("""
    #     INSERT INTO memory_engagement
    #         (memory_id, watch_time_seconds, first_opened_at, last_accessed_at, play_sessions_count)
    #     VALUES
    #         ($1, 0, NOW(), NOW(), 1)
    #     ON CONFLICT DO NOTHING
    # """, memory_id)

    logger.info(f"[STUB] INSERT memory_engagement — memory_id={memory_id}")


async def update_watch_time(video_id: str, watch_time_seconds: int) -> None:
    """
    UPDATE memory_engagement.watch_time_seconds for a given video_id.
    Called on every heartbeat from the extension.
    """
    # TODO: replace with real DB call when STE is ready
    # await db.execute("""
    #     UPDATE memory_engagement me
    #     SET watch_time_seconds = $1,
    #         last_accessed_at = NOW()
    #     FROM memory_items mi
    #     WHERE me.memory_id = mi.memory_id
    #       AND mi.source_type = 'youtube'
    #       AND mi.source_id = $2
    # """, watch_time_seconds, video_id)

    logger.info(f"[STUB] UPDATE watch_time — video_id={video_id} watch_time={watch_time_seconds}s")


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@router.post("/video-detected")
async def handle_video_detected(event: VideoDetectedEvent):
    """
    Main ingestion endpoint — called by playback_tracker.js when intent gate
    conditions are met in the extension.

    Flow:
        1. Check Redis for revisit
        2. Run intent gate (3 conditions)
        3. If PASS → write to DB (memory_items + youtube_metadata + memory_engagement)
        4. If FAIL → discard silently (204)
        5. ENP background worker picks up preprocessed=FALSE rows separately
    """
    logger.info(
        f"video-detected — video_id={event.video_id} "
        f"watch_time={event.watch_time_seconds}s "
        f"triggered_by={event.triggered_by}"
    )

    # Step 1 — revisit check
    is_revisit = await check_revisit(event.video_id)

    # Step 2 — intent gate
    if not passes_intent_gate(event, is_revisit):
        return {"status": "discarded", "reason": "intent_gate_failed"}

    # Step 3 — classify video type
    video_type = classify_video_type(event.url)
    is_short = video_type == "short"

    # Step 4 — write to DB (hot path — instant, no blocking)
    try:
        memory_id = await insert_memory_item(
            video_id=event.video_id,
            is_short=is_short,
            detected_at=event.timestamp,
        )
        await insert_youtube_metadata(memory_id, event.video_id, is_short)
        await insert_memory_engagement(memory_id)

    except Exception as e:
        logger.error(f"DB write failed for video_id={event.video_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to save video memory")

    # Step 5 — ENP picks up preprocessed=FALSE rows in background (not triggered here)

    return {
        "status": "saved",
        "memory_id": memory_id,
        "video_id": event.video_id,
        "is_short": is_short,
        "triggered_by": event.triggered_by,
    }


@router.post("/heartbeat")
async def handle_heartbeat(event: WatchTimeHeartbeat):
    """
    Called every 5 seconds by extension while video is playing in foreground.
    Updates watch_time_seconds in memory_engagement incrementally.
    Lightweight — just a DB update, no intent gate needed.
    """
    await update_watch_time(event.video_id, event.watch_time_seconds)
    return {"status": "ok"}


@router.post("/video-closed")
async def handle_video_closed(event: VideoClosedEvent):
    """
    Called when user navigates away from video or closes tab.
    Final watch_time update for the session.
    """
    await update_watch_time(event.video_id, event.final_watch_time_seconds)
    logger.info(
        f"video-closed — video_id={event.video_id} "
        f"final_watch_time={event.final_watch_time_seconds}s"
    )
    return {"status": "ok"}
