"""
youtube_api_client.py — YTC Module
Echo Personal Memory System

Fetches video metadata from YouTube Data API v3.
Runs in the background (ENP) — never on the hot ingestion path.

API call costs 1 quota unit per video.
Free tier: 10,000 units/day — sufficient for personal use.

Requires:
    YOUTUBE_API_KEY in .env
"""

import os
import re
import logging
from typing import Optional

import httpx
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3/videos"


def parse_iso8601_duration(duration: str) -> int:
    """
    Convert ISO 8601 duration string to total seconds.
    YouTube Data API returns duration in this format.

    Examples:
        PT5M30S  → 330 seconds
        PT1H2M3S → 3723 seconds
        PT45S    → 45 seconds
        PT0S     → 0 seconds (live stream placeholder)
    """
    if not duration:
        return 0

    pattern = r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?"
    match = re.match(pattern, duration)
    if not match:
        return 0

    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)

    return hours * 3600 + minutes * 60 + seconds


async def fetch_video_metadata(video_id: str) -> Optional[dict]:
    """
    Fetch video metadata from YouTube Data API v3.

    Args:
        video_id: 11-character YouTube video ID

    Returns:
        dict with keys:
            title           str
            description     str
            channel_name    str
            channel_id      str
            published_at    str  (ISO 8601)
            duration_seconds int
            category_id     str  (e.g. '27' = Education)
            tags            list[str]
        or None if fetch fails (network error, invalid ID, quota exceeded)

    Note:
        Caller should handle None gracefully — store what we have,
        mark preprocessed=FALSE, ENP will retry.
    """
    if not YOUTUBE_API_KEY:
        logger.warning("YOUTUBE_API_KEY not set — returning stub metadata")
        return _stub_metadata(video_id)

    params = {
        "id": video_id,
        "part": "snippet,contentDetails",
        "key": YOUTUBE_API_KEY,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(YOUTUBE_API_BASE, params=params)
            response.raise_for_status()
            data = response.json()

        items = data.get("items", [])
        if not items:
            logger.warning(f"No metadata found for video_id={video_id}")
            return None

        item = items[0]
        snippet = item.get("snippet", {})
        content_details = item.get("contentDetails", {})

        duration_seconds = parse_iso8601_duration(
            content_details.get("duration", "PT0S")
        )

        return {
            "title": snippet.get("title", ""),
            "description": snippet.get("description", ""),
            "channel_name": snippet.get("channelTitle", ""),
            "channel_id": snippet.get("channelId", ""),
            "published_at": snippet.get("publishedAt", ""),
            "duration_seconds": duration_seconds,
            "category_id": snippet.get("categoryId", ""),
            "tags": snippet.get("tags", []),
        }

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 403:
            logger.error("YouTube API quota exceeded or API key invalid")
        else:
            logger.error(f"YouTube API HTTP error: {e.response.status_code}")
        return None

    except httpx.RequestError as e:
        logger.error(f"YouTube API request failed: {e}")
        return None


def _stub_metadata(video_id: str) -> dict:
    """
    Returns stub metadata when API key is not configured.
    Used during development before Google Cloud is set up.
    All fields marked clearly as stubs.
    """
    return {
        "title": f"[STUB] Video {video_id}",
        "description": "[STUB] No API key configured",
        "channel_name": "[STUB] Channel",
        "channel_id": "STUB_CHANNEL_ID",
        "published_at": "2026-01-01T00:00:00Z",
        "duration_seconds": 300,
        "category_id": "27",
        "tags": [],
    }
