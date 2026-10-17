"""
video_classifier.py — Shorts vs long-form (architecture §7.4, YTC module)
Echo Personal Memory System

Rules (doc §7.4):
  - URL path starts with /shorts/   -> 'short'   (definitive)
  - 0 < duration_seconds < 60       -> 'short'   (fallback)
  - otherwise                       -> 'long'

Caller: youtube_connector.handle_video_detected, with the URL only (the Data API
duration arrives later). The result is stored as youtube_metadata.is_short, and
ENP's Stage-1 classifier (enp/system_group_classifier.py) puts every Short in the
'entertainment' system group. Wellbeing treats Shorts at session level (§7.4).

Pure function, no state and no I/O: safe from any thread. No logger — the
connector logs the stored result.
"""

from __future__ import annotations

from typing import Literal, Optional
from urllib.parse import urlsplit

VideoType = Literal["short", "long"]

SHORTS_PATH_PREFIX = "/shorts/"
# Doc §7.4. Likely (YouTube change in late 2024, not checked here): Shorts can now
# run up to 3 minutes, so this fallback misses longer Shorts. It is unused today
# (the connector passes no duration); the URL rule decides.
SHORT_MAX_DURATION_SECONDS = 60


def classify_video_type(url: str, duration_seconds: Optional[int] = None) -> VideoType:
    """Classify a YouTube video as 'short' or 'long'.

    url:              full video URL (youtube.com/watch?v=... or youtube.com/shorts/...)
    duration_seconds: from the Data API, or None when not known yet. 0 also means
                      unknown (parse_iso8601_duration's value for a live stream or
                      bad input), so it never makes a video 'short'.
    """
    # Primary signal — the URL path. Checked on the path only, so "/shorts/"
    # inside a query string or fragment does not count, and neither does a
    # channel's Shorts tab (/@name/shorts).
    if urlsplit(url or "").path.startswith(SHORTS_PATH_PREFIX):
        return "short"

    # Fallback — duration from the YouTube Data API.
    if duration_seconds is not None and 0 < duration_seconds < SHORT_MAX_DURATION_SECONDS:
        return "short"

    return "long"
