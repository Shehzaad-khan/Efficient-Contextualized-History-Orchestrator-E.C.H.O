"""
video_classifier.py — YTC Module
Echo Personal Memory System

Classifies YouTube videos as 'short' or 'long' based on URL pattern first,
duration as fallback. This is the architecture-specified classification logic.

Rules (from architecture doc):
  - URL contains '/shorts/' → 'short'
  - duration_seconds < 60   → 'short'
  - otherwise               → 'long'

Shorts are auto-assigned to 'entertainment' system_group downstream.
Regret system for Shorts operates at SESSION level, not individual video level.
"""


def classify_video_type(url: str, duration_seconds: int | None = None) -> str:
    """
    Classify a YouTube video as 'short' or 'long'.

    Args:
        url:              Full YouTube URL (e.g. youtube.com/watch?v=... or youtube.com/shorts/...)
        duration_seconds: Video duration from YouTube Data API. Can be None if
                          API call hasn't completed yet — URL pattern is checked first.

    Returns:
        'short' or 'long'
    """
    # Primary signal — URL pattern is definitive
    if "/shorts/" in url:
        return "short"

    # Fallback — duration from YouTube Data API
    if duration_seconds is not None and duration_seconds < 60:
        return "short"

    return "long"


def extract_video_id(url: str) -> str | None:
    """
    Extract the 11-character YouTube video ID from a URL.

    Handles:
      - youtube.com/watch?v=VIDEO_ID
      - youtube.com/watch?v=VIDEO_ID&t=30s  (with extra params)
      - youtube.com/shorts/VIDEO_ID
      - youtube.com/shorts/VIDEO_ID?feature=share

    Returns:
        11-character video ID string, or None if URL doesn't match known patterns.
    """
    if not url:
        return None

    # Pattern 1 — Regular video: ?v=VIDEO_ID
    if "watch?v=" in url:
        try:
            # Split on v= and take the first 11 chars (video ID is always 11 chars)
            video_id = url.split("watch?v=")[1][:11]
            if len(video_id) == 11:
                return video_id
        except IndexError:
            pass

    # Pattern 2 — Shorts: /shorts/VIDEO_ID
    if "/shorts/" in url:
        try:
            after_shorts = url.split("/shorts/")[1]
            # Strip any trailing query params
            video_id = after_shorts.split("?")[0].split("/")[0][:11]
            if len(video_id) == 11:
                return video_id
        except IndexError:
            pass

    return None


def is_youtube_url(url: str) -> bool:
    """
    Check if a URL is a trackable YouTube video URL.
    Filters out non-video pages (homepage, search, channel pages, etc.)

    Args:
        url: Browser URL string

    Returns:
        True if this URL should trigger YouTube tracking
    """
    if not url:
        return False

    trackable_patterns = [
        "youtube.com/watch?v=",
        "youtube.com/shorts/",
    ]

    return any(pattern in url for pattern in trackable_patterns)
