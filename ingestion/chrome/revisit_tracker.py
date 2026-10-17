"""
revisit_tracker.py — Chrome revisit signal (architecture §6.2, Phase 2's 4th condition)
Echo Personal Memory System

A page counts as a revisit when the same canonical URL was seen in the last
24 h. The "seen" flag is one Redis key per URL:

    echo:revisit:chrome:{sha256(canonical_url)[:16]}   value "1", TTL 24 h

Every visit restarts the 24 h window (sliding window), so "seen" means "within
the last 24 h", not "earlier today" as the doc words it.

The Redis work and its DEBUG log live in ste/redis_manager.check_and_record_revisit
(EXISTS + SETEX in one MULTI/EXEC, so two parallel requests can't both see "new").
Both functions are blocking: call them only from sync routes (FastAPI threadpool)
or via asyncio.to_thread. They raise if Redis is down; chrome_connector catches
and logs that.
"""

from __future__ import annotations

from ste.redis_manager import REVISIT_TTL_SECONDS, check_and_record_revisit


def record_visit(canonical_url: str) -> None:
    """Mark the URL as seen now (used by POST /chrome/ingest after Phase 1)."""
    check_and_record_revisit("chrome", canonical_url, ttl_seconds=REVISIT_TTL_SECONDS)


def check_and_record_visit(canonical_url: str) -> bool:
    """Return True if the URL was seen in the last 24 h, then mark it as seen now
    (used by POST /chrome/revisit-check, which the extension calls at Phase 1)."""
    return check_and_record_revisit("chrome", canonical_url, ttl_seconds=REVISIT_TTL_SECONDS)
