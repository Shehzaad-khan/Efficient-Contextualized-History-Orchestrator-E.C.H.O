"""
redis_manager.py — shared Redis clients and the small Redis-backed helpers.

Two clients, because the callers live in two worlds:

  Sync client  (get_sync_client)  — for code running in a plain thread:
      - Chrome revisit check   ingestion/chrome/revisit_tracker.py
        (called from the sync /chrome routes, which FastAPI runs in its threadpool)
      - capture settings       ste/settings_store.py (routes + all 3 connectors)
      - regret reminders       wba/regret_system.py (sync /wellbeing routes)
      - Tier-2 attachment text enp/attachment_processor.py (ENP thread, RSE)
  Async client (get_async_client) — for code running ON the asyncio event loop:
      - YouTube revisit check  ingestion/youtube/youtube_connector.py
        (async route; a sync call there would freeze the whole event loop)

Thread safety:
  - The sync client (redis.Redis + its ConnectionPool) is thread-safe; one
    instance is shared by every thread. Its lazy creation is guarded by
    `_client_lock` so two threads can't build two clients (two pools).
  - The async client must only be used from the event loop it was first used
    on (uvicorn's loop). Never call it from another thread.

Keys are namespaced `echo:*`. TTLs: revisit 24h, attachment text 1h.
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
from typing import Optional
from urllib.parse import urlparse

import redis
import redis.asyncio as aioredis

from ste.security import validate_redis_tls_url

logger = logging.getLogger(__name__)

REVISIT_TTL_SECONDS = 86400
ATTACHMENT_CACHE_TTL_SECONDS = 3600

# Fail fast instead of hanging a request/ENP thread when Redis is unreachable.
_SOCKET_TIMEOUT_SECONDS = 5

_sync_client: redis.Redis | None = None
_async_client: aioredis.Redis | None = None
_client_lock = threading.Lock()


def get_redis_url() -> str:
    # REDIS_URL first, matching backend/main.py and ingestion/gmail/config.py,
    # so every module talks to the same Redis when both variables are set.
    redis_url = os.environ.get("REDIS_URL") or os.environ.get("UPSTASH_REDIS_URL")
    if not redis_url:
        raise ValueError("No Redis URL configured (set REDIS_URL)")
    return validate_redis_tls_url(redis_url)


def _safe_target(redis_url: str) -> str:
    """host:port only — the full URL carries the password and must not be logged."""
    parsed = urlparse(redis_url)
    return f"{parsed.hostname}:{parsed.port or 6379}"


def get_sync_client() -> redis.Redis:
    """Return the shared thread-safe client, creating it on first use.

    Creating the client does not connect; the first command does. So this
    never fails on a down Redis — the caller's first command raises instead.
    """
    global _sync_client
    if _sync_client is not None:
        return _sync_client
    with _client_lock:
        if _sync_client is None:
            url = get_redis_url()
            _sync_client = redis.from_url(
                url,
                decode_responses=True,
                socket_connect_timeout=_SOCKET_TIMEOUT_SECONDS,
                socket_timeout=_SOCKET_TIMEOUT_SECONDS,
            )
            logger.info("Redis sync client created for %s", _safe_target(url))
    return _sync_client


def get_async_client() -> aioredis.Redis:
    """Return the shared asyncio client, creating it on first use.

    Only call this from the event loop (async code). The lock is not needed for
    the loop itself — there is no await between the check and the assignment —
    it just keeps creation safe if a thread ever calls this by mistake.
    """
    global _async_client
    if _async_client is not None:
        return _async_client
    with _client_lock:
        if _async_client is None:
            url = get_redis_url()
            _async_client = aioredis.from_url(
                url,
                decode_responses=True,
                socket_connect_timeout=_SOCKET_TIMEOUT_SECONDS,
                socket_timeout=_SOCKET_TIMEOUT_SECONDS,
            )
            logger.info("Redis async client created for %s", _safe_target(url))
    return _async_client


def _hash_key(value: str) -> str:
    # URLs can be long and contain query strings; a short hash keeps keys small.
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def revisit_key(namespace: str, raw_key: str) -> str:
    return f"echo:revisit:{namespace}:{_hash_key(raw_key)}"


def attachment_cache_key(memory_id: str, attachment_id: str) -> str:
    return f"echo:attachment:{memory_id}:{attachment_id}"


def check_and_record_revisit(namespace: str, raw_key: str, ttl_seconds: int = REVISIT_TTL_SECONDS) -> bool:
    """Return True if `raw_key` was seen in the last `ttl_seconds`, then record this visit.

    EXISTS + SETEX run in one MULTI/EXEC transaction: one round trip, and two
    concurrent requests for the same URL can't both see "not visited".
    Every visit restarts the 24h window (sliding window).
    """
    key = revisit_key(namespace, raw_key)
    with get_sync_client().pipeline(transaction=True) as pipe:
        pipe.exists(key)
        pipe.setex(key, ttl_seconds, "1")
        existed_count, _ = pipe.execute()
    is_revisit = existed_count > 0
    logger.debug("Revisit check %s:%s -> %s", namespace, raw_key[:80], is_revisit)
    return is_revisit


async def check_and_record_revisit_async(
    namespace: str,
    raw_key: str,
    ttl_seconds: int = REVISIT_TTL_SECONDS,
) -> bool:
    """Async twin of check_and_record_revisit, for code on the event loop."""
    key = revisit_key(namespace, raw_key)
    async with get_async_client().pipeline(transaction=True) as pipe:
        pipe.exists(key)
        pipe.setex(key, ttl_seconds, "1")
        existed_count, _ = await pipe.execute()
    is_revisit = existed_count > 0
    logger.debug("Revisit check %s:%s -> %s", namespace, raw_key[:80], is_revisit)
    return is_revisit


def cache_attachment_text(memory_id: str, attachment_id: str, text_value: str) -> None:
    """Cache extracted attachment text for 1h so repeat queries skip re-extraction."""
    get_sync_client().setex(
        attachment_cache_key(memory_id, attachment_id),
        ATTACHMENT_CACHE_TTL_SECONDS,
        text_value,
    )
    logger.debug("Cached attachment text %s/%s (%d chars)", memory_id, attachment_id, len(text_value))


def get_attachment_text(memory_id: str, attachment_id: str) -> Optional[str]:
    """Return cached attachment text, or None on a cache miss."""
    text_value = get_sync_client().get(attachment_cache_key(memory_id, attachment_id))
    logger.debug("Attachment cache %s for %s/%s", "hit" if text_value is not None else "miss", memory_id, attachment_id)
    return text_value
