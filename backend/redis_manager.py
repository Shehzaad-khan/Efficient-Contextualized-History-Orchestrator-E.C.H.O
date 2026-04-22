from __future__ import annotations

import hashlib
import os
from typing import Optional

import redis
import redis.asyncio as aioredis

REVISIT_TTL_SECONDS = 86400
ATTACHMENT_CACHE_TTL_SECONDS = 3600

_sync_client: redis.Redis | None = None
_async_client: aioredis.Redis | None = None


def get_redis_url() -> str:
    redis_url = os.environ.get("UPSTASH_REDIS_URL") or os.environ.get("REDIS_URL")
    if not redis_url:
        raise ValueError("No Redis URL configured")
    return redis_url


def get_sync_client() -> redis.Redis:
    global _sync_client
    if _sync_client is None:
        _sync_client = redis.from_url(get_redis_url(), decode_responses=True)
    return _sync_client


def get_async_client() -> aioredis.Redis:
    global _async_client
    if _async_client is None:
        _async_client = aioredis.from_url(get_redis_url(), decode_responses=True)
    return _async_client


def _hash_key(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def revisit_key(namespace: str, raw_key: str) -> str:
    return f"echo:revisit:{namespace}:{_hash_key(raw_key)}"


def attachment_cache_key(memory_id: str, attachment_id: str) -> str:
    return f"echo:attachment:{memory_id}:{attachment_id}"


def check_and_record_revisit(namespace: str, raw_key: str, ttl_seconds: int = REVISIT_TTL_SECONDS) -> bool:
    client = get_sync_client()
    key = revisit_key(namespace, raw_key)
    existed = client.exists(key) > 0
    client.setex(key, ttl_seconds, "1")
    return existed


async def check_and_record_revisit_async(
    namespace: str,
    raw_key: str,
    ttl_seconds: int = REVISIT_TTL_SECONDS,
) -> bool:
    client = get_async_client()
    key = revisit_key(namespace, raw_key)
    existed = bool(await client.exists(key))
    await client.setex(key, ttl_seconds, "1")
    return existed


def cache_attachment_text(memory_id: str, attachment_id: str, text_value: str) -> None:
    get_sync_client().setex(
        attachment_cache_key(memory_id, attachment_id),
        ATTACHMENT_CACHE_TTL_SECONDS,
        text_value,
    )


def get_attachment_text(memory_id: str, attachment_id: str) -> Optional[str]:
    return get_sync_client().get(attachment_cache_key(memory_id, attachment_id))
