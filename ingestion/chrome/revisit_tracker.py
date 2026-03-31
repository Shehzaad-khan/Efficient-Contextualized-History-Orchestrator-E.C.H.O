from __future__ import annotations

import hashlib
import os

import redis

REVISIT_TTL_SECONDS = 86400
BRIEF_VISIT_PREFIX = "echo:brief:"


def _get_redis_client():
    redis_url = os.environ.get("UPSTASH_REDIS_URL") or os.environ.get("REDIS_URL")
    if not redis_url:
        raise ValueError("No Redis URL configured")
    return redis.from_url(redis_url, decode_responses=True)


def _url_key(canonical_url: str) -> str:
    url_hash = hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()[:16]
    return f"{BRIEF_VISIT_PREFIX}{url_hash}"


def check_and_record_visit(canonical_url: str) -> bool:
    client = _get_redis_client()
    key = _url_key(canonical_url)
    existing = client.get(key)
    client.setex(key, REVISIT_TTL_SECONDS, "1")
    return existing is not None
