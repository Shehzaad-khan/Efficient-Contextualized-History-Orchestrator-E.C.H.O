from __future__ import annotations

from backend.redis_manager import REVISIT_TTL_SECONDS, check_and_record_revisit


def check_and_record_visit(canonical_url: str) -> bool:
    return check_and_record_revisit("chrome", canonical_url, ttl_seconds=REVISIT_TTL_SECONDS)
