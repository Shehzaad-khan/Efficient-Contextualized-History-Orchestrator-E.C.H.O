from __future__ import annotations

from ste.redis_manager import REVISIT_TTL_SECONDS, check_and_record_revisit


def record_visit(canonical_url: str) -> None:
    check_and_record_revisit("chrome", canonical_url, ttl_seconds=REVISIT_TTL_SECONDS)


def check_and_record_visit(canonical_url: str) -> bool:
    return check_and_record_revisit("chrome", canonical_url, ttl_seconds=REVISIT_TTL_SECONDS)
