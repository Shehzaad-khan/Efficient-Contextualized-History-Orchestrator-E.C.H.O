from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from uuid import uuid4

import psycopg2
from dotenv import load_dotenv
from fastapi import APIRouter
from pydantic import BaseModel, Field

from ingestion.chrome import intent_filter
from ingestion.chrome.revisit_tracker import check_and_record_visit

load_dotenv()

router = APIRouter(prefix="/chrome", tags=["Chrome Connector"])

IGNORED_QUERY_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
    "ref",
    "_hsenc",
    "mc_eid",
    "yclid",
}

APPLICATION_PATH_PREFIXES = (
    "github.com/issues",
    "github.com/pulls",
)


class ChromeIngestRequest(BaseModel):
    url: str
    canonical_url: str | None = None
    title: str
    domain: str
    dwell_seconds: int = Field(ge=0)
    scroll_depth: float = Field(ge=0.0, le=1.0)
    interaction_count: int = Field(ge=0)
    revisit_count: int = Field(default=0, ge=0)


class RevisitCheckRequest(BaseModel):
    canonical_url: str


def canonicalize_url(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    clean_query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in IGNORED_QUERY_PARAMS
    ]
    normalized = parsed._replace(query=urlencode(clean_query, doseq=True), fragment="")
    return urlunparse(normalized)


def is_skipped_page(url: str, domain: str) -> bool:
    normalized_url = (url or "").strip().lower()
    normalized_domain = (domain or "").strip().lower()
    if intent_filter.is_application_page(normalized_domain):
        return True
    return any(prefix in normalized_url for prefix in APPLICATION_PATH_PREFIXES)


def get_database_url() -> str:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not set")
    return database_url


@contextmanager
def get_connection() -> Iterator[psycopg2.extensions.connection]:
    connection = psycopg2.connect(get_database_url())
    connection.autocommit = False
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _fetch_existing_memory_id(cursor, canonical_url: str):
    cursor.execute(
        """
        SELECT memory_id
        FROM memory_items
        WHERE source_type = 'chrome' AND source_id = %s
        LIMIT 1
        """,
        (canonical_url,),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def _insert_or_get_memory_id(cursor, canonical_url: str, title: str):
    memory_id = str(uuid4())
    created_at = _utc_now_naive()
    cursor.execute(
        """
        INSERT INTO memory_items (
            memory_id,
            system_group_id,
            source_type,
            source_id,
            title,
            raw_text,
            preprocessed,
            classified_by,
            created_at,
            first_ingested_at,
            last_updated_at
        )
        VALUES (%s, 5, 'chrome', %s, %s, NULL, FALSE, 'pending', %s, NOW(), NOW())
        ON CONFLICT (source_type, source_id) DO NOTHING
        RETURNING memory_id
        """,
        (memory_id, canonical_url, title, created_at),
    )
    inserted = cursor.fetchone()
    if inserted:
        return inserted[0], True
    return _fetch_existing_memory_id(cursor, canonical_url), False


def _upsert_child_rows(cursor, memory_id, payload: ChromeIngestRequest, canonical_url: str):
    cursor.execute(
        """
        INSERT INTO chrome_metadata (
            memory_id,
            url,
            canonical_url,
            domain,
            referrer,
            scroll_depth,
            interaction_count,
            revisit_count,
            word_count
        )
        VALUES (%s, %s, %s, %s, NULL, %s, %s, %s, NULL)
        ON CONFLICT (memory_id) DO UPDATE
        SET url = EXCLUDED.url,
            canonical_url = EXCLUDED.canonical_url,
            domain = EXCLUDED.domain,
            scroll_depth = GREATEST(COALESCE(chrome_metadata.scroll_depth, 0.0), EXCLUDED.scroll_depth),
            interaction_count = COALESCE(chrome_metadata.interaction_count, 0) + EXCLUDED.interaction_count,
            revisit_count = COALESCE(chrome_metadata.revisit_count, 0) + EXCLUDED.revisit_count
        """,
        (
            memory_id,
            payload.url,
            canonical_url,
            payload.domain,
            payload.scroll_depth,
            payload.interaction_count,
            payload.revisit_count,
        ),
    )

    cursor.execute(
        """
        INSERT INTO memory_engagement (
            memory_id,
            dwell_time_seconds,
            watch_time_seconds,
            first_opened_at,
            last_accessed_at,
            play_sessions_count
        )
        VALUES (%s, %s, 0, NOW(), NOW(), 1)
        ON CONFLICT (memory_id) DO UPDATE
        SET dwell_time_seconds = COALESCE(memory_engagement.dwell_time_seconds, 0) + EXCLUDED.dwell_time_seconds,
            last_accessed_at = NOW(),
            play_sessions_count = COALESCE(memory_engagement.play_sessions_count, 0) + 1
        """,
        (memory_id, payload.dwell_seconds),
    )


@router.post("/ingest")
def ingest_chrome_page(payload: ChromeIngestRequest):
    canonical_url = payload.canonical_url or canonicalize_url(payload.url)

    if is_skipped_page(payload.url, payload.domain):
        return {"status": "discarded", "reason": "application_page"}

    if not intent_filter.evaluate(
        dwell_seconds=payload.dwell_seconds,
        scroll_depth=payload.scroll_depth,
        interaction_count=payload.interaction_count,
        revisit_count=payload.revisit_count,
    ):
        return {"status": "discarded"}

    with get_connection() as connection:
        with connection.cursor() as cursor:
            memory_id, _ = _insert_or_get_memory_id(
                cursor=cursor,
                canonical_url=canonical_url,
                title=payload.title,
            )
            if memory_id is None:
                raise ValueError("Failed to resolve memory_id for chrome ingestion")

            _upsert_child_rows(cursor, memory_id, payload, canonical_url)

    return {"status": "saved", "memory_id": str(memory_id)}


@router.post("/revisit-check")
def revisit_check(payload: RevisitCheckRequest):
    try:
        return {"is_revisit": check_and_record_visit(payload.canonical_url)}
    except Exception:
        return {"is_revisit": False}
