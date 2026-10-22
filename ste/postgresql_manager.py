"""
postgresql_manager.py — shared SQLAlchemy access to PostgreSQL (Neon).

One process-wide Engine (and its connection pool) serves every SQLAlchemy
user: wba/, enp/, backend/items.py, ingestion/gmail and ste/storage_engine.
rse/search_coordinator.py has its own psycopg2 pool and does not use this file.

Queries use SQLAlchemy `:name` parameters. Every helper here is synchronous
and does network I/O, so async code must call it through asyncio.to_thread
(or from a sync FastAPI route, which runs in the threadpool).

Logging: logger comes from backend/logging_config.py (configured once at
startup). Engine creation is logged at INFO. Every query is timed: slow ones
(>= 1s) are logged at WARNING with a one-line preview of the SQL; the rest at
DEBUG (hidden at INFO). Parameters are not logged. Failures are left to the
caller, which knows the context.

Thread safety: the Engine and its QueuePool are thread-safe. Each helper
checks out its own connection and returns it before it exits, so no
connection is ever shared between threads. The only shared state is the
lazily created `_engine`, which is guarded by `_engine_lock`.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine, Result

logger = logging.getLogger(__name__)

# Pool settings. pool_size + max_overflow = at most 15 connections per process.
_POOL_SIZE = 5
_MAX_OVERFLOW = 10
# Neon closes idle connections; recycling every 4 min keeps us inside that window.
_POOL_RECYCLE_SECONDS = 240

# A query at least this slow is logged at WARNING (Neon round trips are ~0.2-0.9s).
_SLOW_QUERY_SECONDS = 1.0
# How much of the SQL text to show in a query log line.
_QUERY_PREVIEW_CHARS = 120

_engine: Engine | None = None
_engine_lock = threading.Lock()


def get_database_url() -> str:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not set")
    return database_url


def get_engine() -> Engine:
    """Return the process-wide Engine, creating it on first use.

    Double-checked locking: the fast path reads `_engine` without the lock.
    Only the first callers take the lock, and the second check stops two
    threads that raced past the first check from building two engines
    (two pools = up to 30 Neon connections instead of 15).
    """
    global _engine
    if _engine is not None:
        return _engine
    with _engine_lock:
        if _engine is None:
            _engine = create_engine(
                get_database_url(),
                future=True,
                hide_parameters=True,
                # pool_pre_ping issues a validation SELECT 1 before every checkout.
                # Against Neon in us-east-1 that measured ~190ms of the ~860ms each
                # operation cost — roughly 4s across a full retrieval query. Recycling
                # connections well inside Neon's idle window guards staleness instead.
                pool_pre_ping=False,
                pool_recycle=_POOL_RECYCLE_SECONDS,
                pool_size=_POOL_SIZE,
                max_overflow=_MAX_OVERFLOW,
            )
            # Never log the URL: it carries the database password.
            logger.info(
                "PostgreSQL engine created (pool_size=%d, max_overflow=%d, recycle=%ds)",
                _POOL_SIZE,
                _MAX_OVERFLOW,
                _POOL_RECYCLE_SECONDS,
            )
    return _engine


def _log_query(helper: str, query: str, started_at: float) -> None:
    """Log how long one query took; WARNING when slow, DEBUG otherwise."""
    elapsed = time.perf_counter() - started_at
    preview = " ".join(query.split())[:_QUERY_PREVIEW_CHARS]
    if elapsed >= _SLOW_QUERY_SECONDS:
        logger.warning("Slow query in %s (%.2fs): %s", helper, elapsed, preview)
    else:
        logger.debug("%s took %.3fs: %s", helper, elapsed, preview)


@contextmanager
def transaction() -> Iterator[Connection]:
    """Yield a connection inside one transaction.

    Commits when the block exits normally; rolls back if it raises.
    Use this when several statements must succeed or fail together.
    """
    with get_engine().begin() as connection:
        yield connection


def execute(query: str, params: dict[str, Any] | None = None) -> Result:
    """Run one write statement (INSERT/UPDATE/DELETE) in its own transaction.

    The connection is committed and returned to the pool before this returns,
    so callers may read `result.rowcount` but must NOT fetch rows from the
    result. Use fetchone/fetchall/scalar for reads.
    """
    started_at = time.perf_counter()
    with transaction() as connection:
        result = connection.execute(text(query), params or {})
    # Timed after the block so the commit round trip is included.
    _log_query("execute", query, started_at)
    return result


def fetchone(query: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Return the first row as a dict, or None when the query matches nothing."""
    started_at = time.perf_counter()
    with get_engine().connect() as connection:
        row = connection.execute(text(query), params or {}).mappings().first()
    _log_query("fetchone", query, started_at)
    return dict(row) if row else None


def fetchall(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Return every row as a list of dicts (empty list when nothing matches)."""
    started_at = time.perf_counter()
    with get_engine().connect() as connection:
        rows = connection.execute(text(query), params or {}).mappings().all()
    _log_query("fetchall", query, started_at)
    return [dict(row) for row in rows]


def scalar(query: str, params: dict[str, Any] | None = None) -> Any:
    """Return the first column of the first row, or None when there is no row."""
    started_at = time.perf_counter()
    with get_engine().connect() as connection:
        value = connection.execute(text(query), params or {}).scalar()
    _log_query("scalar", query, started_at)
    return value
