from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine, Result

_engine: Engine | None = None


def get_database_url() -> str:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not set")
    return database_url


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(
            get_database_url(),
            future=True,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
    return _engine


@contextmanager
def transaction() -> Iterator[Connection]:
    with get_engine().begin() as connection:
        yield connection


def execute(query: str, params: dict[str, Any] | None = None) -> Result:
    with transaction() as connection:
        return connection.execute(text(query), params or {})


def fetchone(query: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    with get_engine().connect() as connection:
        result = connection.execute(text(query), params or {})
        row = result.mappings().first()
        return dict(row) if row else None


def fetchall(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    with get_engine().connect() as connection:
        result = connection.execute(text(query), params or {})
        return [dict(row) for row in result.mappings().all()]


def scalar(query: str, params: dict[str, Any] | None = None) -> Any:
    with get_engine().connect() as connection:
        return connection.execute(text(query), params or {}).scalar()


def legacy_gmail_counts() -> dict[str, int]:
    legacy_exists = scalar(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'gmail_memory'
        )
        """
    )
    if not legacy_exists:
        return {"gmail_memory": 0, "canonical_gmail": 0}

    legacy_count = int(scalar("SELECT COUNT(*) FROM gmail_memory") or 0)
    canonical_count = int(
        scalar(
            """
            SELECT COUNT(*)
            FROM memory_items mi
            JOIN gmail_metadata gm ON gm.memory_id = mi.memory_id
            WHERE mi.source_type = 'gmail'
            """
        )
        or 0
    )
    return {"gmail_memory": legacy_count, "canonical_gmail": canonical_count}


def drop_legacy_gmail_table() -> None:
    execute("DROP TABLE IF EXISTS gmail_memory")


def memory_items_unique_constraints() -> list[str]:
    rows = fetchall(
        """
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_name = 'memory_items'
          AND constraint_type = 'UNIQUE'
        ORDER BY constraint_name
        """
    )
    return [row["constraint_name"] for row in rows]
