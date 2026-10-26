"""
Shared test setup: make the repository importable and keep tests off real services.

The tests never need PostgreSQL, Redis, FAISS, Gmail or an OpenAI key. Where a driver is not installed
(psycopg2 on a bare machine) a stand-in module is registered so the RSE modules can still be imported;
where it is installed it is left alone and the tests patch the functions that would touch a database.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    import psycopg2  # noqa: F401
except ImportError:
    for _name in ("psycopg2", "psycopg2.pool", "psycopg2.extras", "psycopg2.extensions"):
        sys.modules[_name] = MagicMock()


# ── Markers: every test is tagged by the folder it lives in ───────────────────

def pytest_collection_modifyitems(config, items):
    import pytest

    for item in items:
        parts = Path(str(item.fspath)).parts
        for layer in ("unit", "integration", "system"):
            if layer in parts:
                item.add_marker(getattr(pytest.mark, layer))


# ── Fakes for the outside world (PostgreSQL and Redis) ────────────────────────

class FakeRedis:
    """A dictionary that answers the Redis calls Echo makes (strings, TTLs, counters, a MULTI pipeline)."""

    def __init__(self):
        self.data = {}
        self.ttls = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.data:
            return None
        self.data[key] = str(value)
        if ex:
            self.ttls[key] = ex
        return True

    def setex(self, key, seconds, value):
        self.data[key] = str(value)
        self.ttls[key] = seconds
        return True

    def exists(self, *keys):
        return sum(1 for key in keys if key in self.data)

    def delete(self, *keys):
        removed = 0
        for key in keys:
            if self.data.pop(key, None) is not None:
                removed += 1
        return removed

    def incr(self, key):
        self.data[key] = str(int(self.data.get(key, 0)) + 1)
        return int(self.data[key])

    def expire(self, key, seconds):
        self.ttls[key] = seconds
        return True

    def scan_iter(self, match=None):
        prefix = (match or "*").rstrip("*")
        return iter([key for key in list(self.data) if key.startswith(prefix)])

    def pipeline(self, transaction=True):
        return _FakePipeline(self)


class _FakePipeline:
    def __init__(self, client):
        self.client = client
        self.queue = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def __getattr__(self, name):
        def queued(*args, **kwargs):
            self.queue.append((name, args, kwargs))
            return self
        return queued

    def execute(self):
        results = [getattr(self.client, name)(*args, **kwargs) for name, args, kwargs in self.queue]
        self.queue = []
        return results


class FakeResult:
    """What SQLAlchemy returns from execute(): rows, .mappings(), .first(), .one(), .rowcount."""

    def __init__(self, rows=None, rowcount=None):
        self.rows = rows or []
        self.rowcount = rowcount if rowcount is not None else len(self.rows)

    def mappings(self):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def one(self):
        if len(self.rows) != 1:
            raise ValueError("expected exactly one row")
        return self.rows[0]

    def all(self):
        return list(self.rows)

    def __iter__(self):
        return iter(self.rows)

    def scalar(self):
        return next(iter(self.rows[0].values())) if self.rows else None


class FakeConnection:
    """A database connection that records every statement and answers from a handler.

    handler(sql, params) returns a list of row dicts, a FakeResult, an int (rowcount) or None (no rows).
    """

    def __init__(self, handler=None):
        self.handler = handler
        self.calls = []

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        self.calls.append((sql, params))
        answer = self.handler(sql, params) if self.handler else None
        if isinstance(answer, FakeResult):
            return answer
        if isinstance(answer, int):
            return FakeResult(rowcount=answer)
        return FakeResult(answer or [])

    def statements(self):
        return [sql for sql, _ in self.calls]


import contextlib  # noqa: E402

import pytest  # noqa: E402


@pytest.fixture
def fake_redis():
    return FakeRedis()


@pytest.fixture
def fake_result():
    return FakeResult


@pytest.fixture
def fake_connection():
    """Factory: fake_connection(handler) -> FakeConnection."""
    return FakeConnection


@pytest.fixture
def use_connection(monkeypatch):
    """Make ste.postgresql_manager.transaction() hand out the given fake connection."""
    from ste import postgresql_manager

    def install(connection):
        @contextlib.contextmanager
        def fake_transaction():
            yield connection

        monkeypatch.setattr(postgresql_manager, "transaction", fake_transaction)
        return connection

    return install


@pytest.fixture
def isolated_settings(monkeypatch, fake_redis):
    """Capture settings backed by a FakeRedis, with a clean in-process cache."""
    from ste import settings_store

    monkeypatch.setattr(settings_store, "get_sync_client", lambda: fake_redis)
    monkeypatch.setattr(settings_store, "_cache", None)
    monkeypatch.setattr(settings_store, "_cache_loaded_at", 0.0)
    return fake_redis


# ── A stand-in for the embedding model (no torch, no download) ────────────────

class FakeTokenizer:
    """Counts one token per whitespace-separated word and reports each word's end offset."""

    def num_special_tokens_to_add(self, pair=False):
        return 2

    def __call__(self, text, add_special_tokens=False, return_offsets_mapping=False, verbose=False):
        import re

        spans = [(m.start(), m.end()) for m in re.finditer(r"\S+", text)]
        return {"offset_mapping": spans, "input_ids": list(range(len(spans)))}


class FakeEmbeddingModel:
    """Bag-of-words hashing into 384 dimensions, unit length: texts that share words are close."""

    max_seq_length = 512
    device = "cpu"
    prompts = {"query": "Represent this sentence for searching relevant passages: ", "document": ""}

    def __init__(self):
        self.tokenizer = FakeTokenizer()

    def encode(self, texts, prompt_name=None, batch_size=32, show_progress_bar=False,
               convert_to_numpy=True, normalize_embeddings=False):
        import hashlib
        import re

        import numpy as np

        matrix = np.zeros((len(texts), 384), dtype=np.float32)
        for row, text in enumerate(texts):
            for word in re.findall(r"[a-z0-9]+", str(text).lower()):
                matrix[row, int(hashlib.md5(word.encode()).hexdigest(), 16) % 384] += 1.0
            if not matrix[row].any():
                matrix[row, 0] = 1.0
            matrix[row] /= np.linalg.norm(matrix[row])
        return matrix


@pytest.fixture
def fake_embedding_model(monkeypatch):
    from enp import embedding_generator

    model = FakeEmbeddingModel()
    monkeypatch.setattr(embedding_generator, "_model", model)
    return model


# ── A real SQL database for storage tests: in-memory SQLite shaped like Echo's schema ──
# PostgreSQL-only words (NOW, GREATEST, LEAST) are registered as SQLite functions and uuid defaults
# are random hex, so the SAME SQL strings the app runs can execute here. It is a stand-in for
# scripts/setup_db.py, used so the "UNIQUE(source_type, source_id)" rule is tested with real SQL.

SQLITE_SCHEMA = """
CREATE TABLE system_groups (system_group_id INTEGER PRIMARY KEY, group_name TEXT UNIQUE NOT NULL);
INSERT INTO system_groups VALUES (1,'work'),(2,'study'),(3,'entertainment'),(4,'personal'),(5,'misc');
CREATE TABLE memory_items (
    memory_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' || substr(hex(randomblob(2)), 2) || '-8' || substr(hex(randomblob(2)), 2) || '-' || hex(randomblob(6)))),
    system_group_id INTEGER NOT NULL REFERENCES system_groups(system_group_id),
    source_type TEXT NOT NULL CHECK (source_type IN ('gmail','chrome','youtube')),
    source_id TEXT NOT NULL,
    title TEXT, raw_text TEXT, auto_keywords TEXT, preprocessed BOOLEAN DEFAULT 0,
    classified_by TEXT DEFAULT 'pending', created_at TIMESTAMP NOT NULL,
    first_ingested_at TIMESTAMP, last_updated_at TIMESTAMP, is_deleted BOOLEAN DEFAULT 0,
    UNIQUE (source_type, source_id)
);
CREATE TABLE gmail_metadata (memory_id TEXT PRIMARY KEY, email_id TEXT NOT NULL, thread_id TEXT, sender TEXT,
    recipients TEXT, subject TEXT, received_at TIMESTAMP, has_attachments BOOLEAN DEFAULT 0, gmail_labels TEXT,
    is_sent BOOLEAN DEFAULT 0);
CREATE TABLE gmail_attachments (attachment_id INTEGER PRIMARY KEY AUTOINCREMENT, memory_id TEXT NOT NULL,
    filename TEXT, mime_type TEXT, file_size INTEGER, lightweight_extract TEXT, last_extracted_at TIMESTAMP,
    is_processed BOOLEAN DEFAULT 0, extracted_text TEXT);
CREATE TABLE chrome_metadata (memory_id TEXT PRIMARY KEY, url TEXT NOT NULL, canonical_url TEXT NOT NULL,
    domain TEXT, referrer TEXT, scroll_depth REAL DEFAULT 0, interaction_count INTEGER DEFAULT 0,
    revisit_count INTEGER DEFAULT 0, word_count INTEGER);
CREATE TABLE youtube_metadata (memory_id TEXT PRIMARY KEY, video_id TEXT NOT NULL, channel_name TEXT,
    channel_id TEXT, duration_seconds INTEGER, is_short BOOLEAN DEFAULT 0, transcript_text TEXT,
    youtube_category_id INTEGER);
CREATE TABLE memory_engagement (memory_id TEXT PRIMARY KEY, dwell_time_seconds INTEGER DEFAULT 0,
    watch_time_seconds INTEGER DEFAULT 0, first_opened_at TIMESTAMP, last_accessed_at TIMESTAMP,
    play_sessions_count INTEGER DEFAULT 0, completion_rate REAL);
CREATE TABLE visits (visit_id INTEGER PRIMARY KEY AUTOINCREMENT, memory_id TEXT NOT NULL,
    visited_at TIMESTAMP NOT NULL DEFAULT (datetime('now')), dwell_seconds INTEGER NOT NULL DEFAULT 0);
CREATE TABLE user_groups (group_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' || substr(hex(randomblob(2)), 2) || '-8' || substr(hex(randomblob(2)), 2) || '-' || hex(randomblob(6)))), group_name TEXT NOT NULL,
    description TEXT, is_active BOOLEAN DEFAULT 1, created_at TIMESTAMP DEFAULT (datetime('now')),
    updated_at TIMESTAMP DEFAULT (datetime('now')));
CREATE TABLE memory_user_groups (memory_id TEXT NOT NULL, group_id TEXT NOT NULL,
    assigned_at TIMESTAMP DEFAULT (datetime('now')), PRIMARY KEY (memory_id, group_id));
CREATE TABLE group_rules (rule_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' || substr(hex(randomblob(2)), 2) || '-8' || substr(hex(randomblob(2)), 2) || '-' || hex(randomblob(6)))), group_id TEXT NOT NULL,
    rule_type TEXT NOT NULL, rule_value TEXT NOT NULL, is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT (datetime('now')), match_count INTEGER DEFAULT 0);
CREATE TABLE group_suggestions (suggestion_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' || substr(hex(randomblob(2)), 2) || '-8' || substr(hex(randomblob(2)), 2) || '-' || hex(randomblob(6)))),
    memory_id TEXT NOT NULL, group_id TEXT NOT NULL, rule_score REAL, knn_score REAL,
    suggested_at TIMESTAMP DEFAULT (datetime('now')), decision TEXT DEFAULT 'pending', reviewed BOOLEAN DEFAULT 0,
    decided_at TIMESTAMP);
CREATE TABLE regret_events (regret_id INTEGER PRIMARY KEY AUTOINCREMENT, memory_id TEXT NOT NULL,
    marked_at TIMESTAMP DEFAULT (datetime('now')), regret_note TEXT, regret_hour INTEGER, regret_day_of_week INTEGER);
CREATE TABLE message_store (id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL, message TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT (datetime('now')));
"""


@pytest.fixture
def sqlite_db(monkeypatch):
    """Point ste.postgresql_manager at an in-memory SQLite database with Echo's tables."""
    import json
    import sqlite3
    from datetime import datetime, timezone

    from sqlalchemy import create_engine, event
    from sqlalchemy.pool import StaticPool

    from ste import postgresql_manager

    sqlite3.register_adapter(list, json.dumps)
    sqlite3.register_adapter(datetime, lambda value: value.isoformat(sep=" "))

    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(engine, "connect")
    def register_functions(dbapi_connection, _record):
        dbapi_connection.create_function(
            "NOW", 0, lambda: datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" "))
        dbapi_connection.create_function("GREATEST", 2, lambda a, b: max(x for x in (a, b) if x is not None))
        dbapi_connection.create_function("LEAST", 2, lambda a, b: min(x for x in (a, b) if x is not None))

    @event.listens_for(engine, "before_cursor_execute", retval=True)
    def postgres_alias_to_sqlite(_conn, _cursor, statement, parameters, _context, _executemany):
        # PostgreSQL writes "UPDATE table alias SET"; SQLite wants "UPDATE table AS alias SET".
        import re

        return re.sub(r"UPDATE (\w+) (?!AS\b|SET\b)(\w+)\s+SET", r"UPDATE \1 AS \2 SET", statement), parameters

    with engine.begin() as connection:
        for statement in SQLITE_SCHEMA.split(";\n"):
            if statement.strip():
                connection.exec_driver_sql(statement)
    monkeypatch.setattr(postgresql_manager, "_engine", engine)
    yield engine
    monkeypatch.setattr(postgresql_manager, "_engine", None)
    engine.dispose()


# ── A psycopg2-style connection (cursor(), `with conn`, fetchall) answered by a handler ──

class FakePgCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        sql = " ".join(str(sql).split())
        self.connection.calls.append((sql, params))
        answer = self.connection.handler(sql, params) if self.connection.handler else None
        if isinstance(answer, Exception):
            raise answer
        if isinstance(answer, int):
            self.rows, self.rowcount = [], answer
        else:
            self.rows = list(answer or [])
            self.rowcount = len(self.rows)

    def executemany(self, sql, seq):
        for params in seq:
            self.execute(sql, params)

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def __iter__(self):
        return iter(self.rows)


class FakePgConnection:
    """handler(sql, params) -> list of row tuples | int (rowcount) | an Exception to raise | None."""

    def __init__(self, handler=None):
        self.handler = handler
        self.calls = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self, *args, **kwargs):
        return FakePgCursor(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.commits += 1
        else:
            self.rollbacks += 1
        return False

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True

    def statements(self):
        return [sql for sql, _ in self.calls]


@pytest.fixture
def fake_pg():
    """Factory: fake_pg(handler) -> FakePgConnection."""
    return FakePgConnection
