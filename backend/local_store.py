from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / "echo_local_memory.db"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get_connection(db_path: str | Path | None = None) -> sqlite3.Connection:
    target = Path(db_path) if db_path is not None else _DEFAULT_DB_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(target)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            payload TEXT NOT NULL
        )
        """
    )
    return conn


def append_record(source: str, payload: dict[str, Any], target_path: str | Path | None = None) -> dict[str, Any]:
    record = {
        "timestamp": utc_now_iso(),
        "source": source,
        "payload": payload,
    }

    path = Path(target_path) if target_path is not None else _DEFAULT_DB_PATH
    if path.suffix.lower() == ".json":
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                existing = {"records": []}
        else:
            existing = {"records": []}
        existing.setdefault("records", []).append(record)
        path.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")
        return record

    conn = _get_connection(path)
    try:
        conn.execute(
            "INSERT INTO memory_records (source, timestamp, payload) VALUES (?, ?, ?)",
            (source, record["timestamp"], json.dumps(payload, ensure_ascii=False)),
        )
        conn.commit()
    finally:
        conn.close()
    return record


def list_records(source: str | None = None, limit: int = 50, db_path: str | Path | None = None) -> list[dict[str, Any]]:
    conn = _get_connection(db_path)
    try:
        if source:
            rows = conn.execute(
                "SELECT source, timestamp, payload FROM memory_records WHERE source = ? ORDER BY id DESC LIMIT ?",
                (source, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT source, timestamp, payload FROM memory_records ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
    finally:
        conn.close()

    records: list[dict[str, Any]] = []
    for row in rows:
        records.append({
            "source": row["source"],
            "timestamp": row["timestamp"],
            "payload": json.loads(row["payload"]),
        })
    return records


def search_records(query: str, source: str | None = None, db_path: str | Path | None = None) -> list[dict[str, Any]]:
    q = (query or "").strip().lower()
    if not q:
        return list_records(source=source, limit=20, db_path=db_path)

    conn = _get_connection(db_path)
    try:
        clause = "" if source is None else " AND source = ?"
        params: list[Any] = [f"%{q}%", *([source] if source else [])]
        rows = conn.execute(
            f"SELECT source, timestamp, payload FROM memory_records WHERE lower(payload) LIKE ?{clause} ORDER BY id DESC LIMIT 20",
            params,
        ).fetchall()
    finally:
        conn.close()

    return [{
        "source": row["source"],
        "timestamp": row["timestamp"],
        "payload": json.loads(row["payload"]),
    } for row in rows]
