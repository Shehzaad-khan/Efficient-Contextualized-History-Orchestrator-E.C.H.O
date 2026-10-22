"""
settings_store.py — STE Module
Echo Personal Memory System

Capture settings (architecture §12.4 POST /api/settings, §13.3 user control
points): per-source enable/disable, excluded domains, excluded senders.

The locked 16-table schema has no settings table, so settings live in Redis
under a single JSON key — the same tradeoff already made for the regret
reminder disable flag. Redis is rebuildable/disposable; losing settings means
falling back to defaults (everything enabled, no exclusions), which is safe.

Ingestion connectors call the is_* helpers on their hot paths, so reads go
through a short in-process cache (30 s) — one Redis roundtrip per window, and
a Redis outage degrades to default-allow instead of blocking capture.

Callers:
    backend/items.py              GET/POST /settings (sync routes, threadpool)
    ingestion/chrome/...          is_source_enabled, is_domain_excluded
    ingestion/gmail/router.py     is_source_enabled   (async poller, event loop)
    ingestion/gmail/gmail_api.py  is_sender_excluded
    ingestion/youtube/...         is_source_enabled   (async route, event loop)

Thread safety:
    - `_cache_lock` guards the three cache globals so they always change
      together. The Redis read itself runs OUTSIDE the lock, so a slow Redis
      never makes every other thread wait.
    - `_cache_version` is bumped by every save. A reader that started before a
      save will not overwrite the newer saved value with what it read.
    - `_save_lock` serialises saves: two POSTs at once can't both read the
      old settings and lose one of the updates (read-modify-write race).
    - get_settings() returns a copy, so no caller can mutate the shared cache.

Blocking: a cache miss is a sync Redis GET (up to the 5 s socket timeout when
Redis is down). The two async callers run it on the event loop at most once
per 30 s window — tracked as a follow-up for those files.
"""

from __future__ import annotations

import copy
import json
import logging
import threading
import time
from typing import Any

from ste.redis_manager import get_sync_client

logger = logging.getLogger(__name__)

SETTINGS_KEY = "echo:settings:capture"
_CACHE_TTL_SECONDS = 30.0

DEFAULT_SETTINGS: dict[str, Any] = {
    "gmail_enabled": True,
    "chrome_enabled": True,
    "youtube_enabled": True,
    "excluded_domains": [],   # exact match or subdomain, lowercase hostnames
    "excluded_senders": [],   # matched as substring of the From address, lowercase
}

_BOOL_KEYS = ("gmail_enabled", "chrome_enabled", "youtube_enabled")
_LIST_KEYS = ("excluded_domains", "excluded_senders")

_cache: dict[str, Any] | None = None
_cache_loaded_at: float = 0.0
_cache_version: int = 0
_cache_lock = threading.Lock()
_save_lock = threading.Lock()


def _defaults() -> dict[str, Any]:
    # Deep copy: DEFAULT_SETTINGS holds lists, which must never be shared.
    return copy.deepcopy(DEFAULT_SETTINGS)


def _normalize(raw: dict[str, Any]) -> dict[str, Any]:
    """Keep only known keys; coerce types; lowercase, strip, de-duplicate and sort lists."""
    settings = _defaults()
    for key in _BOOL_KEYS:
        if key in raw:
            settings[key] = bool(raw[key])
    for key in _LIST_KEYS:
        values = raw.get(key)
        if isinstance(values, list):
            settings[key] = sorted({str(v).strip().lower() for v in values if str(v).strip()})
    return settings


def _read_stored_settings() -> dict[str, Any]:
    """Read settings from Redis.

    Returns defaults when the key is missing.
    Raises redis.RedisError when Redis is unreachable, and ValueError when the
    stored value is not a JSON object (json.JSONDecodeError is a ValueError).
    """
    stored = get_sync_client().get(SETTINGS_KEY)
    if not stored:
        return _defaults()
    raw = json.loads(stored)
    if not isinstance(raw, dict):
        raise ValueError("stored capture settings are not a JSON object")
    return _normalize(raw)


def _current_settings(use_cache: bool) -> dict[str, Any]:
    """Return the shared cached dict (do not mutate). Refreshes it from Redis
    when the cache is empty, older than 30 s, or use_cache is False."""
    global _cache, _cache_loaded_at

    with _cache_lock:
        if use_cache and _cache is not None and (time.monotonic() - _cache_loaded_at) < _CACHE_TTL_SECONDS:
            return _cache
        version_at_start = _cache_version
        previous = _cache

    read_ok = True
    try:
        settings = _read_stored_settings()
    except Exception as exc:
        read_ok = False
        logger.warning("Capture settings read failed — using defaults (everything allowed): %s", exc)
        settings = _defaults()

    with _cache_lock:
        if _cache_version != version_at_start:
            # A save landed while we were reading; its value is newer than ours.
            return _cache  # type: ignore[return-value]  (a save always sets _cache)
        _cache = settings
        _cache_loaded_at = time.monotonic()

    if read_ok and settings != previous:
        # First load, or changed from outside this process (e.g. another backend).
        logger.info(
            "Capture settings loaded (gmail=%s, chrome=%s, youtube=%s, "
            "excluded_domains=%d, excluded_senders=%d)",
            settings["gmail_enabled"],
            settings["chrome_enabled"],
            settings["youtube_enabled"],
            len(settings["excluded_domains"]),
            len(settings["excluded_senders"]),
        )
    return settings


def get_settings(use_cache: bool = True) -> dict[str, Any]:
    """Return current capture settings (a copy); defaults when Redis is unavailable."""
    return copy.deepcopy(_current_settings(use_cache))


def save_settings(updates: dict[str, Any]) -> dict[str, Any]:
    """Merge updates into stored settings and persist. Raises on Redis failure —
    a settings write the user asked for must not silently no-op.

    Reads the CURRENT stored value directly (never the default fallback): if
    Redis is briefly unreadable, falling back to defaults here would overwrite
    the user's saved exclusions with empty lists.
    """
    global _cache, _cache_loaded_at, _cache_version

    with _save_lock:
        try:
            current = _read_stored_settings()
        except ValueError as exc:
            # Corrupt stored value: start from defaults so the save repairs it,
            # instead of leaving the user unable to save forever.
            logger.warning("Stored capture settings were corrupt — rebuilding from defaults: %s", exc)
            current = _defaults()
        merged = _normalize({**current, **updates})
        get_sync_client().set(SETTINGS_KEY, json.dumps(merged))

        with _cache_lock:
            _cache_version += 1
            _cache = merged
            _cache_loaded_at = time.monotonic()

    logger.info(
        "Capture settings saved (changed keys: %s; gmail=%s, chrome=%s, "
        "youtube=%s, excluded_domains=%d, excluded_senders=%d)",
        sorted(updates),
        merged["gmail_enabled"],
        merged["chrome_enabled"],
        merged["youtube_enabled"],
        len(merged["excluded_domains"]),
        len(merged["excluded_senders"]),
    )
    return copy.deepcopy(merged)


# ── Hot-path helpers (default-allow on any failure) ──────────────────────────
# These read the shared cache without copying (they only read it) — they run
# on every ingested item.

def is_source_enabled(source_type: str) -> bool:
    return bool(_current_settings(use_cache=True).get(f"{source_type}_enabled", True))


def is_domain_excluded(domain: str) -> bool:
    if not domain:
        return False
    domain = domain.strip().lower()
    excluded = _current_settings(use_cache=True).get("excluded_domains", [])
    # Exact match or subdomain of an excluded domain.
    return any(domain == d or domain.endswith(f".{d}") for d in excluded)


def is_sender_excluded(sender: str) -> bool:
    if not sender:
        return False
    sender = sender.strip().lower()
    # Substring match: "newsletter@" excludes every sender address containing it.
    return any(s in sender for s in _current_settings(use_cache=True).get("excluded_senders", []))
