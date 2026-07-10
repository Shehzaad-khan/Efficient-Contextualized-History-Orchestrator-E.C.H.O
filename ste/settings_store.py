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
"""

from __future__ import annotations

import json
import logging
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
    "excluded_domains": [],   # exact-match, lowercase hostnames
    "excluded_senders": [],   # matched as substring of the From address, lowercase
}

_cache: dict[str, Any] | None = None
_cache_loaded_at: float = 0.0


def _normalize(raw: dict[str, Any]) -> dict[str, Any]:
    settings = dict(DEFAULT_SETTINGS)
    for key in ("gmail_enabled", "chrome_enabled", "youtube_enabled"):
        if key in raw:
            settings[key] = bool(raw[key])
    for key in ("excluded_domains", "excluded_senders"):
        values = raw.get(key)
        if isinstance(values, list):
            settings[key] = sorted({str(v).strip().lower() for v in values if str(v).strip()})
    return settings


def get_settings(use_cache: bool = True) -> dict[str, Any]:
    """Return current capture settings; defaults when Redis is unavailable."""
    global _cache, _cache_loaded_at
    now = time.monotonic()
    if use_cache and _cache is not None and (now - _cache_loaded_at) < _CACHE_TTL_SECONDS:
        return _cache

    settings = dict(DEFAULT_SETTINGS)
    try:
        stored = get_sync_client().get(SETTINGS_KEY)
        if stored:
            settings = _normalize(json.loads(stored))
    except Exception as exc:
        logger.warning("settings_store: Redis read failed — using defaults: %s", exc)

    _cache = settings
    _cache_loaded_at = now
    return settings


def save_settings(updates: dict[str, Any]) -> dict[str, Any]:
    """Merge updates into stored settings and persist. Raises on Redis failure —
    a settings write the user asked for must not silently no-op."""
    global _cache, _cache_loaded_at
    current = get_settings(use_cache=False)
    merged = _normalize({**current, **updates})
    get_sync_client().set(SETTINGS_KEY, json.dumps(merged))
    _cache = merged
    _cache_loaded_at = time.monotonic()
    return merged


# ── Hot-path helpers (default-allow on any failure) ──────────────────────────

def is_source_enabled(source_type: str) -> bool:
    return bool(get_settings().get(f"{source_type}_enabled", True))


def is_domain_excluded(domain: str) -> bool:
    if not domain:
        return False
    domain = domain.strip().lower()
    excluded = get_settings().get("excluded_domains", [])
    # Exact match or subdomain of an excluded domain.
    return any(domain == d or domain.endswith(f".{d}") for d in excluded)


def is_sender_excluded(sender: str) -> bool:
    if not sender:
        return False
    sender = sender.strip().lower()
    return any(s in sender for s in get_settings().get("excluded_senders", []))
