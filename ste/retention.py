"""
retention.py — STE Module
Echo Personal Memory System

A37 data retention: the user picks how long items are kept (Settings →
Retention: forever / 1 year / 6 months, stored with the capture settings). The
job below runs once at backend startup and HARD-deletes every item older than
that window from every store. Deleting is always a hard delete: there is no
`is_deleted` flag any more, because a flag would leave the text in Postgres and the
vector in FAISS.

`delete_items()` is the single deletion path. The retention job calls it for expired
items, and `DELETE /items/{id}` (backend/items.py) calls it for one item the user
chose to forget. Both remove the item from every store.

Age is `memory_items.created_at` — when the thing happened (mail received,
page visited), not when Echo ingested it. 1 year = 365 days, 6 months = 183.

Order of deletion (the safe order if the job stops half way):
    1. Postgres  — DELETE FROM memory_items. Every child table (metadata,
                   engagement, embedding_index, group links, attachments,
                   sessions map, regret events, visits) is ON DELETE CASCADE.
    2. FAISS     — remove the vectors, then save the index to disk.
    3. Redis     — the attachment-text cache, the ENP retry counter and (Chrome) the
                   24 h revisit flag of the page's URL.
  A crash after step 1 leaves vectors with no row: search ignores them (the
  result join finds nothing) and the next run does not see them. The reverse
  order would leave rows without vectors, which search can never find again.

Two guards keep a wrong setting or a wrong clock from wiping the data:
    - the default is "forever", and any unknown value counts as "forever";
    - the job refuses to run when EVERY item is past the cutoff (a clock set
      far ahead, or no new item for the whole window).

What a delete deliberately leaves alone (owner's decisions): the `sessions` rows and
their stored totals, and the saved chat history (`message_store`).

Nothing blocks the item from being captured again later (its dedup key goes with the
row), so the same mail or page can be ingested as a new memory item.

Logging: counts and the cutoff date only — never titles, URLs or ids.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

from ste import postgresql_manager, settings_store
from ste.faiss_manager import get_manager
from ste.redis_manager import get_sync_client, revisit_key

logger = logging.getLogger(__name__)

RETENTION_DAYS: dict[str, int] = {"1y": 365, "6m": 183}

_EXPIRED_IDS_SQL = "SELECT memory_id::text AS memory_id FROM memory_items WHERE created_at < :cutoff"
_NEWEST_ITEM_SQL = "SELECT MAX(created_at) FROM memory_items"
_DELETE_SQL = "DELETE FROM memory_items WHERE memory_id = ANY(CAST(:ids AS uuid[]))"
# Read BEFORE the delete: the revisit flag is keyed by the page URL, which dies with the row.
_CHROME_URLS_SQL = "SELECT canonical_url FROM chrome_metadata WHERE memory_id = ANY(CAST(:ids AS uuid[]))"


class RetentionResult(NamedTuple):
    items_deleted: int
    vectors_removed: int
    redis_keys_deleted: int


_NOTHING_DELETED = RetentionResult(0, 0, 0)


def _delete_redis_keys(memory_ids: list[str]) -> int:
    """Delete the attachment cache and retry counter of these items. Best effort:
    Redis holds only rebuildable data, so a failure is logged and not raised."""
    deleted = 0
    try:
        client = get_sync_client()
        for memory_id in memory_ids:
            keys = list(client.scan_iter(match=f"echo:attachment:{memory_id}:*"))
            keys.append(f"echo:enp:retry:{memory_id}")
            deleted += client.delete(*keys)
    except Exception as exc:
        logger.warning("Retention: Redis cleanup stopped early (%d keys deleted): %s", deleted, exc)
    return deleted


def _revisit_keys(memory_ids: list[str]) -> list[str]:
    """Redis revisit-flag keys of the Chrome items among these ids (call before the delete)."""
    rows = postgresql_manager.fetchall(_CHROME_URLS_SQL, {"ids": memory_ids})
    return [revisit_key("chrome", row["canonical_url"]) for row in rows if row.get("canonical_url")]


def _delete_revisit_keys(keys: list[str]) -> int:
    """Best effort, like _delete_redis_keys: the flag expires after 24 h on its own."""
    if not keys:
        return 0
    try:
        return int(get_sync_client().delete(*keys))
    except Exception as exc:
        logger.warning("Delete: Redis revisit-flag cleanup failed: %s", exc)
        return 0


def delete_items(memory_ids: list[str]) -> RetentionResult:
    """Hard-delete these items from Postgres, FAISS and Redis (order: see the module docstring).

    Raises on a Postgres failure (nothing else was touched) and on a FAISS failure
    (the row is already gone; search ignores the orphan vector because its result
    join finds no row). Redis cleanup is best effort and never raises.
    """
    if not memory_ids:
        return _NOTHING_DELETED

    revisit_keys = _revisit_keys(memory_ids)

    postgresql_manager.execute(_DELETE_SQL, {"ids": memory_ids})

    manager = get_manager()
    vectors_removed = manager.remove_many(memory_ids)
    if vectors_removed:
        manager.save_index()

    redis_keys_deleted = _delete_redis_keys(memory_ids) + _delete_revisit_keys(revisit_keys)
    return RetentionResult(len(memory_ids), vectors_removed, redis_keys_deleted)


def purge_expired_items(now: datetime | None = None) -> RetentionResult:
    """Hard-delete every item older than the user's retention window.

    Args:
        now: naive UTC "current time" (tests pass their own; default is the clock).

    Returns:
        What was deleted; all zeros when retention is "forever", nothing is
        expired, or a guard refused the run. Raises on a database failure.
    """
    retention = settings_store.get_settings(use_cache=False)["retention"]
    days = RETENTION_DAYS.get(retention)
    if days is None:
        logger.info("Retention: keep forever — nothing to delete")
        return _NOTHING_DELETED

    current_time = now or datetime.now(timezone.utc).replace(tzinfo=None)
    cutoff = current_time - timedelta(days=days)

    newest = postgresql_manager.scalar(_NEWEST_ITEM_SQL)
    if newest is None:
        return _NOTHING_DELETED
    if newest < cutoff:
        logger.warning(
            "Retention: refusing to run — every item is older than the %s cutoff (%s). "
            "Check the system clock.",
            retention,
            cutoff.date(),
        )
        return _NOTHING_DELETED

    memory_ids = [row["memory_id"] for row in postgresql_manager.fetchall(_EXPIRED_IDS_SQL, {"cutoff": cutoff})]
    if not memory_ids:
        logger.info("Retention %s: no item older than %s", retention, cutoff.date())
        return _NOTHING_DELETED

    result = delete_items(memory_ids)

    logger.info(
        "Retention %s: deleted %d items older than %s (%d vectors, %d Redis keys)",
        retention,
        result.items_deleted,
        cutoff.date(),
        result.vectors_removed,
        result.redis_keys_deleted,
    )
    return result
