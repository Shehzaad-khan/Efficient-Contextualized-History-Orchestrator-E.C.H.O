"""
items.py — Backend Module
Echo Personal Memory System

User data control routes (architecture §12.4 / §13.3):

    DELETE /items/{memory_id}   — soft-delete a memory item (is_deleted=TRUE)
    GET    /export              — export all memory data as JSON or CSV
    GET    /settings            — current capture settings
    POST   /settings            — update capture settings (source toggles,
                                  domain exclusions, sender exclusions)

Soft delete never destroys rows — is_deleted=TRUE hides the item from search
(RSE filters on it) and from future exports. Export includes only non-deleted
items and never includes FAISS vectors or OAuth material.
"""

from __future__ import annotations

import csv
import io
import json
import logging
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field

from ste import postgresql_manager, settings_store

logger = logging.getLogger(__name__)

router = APIRouter(tags=["User Data Controls"])

_EXPORT_SQL = """
SELECT
    m.memory_id,
    m.source_type,
    m.title,
    m.created_at,
    m.first_ingested_at,
    m.auto_keywords,
    sg.group_name AS system_group,
    m.classified_by,
    me.dwell_time_seconds,
    me.watch_time_seconds,
    me.first_opened_at,
    me.last_accessed_at,
    gm.sender,
    gm.subject,
    gm.thread_id,
    gm.has_attachments,
    cm.url,
    cm.domain,
    cm.scroll_depth,
    cm.revisit_count,
    ym.video_id,
    ym.channel_name,
    ym.is_short,
    ym.duration_seconds
FROM memory_items m
LEFT JOIN system_groups     sg ON m.system_group_id = sg.system_group_id
LEFT JOIN memory_engagement me ON m.memory_id = me.memory_id
LEFT JOIN gmail_metadata    gm ON m.memory_id = gm.memory_id
LEFT JOIN chrome_metadata   cm ON m.memory_id = cm.memory_id
LEFT JOIN youtube_metadata  ym ON m.memory_id = ym.memory_id
WHERE m.is_deleted = FALSE
ORDER BY m.created_at DESC
"""


@router.delete("/items/{memory_id}")
def soft_delete_item(memory_id: UUID):
    """Soft-delete one memory item. CASCADE is not needed — the row stays,
    flagged is_deleted, and every query path filters on that flag."""
    try:
        result = postgresql_manager.execute(
            """
            UPDATE memory_items
            SET is_deleted = TRUE, last_updated_at = NOW()
            WHERE memory_id = :memory_id AND is_deleted = FALSE
            """,
            {"memory_id": str(memory_id)},
        )
    except Exception as exc:
        logger.error("soft_delete_item failed for %s: %s", memory_id, exc)
        raise HTTPException(status_code=500, detail="Internal server error")

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Item not found or already deleted")
    return {"status": "deleted", "memory_id": str(memory_id)}


@router.get("/export")
def export_data(format: str = Query("json", pattern="^(json|csv)$")):
    """Export all non-deleted memory items with engagement and source metadata."""
    try:
        rows = postgresql_manager.fetchall(_EXPORT_SQL)
    except Exception as exc:
        logger.error("export failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")

    if format == "csv":
        buffer = io.StringIO()
        if rows:
            writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            for row in rows:
                writer.writerow({k: "" if v is None else v for k, v in row.items()})
        return Response(
            content=buffer.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=echo_export.csv"},
        )

    return Response(
        content=json.dumps({"item_count": len(rows), "items": rows}, default=str),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=echo_export.json"},
    )


class SettingsUpdate(BaseModel):
    gmail_enabled: Optional[bool] = None
    chrome_enabled: Optional[bool] = None
    youtube_enabled: Optional[bool] = None
    excluded_domains: Optional[list[str]] = Field(None, max_length=500)
    excluded_senders: Optional[list[str]] = Field(None, max_length=500)


@router.get("/settings")
def get_settings():
    return settings_store.get_settings(use_cache=False)


@router.post("/settings")
def update_settings(update: SettingsUpdate):
    updates = {k: v for k, v in update.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(status_code=400, detail="No settings provided")
    try:
        return settings_store.save_settings(updates)
    except Exception as exc:
        logger.error("update_settings failed: %s", exc)
        raise HTTPException(status_code=503, detail="Settings storage unavailable")
