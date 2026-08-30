"""
wellbeing.py — Backend Module
Echo Personal Memory System

FastAPI router exposing the WBA (Digital Wellbeing Analytics) module as HTTP
endpoints for the React dashboard and extension popup (architecture §12.4).

Endpoints (prefix /wellbeing):
    Analytics:
        GET  /analytics/daily            — daily summary (time, categories, regret, focus)
        GET  /analytics/heatmap          — hour × day activity matrix with regret markers
        GET  /recent                     — last N items across all sources
    Sessions:
        POST /sessions/rebuild           — (re)compute sessions for a day
        GET  /sessions                   — stored sessions for a day + focus scores
    User groups:
        GET  /groups                     — list groups with member counts
        POST /groups                     — create group
        DELETE /groups/{group_id}        — deactivate (soft delete)
        GET  /groups/{group_id}/items    — confirmed members
        POST /groups/{group_id}/assign   — manual seeding (Phase 1)
        GET  /groups/{group_id}/rules    — list rules
        POST /groups/{group_id}/rules    — add rule
        POST /groups/auto-assign         — run hybrid classification (Phase 2)
        GET  /groups/review              — weekly review queue (Phase 3)
        POST /suggestions/{id}/decision  — accept/reject a suggestion
    Regret:
        POST /regret/{memory_id}         — toggle regret mark (with optional note)
        GET  /regret/analytics           — rate, by hour, by category, item list
        GET  /regret/reminder            — rate-limited reminder check
        POST /regret/reminders/settings  — enable/disable reminders
    Insights:
        GET  /insights/weekly            — aggregated weekly insight (LLM or fallback)
"""

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/wellbeing", tags=["Wellbeing Analytics"])


# ── Request models ────────────────────────────────────────────────────────────

class GroupCreateRequest(BaseModel):
    group_name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)


class RuleCreateRequest(BaseModel):
    rule_type: str = Field(..., description="keyword | domain | channel | sender | time_window")
    rule_value: str = Field(..., min_length=1, max_length=500)


class ManualAssignRequest(BaseModel):
    memory_id: str = Field(..., min_length=1, max_length=64)


class SuggestionDecisionRequest(BaseModel):
    accept: bool


class RegretToggleRequest(BaseModel):
    note: Optional[str] = Field(None, max_length=1000)


class ReminderSettingsRequest(BaseModel):
    enabled: bool


# ── Analytics ─────────────────────────────────────────────────────────────────

@router.get("/analytics/daily")
def analytics_daily(day: Optional[date] = Query(None, description="Defaults to today")):
    from wba.wellbeing_analytics import daily_summary

    try:
        return daily_summary(day)
    except Exception as e:
        logger.error(f"analytics_daily error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/analytics/heatmap")
def analytics_heatmap(days: int = Query(7, ge=1, le=90)):
    from datetime import timedelta

    from wba.time_aggregator import activity_heatmap

    try:
        end_day = date.today()
        start_day = end_day - timedelta(days=days - 1)
        return {
            "start_day": start_day.isoformat(),
            "end_day": end_day.isoformat(),
            "cells": activity_heatmap(start_day, end_day),
        }
    except Exception as e:
        logger.error(f"analytics_heatmap error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/recent")
def recent(limit: int = Query(10, ge=1, le=100)):
    from wba.time_aggregator import recent_items

    try:
        items = recent_items(limit)
        for item in items:
            item["memory_id"] = str(item["memory_id"])
        return {"items": items}
    except Exception as e:
        logger.error(f"recent error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ── Sessions ──────────────────────────────────────────────────────────────────

@router.post("/sessions/rebuild")
def sessions_rebuild(day: Optional[date] = Query(None, description="Defaults to today")):
    from wba.wellbeing_analytics import rebuild_sessions_for_day

    try:
        return rebuild_sessions_for_day(day or date.today())
    except Exception as e:
        logger.error(f"sessions_rebuild error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/sessions")
def sessions(day: Optional[date] = Query(None, description="Defaults to today")):
    from wba.wellbeing_analytics import fragmentation_for_day, sessions_for_day

    try:
        target = day or date.today()
        return {
            "day": target.isoformat(),
            "sessions": sessions_for_day(target),
            "fragmentation": fragmentation_for_day(target),
        }
    except Exception as e:
        logger.error(f"sessions error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ── User groups ───────────────────────────────────────────────────────────────

@router.get("/groups")
def groups_list(include_inactive: bool = Query(False)):
    from wba.group_manager import list_groups

    try:
        return {"groups": list_groups(include_inactive)}
    except Exception as e:
        logger.error(f"groups_list error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/groups", status_code=201)
def groups_create(request: GroupCreateRequest):
    from wba.group_manager import create_group

    try:
        return create_group(request.group_name, request.description)
    except Exception as e:
        logger.error(f"groups_create error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/groups/{group_id}")
def groups_deactivate(group_id: str):
    from wba.group_manager import deactivate_group

    try:
        if not deactivate_group(group_id):
            raise HTTPException(status_code=404, detail="Group not found")
        return {"group_id": group_id, "is_active": False}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"groups_deactivate error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/groups/review")
def groups_review(group_id: Optional[str] = Query(None)):
    """Weekly review queue — MUST be declared before /groups/{group_id} routes."""
    from wba.group_manager import review_queue

    try:
        return {"suggestions": review_queue(group_id)}
    except Exception as e:
        logger.error(f"groups_review error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/groups/auto-assign")
def groups_auto_assign():
    from wba.group_manager import run_auto_assignment

    try:
        return run_auto_assignment()
    except Exception as e:
        logger.error(f"groups_auto_assign error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/groups/{group_id}/items")
def groups_items(group_id: str, limit: int = Query(100, ge=1, le=500)):
    from wba.group_manager import group_items

    try:
        return {"group_id": group_id, "items": group_items(group_id, limit)}
    except Exception as e:
        logger.error(f"groups_items error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/groups/{group_id}/assign")
def groups_manual_assign(group_id: str, request: ManualAssignRequest):
    from wba.group_manager import manual_assign

    try:
        return manual_assign(request.memory_id, group_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"groups_manual_assign error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/groups/{group_id}/rules")
def groups_rules_list(group_id: str, include_inactive: bool = Query(False)):
    from wba.group_manager import list_rules

    try:
        return {"group_id": group_id, "rules": list_rules(group_id, include_inactive)}
    except Exception as e:
        logger.error(f"groups_rules_list error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/groups/{group_id}/rules", status_code=201)
def groups_rules_create(group_id: str, request: RuleCreateRequest):
    from wba.group_manager import add_rule

    try:
        return add_rule(group_id, request.rule_type, request.rule_value)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"groups_rules_create error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/groups/{group_id}/rule-suggestions")
def groups_rule_suggestions(group_id: str):
    """Step 9 of the 9-step flow: patterns shared by 5+ accepted members,
    offered as candidate rules. User approves via POST .../rules."""
    from wba.group_manager import suggest_rules_for_group

    try:
        return {"group_id": group_id, "suggestions": suggest_rules_for_group(group_id)}
    except Exception as e:
        logger.error(f"groups_rule_suggestions error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/suggestions/{suggestion_id}/decision")
def suggestion_decision(suggestion_id: str, request: SuggestionDecisionRequest):
    from wba.group_manager import decide_suggestion

    try:
        return decide_suggestion(suggestion_id, request.accept)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"suggestion_decision error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ── Regret ────────────────────────────────────────────────────────────────────

@router.post("/regret/{memory_id}")
def regret_toggle(memory_id: str, request: RegretToggleRequest | None = None):
    from wba.regret_system import toggle_regret

    try:
        note = request.note if request else None
        return toggle_regret(memory_id, note)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"regret_toggle error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/regret/analytics")
def regret_analytics(day: Optional[date] = Query(None)):
    from wba.regret_system import regret_by_category, regret_by_hour, regret_rate, regretted_items

    try:
        return {
            "rate": regret_rate(day or date.today()),
            "by_hour": regret_by_hour(),
            "by_category": regret_by_category(),
            "items": regretted_items(),
        }
    except Exception as e:
        logger.error(f"regret_analytics error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/regret/reminder")
def regret_reminder(
    source_type: str = Query(..., description="gmail | chrome | youtube"),
    session_minutes: int = Query(..., ge=0),
):
    from wba.regret_system import maybe_get_reminder

    try:
        reminder = maybe_get_reminder(source_type, session_minutes)
        return {"reminder": reminder}
    except Exception as e:
        logger.error(f"regret_reminder error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/regret/reminders/settings")
def regret_reminder_settings(request: ReminderSettingsRequest):
    from wba.regret_system import reminders_enabled, set_reminders_enabled

    try:
        applied = set_reminders_enabled(request.enabled)
        if not applied:
            raise HTTPException(status_code=503, detail="Reminder settings unavailable (Redis down)")
        return {"reminders_enabled": reminders_enabled()}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"regret_reminder_settings error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ── Insights ──────────────────────────────────────────────────────────────────

@router.get("/insights/weekly")
def insights_weekly(week_end: Optional[date] = Query(None)):
    from wba.insight_generator import generate_weekly_insight

    try:
        return generate_weekly_insight(week_end)
    except Exception as e:
        logger.error(f"insights_weekly error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
