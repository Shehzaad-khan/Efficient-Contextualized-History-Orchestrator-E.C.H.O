"""
wellbeing.py — Backend Module
Echo Personal Memory System

FastAPI router exposing the WBA (Digital Wellbeing Analytics) module as HTTP
endpoints for the React dashboard and extension popup (architecture §12.4).
backend/main.py mounts it only when ECHO_WBA_ENABLED=1.

Endpoints (prefix /wellbeing):
    Analytics:
        GET  /analytics/daily                  — daily summary (time, categories, regret, focus)
        GET  /analytics/heatmap                — hour × day activity matrix with regret markers
        GET  /recent                           — last N items across all sources
    Sessions:
        POST /sessions/rebuild                 — (re)compute sessions for a day
        GET  /sessions                         — stored sessions for a day + focus scores
    User groups:
        GET  /groups                           — list groups with member counts
        POST /groups                           — create group
        DELETE /groups/{group_id}              — deactivate (soft delete)
        GET  /groups/review                    — weekly review queue (Phase 3)
        POST /groups/auto-assign               — run hybrid classification (Phase 2)
        GET  /groups/{group_id}/items          — confirmed members
        POST /groups/{group_id}/assign         — manual seeding (Phase 1)
        GET  /groups/{group_id}/rules          — list rules
        POST /groups/{group_id}/rules          — add rule
        PATCH /groups/{group_id}/rules/{rule_id} — pause/resume a rule
        GET  /groups/{group_id}/rule-suggestions — candidate rules from shared patterns (step 9)
        POST /suggestions/{id}/decision        — accept/reject a suggestion
    Regret:
        POST /regret/{memory_id}               — toggle regret mark (with optional note)
        GET  /regret/analytics                 — rate, by hour, by category, item list
        GET  /regret/reminder                  — rate-limited reminder check
        POST /regret/reminders/settings        — enable/disable reminders
    Insights:
        GET  /insights/weekly                  — aggregated weekly insight (LLM or fallback)

Rules for every route:
    - Plain `def`: FastAPI runs it in its threadpool, because every wba call
      blocks on the DB, Redis or the LLM.
    - Ids (memory, group, suggestion) are UUID path/body fields, so a malformed
      id is a 422 here instead of a Postgres cast error (500) inside wba.
    - "Today" and days are the user's LOCAL days (see wba/__init__.py).
    - ValueError from wba = the caller's mistake (404 or 400, message passed on).
      Anything else is logged with its traceback and answered with a plain 500.
    - Never log notes, titles, group names or rule values — ids and counts only.
"""

import logging
from datetime import date, timedelta
from typing import Literal, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError

from wba.group_manager import (
    add_rule,
    create_group,
    deactivate_group,
    decide_suggestion,
    group_items,
    list_groups,
    list_rules,
    manual_assign,
    review_queue,
    run_auto_assignment,
    set_rule_active,
    suggest_rules_for_group,
)
from wba.insight_generator import generate_weekly_insight
from wba.regret_system import (
    maybe_get_reminder,
    regret_by_category,
    regret_by_hour,
    regret_rate,
    regretted_items,
    reminders_enabled,
    set_reminders_enabled,
    toggle_regret,
)
from wba.time_aggregator import activity_heatmap, recent_items
from wba.wellbeing_analytics import (
    daily_summary,
    fragmentation_for_day,
    rebuild_sessions_for_day,
    sessions_for_day,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/wellbeing", tags=["Wellbeing Analytics"])

ReminderSourceType = Literal["gmail", "chrome", "youtube"]


def _server_error(route: str) -> HTTPException:
    """Log the exception being handled (with traceback) and build a plain 500.

    Call only inside an `except` block. The client never sees the error text.
    """
    logger.exception("/wellbeing %s failed", route)
    return HTTPException(status_code=500, detail="Internal server error")


# ── Request models ────────────────────────────────────────────────────────────

class GroupCreateRequest(BaseModel):
    group_name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)


class RuleCreateRequest(BaseModel):
    # The allowed types are checked once, in wba.group_manager.add_rule (400).
    rule_type: str = Field(..., description="keyword | domain | channel | sender | time_window")
    rule_value: str = Field(..., min_length=1, max_length=500)


class RuleActiveRequest(BaseModel):
    active: bool


class ManualAssignRequest(BaseModel):
    memory_id: UUID


class SuggestionDecisionRequest(BaseModel):
    accept: bool


class RegretToggleRequest(BaseModel):
    note: Optional[str] = Field(None, max_length=1000)


class ReminderSettingsRequest(BaseModel):
    enabled: bool


# ── Analytics ─────────────────────────────────────────────────────────────────

@router.get("/analytics/daily")
def analytics_daily(day: Optional[date] = Query(None, description="Defaults to today")):
    try:
        return daily_summary(day)
    except Exception:
        raise _server_error("analytics/daily")


@router.get("/analytics/heatmap")
def analytics_heatmap(days: int = Query(7, ge=1, le=90)):
    try:
        end_day = date.today()
        start_day = end_day - timedelta(days=days - 1)
        return {
            "start_day": start_day.isoformat(),
            "end_day": end_day.isoformat(),
            "cells": activity_heatmap(start_day, end_day),
        }
    except Exception:
        raise _server_error("analytics/heatmap")


@router.get("/recent")
def recent(limit: int = Query(10, ge=1, le=100)):
    try:
        # memory_id comes back as a UUID; FastAPI's encoder writes it as a string.
        return {"items": recent_items(limit)}
    except Exception:
        raise _server_error("recent")


# ── Sessions ──────────────────────────────────────────────────────────────────

@router.post("/sessions/rebuild")
def sessions_rebuild(day: Optional[date] = Query(None, description="Defaults to today")):
    try:
        return rebuild_sessions_for_day(day or date.today())
    except Exception:
        raise _server_error("sessions/rebuild")


@router.get("/sessions")
def sessions(day: Optional[date] = Query(None, description="Defaults to today")):
    try:
        target = day or date.today()
        return {
            "day": target.isoformat(),
            "sessions": sessions_for_day(target),
            "fragmentation": fragmentation_for_day(target),
        }
    except Exception:
        raise _server_error("sessions")


# ── User groups ───────────────────────────────────────────────────────────────
# Static paths (/groups/review, /groups/auto-assign) are declared before the
# /groups/{group_id}/... routes, so "review" is never read as a group id.

@router.get("/groups")
def groups_list(include_inactive: bool = Query(False)):
    try:
        return {"groups": list_groups(include_inactive)}
    except Exception:
        raise _server_error("groups list")


@router.post("/groups", status_code=201)
def groups_create(request: GroupCreateRequest):
    try:
        return create_group(request.group_name, request.description)
    except ValueError as exc:
        # e.g. a name of only spaces
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        raise _server_error("groups create")


@router.delete("/groups/{group_id}")
def groups_deactivate(group_id: UUID):
    try:
        changed = deactivate_group(str(group_id))
    except Exception:
        raise _server_error("groups deactivate")
    if not changed:
        raise HTTPException(status_code=404, detail="Group not found")
    return {"group_id": str(group_id), "is_active": False}


@router.get("/groups/review")
def groups_review(group_id: Optional[UUID] = Query(None)):
    """Weekly review queue — MUST be declared before /groups/{group_id} routes."""
    try:
        return {"suggestions": review_queue(str(group_id) if group_id else None)}
    except Exception:
        raise _server_error("groups review")


@router.post("/groups/auto-assign")
def groups_auto_assign():
    try:
        return run_auto_assignment()
    except Exception:
        raise _server_error("groups auto-assign")


@router.get("/groups/{group_id}/items")
def groups_items(group_id: UUID, limit: int = Query(100, ge=1, le=500)):
    try:
        return {"group_id": str(group_id), "items": group_items(str(group_id), limit)}
    except Exception:
        raise _server_error("groups items")


@router.post("/groups/{group_id}/assign")
def groups_manual_assign(group_id: UUID, request: ManualAssignRequest):
    try:
        return manual_assign(str(request.memory_id), str(group_id))
    except ValueError as exc:
        # unknown item, or no active group with this id
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception:
        raise _server_error("groups manual assign")


@router.get("/groups/{group_id}/rules")
def groups_rules_list(group_id: UUID, include_inactive: bool = Query(False)):
    try:
        return {"group_id": str(group_id), "rules": list_rules(str(group_id), include_inactive)}
    except Exception:
        raise _server_error("groups rules list")


@router.post("/groups/{group_id}/rules", status_code=201)
def groups_rules_create(group_id: UUID, request: RuleCreateRequest):
    try:
        return add_rule(str(group_id), request.rule_type, request.rule_value)
    except ValueError as exc:
        # bad rule type, empty value, malformed time window
        raise HTTPException(status_code=400, detail=str(exc))
    except IntegrityError:
        # group_rules.group_id is a foreign key: no such group
        raise HTTPException(status_code=404, detail="Group not found")
    except Exception:
        raise _server_error("groups rules create")


@router.patch("/groups/{group_id}/rules/{rule_id}")
def groups_rule_set_active(group_id: UUID, rule_id: UUID, request: RuleActiveRequest):
    try:
        changed = set_rule_active(str(group_id), str(rule_id), request.active)
    except Exception:
        raise _server_error("groups rule active")
    if not changed:
        raise HTTPException(status_code=404, detail="Rule not found in group")
    return {"group_id": str(group_id), "rule_id": str(rule_id), "is_active": request.active}


@router.get("/groups/{group_id}/rule-suggestions")
def groups_rule_suggestions(group_id: UUID):
    """Step 9 of the 9-step flow: patterns shared by 5+ accepted members,
    offered as candidate rules. User approves via POST .../rules."""
    try:
        return {"group_id": str(group_id), "suggestions": suggest_rules_for_group(str(group_id))}
    except Exception:
        raise _server_error("groups rule suggestions")


@router.post("/suggestions/{suggestion_id}/decision")
def suggestion_decision(suggestion_id: UUID, request: SuggestionDecisionRequest):
    try:
        return decide_suggestion(str(suggestion_id), request.accept)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception:
        raise _server_error("suggestion decision")


# ── Regret ────────────────────────────────────────────────────────────────────

@router.post("/regret/{memory_id}")
def regret_toggle(memory_id: UUID, request: RegretToggleRequest | None = None):
    try:
        note = request.note if request else None
        return toggle_regret(str(memory_id), note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception:
        raise _server_error("regret toggle")


@router.get("/regret/analytics")
def regret_analytics(day: Optional[date] = Query(None)):
    try:
        return {
            "rate": regret_rate(day or date.today()),
            "by_hour": regret_by_hour(),
            "by_category": regret_by_category(),
            "items": regretted_items(),
        }
    except Exception:
        raise _server_error("regret analytics")


@router.get("/regret/reminder")
def regret_reminder(
    source_type: ReminderSourceType = Query(..., description="gmail | chrome | youtube"),
    session_minutes: int = Query(..., ge=0),
):
    # maybe_get_reminder never raises on Redis trouble; it returns None.
    try:
        return {"reminder": maybe_get_reminder(source_type, session_minutes)}
    except Exception:
        raise _server_error("regret reminder")


@router.post("/regret/reminders/settings")
def regret_reminder_settings(request: ReminderSettingsRequest):
    try:
        applied = set_reminders_enabled(request.enabled)
        enabled_now = reminders_enabled() if applied else None
    except Exception:
        raise _server_error("regret reminder settings")
    if not applied:
        raise HTTPException(status_code=503, detail="Reminder settings unavailable (Redis down)")
    return {"reminders_enabled": enabled_now}


# ── Insights ──────────────────────────────────────────────────────────────────

@router.get("/insights/weekly")
def insights_weekly(week_end: Optional[date] = Query(None)):
    try:
        return generate_weekly_insight(week_end)
    except Exception:
        raise _server_error("insights weekly")
