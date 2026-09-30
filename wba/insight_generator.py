"""
insight_generator.py — WBA weekly LLM insights (architecture §11.7).

PRIVACY CONTRACT (non-negotiable): only AGGREGATED NUMBERS are sent to the
LLM — totals per category, regret counts, session patterns, peak hours,
Shorts stats. Never email subjects/bodies, page URLs/titles, or video titles.

The LLM is instructed to stay descriptive and non-judgmental (§11.1). On any
LLM failure a deterministic text summary is returned instead — insights never
depend on API availability.

Provider comes from the shared plug-and-play LLM_CONFIG (rse.config) so one
config line switches the whole app.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from wba import time_aggregator
from wba.pattern_detector import peak_activity_hours, regret_time_patterns, session_source_sequences
from wba.regret_system import regret_by_category

logger = logging.getLogger(__name__)

_INSIGHT_PROMPT = """You are Echo's wellbeing reflection assistant. Below are AGGREGATED weekly
statistics about one user's digital activity. Write a short reflection (3-5 sentences).

STRICT RULES:
- Purely descriptive. Point out patterns and correlations in the numbers.
- NEVER judge, score, predict problems, or prescribe behaviour changes.
- No phrases like "you should", "wasted", "too much", "unhealthy", "addicted".
- Regret data is the user's OWN declaration — acknowledge it neutrally.

WEEKLY AGGREGATES:
{aggregates}

Reflection:"""


def collect_weekly_aggregates(week_end: date | None = None) -> dict[str, Any]:
    """
    Gather the week's aggregated numbers (the ONLY payload the LLM may see).
    Week = 7 days ending on week_end (default: today).
    """
    end_day = week_end or date.today()
    start_day = end_day - timedelta(days=6)

    total_seconds = time_aggregator.total_time(start_day, end_day)
    by_group = time_aggregator.time_by_system_group(start_day, end_day)
    shorts = time_aggregator.shorts_summary(start_day, end_day)

    group_percent: dict[str, int] = {}
    if total_seconds:
        for row in by_group:
            group_percent[row["group_name"]] = round(row["total_seconds"] / total_seconds * 100)

    regret_categories = regret_by_category()
    regret_patterns = regret_time_patterns(lookback_days=7)
    top_regret_hours = [r["hour"] for r in regret_patterns["by_hour"][:2]]

    return {
        "week_start": start_day.isoformat(),
        "week_end": end_day.isoformat(),
        "total_hours": round(total_seconds / 3600.0, 1),
        "category_breakdown_percent": group_percent,
        "regret_count_by_category": [
            {"category": r["group_name"], "count": r["regret_count"]} for r in regret_categories
        ],
        "regret_peak_hours": top_regret_hours,
        "session_patterns": session_source_sequences(start_day, end_day),
        "peak_activity_hours": [r["hour"] for r in peak_activity_hours(start_day, end_day)],
        "shorts_count": shorts.get("shorts_count", 0),
        "shorts_time_minutes": round(int(shorts.get("shorts_seconds", 0) or 0) / 60),
    }


def _deterministic_summary(aggregates: dict[str, Any]) -> str:
    """Readable fallback reflection built without the LLM."""
    lines = [
        f"Week {aggregates['week_start']} to {aggregates['week_end']}: "
        f"{aggregates['total_hours']} hours of tracked activity."
    ]
    breakdown = aggregates.get("category_breakdown_percent") or {}
    if breakdown:
        parts = ", ".join(f"{name} {pct}%" for name, pct in breakdown.items() if pct)
        if parts:
            lines.append(f"Category split: {parts}.")
    regrets = aggregates.get("regret_count_by_category") or []
    total_regrets = sum(r["count"] for r in regrets)
    if total_regrets:
        lines.append(f"You marked {total_regrets} item(s) as regretful this week.")
    if aggregates.get("shorts_count"):
        lines.append(
            f"Shorts: {aggregates['shorts_count']} watched, "
            f"{aggregates['shorts_time_minutes']} minutes total."
        )
    patterns = aggregates.get("session_patterns") or []
    if patterns:
        top = patterns[0]
        lines.append(f"Most common session flow: {top['sequence']} ({top['occurrences']}×).")
    return " ".join(lines)


def generate_weekly_insight(week_end: date | None = None) -> dict[str, Any]:
    """
    Weekly insight: aggregates + a short reflection. Uses the configured LLM
    when available; falls back to the deterministic summary otherwise.
    """
    aggregates = collect_weekly_aggregates(week_end)

    reflection: str
    generated_by = "deterministic"
    try:
        import json

        from langchain.chat_models import init_chat_model
        from rse.config import LLM_CONFIG

        llm = init_chat_model(
            model=LLM_CONFIG["synthesizer_model"],
            model_provider=LLM_CONFIG["provider"],
            temperature=LLM_CONFIG.get("synthesizer_temperature", 0.3),
        )
        prompt = _INSIGHT_PROMPT.format(aggregates=json.dumps(aggregates, indent=2))
        response = llm.invoke(prompt)
        reflection = str(response.content).strip()
        if not reflection:
            raise ValueError("LLM returned an empty reflection")
        generated_by = "llm"
    except Exception as exc:
        logger.warning("generate_weekly_insight: LLM unavailable, using fallback — %s", exc)
        reflection = _deterministic_summary(aggregates)

    return {
        "aggregates": aggregates,
        "reflection": reflection,
        "generated_by": generated_by,
    }
