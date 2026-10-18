"""
WBA — Digital Wellbeing Analytics module (architecture §11).

Descriptive, non-judgmental analytics over Echo's captured memory:
time aggregation, session computation, user groups (hybrid rule+KNN engine
with human-in-the-loop approval), the user-declared regret system, pattern
detection, and LLM-generated weekly insights from aggregated numbers only.

    time_aggregator.py      on-demand time totals, heatmap, recent items; local-day helpers
    regret_system.py        regret toggles, regret analytics, rate-limited reminders (Redis)
    pattern_detector.py     regret habits, peak hours, session source sequences
    wellbeing_analytics.py  session building (20-minute gap) + daily summary facade
    insight_generator.py    weekly reflection: aggregates -> one LLM call (rse.llm_factory)
    group_manager.py        user groups, rules, auto-assignment, the review workflow

Shared rules: days are the user's LOCAL days; DB timestamps are naive UTC and
go through time_aggregator.local_day_range_utc (regret marked_at is the one
local-time column, C22). Every function is blocking and runs in FastAPI's
threadpool.

Public HTTP surface: backend/wellbeing.py (FastAPI router, prefix /wellbeing).
Import the modules directly — this file re-exports nothing.
"""
