"""
WBA — Digital Wellbeing Analytics module.

Descriptive, non-judgmental analytics over Echo's captured memory:
time aggregation, session computation, user groups (hybrid rule+KNN engine
with human-in-the-loop approval), the user-declared regret system, pattern
detection, and LLM-generated weekly insights from aggregated numbers only.

Public HTTP surface: backend/wellbeing.py (FastAPI router, prefix /wellbeing).
"""
