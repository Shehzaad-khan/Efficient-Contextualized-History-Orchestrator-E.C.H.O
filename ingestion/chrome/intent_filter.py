"""
intent_filter.py — Chrome two-phase intent gate (architecture §6.2)
Echo Personal Memory System

    Phase 1 (exposure):  foreground >= 5 s, else discard.
    Phase 2 (intent):    at least ONE of
                           foreground >= 10 s
                           scroll depth >= 15 %
                           >= 1 interaction (click not on a link, text selection)
                           a visit to the same URL in the last 24 h (Redis flag)

Pure functions, no state and no I/O: safe to call from any thread.
Used by chrome_connector.py, which logs the discard reason.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# These MUST match the extension's thresholds in extension/background.js
# (evaluateChromeIntent): the extension sends /chrome/ingest exactly once when
# ITS gate passes, so a stricter server gate silently discards the page with no
# retry ever happening.
PHASE1_MIN_SECONDS = 5
PHASE2_MIN_FOREGROUND = 10
PHASE2_MIN_SCROLL = 0.15
PHASE2_MIN_INTERACTIONS = 1

# Web apps whose content is private (§6.5): only time spent is kept, never text.
# Matches the domain itself and every subdomain. Must match the lists in
# extension/background.js and extension/content/chrome_tracker.js.
APPLICATION_DOMAINS = {
    "slack.com",
    "atlassian.net",
    "notion.so",
    "figma.com",
    "linear.app",
    "trello.com",
    "asana.com",
}


def is_application_page(domain: str) -> bool:
    normalized = (domain or "").strip().lower()
    return any(normalized == app_domain or normalized.endswith(f".{app_domain}") for app_domain in APPLICATION_DOMAINS)


def phase1_passes(dwell_seconds: int) -> bool:
    return dwell_seconds >= PHASE1_MIN_SECONDS


def phase2_passes(
    dwell_seconds: int,
    scroll_depth: float,
    interaction_count: int,
    is_revisit: bool,
) -> bool:
    return (
        dwell_seconds >= PHASE2_MIN_FOREGROUND
        or scroll_depth >= PHASE2_MIN_SCROLL
        or interaction_count >= PHASE2_MIN_INTERACTIONS
        or is_revisit
    )


def evaluate(
    dwell_seconds: int,
    scroll_depth: float,
    interaction_count: int,
    revisit_count: int,
) -> bool:
    """Run both phases. revisit_count > 0 is the extension's report that the
    Redis revisit flag was already set when the page passed Phase 1."""
    if not phase1_passes(dwell_seconds):
        logger.debug("Intent gate: phase 1 failed (dwell=%ss < %ss)", dwell_seconds, PHASE1_MIN_SECONDS)
        return False
    passed = phase2_passes(
        dwell_seconds=dwell_seconds,
        scroll_depth=scroll_depth,
        interaction_count=interaction_count,
        is_revisit=revisit_count > 0,
    )
    logger.debug(
        "Intent gate: phase 2 %s (dwell=%ss scroll=%.2f interactions=%d revisit=%s)",
        "passed" if passed else "failed",
        dwell_seconds,
        scroll_depth,
        interaction_count,
        revisit_count > 0,
    )
    return passed
