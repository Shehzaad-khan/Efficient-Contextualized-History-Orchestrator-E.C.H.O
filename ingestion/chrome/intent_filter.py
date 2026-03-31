from __future__ import annotations

PHASE1_MIN_SECONDS = 5
PHASE2_MIN_FOREGROUND = 10
PHASE2_MIN_SCROLL = 0.25
PHASE2_MIN_INTERACTIONS = 1

APPLICATION_DOMAINS = {
    "slack.com",
    "app.slack.com",
    "jira.com",
    "atlassian.net",
    "notion.so",
    "figma.com",
    "linear.app",
    "trello.com",
    "asana.com",
    "confluence.atlassian.net",
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
    if not phase1_passes(dwell_seconds):
        return False
    return phase2_passes(
        dwell_seconds=dwell_seconds,
        scroll_depth=scroll_depth,
        interaction_count=interaction_count,
        is_revisit=revisit_count > 0,
    )
