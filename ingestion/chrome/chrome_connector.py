"""
chrome_connector.py — Chrome page capture API (architecture §6, CHC module)
Echo Personal Memory System

    POST /chrome/ingest          the extension sends one page after ITS intent gate passes
    POST /chrome/revisit-check   the extension asks "was this URL seen in the last 24 h?"

The extension (extension/background.js) already canonicalizes the URL, skips
incognito / sensitive / GitHub issue pages and runs the same two-phase intent
gate. This router re-checks everything server-side, so a stale or modified
extension can't store what the privacy rules forbid.

Threads: both routes are plain `def`, so FastAPI runs them in its threadpool.
The blocking calls inside them (settings read from Redis, the revisit flag in
Redis, the PostgreSQL upsert) never run on the event loop.
"""

from __future__ import annotations

import logging
import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from fastapi import APIRouter
from pydantic import BaseModel, Field

from ste import settings_store
from ste.storage_engine import store_chrome_page
from ingestion.chrome import intent_filter
from ingestion.chrome.revisit_tracker import check_and_record_visit, record_visit

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chrome", tags=["Chrome Connector"])

# The four lists below MUST match extension/background.js (and the sensitive
# terms also extension/content/chrome_tracker.js). If they drift, the extension
# and the server disagree about which pages exist.

# Tracking parameters stripped from the URL, so the same article reached from a
# newsletter, Facebook or Google Ads is one row (UNIQUE(source_type, source_id)).
IGNORED_QUERY_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
    "ref",
    "_hsenc",
    "mc_eid",
    "yclid",
}

# Query parameters that can carry authentication or session secrets. Strip them
# before URLs become a memory item's source_id or stored metadata.
SECRET_QUERY_PARAMS = {
    "token",
    "access_token",
    "id_token",
    "code",
    "state",
    "key",
    "sig",
    "signature",
    "session",
    "sid",
    "pwd",
    "otp",
    "rlkey",
}

# Pages that are never captured. Plain substring match on the full URL.
APPLICATION_PATH_PREFIXES = (
    "github.com/issues",
    "github.com/pulls",
)

# Pages that may show personal data (login, payment, account). Never captured.
# 38-word list (A7.1 + A7.2) matched as whole tokens against host + path + query keys.
SENSITIVE_PATH_TERMS = (
    "2fa",
    "account",
    "accounts",
    "admin",
    "auth",
    "bank",
    "banking",
    "billing",
    "callback",
    "cart",
    "checkout",
    "dashboard",
    "inbox",
    "invoice",
    "invoices",
    "login",
    "logout",
    "messages",
    "mfa",
    "netbanking",
    "oauth",
    "orders",
    "otp",
    "password",
    "payment",
    "payments",
    "profile",
    "recover",
    "recovery",
    "reset",
    "settings",
    "signin",
    "signup",
    "sso",
    "upi",
    "verification",
    "verify",
    "wallet",
)
SENSITIVE_PATH_SET = frozenset(SENSITIVE_PATH_TERMS)
TOKEN_SPLIT_RE = re.compile(r"[/_\-.]+")
COLLAPSE_RE = re.compile(r"[-_.]+")


class ChromeIngestRequest(BaseModel):
    url: str
    canonical_url: str | None = None
    title: str
    domain: str
    dwell_seconds: int = Field(ge=0)
    scroll_depth: float = Field(ge=0.0, le=1.0)
    interaction_count: int = Field(ge=0)
    revisit_count: int = Field(default=0, ge=0)
    content_extract: str | None = None
    word_count: int | None = Field(default=None, ge=0)
    referrer: str | None = None
    is_app_page: bool = False


class RevisitCheckRequest(BaseModel):
    canonical_url: str


def canonicalize_url(raw_url: str) -> str:
    """Drop tracking parameters and the #fragment. Used only when the
    extension sent no canonical_url (it normally sends its own)."""
    parsed = urlparse(raw_url)
    clean_query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in IGNORED_QUERY_PARAMS
    ]
    normalized = parsed._replace(query=urlencode(clean_query, doseq=True), fragment="")
    return urlunparse(normalized)


def strip_secret_query_params(raw_url: str) -> str:
    """Remove secret-bearing query keys while preserving other URL parts."""
    parsed = urlparse(raw_url or "")
    query = parse_qsl(parsed.query, keep_blank_values=True)
    clean_query = [(key, value) for key, value in query if key.lower() not in SECRET_QUERY_PARAMS]
    if len(clean_query) == len(query):
        return raw_url
    return urlunparse(parsed._replace(query=urlencode(clean_query, doseq=True)))


def extract_domain(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    return (parsed.hostname or "").lower()


def is_skipped_page(url: str) -> bool:
    normalized_url = (url or "").strip().lower()
    return any(prefix in normalized_url for prefix in APPLICATION_PATH_PREFIXES)


def is_sensitive_page(url: str) -> bool:
    """True when a sensitive term is matched as a whole token in the host or path
    (split on / - _ .), as a path segment with separators removed (e.g. 'sign-in' -> 'signin'),
    or as a query parameter key (e.g. '?password=...')."""
    if not url:
        return False
    try:
        parsed = urlparse(url)
    except Exception:
        return True

    # 1. Whole tokens from host and path
    target = f"{parsed.netloc or ''}{parsed.path or ''}".lower()
    tokens = {t for t in TOKEN_SPLIT_RE.split(target) if t}
    if any(t in SENSITIVE_PATH_SET for t in tokens):
        return True

    # 2. Path segments with separators removed (e.g. 'sign-in' -> 'signin', 'check_out' -> 'checkout')
    path_segments = [p.lower() for p in (parsed.path or "").split("/") if p]
    for seg in path_segments:
        collapsed = COLLAPSE_RE.sub("", seg)
        if collapsed in SENSITIVE_PATH_SET:
            return True

    # 3. Query keys (e.g. ?password=..., ?reset=...)
    if parsed.query:
        query_keys = {k.lower() for k, _ in parse_qsl(parsed.query, keep_blank_values=True)}
        if any(k in SENSITIVE_PATH_SET for k in query_keys):
            return True
        query_tokens = {t for k in query_keys for t in TOKEN_SPLIT_RE.split(k) if t}
        if any(t in SENSITIVE_PATH_SET for t in query_tokens):
            return True

    return False


def _discard(reason: str, domain: str) -> dict:
    # Only the domain is logged: a discarded URL can carry secrets
    # (an OAuth callback's ?code=, a reset link's ?token=).
    logger.info("Chrome page discarded: reason=%s domain=%s", reason, domain)
    return {"status": "discarded", "reason": reason}


@router.post("/ingest")
def ingest_chrome_page(payload: ChromeIngestRequest):
    payload.canonical_url = payload.canonical_url or canonicalize_url(payload.url)
    payload.domain = extract_domain(payload.canonical_url) or extract_domain(payload.url) or payload.domain.strip().lower()
    payload.is_app_page = payload.is_app_page or intent_filter.is_application_page(payload.domain)

    # Cheapest and strictest checks first; the order also decides which
    # reason is reported when several apply.
    if not settings_store.is_source_enabled("chrome"):
        return _discard("chrome_capture_disabled", payload.domain)

    # Gmail activity is captured through its API and its dedicated tracker,
    # never as a Chrome page item.
    if payload.domain == "mail.google.com" or payload.domain.endswith(".mail.google.com"):
        return _discard("gmail_web_excluded", payload.domain)

    if settings_store.is_domain_excluded(payload.domain):
        return _discard("domain_excluded", payload.domain)

    if is_skipped_page(payload.url):
        return _discard("application_path_excluded", payload.domain)

    if is_sensitive_page(payload.url) or is_sensitive_page(payload.canonical_url):
        return _discard("sensitive_page_excluded", payload.domain)

    # Check the original URLs first so sensitive query keys such as password
    # remain visible to is_sensitive_page; then scrub secrets from both stored
    # URL fields (including the source_id derived from canonical_url).
    payload.url = strip_secret_query_params(payload.url)
    payload.canonical_url = strip_secret_query_params(payload.canonical_url)

    # App pages bypass the regular intent gate, so enforce their minimum
    # engagement threshold here as a server-side copy of the extension rule.
    if payload.is_app_page and not intent_filter.phase1_passes(payload.dwell_seconds) and payload.interaction_count == 0:
        return _discard("app_page_not_engaged", payload.domain)

    # §6.2 Phase 1: a page seen for >= 5 s leaves a 24 h revisit flag in Redis,
    # even if Phase 2 then fails — a later return to it counts as a revisit.
    if intent_filter.phase1_passes(payload.dwell_seconds):
        try:
            record_visit(payload.canonical_url)
        except Exception:
            # The page can still be stored; only the revisit signal is lost.
            logger.warning("Chrome revisit flag not recorded: domain=%s", payload.domain, exc_info=True)

    # App pages (Slack, Notion, Jira, ...): engagement only, never their text
    # (§6.7), and they skip the intent gate below.
    if payload.is_app_page:
        payload.content_extract = ""
        payload.word_count = None

    if not payload.is_app_page and not intent_filter.evaluate(
        dwell_seconds=payload.dwell_seconds,
        scroll_depth=payload.scroll_depth,
        interaction_count=payload.interaction_count,
        revisit_count=payload.revisit_count,
    ):
        return _discard("intent_not_confirmed", payload.domain)

    # storage_engine logs the stored row. A DB error is logged here and
    # re-raised, so the extension gets a 500.
    try:
        saved = store_chrome_page(payload)
    except Exception:
        logger.exception("Chrome page storage failed: domain=%s", payload.domain)
        raise
    return {
        "status": "saved",
        "memory_id": str(saved["memory_id"]),
        "mode": "engagement_only" if payload.is_app_page else "content_and_engagement",
    }


@router.post("/revisit-check")
def revisit_check(payload: RevisitCheckRequest):
    """Return whether this URL was seen in the last 24 h, and mark it as seen now.
    If Redis is down, answer False (no revisit signal) instead of failing."""
    try:
        # Same stripped URL that /ingest records, or the two Redis keys differ.
        return {"is_revisit": check_and_record_visit(strip_secret_query_params(payload.canonical_url))}
    except Exception:
        logger.warning("Chrome revisit check failed; answering False", exc_info=True)
        return {"is_revisit": False}
