from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from dotenv import load_dotenv
from fastapi import APIRouter
from pydantic import BaseModel, Field

from ste.storage_engine import store_chrome_page
from ingestion.chrome import intent_filter
from ingestion.chrome.revisit_tracker import check_and_record_visit, record_visit

load_dotenv()

router = APIRouter(prefix="/chrome", tags=["Chrome Connector"])

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

APPLICATION_PATH_PREFIXES = (
    "github.com/issues",
    "github.com/pulls",
)

SENSITIVE_PATH_TERMS = (
    "account",
    "auth",
    "billing",
    "callback",
    "checkout",
    "login",
    "logout",
    "oauth",
    "password",
    "payment",
    "profile",
    "settings",
    "signin",
    "signup",
)


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
    parsed = urlparse(raw_url)
    clean_query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in IGNORED_QUERY_PARAMS
    ]
    normalized = parsed._replace(query=urlencode(clean_query, doseq=True), fragment="")
    return urlunparse(normalized)


def extract_domain(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    return (parsed.hostname or "").lower()


def is_skipped_page(url: str) -> bool:
    normalized_url = (url or "").strip().lower()
    return any(prefix in normalized_url for prefix in APPLICATION_PATH_PREFIXES)


def is_sensitive_page(url: str) -> bool:
    parsed = urlparse(url or "")
    target = f"{parsed.path}?{parsed.query}".lower()
    path_parts = {part for part in parsed.path.lower().split("/") if part}
    if any(term in path_parts for term in SENSITIVE_PATH_TERMS):
        return True
    return any(f"/{term}" in target or f"{term}=" in target for term in SENSITIVE_PATH_TERMS)


@router.post("/ingest")
def ingest_chrome_page(payload: ChromeIngestRequest):
    payload.canonical_url = payload.canonical_url or canonicalize_url(payload.url)
    payload.domain = extract_domain(payload.canonical_url) or extract_domain(payload.url) or payload.domain.strip().lower()
    payload.is_app_page = payload.is_app_page or intent_filter.is_application_page(payload.domain)

    if is_skipped_page(payload.url):
        return {"status": "discarded", "reason": "application_path_excluded"}

    if is_sensitive_page(payload.canonical_url):
        return {"status": "discarded", "reason": "sensitive_page_excluded"}

    if intent_filter.phase1_passes(payload.dwell_seconds):
        try:
            record_visit(payload.canonical_url)
        except Exception:
            pass

    if payload.is_app_page:
        payload.content_extract = ""
        payload.word_count = None

    if not payload.is_app_page and not intent_filter.evaluate(
        dwell_seconds=payload.dwell_seconds,
        scroll_depth=payload.scroll_depth,
        interaction_count=payload.interaction_count,
        revisit_count=payload.revisit_count,
    ):
        return {"status": "discarded", "reason": "intent_not_confirmed"}

    saved = store_chrome_page(payload)
    return {
        "status": "saved",
        "memory_id": str(saved["memory_id"]),
        "mode": "engagement_only" if payload.is_app_page else "content_and_engagement",
    }


@router.post("/revisit-check")
def revisit_check(payload: RevisitCheckRequest):
    try:
        return {"is_revisit": check_and_record_visit(payload.canonical_url)}
    except Exception:
        return {"is_revisit": False}
