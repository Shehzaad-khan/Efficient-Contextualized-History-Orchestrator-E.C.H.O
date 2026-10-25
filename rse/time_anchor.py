"""
resolve_time_anchor node — deterministic time-anchor resolution.

Answers queries like "OS material after the interview email":
parse_intent extracts time_anchor_query="interview email" and
time_relation="after"; this node finds the anchor item, reads its created_at,
and exposes it as T_anchor. postgres_keyword_search then injects
created_at > T_anchor (or <) as a hard SQL constraint, and merge_and_rrf
applies the same constraint to semantic-only hits.

Resolution strategy (all local, no LLM):
  0. Clean the phrase — words that name a source ("email", "video", "page"…)
     are removed from the search terms and become a source hint instead.
     Subjects rarely contain the word "email", so leaving it in made the
     keyword lookup fail for the most common anchors.
  1. Keyword lookup — FTS + ILIKE over title, subject, sender, domain and
     channel; best ts_rank first, most recent first on ties (the user usually
     means the latest such item); restricted to the hinted source if any.
  2. Semantic fallback — embed the cleaned phrase and take the closest FAISS
     hit whose cosine is at least MIN_TOP_SIMILARITY (hinted source first).
     Without that floor an unrelated item could become the anchor, and its
     hard time constraint would silently filter out the right results.

Logs carry ids, source types, timestamps and scores only — never the anchor
phrase or item titles (they are personal content).
"""
import logging
import re
from typing import Any, Optional

import psycopg2.extras

from rse.config import ANCHOR_CANDIDATE_LIMIT, MIN_TOP_SIMILARITY
from rse.search_coordinator import escape_like, get_connection
from rse.state import EchoState

logger = logging.getLogger(__name__)

# Words that name a source rather than describe the item. Removed from the
# search terms; they set a source hint instead.
_SOURCE_WORDS: dict[str, str] = {
    "email": "gmail", "emails": "gmail", "e-mail": "gmail", "e-mails": "gmail", "mail": "gmail", "mails": "gmail",
    "gmail": "gmail", "message": "gmail", "messages": "gmail", "inbox": "gmail",
    "page": "chrome", "pages": "chrome", "article": "chrome", "articles": "chrome",
    "site": "chrome", "website": "chrome", "blog": "chrome", "webpage": "chrome",
    "video": "youtube", "videos": "youtube", "youtube": "youtube", "clip": "youtube",
}

_SOURCE_WORD_RE = re.compile(r"\b(" + "|".join(_SOURCE_WORDS) + r")\b", re.IGNORECASE)

# Text the anchor is matched against: the item's name-like fields, never raw_text.
_ANCHOR_DOCUMENT = (
    "to_tsvector('english', "
    "COALESCE(m.title, '') || ' ' || COALESCE(gm.subject, '') || ' ' || "
    "COALESCE(gm.sender, '') || ' ' || COALESCE(cm.domain, '') || ' ' || "
    "COALESCE(ym.channel_name, ''))"
)

_ANCHOR_JOINS = """
FROM memory_items m
LEFT JOIN gmail_metadata   gm ON m.memory_id = gm.memory_id
LEFT JOIN chrome_metadata  cm ON m.memory_id = cm.memory_id
LEFT JOIN youtube_metadata ym ON m.memory_id = ym.memory_id
"""

_ANCHOR_SQL = f"""
SELECT
    m.memory_id,
    m.source_type,
    m.title,
    m.created_at,
    gm.subject,
    gm.thread_id,
    ts_rank({_ANCHOR_DOCUMENT}, websearch_to_tsquery('english', %(terms)s)) AS anchor_rank
{_ANCHOR_JOINS}
WHERE m.is_deleted = FALSE
  AND m.preprocessed = TRUE
  AND (%(source)s::text IS NULL OR m.source_type = %(source)s)
  AND (
        {_ANCHOR_DOCUMENT} @@ websearch_to_tsquery('english', %(terms)s)
        OR m.title ILIKE %(pattern)s
        OR COALESCE(gm.subject, '') ILIKE %(pattern)s
  )
ORDER BY anchor_rank DESC, m.created_at DESC
LIMIT %(limit)s
"""

_ANCHOR_BY_IDS_SQL = """
SELECT m.memory_id::text AS memory_id, m.source_type, m.title, m.created_at, gm.subject, gm.thread_id
FROM memory_items m
LEFT JOIN gmail_metadata gm ON m.memory_id = gm.memory_id
WHERE m.is_deleted = FALSE
  AND m.memory_id::text = ANY(%s)
"""


def _clean_anchor_phrase(anchor_query: str) -> tuple[str, Optional[str]]:
    """
    Split the anchor phrase into search terms and an optional source hint.

    "interview email" → ("interview", "gmail"). When the phrase names two
    different sources the hint is dropped. When only source words remain, the
    original phrase is kept as the terms (better than searching for nothing).
    """
    hints = {_SOURCE_WORDS[word.lower()] for word in _SOURCE_WORD_RE.findall(anchor_query)}
    source_hint = hints.pop() if len(hints) == 1 else None
    # Remove only the source words; everything else stays as typed (punctuation
    # included, so an ILIKE on "50% off" still sees the "%").
    cleaned = " ".join(_SOURCE_WORD_RE.sub(" ", anchor_query).split())
    return (cleaned or anchor_query.strip()), source_hint


def _keyword_anchor_lookup(terms: str, source_hint: Optional[str]) -> Optional[dict[str, Any]]:
    """FTS/ILIKE lookup for the anchor item. Returns the best row or None."""
    params = {
        "terms": terms,
        "pattern": f"%{escape_like(terms)}%",
        "source": source_hint,
        "limit": ANCHOR_CANDIDATE_LIMIT,
    }
    try:
        conn = get_connection()
        try:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(_ANCHOR_SQL, params)
                    rows = cur.fetchall()
        finally:
            conn.close()
    except Exception as exc:
        logger.error("resolve_time_anchor: keyword lookup failed — %s", exc)
        return None

    if not rows:
        return None
    row = dict(rows[0])
    row["memory_id"] = str(row["memory_id"])
    return row


def _semantic_anchor_lookup(terms: str, source_hint: Optional[str]) -> Optional[dict[str, Any]]:
    """
    Embed the phrase and take the nearest live FAISS item above the cosine floor.

    Among qualifying hits, an item from the hinted source wins; otherwise the
    closest one. Returns None when nothing reaches MIN_TOP_SIMILARITY.
    """
    try:
        from enp.embedding_generator import generate_query_embedding
        from ste.faiss_manager import get_manager

        vector = generate_query_embedding(terms)
        hits = get_manager().search_all(vector, ANCHOR_CANDIDATE_LIMIT)
        similarity_by_id = {
            memory_id: similarity for memory_id, similarity in hits if similarity >= MIN_TOP_SIMILARITY
        }
        if not similarity_by_id:
            best = hits[0][1] if hits else None
            logger.info("resolve_time_anchor: semantic fallback below floor (best=%s)", best)
            return None

        conn = get_connection()
        try:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(_ANCHOR_BY_IDS_SQL, (list(similarity_by_id),))
                    rows = [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()
    except Exception as exc:
        logger.error("resolve_time_anchor: semantic fallback failed — %s", exc)
        return None

    if not rows:
        return None  # every close hit was deleted
    # Hinted source first, then highest cosine.
    rows.sort(key=lambda row: (row["source_type"] != source_hint, -similarity_by_id[row["memory_id"]]))
    best_row = rows[0]
    best_row["anchor_similarity"] = similarity_by_id[best_row["memory_id"]]
    return best_row


def resolve_time_anchor(state: EchoState) -> dict[str, Any]:
    """
    Resolve the time-anchor item and expose T_anchor to the search branches.

    No-ops (returns null anchor fields) when the intent has no anchor query,
    so the node is safe to keep unconditionally in the graph's linear path.

    Args:
        state: EchoState carrying parsed_intent.

    Returns:
        Partial state dict with anchor_time (ISO string or None) and
        anchor_item (resolved row or None).
    """
    intent = state.get("parsed_intent", {})
    anchor_query = intent.get("time_anchor_query")
    relation = intent.get("time_relation")

    if not anchor_query or relation not in ("after", "before"):
        return {"anchor_time": None, "anchor_item": None}

    terms, source_hint = _clean_anchor_phrase(anchor_query)

    method = "keyword"
    anchor = _keyword_anchor_lookup(terms, source_hint)
    if anchor is None:
        logger.info("resolve_time_anchor: keyword lookup empty (source_hint=%s), trying semantic fallback",
                    source_hint)
        method = "semantic"
        anchor = _semantic_anchor_lookup(terms, source_hint)

    if anchor is None or anchor.get("created_at") is None:
        logger.warning("resolve_time_anchor: anchor not resolved (source_hint=%s) — search runs unanchored",
                       source_hint)
        return {"anchor_time": None, "anchor_item": None}

    anchor_time = anchor["created_at"].isoformat()
    logger.info(
        "resolve_time_anchor: resolved by %s → %s (%s, created_at=%s, relation=%s, similarity=%s)",
        method,
        anchor["memory_id"],
        anchor.get("source_type"),
        anchor_time,
        relation,
        anchor.get("anchor_similarity"),
    )
    return {"anchor_time": anchor_time, "anchor_item": anchor}
