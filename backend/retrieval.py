from __future__ import annotations

from collections import Counter
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from backend.storage_engine import append_message_store, fetch_retrieval_candidates

router = APIRouter(prefix="/retrieval", tags=["Unified Retrieval"])


class RetrievalRequest(BaseModel):
    query: str = Field(min_length=1)
    session_id: str = Field(default="default")
    widen_steps: int = Field(default=2, ge=0, le=5)
    limit: int = Field(default=5, ge=1, le=20)


def _parse_query(query: str) -> list[str]:
    return [token.lower() for token in query.split() if token.strip()]


def _score_candidates(query_tokens: list[str], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    query_counts = Counter(query_tokens)
    for row in rows:
        haystack = f"{row.get('title', '')} {row.get('embeddable_text', '')}".lower().split()
        token_counts = Counter(haystack)
        overlap = sum(min(token_counts[token], count) for token, count in query_counts.items())
        row["quality_score"] = overlap / max(1, len(query_tokens))
    return sorted(rows, key=lambda item: (item["quality_score"], item.get("last_accessed_at") or ""), reverse=True)


def _synthesize_answer(query: str, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return f"No matching memories were found for '{query}'."
    snippets = []
    for row in rows[:3]:
        preview = (row.get("embeddable_text") or row.get("raw_text") or "").strip().replace("\n", " ")
        snippets.append(f"[{row['source_type']}] {row.get('title') or row['source_id']}: {preview[:180]}")
    return " | ".join(snippets)


@router.post("/query")
def retrieve_memories(payload: RetrievalRequest):
    query_tokens = _parse_query(payload.query)
    candidate_limit = payload.limit
    scored_rows: list[dict[str, Any]] = []

    for _ in range(payload.widen_steps + 1):
        rows = fetch_retrieval_candidates(payload.query, limit=candidate_limit)
        scored_rows = _score_candidates(query_tokens, rows)
        if scored_rows and scored_rows[0]["quality_score"] >= 0.3:
            break
        candidate_limit += payload.limit

    append_message_store(payload.session_id, "user", payload.query)
    answer = _synthesize_answer(payload.query, scored_rows)
    append_message_store(payload.session_id, "assistant", answer)

    return {
        "query": payload.query,
        "parsed_tokens": query_tokens,
        "candidate_count": len(scored_rows),
        "results": scored_rows[: payload.limit],
        "answer": answer,
    }
