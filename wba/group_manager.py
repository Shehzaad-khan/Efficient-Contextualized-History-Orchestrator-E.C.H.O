"""
group_manager.py — WBA user groups + hybrid KNN/rule engine (architecture
§11.3–11.4, DB §15.3 9-step flow).

Two-layer grouping: system groups are automatic (ENP's 4-stage classifier);
USER groups answer "what personal goal does this serve?" and live here.

4-phase lifecycle:
  Phase 1 — Manual seeding: user tags items by hand. Minimum 6 confirmed
            items before auto-assignment activates for a group.
  Phase 2 — Auto-assignment: hybrid score = 0.5·rule_score + 0.5·knn_score;
            suggestions are created for any rule match or when KNN alone
            reaches KNN_ONLY_THRESHOLD (currently 0.60).
            knn_score = mean cosine of the 3 most similar confirmed members.
  Phase 3 — Weekly batch review: user accepts/rejects pending suggestions.
  Phase 4 — KNN improves each cycle as the confirmed set grows and cleans.

HUMAN-IN-THE-LOOP (non-negotiable, DB design principle 3): every row in
memory_user_groups is written through group_suggestions with an explicit
'accepted' decision — including manual seeding, which records an
auto-audit-trail suggestion in the same transaction. No other write path.
An item gets at most ONE suggestion per group: once suggested (pending,
accepted or rejected) it is never suggested to that group again, so a
rejection is final until the user assigns it by hand.

Rule matching (DB design Table 14), values stored lowercase:
  keyword     — substring of the title (doc: title ILIKE '%value%').
  domain      — the page's host is the domain or a subdomain of it
                ("github.com" matches www.github.com and gist.github.com).
  channel     — exact YouTube channel name.
  sender      — the address inside the From header ("Priya <hr@acme.com>"):
                "acme.com" matches any @acme.com address; a full address
                matches only itself (doc: sender ILIKE '%@value').
  time_window — "YYYY-MM-DD,YYYY-MM-DD", inclusive, on the item's LOCAL
                created date (created_at is naive UTC).

Threading: all functions are blocking (pooled Postgres) and run in FastAPI's
threadpool. FAISS vectors are read through the locked get_vector API
(tracker C4 step 3), so ENP adding vectors meanwhile is safe. Only one
auto-assignment run at a time per process (_auto_assign_lock).
"""
from __future__ import annotations

import logging
import re
import threading
import time as clock
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Optional

import numpy as np
from sqlalchemy import text

from ste import postgresql_manager
from ste.faiss_manager import cosine, get_manager

logger = logging.getLogger(__name__)

MIN_CONFIRMED_FOR_KNN = 6
AUTO_SUGGEST_THRESHOLD = 0.70
KNN_ONLY_THRESHOLD = 0.60  # Re-tune after the A40 embedding model is active.
KNN_TOP_K = 3
RULE_WEIGHT = 0.5
KNN_WEIGHT = 0.5
AUTO_ASSIGN_BATCH_LIMIT = 200
RULE_SUGGESTION_MIN_MATCHES = 5

VALID_RULE_TYPES = {"keyword", "domain", "channel", "sender", "time_window"}

# The e-mail address inside a From header: "Priya Sharma <Hr@Acme.com>" -> "hr@acme.com".
_ADDRESS_PATTERN = re.compile(r"[\w.+'-]+@[\w-]+(?:\.[\w-]+)+")

_auto_assign_lock = threading.Lock()


# ── Small helpers ─────────────────────────────────────────────────────────────

def _sender_address(sender: Optional[str]) -> str:
    """Lowercase e-mail address from a raw From header; "" when there is none."""
    match = _ADDRESS_PATTERN.search(sender or "")
    return match.group(0).lower() if match else ""


def _host(domain: Optional[str]) -> str:
    """Lowercase host without a leading "www."."""
    host = (domain or "").strip().lower()
    return host[4:] if host.startswith("www.") else host


def _parse_time_window(value: str) -> tuple[date, date]:
    """'YYYY-MM-DD,YYYY-MM-DD' -> (start, end). Raises ValueError when malformed or reversed."""
    start_text, end_text = [part.strip() for part in value.split(",", 1)]
    start, end = date.fromisoformat(start_text), date.fromisoformat(end_text)
    if start > end:
        raise ValueError("time_window start is after its end")
    return start, end


def _local_date(utc_naive: datetime) -> date:
    """Local calendar date of a naive-UTC timestamp."""
    return utc_naive.replace(tzinfo=timezone.utc).astimezone().date()


# ── Groups CRUD ───────────────────────────────────────────────────────────────

def create_group(group_name: str, description: Optional[str] = None) -> dict[str, Any]:
    """Create a user group. Raises ValueError for an empty name."""
    name = (group_name or "").strip()
    if not name:
        raise ValueError("group_name must not be empty")
    row = postgresql_manager.fetchone(
        """
        INSERT INTO user_groups (group_name, description)
        VALUES (:group_name, :description)
        RETURNING group_id, group_name, description, is_active, created_at
        """,
        {"group_name": name, "description": description},
    )
    # Group names are the user's own labels — logged by id only.
    logger.info("User group created: %s", row["group_id"])
    return {**row, "group_id": str(row["group_id"])}


def list_groups(include_inactive: bool = False) -> list[dict[str, Any]]:
    """Groups with member counts (deleted items excluded) and auto-assignment readiness."""
    rows = postgresql_manager.fetchall(
        """
        SELECT ug.group_id, ug.group_name, ug.description, ug.is_active,
               ug.created_at, ug.updated_at,
               COUNT(m.memory_id) AS member_count
        FROM user_groups ug
        LEFT JOIN memory_user_groups mug ON mug.group_id = ug.group_id
        LEFT JOIN memory_items m ON m.memory_id = mug.memory_id AND m.is_deleted = FALSE
        WHERE (:include_inactive OR ug.is_active = TRUE)
        GROUP BY ug.group_id
        ORDER BY ug.created_at DESC
        """,
        {"include_inactive": include_inactive},
    )
    for row in rows:
        row["group_id"] = str(row["group_id"])
        row["auto_assignment_active"] = row["member_count"] >= MIN_CONFIRMED_FOR_KNN
    return rows


def deactivate_group(group_id: str) -> bool:
    """Soft delete — hides the group without destroying associations or rules."""
    result = postgresql_manager.execute(
        """
        UPDATE user_groups SET is_active = FALSE, updated_at = NOW()
        WHERE group_id = :group_id
        """,
        {"group_id": group_id},
    )
    changed = result.rowcount > 0
    logger.info("User group %s deactivated=%s", group_id, changed)
    return changed


# ── Rules ─────────────────────────────────────────────────────────────────────

def add_rule(group_id: str, rule_type: str, rule_value: str) -> dict[str, Any]:
    """Add a rule (the user approved it). Raises ValueError for a bad type or value.

    An empty value is refused: an empty keyword would match every title.
    """
    if rule_type not in VALID_RULE_TYPES:
        raise ValueError(f"rule_type must be one of {sorted(VALID_RULE_TYPES)}")
    # Stored lowercase per DB design Table 14.
    value = (rule_value or "").strip().lower()
    if rule_type == "sender":
        value = value.lstrip("@")
    elif rule_type == "domain":
        value = _host(value)
    if not value:
        raise ValueError("rule_value must not be empty")
    if rule_type == "time_window":
        _parse_time_window(value)  # raises ValueError when malformed

    row = postgresql_manager.fetchone(
        """
        INSERT INTO group_rules (group_id, rule_type, rule_value)
        VALUES (:group_id, :rule_type, :rule_value)
        RETURNING rule_id, group_id, rule_type, rule_value, is_active, created_at, match_count
        """,
        {"group_id": group_id, "rule_type": rule_type, "rule_value": value},
    )
    logger.info("Group rule added: rule_id=%s group_id=%s type=%s", row["rule_id"], group_id, rule_type)
    return {**row, "rule_id": str(row["rule_id"]), "group_id": str(row["group_id"])}


def list_rules(group_id: str, include_inactive: bool = False) -> list[dict[str, Any]]:
    """The group's rules, oldest first."""
    rows = postgresql_manager.fetchall(
        """
        SELECT rule_id, group_id, rule_type, rule_value, is_active, created_at, match_count
        FROM group_rules
        WHERE group_id = :group_id AND (:include_inactive OR is_active = TRUE)
        ORDER BY created_at ASC
        """,
        {"group_id": group_id, "include_inactive": include_inactive},
    )
    for row in rows:
        row["rule_id"] = str(row["rule_id"])
        row["group_id"] = str(row["group_id"])
    return rows


def set_rule_active(group_id: str, rule_id: str, active: bool) -> bool:
    """Pause/resume a rule without deleting it (match history preserved).
    """
    result = postgresql_manager.execute(
        "UPDATE group_rules SET is_active = :active WHERE group_id = :group_id AND rule_id = :rule_id",
        {"group_id": group_id, "rule_id": rule_id, "active": active},
    )
    return result.rowcount > 0


def suggest_rules_for_group(group_id: str) -> list[dict[str, Any]]:
    """
    Step 9 of the 9-step classification flow (DB design §15.3): when 5+
    accepted members of a group share the same pattern, surface a rule
    suggestion. Nothing is written here — the user approves by calling
    add_rule, keeping the rule engine human-in-the-loop.

    Patterns checked: shared Chrome domain (without "www."), shared Gmail
    sender domain (from the address inside the From header), shared YouTube
    channel. Existing rules (active or paused) are excluded.
    """
    candidates = postgresql_manager.fetchall(
        r"""
        WITH members AS (
            SELECT m.memory_id,
                   REGEXP_REPLACE(LOWER(cm.domain), '^www\.', '') AS host,
                   LOWER(SUBSTRING(gm.sender FROM '@([A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+)')) AS sender_domain,
                   LOWER(ym.channel_name) AS channel
            FROM memory_user_groups mug
            JOIN memory_items m       ON m.memory_id = mug.memory_id
            LEFT JOIN chrome_metadata  cm ON cm.memory_id = m.memory_id
            LEFT JOIN gmail_metadata   gm ON gm.memory_id = m.memory_id
            LEFT JOIN youtube_metadata ym ON ym.memory_id = m.memory_id
            WHERE mug.group_id = :group_id AND m.is_deleted = FALSE
        )
        SELECT 'domain' AS rule_type, host AS rule_value, COUNT(*) AS match_count
        FROM members WHERE host IS NOT NULL AND host <> ''
        GROUP BY host HAVING COUNT(*) >= :min_matches
        UNION ALL
        SELECT 'sender', sender_domain, COUNT(*)
        FROM members WHERE sender_domain IS NOT NULL
        GROUP BY sender_domain HAVING COUNT(*) >= :min_matches
        UNION ALL
        SELECT 'channel', channel, COUNT(*)
        FROM members WHERE channel IS NOT NULL AND channel <> ''
        GROUP BY channel HAVING COUNT(*) >= :min_matches
        ORDER BY match_count DESC, rule_value
        """,
        {"group_id": group_id, "min_matches": RULE_SUGGESTION_MIN_MATCHES},
    )

    existing = {
        (rule["rule_type"], rule["rule_value"])
        for rule in list_rules(group_id, include_inactive=True)
    }
    suggestions = [
        {
            "rule_type": row["rule_type"],
            "rule_value": row["rule_value"],
            "match_count": row["match_count"],
        }
        for row in candidates
        if row["rule_value"] and (row["rule_type"], row["rule_value"]) not in existing
    ]
    logger.debug("Rule suggestions for group %s: %d", group_id, len(suggestions))
    return suggestions


def _rule_matches(rule: dict[str, Any], item: dict[str, Any]) -> bool:
    """Evaluate one rule against one item (semantics in the module docstring)."""
    rule_type = rule["rule_type"]
    value = (rule["rule_value"] or "").lower()
    if not value:
        return False

    if rule_type == "keyword":
        return value in (item.get("title") or "").lower()
    if rule_type == "domain":
        host = _host(item.get("domain"))
        return bool(host) and (host == value or host.endswith("." + value))
    if rule_type == "channel":
        return (item.get("channel_name") or "").lower() == value
    if rule_type == "sender":
        address = _sender_address(item.get("sender"))
        if not address:
            return False
        return address == value if "@" in value else address.endswith("@" + value)
    if rule_type == "time_window":
        created = item.get("created_at")
        if created is None:
            return False
        try:
            start, end = _parse_time_window(value)
        except ValueError:
            return False
        return start <= _local_date(created) <= end
    return False


# ── Hybrid scoring ────────────────────────────────────────────────────────────

@dataclass
class _GroupContext:
    """Everything needed to score items against one group, loaded once per run."""
    group_id: str
    member_vectors: list[np.ndarray] = field(default_factory=list)
    rules: list[dict[str, Any]] = field(default_factory=list)


def _load_group_context(group_id: str, manager: Any) -> _GroupContext:
    """Active rules and the vectors of the group's confirmed, non-deleted members."""
    member_ids = [
        str(row["memory_id"])
        for row in postgresql_manager.fetchall(
            """
            SELECT mug.memory_id
            FROM memory_user_groups mug
            JOIN memory_items m ON m.memory_id = mug.memory_id
            WHERE mug.group_id = :group_id AND m.is_deleted = FALSE
            """,
            {"group_id": group_id},
        )
    ]
    vectors: list[np.ndarray] = []
    if manager is not None:
        for member_id in member_ids:
            vector = manager.get_vector(member_id)
            if vector is not None:
                vectors.append(vector)
    return _GroupContext(group_id=group_id, member_vectors=vectors, rules=list_rules(group_id))


def _knn_score(item_vector: Optional[np.ndarray], member_vectors: list[np.ndarray]) -> float:
    """Mean cosine similarity of the top-K most similar confirmed members (0.0 if none)."""
    if item_vector is None or not member_vectors:
        return 0.0
    similarities = sorted((cosine(item_vector, member) for member in member_vectors), reverse=True)
    top = similarities[:KNN_TOP_K]
    return float(sum(top) / len(top))


def score_item_for_group(
    item: dict[str, Any], context: _GroupContext, item_vector: Optional[np.ndarray]
) -> dict[str, Any]:
    """
    Hybrid score for one item against one group (9-step flow steps 2–4).
    Pure: no DB access. Returns the scores plus the ids of the rules that fired.
    """
    matched_rule_ids = [r["rule_id"] for r in context.rules if _rule_matches(r, item)]
    rule_score = (len(matched_rule_ids) / len(context.rules)) if context.rules else 0.0
    knn = _knn_score(item_vector, context.member_vectors)
    return {
        "rule_score": round(rule_score, 4),
        "knn_score": round(knn, 4),
        "final_score": round(RULE_WEIGHT * rule_score + KNN_WEIGHT * knn, 4),
        "matched_rule_ids": matched_rule_ids,
    }


# ── Suggestions workflow (the ONLY path into memory_user_groups) ─────────────

def _already_handled_pairs() -> set[tuple[str, str]]:
    """(memory_id, group_id) pairs that are members or were ever suggested."""
    rows = postgresql_manager.fetchall(
        """
        SELECT memory_id, group_id FROM memory_user_groups
        UNION
        SELECT memory_id, group_id FROM group_suggestions
        """
    )
    return {(str(row["memory_id"]), str(row["group_id"])) for row in rows}


def _insert_suggestion(item_id: str, group_id: str, scores: dict[str, Any]) -> bool:
    """Pending suggestion + match_count bumps, in one transaction.

    The NOT EXISTS guard keeps one suggestion per (item, group) even if the
    pair was suggested after this run loaded its snapshot. Returns True when a
    row was inserted.
    """
    with postgresql_manager.transaction() as connection:
        inserted = connection.execute(
            text(
                """
                INSERT INTO group_suggestions (memory_id, group_id, rule_score, knn_score)
                SELECT :memory_id, :group_id, :rule_score, :knn_score
                WHERE NOT EXISTS (
                    SELECT 1 FROM group_suggestions
                    WHERE memory_id = :memory_id AND group_id = :group_id
                ) AND NOT EXISTS (
                    SELECT 1 FROM memory_user_groups
                    WHERE memory_id = :memory_id AND group_id = :group_id
                )
                """
            ),
            {
                "memory_id": item_id,
                "group_id": group_id,
                "rule_score": scores["rule_score"],
                "knn_score": scores["knn_score"],
            },
        ).rowcount
        if inserted and scores["matched_rule_ids"]:
            # Step 2 observability: each rule that fired on a NEW item.
            connection.execute(
                text("UPDATE group_rules SET match_count = match_count + 1 WHERE rule_id = :rule_id"),
                [{"rule_id": rule_id} for rule_id in scores["matched_rule_ids"]],
            )
    return bool(inserted)


def run_auto_assignment(limit: int = AUTO_ASSIGN_BATCH_LIMIT) -> dict[str, Any]:
    """
    Phase 2 — score recent preprocessed items against every KNN-ready group
    and INSERT pending suggestions when any rule matches or the KNN-only
    threshold fires. Suggestions await the weekly review; nothing is auto-accepted.

    Returns {"scored", "suggested"}; {"scored": 0, "suggested": 0,
    "skipped": "already running"} when another run is in progress.
    """
    if not _auto_assign_lock.acquire(blocking=False):
        logger.info("Auto-assignment already running; this call skipped")
        return {"scored": 0, "suggested": 0, "skipped": "already running"}
    try:
        return _run_auto_assignment(limit)
    finally:
        _auto_assign_lock.release()


def _run_auto_assignment(limit: int) -> dict[str, Any]:
    started = clock.perf_counter()
    groups = [g for g in list_groups() if g["auto_assignment_active"]]
    if not groups:
        return {"scored": 0, "suggested": 0}

    try:
        manager = get_manager()
    except Exception as exc:
        # Rules still work without FAISS; KNN scores are 0 for this run.
        logger.warning("Auto-assignment without KNN: FAISS unavailable (%s)", type(exc).__name__)
        manager = None

    contexts = [_load_group_context(g["group_id"], manager) for g in groups]
    handled = _already_handled_pairs()
    items = postgresql_manager.fetchall(
        """
        SELECT m.memory_id, m.source_type, m.title, m.created_at,
               cm.domain, gm.sender, ym.channel_name
        FROM memory_items m
        LEFT JOIN chrome_metadata  cm ON m.memory_id = cm.memory_id
        LEFT JOIN gmail_metadata   gm ON m.memory_id = gm.memory_id
        LEFT JOIN youtube_metadata ym ON m.memory_id = ym.memory_id
        WHERE m.is_deleted = FALSE AND m.preprocessed = TRUE
        ORDER BY m.first_ingested_at DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )

    scored = 0
    suggested = 0
    for item in items:
        item_id = str(item["memory_id"])
        item_vector = manager.get_vector(item_id) if manager is not None else None
        for context in contexts:
            if (item_id, context.group_id) in handled:
                continue
            scores = score_item_for_group(item, context, item_vector)
            scored += 1
            if (
                scores["final_score"] > AUTO_SUGGEST_THRESHOLD
                or scores["rule_score"] > 0
                or scores["knn_score"] >= KNN_ONLY_THRESHOLD
            ):
                if _insert_suggestion(item_id, context.group_id, scores):
                    suggested += 1

    logger.info("Auto-assignment: %d groups, %d items, scored=%d suggested=%d in %d ms",
                len(contexts), len(items), scored, suggested, (clock.perf_counter() - started) * 1000)
    return {"scored": scored, "suggested": suggested}


def review_queue(group_id: Optional[str] = None) -> list[dict[str, Any]]:
    """Phase 3 — unreviewed suggestions of active groups, for the weekly review screen."""
    rows = postgresql_manager.fetchall(
        """
        SELECT gs.suggestion_id, gs.memory_id, gs.group_id,
               gs.rule_score, gs.knn_score, gs.suggested_at, gs.decision,
               ug.group_name,
               m.title, m.source_type, m.created_at
        FROM group_suggestions gs
        JOIN user_groups ug ON ug.group_id = gs.group_id AND ug.is_active = TRUE
        JOIN memory_items m ON m.memory_id = gs.memory_id AND m.is_deleted = FALSE
        WHERE gs.reviewed = FALSE
          AND (:group_id IS NULL OR gs.group_id = :group_id)
        ORDER BY gs.suggested_at DESC
        """,
        {"group_id": group_id},
    )
    for row in rows:
        for key in ("suggestion_id", "memory_id", "group_id"):
            row[key] = str(row[key])
    return rows


def decide_suggestion(suggestion_id: str, accept: bool) -> dict[str, Any]:
    """
    Record the user's decision — Pattern B transaction (DB design §6):
    UPDATE group_suggestions + (on accept) INSERT INTO memory_user_groups,
    atomically. This is the only write path into memory_user_groups.
    """
    decision = "accepted" if accept else "rejected"
    with postgresql_manager.transaction() as connection:
        row = connection.execute(
            text(
                """
                UPDATE group_suggestions
                SET decision = :decision, reviewed = TRUE, decided_at = NOW()
                WHERE suggestion_id = :suggestion_id
                RETURNING memory_id, group_id
                """
            ),
            {"decision": decision, "suggestion_id": suggestion_id},
        ).mappings().first()
        if row is None:
            raise ValueError(f"No suggestion found for suggestion_id={suggestion_id}")

        if accept:
            connection.execute(
                text(
                    """
                    INSERT INTO memory_user_groups (memory_id, group_id)
                    VALUES (:memory_id, :group_id)
                    ON CONFLICT DO NOTHING
                    """
                ),
                {"memory_id": str(row["memory_id"]), "group_id": str(row["group_id"])},
            )
        else:
            # Rejecting also removes an earlier accepted membership (review cleanup, §11.3 Phase 3).
            connection.execute(
                text(
                    """
                    DELETE FROM memory_user_groups
                    WHERE memory_id = :memory_id AND group_id = :group_id
                    """
                ),
                {"memory_id": str(row["memory_id"]), "group_id": str(row["group_id"])},
            )

    logger.info("Suggestion %s %s", suggestion_id, decision)
    return {"suggestion_id": suggestion_id, "decision": decision}


def manual_assign(memory_id: str, group_id: str) -> dict[str, Any]:
    """
    Phase 1 — manual seeding ("Add to Group" in the UI). The user's direct
    action IS the human approval, so the suggestion row is created already
    accepted/reviewed in the same transaction as the membership insert —
    the audit trail stays complete and the only-via-suggestions rule holds.
    Assigning an item that is already a member changes nothing.

    Raises ValueError when the item or the active group does not exist.
    """
    with postgresql_manager.transaction() as connection:
        item_exists = connection.execute(
            text("SELECT 1 FROM memory_items WHERE memory_id = :memory_id AND is_deleted = FALSE"),
            {"memory_id": memory_id},
        ).first()
        if not item_exists:
            raise ValueError(f"No memory item found for memory_id={memory_id}")
        group_exists = connection.execute(
            text("SELECT 1 FROM user_groups WHERE group_id = :group_id AND is_active = TRUE"),
            {"group_id": group_id},
        ).first()
        if not group_exists:
            raise ValueError(f"No active group found for group_id={group_id}")

        added = connection.execute(
            text(
                """
                INSERT INTO memory_user_groups (memory_id, group_id)
                VALUES (:memory_id, :group_id)
                ON CONFLICT DO NOTHING
                """
            ),
            {"memory_id": memory_id, "group_id": group_id},
        ).rowcount
        if added:
            connection.execute(
                text(
                    """
                    INSERT INTO group_suggestions
                        (memory_id, group_id, rule_score, knn_score, decision, reviewed, decided_at)
                    VALUES (:memory_id, :group_id, NULL, NULL, 'accepted', TRUE, NOW())
                    """
                ),
                {"memory_id": memory_id, "group_id": group_id},
            )

    member_count = postgresql_manager.scalar(
        """
        SELECT COUNT(*) FROM memory_user_groups mug
        JOIN memory_items m ON m.memory_id = mug.memory_id AND m.is_deleted = FALSE
        WHERE mug.group_id = :group_id
        """,
        {"group_id": group_id},
    )
    logger.info("Manual assign memory_id=%s group_id=%s added=%s", memory_id, group_id, bool(added))
    return {
        "memory_id": memory_id,
        "group_id": group_id,
        "member_count": int(member_count or 0),
        "auto_assignment_active": int(member_count or 0) >= MIN_CONFIRMED_FOR_KNN,
    }


def group_items(group_id: str, limit: int = 100) -> list[dict[str, Any]]:
    """Confirmed members of a group, newest assignment first."""
    rows = postgresql_manager.fetchall(
        """
        SELECT m.memory_id, m.source_type, m.title, m.created_at, mug.assigned_at
        FROM memory_user_groups mug
        JOIN memory_items m ON m.memory_id = mug.memory_id
        WHERE mug.group_id = :group_id AND m.is_deleted = FALSE
        ORDER BY mug.assigned_at DESC
        LIMIT :limit
        """,
        {"group_id": group_id, "limit": limit},
    )
    for row in rows:
        row["memory_id"] = str(row["memory_id"])
    return rows
