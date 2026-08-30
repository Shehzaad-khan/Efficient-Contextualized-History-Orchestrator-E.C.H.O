"""
group_manager.py — WBA user groups + hybrid KNN/rule engine (architecture
§11.3–11.4, DB §15.3 9-step flow).

Two-layer grouping: system groups are automatic (ENP's 4-stage classifier);
USER groups answer "what personal goal does this serve?" and live here.

4-phase lifecycle:
  Phase 1 — Manual seeding: user tags items by hand. Minimum 6 confirmed
            items before auto-assignment activates for a group.
  Phase 2 — Auto-assignment: hybrid score = 0.5·rule_score + 0.5·knn_score;
            suggestion created when score > 0.70 OR any rule matched.
  Phase 3 — Weekly batch review: user accepts/rejects pending suggestions.
  Phase 4 — KNN improves each cycle as the confirmed set grows and cleans.

HUMAN-IN-THE-LOOP (non-negotiable, DB design principle 3): every row in
memory_user_groups is written through group_suggestions with an explicit
'accepted' decision — including manual seeding, which records an
auto-audit-trail suggestion in the same transaction. No other write path.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import numpy as np
from sqlalchemy import text

from ste import postgresql_manager

logger = logging.getLogger(__name__)

MIN_CONFIRMED_FOR_KNN = 6
AUTO_SUGGEST_THRESHOLD = 0.70
KNN_TOP_K = 3
RULE_WEIGHT = 0.5
KNN_WEIGHT = 0.5
AUTO_ASSIGN_BATCH_LIMIT = 200

VALID_RULE_TYPES = {"keyword", "domain", "channel", "sender", "time_window"}


# ── Groups CRUD ───────────────────────────────────────────────────────────────

def create_group(group_name: str, description: Optional[str] = None) -> dict[str, Any]:
    row = postgresql_manager.fetchone(
        """
        INSERT INTO user_groups (group_name, description)
        VALUES (:group_name, :description)
        RETURNING group_id, group_name, description, is_active, created_at
        """,
        {"group_name": group_name.strip(), "description": description},
    )
    logger.info("create_group: %r → %s", group_name, row["group_id"])
    return {**row, "group_id": str(row["group_id"])}


def list_groups(include_inactive: bool = False) -> list[dict[str, Any]]:
    """Groups with member counts and auto-assignment readiness."""
    rows = postgresql_manager.fetchall(
        """
        SELECT ug.group_id, ug.group_name, ug.description, ug.is_active,
               ug.created_at, ug.updated_at,
               COUNT(mug.memory_id) AS member_count
        FROM user_groups ug
        LEFT JOIN memory_user_groups mug ON mug.group_id = ug.group_id
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
    return result.rowcount > 0


# ── Rules ─────────────────────────────────────────────────────────────────────

def add_rule(group_id: str, rule_type: str, rule_value: str) -> dict[str, Any]:
    if rule_type not in VALID_RULE_TYPES:
        raise ValueError(f"rule_type must be one of {sorted(VALID_RULE_TYPES)}")
    row = postgresql_manager.fetchone(
        """
        INSERT INTO group_rules (group_id, rule_type, rule_value)
        VALUES (:group_id, :rule_type, :rule_value)
        RETURNING rule_id, group_id, rule_type, rule_value, is_active, created_at, match_count
        """,
        # Stored lowercase per DB design Table 14.
        {"group_id": group_id, "rule_type": rule_type, "rule_value": rule_value.strip().lower()},
    )
    return {**row, "rule_id": str(row["rule_id"]), "group_id": str(row["group_id"])}


def list_rules(group_id: str, include_inactive: bool = False) -> list[dict[str, Any]]:
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


def set_rule_active(rule_id: str, active: bool) -> bool:
    """Pause/resume a rule without deleting it (match history preserved)."""
    result = postgresql_manager.execute(
        "UPDATE group_rules SET is_active = :active WHERE rule_id = :rule_id",
        {"rule_id": rule_id, "active": active},
    )
    return result.rowcount > 0


RULE_SUGGESTION_MIN_MATCHES = 5


def suggest_rules_for_group(group_id: str) -> list[dict[str, Any]]:
    """
    Step 9 of the 9-step classification flow (DB design §15.3): when 5+
    accepted members of a group share the same pattern, surface a rule
    suggestion. Nothing is written here — the user approves by calling
    add_rule, keeping the rule engine human-in-the-loop.

    Patterns checked: shared Chrome domain, shared Gmail sender domain,
    shared YouTube channel. Existing active rules are excluded.
    """
    candidates = postgresql_manager.fetchall(
        """
        WITH members AS (
            SELECT m.memory_id, m.source_type,
                   cm.domain,
                   gm.sender,
                   ym.channel_name
            FROM memory_user_groups mug
            JOIN memory_items m       ON m.memory_id = mug.memory_id
            LEFT JOIN chrome_metadata  cm ON cm.memory_id = m.memory_id
            LEFT JOIN gmail_metadata   gm ON gm.memory_id = m.memory_id
            LEFT JOIN youtube_metadata ym ON ym.memory_id = m.memory_id
            WHERE mug.group_id = :group_id AND m.is_deleted = FALSE
        )
        SELECT 'domain' AS rule_type, LOWER(domain) AS rule_value, COUNT(*) AS match_count
        FROM members WHERE domain IS NOT NULL AND domain <> ''
        GROUP BY LOWER(domain) HAVING COUNT(*) >= :min_matches
        UNION ALL
        SELECT 'sender', LOWER(SPLIT_PART(sender, '@', 2)), COUNT(*)
        FROM members WHERE sender LIKE '%@%'
        GROUP BY LOWER(SPLIT_PART(sender, '@', 2)) HAVING COUNT(*) >= :min_matches
        UNION ALL
        SELECT 'channel', LOWER(channel_name), COUNT(*)
        FROM members WHERE channel_name IS NOT NULL AND channel_name <> ''
        GROUP BY LOWER(channel_name) HAVING COUNT(*) >= :min_matches
        ORDER BY match_count DESC
        """,
        {"group_id": group_id, "min_matches": RULE_SUGGESTION_MIN_MATCHES},
    )

    existing = {
        (rule["rule_type"], rule["rule_value"])
        for rule in list_rules(group_id, include_inactive=True)
    }
    return [
        {
            "rule_type": row["rule_type"],
            "rule_value": row["rule_value"],
            "match_count": row["match_count"],
        }
        for row in candidates
        if row["rule_value"] and (row["rule_type"], row["rule_value"]) not in existing
    ]


def _rule_matches(rule: dict[str, Any], item: dict[str, Any]) -> bool:
    """Evaluate one rule against one item (DB design Table 14 semantics)."""
    rule_type = rule["rule_type"]
    value = (rule["rule_value"] or "").lower()

    if rule_type == "keyword":
        return value in (item.get("title") or "").lower()
    if rule_type == "domain":
        return (item.get("domain") or "").lower() == value
    if rule_type == "channel":
        return (item.get("channel_name") or "").lower() == value
    if rule_type == "sender":
        return (item.get("sender") or "").lower().endswith(f"@{value}") or value in (item.get("sender") or "").lower()
    if rule_type == "time_window":
        # rule_value = 'YYYY-MM-DD,YYYY-MM-DD'
        try:
            start_str, end_str = [part.strip() for part in value.split(",", 1)]
            created = item.get("created_at")
            return created is not None and start_str <= created.date().isoformat() <= end_str
        except (ValueError, AttributeError):
            return False
    return False


# ── Hybrid scoring ────────────────────────────────────────────────────────────

def _item_embedding(memory_id: str) -> Optional[np.ndarray]:
    """Item vector from the shared FAISS manager; None when not indexed yet."""
    try:
        from ste.faiss_manager import get_manager
        manager = get_manager()
        offset = manager.memory_id_to_offset.get(str(memory_id))
        if offset is None:
            return None
        return manager.vectors[offset]
    except Exception as exc:
        logger.warning("group_manager: FAISS unavailable for KNN — %s", exc)
        return None


def _knn_score(item_vector: np.ndarray, member_ids: list[str]) -> float:
    """Mean cosine similarity of the top-K most similar confirmed members."""
    try:
        from ste.faiss_manager import get_manager
        manager = get_manager()
    except Exception:
        return 0.0

    similarities: list[float] = []
    item_norm = float(np.linalg.norm(item_vector))
    if item_norm == 0.0:
        return 0.0
    for member_id in member_ids:
        offset = manager.memory_id_to_offset.get(str(member_id))
        if offset is None:
            continue
        member_vector = manager.vectors[offset]
        denom = item_norm * float(np.linalg.norm(member_vector))
        if denom == 0.0:
            continue
        similarities.append(float(np.dot(item_vector, member_vector) / denom))

    if not similarities:
        return 0.0
    top = sorted(similarities, reverse=True)[:KNN_TOP_K]
    return float(sum(top) / len(top))


def score_item_for_group(item: dict[str, Any], group_id: str) -> Optional[dict[str, float]]:
    """
    Hybrid score for one item against one group (9-step flow steps 2–4).
    Returns None when the group hasn't reached the 6-item KNN threshold.
    Increments match_count for every rule that fires (step 2 observability).
    """
    member_ids = [
        str(row["memory_id"])
        for row in postgresql_manager.fetchall(
            "SELECT memory_id FROM memory_user_groups WHERE group_id = :group_id",
            {"group_id": group_id},
        )
    ]
    if len(member_ids) < MIN_CONFIRMED_FOR_KNN:
        return None

    rules = list_rules(group_id)
    matched_rule_ids = [r["rule_id"] for r in rules if _rule_matches(r, item)]
    rule_score = (len(matched_rule_ids) / len(rules)) if rules else 0.0

    for rule_id in matched_rule_ids:
        postgresql_manager.execute(
            "UPDATE group_rules SET match_count = match_count + 1 WHERE rule_id = :rule_id",
            {"rule_id": rule_id},
        )

    item_vector = _item_embedding(item["memory_id"])
    knn = _knn_score(item_vector, member_ids) if item_vector is not None else 0.0

    return {
        "rule_score": round(rule_score, 4),
        "knn_score": round(knn, 4),
        "final_score": round(RULE_WEIGHT * rule_score + KNN_WEIGHT * knn, 4),
    }


# ── Suggestions workflow (the ONLY path into memory_user_groups) ─────────────

def _has_open_suggestion_or_membership(memory_id: str, group_id: str) -> bool:
    exists = postgresql_manager.scalar(
        """
        SELECT 1 WHERE EXISTS (
            SELECT 1 FROM memory_user_groups
            WHERE memory_id = :memory_id AND group_id = :group_id
        ) OR EXISTS (
            SELECT 1 FROM group_suggestions
            WHERE memory_id = :memory_id AND group_id = :group_id
              AND decision IN ('pending', 'accepted')
        )
        """,
        {"memory_id": memory_id, "group_id": group_id},
    )
    return bool(exists)


def run_auto_assignment(limit: int = AUTO_ASSIGN_BATCH_LIMIT) -> dict[str, int]:
    """
    Phase 2 — score recent preprocessed items against every KNN-ready group
    and INSERT pending suggestions where the hybrid threshold fires.
    Suggestions await the weekly review; nothing is auto-accepted.
    """
    groups = [g for g in list_groups() if g["auto_assignment_active"]]
    if not groups:
        return {"scored": 0, "suggested": 0}

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
        item["memory_id"] = str(item["memory_id"])
        for group in groups:
            if _has_open_suggestion_or_membership(item["memory_id"], group["group_id"]):
                continue
            scores = score_item_for_group(item, group["group_id"])
            if scores is None:
                continue
            scored += 1
            if scores["final_score"] > AUTO_SUGGEST_THRESHOLD or scores["rule_score"] > 0:
                postgresql_manager.execute(
                    """
                    INSERT INTO group_suggestions (memory_id, group_id, rule_score, knn_score)
                    VALUES (:memory_id, :group_id, :rule_score, :knn_score)
                    """,
                    {
                        "memory_id": item["memory_id"],
                        "group_id": group["group_id"],
                        "rule_score": scores["rule_score"],
                        "knn_score": scores["knn_score"],
                    },
                )
                suggested += 1

    logger.info("run_auto_assignment: scored=%d suggested=%d", scored, suggested)
    return {"scored": scored, "suggested": suggested}


def review_queue(group_id: Optional[str] = None) -> list[dict[str, Any]]:
    """Phase 3 — unreviewed suggestions for the weekly batch review screen."""
    rows = postgresql_manager.fetchall(
        """
        SELECT gs.suggestion_id, gs.memory_id, gs.group_id,
               gs.rule_score, gs.knn_score, gs.suggested_at, gs.decision,
               ug.group_name,
               m.title, m.source_type, m.created_at
        FROM group_suggestions gs
        JOIN user_groups ug ON ug.group_id = gs.group_id
        JOIN memory_items m ON m.memory_id = gs.memory_id
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
            # Rejecting also removes an earlier accepted membership (review cleanup).
            connection.execute(
                text(
                    """
                    DELETE FROM memory_user_groups
                    WHERE memory_id = :memory_id AND group_id = :group_id
                    """
                ),
                {"memory_id": str(row["memory_id"]), "group_id": str(row["group_id"])},
            )

    return {"suggestion_id": suggestion_id, "decision": decision}


def manual_assign(memory_id: str, group_id: str) -> dict[str, Any]:
    """
    Phase 1 — manual seeding ("Add to Group" in the UI). The user's direct
    action IS the human approval, so the suggestion row is created already
    accepted/reviewed in the same transaction as the membership insert —
    the audit trail stays complete and the only-via-suggestions rule holds.
    """
    with postgresql_manager.transaction() as connection:
        exists = connection.execute(
            text(
                """
                SELECT 1 FROM memory_items WHERE memory_id = :memory_id AND is_deleted = FALSE
                """
            ),
            {"memory_id": memory_id},
        ).first()
        if not exists:
            raise ValueError(f"No memory item found for memory_id={memory_id}")

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
        connection.execute(
            text(
                """
                INSERT INTO memory_user_groups (memory_id, group_id)
                VALUES (:memory_id, :group_id)
                ON CONFLICT DO NOTHING
                """
            ),
            {"memory_id": memory_id, "group_id": group_id},
        )

    member_count = postgresql_manager.scalar(
        "SELECT COUNT(*) FROM memory_user_groups WHERE group_id = :group_id",
        {"group_id": group_id},
    )
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
