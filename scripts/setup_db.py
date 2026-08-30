import os
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent / ".env")

def get_connection():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise ValueError("DATABASE_URL not found in .env file.")
    return psycopg2.connect(url)


SCHEMA_SQL = """

-- =============================================================================
-- E.C.H.O — DATABASE SCHEMA v5.0
-- PostgreSQL 16
-- Run once. Idempotent — safe to re-run on existing database.
-- =============================================================================


-- =============================================================================
-- TABLE 1: system_groups
-- Fixed reference table. 5 rows seeded once. Never modified at runtime.
-- =============================================================================
CREATE TABLE IF NOT EXISTS system_groups (
    system_group_id SERIAL PRIMARY KEY,
    group_name      VARCHAR(20) UNIQUE NOT NULL
        CONSTRAINT chk_system_group_name
        CHECK (group_name IN ('work', 'study', 'entertainment', 'personal', 'misc'))
);

-- Seed the 5 fixed rows immediately after table creation
INSERT INTO system_groups (group_name) VALUES
    ('work'),
    ('study'),
    ('entertainment'),
    ('personal'),
    ('misc')
ON CONFLICT DO NOTHING;


-- =============================================================================
-- TABLE 2: memory_items
-- Central abstraction. Every captured email, webpage, and video gets one row.
-- All other tables reference this table.
-- =============================================================================
CREATE TABLE IF NOT EXISTS memory_items (
    memory_id                UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    system_group_id          INTEGER      NOT NULL
                                          REFERENCES system_groups(system_group_id),
    source_type              VARCHAR(20)  NOT NULL
        CONSTRAINT chk_source_type
        CHECK (source_type IN ('gmail', 'chrome', 'youtube')),
    source_id                TEXT         NOT NULL,
    title                    TEXT,
    raw_text                 TEXT,
    auto_keywords            TEXT[]       DEFAULT '{}',
    preprocessed             BOOLEAN      DEFAULT FALSE,
    classified_by            VARCHAR(20)  DEFAULT 'pending',
    classification_confidence FLOAT       DEFAULT NULL,
    created_at               TIMESTAMP    NOT NULL,
    first_ingested_at        TIMESTAMP    DEFAULT NOW(),
    last_updated_at          TIMESTAMP    DEFAULT NOW(),
    is_deleted               BOOLEAN      DEFAULT FALSE,

    -- Restart-safety guarantee: re-ingesting the same item is silently ignored
    CONSTRAINT unique_source_entry UNIQUE (source_type, source_id)
);


-- =============================================================================
-- TABLE 3: gmail_metadata
-- Weak subtype. Shares PK with memory_items. Created in same transaction.
-- =============================================================================
CREATE TABLE IF NOT EXISTS gmail_metadata (
    memory_id       UUID        PRIMARY KEY
                                REFERENCES memory_items(memory_id) ON DELETE CASCADE,
    email_id        TEXT        NOT NULL,
    thread_id       TEXT,
    sender          TEXT,
    recipients      TEXT[],
    subject         TEXT,
    received_at     TIMESTAMP,
    has_attachments BOOLEAN     DEFAULT FALSE,

    -- Stage 1 structural classifier signal. Must be stored at ingestion time.
    -- e.g. '{INBOX,CATEGORY_PERSONAL,UNREAD}'
    gmail_labels    TEXT[]      DEFAULT '{}',

    -- TRUE if this email was sent by the user (from Sent folder).
    -- FALSE if received. Needed for wellbeing analytics and sent-mail search.
    is_sent         BOOLEAN     DEFAULT FALSE
);


-- =============================================================================
-- TABLE 4: gmail_attachments
-- Weak multi-valued. One row per attachment per email. Own UUID PK.
-- Two-tier extraction: lightweight always, full on-demand via Redis (1hr TTL).
-- =============================================================================
CREATE TABLE IF NOT EXISTS gmail_attachments (
    attachment_id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    memory_id                UUID        NOT NULL
                                         REFERENCES gmail_metadata(memory_id) ON DELETE CASCADE,
    filename                 TEXT,
    mime_type                TEXT,
    file_size                INTEGER,
    lightweight_extract      TEXT,
    last_extracted_at        TIMESTAMP,

    -- TRUE when lightweight_extract has been extracted and merged into
    -- the parent email's embeddable text for FAISS indexing.
    is_processed             BOOLEAN     DEFAULT FALSE,

    -- Full text is NOT stored in PostgreSQL (storage cost).
    -- Redis caches it for 1 hour on demand.
    -- These flags provide operational visibility without storage bloat.
    full_extract_cached      BOOLEAN     DEFAULT FALSE,
    full_extract_generated_at TIMESTAMP  DEFAULT NULL
);


-- =============================================================================
-- TABLE 5: chrome_metadata
-- Weak subtype. Chrome-specific fields for pages that passed the intent gate.
-- canonical_url is UTM-stripped — used for revisit detection via Redis.
-- =============================================================================
CREATE TABLE IF NOT EXISTS chrome_metadata (
    memory_id         UUID    PRIMARY KEY
                              REFERENCES memory_items(memory_id) ON DELETE CASCADE,
    url               TEXT    NOT NULL,
    canonical_url     TEXT    NOT NULL,
    domain            TEXT,
    referrer          TEXT,
    scroll_depth      FLOAT   DEFAULT 0.0,
    interaction_count INTEGER DEFAULT 0,
    revisit_count     INTEGER DEFAULT 0,

    -- Word count of extracted page content. Set by enrichment pipeline.
    -- Used to compute estimated reading completion for effort scoring.
    -- e.g. 3 min dwell on 200-word page != 3 min dwell on 3000-word article.
    word_count        INTEGER DEFAULT NULL
);


-- =============================================================================
-- TABLE 6: youtube_metadata
-- Weak subtype. YouTube-specific fields for videos that passed the intent gate.
-- transcript_text is the richest embedding source for YouTube items.
-- =============================================================================
CREATE TABLE IF NOT EXISTS youtube_metadata (
    memory_id           UUID    PRIMARY KEY
                                REFERENCES memory_items(memory_id) ON DELETE CASCADE,
    video_id            TEXT    NOT NULL,
    channel_name        TEXT,

    -- Permanent YouTube channel identifier. Channel names can change.
    -- Group rules should prefer matching on channel_id over channel_name.
    channel_id          TEXT    DEFAULT NULL,

    duration_seconds    INTEGER,
    is_short            BOOLEAN DEFAULT FALSE,
    transcript_text     TEXT,

    -- YouTube Data API categoryId. Most powerful Stage 1 classifier signal.
    -- e.g. 27=Education→study, 10=Music→entertainment, 20=Gaming→entertainment
    -- Fetched in the same API call as other metadata. Zero extra quota cost.
    youtube_category_id INTEGER DEFAULT NULL
);


-- =============================================================================
-- TABLE 7: memory_engagement
-- Weak subtype. Tracks engagement depth per item.
-- Separated from memory_items to prevent write amplification on frequent updates.
-- Every memory_items row must have exactly one corresponding row here.
-- Created in the same ingestion transaction as memory_items.
-- =============================================================================
CREATE TABLE IF NOT EXISTS memory_engagement (
    memory_id           UUID        PRIMARY KEY
                                    REFERENCES memory_items(memory_id) ON DELETE CASCADE,
    dwell_time_seconds  INTEGER     DEFAULT 0,
    watch_time_seconds  INTEGER     DEFAULT 0,
    first_opened_at     TIMESTAMP   DEFAULT NULL,
    last_accessed_at    TIMESTAMP   DEFAULT NULL,
    play_sessions_count INTEGER     DEFAULT 0,

    -- Stored to avoid cross-table JOIN on every retrieval re-ranking query.
    -- = watch_time_seconds / duration_seconds. YouTube items only.
    -- Updated every time watch_time_seconds is updated. Capped at 1.0.
    completion_rate     FLOAT       DEFAULT NULL
);


-- =============================================================================
-- TABLE 8: regret_events
-- Weak multi-valued. Each mark AND unmark creates a new row. Never updated.
-- Current regret status = COUNT(*) WHERE memory_id = X is ODD.
-- Design preserves full toggle history for pattern detection.
-- =============================================================================
CREATE TABLE IF NOT EXISTS regret_events (
    regret_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    memory_id         UUID        NOT NULL
                                  REFERENCES memory_items(memory_id) ON DELETE CASCADE,
    marked_at         TIMESTAMP   DEFAULT NOW(),
    regret_note       TEXT,

    -- Pre-computed from marked_at for fast pattern queries.
    -- Reminder system uses GROUP BY regret_hour to detect habits
    -- without running EXTRACT() on every row.
    regret_hour       SMALLINT    DEFAULT NULL,   -- 0–23
    regret_day_of_week SMALLINT   DEFAULT NULL    -- 0=Monday, 6=Sunday
);


-- =============================================================================
-- TABLE 9: user_groups
-- User-created custom categories. e.g. "Capstone Project", "Job Hunt 2026".
-- is_active=FALSE is a soft delete — preserves historical associations.
-- =============================================================================
CREATE TABLE IF NOT EXISTS user_groups (
    group_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    group_name   TEXT        NOT NULL,
    description  TEXT        DEFAULT NULL,
    is_active    BOOLEAN     DEFAULT TRUE,
    created_at   TIMESTAMP   DEFAULT NOW(),
    updated_at   TIMESTAMP   DEFAULT NOW()
);


-- =============================================================================
-- TABLE 10: memory_user_groups
-- M:N bridge — items to user groups.
-- CRITICAL: No direct INSERTs. Only via group_suggestions decision='accepted'.
-- =============================================================================
CREATE TABLE IF NOT EXISTS memory_user_groups (
    memory_id   UUID        NOT NULL
                            REFERENCES memory_items(memory_id) ON DELETE CASCADE,
    group_id    UUID        NOT NULL
                            REFERENCES user_groups(group_id) ON DELETE CASCADE,
    assigned_at TIMESTAMP   DEFAULT NOW(),
    PRIMARY KEY (memory_id, group_id)
);


-- =============================================================================
-- TABLE 11: embedding_index
-- Tracks FAISS embedding state. Created only after preprocessed=TRUE.
-- is_active=FALSE drives the rebuild queue on model upgrades.
-- embeddable_text stores the exact string fed to the model — audit trail.
-- =============================================================================
CREATE TABLE IF NOT EXISTS embedding_index (
    memory_id         UUID        PRIMARY KEY
                                  REFERENCES memory_items(memory_id) ON DELETE CASCADE,
    embedding_version TEXT        NOT NULL,
    vector_dimension  INTEGER     NOT NULL,
    indexed_at        TIMESTAMP   DEFAULT NOW(),
    is_active         BOOLEAN     DEFAULT TRUE,

    -- Exact concatenated string that produced the vector.
    -- title + headings + domain hints + snippet + category label.
    -- Critical for debugging unexpected retrieval results.
    embeddable_text   TEXT        DEFAULT NULL
);


-- =============================================================================
-- TABLE 12: sessions
-- Contiguous activity blocks. Items within 5 minutes = same session.
-- total_duration_seconds is accepted denormalization for fast SUM queries.
-- =============================================================================
CREATE TABLE IF NOT EXISTS sessions (
    session_id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    session_start          TIMESTAMP   NOT NULL,
    session_end            TIMESTAMP   NOT NULL,
    total_duration_seconds INTEGER     DEFAULT NULL,
    dominant_group_id      INTEGER     DEFAULT NULL
                                       REFERENCES system_groups(system_group_id),

    -- Raw count of source-type switches within this session.
    -- Chrome→YouTube = 1 switch. Purely descriptive, not evaluated.
    source_switch_count    INTEGER     DEFAULT 0
);


-- =============================================================================
-- TABLE 13: session_memory_map
-- M:N bridge — sessions to memory items they contain.
-- =============================================================================
CREATE TABLE IF NOT EXISTS session_memory_map (
    session_id  UUID    NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    memory_id   UUID    NOT NULL REFERENCES memory_items(memory_id) ON DELETE CASCADE,
    PRIMARY KEY (session_id, memory_id)
);


-- =============================================================================
-- TABLE 14: group_rules
-- Persistent classifier rules. Survive restarts.
-- Five rule types: keyword | domain | channel | sender | time_window
-- match_count tracks rule utility — dead rules surface for review.
-- =============================================================================
CREATE TABLE IF NOT EXISTS group_rules (
    rule_id       UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    group_id      UUID        NOT NULL
                              REFERENCES user_groups(group_id) ON DELETE CASCADE,
    rule_type     VARCHAR(30) NOT NULL,
    rule_value    TEXT        NOT NULL,
    is_active     BOOLEAN     DEFAULT TRUE,
    created_at    TIMESTAMP   DEFAULT NOW(),

    -- Incremented every time this rule fires and matches a new item.
    -- Rule with match_count=0 after 30 days should be surfaced for review.
    match_count   INTEGER     DEFAULT 0
);


-- =============================================================================
-- TABLE 15: group_suggestions
-- Human-in-the-loop audit trail. Every classification decision passes here.
-- decision DEFAULT is 'pending' — items must be explicitly accepted.
-- CRITICAL: 'auto_accepted' as default breaks the human-in-the-loop guarantee.
-- =============================================================================
CREATE TABLE IF NOT EXISTS group_suggestions (
    suggestion_id UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    memory_id     UUID        NOT NULL
                              REFERENCES memory_items(memory_id) ON DELETE CASCADE,
    group_id      UUID        NOT NULL
                              REFERENCES user_groups(group_id) ON DELETE CASCADE,
    rule_score    FLOAT       DEFAULT NULL,
    knn_score     FLOAT       DEFAULT NULL,
    suggested_at  TIMESTAMP   DEFAULT NOW(),

    -- MUST default to 'pending'. Never 'auto_accepted'.
    decision      VARCHAR(20) DEFAULT 'pending',

    -- FALSE = must appear in next weekly review screen.
    reviewed      BOOLEAN     DEFAULT FALSE,
    decided_at    TIMESTAMP   DEFAULT NULL
);


-- =============================================================================
-- TABLE 16: message_store
-- LangChain conversation history for LangGraph RSE multi-turn context.
-- Auto-compatible with PostgresChatMessageHistory.
-- Purge sessions older than 30 days periodically.
-- =============================================================================
CREATE TABLE IF NOT EXISTS message_store (
    id          SERIAL      PRIMARY KEY,
    session_id  TEXT        NOT NULL,
    message     JSONB       NOT NULL,
    created_at  TIMESTAMP   DEFAULT NOW()
);


-- =============================================================================
-- INDEXES
-- =============================================================================

-- memory_items
CREATE INDEX IF NOT EXISTS idx_memory_items_group
    ON memory_items (system_group_id);

CREATE INDEX IF NOT EXISTS idx_memory_items_created
    ON memory_items (created_at DESC);

-- Partial: enrichment pipeline pickup queue
CREATE INDEX IF NOT EXISTS idx_memory_items_unprocessed
    ON memory_items (preprocessed) WHERE preprocessed = FALSE;

-- Partial: all UI queries filter soft-deleted items
CREATE INDEX IF NOT EXISTS idx_memory_items_active
    ON memory_items (is_deleted) WHERE is_deleted = FALSE;

-- gmail_metadata
CREATE INDEX IF NOT EXISTS idx_gmail_email_id
    ON gmail_metadata (email_id);

CREATE INDEX IF NOT EXISTS idx_gmail_thread_id
    ON gmail_metadata (thread_id);

CREATE INDEX IF NOT EXISTS idx_gmail_sender
    ON gmail_metadata (sender);

-- chrome_metadata
CREATE INDEX IF NOT EXISTS idx_chrome_canonical_url
    ON chrome_metadata (canonical_url);

CREATE INDEX IF NOT EXISTS idx_chrome_domain
    ON chrome_metadata (domain);

-- youtube_metadata
CREATE INDEX IF NOT EXISTS idx_youtube_video_id
    ON youtube_metadata (video_id);

CREATE INDEX IF NOT EXISTS idx_youtube_channel
    ON youtube_metadata (channel_name);

-- memory_engagement
CREATE INDEX IF NOT EXISTS idx_engagement_last_accessed
    ON memory_engagement (last_accessed_at DESC);

-- embedding_index
-- Partial: FAISS rebuild queue — only stale rows indexed
CREATE INDEX IF NOT EXISTS idx_embedding_stale
    ON embedding_index (is_active) WHERE is_active = FALSE;

-- sessions
CREATE INDEX IF NOT EXISTS idx_sessions_start
    ON sessions (session_start DESC);

-- group_rules
CREATE INDEX IF NOT EXISTS idx_group_rules_active
    ON group_rules (group_id, is_active);

-- group_suggestions
CREATE INDEX IF NOT EXISTS idx_suggestions_item
    ON group_suggestions (memory_id, decision);

CREATE INDEX IF NOT EXISTS idx_suggestions_group
    ON group_suggestions (group_id, decision);

-- message_store
CREATE INDEX IF NOT EXISTS idx_message_store_session
    ON message_store (session_id);

"""


VERIFY_SQL = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
"""

VERIFY_GROUPS_SQL = """
SELECT system_group_id, group_name
FROM system_groups
ORDER BY system_group_id;
"""

VERIFY_COLUMNS_SQL = """
SELECT table_name, column_name, data_type, column_default, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN (
      'memory_items', 'gmail_metadata', 'youtube_metadata',
      'chrome_metadata', 'memory_engagement', 'group_suggestions',
      'regret_events', 'embedding_index', 'sessions',
      'user_groups', 'group_rules', 'gmail_attachments'
  )
ORDER BY table_name, ordinal_position;
"""


def run_setup():
    print("=" * 60)
    print("  E.C.H.O — Database Setup v5.0")
    print("=" * 60)

    conn = None
    try:
        print("\n[1/4] Connecting to database...")
        conn = get_connection()
        cur = conn.cursor()
        print("      Connected successfully.")

        print("\n[2/4] Creating schema (16 tables + indexes)...")
        cur.execute(SCHEMA_SQL)
        conn.commit()
        print("      Schema applied.")

        print("\n[3/4] Verifying tables...")
        cur.execute(VERIFY_SQL)
        tables = [row[0] for row in cur.fetchall()]
        print(f"      Tables found: {len(tables)}")
        for t in sorted(tables):
            print(f"        ✓  {t}")

        expected = {
            "system_groups", "memory_items", "gmail_metadata",
            "gmail_attachments", "chrome_metadata", "youtube_metadata",
            "memory_engagement", "regret_events", "user_groups",
            "memory_user_groups", "embedding_index", "sessions",
            "session_memory_map", "group_rules", "group_suggestions",
            "message_store"
        }
        found = set(tables)
        missing = expected - found
        if missing:
            print(f"\n  WARNING — Missing tables: {missing}")
        else:
            print(f"\n      All 16 expected tables present.")

        print("\n[4/4] Verifying system_groups seed rows...")
        cur.execute(VERIFY_GROUPS_SQL)
        groups = cur.fetchall()
        for gid, gname in groups:
            print(f"        ✓  [{gid}] {gname}")

        if len(groups) != 5:
            print(f"\n  WARNING — Expected 5 system groups, found {len(groups)}")
        else:
            print(f"\n      All 5 system groups seeded correctly.")

        # Critical constraint checks
        print("\n[CONSTRAINT CHECK] Verifying critical columns...")

        # Check system_group_id is NOT NULL
        cur.execute("""
            SELECT is_nullable FROM information_schema.columns
            WHERE table_name='memory_items' AND column_name='system_group_id'
        """)
        result = cur.fetchone()
        if result and result[0] == 'NO':
            print("      ✓  memory_items.system_group_id is NOT NULL")
        else:
            print("      ✗  WARNING: memory_items.system_group_id allows NULL — fix immediately")

        # Check group_suggestions.decision default is 'pending'
        cur.execute("""
            SELECT column_default FROM information_schema.columns
            WHERE table_name='group_suggestions' AND column_name='decision'
        """)
        result = cur.fetchone()
        if result and 'pending' in str(result[0]):
            print("      ✓  group_suggestions.decision defaults to 'pending'")
        else:
            print(f"      ✗  WARNING: group_suggestions.decision default is '{result[0]}' — must be 'pending'")

        # Check UNIQUE constraint exists
        cur.execute("""
            SELECT constraint_name FROM information_schema.table_constraints
            WHERE table_name='memory_items' AND constraint_type='UNIQUE'
        """)
        constraints = [r[0] for r in cur.fetchall()]
        if constraints:
            print(f"      ✓  memory_items UNIQUE constraint present: {constraints[0]}")
        else:
            print("      ✗  WARNING: UNIQUE(source_type, source_id) constraint missing")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"\n  ERROR: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise


if __name__ == "__main__":
    run_setup()
