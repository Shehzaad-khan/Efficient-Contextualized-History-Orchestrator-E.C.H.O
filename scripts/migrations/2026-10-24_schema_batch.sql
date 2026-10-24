-- =============================================================================
-- E.C.H.O — schema batch for the restored local database (echo_db)
-- Run once, as the database owner, after a pg_dump -Fc backup:
--   psql -h localhost -U echo -d echo_db -v ON_ERROR_STOP=1 -f scripts/migrations/2026-10-24_schema_batch.sql
-- One transaction: either all four changes apply or none does. Not idempotent on
-- purpose (a second run fails at CREATE TABLE visits instead of doubling the backfill).
-- Fresh installs get the same schema from scripts/setup_db.py.
--
--   A21     drop memory_items.classification_confidence
--   A4      add gmail_attachments.extracted_text
--   A1.1    new Table 17 `visits` (+ backfill from memory_engagement)
--   A24.3   add memory_items.search_vector (stored tsvector) + GIN index (+ backfill)
-- =============================================================================

BEGIN;

-- A21: the value was written by enp/enrichment_pipeline.py but never read.
ALTER TABLE memory_items DROP COLUMN classification_confidence;

-- A4: full extracted attachment text; raw_text stays the real mail body.
ALTER TABLE gmail_attachments ADD COLUMN extracted_text TEXT;

-- A1.1 / A1.1b: one row per gated Chrome/YouTube visit, with that visit's dwell.
CREATE TABLE visits (
    visit_id      UUID      PRIMARY KEY DEFAULT gen_random_uuid(),
    memory_id     UUID      NOT NULL
                            REFERENCES memory_items(memory_id) ON DELETE CASCADE,
    visited_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    dwell_seconds INTEGER   NOT NULL DEFAULT 0
);

-- (memory_id, visited_at DESC) serves "visits of one item" and "latest visit of one item".
CREATE INDEX idx_visits_memory     ON visits (memory_id, visited_at DESC);
CREATE INDEX idx_visits_visited_at ON visits (visited_at);

-- Backfill (A1.1): one visit at first_opened_at (else created_at) and, when it is a
-- different moment, one at last_accessed_at. The item's whole stored total (Chrome
-- dwell_time_seconds, YouTube watch_time_seconds) goes on the LAST visit and the
-- first visit gets 0, so day sums keep today's totals and today's day (A30).
WITH base AS (
    SELECT m.memory_id,
           COALESCE(e.first_opened_at, m.created_at) AS first_visit,
           e.last_accessed_at                        AS last_visit,
           COALESCE(CASE m.source_type
                        WHEN 'youtube' THEN e.watch_time_seconds
                        ELSE e.dwell_time_seconds
                    END, 0)                          AS total_seconds
    FROM memory_items m
    JOIN memory_engagement e ON e.memory_id = m.memory_id
    WHERE m.source_type IN ('chrome', 'youtube')
)
INSERT INTO visits (memory_id, visited_at, dwell_seconds)
SELECT memory_id,
       first_visit,
       CASE WHEN last_visit IS NOT NULL AND last_visit <> first_visit
            THEN 0 ELSE total_seconds END
FROM base
UNION ALL
SELECT memory_id, last_visit, total_seconds
FROM base
WHERE last_visit IS NOT NULL AND last_visit <> first_visit;

-- A24.3: stored full-text vector. Written by ENP from now on (mark_item_processed);
-- this backfill covers the rows that exist today. Same fields as rse/search_coordinator.py
-- _FTS_DOCUMENT, but the FULL raw_text (the 5,000-char cap is dropped) plus attachment text.
-- A generated column cannot do this: it cannot read gmail_metadata, chrome_metadata,
-- youtube_metadata or gmail_attachments.
ALTER TABLE memory_items ADD COLUMN search_vector TSVECTOR;

UPDATE memory_items m
SET search_vector = to_tsvector(
    'english',
    COALESCE(m.title, '') || ' ' ||
    COALESCE(m.raw_text, '') || ' ' ||
    COALESCE(array_to_string(m.auto_keywords, ' '), '') || ' ' ||
    COALESCE((SELECT gm.subject      FROM gmail_metadata   gm WHERE gm.memory_id = m.memory_id), '') || ' ' ||
    COALESCE((SELECT gm.sender       FROM gmail_metadata   gm WHERE gm.memory_id = m.memory_id), '') || ' ' ||
    COALESCE((SELECT cm.domain       FROM chrome_metadata  cm WHERE cm.memory_id = m.memory_id), '') || ' ' ||
    COALESCE((SELECT ym.channel_name FROM youtube_metadata ym WHERE ym.memory_id = m.memory_id), '') || ' ' ||
    COALESCE((SELECT string_agg(ga.extracted_text, ' ')
              FROM gmail_attachments ga WHERE ga.memory_id = m.memory_id), '')
);

CREATE INDEX idx_memory_items_search ON memory_items USING GIN (search_vector);

COMMIT;
