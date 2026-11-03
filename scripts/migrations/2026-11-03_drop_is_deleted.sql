-- =============================================================================
-- E.C.H.O — remove soft delete (memory_items.is_deleted) from the local database
-- Owner's decision 2026-11-03: user deletes are hard deletes (ste/retention.py
-- delete_items), so the flag and its partial index are no longer needed.
--
-- Run ORDER (read before running):
--   1. pg_dump -Fc echo_db into C:\Users\mdshe\echo_backups\
--   2. python scripts/purge_soft_deleted.py          (prints how many rows are flagged)
--      python scripts/purge_soft_deleted.py --yes    (hard-deletes them everywhere)
--   3. psql -h localhost -U echo -d echo_db -v ON_ERROR_STOP=1 -f scripts/migrations/2026-11-03_drop_is_deleted.sql
--
-- One transaction. It refuses to run while any row is still flagged, so a forgotten
-- step 2 can never turn deleted items back into visible ones. Not idempotent on
-- purpose: a second run fails at DROP COLUMN. Fresh installs get the same schema
-- from scripts/setup_db.py.
-- =============================================================================

BEGIN;

DO $$
DECLARE
    flagged BIGINT;
BEGIN
    SELECT COUNT(*) INTO flagged FROM memory_items WHERE is_deleted = TRUE;
    IF flagged > 0 THEN
        RAISE EXCEPTION '% soft-deleted rows remain: run scripts/purge_soft_deleted.py --yes first', flagged;
    END IF;
END
$$;

DROP INDEX IF EXISTS idx_memory_items_active;
ALTER TABLE memory_items DROP COLUMN is_deleted;

COMMIT;
