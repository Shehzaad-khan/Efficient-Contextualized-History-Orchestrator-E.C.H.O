"""Integration tests for ste/storage_engine.py: the real SQL the app runs, against an in-memory database."""
from types import SimpleNamespace

import pytest

from ste import postgresql_manager as db
from ste import storage_engine as storage


def chrome_payload(**overrides):
    base = dict(
        url="https://example.com/a?utm_source=x", canonical_url="https://example.com/a", title="Example page",
        domain="example.com", dwell_seconds=30, scroll_depth=0.4, interaction_count=2, revisit_count=0,
        content_extract="Some page text", word_count=3, referrer=None, is_app_page=False,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def gmail_message(email_id="msg-1", subject="TechCorp interview", attachments=None):
    return {
        "source_item_id": email_id,
        "title": subject,
        "time": {"event_timestamp": "Mon, 05 Oct 2026 09:30:00 +0000"},
        "source_metadata": {"email": {"thread_id": "t1", "from": "Priya <p@techcorp.com>", "to": ["me@x.com"],
                                      "labels": ["INBOX"], "has_attachments": bool(attachments)}},
        "content": {"primary_text": "See you Friday", "attachments": attachments or []},
    }


def rows(sql, **params):
    return db.fetchall(sql, params)


# ── Dedup: UNIQUE(source_type, source_id) is sacred ───────────────────────────

def test_the_same_page_twice_is_one_memory_item_with_two_visits(sqlite_db):
    """Shows: dedup. Re-visiting a page updates the row and adds a visit; it never makes a second item."""
    first = storage.store_chrome_page(chrome_payload())
    second = storage.store_chrome_page(chrome_payload(dwell_seconds=45, scroll_depth=0.9))
    assert first["inserted"] is True and second["inserted"] is False
    assert first["memory_id"] == second["memory_id"]
    assert rows("SELECT COUNT(*) AS n FROM memory_items")[0]["n"] == 1
    assert rows("SELECT COUNT(*) AS n FROM visits")[0]["n"] == 2
    meta = rows("SELECT scroll_depth FROM chrome_metadata")[0]
    assert meta["scroll_depth"] == 0.9  # the larger value wins
    engagement = rows("SELECT dwell_time_seconds FROM memory_engagement")[0]
    assert engagement["dwell_time_seconds"] == 75  # dwell accumulates


def test_the_same_email_twice_is_one_item_and_attachments_are_not_duplicated(sqlite_db):
    """Shows: every Gmail poll re-sees recent mail; storing it again changes nothing."""
    attachment = [{"filename": "offer.pdf", "mime_type": "application/pdf", "size": 2048}]
    _, inserted_first = storage.store_gmail_message(gmail_message(attachments=attachment))
    _, inserted_second = storage.store_gmail_message(gmail_message(attachments=attachment))
    assert inserted_first is True and inserted_second is False
    assert rows("SELECT COUNT(*) AS n FROM memory_items")[0]["n"] == 1
    assert rows("SELECT COUNT(*) AS n FROM gmail_attachments")[0]["n"] == 1
    assert rows("SELECT lightweight_extract FROM gmail_attachments")[0]["lightweight_extract"] == "offer.pdf | application/pdf | 2048"


def test_the_database_itself_refuses_a_duplicate_source_id(sqlite_db):
    """Shows: even a raw INSERT cannot create a second row for the same (source_type, source_id)."""
    insert = ("INSERT INTO memory_items (system_group_id, source_type, source_id, created_at) "
              "VALUES (5, 'chrome', 'same', '2026-10-01')")
    db.execute(insert)
    with pytest.raises(Exception):
        db.execute(insert)


# ── system_group_id is never NULL ─────────────────────────────────────────────

def test_every_source_is_stored_with_a_system_group(sqlite_db):
    """Shows (CLAUDE.md §2.2): system_group_id is never NULL; chrome starts in 'misc', mail in 'personal'."""
    storage.store_chrome_page(chrome_payload())
    storage.store_gmail_message(gmail_message())
    storage.store_youtube_detection("vid1", False, "2026-10-05T10:00:00Z")
    stored = {r["source_type"]: r["system_group_id"] for r in rows("SELECT source_type, system_group_id FROM memory_items")}
    assert stored == {"chrome": storage.SYSTEM_GROUP_IDS["misc"], "gmail": storage.SYSTEM_GROUP_IDS["personal"],
                      "youtube": storage.SYSTEM_GROUP_IDS["entertainment"]}
    assert all(value is not None for value in stored.values())
    assert set(storage.SYSTEM_GROUP_IDS) == {"work", "study", "entertainment", "personal", "misc"}


def test_the_schema_rejects_a_null_system_group(sqlite_db):
    with pytest.raises(Exception):
        db.execute("INSERT INTO memory_items (system_group_id, source_type, source_id, created_at) "
                   "VALUES (NULL, 'chrome', 'x', '2026-10-01')")


# ── Privacy: app pages keep no text ───────────────────────────────────────────

def test_app_page_text_is_never_stored(sqlite_db):
    """Shows (CLAUDE.md §3): Slack/Notion/Jira pages are stored with their title and raw_text = NULL."""
    storage.store_chrome_page(chrome_payload(canonical_url="https://acme.slack.com/x", url="https://acme.slack.com/x",
                                             is_app_page=True, content_extract="secret chat text"))
    stored = rows("SELECT title, raw_text FROM memory_items")[0]
    assert stored["raw_text"] is None and stored["title"] == "Example page"


def test_gmail_open_is_not_stored_at_capture_but_chrome_is(sqlite_db):
    """Shows: receiving mail is not 'opening' it (wellbeing counts only opened mail)."""
    storage.store_gmail_message(gmail_message())
    storage.store_chrome_page(chrome_payload())
    opened = {r["source_type"]: r["first_opened_at"] for r in rows(
        "SELECT m.source_type, e.first_opened_at FROM memory_items m JOIN memory_engagement e USING (memory_id)")}
    assert opened["gmail"] is None and opened["chrome"] is not None


# ── Gmail engagement and YouTube lifecycle ────────────────────────────────────

def test_opening_a_mail_adds_reading_time_and_stamps_first_open_once(sqlite_db):
    storage.store_gmail_message(gmail_message())
    assert storage.update_gmail_engagement("msg-1", 30) is True
    first_open = rows("SELECT first_opened_at FROM memory_engagement")[0]["first_opened_at"]
    assert storage.update_gmail_engagement("msg-1", 15) is True
    after = rows("SELECT dwell_time_seconds, first_opened_at, play_sessions_count FROM memory_engagement")[0]
    assert after["dwell_time_seconds"] == 45 and after["first_opened_at"] == first_open
    assert after["play_sessions_count"] == 2
    assert storage.update_gmail_engagement("never-ingested", 10) is False


def test_youtube_detection_then_metadata_then_watch_time(sqlite_db):
    """Shows: the two writers (detection, Data API) do not wipe each other, and watch time never goes down."""
    memory_id = storage.store_youtube_detection("vid1", True, "2026-10-05T10:00:00Z")
    storage.update_youtube_metadata(memory_id, {
        "video_id": "vid1", "title": "Learn Python", "channel_name": "CodeCh", "duration_seconds": 100,
        "description": "A course", "tags": ["python", "course"], "category_id": "27",
    })
    storage.update_youtube_watch_time("vid1", 50)
    assert rows("SELECT completion_rate FROM memory_engagement")[0]["completion_rate"] == 0.5
    storage.update_youtube_watch_time("vid1", 20)  # lower: the stored watch time stays
    item = rows("SELECT title, raw_text FROM memory_items")[0]
    meta = rows("SELECT channel_name, is_short, youtube_category_id FROM youtube_metadata")[0]
    engagement = rows("SELECT watch_time_seconds FROM memory_engagement")[0]
    assert item["title"] == "Learn Python" and item["raw_text"] == "A course python course"
    assert meta["channel_name"] == "CodeCh" and meta["is_short"] == 1 and meta["youtube_category_id"] == 27
    assert engagement["watch_time_seconds"] == 50
    assert rows("SELECT dwell_seconds FROM visits")[0]["dwell_seconds"] == 50
    storage.update_youtube_watch_time("unknown-video", 5)  # logged and ignored, never raises


def test_update_youtube_metadata_for_unknown_item_raises(sqlite_db):
    with pytest.raises(ValueError):
        storage.update_youtube_metadata("no-such-id", {"title": "T"})


def test_unsupported_source_type_is_refused(sqlite_db):
    with db.transaction() as connection:
        with pytest.raises(ValueError, match="Unsupported"):
            storage._store_source_metadata(connection, "tiktok", "m", "s", "t", None, {})


# ── Small helpers ─────────────────────────────────────────────────────────────

def test_safe_datetime_understands_iso_rfc2822_and_garbage():
    from datetime import datetime, timezone

    assert storage._safe_datetime("2026-10-05T10:00:00Z") == datetime(2026, 10, 5, 10, 0)
    assert storage._safe_datetime("Mon, 05 Oct 2026 09:30:00 +0530") == datetime(2026, 10, 5, 4, 0)
    assert storage._safe_datetime(datetime(2026, 1, 1, tzinfo=timezone.utc)) == datetime(2026, 1, 1)
    assert storage._safe_datetime("not a date").tzinfo is None  # falls back to now
    assert storage._safe_datetime(None).tzinfo is None


def test_attachment_lightweight_extract_is_metadata_not_content():
    assert storage._attachment_lightweight_extract({"filename": "a.txt", "mime_type": "text/plain"}) == "a.txt | text/plain"
    assert storage._attachment_lightweight_extract({}) == ""
