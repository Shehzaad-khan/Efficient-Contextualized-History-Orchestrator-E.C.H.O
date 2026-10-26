"""Unit tests for the storage helpers: Redis TLS rule, token files, capture settings, Redis keys, retention."""
import json
from datetime import datetime, timedelta

import pytest

from ste import redis_manager as rm
from ste import retention, settings_store
from ste import security
from ste.security import SecurityConfigError


# ── Redis TLS rule and token files ────────────────────────────────────────────

@pytest.mark.parametrize("url", ["redis://localhost:6379/0", "redis://127.0.0.1:6379", "redis://[::1]:6379", "rediss://remote.example.com:6380"])
def test_allowed_redis_urls(url):
    assert security.validate_redis_tls_url(url) == url


@pytest.mark.parametrize("url", ["redis://remote.example.com:6379", "redis://:secret@10.0.0.5:6379", "http://localhost"])
def test_remote_plain_redis_is_refused_without_leaking_the_password(url):
    """Shows: a remote Redis must use TLS, and the error message never contains the URL's password."""
    with pytest.raises(SecurityConfigError) as error:
        security.validate_redis_tls_url(url)
    assert "secret" not in str(error.value)


def test_token_files_round_trip_atomically(tmp_path):
    path = tmp_path / "token_gmail.json"
    assert security.read_token_text(path) is None
    security.write_token_text(path, '{"token": "abc"}')
    assert security.read_token_text(path) == '{"token": "abc"}'
    assert not (tmp_path / "token_gmail.json.tmp").exists()
    path.write_bytes(b"\xef\xbb\xbf{\"bom\": true}")           # saved by Notepad
    assert json.loads(security.read_token_text(path)) == {"bom": True}


# ── Capture settings ──────────────────────────────────────────────────────────

def test_defaults_when_nothing_is_stored(isolated_settings):
    settings = settings_store.get_settings(use_cache=False)
    assert settings["gmail_enabled"] and settings["chrome_enabled"] and settings["youtube_enabled"]
    assert settings["excluded_domains"] == [] and settings["retention"] == "forever"


def test_save_settings_normalises_and_persists(isolated_settings):
    saved = settings_store.save_settings({
        "chrome_enabled": False, "excluded_domains": [" Bank.COM ", "bank.com", ""],
        "excluded_senders": ["Newsletter@"], "retention": "6m", "unknown_key": 1,
    })
    assert saved["chrome_enabled"] is False
    assert saved["excluded_domains"] == ["bank.com"] and saved["excluded_senders"] == ["newsletter@"]
    assert "unknown_key" not in saved
    assert json.loads(isolated_settings.data[settings_store.SETTINGS_KEY])["retention"] == "6m"
    assert settings_store.get_settings(use_cache=False)["retention"] == "6m"


def test_an_invalid_retention_value_falls_back_to_forever(isolated_settings):
    """Shows: a typo or corrupt value can never start deleting data."""
    assert settings_store.save_settings({"retention": "5 minutes"})["retention"] == "forever"


def test_capture_switches_and_exclusions(isolated_settings):
    settings_store.save_settings({"youtube_enabled": False, "excluded_domains": ["bank.com"], "excluded_senders": ["noreply@"]})
    assert settings_store.is_source_enabled("youtube") is False
    assert settings_store.is_source_enabled("gmail") is True
    assert settings_store.is_domain_excluded("bank.com") and settings_store.is_domain_excluded("login.BANK.com")
    assert not settings_store.is_domain_excluded("notbank.com") and not settings_store.is_domain_excluded("")
    assert settings_store.is_sender_excluded("Shop <NOREPLY@shop.com>")
    assert not settings_store.is_sender_excluded("friend@x.com") and not settings_store.is_sender_excluded("")


def test_get_settings_returns_a_copy(isolated_settings):
    first = settings_store.get_settings()
    first["excluded_domains"].append("hacked.com")
    assert settings_store.get_settings()["excluded_domains"] == []


def test_a_redis_outage_means_capture_stays_allowed(monkeypatch):
    """Shows: if Redis is down, settings read as defaults (everything allowed) instead of blocking capture."""
    def broken():
        raise ConnectionError("redis down")

    monkeypatch.setattr(settings_store, "get_sync_client", broken)
    monkeypatch.setattr(settings_store, "_cache", None)
    assert settings_store.get_settings(use_cache=False)["gmail_enabled"] is True
    assert settings_store.is_source_enabled("chrome") is True


def test_saving_raises_when_redis_is_down(monkeypatch):
    """Shows: a settings change the user asked for never silently disappears."""
    def broken():
        raise ConnectionError("redis down")

    monkeypatch.setattr(settings_store, "get_sync_client", broken)
    with pytest.raises(ConnectionError):
        settings_store.save_settings({"chrome_enabled": False})


def test_a_corrupt_stored_value_is_repaired_by_the_next_save(isolated_settings):
    isolated_settings.data[settings_store.SETTINGS_KEY] = "[1, 2, 3]"
    assert settings_store.save_settings({"gmail_enabled": False})["gmail_enabled"] is False
    isolated_settings.data[settings_store.SETTINGS_KEY] = "not json"
    assert settings_store.get_settings(use_cache=False)["gmail_enabled"] is True


# ── Redis keys and counters ───────────────────────────────────────────────────

@pytest.fixture
def redis_client(monkeypatch, fake_redis):
    monkeypatch.setattr(rm, "get_sync_client", lambda: fake_redis)
    return fake_redis


def test_revisit_flag_is_set_on_first_visit_and_seen_on_the_second(redis_client):
    assert rm.check_and_record_revisit("chrome", "https://example.com/a") is False
    assert rm.check_and_record_revisit("chrome", "https://example.com/a") is True
    key = rm.revisit_key("chrome", "https://example.com/a")
    assert redis_client.ttls[key] == rm.REVISIT_TTL_SECONDS == 86400
    assert key.startswith("echo:revisit:chrome:") and "example.com" not in key


def test_redis_keys_are_namespaced():
    assert rm.attachment_cache_key("m", "a") == "echo:attachment:m:a"
    assert rm.enp_retry_key("m") == "echo:enp:retry:m"


def test_attachment_cache_lasts_one_hour(redis_client):
    assert rm.get_attachment_text("m", "a") is None
    rm.cache_attachment_text("m", "a", "full text")
    assert rm.get_attachment_text("m", "a") == "full text"
    assert redis_client.ttls[rm.attachment_cache_key("m", "a")] == rm.ATTACHMENT_CACHE_TTL_SECONDS == 3600


def test_retry_counter_counts_and_clears(redis_client):
    assert rm.get_enp_retry_count("m") == 0
    assert rm.increment_enp_retry_count("m") == 1
    assert rm.increment_enp_retry_count("m") == 2
    assert rm.get_enp_retry_count("m") == 2
    rm.clear_enp_retry_count("m")
    assert rm.get_enp_retry_count("m") == 0


def test_retry_counter_fails_closed_when_redis_is_down(monkeypatch):
    """Shows: with no counter the cap cannot hold, so the row is treated as used up, not retried forever."""
    def broken():
        raise ConnectionError("down")

    monkeypatch.setattr(rm, "get_sync_client", broken)
    assert rm.get_enp_retry_count("m") == rm.MAX_ENP_RETRIES
    rm.clear_enp_retry_count("m")           # logged, never raises


def test_gmail_history_id_round_trip_and_outage(monkeypatch, fake_redis):
    monkeypatch.setattr(rm, "get_sync_client", lambda: fake_redis)
    assert rm.get_gmail_history_id() is None
    rm.set_gmail_history_id("12345")
    assert rm.get_gmail_history_id() == "12345"

    def broken():
        raise ConnectionError("down")

    monkeypatch.setattr(rm, "get_sync_client", broken)
    assert rm.get_gmail_history_id() is None
    rm.set_gmail_history_id("1")             # never raises


def test_redis_url_comes_from_the_environment_and_is_validated(monkeypatch):
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("UPSTASH_REDIS_URL", raising=False)
    with pytest.raises(ValueError):
        rm.get_redis_url()
    monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    assert rm.get_redis_url() == "redis://127.0.0.1:6379/0"
    assert rm._safe_target("redis://:pw@127.0.0.1:6380/0") == "127.0.0.1:6380"
    monkeypatch.setenv("REDIS_URL", "redis://evil.example.com:6379")
    with pytest.raises(SecurityConfigError):
        rm.get_redis_url()


# ── Retention (A37) ───────────────────────────────────────────────────────────

NOW = datetime(2026, 10, 26, 12, 0)


@pytest.fixture
def retention_env(monkeypatch):
    calls = {"deleted": [], "removed": []}

    class Vectors:
        def remove_many(self, ids):
            calls["removed"].extend(ids)
            return len(ids)

        def save_index(self):
            calls["saved"] = True

    monkeypatch.setattr(retention, "get_manager", lambda: Vectors())
    monkeypatch.setattr(retention, "_delete_redis_keys", lambda ids: 0)
    monkeypatch.setattr(retention.postgresql_manager, "execute",
                        lambda q, p=None: calls["deleted"].append(p["ids"]))
    return calls


def set_retention(monkeypatch, value):
    monkeypatch.setattr(retention.settings_store, "get_settings", lambda use_cache=False: {"retention": value})


def test_forever_deletes_nothing(monkeypatch, retention_env):
    set_retention(monkeypatch, "forever")
    assert retention.purge_expired_items(NOW) == (0, 0, 0)
    assert retention_env["deleted"] == []


def test_expired_items_are_deleted_with_their_vectors(monkeypatch, retention_env):
    set_retention(monkeypatch, "1y")
    monkeypatch.setattr(retention.postgresql_manager, "scalar", lambda q, p=None: NOW)
    monkeypatch.setattr(retention.postgresql_manager, "fetchall", lambda q, p=None: [{"memory_id": "old1"}, {"memory_id": "old2"}])
    result = retention.purge_expired_items(NOW)
    assert result.items_deleted == 2 and result.vectors_removed == 2
    assert retention_env["deleted"] == [["old1", "old2"]] and retention_env["saved"] is True


def test_retention_refuses_to_run_when_everything_looks_expired(monkeypatch, retention_env):
    """Shows: a wrong system clock can never wipe the whole store."""
    set_retention(monkeypatch, "6m")
    monkeypatch.setattr(retention.postgresql_manager, "scalar", lambda q, p=None: NOW - timedelta(days=400))
    assert retention.purge_expired_items(NOW) == (0, 0, 0)
    assert retention_env["deleted"] == []


def test_retention_with_an_empty_store_or_nothing_expired(monkeypatch, retention_env):
    set_retention(monkeypatch, "1y")
    monkeypatch.setattr(retention.postgresql_manager, "scalar", lambda q, p=None: None)
    assert retention.purge_expired_items(NOW) == (0, 0, 0)
    monkeypatch.setattr(retention.postgresql_manager, "scalar", lambda q, p=None: NOW)
    monkeypatch.setattr(retention.postgresql_manager, "fetchall", lambda q, p=None: [])
    assert retention.purge_expired_items(NOW) == (0, 0, 0)


def test_retention_windows(monkeypatch):
    assert retention.RETENTION_DAYS == {"1y": 365, "6m": 183}


def test_redis_cleanup_removes_attachment_and_retry_keys(monkeypatch, fake_redis):
    monkeypatch.setattr(retention, "get_sync_client", lambda: fake_redis)
    fake_redis.set("echo:attachment:m1:a1", "x")
    fake_redis.set("echo:enp:retry:m1", "2")
    assert retention._delete_redis_keys(["m1"]) == 2
    assert fake_redis.data == {}

    def broken():
        raise ConnectionError("down")

    monkeypatch.setattr(retention, "get_sync_client", broken)
    assert retention._delete_redis_keys(["m1"]) == 0     # best effort, never raises
