"""Unit tests for the Chrome intent gate, the privacy filters and the YouTube Shorts classifier."""
import pytest

from ingestion.chrome import chrome_connector as cc
from ingestion.chrome import intent_filter as gate
from ingestion.youtube.video_classifier import classify_video_type


# ── Two-phase intent gate (architecture §6.2) ─────────────────────────────────

def test_phase1_needs_five_seconds():
    """Shows: a page seen for under 5 seconds is thrown away."""
    assert not gate.phase1_passes(4)
    assert gate.phase1_passes(5)


@pytest.mark.parametrize(
    "dwell, scroll, clicks, revisit, expected",
    [
        (3, 0.9, 5, 3, False),    # fails phase 1 whatever else is true
        (6, 0.0, 0, 0, False),    # seen, but no sign of intent
        (10, 0.0, 0, 0, True),    # stayed 10 s
        (6, 0.15, 0, 0, True),    # scrolled 15 %
        (6, 0.0, 1, 0, True),     # one interaction
        (6, 0.0, 0, 1, True),     # came back within 24 h
    ],
)
def test_evaluate_requires_one_intent_signal(dwell, scroll, clicks, revisit, expected):
    """Shows: exposure is not enough; one of the four intent signals must also be present."""
    assert gate.evaluate(dwell, scroll, clicks, revisit) is expected


@pytest.mark.parametrize(
    "domain, path, private",
    [
        ("slack.com", "/client/T1", True),
        ("acme.slack.com", "", True),
        ("help.slack.com", "", False),          # public subdomain
        ("notion.so", "/pricing", False),       # public path
        ("notion.so", "/my-private-page", True),
        ("team.atlassian.net", "/browse/X-1", True),
        ("help.atlassian.net", "/browse/X-1", True),  # atlassian is exempt from the subdomain rule
        ("example.com", "/", False),
        ("", "", False),
    ],
)
def test_is_application_page(domain, path, private):
    """Shows: Slack/Notion/Jira pages are private (title only), their public sections are ordinary pages."""
    assert gate.is_application_page(domain, path) is private


# ── Server-side privacy filters ───────────────────────────────────────────────

def test_canonicalize_url_removes_tracking_parameters_and_fragment():
    """Shows: the same article from a newsletter or an ad becomes one row (dedup)."""
    url = "https://example.com/a?id=7&utm_source=news&fbclid=abc#section"
    assert cc.canonicalize_url(url) == "https://example.com/a?id=7"


def test_strip_secret_query_params_keeps_the_rest():
    """Shows: tokens never end up in a stored URL."""
    url = "https://example.com/p?page=2&token=SECRET&sid=9"
    assert cc.strip_secret_query_params(url) == "https://example.com/p?page=2"
    assert cc.strip_secret_query_params("https://example.com/p?page=2") == "https://example.com/p?page=2"
    assert cc.strip_secret_query_params("") == ""


@pytest.mark.parametrize(
    "url",
    [
        "https://bank.example.com/home",
        "https://shop.example.com/checkout",
        "https://example.com/user/sign-in",
        "https://example.com/check_out/step1",
        "https://example.com/page?password=abc",
        "https://example.com/oauth/callback",
        "https://accounts.google.com/",
    ],
)
def test_sensitive_pages_are_never_captured(url):
    """Shows: login, payment and account pages are recognised from host, path or query keys."""
    assert cc.is_sensitive_page(url)


@pytest.mark.parametrize("url", ["https://example.com/blog/python-tips", "https://news.example.org/world", ""])
def test_ordinary_pages_are_not_sensitive(url):
    """Shows: the filter does not swallow normal pages."""
    assert not cc.is_sensitive_page(url)


def test_skipped_pages_and_domain_extraction():
    """Shows: GitHub issue pages are skipped, domains are lower-cased host names."""
    assert cc.is_skipped_page("https://github.com/issues/12")
    assert not cc.is_skipped_page("https://github.com/org/repo")
    assert cc.extract_domain("https://WWW.Example.com:8080/x") == "www.example.com"


# ── YouTube Shorts ────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "url, duration, expected",
    [
        ("https://www.youtube.com/shorts/abc123", None, "short"),
        ("https://www.youtube.com/watch?v=abc", 45, "short"),
        ("https://www.youtube.com/watch?v=abc", 600, "long"),
        ("https://www.youtube.com/watch?v=abc", 0, "long"),            # 0 means unknown
        ("https://www.youtube.com/watch?v=a&x=/shorts/", None, "long"),  # only the path counts
        ("https://www.youtube.com/@chan/shorts", None, "long"),
        ("", None, "long"),
    ],
)
def test_classify_video_type(url, duration, expected):
    """Shows: Shorts are detected from the URL path first, then from a duration under 60 s."""
    assert classify_video_type(url, duration) == expected
