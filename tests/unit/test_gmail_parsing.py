"""Unit tests for Gmail body parsing, hidden-text removal, attachments and headers (ingestion/gmail/gmail_api.py)."""
import base64

import pytest

from ingestion.gmail import gmail_api as ga


def b64(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def part(mime, text=None, **extra):
    body = {"data": b64(text)} if text is not None else {}
    return {"mimeType": mime, "body": body, **extra}


# ── Bodies ────────────────────────────────────────────────────────────────────

def test_plain_body():
    assert ga.extract_body({"payload": part("text/plain", "Hello\r\n\r\n\r\n\r\nthere  friend")}) == "Hello\n\nthere friend"


def test_alternative_parts_keep_the_plain_version_once():
    """Shows: an email with both a text and an HTML version is stored once, not twice."""
    payload = {"mimeType": "multipart/alternative", "parts": [part("text/plain", "Plain version"), part("text/html", "<p>HTML version</p>")]}
    assert ga.extract_body({"payload": payload}) == "Plain version"


def test_html_only_mail_becomes_text_with_its_line_breaks():
    html = "<html><head><style>p{}</style></head><body><p>Dear Priya</p><p>On Mon, Bob wrote:</p><img src='x.gif'></body></html>"
    assert ga.extract_body({"payload": part("text/html", html)}) == "Dear Priya\nOn Mon, Bob wrote:"


def test_preheader_padding_and_comments_are_removed():
    html = "<!--[if mso]>outlook only<![endif]--><div>Real&nbsp;text​‌‍</div>"
    assert ga._html_to_text(html) == "Real text"
    assert ga._html_to_text("") == ""


def test_attached_messages_are_skipped_unless_they_are_all_there_is():
    bounce = {"mimeType": "multipart/mixed", "parts": [part("text/plain", "Delivery failed"),
                                                        {"mimeType": "message/rfc822", "parts": [part("text/plain", "original text")]}]}
    assert ga.extract_body({"payload": bounce}) == "Delivery failed"
    only_attached = {"mimeType": "multipart/mixed", "parts": [{"mimeType": "message/rfc822", "parts": [part("text/plain", "forwarded")]}]}
    assert ga.extract_body({"payload": only_attached}) == "forwarded"


def test_attachment_parts_are_not_body_text():
    payload = {"mimeType": "multipart/mixed", "parts": [part("text/plain", "Body"), part("text/plain", "attached notes", filename="notes.txt")]}
    assert ga.extract_body({"payload": payload}) == "Body"
    assert ga.extract_body({}) == ""


def test_charset_and_bad_data_are_handled():
    latin = base64.urlsafe_b64encode("café".encode("latin-1")).decode()
    p = {"mimeType": "text/plain", "body": {"data": latin}, "headers": [{"name": "Content-Type", "value": 'text/plain; charset="iso-8859-1"'}]}
    assert ga._decode_part_data(p) == "café"
    unknown = {**p, "headers": [{"name": "Content-Type", "value": "text/plain; charset=made-up-9"}]}
    assert ga._part_charset(unknown) == "utf-8"
    assert ga._decode_part_data({"mimeType": "text/plain", "body": {"data": "!!!"}}) in ("", "\x1e\x1e")
    assert ga._decode_part_data({"body": {}}) == ""


# ── Hidden text (A6.1) ────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "html, hidden_word",
    [
        ('<div style="display:none">SECRET</div><p>shown</p>', "SECRET"),
        ('<span style="opacity: 0">SECRET</span><p>shown</p>', "SECRET"),
        ('<div style="max-height:0; overflow:hidden">SECRET</div><p>shown</p>', "SECRET"),
        ('<span style="font-size:0px !important">SECRET</span><p>shown</p>', "SECRET"),
        ('<span style="visibility:hidden">SECRET</span><p>shown</p>', "SECRET"),
    ],
)
def test_hidden_marketing_text_is_dropped(html, hidden_word):
    """Shows: invisible 'preview text' that mail clients hide is not stored as if the user had read it."""
    text = ga._html_to_text(html)
    assert hidden_word not in text and "shown" in text


def test_hidden_wrappers_with_visible_children_keep_their_text():
    """Shows: an Outlook-style font-size:0 wrapper around real content is not thrown away."""
    html = '<td style="font-size:0"><p>real content</p></td>'
    assert "real content" in ga._html_to_text(html)
    html = '<div style="visibility:hidden"><span style="visibility:visible">child shows</span></div>'
    assert "child shows" in ga._html_to_text(html)


def test_style_helpers():
    assert ga._style_declarations("A: B; c : d !important ;bad") == {"a": "b", "c": "d"}
    assert ga._declaration_hides("display", "none") and not ga._declaration_hides("display", "block")
    assert ga._declaration_hides("font-size", "0em") and not ga._declaration_hides("font-size", "12px")
    assert not ga._may_contain_hidden_element("<p>plain</p>")


# ── Attachments ───────────────────────────────────────────────────────────────

def test_real_attachments_are_listed_and_inline_or_tiny_images_are_not():
    def attachment(name, mime, size, headers=None):
        return {"filename": name, "mimeType": mime, "body": {"size": size, "attachmentId": f"id-{name}"}, "headers": headers or []}

    payload = {"parts": [
        attachment("offer.pdf", "application/pdf", 5000),
        attachment("logo.png", "image/png", 2000),                                                   # tiny image: signature logo
        attachment("photo.jpg", "image/jpeg", 200_000),
        attachment("pic.jpg", "image/jpeg", 50_000, [{"name": "Content-Disposition", "value": "inline; filename=pic.jpg"}]),
        attachment("cid.png", "image/png", 50_000, [{"name": "Content-ID", "value": "<abc>"}]),
        attachment("unknown-size.png", "image/png", 0),
        {"filename": "", "mimeType": "text/plain", "body": {}},
    ]}
    names = [a["filename"] for a in ga.extract_attachments({"payload": payload}, "e1")]
    assert names == ["offer.pdf", "photo.jpg", "unknown-size.png"]


# ── Headers ───────────────────────────────────────────────────────────────────

def test_header_helpers():
    message = {"payload": {"headers": [{"name": "Subject", "value": "Hi"}, {"name": "FROM", "value": "a@x.com"},
                                       {"name": "Subject", "value": "second ignored"}, {"name": "X-Other", "value": "no"}]}}
    assert ga._headers(message) == {"subject": "Hi", "from": "a@x.com"}
    assert ga._split_recipients('Bob <bob@x.com>, carol@y.com, ""') == ["Bob <bob@x.com>", "carol@y.com"]
    assert ga._split_recipients("") == []


def test_received_time_prefers_gmails_clock_over_the_senders_date_header():
    assert ga._received_at({"internalDate": "1760000000000"}, "Mon, 01 Jan 2001 00:00:00 +0000").startswith("2025-10-09")
    assert ga._received_at({}, "Mon, 05 Oct 2026 09:30:00 +0000") == "2026-10-05T09:30:00+00:00"
    assert ga._received_at({}, "garbage") == "" and ga._received_at({}, None) == ""


def test_email_data_matches_what_the_storage_engine_expects():
    msg = {"payload": {"headers": [], "mimeType": "text/plain", "body": {"data": b64("Hello")}}, "labelIds": ["SENT"], "threadId": "t9"}
    data = ga._build_email_data("m9", msg, {"subject": "Subj", "from": "me@x.com", "to": "Bob <b@x.com>"}, "2026-10-05T00:00:00+00:00")
    assert data["source_item_id"] == "m9" and data["title"] == "Subj" and data["content"]["primary_text"] == "Hello"
    assert data["source_metadata"]["email"]["is_sent"] is True and data["source_metadata"]["email"]["thread_id"] == "t9"
    assert data["source_link"].endswith("/m9")


# ── Retry with backoff ────────────────────────────────────────────────────────

def test_backoff_retries_then_gives_up(monkeypatch):
    sleeps = []
    monkeypatch.setattr(ga.time, "sleep", sleeps.append)
    attempts = []

    def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise ConnectionError("blip")
        return "ok"

    assert ga._call_with_backoff(flaky, "test") == "ok" and sleeps == [2, 4]
    sleeps.clear()
    with pytest.raises(ConnectionError):
        ga._call_with_backoff(lambda: (_ for _ in ()).throw(ConnectionError("down")), "test")
    assert sleeps == [2, 4, 8]


def test_http_404_is_not_retried_when_listed(monkeypatch):
    from googleapiclient.errors import HttpError

    class Resp:
        status = 404
        reason = "gone"

    monkeypatch.setattr(ga.time, "sleep", lambda s: pytest.fail("must not sleep"))
    with pytest.raises(HttpError):
        ga._call_with_backoff(lambda: (_ for _ in ()).throw(HttpError(Resp(), b"")), "history", no_retry_statuses=(404,))
