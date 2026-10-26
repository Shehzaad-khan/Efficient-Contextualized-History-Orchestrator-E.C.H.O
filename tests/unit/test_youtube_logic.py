"""Unit tests for YouTube parsing helpers, transcript formats and the intent gate."""
from datetime import datetime

import pytest

from ingestion.youtube import youtube_api_client as yt
from ingestion.youtube import youtube_connector as yc


@pytest.mark.parametrize("value, expected", [
    ("PT4M13S", 253), ("PT1H2M3S", 3723), ("PT1H", 3600), ("P1DT2H3M", 86400 + 7380), ("P0D", 0), ("", 0), ("garbage", 0), (None, 0),
])
def test_iso_duration(value, expected):
    assert yt.parse_iso8601_duration(value) == expected


def test_video_ids_are_eleven_safe_characters():
    assert yt.is_valid_video_id("dQw4w9WgXcQ") and yt.is_valid_video_id("a-b_c1234Zz")
    assert not yt.is_valid_video_id("short") and not yt.is_valid_video_id("dQw4w9WgXcQ\n") and not yt.is_valid_video_id("../etc/passwd")
    assert not yt.is_valid_video_id(None)


def test_transcript_formats():
    json3 = '{"events": [{"segs": [{"utf8": "Hello"}, {"utf8": " there"}]}, {"segs": [{"utf8": "\\n"}]}, {"segs": null}, {"segs": [{"utf8": "friend"}]}]}'
    assert yt._parse_transcript(json3) == "Hello there friend"
    xml = '<transcript><text start="0">It&amp;#39;s <font color="#fff">great</font></text><text start="1">  </text></transcript>'
    assert yt._parse_transcript(xml) == "It's great"
    srv3 = "<timedtext><body><p><s>one</s> <s>two</s></p></body></timedtext>"
    assert yt._parse_transcript(srv3) == "one two"
    assert "words" in yt._parse_transcript("<broken <text>words</text>")        # unparseable XML: tags are stripped
    assert yt._parse_transcript("plain words") == "" and yt._parse_transcript("") == ""


def event(**overrides):
    base = dict(url="https://www.youtube.com/watch?v=dQw4w9WgXcQ", video_id="dQw4w9WgXcQ", is_short=False,
                watch_time_seconds=0, triggered_by="watch_time", timestamp=datetime(2026, 10, 5, 10, 0))
    base.update(overrides)
    return yc.VideoDetectedEvent(**base)


@pytest.mark.parametrize(
    "overrides, is_short, is_revisit, expected",
    [
        ({"watch_time_seconds": 19}, False, False, False),
        ({"watch_time_seconds": 20}, False, False, True),             # regular video: 20 s
        ({"watch_time_seconds": 10}, True, False, True),              # Short: 10 s
        ({"watch_time_seconds": 9}, True, False, False),
        ({"watch_time_seconds": 6, "duration_seconds": 50}, False, False, True),    # 12 % complete
        ({"watch_time_seconds": 4, "duration_seconds": 10}, False, False, False),   # under the 5 s floor
        ({"watch_time_seconds": 1}, False, True, True),               # confirmed revisit
        ({"watch_time_seconds": 1, "triggered_by": "manual_interaction", "interaction_type": "seek"}, False, False, True),
        ({"watch_time_seconds": 1, "triggered_by": "manual_interaction"}, False, False, False),
    ],
)
def test_youtube_intent_gate(overrides, is_short, is_revisit, expected):
    """Shows (§7.3): any ONE of watch time, completion, revisit or a manual action passes."""
    assert yc.passes_intent_gate(event(**overrides), is_short, is_revisit) is expected


def test_a_revisit_confirmation_works_once_and_expires(monkeypatch):
    monkeypatch.setattr(yc, "_confirmed_revisits", {})
    yc._confirm_revisit("vid")
    assert yc._consume_confirmed_revisit("vid") is True
    assert yc._consume_confirmed_revisit("vid") is False         # a client cannot replay it
    yc._confirmed_revisits["old"] = 0.0
    yc._confirm_revisit("new")
    assert "old" not in yc._confirmed_revisits
    yc._confirmed_revisits["late"] = -1
    assert yc._consume_confirmed_revisit("late") is False
