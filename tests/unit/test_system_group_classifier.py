"""Unit tests for the 4-stage system group classifier (enp/system_group_classifier.py).

Every item ends in exactly one of work | study | entertainment | personal | misc, and never in NULL.
The LLM (stage 4) is a scripted fake: it never makes a network call.
"""
import time
from types import SimpleNamespace

import numpy as np
import pytest

from enp import system_group_classifier as sgc


@pytest.fixture(autouse=True)
def clean_state(monkeypatch, tmp_path):
    monkeypatch.setattr(sgc, "CENTROIDS", {})
    monkeypatch.setattr(sgc, "_persisted_categories", set())
    monkeypatch.setattr(sgc, "_llm_client", None)
    monkeypatch.setattr(sgc, "_llm_unavailable", False)
    monkeypatch.setattr(sgc, "_decisions_setup", None)
    monkeypatch.setattr(sgc, "_decisions_unavailable", True)    # no Decisions API unless a test asks for it
    monkeypatch.setattr(sgc, "_decisions_failures", 0)
    monkeypatch.setattr(sgc, "_decisions_paused_until", 0.0)
    monkeypatch.setattr(sgc, "_centroids_path", lambda: tmp_path / "centroids.npz")


# ── Stage 1: structural ───────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "item, expected",
    [
        ({"source": "gmail", "gmail_labels": ["INBOX", "CATEGORY_PERSONAL"]}, "personal"),
        ({"source": "gmail", "gmail_labels": ["CATEGORY_PROMOTIONS"]}, "misc"),
        ({"source": "gmail", "gmail_labels": [], "sender_domain": "uni.ac.in"}, "study"),
        ({"source": "gmail", "gmail_labels": [], "sender": "prof@uni.ac.in"}, "study"),
        ({"source": "gmail", "sender_domain": "github.com"}, "work"),
        ({"source": "youtube", "is_short": True}, "entertainment"),
        ({"source": "youtube", "youtube_category_id": "27"}, "study"),
        ({"source": "youtube", "youtube_category_id": 10}, "entertainment"),
        ({"source": "chrome", "domain": "cs.stanford.edu"}, "study"),
        ({"source": "chrome", "domain": "github.com"}, "work"),
        ({"source": "chrome", "canonical_url": "https://www.reddit.com/r/python"}, "entertainment"),
    ],
)
def test_stage1_structural(item, expected):
    assert sgc.stage1_structural(item) == (expected, "structural", 1.0)


@pytest.mark.parametrize("item", [
    {"source": "gmail", "sender": "someone@random.org"}, {"source": "youtube", "youtube_category_id": "abc"},
    {"source": "chrome", "domain": "unknown-site.example"}, {"source": "other"}, {},
])
def test_stage1_gives_up_on_unknown_items(item):
    assert sgc.stage1_structural(item) is None


def test_domain_helpers():
    assert sgc._normalize_domain("https://www.Docs.Python.org/3/") == "docs.python.org"
    assert sgc._normalize_domain("Priya <p@Acme.io>".split("<")[1].rstrip(">")) == "acme.io"
    assert sgc._normalize_domain("host.com:8080/x") == "host.com"
    assert sgc._normalize_domain(None) == ""
    assert sgc._domain_matches("gist.github.com", ["github.com"]) and not sgc._domain_matches("notgithub.com", ["github.com"])
    assert sgc._coerce_category_id("27") == 27 and sgc._coerce_category_id("x") is None


# ── Stage 2: domain lookup ────────────────────────────────────────────────────

def test_stage2_matches_the_host_and_its_parents():
    assert sgc.stage2_domain_lookup({"source": "chrome", "domain": "docs.python.org"}) == ("study", "domain", 0.95)
    assert sgc.stage2_domain_lookup({"source": "chrome", "url": "https://leetcode.com/problems/two-sum"})[0] == "study"
    assert sgc.stage2_domain_lookup({"source": "chrome", "domain": "unknown.example"}) is None
    assert sgc.stage2_domain_lookup({"source": "gmail", "domain": "leetcode.com"}) is None
    assert sgc._domain_category("") is None


def test_no_domain_is_listed_in_both_stage_one_and_stage_two():
    """Shows: stage 2's map holds only domains that stage 1 does not list (a duplicate would never be reached)."""
    stage1 = set(sgc.WORK_DOMAINS) | set(sgc.ENTERTAINMENT_DOMAINS)
    assert not stage1 & set(sgc.DOMAIN_CATEGORY_MAP)


# ── Stage 3: centroids ────────────────────────────────────────────────────────

def vec(*values):
    arr = np.zeros(384, dtype=np.float32)
    arr[: len(values)] = values
    return arr / np.linalg.norm(arr)


def test_stage3_needs_a_high_score_and_a_clear_lead():
    sgc.CENTROIDS.update({"work": vec(1, 0), "study": vec(0, 1), "misc": vec(0, 0, 1)})
    category, via, score = sgc.stage3_centroid(vec(1, 0.05))
    assert (category, via) == ("work", "centroid") and score > 0.55
    assert sgc.stage3_centroid(vec(1, 1)) is None            # tie between work and study: no lead
    assert sgc.stage3_centroid(vec(0, 0, 0, 1)) is None       # best score below the bar
    assert sgc.stage3_centroid(None) is None


def test_stage3_is_skipped_with_fewer_than_two_centroids():
    sgc.CENTROIDS["work"] = vec(1)
    assert sgc.stage3_centroid(vec(1)) is None


def test_initialize_centroids_averages_the_seed_sentences():
    calls = []

    def fake_embed(sentence):
        calls.append(sentence)
        return vec(1.0 + len(calls) % 3, 1.0)

    sgc.initialize_centroids(fake_embed)
    assert set(sgc.CENTROIDS) == {"work", "study", "entertainment", "personal", "misc"}
    assert len(calls) == sum(len(s) for s in sgc.SEED_TEXTS.values()) and all(len(s) >= 5 for s in sgc.SEED_TEXTS.values())
    sgc.initialize_centroids(fake_embed)                      # a second call does nothing
    assert len(calls) == sum(len(s) for s in sgc.SEED_TEXTS.values())


def test_persisted_centroids_override_seeds_only_for_the_right_model(tmp_path):
    path = tmp_path / "centroids.npz"
    stamp = {sgc._VERSION_KEY: np.array(sgc.EMBEDDING_VERSION), sgc._DIMENSION_KEY: np.array(sgc.VECTOR_DIMENSION)}
    np.savez(path, work=vec(0, 0, 5), **stamp)
    assert sgc._centroids_match_model(path) is True
    assert sgc._load_persisted_centroids() is True
    assert np.allclose(sgc.CENTROIDS["work"], vec(0, 0, 5)) and "work" in sgc._persisted_categories


def test_centroids_from_another_model_or_without_a_stamp_are_ignored(tmp_path):
    """Shows (CLAUDE.md §5): when the embedding model changes, old centroids must be rebuilt, never reused."""
    path = tmp_path / "centroids.npz"
    np.savez(path, work=vec(1))
    assert sgc._centroids_match_model(path) is False
    np.savez(path, work=vec(1), **{sgc._VERSION_KEY: np.array("old-model"), sgc._DIMENSION_KEY: np.array(384)})
    assert sgc._centroids_match_model(path) is False
    assert sgc._load_persisted_centroids() is False
    assert sgc.CENTROIDS == {}
    (tmp_path / "centroids.npz").write_bytes(b"garbage")
    assert sgc._centroids_match_model(path) is False


def test_centroids_need_recompute_when_missing_old_or_foreign(tmp_path):
    import os

    path = tmp_path / "centroids.npz"
    assert sgc.centroids_need_recompute() is True                     # missing
    stamp = {sgc._VERSION_KEY: np.array(sgc.EMBEDDING_VERSION), sgc._DIMENSION_KEY: np.array(sgc.VECTOR_DIMENSION)}
    np.savez(path, work=vec(1), **stamp)
    assert sgc.centroids_need_recompute() is False                    # fresh and matching
    old = time.time() - 31 * 24 * 3600
    os.utime(path, (old, old))
    assert sgc.centroids_need_recompute() is True                     # older than 30 days
    assert sgc._load_persisted_centroids() is False


def test_recompute_centroids_needs_ten_confirmed_items_per_category(monkeypatch, tmp_path):
    rows = [{"group_name": "study", "source_type": "chrome", "memory_id": f"s{i}"} for i in range(10)]
    rows += [{"group_name": "work", "source_type": "gmail", "memory_id": f"w{i}"} for i in range(3)]   # too few
    rows += [{"group_name": "study", "source_type": "youtube", "memory_id": "missing-vector"}]
    vectors = {r["memory_id"]: vec(0, 1) for r in rows if r["memory_id"] != "missing-vector"}
    monkeypatch.setattr("ste.postgresql_manager.fetchall", lambda q, p=None: rows)
    monkeypatch.setattr(sgc, "get_manager", lambda: SimpleNamespace(get_vector=vectors.get))
    sgc.CENTROIDS["work"] = vec(1)
    updated = sgc.recompute_centroids()
    assert updated == {"study": 10}
    assert np.allclose(sgc.CENTROIDS["study"], vec(0, 1)) and np.allclose(sgc.CENTROIDS["work"], vec(1))
    assert (tmp_path / "centroids.npz").exists()
    assert sgc._centroids_match_model(tmp_path / "centroids.npz")


def test_recompute_with_nothing_to_update_writes_nothing(monkeypatch, tmp_path):
    monkeypatch.setattr("ste.postgresql_manager.fetchall", lambda q, p=None: [])
    monkeypatch.setattr(sgc, "get_manager", lambda: SimpleNamespace(get_vector=lambda i: None))
    assert sgc.recompute_centroids() == {}
    assert not (tmp_path / "centroids.npz").exists()


# ── Stage 4: LLM fallback ─────────────────────────────────────────────────────

class ScriptedLLM:
    def __init__(self, reply=None, error=None):
        self.reply, self.error, self.prompts = reply, error, []

    def invoke(self, prompt):
        self.prompts.append(prompt)
        if self.error:
            raise self.error
        return SimpleNamespace(text=self.reply)


def test_stage4_reads_the_first_word_of_the_reply(monkeypatch):
    monkeypatch.setattr(sgc, "_get_stage4_llm", lambda: ScriptedLLM("Study."))
    assert sgc.stage4_llm_fallback({"source": "chrome", "title": "T"}) == ("study", "llm", 0.90)


def test_stage4_falls_back_to_misc_on_every_kind_of_failure(monkeypatch):
    """Shows (CLAUDE.md §3): a missing LLM, a failed call or an invalid category all end as misc/'fallback'."""
    monkeypatch.setattr(sgc, "_get_stage4_llm", lambda: None)
    assert sgc.stage4_llm_fallback({}) == ("misc", "fallback", 0.40)
    monkeypatch.setattr(sgc, "_get_stage4_llm", lambda: ScriptedLLM(error=TimeoutError("slow")))
    assert sgc.stage4_llm_fallback({}) == ("misc", "fallback", 0.40)
    monkeypatch.setattr(sgc, "_get_stage4_llm", lambda: ScriptedLLM("shopping"))
    assert sgc.stage4_llm_fallback({}) == ("misc", "fallback", 0.40)


def test_stage4_sends_only_source_title_and_a_short_snippet(monkeypatch):
    """Shows (CLAUDE.md §3 + §2.6): title capped at 200 chars, snippet at 300; nothing else leaves the machine."""
    llm = ScriptedLLM("work")
    monkeypatch.setattr(sgc, "_get_stage4_llm", lambda: llm)
    sgc.stage4_llm_fallback({"source": "gmail", "title": "T" * 500, "clean_text": "S" * 5000,
                             "sender": "secret@person.com", "raw_text": "R" * 9000, "memory_id": "m1"})
    prompt = llm.prompts[0]
    assert "T" * 200 in prompt and "T" * 201 not in prompt
    assert "S" * 300 in prompt and "S" * 301 not in prompt
    assert "secret@person.com" not in prompt and "R" * 10 not in prompt and "m1" not in prompt
    assert sgc._stage4_fields({"source_type": "chrome", "raw_text": "abc"}) == ("chrome", "", "abc")


def test_parse_category():
    assert sgc._parse_category("Entertainment!") == "entertainment"
    assert sgc._parse_category("I think work") is None
    assert sgc._parse_category(None) is None


def test_stage4_llm_is_built_once_and_failure_is_remembered(monkeypatch):
    built = []
    monkeypatch.setattr("rse.llm_factory.build_chat_model", lambda a, b: built.append(1) or ScriptedLLM("misc"))
    first = sgc._get_stage4_llm()
    assert sgc._get_stage4_llm() is first and len(built) == 1
    monkeypatch.setattr(sgc, "_llm_client", None)

    def fail(a, b):
        raise RuntimeError("no api key")

    monkeypatch.setattr("rse.llm_factory.build_chat_model", fail)
    assert sgc._get_stage4_llm() is None and sgc._llm_unavailable is True
    assert sgc._get_stage4_llm() is None


class FakeDecisionsClient:
    def __init__(self, choice="study", confidence=0.8, error=None, refuse=False):
        self.choice, self.confidence, self.error, self.refuse, self.requests = choice, confidence, error, refuse, []

    def invoke(self, request):
        self.requests.append(request)
        if self.error:
            raise self.error
        if self.refuse:
            return SimpleNamespace(choices={}, refusals={"category": "no"})
        return SimpleNamespace(choices={"category": SimpleNamespace(choice=self.choice, confidence=self.confidence)}, refusals={})


def test_decisions_api_answer_is_used_when_available(monkeypatch):
    client = FakeDecisionsClient("study", 0.77)
    monkeypatch.setattr(sgc, "_get_stage4_decisions", lambda: (client, {"category": object()}))
    assert sgc.stage4_llm_fallback({"source": "chrome", "title": "Lecture"}) == ("study", "llm", 0.77)
    assert 'Title: "Lecture"' in client.requests[0]["input"]


def test_decisions_refusals_and_bad_values_use_the_one_word_call(monkeypatch):
    monkeypatch.setattr(sgc, "_get_stage4_llm", lambda: ScriptedLLM("work"))
    for client in (FakeDecisionsClient(refuse=True), FakeDecisionsClient(choice="nonsense")):
        monkeypatch.setattr(sgc, "_get_stage4_decisions", lambda c=client: (c, {}))
        assert sgc.stage4_llm_fallback({}) == ("work", "llm", 0.90)


def test_decisions_circuit_breaker_pauses_after_three_failures(monkeypatch):
    client = FakeDecisionsClient(error=RuntimeError("beta not enabled"))
    monkeypatch.setattr(sgc, "_get_stage4_decisions", lambda: (client, {}))
    monkeypatch.setattr(sgc, "_get_stage4_llm", lambda: ScriptedLLM("misc"))
    for _ in range(3):
        sgc.stage4_llm_fallback({})
    assert len(client.requests) == 3 and sgc._decisions_paused_until > time.monotonic()
    sgc.stage4_llm_fallback({})
    assert len(client.requests) == 3                 # paused: the Decisions API is not called again


def test_decisions_are_off_for_other_providers(monkeypatch):
    monkeypatch.setattr(sgc, "_decisions_unavailable", False)
    monkeypatch.setattr("rse.config.LLM_CONFIG", {**__import__("rse.config", fromlist=["LLM_CONFIG"]).LLM_CONFIG, "provider": "anthropic"})
    assert sgc._get_stage4_decisions() is None and sgc._decisions_unavailable is True


# ── The whole classifier ──────────────────────────────────────────────────────

def test_the_first_confident_stage_wins_and_the_result_is_never_empty(monkeypatch):
    """Shows (CLAUDE.md §2.2): classify_system_group always returns one of the five names."""
    monkeypatch.setattr(sgc, "_get_stage4_llm", lambda: None)
    assert sgc.classify_system_group({"source": "chrome", "domain": "github.com"})[1] == "structural"
    assert sgc.classify_system_group({"source": "chrome", "domain": "arxiv.org"})[1] == "domain"
    sgc.CENTROIDS.update({"work": vec(1, 0), "study": vec(0, 1)})
    assert sgc.classify_system_group({"source": "chrome", "domain": "blog.example"}, embedding=vec(1, 0.02))[1] == "centroid"
    for item in ({}, {"source": "gmail"}, {"source": "chrome", "domain": "zzz.example"}):
        category, how, confidence = sgc.classify_system_group(item)
        assert category in sgc._VALID_CATEGORIES and how in {"structural", "domain", "centroid", "llm", "fallback"}
        assert 0 < confidence <= 1.0
