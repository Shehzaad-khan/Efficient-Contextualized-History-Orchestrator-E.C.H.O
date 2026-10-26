"""Unit tests for keyword/topic extraction and the embedding window (A16 + A40), with a fake model."""
import numpy as np
import pytest

from enp import embedding_generator as eg
from enp import topic_extractor as te
from enp.topic_extractor import TextPart


# ── Keywords and embeddable text ──────────────────────────────────────────────

def test_extract_keywords_ranks_by_count_and_drops_stopwords_and_numbers():
    text = "Interview interview schedule schedule schedule the and 2026 12345 for you"
    assert te.extract_keywords(text) == ["schedule", "interview"]
    assert te.extract_keywords("") == []
    assert len(te.extract_keywords("alpha bravo charlie delta echo foxtrot golf hotel india", limit=3)) == 3


def test_sender_and_domain_hints():
    assert te.sender_domain_hint("Priya <priya@Acme.io>") == "acme.io"
    assert te.sender_domain_hint("bob@example.com") == "example.com"
    assert te.sender_domain_hint("no address") == ""
    assert te.sender_domain_hint("") == ""
    assert te.domain_hint("https://News.Example.com/a") == "news.example.com"
    assert te.domain_hint(" Example.com ") == "example.com"
    assert te.domain_hint("") == ""


def test_youtube_category_hint():
    assert te.youtube_category_hint(27) == "education learning tutorial"
    assert te.youtube_category_hint("20") == "gaming"
    assert te.youtube_category_hint("abc") == ""
    assert te.youtube_category_hint(None) == ""
    assert te.youtube_category_hint(999) == ""


def test_gmail_keywords_add_thread_context_without_duplicates():
    own = te.gmail_keywords("Quick thanks", "looks good", message_history=["scheduling project scheduling"])
    assert "scheduling" in own and "project" in own
    assert len(own) == len(set(own))


def test_filename_words_drop_extension_and_separators():
    assert te._filename_words(["Offer_Letter-2026.pdf"]) == "Offer Letter 2026"


def test_coerce_text_accepts_lists_and_dicts():
    assert te._coerce_text(None) == ""
    assert te._coerce_text(["a", {"k": "b"}]) == "a b"
    assert te._coerce_text(5) == "5"


def test_build_embeddable_text_gmail_puts_short_hints_first_and_body_last():
    item = {"source": "gmail", "subject": "TechCorp interview", "sender": "Priya <p@techcorp.com>",
            "attachment_filenames": ["schedule.pdf"],
            "attachment_texts": [{"text": "Round one on Friday"}, {"text": "  "}]}
    parts, keywords = te.build_embeddable_text(item, "See you on Friday for the interview")
    assert parts[0] == TextPart("TechCorp interview", None)
    assert parts[2].text == "techcorp.com"
    body = parts[4]
    assert body.text.startswith("See you") and body.max_tokens == te.GMAIL_BODY_MAX_TOKENS
    assert parts[-1].text == "Round one on Friday"
    assert "interview" in keywords


def test_build_embeddable_text_other_sources():
    chrome_parts, _ = te.build_embeddable_text(
        {"source": "chrome", "title": "Pandas guide", "domain": "pandas.pydata.org"}, "dataframes", ["Intro"])
    assert [p.text for p in chrome_parts] == ["Pandas guide", "Intro", "pandas.pydata.org", "dataframes"]
    yt_parts, yt_keywords = te.build_embeddable_text(
        {"source": "youtube", "title": "Learn Python", "channel_name": "CodeCh", "youtube_category_id": 27,
         "description": "A python course"}, "")
    assert yt_parts[1].text == "CodeCh" and yt_parts[3].max_tokens == te.YOUTUBE_DESCRIPTION_MAX_TOKENS
    assert "python" in yt_keywords
    other, _ = te.build_embeddable_text({"source": "x", "title": "T"}, "body")
    assert [p.text for p in other] == ["T", "body"]


# ── fit_to_window with a fake tokenizer (1 word = 1 token, window = 510) ──────

def words(n, prefix="w"):
    return " ".join(f"{prefix}{i}" for i in range(n))


def test_fit_to_window_keeps_short_text_whole(fake_embedding_model):
    assert eg.fit_to_window([TextPart("Hello", None), TextPart("world", None)]) == "Hello world"
    assert eg.fit_to_window([TextPart("", None)]) == ""


def test_fit_to_window_never_exceeds_the_models_window(fake_embedding_model):
    """Shows: the stored embeddable_text is always within the 510-token budget."""
    long_body = words(2000)
    fitted = eg.fit_to_window([TextPart("title hint", None), TextPart(long_body, 300), TextPart(words(400, "a"), None)])
    assert len(fitted.split()) <= 510
    assert fitted.startswith("title hint")


def test_fit_to_window_caps_a_part_only_while_later_parts_need_room(fake_embedding_model):
    """Shows: unused room flows back to a capped part (no body is cut while room is free)."""
    fitted = eg.fit_to_window([TextPart(words(400), 300)])
    assert len(fitted.split()) == 400
    fitted = eg.fit_to_window([TextPart(words(400), 300), TextPart(words(300, "z"), None)])
    tokens = fitted.split()
    assert len(tokens) == 510 and tokens[300] == "z0"


def test_cut_to_tokens_backs_off_to_a_whole_word(fake_embedding_model):
    text = "alpha beta gamma"
    ends = eg._token_end_offsets(text)
    assert eg._cut_to_tokens(text, ends, 2) == "alpha beta"
    assert eg._cut_to_tokens(text, ends, 0) == ""
    assert eg._cut_to_tokens(text, ends, 10) == text


def test_window_prefix_shortens_huge_texts(fake_embedding_model):
    huge = ("word " * 30000).strip()
    assert len(eg._window_prefix(huge, 510)) <= eg.PREFIX_CHARS


# ── Embedding checks ──────────────────────────────────────────────────────────

def test_embeddings_are_unit_length_and_384_wide(fake_embedding_model):
    vectors = eg.generate_embeddings(["one text", "another text"])
    assert vectors.shape == (2, 384)
    assert np.allclose(np.linalg.norm(vectors, axis=1), 1.0, atol=1e-3)
    assert eg.generate_embedding("single").shape == (384,)
    assert eg.generate_query_embedding("question").shape == (384,)
    assert eg.generate_embeddings([]).shape == (0, 384)


def test_similar_texts_are_closer_than_different_ones(fake_embedding_model):
    a, b, c = eg.generate_embeddings(["python pandas dataframe", "pandas dataframe tutorial", "football match score"])
    assert float(a @ b) > float(a @ c)


def test_validate_embeddings_rejects_wrong_size_and_non_unit_vectors():
    with pytest.raises(ValueError, match="dimension"):
        eg._validate_embeddings(np.ones((1, 10), dtype=np.float32))
    with pytest.raises(ValueError, match="unit length"):
        eg._validate_embeddings(np.full((1, 384), 0.5, dtype=np.float32))
