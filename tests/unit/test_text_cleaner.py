"""Unit tests for enp/text_cleaner.py: the cleaning every item goes through before it is embedded."""
from enp import text_cleaner as tc


def test_normalize_text_unifies_line_endings_and_spaces():
    """Shows: Windows line endings, tabs, non-breaking spaces and blank-line runs are tidied."""
    raw = "Hello\r\n\r\n\r\n\r\nWorld \t  again here​"
    assert tc.normalize_text(raw) == "Hello\n\nWorld again here"


def test_truncate_cuts_at_a_word_boundary():
    """Shows: snippets never end in half a word."""
    assert tc.truncate("one two three four", limit=10) == "one two"
    assert tc.truncate("short", limit=100) == "short"


def test_html_detection_ignores_math_in_prose():
    """Shows: 'a < b and b > c' is text, '<div>' is HTML."""
    assert not tc.looks_like_html("if a < b and b > c then stop")
    assert tc.looks_like_html("<div>hello</div>")
    assert not tc.looks_like_html("")


def test_html_to_text_drops_scripts_comments_and_keeps_lines():
    """Shows: tracking pixels, scripts and comments vanish; paragraphs stay on their own lines."""
    html = (
        "<html><head><title>t</title><style>p{}</style></head><body>"
        "<!-- hidden --><p>First &amp; second</p><p>Third</p>"
        "<img src='https://track.example/pixel.gif'><script>alert(1)</script></body></html>"
    )
    text = tc.html_to_text(html)
    assert text == "First & second\nThird"
    assert tc.html_to_text("") == ""


def test_remove_quoted_replies_drops_quotes_and_header():
    """Shows: the quoted old mail is removed but the new reply stays."""
    mail = "Sounds good, see you Friday.\n\nOn Mon, 5 Oct 2026, Priya <p@x.com> wrote:\n> Are you free Friday?\n> Let me know."
    assert tc.remove_quoted_replies(mail) == "Sounds good, see you Friday.\n"


def test_remove_quoted_replies_keeps_inline_answers_below_a_header():
    """Shows: an answer written between quoted lines is kept."""
    mail = "On Mon, Priya wrote:\n> Question one?\nMy answer one.\n> Question two?\nMy answer two."
    cleaned = tc.remove_quoted_replies(mail)
    assert "My answer one." in cleaned and "My answer two." in cleaned
    assert "Question" not in cleaned


def test_remove_quoted_replies_cuts_an_unmarked_old_thread():
    """Shows: with no '>' markers the old thread below the header is cut."""
    mail = "Thanks!\n\nOn Mon, Priya wrote:\nOld text with no markers"
    assert tc.remove_quoted_replies(mail).strip() == "Thanks!"


def test_remove_quoted_replies_handles_a_wrapped_header_and_empty_input():
    """Shows: Gmail's wrapped 'On ... / wrote:' header is recognised; empty text is safe."""
    mail = "Reply body\nOn Mon, 5 Oct 2026 at 10:00, Priya Sharma <priya@example.com>\nwrote:\n> old"
    assert tc.remove_quoted_replies(mail).strip() == "Reply body"
    assert tc.remove_quoted_replies("") == ""


def test_clean_gmail_text_plain_and_html():
    """Shows: Gmail rows that still hold HTML are converted, plain rows are only normalised."""
    assert tc.clean_gmail_text("Hi  there\n> quoted").clean_text == "Hi there"
    assert tc.clean_gmail_text("<p>Hello</p><p>World</p>").clean_text == "Hello\nWorld"
    assert tc.clean_gmail_text("").clean_text == ""


def test_lift_heading_lines_takes_only_leading_headings():
    """Shows: the extension's '## heading' lines are lifted off the top and the body is untouched."""
    headings, body = tc.lift_heading_lines("## One\n## Two\nBody ## not a heading")
    assert headings == ["One", "Two"]
    assert body == "Body ## not a heading"


def test_lift_heading_lines_respects_the_limits():
    """Shows: at most 10 headings and none longer than 200 characters are taken."""
    many = "\n".join(f"## H{i}" for i in range(15)) + "\nbody"
    headings, _ = tc.lift_heading_lines(many)
    assert len(headings) == tc.MAX_HEADINGS
    headings, body = tc.lift_heading_lines("## " + "x" * 201 + "\nbody")
    assert headings == [] and body.startswith("## ")


def test_clean_chrome_text_plain_with_headings():
    """Shows: the normal Chrome row is a plain string with heading lines first."""
    cleaned = tc.clean_chrome_text("## Title\nArticle   body here")
    assert cleaned.headings == ["Title"]
    assert cleaned.clean_text == "Article body here"
    assert tc.clean_chrome_text("").clean_text == ""


def test_clean_chrome_text_html_path_extracts_the_article():
    """Shows: an old row holding real HTML goes through the article extractor."""
    html = "<html><body><nav>menu</nav><article><h1>Big Story</h1><p>" + "Real words. " * 30 + "</p></article></body></html>"
    cleaned = tc.clean_chrome_text(html)
    assert "Real words." in cleaned.clean_text
    assert "Big Story" in cleaned.headings or "Big Story" in cleaned.clean_text


def test_fallback_article_extractor_keeps_the_biggest_block():
    """Shows: the stand-in for Readability drops page chrome and keeps the main text."""
    html = "<html><body><nav>skip</nav><main><p>" + "keep this text " * 20 + "</p></main><footer>skip</footer></body></html>"
    extracted = tc._fallback_extract_article_html(html)
    assert "keep this text" in extracted and "skip" not in extracted
    assert tc._fallback_extract_article_html("") == ""


def test_clean_youtube_text_joins_title_description_transcript():
    """Shows: YouTube text is the three parts joined, blanks skipped."""
    assert tc.clean_youtube_text("Title", "  ", "Words").clean_text == "Title Words"


def test_clean_item_text_routes_by_source():
    """Shows: each source has its own cleaning rule, unknown sources are only normalised."""
    assert tc.clean_item_text({"source": "gmail", "raw_text": "Hi\n> old"}).clean_text == "Hi"
    assert tc.clean_item_text({"source_type": "chrome", "raw_text": "## H\nbody"}).headings == ["H"]
    assert tc.clean_item_text({"source": "youtube", "title": "T", "description": "D"}).clean_text == "T D"
    assert tc.clean_item_text({"source": "other", "title": "Just  a title"}).clean_text == "Just a title"
