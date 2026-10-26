"""
system_group_classifier.py — 4-stage system group classifier (architecture §9; ENP module)
Echo Personal Memory System

Every item gets exactly one of work | study | entertainment | personal | misc.
Caller: enrichment_pipeline.classify_item -> classify_system_group(item, embedding)
-> (category, classified_by, confidence). The first stage that is sure wins:

  1. stage1_structural     source metadata: Gmail labels / sender domain,
                           YouTube Shorts + categoryId, Chrome .edu/.ac.in and
                           WORK_DOMAINS / ENTERTAINMENT_DOMAINS     -> 'structural', 1.0
  2. stage2_domain_lookup  Chrome only: DOMAIN_CATEGORY_MAP          -> 'domain', 0.95
  3. stage3_centroid       cosine of the item's embedding vector (its embeddable_text)
                           to 5 category centroids; needs best > 0.55 AND a
                           0.08 lead over the runner-up            -> 'centroid', cosine
  4. stage4_llm_fallback   source + title + first 300 chars of the clean text go to
                           the LLM (A21). provider 'openai': the Decisions API, one
                           `choice` question over the 5 names -> 'llm', API confidence
                           (logged, not stored). Otherwise, or if that call fails or
                           refuses: a strict one-word text call (_STAGE4_PROMPT)
                           -> 'llm', 0.90. No LLM configured / failure / bad answer
                           -> 'misc', 'fallback', 0.40

Centroids (CENTROIDS):
  - initialize_centroids (ENP start): embed the SEED_TEXTS sentences (8 per category)
    and average them into 5 centroids, then override them
    with data/.../centroids.npz when a recompute has saved one.
  - recompute_centroids (ENP start when centroids.npz is missing or > 30 days
    old): per category, average the per-source means of stored FAISS vectors
    classified 'structural', 'user_override' or 'llm' (needs >= 10 items), and
    save the result to centroids.npz and the current process's CENTROIDS.

Threading: CENTROIDS and the Stage-4 client are module globals written only by
the ENP thread (including its startup recompute); nothing else in the backend
calls this module, so no lock. Never logs titles or content.
"""

import logging
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any, Dict
from urllib.parse import urlparse

import numpy as np

from ste.faiss_manager import EMBEDDING_VERSION, VECTOR_DIMENSION, cosine, get_manager

logger = logging.getLogger(__name__)

# -------------------------
# CONSTANTS
# -------------------------
# Stage 1 (structural). Stage 2's DOMAIN_CATEGORY_MAP holds only domains NOT
# listed here: these lists are checked first, so a duplicate there would never
# be reached.
WORK_DOMAINS = [
    "github.com", "gitlab.com", "bitbucket.org",
    "slack.com", "teams.microsoft.com", "discord.com",
    "jira.com", "asana.com", "monday.com", "trello.com",
    "notion.so", "confluence.atlassian.net",
    "outlook.office.com", "mail.google.com",
    "zoom.us", "meet.google.com", "whereby.com",
    "aws.amazon.com", "cloud.google.com", "azure.microsoft.com",
    "salesforce.com", "hubspot.com", "pipedrive.com",
    "figma.com", "adobe.com", "canva.com",
    "tableau.com", "power.bi", "metabase.com",
    "stripe.com", "square.com", "shopify.com",
]

ENTERTAINMENT_DOMAINS = [
    "youtube.com", "youtu.be",
    "netflix.com", "primevideo.com", "hulu.com", "disneyplus.com",
    "spotify.com", "music.apple.com", "soundcloud.com",
    "twitch.tv", "kick.com",
    "reddit.com", "9gag.com", "imgur.com",
    "tiktok.com", "instagram.com", "facebook.com",
    "twitter.com", "x.com",
    "pinterest.com", "tumblr.com",
    "steam.com", "epicgames.com", "playstation.com", "xbox.com",
    "roblox.com", "minecraft.net",
    "imdb.com", "rottentomatoes.com",
    "letterboxd.com", "goodreads.com",
]

# Stage 2: domain -> category for Chrome pages. Matched on the host and every
# parent domain (docs.python.org -> python.org). Keys are host names only: the
# URL path is not looked at, so a "site.com/section" key could never match.
# Coverage is small and grown by hand (decision A17; extended 2026-10-24).
DOMAIN_CATEGORY_MAP = {
    # STUDY DOMAINS
    "geeksforgeeks.org": "study",
    "stackoverflow.com": "study",
    "leetcode.com": "study",
    "hackerrank.com": "study",
    "codewars.com": "study",
    "coursera.org": "study",
    "edx.org": "study",
    "udacity.com": "study",
    "udemy.com": "study",
    "khan-academy.org": "study",
    "khanacademy.org": "study",
    "duolingo.com": "study",
    "babbel.com": "study",
    "rosettastone.com": "study",
    "quizlet.com": "study",
    "wikipedia.org": "study",
    "scholar.google.com": "study",
    "researchgate.net": "study",
    "arxiv.org": "study",
    "openreview.net": "study",
    # medium.com and substack.com intentionally omitted — content is too varied
    # to classify by domain alone. These fall through to Stage 3 centroid.
    "dev.to": "study",
    "hashnode.com": "study",
    "freecodecamp.org": "study",
    "codecademy.com": "study",
    "datacamp.com": "study",
    "pluralsight.com": "study",
    "w3schools.com": "study",
    "mdn.mozilla.org": "study",
    "developer.mozilla.org": "study",
    "docs.microsoft.com": "study",
    "python.org": "study",
    "cplusplus.com": "study",
    "cppreference.com": "study",
    "rust-lang.org": "study",
    "golang.org": "study",
    "nodejs.org": "study",
    "ruby-lang.org": "study",
    "php.net": "study",
    "swift.org": "study",
    "kotlinlang.org": "study",
    "scala-lang.org": "study",
    "elixir-lang.org": "study",
    "erlang.org": "study",
    "haskell.org": "study",
    "clojure.org": "study",
    "numpy.org": "study",
    "scipy.org": "study",
    "matplotlib.org": "study",
    "pandas.pydata.org": "study",
    "scikit-learn.org": "study",
    "tensorflow.org": "study",
    "pytorch.org": "study",
    "keras.io": "study",
    "jupyter.org": "study",
    "anaconda.com": "study",
    "docker.com": "study",
    "kubernetes.io": "study",
    "mysql.com": "study",
    "postgresql.org": "study",
    "mongodb.com": "study",
    "redis.io": "study",
    "elasticsearch.org": "study",
    "graphql.org": "study",
    "restfulapi.net": "study",
    "swagger.io": "study",
    "openapi.tools": "study",
    "git-scm.com": "study",
    "svnbook.red-bean.com": "study",
    "jenkins.io": "study",
    "circleci.com": "study",
    "travis-ci.org": "study",
    "webpack.js.org": "study",
    "rollupjs.org": "study",
    "parceljs.org": "study",
    "vitejs.dev": "study",
    "nextjs.org": "study",
    "nuxtjs.org": "study",
    "reactjs.org": "study",
    "vuejs.org": "study",
    "angularjs.org": "study",
    "angular.io": "study",
    "svelte.dev": "study",
    "backbonejs.org": "study",
    "jquery.com": "study",
    "jsfiddle.net": "study",
    "codepen.io": "study",
    "jsbench.me": "study",
    "jsperf.com": "study",
    "caniuse.com": "study",
    # WORK DOMAINS
    "gmail.com": "work",
    "wistia.com": "work",
    "vimeo.com": "work",
    "airtable.com": "work",
    "zapier.com": "work",
    "ifttt.com": "work",
    "integromat.com": "work",
    "mailchimp.com": "work",
    "sendgrid.com": "work",
    "twilio.com": "work",
    "intercom.com": "work",
    "zendesk.com": "work",
    "freshdesk.com": "work",
    "servicenow.com": "work",
    "atlassian.net": "work",
    "bitbucket.io": "work",
    # ENTERTAINMENT DOMAINS
    "wattpad.com": "entertainment",
    "webtoon.com": "entertainment",
    "comixology.com": "entertainment",
    "crunchyroll.com": "entertainment",
    "funimation.com": "entertainment",
    "plex.tv": "entertainment",
    "peacocktv.com": "entertainment",
    "paramountplus.com": "entertainment",
    "appletv.apple.com": "entertainment",
    "hulumax.com": "entertainment",
    "max.hbo.com": "entertainment",
    "bandcamp.com": "entertainment",
    "deezer.com": "entertainment",
    "tidal.com": "entertainment",
    "youmusic.com": "entertainment",
    "pandora.com": "entertainment",
    "iheartradio.com": "entertainment",
    "radioparadise.com": "entertainment",
    "last.fm": "entertainment",
    "genius.com": "entertainment",
    "songkick.com": "entertainment",
    "eventbrite.com": "entertainment",
    "meetup.com": "entertainment",
    "4chan.org": "entertainment",
    "8kun.top": "entertainment",
    "mastodon.social": "entertainment",
    "pixiv.net": "entertainment",
    "danbooru.donmai.us": "entertainment",
    "deviantart.com": "entertainment",
    "artstation.com": "entertainment",
    "behance.net": "entertainment",
    # A17 hand-extension (2026-10-24), from the misses of the 2026-10-17 simulation of
    # typical domains. Ambiguous hosts (chatgpt.com, claude.ai, docs.google.com, news
    # sites) are left out on purpose, like medium.com: they go to Stage 3 / Stage 4.
    # study
    "kaggle.com": "study",
    "huggingface.co": "study",
    "javatpoint.com": "study",
    "tutorialspoint.com": "study",
    "programiz.com": "study",
    "realpython.com": "study",
    "baeldung.com": "study",
    # work (career and job search)
    "linkedin.com": "work",
    "naukri.com": "work",
    "internshala.com": "work",
    "indeed.com": "work",
    # entertainment
    "hotstar.com": "entertainment",
    "jiocinema.com": "entertainment",
    "sonyliv.com": "entertainment",
    "zee5.com": "entertainment",
    "bookmyshow.com": "entertainment",
    "cricbuzz.com": "entertainment",
    "espncricinfo.com": "entertainment",
    # personal (shopping, food, travel, money)
    "amazon.in": "personal",
    "amazon.com": "personal",
    "flipkart.com": "personal",
    "myntra.com": "personal",
    "swiggy.com": "personal",
    "zomato.com": "personal",
    "irctc.co.in": "personal",
    "makemytrip.com": "personal",
    "redbus.in": "personal",
    "zerodha.com": "personal",
    "paytm.com": "personal",
}

# Seed examples (A18): 8 short sentences per category, written like the titles and
# snippets that reach Stage 3 (Stage 1/2 already take the obvious domains and labels).
# Each seed centroid is the mean of its sentences' embeddings (initialize_centroids).
# One 7-9-word bag of words per category scored real items only 0.10-0.55.
SEED_TEXTS: Dict[str, list[str]] = {
    "work": [
        "Sprint planning meeting moved to Thursday, please update the project board",
        "Client review of the deliverable: feedback on the Q3 project report",
        "Pull request review: fix the failing API integration test before the release deadline",
        "Weekly status update on the product roadmap and team deadlines",
        "Invoice and contract details for the freelance project with the client",
        "Internship offer letter and onboarding schedule from the company HR team",
        "Production deployment checklist and incident postmortem for the backend service",
        "Job application status: interview scheduled with the engineering manager",
    ],
    "study": [
        "Lecture 5: dynamic programming and graph algorithms explained step by step",
        "Semester exam timetable and syllabus for the data structures course",
        "Tutorial: how gradient descent works in machine learning with worked examples",
        "Lab assignment submission deadline for operating systems and database management",
        "Research paper on transformer neural networks: abstract, method and results",
        "University notice: internal marks, attendance and registration for the next semester",
        "Chapter summary and practice problems from the computer networks textbook",
        "Online course certificate: learn Python programming, quizzes and coding exercises",
    ],
    "entertainment": [
        "Official trailer of the new movie, released in theatres this Friday",
        "Funny cricket match highlights and best moments of the IPL season",
        "Stand-up comedy special: hilarious jokes and audience reactions",
        "Top 10 gaming moments of the week: gameplay, walkthrough and live stream",
        "New song with lyrics, official music video and reaction",
        "Web series episode review, plot explained and ending breakdown",
        "Viral meme compilation and funny reels that went trending this week",
        "Daily vlog: travelling around with friends, food challenge and a fun day out",
    ],
    "personal": [
        "Happy birthday! Family dinner plans and gift ideas for this weekend",
        "Train ticket booking confirmation for the trip home during the holidays",
        "Order confirmation: your package from the online shop is out for delivery",
        "Bank account statement, UPI payment receipt and monthly expenses",
        "Wedding invitation from a cousin: ceremony date, venue and travel arrangements",
        "Doctor appointment reminder and health insurance details for the family",
        "Hey, are you free to catch up this evening? Let's meet with friends for dinner",
        "Flight and hotel booking for the vacation, packing list and itinerary",
    ],
    "misc": [
        "Breaking news: government announces new policy, analysts react to the update",
        "Newsletter: this week's top stories, articles and links worth reading",
        "Your verification code and account security alert for your login",
        "Terms of service and privacy policy update for our users",
        "Weather forecast, traffic and local news for today",
        "Encyclopedia article: history, geography and general facts about the topic",
        "Limited time offer: discount coupon and sale on selected products",
        "How to fix a common problem: quick tips, search results and general information",
    ],
}

# Stage 3 thresholds (doc §9.3). Measured 2026-10-17: against the old one-line SEED
# centroids, realistic items scored only 0.10-0.55, so Stage 3 fired on 1 of 20 (decision A18).
# CENTROID_*: a category whose centroid was recomputed from real items (centroids.npz).
CENTROID_MIN_SCORE = 0.55
CENTROID_MIN_MARGIN = 0.08
# SEED_*: a category still on its seed sentences (A18 part 2, measured 2026-10-26 on the 78 existing structural rows,
# scripts/calibrate_thresholds.py). With the 0.08 lead the seeds fired on 0 rows; the lowest lead with zero wrong
# answers is 0.05 (14 rows fire, 14 correct). No row scored below 0.585, so the score floor could not be measured
# and stays at 0.55.
SEED_MIN_SCORE = 0.55
SEED_MIN_MARGIN = 0.05

# category -> centroid vector; filled by initialize_centroids / recompute_centroids.
CENTROIDS: Dict[str, np.ndarray] = {}
# Categories whose centroid came from centroids.npz (kept when saving again).
_persisted_categories: set[str] = set()


def _normalize_domain(value: Any) -> str:
    """'https://www.Docs.Python.org/3/' or 'a@b.org' -> 'docs.python.org' / 'b.org'."""
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    if "@" in raw and "://" not in raw:
        raw = raw.rsplit("@", 1)[1]
    if "://" in raw:
        raw = urlparse(raw).netloc.lower()
    raw = raw.split("/", 1)[0].split(":", 1)[0].strip(".")
    if raw.startswith("www."):
        raw = raw[4:]
    return raw


def _domain_matches(domain: str, candidates) -> bool:
    """True when domain is a candidate or a subdomain of one."""
    normalized = _normalize_domain(domain)
    return any(normalized == candidate or normalized.endswith(f".{candidate}") for candidate in candidates)


def _domain_category(domain: str) -> str | None:
    """Look up the host, then each parent domain, in DOMAIN_CATEGORY_MAP."""
    normalized = _normalize_domain(domain)
    if not normalized:
        return None
    parts = normalized.split(".")
    variants = [".".join(parts[index:]) for index in range(len(parts))]
    for variant in variants:
        category = DOMAIN_CATEGORY_MAP.get(variant)
        if category:
            return category
    return None


def _coerce_category_id(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

# -------------------------
# INIT CENTROIDS
# -------------------------
def _centroids_path() -> Path:
    return get_manager().index_path.parent / "centroids.npz"


# Two extra arrays stored beside the category vectors. A centroid from another
# embedding model is a plain 384-d vector too (MiniLM and arctic are both 384-d),
# so only this stamp tells the two apart. Their names start with "_" and are not
# in SEED_TEXTS, so the loader never mistakes them for categories.
_VERSION_KEY = "_embedding_version"
_DIMENSION_KEY = "_dimension"


def _centroids_match_model(path: Path) -> bool:
    """True when centroids.npz was written for the current embedding model.

    A file with no stamp (written before the stamp existed) or another version or
    dimension counts as a mismatch: Stage 3 would compare this model's vectors with
    another model's centroids and give meaningless scores, without any error.
    """
    try:
        with np.load(path) as stored:
            if _VERSION_KEY not in stored.files or _DIMENSION_KEY not in stored.files:
                return False
            return str(stored[_VERSION_KEY]) == EMBEDDING_VERSION and int(stored[_DIMENSION_KEY]) == VECTOR_DIMENSION
    except Exception as exc:
        logger.warning("Could not read centroids.npz at %s: %s", path, exc)
        return False


def _load_persisted_centroids() -> bool:
    """Load centroids saved by a previous recompute_centroids() run.
    Returns True when at least one category was loaded."""
    path = _centroids_path()
    if not path.exists():
        return False
    try:
        if time.time() - path.stat().st_mtime > _CENTROID_MAX_AGE_SECONDS:
            logger.info("Ignoring centroids.npz older than 30 days; using seeds before recompute")
            return False
    except OSError:
        logger.warning("Could not inspect persisted centroid age at %s; using seeds", path)
        return False
    if not _centroids_match_model(path):
        logger.warning(
            "Ignoring centroids.npz: not written for embedding version %s; using seeds before recompute",
            EMBEDDING_VERSION,
        )
        return False
    try:
        with np.load(path) as stored:
            for category in stored.files:
                if category in SEED_TEXTS and stored[category].shape == (VECTOR_DIMENSION,):
                    CENTROIDS[category] = stored[category]
                    _persisted_categories.add(category)
    except Exception as exc:
        logger.warning("Failed to load persisted centroids from %s: %s", path, exc)
        return False
    logger.info("Loaded recomputed centroids for %s from %s", sorted(_persisted_categories), path.name)
    return bool(_persisted_categories)


def initialize_centroids(generate_embedding) -> None:
    """Seed all 5 centroids, then let recomputed ones (centroids.npz) override.

    Categories below the recompute threshold keep their seeds. Runs once per
    process; later calls do nothing.
    """
    if CENTROIDS:
        return
    for category, sentences in SEED_TEXTS.items():
        CENTROIDS[category] = np.mean(
            np.asarray([generate_embedding(sentence) for sentence in sentences], dtype=np.float32),
            axis=0,
        )
    if not _load_persisted_centroids():
        logger.info("Using seed centroids only (no centroids.npz yet)")

# -------------------------
# STAGE 1
# -------------------------
def stage1_structural(item: Dict[str, Any]):

    source = item.get("source")

    if source == "gmail":
        labels = item.get("gmail_labels", [])
        sender = _normalize_domain(item.get("sender_domain") or item.get("sender"))

        if labels and "CATEGORY_PERSONAL" in labels:
            return "personal", "structural", 1.0

        if labels and any(l in labels for l in ["CATEGORY_PROMOTIONS", "CATEGORY_UPDATES", "CATEGORY_FORUMS"]):
            return "misc", "structural", 1.0

        if sender.endswith((".edu", ".ac.in", ".ac.uk")):
            return "study", "structural", 1.0

        if _domain_matches(sender, WORK_DOMAINS):
            return "work", "structural", 1.0

    elif source == "youtube":
        category = _coerce_category_id(item.get("youtube_category_id"))
        is_short = item.get("is_short", False)

        if is_short:
            return "entertainment", "structural", 1.0

        if category in [27, 28]:
            return "study", "structural", 1.0

        if category in [10, 24, 20, 23]:
            return "entertainment", "structural", 1.0

    elif source == "chrome":
        domain = _normalize_domain(item.get("domain") or item.get("canonical_url") or item.get("url"))

        if domain.endswith((".edu", ".ac.in")):
            return "study", "structural", 1.0

        if _domain_matches(domain, WORK_DOMAINS):
            return "work", "structural", 1.0

        if _domain_matches(domain, ENTERTAINMENT_DOMAINS):
            return "entertainment", "structural", 1.0

    return None

# -------------------------
# STAGE 2
# -------------------------
def stage2_domain_lookup(item: Dict[str, Any]):

    if item.get("source") != "chrome":
        return None

    domain = item.get("domain") or item.get("canonical_url") or item.get("url") or ""
    category = _domain_category(domain)

    if category:
        return category, "domain", 0.95

    return None

# -------------------------
# STAGE 3
# -------------------------
def stage3_centroid(embedding):
    """Nearest centroid by true cosine (vectors are unit-length), only when
    the best score is high enough AND clearly ahead of the runner-up."""
    if embedding is None or len(CENTROIDS) < 2:
        return None

    scores = {category: cosine(embedding, centroid) for category, centroid in CENTROIDS.items()}
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    best_cat, best_score = sorted_scores[0]
    second_score = sorted_scores[1][1]

    recomputed = best_cat in _persisted_categories
    min_score = CENTROID_MIN_SCORE if recomputed else SEED_MIN_SCORE
    min_margin = CENTROID_MIN_MARGIN if recomputed else SEED_MIN_MARGIN
    if best_score > min_score and (best_score - second_score) > min_margin:
        return best_cat, "centroid", float(best_score)

    logger.debug(
        "Stage 3 not confident: best=%s %.2f, margin %.2f",
        best_cat,
        best_score,
        best_score - second_score,
    )
    return None

# -------------------------
# STAGE 4 — LLM fallback (architecture §9.4)
# -------------------------
_VALID_CATEGORIES = {"work", "study", "entertainment", "personal", "misc"}
_FALLBACK = ("misc", "fallback", 0.40)
_llm_client = None
_llm_unavailable = False

# The doc's prompt (§9.4), word for word. Only the source, a 200-char title and
# a 300-char snippet leave the machine (privacy rule: short snippets only).
_STAGE4_PROMPT = """Classify the following content into exactly one category:
  work | study | entertainment | personal | misc

Source: {source}
Title:  "{title}"
Snippet:"{snippet}"

Reply with exactly one word. Nothing else."""
_STAGE4_TITLE_CHARS = 200
_STAGE4_SNIPPET_CHARS = 300


def _get_stage4_llm():
    """
    Lazily build the fallback classifier LLM from the shared plug-and-play
    LLM_CONFIG (rse.config — single provider change point for the whole app),
    with the same temperature and fail-fast retry limit as the RSE's former query parser (parser_model).
    Returns None when no provider/API key is configured; Stage 4 then degrades
    to the deterministic 'misc' fallback so enrichment never blocks on the LLM.
    """
    global _llm_client, _llm_unavailable
    if _llm_client is not None or _llm_unavailable:
        return _llm_client
    try:
        from rse.llm_factory import build_chat_model
        from rse.config import LLM_CONFIG

        _llm_client = build_chat_model("parser_model", "parser_temperature")
        logger.info("Stage 4 LLM ready: %s / %s", LLM_CONFIG["provider"], LLM_CONFIG["parser_model"])
    except Exception as exc:
        logger.warning("Stage 4 LLM unavailable, falling back to 'misc': %s", exc)
        _llm_unavailable = True
        _llm_client = None
    return _llm_client


def _parse_category(reply: Any) -> str | None:
    """First word of the reply, lower-cased, punctuation stripped ('Work.' -> 'work')."""
    words = re.findall(r"[a-z]+", str(reply or "").lower())
    if words and words[0] in _VALID_CATEGORIES:
        return words[0]
    return None


def _stage4_fields(item) -> tuple[str, str, str]:
    """The only item data Stage 4 sends out: source, 200-char title, 300-char snippet."""
    return (
        item.get("source") or item.get("source_type") or "unknown",
        (item.get("title") or "")[:_STAGE4_TITLE_CHARS],
        (item.get("clean_text") or item.get("raw_text") or "")[:_STAGE4_SNIPPET_CHARS],
    )


# -------------------------
# Stage 4 via the OpenAI Decisions API (A21, beta; POST /v1/decisions)
# -------------------------
# One `choice` question over the 5 category names. The API answers with one of the
# supplied values (never free text), so there is no reply parsing. Only used when
# LLM_CONFIG["provider"] == "openai"; any failure drops that item to the one-word path.
_STAGE4_QUESTION = "Which category best describes this content?"
_STAGE4_CHOICES = {
    "work": "Job or professional tasks: meetings, projects, deadlines, clients, code, workplace tools.",
    "study": "Learning and coursework: lectures, tutorials, textbooks, exams, university or academic material.",
    "entertainment": "Leisure media: music, games, comedy, memes, trailers, vlogs, viral videos.",
    "personal": "Private life: family, friends, events, travel, personal messages.",
    "misc": "Anything else, or too general to place in the other four (news, shopping, general browsing).",
}
_STAGE4_DECISION_INPUT = """Source: {source}
Title: "{title}"
Snippet: "{snippet}\""""

# Circuit breaker: a persistent failure (beta not enabled for the account, bad key)
# would otherwise cost every item a failed call plus its retries before the
# one-word path runs. After this many failures in a row, skip the Decisions API for a while.
_DECISIONS_MAX_FAILURES = 3
_DECISIONS_PAUSE_SECONDS = 10 * 60

_decisions_setup = None  # (OpenAIDecisions client, {"category": Choice}) once built
_decisions_unavailable = False
_decisions_failures = 0
_decisions_paused_until = 0.0  # time.monotonic() value


def _get_stage4_decisions():
    """
    Lazily build the Decisions client and its question. Returns None (for the rest of
    the process) when the provider is not openai, LLM_CONFIG["decisions_model"] is
    empty, or langchain-openai is older than 1.7 (no `langchain_openai.decisions`).
    Timeout and retry limit come from LLM_CONFIG, like every other LLM client.
    """
    global _decisions_setup, _decisions_unavailable
    if _decisions_setup is not None or _decisions_unavailable:
        return _decisions_setup
    try:
        from rse.config import LLM_CONFIG

        model = LLM_CONFIG.get("decisions_model")
        if LLM_CONFIG["provider"] != "openai" or not model:
            _decisions_unavailable = True
            return None
        from langchain_openai.decisions import Choice, OpenAIDecisions

        client = OpenAIDecisions(
            model=model,
            max_retries=LLM_CONFIG.get("max_retries", 2),
            timeout=LLM_CONFIG.get("timeout"),
        )
        question = Choice(instructions=_STAGE4_QUESTION, choices=_STAGE4_CHOICES)
        _decisions_setup = (client, {"category": question})
        logger.info("Stage 4 Decisions API ready: %s", model)
    except Exception as exc:
        logger.warning("Stage 4 Decisions API unavailable, using the one-word call: %s", exc)
        _decisions_unavailable = True
        _decisions_setup = None
    return _decisions_setup


def _stage4_decisions(item):
    """
    Ask the Decisions API. Returns (category, 'llm', confidence) or None, meaning "use
    the one-word path". `confidence` is the API's own 0-1 value; it is only logged,
    never stored (A21). A refusal or an unexpected value returns None without counting
    toward the circuit breaker (it is about that item, not about the service).
    """
    global _decisions_failures, _decisions_paused_until
    if time.monotonic() < _decisions_paused_until:
        return None
    setup = _get_stage4_decisions()
    if setup is None:
        return None
    client, questions = setup

    source, title, snippet = _stage4_fields(item)
    request = {
        "input": _STAGE4_DECISION_INPUT.format(source=source, title=title, snippet=snippet),
        "questions": questions,
    }
    try:
        response = client.invoke(request)
    except Exception as exc:
        _decisions_failures += 1
        logger.warning("Stage 4 Decisions call failed (%s); using the one-word call", type(exc).__name__)
        if _decisions_failures >= _DECISIONS_MAX_FAILURES:
            _decisions_paused_until = time.monotonic() + _DECISIONS_PAUSE_SECONDS
            _decisions_failures = 0
            logger.warning("Stage 4 Decisions API paused for %d s after repeated failures", _DECISIONS_PAUSE_SECONDS)
        return None
    _decisions_failures = 0

    answer = response.choices.get("category")  # absent when the answer was a refusal
    if answer is None or answer.choice not in _VALID_CATEGORIES:
        logger.warning("Stage 4 Decisions gave no usable category (refused=%s); using the one-word call",
                       "category" in response.refusals)
        return None
    return answer.choice, "llm", float(answer.confidence)


def stage4_llm_fallback(item):
    """Decisions API on OpenAI, else (or if that item fails) the strict one-word text call."""
    result = _stage4_decisions(item)
    if result is not None:
        return result
    return _stage4_one_word(item)


def _stage4_one_word(item):
    llm = _get_stage4_llm()
    if llm is None:
        return _FALLBACK

    source, title, snippet = _stage4_fields(item)
    prompt = _STAGE4_PROMPT.format(source=source, title=title, snippet=snippet)
    try:
        response = llm.invoke(prompt)
    except Exception as exc:
        logger.warning("Stage 4 LLM call failed (%s); using 'misc'", type(exc).__name__)
        return _FALLBACK

    # .text, not .content: with the Responses API (LLM_CONFIG use_responses_api)
    # .content is a list of blocks, whose repr starts with the word "type".
    reply = response.text
    category = _parse_category(reply)
    if category is None:
        logger.warning("Stage 4 LLM reply is not a category (%r); using 'misc'", reply[:40])
        return _FALLBACK
    return category, "llm", 0.90

# -------------------------
# CENTROID RECOMPUTATION (architecture §9.3 — "How Centroids Improve Over Time")
# -------------------------
_MIN_CONFIRMED_FOR_RECOMPUTE = 10
_CENTROID_MAX_AGE_SECONDS = 30 * 24 * 60 * 60


def centroids_need_recompute() -> bool:
    """Return True when no saved centroids exist or they are over 30 days old."""
    path = _centroids_path()
    try:
        age_seconds = time.time() - path.stat().st_mtime
    except FileNotFoundError:
        logger.info("No persisted centroids found; recomputing at ENP start")
        return True
    except OSError:
        logger.exception("Could not inspect persisted centroids at %s; attempting recompute", path)
        return True

    if age_seconds > _CENTROID_MAX_AGE_SECONDS:
        logger.info("Persisted centroids are older than 30 days; recomputing at ENP start")
        return True
    if not _centroids_match_model(path):
        logger.info("Persisted centroids are not for embedding version %s; recomputing at ENP start", EMBEDDING_VERSION)
        return True
    return False


def _save_centroids(categories: set[str]) -> None:
    """Write CENTROIDS[categories] to centroids.npz atomically (temp file + replace)."""
    path = _centroids_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    # The temp name must end in .npz, or np.savez appends it.
    fd, temp_name = tempfile.mkstemp(dir=path.parent, prefix="centroids.", suffix=".tmp.npz")
    os.close(fd)
    try:
        np.savez(
            temp_name,
            **{category: CENTROIDS[category] for category in sorted(categories)},
            **{_VERSION_KEY: np.array(EMBEDDING_VERSION), _DIMENSION_KEY: np.array(VECTOR_DIMENSION)},
        )
        os.replace(temp_name, path)
    except BaseException:
        Path(temp_name).unlink(missing_ok=True)
        raise


def recompute_centroids() -> Dict[str, int]:
    """
    Recompute each category centroid as the mean of its per-source mean embeddings
    (classified_by IN structural/user_override — plus llm results, which the
    architecture also feeds back into the monthly recomputation, doc §9.4).
    Each source with vectors gets one equal vote, regardless of its item count.

    A category keeps its current centroid until it has at least 10 confirmed
    items. The ENP worker runs this at startup when centroids.npz is missing or
    > 30 days old — passive learning,
    no retraining. Expects the shared FAISS manager to be loaded and
    initialize_centroids() to have run.

    Returns:
        {category: confirmed_item_count} for every category that was updated.
    """
    from ste import postgresql_manager

    rows = postgresql_manager.fetchall(
        """
        SELECT sg.group_name, m.source_type, m.memory_id
        FROM memory_items m
        JOIN system_groups sg ON m.system_group_id = sg.system_group_id
        WHERE m.is_deleted = FALSE
          AND m.preprocessed = TRUE
          AND m.classified_by IN ('structural', 'user_override', 'llm')
        """
    )

    manager = get_manager()
    by_category: Dict[str, Dict[str, list]] = {}
    not_indexed = 0
    for row in rows:
        vector = manager.get_vector(str(row["memory_id"]))  # locked read (tracker C4)
        if vector is None:
            not_indexed += 1
            continue
        by_category.setdefault(row["group_name"], {}).setdefault(row["source_type"], []).append(vector)
    if not_indexed:
        logger.warning("recompute_centroids: %d confirmed items have no FAISS vector; skipped", not_indexed)

    updated: Dict[str, int] = {}
    for category, vectors_by_source in by_category.items():
        item_count = sum(len(vectors) for vectors in vectors_by_source.values())
        if category not in _VALID_CATEGORIES or item_count < _MIN_CONFIRMED_FOR_RECOMPUTE:
            logger.info("recompute_centroids: %s kept (%d confirmed items, need %d)",
                        category, item_count, _MIN_CONFIRMED_FOR_RECOMPUTE)
            continue
        source_means = [
            np.mean(np.asarray(vectors, dtype=np.float32), axis=0)
            for vectors in vectors_by_source.values()
        ]
        CENTROIDS[category] = np.mean(np.asarray(source_means, dtype=np.float32), axis=0)
        updated[category] = item_count
        logger.info("recompute_centroids: %s updated from %d confirmed items across %d sources",
                    category, item_count, len(source_means))

    if updated:
        # Persist for future starts. Categories recomputed in an earlier run are
        # saved again too, so a run that updates fewer never drops them.
        to_save = set(updated) | _persisted_categories
        try:
            _save_centroids(to_save)
            _persisted_categories.update(updated)
            logger.info("recompute_centroids: persisted %d centroids to %s", len(to_save), _centroids_path())
        except Exception:
            logger.exception("recompute_centroids: failed to persist centroids")

    return updated


# -------------------------
# MAIN FUNCTION
# -------------------------
def classify_system_group(item: Dict[str, Any], embedding=None):
    """Run the stages in order; the first confident one wins. Never returns
    None: the last resort is 'misc' (system_group_id must never be NULL)."""
    result = stage1_structural(item)
    if result is None:
        result = stage2_domain_lookup(item)
    if result is None and embedding is not None:
        result = stage3_centroid(embedding)
    if result is None:
        result = stage4_llm_fallback(item)

    logger.debug(
        "Classified memory_id=%s (%s) as %s via %s (%.2f)",
        item.get("memory_id"),
        item.get("source"),
        *result,
    )
    return result
