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
  3. stage3_centroid       cosine of the item's MiniLM vector (its embeddable_text)
                           to 5 category centroids; needs best > 0.55 AND a
                           0.08 lead over the runner-up            -> 'centroid', cosine
  4. stage4_llm_fallback   LLM reads source + title + first 300 chars of the
                           clean text (_STAGE4_PROMPT, the doc's prompt) -> 'llm', 0.90
                           No LLM configured / failure / bad answer -> 'misc', 'fallback', 0.40

Centroids (CENTROIDS):
  - initialize_centroids (ENP start): embed the 5 SEED_TEXTS, then override them
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

from ste.faiss_manager import cosine, get_manager

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
# Coverage is small (decision A17).
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
}

SEED_TEXTS = {
    "work": "meeting deadline client project deliverable sprint report",
    "study": "lecture tutorial concept algorithm textbook exam university",
    "entertainment": "funny comedy music gaming meme trailer reaction vlog viral",
    "personal": "family friend birthday vacation message dinner wedding travel",
    "misc": "general information article news update random browse read",
}

# Stage 3 thresholds (doc §9.3). Measured 2026-10-17: against the SEED centroids,
# realistic items score only 0.10-0.55, so Stage 3 fired on 1 of 20 (decision A18).
CENTROID_MIN_SCORE = 0.55
CENTROID_MIN_MARGIN = 0.08

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
    try:
        with np.load(path) as stored:
            for category in stored.files:
                if category in SEED_TEXTS:
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
    for category, text in SEED_TEXTS.items():
        CENTROIDS[category] = generate_embedding(text)
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

    if best_score > CENTROID_MIN_SCORE and (best_score - second_score) > CENTROID_MIN_MARGIN:
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
    with the same temperature and fail-fast retry limit as RSE's parse_intent.
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


def stage4_llm_fallback(item):
    llm = _get_stage4_llm()
    if llm is None:
        return _FALLBACK

    prompt = _STAGE4_PROMPT.format(
        source=item.get("source") or item.get("source_type") or "unknown",
        title=(item.get("title") or "")[:_STAGE4_TITLE_CHARS],
        snippet=(item.get("clean_text") or item.get("raw_text") or "")[:_STAGE4_SNIPPET_CHARS],
    )
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
    return False


def _save_centroids(categories: set[str]) -> None:
    """Write CENTROIDS[categories] to centroids.npz atomically (temp file + replace)."""
    path = _centroids_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    # The temp name must end in .npz, or np.savez appends it.
    fd, temp_name = tempfile.mkstemp(dir=path.parent, prefix="centroids.", suffix=".tmp.npz")
    os.close(fd)
    try:
        np.savez(temp_name, **{category: CENTROIDS[category] for category in sorted(categories)})
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
