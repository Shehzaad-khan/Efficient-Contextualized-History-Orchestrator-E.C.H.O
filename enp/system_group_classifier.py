import logging
from typing import Any, Dict
from urllib.parse import urlparse

from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger(__name__)

# -------------------------
# CONSTANTS
# -------------------------
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

# Comprehensive domain-to-category mappings (200+ entries)
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
    "linkedin.com/learning": "study",
    "w3schools.com": "study",
    "mdn.mozilla.org": "study",
    "developer.mozilla.org": "study",
    "docs.microsoft.com": "study",
    "cloud.google.com/docs": "study",
    "aws.amazon.com/documentation": "study",
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
    "github.com/actions": "study",
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
    "ember.js": "study",
    "backbonejs.org": "study",
    "jquery.com": "study",
    "jsfiddle.net": "study",
    "codepen.io": "study",
    "jsbench.me": "study",
    "jsperf.com": "study",
    "caniuse.com": "study",
    # WORK DOMAINS
    "github.com": "work",
    "gitlab.com": "work",
    "bitbucket.org": "work",
    "slack.com": "work",
    "teams.microsoft.com": "work",
    "discord.com": "work",
    "jira.com": "work",
    "asana.com": "work",
    "monday.com": "work",
    "trello.com": "work",
    "notion.so": "work",
    "confluence.atlassian.net": "work",
    "outlook.office.com": "work",
    "mail.google.com": "work",
    "gmail.com": "work",
    "zoom.us": "work",
    "meet.google.com": "work",
    "whereby.com": "work",
    "aws.amazon.com": "work",
    "cloud.google.com": "work",
    "azure.microsoft.com": "work",
    "salesforce.com": "work",
    "hubspot.com": "work",
    "pipedrive.com": "work",
    "figma.com": "work",
    "adobe.com": "work",
    "canva.com": "work",
    "tableau.com": "work",
    "power.bi": "work",
    "metabase.com": "work",
    "stripe.com": "work",
    "square.com": "work",
    "shopify.com": "work",
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
    "youtube.com": "entertainment",
    "youtu.be": "entertainment",
    "netflix.com": "entertainment",
    "primevideo.com": "entertainment",
    "hulu.com": "entertainment",
    "disneyplus.com": "entertainment",
    "spotify.com": "entertainment",
    "music.apple.com": "entertainment",
    "soundcloud.com": "entertainment",
    "twitch.tv": "entertainment",
    "kick.com": "entertainment",
    "reddit.com": "entertainment",
    "9gag.com": "entertainment",
    "imgur.com": "entertainment",
    "tiktok.com": "entertainment",
    "instagram.com": "entertainment",
    "facebook.com": "entertainment",
    "twitter.com": "entertainment",
    "x.com": "entertainment",
    "pinterest.com": "entertainment",
    "tumblr.com": "entertainment",
    "steam.com": "entertainment",
    "epicgames.com": "entertainment",
    "playstation.com": "entertainment",
    "xbox.com": "entertainment",
    "roblox.com": "entertainment",
    "minecraft.net": "entertainment",
    "imdb.com": "entertainment",
    "rottentomatoes.com": "entertainment",
    "letterboxd.com": "entertainment",
    "goodreads.com": "entertainment",
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

CENTROIDS = {}


def _normalize_domain(value: Any) -> str:
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
    normalized = _normalize_domain(domain)
    return any(normalized == candidate or normalized.endswith(f".{candidate}") for candidate in candidates)


def _domain_category(domain: str) -> str | None:
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
def _centroids_path():
    from pathlib import Path
    from ste.faiss_manager import DEFAULT_INDEX_PATH

    return Path(DEFAULT_INDEX_PATH).parent / "centroids.npz"


def _load_persisted_centroids() -> bool:
    """Load centroids saved by a previous recompute_centroids() run.
    Returns True when at least one category was loaded."""
    import numpy as np

    path = _centroids_path()
    if not path.exists():
        return False
    try:
        with np.load(path) as stored:
            for category in stored.files:
                if category in SEED_TEXTS:
                    CENTROIDS[category] = stored[category]
        return bool(CENTROIDS)
    except Exception as exc:
        logger.warning("Failed to load persisted centroids from %s: %s", path, exc)
        return False


def initialize_centroids(generate_embedding):
    global CENTROIDS
    if CENTROIDS:
        return
    # Seed every category first, then let persisted (recomputed) centroids
    # override — categories below the recompute threshold keep their seeds.
    for category, text in SEED_TEXTS.items():
        CENTROIDS[category] = generate_embedding(text)
    _load_persisted_centroids()

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
    if embedding is None or len(CENTROIDS) < 2:
        return None

    scores = {}

    for category, centroid in CENTROIDS.items():
        sim = cosine_similarity([embedding], [centroid])[0][0]
        scores[category] = sim

    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    best_cat, best_score = sorted_scores[0]
    second_score = sorted_scores[1][1]

    if best_score > 0.55 and (best_score - second_score) > 0.08:
        return best_cat, "centroid", float(best_score)

    return None

# -------------------------
# STAGE 4 — LLM fallback (architecture §9.4)
# -------------------------
_VALID_CATEGORIES = {"work", "study", "entertainment", "personal", "misc"}
_llm_client = None
_llm_unavailable = False

_STAGE4_PROMPT = """Classify the following content into exactly one category:
  work | study | entertainment | personal | misc

Source: {source}
Title:  "{title}"
Snippet:"{snippet}"

Reply with exactly one word. Nothing else."""


def _get_stage4_llm():
    """
    Lazily build the fallback classifier LLM from the shared plug-and-play
    LLM_CONFIG (rse.config — single provider change point for the whole app).
    Returns None when no provider/API key is configured; Stage 4 then degrades
    to the deterministic 'misc' fallback so enrichment never blocks on the LLM.
    """
    global _llm_client, _llm_unavailable
    if _llm_client is not None or _llm_unavailable:
        return _llm_client
    try:
        from langchain.chat_models import init_chat_model
        from rse.config import LLM_CONFIG

        _llm_client = init_chat_model(
            model=LLM_CONFIG["parser_model"],
            model_provider=LLM_CONFIG["provider"],
            temperature=0.0,
        )
    except Exception as exc:
        logger.warning("Stage 4 LLM unavailable, falling back to 'misc': %s", exc)
        _llm_unavailable = True
        _llm_client = None
    return _llm_client


def stage4_llm_fallback(item):
    llm = _get_stage4_llm()
    if llm is None:
        return "misc", "fallback", 0.40

    prompt = _STAGE4_PROMPT.format(
        source=item.get("source") or item.get("source_type") or "unknown",
        title=(item.get("title") or "")[:200],
        snippet=(item.get("clean_text") or item.get("raw_text") or "")[:300],
    )
    try:
        response = llm.invoke(prompt)
        category = str(response.content).strip().lower().split()[0]
        if category in _VALID_CATEGORIES:
            return category, "llm", 0.90
        logger.warning("Stage 4 LLM returned invalid category %r; using 'misc'", category)
    except Exception as exc:
        logger.warning("Stage 4 LLM classification failed: %s", exc)
    return "misc", "fallback", 0.40

# -------------------------
# CENTROID RECOMPUTATION (architecture §9.3 — "How Centroids Improve Over Time")
# -------------------------
_MIN_CONFIRMED_FOR_RECOMPUTE = 10


def recompute_centroids() -> Dict[str, int]:
    """
    Recompute each category centroid as the mean embedding of confirmed items
    (classified_by IN structural/user_override — plus llm results, which the
    architecture also feeds back into the monthly recomputation).

    A category keeps its current centroid until it has at least 10 confirmed
    items. Intended to run monthly via scripts/enp_recompute_centroids.py —
    passive learning, no retraining.

    Returns:
        {category: confirmed_item_count} for every category that was updated.
    """
    import numpy as np

    from ste import postgresql_manager
    from enp.faiss_manager import get_manager

    rows = postgresql_manager.fetchall(
        """
        SELECT sg.group_name, m.memory_id
        FROM memory_items m
        JOIN system_groups sg ON m.system_group_id = sg.system_group_id
        WHERE m.is_deleted = FALSE
          AND m.preprocessed = TRUE
          AND m.classified_by IN ('structural', 'user_override', 'llm')
        """
    )

    manager = get_manager()
    by_category: Dict[str, list] = {}
    for row in rows:
        offset = manager.memory_id_to_offset.get(str(row["memory_id"]))
        if offset is not None:
            by_category.setdefault(row["group_name"], []).append(manager.vectors[offset])

    updated: Dict[str, int] = {}
    for category, vectors in by_category.items():
        if category not in _VALID_CATEGORIES or len(vectors) < _MIN_CONFIRMED_FOR_RECOMPUTE:
            continue
        CENTROIDS[category] = np.mean(np.asarray(vectors, dtype=np.float32), axis=0)
        updated[category] = len(vectors)
        logger.info("recompute_centroids: %s updated from %d confirmed items", category, len(vectors))

    if updated:
        # Persist so the ENP worker (a different process) picks the new
        # centroids up on its next start via initialize_centroids().
        path = _centroids_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            np.savez(path, **{c: CENTROIDS[c] for c in updated})
            logger.info("recompute_centroids: persisted %d centroids to %s", len(updated), path)
        except Exception as exc:
            logger.error("recompute_centroids: failed to persist centroids — %s", exc)

    return updated


# -------------------------
# MAIN FUNCTION
# -------------------------
def classify_system_group(item: Dict[str, Any], embedding=None):
    if item.get("source") == "youtube" and item.get("is_short"):
        return "entertainment", "structural", 1.0

    result = stage1_structural(item)
    if result:
        return result

    result = stage2_domain_lookup(item)
    if result:
        return result

    if embedding is not None:
        result = stage3_centroid(embedding)
        if result:
            return result

    return stage4_llm_fallback(item)
