import numpy as np
from typing import Dict, Any
from sklearn.metrics.pairwise import cosine_similarity

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
    "medium.com": "study",
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
    "serviceNow.com": "work",
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

# -------------------------
# INIT CENTROIDS
# -------------------------
def initialize_centroids(generate_embedding):
    global CENTROIDS
    for category, text in SEED_TEXTS.items():
        CENTROIDS[category] = generate_embedding(text)

# -------------------------
# STAGE 1
# -------------------------
def stage1_structural(item: Dict[str, Any]):

    source = item.get("source")

    if source == "gmail":
        labels = item.get("gmail_labels", [])
        sender = item.get("sender_domain", "")

        if labels and "CATEGORY_PERSONAL" in labels:
            return "personal", "structural", 1.0

        if labels and any(l in labels for l in ["CATEGORY_PROMOTIONS", "CATEGORY_UPDATES", "CATEGORY_FORUMS"]):
            return "misc", "structural", 1.0

        if sender.endswith((".edu", ".ac.in", ".ac.uk")):
            return "study", "structural", 1.0

        if sender in WORK_DOMAINS:
            return "work", "structural", 1.0

    elif source == "youtube":
        category = item.get("youtube_category_id")
        is_short = item.get("is_short", False)

        if is_short:
            return "entertainment", "structural", 1.0

        if category in [27, 28]:
            return "study", "structural", 1.0

        if category in [10, 24, 20, 23]:
            return "entertainment", "structural", 1.0

    elif source == "chrome":
        domain = item.get("domain", "")

        if domain.endswith((".edu", ".ac.in")):
            return "study", "structural", 1.0

        if domain in WORK_DOMAINS:
            return "work", "structural", 1.0

        if domain in ENTERTAINMENT_DOMAINS:
            return "entertainment", "structural", 1.0

    return None

# -------------------------
# STAGE 2
# -------------------------
def stage2_domain_lookup(item: Dict[str, Any]):

    if item.get("source") != "chrome":
        return None

    domain = item.get("domain", "")
    category = DOMAIN_CATEGORY_MAP.get(domain)

    if category:
        return category, "domain", 0.95

    return None

# -------------------------
# STAGE 3
# -------------------------
def stage3_centroid(embedding):

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
# STAGE 4
# -------------------------
def stage4_llm_fallback(item):
    return "misc", "llm", 0.90

# -------------------------
# MAIN FUNCTION
# -------------------------
def classify_system_group(item: Dict[str, Any], embedding=None):

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