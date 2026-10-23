"""Build the Capstone Phase 3 Review 2 Low Level Design & Implementation Document from the PES template.

    python build_lld.py                      # two passes: build, read real page numbers from Word, rebuild
    python build_lld.py --single-pass        # one build, TOC page column left empty
    python build_lld.py --output other.docx

The template is never modified: it is opened, filled and saved under a new name. Page numbers for the manual
table of contents come from Word itself (render_docx.ps1), so they match the printed document.
"""
from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys
import tempfile

from docx_writer import CONTENT_INDENT, SECTION_INDENT, TemplateDocument

LLD_DIR = pathlib.Path(__file__).resolve().parent
BUILD_DIR = LLD_DIR.parent
PHASE3_DIR = BUILD_DIR.parent
ROOT = PHASE3_DIR.parent.parent
TEMPLATE = PHASE3_DIR / "UE23CS441A _LOW LEVEL DESIGN & IMPLEMENTATION DOCUMENT.docx"
OUTPUT = PHASE3_DIR / "ECHO_Phase3_Review2_LLD.docx"
DIAGRAMS = LLD_DIR / "diagrams"
DECK_DIAGRAMS = BUILD_DIR / "diagrams"
GRAPH_PNG = ROOT / "rse" / "rse_langgraph.png"
RENDER_DOCX = BUILD_DIR / "render_docx.ps1"

MEMBER_COLS = ["Data Type", "Data Name", "Access Modifiers", "Initial Value", "Description"]
MEMBER_WIDTHS = [1250, 2450, 1500, 1400, 2400]
USE_CASE_WIDTHS = [2900, 6100]


# ═════════════════════════════════════════════════════════════════════════════
# Reusable blocks
# ═════════════════════════════════════════════════════════════════════════════
def class_block(d: TemplateDocument, n: int, name: str, description: list[str],
                members: list[list[str]], methods: list[dict]) -> None:
    """Class Name → Class Description → Data Members → Method 1..n, exactly the template's order."""
    d.h4(f"Class Name {n}: {name}", toc=True)
    d.h4(f"Class Description {n}")
    for text in description:
        d.para(text, CONTENT_INDENT)
    d.h4(f"Data Members {n}")
    d.table(MEMBER_COLS, members, MEMBER_WIDTHS)
    for index, m in enumerate(methods, start=1):
        d.h4(f"Method {index}: {m['name']}")
        d.bullets([
            f"**Purpose:** {m['purpose']}",
            f"**Input:** {m['input']}",
            f"**Output:** {m['output']}",
            f"**Parameters:** {m['parameters']}",
            f"**Exceptions:** {m['exceptions']}",
            "**Pseudo-code:**",
        ], indent=CONTENT_INDENT, align_left=True, keep_last_with_next=True)
        d.code(m["pseudo"])


def use_case_table(d: TemplateDocument, rows: list[list[str]]) -> None:
    d.table(["Use Case Item", "Description"], rows, USE_CASE_WIDTHS)


# ═════════════════════════════════════════════════════════════════════════════
# Cover
# ═════════════════════════════════════════════════════════════════════════════
def fill_cover(d: TemplateDocument) -> None:
    cover = d.cover_table
    d.replace_paragraph_text(cover, "<TITLE OF THE PROJECT>",
                             "E.C.H.O. – Efficient Contextualized History Orchestrator", drop_highlight=True)
    team = [("Mrigank Jain", "PES2UG23CS352"), ("Mohammed Mir Fazlai Ali", "PES2UG23CS346"),
            ("Mohammed Shehzaad Khan", "PES2UG23CS349"), ("Arjun Sai N", "PES2UG23CS091")]
    for index, (name, srn) in enumerate(team, start=1):
        d.replace_paragraph_text(cover, f"Name {index}", name)
        d.replace_paragraph_text(cover, f"<SRN {index}>", srn)
    d.replace_paragraph_text(cover, "Prof. Guide Name", "Dr. M. Shalima Sulthana")
    d.replace_paragraph_text(cover, "Designation", "Associate Professor")
    # Widen the name column so the longest names stay on one line (the total width is unchanged).
    d.set_nested_table_widths(cover, [2847, 3216], [3700, 2363])


# ═════════════════════════════════════════════════════════════════════════════
# 1. Introduction
# ═════════════════════════════════════════════════════════════════════════════
def section_introduction(d: TemplateDocument) -> None:
    d.h1("Introduction")
    d.h2("Overview")
    d.para("E.C.H.O. addresses fragmented personal digital history across Gmail, Chrome, and YouTube. Instead of "
           "forcing users to remember which application contained a previous interaction, the system creates a "
           "unified, searchable memory layer.")
    d.para("The system captures meaningful activity, stores source-aware canonical records, enriches them for "
           "semantic search, retrieves context through hybrid deterministic retrieval, and presents results "
           "through a React dashboard and Chrome extension popup.")
    d.para("Phase III focus: system integration, reliable enrichment/indexing, retrieval-quality improvements, "
           "digital wellbeing analytics, and UI completion.")
    d.para("This low-level design describes how each part of E.C.H.O. is built: its modules, classes, data "
           "members, methods, algorithms, and how they are packaged and deployed.")

    d.h2("Purpose")
    d.para("The purpose of this document is to record the low-level design and the implementation status of "
           "E.C.H.O. at Capstone Phase 3 Review 2. It explains, module by module, which classes exist, what data "
           "they hold, what each important method does, and the algorithms behind retrieval and wellbeing. It is "
           "the reference for the remaining implementation work and for tracing the design back to the "
           "requirements (Project Scope Document) and the high-level design (Architecture Design Document).")

    d.h2("Scope")
    d.para("The E.C.H.O. system encompasses:")
    d.bullets([
        "Gmail, Chrome, and YouTube activity capture",
        "Canonical memory storage with source-specific metadata",
        "Intent-aware capture and revisit detection",
        "Background enrichment, classification, and FAISS semantic indexing",
        "Natural-language hybrid retrieval with conversational context",
        "Digital wellbeing analytics and user-controlled reflections",
        "React dashboard, browser extension popup, settings, export",
    ])
    d.para("All functionality is designed with privacy-first principles and local-first architecture.")
    d.para("This document covers the system up to the Digital Wellbeing module, in three functional modules: "
           "Memory Capture and Enrichment (Section 3.2), the Retrieval and Synthesis Engine (Section 3.3), and "
           "Digital Wellbeing and Analytics (Section 3.4). The user-interface layer appears in the packaging and "
           "deployment view (Section 3.5) and in the plan for the remaining work (Section 4.3).")


# ═════════════════════════════════════════════════════════════════════════════
# 2. Constraints, assumptions, dependencies
# ═════════════════════════════════════════════════════════════════════════════
def section_constraints(d: TemplateDocument) -> None:
    d.h1("Design Constraints, Assumptions, and Dependencies")
    d.para("The design follows the constraints, assumptions, and dependencies below. The constraints are "
           "deliberate design decisions; each is enforced in code, not only in documentation.")
    d.subhead("Design constraints")
    d.bullets([
        "Local-first: no permanent cloud copy of personal data.",
        "No capture of typed text, incognito tabs, or application pages (Slack, Notion) — application pages are "
        "recorded for time spent only.",
        "Exactly 2 LLM calls per query: one to parse the question, one to write the answer.",
        "The LLM sees only the question, prior turns, short snippets, and aggregated numbers — plus an "
        "attachment's extracted text when the user asks about that attachment.",
        "The 16-table database schema is locked.",
        "Wellbeing language stays descriptive — no scores, no \"wasted time\".",
        "No item joins a user group without the user's approval.",
    ])
    d.subhead("Assumptions")
    d.bullets([
        "A single user on desktop Chrome.",
        "Gmail, Chrome, and YouTube cover most relevant activity.",
        "Engagement (time, scroll, revisits) approximates effort.",
        "Google APIs stay available and stable.",
    ])
    d.subhead("Dependencies")
    d.table(["Dependency", "Used for", "Impact if it fails"], [
        ["Gmail API, YouTube Data API v3", "Email capture; video title, channel, and duration",
         "Capture stops if an API or its OAuth token fails"],
        ["Chrome Manifest V3 extension APIs", "Chrome page and YouTube watch capture",
         "Chrome and YouTube capture rely on them"],
        ["PostgreSQL, Redis, FAISS", "Storage layers",
         "PostgreSQL is the source of truth; FAISS can be rebuilt from PostgreSQL"],
        ["Local models (MiniLM, cross-encoder)", "Embeddings and re-ranking",
         "One-time download on first run"],
        ["OpenAI gpt-4.1-mini (swappable)", "Intent parsing and answer writing",
         "Paid API with a small cost per query; switching provider is one configuration change"],
    ], [2700, 3000, 3300])


# ═════════════════════════════════════════════════════════════════════════════
# 3. Design description
# ═════════════════════════════════════════════════════════════════════════════
def section_design_intro(d: TemplateDocument) -> None:
    d.h1("Design Description")
    d.para("**Design approach.** E.C.H.O. is a layered, modular monolith — private, intent-aware memory with "
           "deterministic retrieval-augmented generation: fixed Python search steps, with the LLM only writing "
           "the final answer. This keeps data on the device, cuts noise, unifies search across sources, makes "
           "results explainable, and let four people build modules in parallel.")
    d.para("The design is described in three functional modules:")
    d.table(["Module", "Packages", "Responsibility"], [
        ["Module 1 — Memory Capture and Enrichment", "`extension/`, `ingestion/`, `ste/`, `enp/`",
         "Capture engaged activity, store it atomically, enrich and index it in the background"],
        ["Module 2 — Retrieval and Synthesis Engine (RSE)", "`rse/`, `backend/retrieval.py`",
         "Answer natural-language questions with hybrid search and exactly 2 LLM calls"],
        ["Module 3 — Digital Wellbeing and Analytics (WBA)", "`wba/`, `backend/wellbeing.py`",
         "Describe time use, sessions, regret, and user groups — with the user in control"],
    ], [3000, 2600, 3400])
    d.para("**Implementation note.** E.C.H.O. is written in Python. Where a component is a module of functions "
           "rather than a Python class (for example `ingestion/chrome/chrome_connector.py`), it is documented as a "
           "class: its module-level constants and request fields are its data members, and its functions are "
           "its methods. Access modifiers follow the Python convention — a leading underscore marks a private "
           "member; everything else is public.")

    d.h2("Master Class Diagram")
    d.para("The master class diagram gives the whole system at a high level. It is broken down into one class "
           "diagram per module in Sections 3.2.3, 3.3.3, and 3.4.3, which show the classes as implemented.",
           keep_next=True)
    d.figure(DIAGRAMS / "master_class_phase2.png", "Master class diagram (Phase II design, high level)", 6.4)
    d.para("How the high-level classes map to the implementation:", keep_next=True)
    d.table(["Master diagram class", "Implemented as"], [
        ["Memory, GmailMemory, ChromeMemory, YouTubeMemory",
         "`memory_items` plus one source table each (`gmail_metadata`, `chrome_metadata`, `youtube_metadata`), "
         "written by StorageEngine"],
        ["AttentionSession", "`memory_engagement` — dwell time, watch time, play sessions"],
        ["IntentFilter", "`ingestion/chrome/intent_filter.py` and `YouTubeConnector.passes_intent_gate()`"],
        ["Preprocessor", "`enp/text_cleaner.py` and `enp/topic_extractor.py`"],
        ["EmbeddingGenerator, Embedding", "`enp/embedding_generator.py`; vectors held by FAISSManager and the "
         "`embedding_index` table"],
        ["QueryParser, LLMService", "`rse/query_parser.py` and `rse/llm_synthesizer.py` — the 2 LLM calls"],
        ["RetrievalEngine", "`rse/retrieval_engine.py` — a 14-node LangGraph"],
        ["EffortRanker", "`rse/reranker.py` — cross-encoder plus effort score"],
        ["Session, UserGroup", "`wba/wellbeing_analytics.py` and `wba/group_manager.py`"],
        ["RedisObservation", "Redis revisit keys in `ste/redis_manager.py` (24-hour expiry)"],
        ["User", "Single-user installation — there is no users table"],
    ], [3200, 5800])


# ── Module 1 ─────────────────────────────────────────────────────────────────
def module_memory(d: TemplateDocument) -> None:
    d.h2("Module 1: Memory Capture and Enrichment")
    d.h3("Description")
    ind = CONTENT_INDENT
    d.para("Module 1 turns everyday activity into stored, searchable memory. It has three parts that run inside "
           "the same backend process:", ind)
    d.bullets([
        "**Capture** (`extension/`, `ingestion/`). The Chrome extension watches web pages and YouTube videos in "
        "desktop Chrome and sends an event only after its intent gate passes; the backend repeats the same gate "
        "before saving. Gmail is polled every 30 seconds through the Gmail API with a read-only scope. Every "
        "new email — received or sent — is stored, and reading time is added when the extension sees the user "
        "open it.",
        "**Storage** (`ste/`). The Storage Engine is the single write path. Each item is written to "
        "`memory_items`, its source table, and `memory_engagement` in one transaction, de-duplicated by "
        "UNIQUE(source_type, source_id). Redis holds short-lived keys — revisits (24 hours), attachment text, "
        "a copy of each newly stored email, and the encrypted Gmail token (1 hour each) — and the capture settings.",
        "**Enrichment** (`enp/`). A background thread picks up items that are not yet processed, cleans their "
        "text, builds a 384-dimension MiniLM embedding, assigns one of five system groups (work, study, "
        "entertainment, personal, misc), and appends the vector to the FAISS index. Capture never waits for "
        "enrichment.",
    ], indent=ind)
    d.para("Privacy rules are enforced at capture time: incognito tabs and typed text are never captured; login, "
           "payment, and account pages are discarded; application pages such as Slack and Notion are stored for "
           "time spent only, with no content.", ind)

    d.h3("Use Case Diagram")
    d.para("The use cases are shown at two levels. Level 0 covers the whole system; each module then has its "
           "own level-1 diagram (this section, 3.3.2, and 3.4.2). Dashed arrows are «include» relations.",
           ind, keep_next=True)
    d.figure(DIAGRAMS / "use_case_system_phase2.png", "Level 0 — system use case diagram (Phase II design)", 6.4)
    d.figure(DIAGRAMS / "uc_m1_memory.png", "Level 1 — Memory Capture and Enrichment", 5.6)
    use_case_table(d, [
        ["Capture an engaged web page", "The extension sends a page after 5 s of exposure, then any one of: 10 s "
         "in the foreground, 25% scroll, one interaction, or a revisit; the backend repeats the gate and stores it."],
        ["Capture a watched YouTube video", "Stored after 20 s of playback (15 s for Shorts), 50% completion, a "
         "revisit within 24 hours, or a manual interaction; title, channel, and duration are fetched afterwards."],
        ["Capture a received email", "A background poll every 30 s stores each new email — read or unread, received or sent — with "
         "sender, labels, thread, and attachment details."],
        ["Record email reading time", "When the user opens an email in Gmail, the extension reports the dwell "
         "time, which is added to that email's engagement."],
        ["Apply source intent gate", "Keeps only content the user paid attention to; the same thresholds run in "
         "the extension and the backend."],
        ["Store memory item atomically", "One transaction per item; a repeated item is recognised and not stored "
         "twice."],
        ["Enrich item", "Clean, embed, classify, and index in the background; failures are recorded and can be "
         "retried."],
        ["Manage capture settings", "Turn each source on or off, and exclude domains or senders."],
        ["Delete or export stored data", "Soft delete (`DELETE /items/{id}`) and export as JSON or CSV "
         "(`GET /export`)."],
    ])

    d.h3("Class Diagram")
    d.para("The classes of Module 1 and how they depend on each other. Supporting modules not detailed below — "
           "`settings_store.py`, `redis_manager.py`, `postgresql_manager.py`, `text_cleaner.py`, "
           "`topic_extractor.py`, and `embedding_generator.py` — are small helpers called by these classes.",
           ind, keep_next=True)
    d.figure(DIAGRAMS / "cls_m1_memory.png", "Class diagram — Memory Capture and Enrichment", 5.5)

    class_block(d, 1, "ChromeConnector", [
        "File: `ingestion/chrome/chrome_connector.py` with `intent_filter.py`. A FastAPI router mounted at "
        "`/chrome`. It receives page events that the Chrome extension sends once its own intent gate passes, "
        "re-checks them on the server, and stores admitted pages through the Storage Engine. Its request body is "
        "the Pydantic model `ChromeIngestRequest`.",
    ], [
        ["str", "url", "public", "—", "Page URL as visited"],
        ["str", "canonical_url", "public", "None", "URL without tracking parameters and fragment"],
        ["str", "title", "public", "—", "Page title"],
        ["str", "domain", "public", "—", "Lower-case host name"],
        ["int", "dwell_seconds", "public", "— (≥ 0)", "Foreground seconds when the intent gate passed (sent once per visit)"],
        ["float", "scroll_depth", "public", "— (0 to 1)", "Deepest scroll position"],
        ["int", "interaction_count", "public", "— (≥ 0)", "Clicks and text selections"],
        ["int", "revisit_count", "public", "0", "Earlier visits in the last 24 hours"],
        ["str", "content_extract", "public", "None", "Main page text extracted by the extension"],
        ["bool", "is_app_page", "public", "False", "True for application pages — time only, no content"],
        ["set", "IGNORED_QUERY_PARAMS", "public, constant", "11 keys", "Tracking parameters removed "
         "(utm_*, fbclid, gclid, …)"],
        ["tuple", "SENSITIVE_PATH_TERMS", "public, constant", "14 terms", "Pages with login, payment, account, "
         "… in the path are never stored"],
        ["int", "PHASE1_MIN_SECONDS", "public, constant", "5", "Intent gate phase 1: minimum exposure"],
        ["int / float", "PHASE2_MIN_FOREGROUND, PHASE2_MIN_SCROLL, PHASE2_MIN_INTERACTIONS", "public, constant",
         "10 s, 0.25, 1", "Intent gate phase 2: any one is enough"],
    ], [
        dict(name="ingest_chrome_page()",
             purpose="Admit or discard one page event and store admitted pages (`POST /chrome/ingest`).",
             input="JSON body from the extension, validated as ChromeIngestRequest.",
             output="`{status: \"saved\", memory_id, mode}` or `{status: \"discarded\", reason}`.",
             parameters="`payload: ChromeIngestRequest`",
             exceptions="Invalid fields are rejected with HTTP 422 by validation. A database error from the "
                        "Storage Engine returns HTTP 500. A Redis error while recording the visit is logged and "
                        "ignored.",
             pseudo=[
                 "canonical_url = payload.canonical_url or canonicalize_url(payload.url)",
                 "if chrome capture is off or domain is excluded: return discarded",
                 "if application path or sensitive page:        return discarded",
                 "if dwell_seconds >= 5: record visit in Redis (expires in 24 h)",
                 "if is_app_page: drop the content (time only)",
                 "elif not intent_filter.evaluate(dwell, scroll, interactions, revisits):",
                 "    return discarded",
                 "saved = StorageEngine.store_chrome_page(payload)",
                 "return saved(memory_id, mode)",
             ]),
        dict(name="intent_filter.evaluate()",
             purpose="Two-phase intent gate. The thresholds match the extension exactly, so a page the extension "
                     "sends is never silently dropped by a stricter server.",
             input="Engagement signals of one page.",
             output="`True` to keep the page, `False` to discard it.",
             parameters="`dwell_seconds: int`, `scroll_depth: float`, `interaction_count: int`, "
                        "`revisit_count: int`",
             exceptions="None — a pure function.",
             pseudo=[
                 "if dwell_seconds < 5: return False            # phase 1",
                 "return dwell_seconds >= 10 or scroll_depth >= 0.25",
                 "       or interaction_count >= 1 or revisit_count > 0   # phase 2",
             ]),
    ])

    class_block(d, 2, "YouTubeConnector", [
        "File: `ingestion/youtube/youtube_connector.py`. A FastAPI router mounted at `/ytc` with three routes: "
        "`/video-detected`, `/heartbeat`, and `/video-closed`. A video is stored as soon as it passes the intent "
        "gate; its title, channel, and duration are fetched afterwards from the YouTube Data API without "
        "blocking the response. Watch time is updated by the heartbeat and closing events.",
    ], [
        ["str", "url", "public", "—", "Watch or Shorts URL"],
        ["str", "video_id", "public", "—", "11-character YouTube id, validated"],
        ["bool", "is_short", "public", "—", "Reported by the extension; re-derived from the URL"],
        ["int", "watch_time_seconds", "public", "—", "Foreground playback time so far"],
        ["str", "triggered_by", "public", "—", "What fired the event, e.g. manual_interaction"],
        ["int", "duration_seconds", "public", "None", "Video length, when the page reports it"],
        ["datetime", "timestamp", "public", "—", "Time the video was detected"],
        ["int", "SHORT_MIN_WATCH_SECONDS", "public, constant", "15", "Watch-time bar for Shorts"],
        ["int", "REGULAR_MIN_WATCH_SECONDS", "public, constant", "20", "Watch-time bar for regular videos"],
        ["float", "COMPLETION_RATE_THRESHOLD", "public, constant", "0.5", "Alternative bar: half the video"],
        ["ThreadPoolExecutor", "executor", "public", "4 workers", "Runs blocking database writes off the event "
         "loop"],
    ], [
        dict(name="handle_video_detected()",
             purpose="Gate and store a newly watched video, then fill in its metadata in the background "
                     "(`POST /ytc/video-detected`).",
             input="JSON body validated as VideoDetectedEvent.",
             output="`{status: \"saved\", memory_id, video_id, is_short, triggered_by}` or "
                    "`{status: \"discarded\", reason}`.",
             parameters="`event: VideoDetectedEvent`",
             exceptions="A failed database write returns HTTP 500 (\"Failed to save video memory\"). A Redis "
                        "error counts as \"not a revisit\". A metadata-fetch error is logged by the background task.",
             pseudo=[
                 "if youtube capture is off:      return discarded",
                 "if video_id is not valid:       return discarded",
                 "is_revisit = Redis.check_and_record_revisit(\"youtube\", video_id)",
                 "if not passes_intent_gate(event, is_revisit): return discarded",
                 "is_short  = classify_video_type(event.url) == \"short\"",
                 "memory_id = StorageEngine.store_youtube_detection(video_id, is_short,",
                 "                                                  timestamp)",
                 "in background: meta = fetch_video_metadata(video_id)",
                 "               StorageEngine.update_youtube_metadata(memory_id, meta)",
                 "return saved",
             ]),
        dict(name="passes_intent_gate()",
             purpose="Decide whether the user actually watched the video.",
             input="The detection event and whether the video was seen earlier today.",
             output="`True` to store the video.",
             parameters="`event: VideoDetectedEvent`, `is_revisit: bool`",
             exceptions="None — a pure function.",
             pseudo=[
                 "min_watch = 15 if event.is_short else 20",
                 "if event.watch_time_seconds >= min_watch:              return True",
                 "if duration known and watch_time / duration >= 0.5:  return True",
                 "if is_revisit:                                         return True",
                 "if manual interaction with an interaction type:       return True",
                 "return False",
             ]),
    ])

    class_block(d, 3, "GmailConnector", [
        "Files: `ingestion/gmail/gmail_api.py` and `router.py`. A background task started with the backend polls "
        "Gmail every 30 seconds with the read-only scope. There is no intent gate on receipt — every new email, received "
        "or sent, is stored — but reading time is recorded only when the user opens the email (`POST /gmail/engagement`). "
        "The OAuth token is stored encrypted (`token_gmail.enc`). Mail in the Spam folder is not listed, so it "
        "is never captured.",
    ], [
        ["list[str]", "SCOPES", "public, constant", "gmail.readonly", "OAuth scope — read-only access"],
        ["int", "CHECK_INTERVAL", "public, constant", "30", "Seconds between polls"],
        ["Path", "TOKEN_PATH", "public, constant", "token_gmail.enc", "Encrypted OAuth token file"],
        ["Resource", "_service", "private", "None", "Cached Gmail API client, built on first poll"],
        ["str", "email_id", "public", "—", "Engagement event: Gmail message id"],
        ["int", "dwell_seconds", "public", "— (≥ 0)", "Engagement event: time the email was open"],
    ], [
        dict(name="poll_once()",
             purpose="Run one polling cycle. Also called by the retrieval engine for \"today / just received\" "
                     "questions, so new mail is included immediately.",
             input="None.",
             output="`{status: \"ok\", processed: n}` or `{status: \"skipped\", reason: "
                    "\"gmail_capture_disabled\", processed: 0}`.",
             parameters="None.",
             exceptions="An authentication error propagates to the polling loop, which logs it and tries again "
                        "after 30 seconds.",
             pseudo=[
                 "if gmail capture is off: return skipped",
                 "initialize_database()",
                 "service = cached authenticate_gmail()",
                 "processed = fetch_and_store_new_emails(service)",
                 "return ok(processed)",
             ]),
        dict(name="fetch_and_store_new_emails()",
             purpose="List messages, skip those already stored, and store new ones oldest first.",
             input="An authorised Gmail API client.",
             output="Number of emails stored in this cycle.",
             parameters="`service: googleapiclient Resource`",
             exceptions="Gmail API calls are retried with exponential back-off (4 attempts). Any other error is "
                        "logged and the cycle returns 0; the next cycle starts clean.",
             pseudo=[
                 "ids = page through messages.list(maxResults=100)",
                 "for id not yet in gmail_metadata:",
                 "    msg = messages.get(id, format=full); read Subject, From, To, Date",
                 "sort new messages by Date, oldest first",
                 "for each message:",
                 "    if sender is excluded in settings: skip",
                 "    record = body, attachment details, labels, thread_id, is_sent",
                 "    if StorageEngine.store_gmail_message(record) inserted: count += 1",
                 "return count",
             ]),
    ])

    class_block(d, 4, "StorageEngine", [
        "File: `ste/storage_engine.py`. The single write path for all three connectors. `store_memory_item()` "
        "writes `memory_items`, the source table, and `memory_engagement` in one database transaction, so a "
        "crash can never leave half an item (Gmail attachment rows follow in a second transaction). Duplicates are prevented by UNIQUE(source_type, source_id) with "
        "ON CONFLICT DO NOTHING, which makes every connector safe to restart. New items start with "
        "`preprocessed = FALSE` and a provisional system group (Gmail → personal, Chrome → misc, "
        "YouTube → entertainment) that the Enrichment Pipeline later replaces.",
    ], [
        ["dict[str, int]", "SYSTEM_GROUP_IDS", "public, constant", "work 1, study 2, entertainment 3, "
         "personal 4, misc 5", "Ids of the five system groups; `system_group_id` is never NULL"],
        ["Engine", "postgresql_manager._engine", "private", "None → pool on first use", "SQLAlchemy pool: 5 "
         "connections, 10 overflow, recycled every 240 s"],
    ], [
        dict(name="store_memory_item()",
             purpose="Store one captured item and its metadata atomically.",
             input="Canonical fields of the item plus source metadata and engagement.",
             output="`(memory_id, created_new)` — `created_new` is False when the item already existed.",
             parameters="`source_type: str`, `source_id: str`, `system_group_id: int`, `title: str`, "
                        "`raw_text: str | None`, `created_at: datetime`, `source_metadata: dict`, "
                        "`engagement: dict | None`",
             exceptions="`ValueError` for an unsupported source type or an existing row that cannot be found. "
                        "Any database error rolls back the whole transaction.",
             pseudo=[
                 "begin transaction",
                 "  INSERT INTO memory_items (..., preprocessed = FALSE,",
                 "                            classified_by = 'pending')",
                 "  ON CONFLICT (source_type, source_id) DO NOTHING RETURNING memory_id",
                 "  if a row was inserted: created_new = True",
                 "  else: memory_id = existing id; update title / text if new values given",
                 "  upsert the source table (gmail / chrome / youtube metadata)",
                 "  upsert memory_engagement: dwell adds up, watch time keeps the maximum",
                 "commit; return (memory_id, created_new)",
             ]),
        dict(name="store_chrome_page()",
             purpose="Map an admitted Chrome page onto `store_memory_item()`.",
             input="The validated ChromeIngestRequest.",
             output="`{memory_id, inserted}`.",
             parameters="`payload: ChromeIngestRequest`",
             exceptions="Propagates database errors to the caller (HTTP 500).",
             pseudo=[
                 "return store_memory_item(",
                 "    source_type = \"chrome\", source_id = canonical_url, group = misc,",
                 "    title = title or canonical_url,",
                 "    raw_text = None if is_app_page else content_extract,",
                 "    metadata = url, domain, referrer, scroll, interactions, revisits,",
                 "    engagement = dwell_time_seconds, play_sessions_count = 1)",
             ]),
    ])

    class_block(d, 5, "FAISSManager", [
        "File: `ste/faiss_manager.py` — a Python class. It holds the semantic index: a FAISS IndexFlatL2 over "
        "unnormalised 384-dimension MiniLM vectors, mirrored by a numpy array that is the source of truth. Where "
        "a similarity threshold matters (quality gate, KNN groups, centroids), callers compute true cosine "
        "similarity from these vectors instead of reading L2 distance. The index is saved as "
        "`enp/echo_faiss.index` with a metadata file, and each vector has a row in `embedding_index` so the "
        "index can be rebuilt from PostgreSQL.",
    ], [
        ["int", "dimension", "public", "384", "Vector length (all-MiniLM-L6-v2)"],
        ["str", "embedding_version", "public", "all-MiniLM-L6-v2-v1", "Model version recorded with the index"],
        ["Path", "index_path", "public", "enp/echo_faiss.index", "Index file (overridable by environment)"],
        ["list[str]", "memory_ids", "public", "[]", "Memory id stored at each vector offset"],
        ["dict[str, int]", "memory_id_to_offset", "public", "{}", "Reverse lookup: memory id → offset"],
        ["IndexFlatL2", "_index", "private", "empty index", "FAISS index derived from the buffer"],
        ["bool", "_index_stale", "private", "False", "True after an in-place update; rebuilt on next read"],
        ["ndarray", "_buffer", "private", "0 × 384", "Vector cache; grows by doubling"],
        ["int", "_count", "private", "0", "Number of vectors in use"],
    ], [
        dict(name="add()",
             purpose="Add a new item's vector, or replace the vector of an item that is re-embedded.",
             input="Memory id, its vector, and the text that was embedded.",
             output="`True` for a new item, `False` when an existing vector was replaced.",
             parameters="`memory_id: str`, `vector: ndarray`, `embeddable_text: str | None`",
             exceptions="`ValueError` if the vector is not 384-dimensional.",
             pseudo=[
                 "if memory_id already indexed:",
                 "    overwrite its row in _buffer; _index_stale = True",
                 "    upsert embedding_index row; return False",
                 "grow _buffer if full (doubling); append vector",
                 "memory_ids.append(memory_id); memory_id_to_offset[memory_id] = offset",
                 "if not _index_stale: _index.add(vector)",
                 "upsert embedding_index row; return True",
             ]),
        dict(name="search()",
             purpose="Nearest neighbours of a query vector among a given set of candidate items.",
             input="Query vector, candidate memory ids, and k.",
             output="List of `(memory_id, L2 distance)`, nearest first.",
             parameters="`query_vector: ndarray`, `candidate_ids: Iterable[str]`, `k: int = 20`",
             exceptions="`ValueError` for a wrong vector size; unknown ids are skipped.",
             pseudo=[
                 "ids = candidates that are indexed; if none: return []",
                 "temp = IndexFlatL2 built from the candidates' vectors",
                 "distances, offsets = temp.search(query, min(k, len(ids)))",
                 "return [(ids[offset], distance) ...]",
             ]),
    ])

    class_block(d, 6, "EnrichmentPipeline", [
        "File: `enp/enrichment_pipeline.py`. The background worker. The backend starts it at boot as a thread "
        "inside the same process; it can also run on its own. Every 10 seconds it takes up to 10 unprocessed "
        "items, prepares them (source context, cleaned text, embeddable text, keywords), embeds them in one "
        "batch, classifies them, indexes them, and marks them processed. A failed item is marked `failed` with "
        "the reason and is retried on demand.",
    ], [
        ["int", "DEFAULT_BATCH_SIZE", "public, constant", "10", "Items per batch"],
        ["int", "DEFAULT_POLL_INTERVAL", "public, constant", "10", "Seconds between batches"],
        ["str", "FAILED_CLASSIFIED_BY", "public, constant", "\"failed\"", "Marks items that failed enrichment"],
        ["str", "RETRY_FAILED_ENV", "public, constant", "\"ENP_RETRY_FAILED\"", "Set to 1 to retry failed items"],
        ["int", "HYDRATION_GRACE_SECONDS", "public, constant", "600", "Wait for YouTube metadata before "
         "indexing an empty video"],
    ], [
        dict(name="run_pipeline()",
             purpose="Worker entry point: load the index and centroids once, then process batches forever.",
             input="Batch size and poll interval.",
             output="None — runs until the backend stops.",
             parameters="`batch_size: int = 10`, `poll_interval: int = 10`",
             exceptions="An error inside a batch is logged and the loop continues. A FAISS dimension mismatch "
                        "at start-up (`ValueError`) stops the worker.",
             pseudo=[
                 "manager = FAISSManager(); manager.load_index()",
                 "initialize_centroids(generate_embedding)",
                 "loop forever:",
                 "    try: process_batch(manager, batch_size)",
                 "    except error: log it",
                 "    sleep(poll_interval)",
             ]),
        dict(name="process_batch()",
             purpose="Enrich one batch of items end to end.",
             input="The shared FAISSManager and the batch size.",
             output="`(processed, failed)` counts.",
             parameters="`manager: FAISSManager`, `batch_size: int`",
             exceptions="Per-item errors mark that item `failed` and never stop the batch.",
             pseudo=[
                 "items = oldest rows with preprocessed = FALSE, not deleted, not failed",
                 "for item in items:",
                 "    if YouTube metadata has not arrived yet (< 10 min): defer",
                 "    prepare: load context, clean text, build embeddable text + keywords",
                 "vectors = generate_embeddings(all texts)        # one batch",
                 "for item, vector:",
                 "    category, method, confidence = classify_system_group(item, vector)",
                 "    manager.add(memory_id, vector, embeddable_text)",
                 "    mark_item_processed(...)                   # preprocessed = TRUE",
                 "    on error: mark_item_failed(memory_id, reason)",
                 "if any processed: manager.save_index()",
             ]),
    ])

    class_block(d, 7, "SystemGroupClassifier", [
        "File: `enp/system_group_classifier.py`. Assigns every item to one of five system groups with a "
        "four-stage cascade that stops at the first confident answer: (1) structural signals such as Gmail "
        "labels, .edu senders, and YouTube categories; (2) a lookup table of 200+ known domains; (3) the "
        "nearest category centroid by cosine similarity; (4) an LLM fallback that answers with one word. If the "
        "LLM is unavailable the item becomes `misc`, so enrichment never blocks. Centroids start from seed "
            "texts and are improved by `recompute_centroids()` from confirmed items; the ENP worker runs it at "
        "startup when `centroids.npz` is missing or older than 30 days.",
    ], [
        ["dict[str, str]", "DOMAIN_CATEGORY_MAP", "public, constant", "200+ entries", "Stage 2 domain → category"],
        ["dict[str, ndarray]", "CENTROIDS", "public", "{}", "Category → centroid vector (stage 3)"],
        ["set[str]", "_VALID_CATEGORIES", "private, constant", "5 categories", "Allowed LLM answers"],
        ["ChatModel", "_llm_client", "private", "None", "Stage-4 LLM, built lazily from the shared LLM_CONFIG"],
        ["int", "_MIN_CONFIRMED_FOR_RECOMPUTE", "private, constant", "10", "Items needed before a centroid is "
         "recomputed"],
    ], [
        dict(name="classify_system_group()",
             purpose="Return the system group of one item.",
             input="The prepared item and its embedding.",
             output="`(category, method, confidence)`, e.g. `(\"study\", \"centroid\", 0.71)`.",
             parameters="`item: dict`, `embedding: ndarray | None`",
             exceptions="None raised — stage 4 catches LLM errors and returns `(\"misc\", \"fallback\", 0.40)`.",
             pseudo=[
                 "if YouTube Short: return (\"entertainment\", \"structural\", 1.0)",
                 "for stage in [structural, domain lookup, centroid(embedding)]:",
                 "    result = stage(item)",
                 "    if result: return result",
                 "return stage4_llm_fallback(item)",
             ]),
        dict(name="stage3_centroid()",
             purpose="Pick the closest category centroid when it is clearly closer than the runner-up.",
             input="The item's embedding.",
             output="`(category, \"centroid\", similarity)` or None.",
             parameters="`embedding: ndarray`",
             exceptions="None.",
             pseudo=[
                 "if fewer than 2 centroids: return None",
                 "scores = cosine(embedding, centroid) for every category",
                 "best, second = two highest scores",
                 "if best > 0.55 and best - second > 0.08:",
                 "    return (best category, \"centroid\", best)",
                 "return None",
             ]),
    ])

    d.h3("Sequence Diagram")
    d.para("Capture and enrichment are two independent flows. Capture ends when the item is committed; the "
           "enrichment worker picks it up later, so the user's browsing is never slowed down.", ind, keep_next=True)
    d.figure(DECK_DIAGRAMS / "seq_capture.png", "Sequence diagram — capture and enrichment", 6.4)


# ── Module 2 ─────────────────────────────────────────────────────────────────
def module_retrieval(d: TemplateDocument) -> None:
    d.h2("Module 2: Retrieval and Synthesis Engine (RSE)")
    ind = CONTENT_INDENT
    d.h3("Description")
    d.para("Module 2 answers natural-language questions about the stored memory (`POST /retrieval/query`). It is "
           "a LangGraph state graph with 14 nodes (`rse/retrieval_engine.py`). The LLM is called exactly twice "
           "per question — once to understand it and once to write the answer; every step in between is "
           "deterministic Python:", ind)
    d.bullets([
        "**parse_intent** (LLM call 1) turns the question and the previous turns into a structured intent: "
        "sources, time filter, cleaned query, and query variants.",
        "**resolve_time_anchor** turns phrases such as \"after that email\" into a timestamp.",
        "**Keyword search** (PostgreSQL full-text) and **semantic search** (FAISS, cosine similarity) run in "
        "parallel.",
        "**merge_and_rrf** fuses both ranked lists with Reciprocal Rank Fusion (k = 60).",
        "**evaluate_quality** runs five checks; a weak or empty pool triggers **widen_scope**, up to three times, "
        "before **no_results_found**.",
        "**rerank_cross_encoder** re-scores the pool: 0.8 × cross-encoder + 0.2 × effort.",
        "**extend_neighborhood** adds same-thread or same-session context; attachment text or today's new mail "
        "is fetched only when the question asks for it.",
        "**synthesize** (LLM call 2) writes the answer from short snippets.",
    ], indent=ind)
    d.para("Conversation turns are stored in PostgreSQL (`message_store`), so a follow-up question can refer to "
           "the previous one. Recency boosting is deliberately not used, so older but still relevant memories are "
           "not pushed down.", ind)

    d.h3("Use Case Diagram")
    d.para("Level-1 use cases of the retrieval engine (the level-0 diagram is in Section 3.2.2).", ind,
           keep_next=True)
    d.figure(DIAGRAMS / "uc_m2_retrieval.png", "Level 1 — Retrieval and Synthesis Engine", 5.2)
    use_case_table(d, [
        ["Ask a natural-language question", "For example \"the Kafka article I read last week\"; answered from "
         "Gmail, Chrome, and YouTube memory together."],
        ["Ask a follow-up question", "Uses the earlier turns of the same session."],
        ["Parse intent — LLM call 1", "Question and history → sources, time filter, cleaned query, variants."],
        ["Hybrid search", "Keyword and semantic search run in parallel."],
        ["Fuse, check quality, widen scope", "RRF fusion, five quality checks, up to three widenings."],
        ["Re-rank", "Cross-encoder relevance plus the user's effort on the item."],
        ["Fetch attachment text", "Only when the question asks about an attachment and a top-3 result has one; "
         "the text is cached for 1 hour."],
        ["Fetch today's new mail", "For \"today\" or \"just received\" questions; runs a live Gmail poll first."],
        ["Synthesize answer — LLM call 2", "Answer written from short snippets; a plain list of the top results "
         "is returned if the LLM fails."],
        ["Clear conversation session", "`DELETE /retrieval/session/{id}` removes the session's history (requires the "
         "API key header)."],
    ])

    d.h3("Class Diagram")
    d.para("The retrieval engine and the node groups it wires together. `EchoState` is the single data contract "
           "passed between nodes.", ind, keep_next=True)
    d.figure(DIAGRAMS / "cls_m2_retrieval.png", "Class diagram — Retrieval and Synthesis Engine", 6.4)

    class_block(d, 1, "EchoState", [
        "File: `rse/state.py` — a TypedDict. It carries all data between the graph nodes. Every node receives "
        "the full state and returns only the keys it changes; LangGraph merges them. It has no methods. Initial "
        "values are set by `RetrievalEngine.run_query()`.",
    ], [
        ["str", "user_query", "public", "the question", "Question as typed by the user"],
        ["list[BaseMessage]", "conversation_history", "public", "loaded", "Earlier turns of this session"],
        ["ParsedIntent", "parsed_intent", "public", "{}", "Output of LLM call 1"],
        ["str | None", "anchor_time", "public", "None", "Resolved time anchor"],
        ["dict | None", "anchor_item", "public", "None", "Item the anchor refers to"],
        ["list[dict]", "postgres_results", "public", "[]", "Keyword branch, rank ordered"],
        ["list[tuple]", "faiss_results", "public", "[]", "Semantic branch: (memory_id, cosine)"],
        ["list[dict]", "merged_candidates", "public", "[]", "RRF-fused pool (at most 50)"],
        ["list[dict]", "ranked_results", "public", "[]", "Pool after cross-encoder re-ranking"],
        ["dict", "neighbor_items", "public", "{}", "Context items per top result"],
        ["str | None", "attachment_content", "public", "None", "Extracted attachment text, when asked"],
        ["list[dict]", "api_results", "public", "[]", "Today's fresh items, when asked"],
        ["str", "result_quality", "public", "\"empty\"", "strong / weak / empty"],
        ["int", "attempt_count", "public", "0", "Widen-scope attempts so far (max 3)"],
        ["str", "final_answer", "public", "\"\"", "Answer returned to the user"],
        ["bool", "no_results", "public", "False", "True when nothing relevant was found"],
    ], [])

    class_block(d, 2, "ParsedIntent", [
        "File: `rse/state.py` — a TypedDict produced by LLM call 1 and repaired by `_normalise_intent()` so later "
        "nodes never see malformed values. If parsing fails, a safe fallback intent searches all sources with "
        "the question as typed. It has no methods; `widen_scope` edits it between search attempts.",
    ], [
        ["list[str]", "sources", "public", "all three", "gmail, chrome, youtube"],
        ["str | None", "time_filter", "public", "None", "Earliest date to include (ISO)"],
        ["bool", "fetch_attachment", "public", "False", "Question asks about an attachment"],
        ["bool", "fetch_api", "public", "False", "Question asks about today's new items"],
        ["str", "query_clean", "public", "the question", "Question without filler words"],
        ["list[str]", "query_variants", "public", "[question]", "Rephrasings for semantic search"],
        ["str | None", "time_anchor_query", "public", "None", "Phrase naming the anchor item"],
        ["str | None", "time_relation", "public", "None", "before / after the anchor"],
        ["int", "scope_level", "public", "0", "Reserved; currently always 0"],
        ["bool", "is_ambiguous", "public", "True (fallback)", "Parser was unsure"],
        ["str", "original_query", "public", "the question", "Unmodified question"],
        ["bool", "skip_postgres_filter", "public", "False", "Set by widen attempt 3"],
        ["bool", "full_faiss_scan", "public", "False", "Set by widen attempt 3 (k × 3)"],
    ], [])

    class_block(d, 3, "RetrievalEngine", [
        "File: `rse/retrieval_engine.py`. Builds and compiles the LangGraph once when the module is imported, "
        "and runs it for each question. It loads the conversation history before the run and saves the new turn "
        "after it. Its constants are defined in `rse/config.py`.",
    ], [
        ["CompiledGraph", "_COMPILED_GRAPH", "private", "_build_graph()", "The compiled 14-node graph, reused "
         "for every query"],
        ["int", "MAX_WIDEN_ATTEMPTS", "public, constant", "3", "Widenings before no_results_found"],
        ["int", "CONVERSATION_HISTORY_DAYS", "public, constant", "30", "Age used by the cleanup helper "
         "`cleanup_old_sessions()` (run manually)"],
    ], [
        dict(name="run_query()",
             purpose="Answer one question; the entry point used by `POST /retrieval/query`.",
             input="The question and an optional session id.",
             output="Final EchoState plus `session_id` and `result_count`.",
             parameters="`user_query: str`, `session_id: str | None = None`",
             exceptions="Any error inside the graph is caught: the answer becomes an error message and "
                        "`no_results = True`, so the API always responds.",
             pseudo=[
                 "if no session_id: session_id = new uuid",
                 "history = load_conversation_history(session_id)",
                 "state = initial EchoState(user_query, history)",
                 "try:   final = _COMPILED_GRAPH.invoke(state)",
                 "except error: final = state with error answer, no_results = True",
                 "if final.final_answer: save_turn(session_id, user_query, answer)",
                 "return final + session_id + result_count",
             ]),
        dict(name="_build_graph()",
             purpose="Register the 14 nodes and their edges.",
             input="None.",
             output="A compiled LangGraph.",
             parameters="None.",
             exceptions="LangGraph raises at import time if the graph is wired incorrectly.",
             pseudo=[
                 "parse_intent -> resolve_time_anchor",
                 "resolve_time_anchor -> keyword_search  AND  semantic_search   # parallel",
                 "both -> merge_and_rrf -> evaluate_quality",
                 "evaluate_quality: strong -> rerank_cross_encoder",
                 "                  weak/empty, attempts < 3 -> widen_scope -> searches",
                 "                  otherwise -> no_results_found -> END",
                 "rerank -> extend_neighborhood -> check_attachments",
                 "check_attachments -> fetch_attachment | fetch_api | synthesize",
                 "synthesize -> END",
             ]),
    ])

    class_block(d, 4, "HybridSearch", [
        "Files: `rse/search_coordinator.py` and `rse/fusion.py`. The two search branches and their fusion. The "
        "keyword branch uses PostgreSQL full-text search with an ILIKE fallback on title, subject, sender, "
        "domain, channel, and keywords. The semantic branch embeds each query variant locally and searches the "
        "full FAISS index, scoring hits by true cosine similarity. A failure in one branch returns an empty "
        "list, so the other branch still answers. Constants are defined in `rse/config.py`.",
    ], [
        ["int", "PG_KEYWORD_LIMIT", "public, constant", "200", "Keyword candidates per search"],
        ["int", "FAISS_TOP_K_PER_VARIANT", "public, constant", "30", "Semantic hits per query variant"],
        ["int", "FULL_SCAN_K_MULTIPLIER", "public, constant", "3", "k multiplier on widen attempt 3"],
        ["int", "RRF_K", "public, constant", "60", "Reciprocal Rank Fusion constant"],
        ["int", "RRF_POOL_SIZE", "public, constant", "50", "Fused candidates kept for re-ranking"],
    ], [
        dict(name="postgres_keyword_search()",
             purpose="Keyword branch (node 2a).",
             input="EchoState with the parsed intent and anchor time.",
             output="`{postgres_results: [...]}` ordered by full-text rank.",
             parameters="`state: EchoState`",
             exceptions="Database errors are logged and return an empty list.",
             pseudo=[
                 "if skip_postgres_filter or no keyword:",
                 "    return most recently accessed items (limit 1000)",
                 "where = not deleted AND processed AND source / time / anchor filters",
                 "match = full-text(query_clean) OR ILIKE on title, subject, sender,",
                 "        domain, channel, keywords",
                 "return rows ordered by ts_rank, limit 200",
             ]),
        dict(name="faiss_semantic_search()",
             purpose="Semantic branch (node 2b) with query expansion.",
             input="EchoState with the query variants.",
             output="`{faiss_results: [(memory_id, cosine), ...]}`, best first.",
             parameters="`state: EchoState`",
             exceptions="Index or model errors are logged and return an empty list.",
             pseudo=[
                 "variants = query_variants or [query_clean]",
                 "k = 30 (x3 when full_faiss_scan)",
                 "for vector in embed(variants):              # local model, no API call",
                 "    for hit in FAISS top-k: keep best cosine per memory_id",
                 "return hits sorted by cosine",
             ]),
        dict(name="merge_and_rrf()",
             purpose="Fuse the two ranked lists (node 3).",
             input="EchoState with both branch results.",
             output="`{merged_candidates: [...]}` — at most 50, with rrf_score, ranks, and similarity.",
             parameters="`state: EchoState`",
             exceptions="None; empty branches give an empty pool.",
             pseudo=[
                 "for each branch, for rank, item: score[item] += 1 / (60 + rank)",
                 "load full rows for semantic-only hits; re-apply source and time filters",
                 "sort by score, keep the top 50",
             ]),
    ])

    class_block(d, 5, "QualityGate", [
        "Files: `rse/graph_nodes.py` and `rse/graph_routing.py`. Decides whether the fused pool is good enough, "
        "and if not, how to widen the search. It makes no LLM call. After three widenings a weak or empty pool "
        "ends in `no_results_found`, so the engine says so honestly instead of guessing. Constants are defined in "
        "`rse/config.py`.",
    ], [
        ["float", "MIN_TOP_SIMILARITY", "public, constant", "0.35", "Best cosine below this → weak"],
        ["int", "MIN_RESULT_COUNT", "public, constant", "2", "Fewer candidates → weak"],
        ["int", "MAX_WIDEN_ATTEMPTS", "public, constant", "3", "Widenings before giving up"],
    ], [
        dict(name="node_evaluate_quality()",
             purpose="Grade the fused pool as strong, weak, or empty (node 4).",
             input="EchoState with merged_candidates and parsed_intent.",
             output="`{result_quality: \"strong\" | \"weak\" | \"empty\"}`.",
             parameters="`state: EchoState`",
             exceptions="An unparseable time filter skips check 3 with a warning.",
             pseudo=[
                 "if pool is empty:                                  return empty",
                 "if a source was requested and none in pool:       return empty",
                 "if a time filter is set and nothing inside it:    return weak",
                 "if fewer than 2 candidates:                        return weak",
                 "if best cosine similarity < 0.35:                  return weak",
                 "return strong",
             ]),
        dict(name="node_widen_scope()",
             purpose="Relax one dimension of the search and loop back to both branches (node 5).",
             input="EchoState with attempt_count and parsed_intent.",
             output="Updated `parsed_intent` and `attempt_count + 1`.",
             parameters="`state: EchoState`",
             exceptions="None.",
             pseudo=[
                 "attempt 1: widen time window by 4 days, or open all sources,",
                 "           or trim the query to its core keyword",
                 "attempt 2: drop time filter and anchor; all sources; core keyword only",
                 "attempt 3: skip keyword filters; full FAISS scan (k x 3)",
             ]),
    ])

    class_block(d, 6, "Reranker", [
        "File: `rse/reranker.py`. Re-scores the fused pool against the actual question with a local "
        "cross-encoder (ms-marco-MiniLM-L-6-v2), and adds how much effort the user put into each item. There is "
        "no recency term by design. If the model cannot load, ranking falls back to RRF position plus effort, so "
        "retrieval never fails on a model problem. The model name and weights are defined in `rse/config.py`.",
    ], [
        ["str", "CROSS_ENCODER_MODEL", "public, constant", "cross-encoder/ ms-marco-MiniLM-L-6-v2", "Local re-ranking model"],
        ["float", "CROSS_ENCODER_WEIGHT", "public, constant", "0.8", "Weight of relevance"],
        ["float", "EFFORT_WEIGHT", "public, constant", "0.2", "Weight of user effort"],
        ["int", "RERANK_DOC_CHAR_LIMIT", "public, constant", "512", "Characters of each item scored"],
        ["int", "_DWELL_CEILING_SECONDS, _REVISIT_CEILING, _INTERACTION_CEILING", "private, constant",
         "600, 5, 10", "Values at which each effort signal reaches 1.0"],
    ], [
        dict(name="rerank_cross_encoder()",
             purpose="Produce the final ranking (node 6).",
             input="EchoState with merged_candidates and user_query.",
             output="`{ranked_results: [...]}` with ce_score, effort_score, and final_score.",
             parameters="`state: EchoState`",
             exceptions="A model or scoring error is logged; ranking falls back to RRF order plus effort.",
             pseudo=[
                 "pairs = (question, title + subject + snippet) for each candidate",
                 "relevance = sigmoid(cross_encoder.predict(pairs))",
                 "for each candidate:",
                 "    final = 0.8 x relevance + 0.2 x compute_effort_score(candidate)",
                 "sort by final score, highest first",
             ]),
        dict(name="compute_effort_score()",
             purpose="Measure how much attention the user gave an item, from 0 to 1.",
             input="One candidate with its engagement fields.",
             output="Mean of the available signals.",
             parameters="`item: dict`",
             exceptions="None; an item never opened scores 0.",
             pseudo=[
                 "signals = [min((dwell + watch time) / 600, 1)]",
                 "add scroll depth, revisits / 5, interactions / 10, completion rate",
                 "    (each capped at 1, only when present for that source)",
                 "return average(signals)",
             ]),
    ])

    class_block(d, 7, "LLMCalls", [
        "Files: `rse/query_parser.py` and `rse/llm_synthesizer.py` — the only two places the retrieval engine "
        "calls the LLM. The provider is plug-and-play through `LLM_CONFIG` in `rse/config.py`; it is currently "
        "OpenAI gpt-4.1-mini. Both calls have a deterministic fallback, so an LLM outage never loses the "
        "retrieval work.",
    ], [
        ["str", "LLM_CONFIG[\"provider\"]", "public, constant", "\"openai\"", "Swappable in one place"],
        ["str", "parser_model, synthesizer_model", "public, constant", "gpt-4.1-mini", "Models for call 1 and "
         "call 2"],
        ["float", "parser_temperature, synthesizer_temperature", "public, constant", "0.0, 0.3", "Strict JSON "
         "for parsing; natural prose for answers"],
        ["int", "max_retries", "public, constant", "2", "Fail fast instead of long retry loops"],
        ["int", "SYNTHESIS_TOP_RESULTS", "public, constant", "10", "Results given to call 2"],
        ["int", "SNIPPET_CHAR_LIMIT", "public, constant", "500", "Characters of text per result"],
    ], [
        dict(name="parse_intent()",
             purpose="LLM call 1: turn the question into a ParsedIntent (node 1).",
             input="EchoState with user_query and conversation_history.",
             output="`{parsed_intent: ParsedIntent}`.",
             parameters="`state: EchoState`",
             exceptions="Invalid JSON or an LLM error returns the fallback intent (all sources, question as "
                        "typed).",
             pseudo=[
                 "prompt = today's date + history + few-shot examples + question",
                 "reply = LLM(prompt)                          # LLM call 1",
                 "strip code fences; parsed = json.loads(reply)",
                 "return _normalise_intent(parsed)             # repairs bad fields",
             ]),
        dict(name="synthesize()",
             purpose="LLM call 2: write the answer from the ranked results (node 10).",
             input="EchoState with ranked_results, neighbours, attachment text, fresh items, and history.",
             output="`{final_answer, no_results: False}`.",
             parameters="`state: EchoState`",
             exceptions="An LLM error returns a readable list of the top results instead of an error.",
             pseudo=[
                 "blocks = top 10 results: source, date, 500-char snippet, neighbours",
                 "add attachment text and today's items when present",
                 "answer = LLM(question + history + blocks)     # LLM call 2",
                 "return answer",
             ]),
    ])

    d.h3("Sequence Diagram")
    d.para("One question from the user to the answer. The LLM appears exactly twice; the hybrid search, fusion, "
           "quality loop, and re-ranking run locally.", ind, keep_next=True)
    d.figure(DECK_DIAGRAMS / "seq_recall.png", "Sequence diagram — recall with exactly 2 LLM calls", 6.0)


# ── Module 3 ─────────────────────────────────────────────────────────────────
def module_wellbeing(d: TemplateDocument) -> None:
    d.h2("Module 3: Digital Wellbeing and Analytics (WBA)")
    ind = CONTENT_INDENT
    d.h3("Description")
    d.para("Module 3 reflects on how time is spent, without judging it. Time totals are computed on demand from "
           "engagement data (`wba/`); sessions are stored when a rebuild is requested (manually or by an OS "
           "scheduler), and its API is mounted under `/wellbeing` when `ECHO_WBA_ENABLED=1`. The "
           "module is implemented; validation on live data is scheduled in October.", ind)
    d.bullets([
        "**Time:** totals per day, per system group, and per source. Only opened emails, admitted pages, and "
        "watched videos count; Shorts are reported separately.",
        "**Sessions:** items less than 5 minutes apart form one session. Focus = 1 / number of distinct sources "
        "in the session; fragmentation = sessions per day / average session length in minutes.",
        "**Regret:** the user marks an item as regretted. Each mark or unmark is a new row, and an odd count "
        "means regretted. Gentle reminders repeat only the user's own note, at most 2 per day.",
        "**User groups:** the user creates groups (for example \"Placement preparation\") and rules. Once a group "
        "has 6 confirmed items, new items are scored with 0.5 × rule score + 0.5 × KNN score and become pending "
        "suggestions; an item joins a group only when the user accepts it.",
        "**Weekly insight:** a short, descriptive reflection written by the LLM from aggregated numbers only.",
    ], indent=ind)
    d.para("Wellbeing language is descriptive — there are no scores and no \"wasted time\" phrasing.", ind)

    d.h3("Use Case Diagram")
    d.para("Level-1 use cases of the wellbeing module (the level-0 diagram is in Section 3.2.2).", ind,
           keep_next=True)
    d.figure(DIAGRAMS / "uc_m3_wellbeing.png", "Level 1 — Digital Wellbeing and Analytics", 4.6)
    use_case_table(d, [
        ["View daily summary and heatmap", "`GET /wellbeing/analytics/daily` and `/analytics/heatmap`: time, "
         "categories, Shorts, regret rate, sessions."],
        ["Mark or unmark an item as regretted", "`POST /wellbeing/regret/{memory_id}`, with an optional note."],
        ["Receive a gentle reminder", "Only when the session is at least 15 minutes, a habitual pattern exists, "
         "and daily limits allow; reminders can be turned off."],
        ["Create a group and its rules", "Rules by keyword, domain, channel, sender, or date range."],
        ["Add an item to a group by hand", "`POST /wellbeing/groups/{id}/assign` — the user's action is the "
         "approval."],
        ["Review group suggestions", "`GET /wellbeing/groups/review`, then accept or reject each suggestion."],
        ["View weekly insight", "`GET /wellbeing/insights/weekly`."],
        ["Rebuild sessions for a day", "`POST /wellbeing/sessions/rebuild` — recomputes that day's sessions."],
        ["Run group auto-assignment", "`POST /wellbeing/groups/auto-assign` — creates pending suggestions "
         "only."],
    ])

    d.h3("Class Diagram")
    d.para("The wellbeing classes. `TimeAggregator` and `PatternDetector` provide the numbers that the other "
           "classes describe.", ind, keep_next=True)
    d.figure(DIAGRAMS / "cls_m3_wellbeing.png", "Class diagram — Digital Wellbeing and Analytics", 6.4)

    class_block(d, 1, "WellbeingAnalytics", [
        "Files: `wba/wellbeing_analytics.py` with `wba/time_aggregator.py`. Computes sessions and the daily "
        "summary that the dashboard shows. Time totals are never stored — they are summed "
        "from `memory_engagement` on request, which avoids double counting. Sessions are stored by "
        "`rebuild_sessions_for_day()`, so the daily summary shows sessions only after a rebuild for that day.",
    ], [
        ["int", "SESSION_GAP_MINUTES", "public, constant", "5", "A longer gap closes a session"],
        ["str", "_TIME_UNION", "private, constant", "SQL", "One query per source, each with its own inclusion "
         "rule"],
    ], [
        dict(name="rebuild_sessions_for_day()",
             purpose="Recompute and store the sessions of one day. Safe to run again.",
             input="A calendar day.",
             output="`{day, sessions, items}` counts.",
             parameters="`day: date`",
             exceptions="A database error rolls back the transaction, so the previous sessions stay intact.",
             pseudo=[
                 "items = engaged items of the day, ordered by activity time",
                 "sessions = split items wherever the gap is more than 5 minutes",
                 "begin transaction",
                 "  delete that day's sessions",
                 "  for each session: insert start, end, total time, dominant group,",
                 "                    source switches; map its items",
                 "commit",
             ]),
        dict(name="daily_summary()",
             purpose="Everything the Daily Summary view needs, in one call.",
             input="A day (defaults to today).",
             output="Total time, time by group and by source, Shorts, regret rate, and session fragmentation.",
             parameters="`day: date | None = None`",
             exceptions="Database errors propagate to the API (HTTP 500).",
             pseudo=[
                 "return {",
                 "  total_seconds:   TimeAggregator.total_time(day, day),",
                 "  by_system_group, by_source, shorts,",
                 "  regret:          regret_rate(day),",
                 "  sessions:        fragmentation_for_day(day)  # sessions / avg minutes",
                 "}",
             ]),
    ])

    class_block(d, 2, "GroupManager", [
        "File: `wba/group_manager.py`. User groups with a hybrid rule-plus-KNN engine and human-in-the-loop "
        "approval. It is the only code that writes `memory_user_groups`, and it writes only through "
        "`group_suggestions` with an accepted decision. Adding an item by hand records an accepted suggestion "
        "in the same transaction, so the audit trail stays complete.",
    ], [
        ["int", "MIN_CONFIRMED_FOR_KNN", "public, constant", "6", "Confirmed items before auto-suggestions start"],
        ["float", "AUTO_SUGGEST_THRESHOLD", "public, constant", "0.70", "Hybrid score needed to suggest"],
        ["int", "KNN_TOP_K", "public, constant", "3", "Most similar members averaged"],
        ["float", "RULE_WEIGHT, KNN_WEIGHT", "public, constant", "0.5, 0.5", "Hybrid score weights"],
        ["int", "AUTO_ASSIGN_BATCH_LIMIT", "public, constant", "200", "Recent items scored per run"],
        ["set[str]", "VALID_RULE_TYPES", "public, constant", "5 types", "keyword, domain, channel, sender, "
         "time_window"],
    ], [
        dict(name="score_item_for_group()",
             purpose="Hybrid score of one item against one group.",
             input="An item and a group id.",
             output="`{rule_score, knn_score, final_score}`, or None while the group has fewer than 6 members.",
             parameters="`item: dict`, `group_id: str`",
             exceptions="If FAISS is unavailable the KNN score is 0 and the rules still apply.",
             pseudo=[
                 "members = confirmed items of the group",
                 "if len(members) < 6: return None",
                 "rule = rules matched / total rules          # count each match",
                 "knn  = mean cosine of the 3 most similar members",
                 "return rule, knn, 0.5 x rule + 0.5 x knn",
             ]),
        dict(name="run_auto_assignment()",
             purpose="Create pending suggestions for recent items. Nothing is auto-accepted.",
             input="How many recent items to score.",
             output="`{scored, suggested}` counts.",
             parameters="`limit: int = 200`",
             exceptions="Database errors propagate to the caller.",
             pseudo=[
                 "for item in recent processed items:",
                 "  for group with 6+ confirmed members:",
                 "    skip if already a member or already suggested",
                 "    s = score_item_for_group(item, group)",
                 "    if s.final > 0.70 or s.rule > 0:",
                 "        insert suggestion with decision = 'pending'",
             ]),
        dict(name="decide_suggestion()",
             purpose="Record the user's accept or reject decision — the only path into a group.",
             input="A suggestion id and the decision.",
             output="`{suggestion_id, decision}`.",
             parameters="`suggestion_id: str`, `accept: bool`",
             exceptions="`ValueError` if the suggestion does not exist; the transaction then rolls back.",
             pseudo=[
                 "begin transaction",
                 "  mark suggestion accepted or rejected, reviewed = TRUE",
                 "  if accepted: insert (memory_id, group_id) into memory_user_groups",
                 "  else:        remove that membership if present",
                 "commit",
             ]),
    ])

    class_block(d, 3, "RegretSystem", [
        "File: `wba/regret_system.py`. Records the user's own regret marks and offers optional, rate-limited "
        "reminders. E.C.H.O. never decides what is regretful. Rows are only ever inserted, never updated or "
        "deleted, so the full history is kept. Reminder limits live in Redis.",
    ], [
        ["int", "MIN_SESSION_MINUTES_FOR_REMINDER", "public, constant", "15", "Session length before a reminder"],
        ["int", "MAX_REMINDERS_PER_TYPE_PER_DAY", "public, constant", "1", "Per content type"],
        ["int", "MAX_REMINDERS_TOTAL_PER_DAY", "public, constant", "2", "Across all types"],
        ["int", "REMINDER_COOLDOWN_SECONDS", "public, constant", "7200", "Two hours between reminders"],
        ["str", "_DISABLED_KEY", "private, constant", "echo:regret:reminders:disabled", "Set when the user "
         "turns reminders off"],
    ], [
        dict(name="toggle_regret()",
             purpose="Mark an item as regretted, or unmark it (`POST /wellbeing/regret/{memory_id}`).",
             input="A memory id and an optional note.",
             output="`{memory_id, regretted, marked_at}`.",
             parameters="`memory_id: str`, `note: str | None = None`",
             exceptions="`ValueError` (HTTP 404) when the item does not exist or is deleted.",
             pseudo=[
                 "if item does not exist: raise ValueError",
                 "insert regret_events row (time, note, hour, day of week)",
                 "regretted = (number of rows for the item) is odd",
                 "return regretted",
             ]),
        dict(name="maybe_get_reminder()",
             purpose="Return a gentle reminder only when every condition holds.",
             input="The current content type and session length.",
             output="`{source_type, your_note, options}` or None.",
             parameters="`source_type: str`, `current_session_minutes: int`",
             exceptions="Without Redis, reminders are off (returns None). Database errors propagate (HTTP 500).",
             pseudo=[
                 "if reminders disabled or Redis unavailable: return None",
                 "if session < 15 minutes: return None",
                 "if no habitual regret pattern at this hour: return None",
                 "if cooldown active, or type limit (1) or daily limit (2) reached:",
                 "    return None",
                 "note = user's latest note for this type; if none: return None",
                 "record usage in Redis; return the user's own note",
             ]),
    ])

    class_block(d, 4, "InsightGenerator", [
        "File: `wba/insight_generator.py`. Writes the weekly reflection. Only aggregated numbers reach the LLM — "
        "totals per category, regret counts, session patterns, peak hours, and Shorts statistics — never email "
        "subjects, page titles, URLs, or video titles. The prompt forbids judging or advice; if the LLM fails, a "
        "deterministic summary is returned.",
    ], [
        ["str", "_INSIGHT_PROMPT", "private, constant", "prompt text", "Descriptive-only rules for the LLM"],
    ], [
        dict(name="generate_weekly_insight()",
             purpose="Weekly aggregates plus a short reflection (`GET /wellbeing/insights/weekly`).",
             input="The last day of the week (defaults to today).",
             output="`{aggregates, reflection, generated_by: \"llm\" | \"deterministic\"}`.",
             parameters="`week_end: date | None = None`",
             exceptions="Any LLM error falls back to the deterministic summary.",
             pseudo=[
                 "aggregates = collect_weekly_aggregates(week_end)   # numbers only",
                 "try:   reflection = LLM(descriptive prompt + aggregates)",
                 "except error: reflection = deterministic summary",
                 "return aggregates, reflection",
             ]),
    ])

    d.h3("Sequence Diagram")
    d.para("Opening the wellbeing views. The analytics are computed from PostgreSQL; the LLM sees aggregated "
           "numbers only, and group suggestions wait for the user's decision.", ind, keep_next=True)
    d.figure(DECK_DIAGRAMS / "seq_wellbeing.png", "Sequence diagram — wellbeing analytics", 6.0)


def section_packaging(d: TemplateDocument) -> None:
    d.h2("Packaging and Deployment Diagram")
    d.para("E.C.H.O. runs on the user's laptop. The Chrome extension and the React dashboard talk to one FastAPI "
           "process on port 8000 over HTTP JSON. That process hosts every Python package and runs the enrichment "
           "worker as a background thread. During development PostgreSQL is hosted on Neon so the four team "
           "members can work in parallel; the design target is a local database.", keep_next=True)
    d.figure(DIAGRAMS / "deployment.png", "Packaging and deployment diagram", 6.4)
    d.table(["Package", "Contents", "Runs in"], [
        ["`extension/`", "Manifest V3 extension: Chrome, YouTube, and Gmail trackers, intent gate; all capture "
         "traffic goes through one function, `postToBackend()`", "Chrome browser"],
        ["`frontend/`", "React + TypeScript dashboard (Vite, port 3000)", "Browser"],
        ["`backend/`", "FastAPI app, routers, CORS, `/health`; starts the Gmail poller and the enrichment thread",
         "uvicorn, port 8000"],
        ["`ingestion/`", "Gmail, Chrome, and YouTube connectors", "uvicorn"],
        ["`ste/`", "Storage engine; PostgreSQL, Redis, and FAISS managers; settings; token encryption",
         "uvicorn"],
        ["`enp/`", "Enrichment worker, classifier, embeddings, attachment extraction",
         "Background thread in uvicorn"],
        ["`rse/`", "LangGraph retrieval and synthesis engine", "uvicorn"],
        ["`wba/`", "Wellbeing analytics", "uvicorn (when `ECHO_WBA_ENABLED=1`)"],
        ["Data stores", "PostgreSQL 16 (16 tables), Redis, FAISS index file, local models", "Laptop / Neon during "
         "development"],
    ], [1700, 5000, 2300])


# ═════════════════════════════════════════════════════════════════════════════
# 4. Methodology
# ═════════════════════════════════════════════════════════════════════════════
def section_methodology(d: TemplateDocument) -> None:
    d.h1("Proposed Methodology / Approach")
    d.para("The basic approach of E.C.H.O.:", keep_next=True)
    d.bullets([
        "Capture only engaged activity (intent gates).",
        "Store atomically in PostgreSQL — the source of truth.",
        "Enrich in the background: clean → embed → classify.",
        "Answer with hybrid search and exactly 2 LLM calls.",
        "Reflect on time use — descriptive, never judgmental.",
    ])

    d.h2("Algorithm and Pseudocode")
    d.subhead("Algorithm 1 — Hybrid retrieval with a self-correcting loop")
    d.para("The retrieval engine combines keyword and semantic search, checks the quality of what it found, and "
           "widens the search when needed. Only the first and last steps use the LLM.", keep_next=True)
    d.code([
        "function answer(question, history):",
        "  intent = LLM.parse_intent(question, history)   # LLM call 1",
        "  intent = resolve_time_anchor(intent)  # 'after that email' -> date",
        "  attempts = 0",
        "  loop:",
        "    in parallel:",
        "      kw  = postgres_fulltext_search(intent)",
        "      sem = faiss_search(embed(intent.query))",
        "    pool = RRF(kw, sem)       # score = sum of 1 / (60 + rank)",
        "    quality = evaluate(pool)  # 5 checks -> strong/weak/empty",
        "    if quality == strong: break",
        "    if attempts == 3: return no_results_found()",
        "    intent = widen_scope(intent); attempts += 1",
        "  top = rerank(pool)   # 0.8 x cross-encoder + 0.2 x effort",
        "  top = add_neighbours(top)    # same thread / session",
        "  ctx = fetch_attachment_or_live_gmail(top)  # if asked",
        "  return LLM.synthesize(question, top, ctx)  # LLM call 2",
    ], indent=SECTION_INDENT + 360)
    d.figure(GRAPH_PNG, "The live LangGraph, generated from the code (14 nodes)", 3.2)

    d.subhead("Algorithm 2 — User-group suggestions (hybrid rules + KNN)")
    d.code([
        "for each recent item, for each active group:",
        "  if group has < 6 confirmed items: skip     # not enough examples yet",
        "  rule  = rules matched / total rules",
        "  knn   = mean cosine similarity of the 3 most similar confirmed members",
        "  score = 0.5 x rule + 0.5 x knn",
        "  if score > 0.70 or rule > 0:",
        "    add suggestion with decision = 'pending'",
        "# weekly review:  accept -> item joins the group",
        "#                 reject -> item is never added",
    ], indent=SECTION_INDENT + 360)
    d.para("Human-in-the-loop by design: an item enters a user group only through an explicit user action — "
           "adding it by hand, or accepting a suggestion in the review queue.")

    d.h2("Implementation and Results")
    d.subhead("Results obtained")
    d.bullets([
        "All three sources captured into one PostgreSQL store.",
        "Enrichment tracks pending / processed / failed; failed items are retried on demand.",
        "Multi-turn natural-language questions answered.",
        "Wellbeing analytics implemented — live-data validation pending.",
    ])
    d.subhead("How the initial approach was fine-tuned")
    d.para("Three changes were made since Phase II:", keep_next=True)
    d.table(["Aspect", "Phase II plan", "Phase III approach — and why"], [
        ["Ranking", "One weighted sum: 40% semantic + 30% engagement + 20% recency + 10% effort",
         "Merge keyword and semantic lists by rank (RRF, k = 60), then re-score: 0.8 × cross-encoder + "
         "0.2 × effort. Why: the two searches score on different scales, so adding raw scores is unfair."],
        ["Recency", "20% boost for recent items",
         "Dropped on purpose, so older but still-relevant memories are not pushed down."],
        ["User groups", "Suggested items auto-accepted",
         "Suggestions start as pending; an item joins a group only after the user accepts."],
    ], [1500, 3000, 4500])
    d.bullets([
        "**Benefits:** fairer merge of keyword and semantic results; candidates re-scored against the actual "
        "question; still exactly 2 LLM calls; the user stays in control of groups.",
        "**Drawbacks:** the cross-encoder adds CPU time to every query; one more local model to download and "
        "load.",
    ])
    d.subhead("Progress so far — 80% complete")
    d.table(["Area", "Status"], [
        ["Capture (Gmail, Chrome, YouTube), storage engine, enrichment, 14-node hybrid retrieval engine",
         "Done — shown at Review 1"],
        ["Regret marking", "New since Review 1"],
        ["User groups", "New since Review 1"],
        ["Wellbeing API routes for the dashboard", "New since Review 1"],
        ["UI (10%): finish the dashboard screens and the extension popup", "Remaining"],
        ["Cleanup (10%): fix breaking paths, add logging, clean code, remove dead code", "Remaining"],
    ], [6300, 2700])
    d.para("The wellbeing module is implemented; its validation on live data is pending and scheduled in "
           "October.")
    d.subhead("Suggestions from Review 1")
    d.table(["Suggestion from the panel", "Response"], [
        ["Data retention timeline: keep captured data for a fixed period (6 months or 1 year), then delete it "
         "automatically to save storage, as production systems do",
         "Planned as future work — a configurable auto-delete window (6 or 12 months)."],
        ["Spam filtering: add a model that filters out spam emails before they are captured",
         "Mail that Gmail already flags as spam is never captured, since the connector reads mail without the "
         "Spam folder; a dedicated spam model is planned as future work."],
        ["Focused presentation: frame the demo to show only what is needed and leave out what isn't",
         "We have kept this review to the point — it shows only what is needed, up to the Digital Wellbeing "
         "module."],
    ], [4500, 4500])

    d.h2("Further Exploration Plans and Timelines")
    d.para("Target: 100% complete in the last week of October.", keep_next=True)
    d.table(["Week", "Work", "Share"], [
        ["October · Week 1", "UI completion: implement wellbeing screens and popup; connect to the API", "UI — 10%"],
        ["October · Week 2", "UI completion: citation cards, source-breakdown chart, group-review polish", "UI — 10%"],
        ["October · Week 3", "Refactor and hardening: fix breaking paths, add logging, remove dead code. "
         "Validation: end-to-end run of all 3 sources; WBA on live data", "Cleanup — 10%"],
        ["October · Week 4 (final)", "Final testing, retrieval quality check, privacy check, documentation",
         "Validation and finish"],
    ], [2200, 4900, 1900])
    d.para("Carried forward from Review 1: wellbeing validation, dashboard refinements, end-to-end validation, "
           "retrieval quality measurement, privacy audit, and documentation — all covered above.")
    d.para("Beyond the capstone, two items remain as future work from the Review 1 suggestions: a configurable "
           "auto-delete window for stored data (6 or 12 months), and a dedicated spam-filtering model.")


# ═════════════════════════════════════════════════════════════════════════════
# Appendices
# ═════════════════════════════════════════════════════════════════════════════
def appendices(d: TemplateDocument) -> None:
    d.appendix("Appendix A:  Definitions, Acronyms and Abbreviations")
    d.table(["Term", "Definition"], [
        ["E.C.H.O.", "Efficient Contextualized History Orchestrator — this system"],
        ["GMC / CHC / YTC", "Gmail, Chrome, and YouTube connectors (capture modules)"],
        ["STE", "Storage Engine — the single write path to PostgreSQL, FAISS, and Redis"],
        ["ENP", "Enrichment Pipeline — background worker that cleans, embeds, and classifies items"],
        ["RSE", "Retrieval and Synthesis Engine — the LangGraph that answers questions"],
        ["WBA", "Digital Wellbeing and Analytics module"],
        ["UIL", "User Interface Layer — React dashboard, extension popup, and FastAPI gateway"],
        ["LLM", "Large Language Model; called exactly twice per question (parse, synthesize)"],
        ["FAISS", "Facebook AI Similarity Search — library for fast nearest-neighbour search over vectors"],
        ["RRF", "Reciprocal Rank Fusion — merges ranked lists by rank: score = Σ 1 / (60 + rank)"],
        ["Cross-encoder", "A model that reads the question and an item together and scores their relevance"],
        ["KNN", "K-Nearest Neighbours — here, the 3 most similar confirmed members of a user group"],
        ["FTS", "Full-text search in PostgreSQL"],
        ["MV3", "Chrome Extension Manifest Version 3"],
        ["OAuth", "Open Authorization — lets E.C.H.O. read Gmail and YouTube data without storing passwords"],
        ["TTL", "Time-to-live of a Redis key: 24 hours for revisits, 1 hour for attachment text"],
        ["EchoState", "The TypedDict passed between the nodes of the retrieval graph"],
        ["Memory item", "One captured unit of activity: one email, one web page, or one video"],
        ["Intent gate", "Rules that keep only activity the user actually paid attention to"],
        ["Dwell time", "Foreground time on an email or page while the user is active"],
        ["Effort score", "0-to-1 measure of attention: time, scroll, revisits, interactions, completion"],
        ["System group", "One of five fixed categories: work, study, entertainment, personal, misc"],
        ["User group", "A user-defined category (e.g. \"Placement preparation\") managed with approval"],
        ["Session", "Consecutive activity with gaps of at most 5 minutes"],
        ["Regret event", "A user's own mark on an item; each mark or unmark is a new row"],
    ], [2400, 6600])

    d.appendix("Appendix B: References")
    d.para("REFERENCES (IEEE Format):", keep_next=True)
    for ref in [
        "[1] N. Reimers and I. Gurevych, \"Sentence-BERT: Sentence embeddings using Siamese BERT-networks,\" in "
        "Proceedings of the 2019 Conference on Empirical Methods in Natural Language Processing, 2019.",
        "[2] J. Johnson, M. Douze, and H. Jégou, \"Billion-scale similarity search with GPUs,\" IEEE Transactions "
        "on Big Data, vol. 7, no. 3, pp. 535–547, 2021.",
        "[3] LangChain and LangGraph Documentation, \"Building language model applications with composable "
        "components,\" https://python.langchain.com, 2024.",
        "[4] M. Kleppmann, A. Wiggins, P. van Hardenberg, and M. McGranaghan, \"Local-first software: You own "
        "your data, in spite of the cloud,\" in Proceedings of the 2019 ACM SIGPLAN International Symposium on "
        "New Ideas, New Paradigms, and Reflections on Programming and Software, 2019.",
    ]:
        d.para(ref)
    d.para("Project documents referred to:", keep_next=True)
    d.bullets([
        "E.C.H.O. Project Scope Document, version 2.0 (architecture-aligned), Team 92, 2026.",
        "E.C.H.O. Architecture Design Document (high-level design), Team 92, 2026.",
        "E.C.H.O. Database Design Document (16-table schema), Team 92, 2026.",
        "Capstone Project Phase 2 report, Team 92, PES University, January – May 2026.",
    ])
    d.para("Standards: diagrams follow UML 2 notation; references follow the IEEE format.")

    d.appendix("Appendix C: Record of Change History")
    d.template_table("change_table", [
        ["", "Mar 2026", "0.1", "Scope and high-level architecture defined: three sources, intent-first capture, "
         "local-first storage, 16-table schema", "Phase II baseline design"],
        ["", "Apr 2026", "0.2", "Capture connectors and a single Storage Engine write path with de-duplication",
         "Phase II implementation"],
        ["", "May 2026", "0.3", "Enrichment pipeline and first retrieval graph with a self-correcting loop",
         "Phase II evaluation"],
        ["", "Jul 2026", "0.4", "Retrieval redesigned: hybrid search, RRF fusion, cross-encoder re-ranking; "
         "recency weighting removed; LLM provider made swappable", "Fairer ranking and reliable LLM access"],
        ["", "Aug 2026", "0.5", "System integrated end to end: capture → storage → enrichment → retrieval",
         "Phase 3 Review 1"],
        ["", "Sep 2026", "1.0", "Wellbeing module added with human-in-the-loop user groups; this document",
         "Phase 3 Review 2"],
    ])

    d.appendix("Appendix D: Traceability Matrix")
    d.para("Forward and backward traceability from the requirements (Project Scope Document, v2.0) through the "
           "high-level design (Architecture Design Document) to this low-level design.", keep_next=True)
    d.template_table("trace_table", [
        ["§4 Module 1 — Gmail Module", "§5 Gmail Module (GMC)", "3.2 Module 1 — GmailConnector"],
        ["§4 Module 2 — Chrome Browsing Module", "§6 Chrome Browsing Module (CHC)",
         "3.2 Module 1 — ChromeConnector"],
        ["§4 Module 3 — YouTube Module", "§7 YouTube Module (YTC)", "3.2 Module 1 — YouTubeConnector"],
        ["§4 Module 5 — Canonical Memory Module (Storage Engine)", "§4 Storage Layer; §15 Database Schema",
         "3.2 Module 1 — StorageEngine, FAISSManager"],
        ["§4 Module 6 — Enrichment Pipeline", "§8 Enrichment Pipeline; §9 System Group Classification",
         "3.2 Module 1 — EnrichmentPipeline, SystemGroupClassifier"],
        ["§4 Module 4 — Unified Retrieval Module (LangGraph RSE)", "§10 LangGraph Retrieval & Synthesis Engine",
         "3.3 Module 2; 4.1 Algorithm 1"],
        ["§4 Module 7 — Digital Wellbeing and Analytics Module", "§11 Digital Wellbeing Module (WBA)",
         "3.4 Module 3; 4.1 Algorithm 2"],
        ["§4 Module 8 — User Interface Layer", "§12 User Interface Layer (UIL)",
         "3.5 Packaging and Deployment; 4.3 Timelines"],
        ["§2 Core Design Principles (privacy, non-judgmental wellbeing)", "§13 Privacy & Ethics",
         "2 Design Constraints"],
    ])


# ═════════════════════════════════════════════════════════════════════════════
# Build
# ═════════════════════════════════════════════════════════════════════════════
def build(output: pathlib.Path, pages: dict[str, str]) -> None:
    d = TemplateDocument(TEMPLATE)
    fill_cover(d)
    d.fix_footer_page_total()
    section_introduction(d)
    section_constraints(d)
    section_design_intro(d)
    module_memory(d)
    module_retrieval(d)
    module_wellbeing(d)
    section_packaging(d)
    section_methodology(d)
    appendices(d)
    d.build_toc(pages)
    d.save(output)
    for warning in d.warnings:
        print("WARNING:", warning, file=sys.stderr)


def read_pages(docx_path: pathlib.Path) -> dict[str, str]:
    """Ask Word for the page of every heading; key by list number ("3.2.3.1") or appendix title."""
    with tempfile.TemporaryDirectory() as tmp:
        pdf = pathlib.Path(tmp) / "out.pdf"
        outline = pathlib.Path(tmp) / "outline.txt"
        subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(RENDER_DOCX),
                        "-DocxPath", str(docx_path), "-PdfPath", str(pdf), "-OutlinePath", str(outline)],
                       check=True, capture_output=True, timeout=600)
        pages: dict[str, str] = {}
        for line in outline.read_text(encoding="utf-8").splitlines():
            page, number, style, text = (line.split("\t") + ["", "", "", ""])[:4]
            key = number.rstrip(".") if number else text.strip()
            if key.count(".") == 0 and number:
                # level-1 rows are numbered by the TOC itself; key them by title
                key = text.strip()
            # The TOC's own numbered rows repeat the level-1 titles on page 2; the body heading comes
            # later in the document, so the last occurrence of a key is the one to keep.
            pages[key] = page
        return pages


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", type=pathlib.Path, default=OUTPUT)
    parser.add_argument("--single-pass", action="store_true")
    args = parser.parse_args()
    output = args.output.resolve()
    build(output, {})
    if args.single_pass:
        print(f"Built {output} (TOC page numbers empty)")
        return
    pages = read_pages(output)
    build(output, pages)
    second = read_pages(output)
    if second != pages:
        changed = {k: (pages.get(k), v) for k, v in second.items() if pages.get(k) != v}
        print(f"WARNING: page numbers moved after filling the TOC: {changed}", file=sys.stderr)
    print(f"Built {output} with {len(pages)} headings paged")


if __name__ == "__main__":
    main()
