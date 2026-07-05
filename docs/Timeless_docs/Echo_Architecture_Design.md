ECHO
Personal Memory System
Complete Architecture & Design Document
Version 2.1  ·  Architecture-Aligned  ·  March 2026

The following table:
Document Type,Complete Architecture & Design Reference
Version,2.1 — LangGraph RSE Updated
Status,Architecture Locked
Submission Deadline,August 2027
Project Type,Final-Year Capstone Project
Dev Database,Neon and upstash— PostgreSQL 15 + Redis (cloud dev)
LLM Architecture,LangGraph stateful graph  ·  Plug-and-play provider

The following table:
ARCHITECTURE IS THE SOURCE OF TRUTH.,"This document supersedes the original frozen scope. All design decisions, limitations,","feature definitions, and implementation guidelines follow the architecture documents",and design session decisions recorded January–March 2026.


1.  What is Echo?
1.1  The Problem
A user researches for an interview. They receive a confirmation email, then read articles about Operating Systems, then watch YouTube tutorials.
A week later they cannot find any of it — scattered across Gmail, Chrome history, and YouTube with no way to search across all three simultaneously.
Echo solves this.
1.2  What Echo Does
Captures digital activity — emails, web pages, YouTube videos — on the user's laptop
Understands what is important using intent-based filtering. Not everything is worth remembering
Unifies everything into one cross-source searchable memory
Reflects on time usage without judging, scoring, or blocking
Supports multi-turn conversational queries that remember context across turns
1.3  What Makes Echo Different

The following table:
Principle,What It Means
Local-First,All personal data stays on the user's laptop in PostgreSQL. Nothing is stored in the cloud permanently. Only the query text and retrieved snippets are sent to the LLM API.
Intent-Aware,Only saves content the user actually engaged with. A page opened for 2 seconds is discarded. An article read for 2 minutes is saved.
Cross-Source Search,"Ask 'OS material after interview email' and get emails, web pages, and videos together in one ranked result."
Deterministic Retrieval,The LLM parses query intent into structured JSON. The backend executes retrieval deterministically from that JSON. Every step is visible and debuggable.
Wellbeing Focus,"Shows time patterns without scoring, judging, or blocking. Regret system is entirely user-declared."
Conversational Memory,Multi-turn queries remember context. 'Find Chrome pages about that topic after that email' resolves references from prior turns.

1.4  What Echo is NOT
Not a productivity tracker or scoring system
Not a cloud service — all personal data is local
Not a content blocker or screen time enforcer
Not a surveillance tool — incognito mode is never tracked
Not a mobile application — desktop Chrome browser only for browsing and YouTube
Not an agentic free-roaming AI — the retrieval pipeline is deterministic and visible

2.  Core Design Principles
These six principles are non-negotiable. Every feature, every module, and every implementation decision must respect them.
2.1  Intent-First, Not Surveillance
Only save content the user actually paid attention to. Accidentally clicking a link and closing it in 2 seconds → NOT SAVED.
Opening an article and reading it for 2 minutes → SAVED. This keeps Echo's memory signal-rich, not noise-filled.
It respects the user's actual interests and prevents database bloat.
2.2  Local-First (Privacy by Design)
All personal data lives in PostgreSQL on the user's laptop. Nothing is permanently stored in the cloud.
The only external transmissions are: (1) OAuth API calls to Gmail and YouTube for metadata retrieval, (2) query text and retrieved result snippets sent to the LLM API for synthesis.
OAuth tokens are encrypted separately. The LLM API never receives the user's full database.
2.3  Progressive Enrichment
When content first enters Echo, minimal fields are saved instantly — crash-safe, no blocking.
Background processing later adds HTML cleaning, topic extraction, system group classification, and embedding generation.
The ingestion hot path is never delayed by expensive operations.
2.4  Deterministic Retrieval with Intelligent Parsing
The LLM parses query intent once into structured JSON. The backend executes all retrieval logic deterministically from that JSON: Postgres metadata filter, FAISS semantic search, quality evaluation, scope widening, and on-demand API calls.
The LLM only synthesizes the final answer. It does not query databases, select tools dynamically, or make hidden routing decisions.
2.5  Single-User Installation
Echo is designed for one user on one laptop. There is no multi-user isolation, no users table, no cloud sync.
Everything is scoped to the installation. This eliminates complexity while keeping the architecture clean and privacy-preserving.
2.6  Non-Judgmental Wellbeing
Echo shows descriptive patterns. It never scores productivity, predicts addiction, judges behaviour, or blocks content.
The regret system is entirely user-declared — Echo never decides what is regretful.
Language in all outputs is neutral and descriptive, never prescriptive.

3.  System Architecture Overview
3.1  The Big Picture
Three data sources feed into one ingestion pipeline which stores data in a three-layer storage system.
A LangGraph retrieval pipeline and LLM synthesis serve the user interface. The Enrichment Pipeline runs entirely in the background.

The following table:
USER ACTIVITY,  Gmail (web / mobile)       Chrome (desktop)        YouTube (desktop Chrome),       │                           │                          │,       ▼                           ▼                          ▼,  Gmail API (OAuth)     Chrome Extension MV3       YouTube Data API (OAuth),       │                           │                          │,       └───────────────────────────┼──────────────────────────┘,                                   ▼,                     INGESTION PIPELINE,              (Intent Filtering per source type),                                   │,                                   ▼,       ┌───────────────────────────────────────────────────────┐,       │              STORAGE LAYER (3 components)             │,       │  PostgreSQL (permanent)  │  FAISS (semantic search)   │,       │                          │  Redis (temp cache 24hr)   │,       └───────────────────┬──────────────────────────────────┘,                           │              ▲,                           │              │ (background),                           │         ENRICHMENT PIPELINE,"                           │         (clean, classify, embed)",                           ▼,                    LANGGRAPH RSE,          (parse → search → evaluate → widen → synthesize),                           │,                           ▼,               LLM SYNTHESIS (Claude / Gemini / Ollama),                           │,                           ▼,              USER INTERFACE (Dashboard + Extension)

3.2  Three-Layer Storage

The following table:
Database,Technology,What It Stores,Rebuildable?
PostgreSQL,PostgreSQL 15+,"All emails, pages, videos, groups, rules, conversation history, sessions",NO — permanent truth
FAISS,faiss-cpu (local),384-dim embedding vectors per memory item. Rebuilt from PostgreSQL via embedding_index table.,YES — from PostgreSQL
Redis,Redis 7+,Revisit signals (24hr TTL). Attachment text cache (1hr TTL). Max 200 MB.,YES — disposable

3.3  Data Flow Example
Scenario: User receives interview email, researches OS concepts, searches next day.

The following table:
Step 1: Email arrives → Gmail API → Saved to PostgreSQL instantly (raw),Step 2: User opens email → Extension detects → dwell_time timer starts,"Step 3: Background ENP → clean body, extract topics, generate embedding → FAISS",Step 4: User browses geeksforgeeks.org (OS article),        Extension: foreground 180s + scroll 50% → INTENT CONFIRMED,        → Saved to PostgreSQL → ENP queues for enrichment,Step 5: User watches 'OS Scheduling Algorithms' on YouTube (10 min),        Extension: playing + foreground > 20s → INTENT CONFIRMED → Saved,"Step 6: Next day, user searches: 'OS stuff after the interview email'",        LangGraph RSE fires:,"          parse_intent(LLM) → {sources:'all', time_filter:'after interview email',...}","          postgres_search   → find interview email timestamp T1, get items after T1",          faiss_search      → semantic match to 'operating systems',"          evaluate_quality  → 3 results, score 0.82 → STRONG","          synthesize(LLM)   → 'After your TechCorp email on Feb 5th, you studied OS...'"


4.  Storage Layer — Detail
4.1  PostgreSQL — Main Database
Role: The ONLY permanent storage. FAISS and Redis can both be rebuilt from PostgreSQL if lost.
PostgreSQL cannot be recovered from them.
Storage estimate for a typical student:
20 emails/day × ~20 KB = 400 KB/day from Gmail
35 web pages/day × ~20 KB = 700 KB/day from Chrome
10 videos/day × ~15 KB = 150 KB/day from YouTube
Total: ~1.25 MB/day — ~450 MB/year
Development setup: neon cloud PostgreSQL 15 (shared team database during development).
Migration to local at submission via a single pg_dump command.
Pin PostgreSQL version to 17 on neon to match target local version.
4.2  FAISS — Semantic Search Index
Role: Finds content based on meaning, not exact keyword matching.
Every memory item gets a 384-dimensional embedding vector representing its semantic meaning. Similar content gets similar vectors.
Example of semantic matching:
'OS' query matches 'Operating Systems' stored content — synonym handled
'operatin systms' query matches 'Operating Systems' — spelling error handled
'CPU scheduling' query matches 'process management' — related concept handled
Storage: ~1.5 KB per item. For 50,000 items = ~75 MB. FAISS is file-based, runs locally in Python.
It is not a cloud service. Each developer runs FAISS locally and rebuilds from the shared neon PostgreSQL using the embedding_index table.
The embedding_index.is_active flag controls rebuild: set is_active=FALSE on all rows → background worker re-embeds all → new FAISS index built.
4.3  Redis — Temporary Cache
Role: Remember observations for a short time without cluttering PostgreSQL.
Use Case 1: Revisit Detection
Problem: User opens a page for 3 seconds (not enough to save). Returns 20 minutes later for 5 seconds.
Combined = 8 seconds but each individual visit is too short.
Solution: Redis stores 'saw this page/video today with N seconds'.
When user returns, Echo detects 'revisit signal' → this counts as intent. After 24 hours, Redis forgets.
Next week's visit is treated as fresh.
Use Case 2: Attachment Text Cache
When a search returns an email with a PDF attachment and the email ranks in the top 3, Echo fetches and extracts the full PDF text.
This takes 2–5 seconds. Redis caches the extracted text for 1 hour.
If the user searches again within that hour, the result is instant. After 1 hour, Redis deletes the cached text.
Binary PDF is never stored.
Development setup:cloud Redis (Upstash ). Standard Redis URL, works with redis-py directly. No changes to redis_manager.py.

5.  Gmail Module (GMC)
5.1  What Gets Captured
All emails sent and received — with no intent filter on receipt.
Gmail is saved immediately at arrival regardless of whether the user opens it. This is different from Chrome and YouTube.
Why no intent filter for Gmail?
Inbox awareness: user needs to search 'Did I receive an email about X?' even for unread emails
Push-based: emails come TO the user (not chosen), unlike web pages which the user actively navigates to
Bounded volume: 20–50 emails/day is manageable, unlike hundreds of page views
Communication is critical: even unread emails may be important references
Important: unread emails do NOT count toward wellbeing time analytics. Only opened emails (engagement_status = 'visited') contribute to time tracking.
5.2  Ingestion Flow — Step by Step
Step 1: Immediate Save on Arrival
Gmail API fires a webhook or polling event. Echo saves the following to PostgreSQL instantly:

The following table:
INSERT INTO memory_items:,"  memory_id, source_type='gmail', source_id=email_id","  title=subject, raw_text=raw_body, created_at=received_time","  preprocessed=FALSE, classified_by='pending'",,INSERT INTO gmail_metadata:,"  email_id, thread_id, sender, recipients[]","  subject, received_at, has_attachments",,INSERT INTO memory_engagement:,"  memory_id, dwell_time_seconds=0",  (first_opened_at and last_accessed_at remain NULL until opened)

Status: Email is permanently saved. If system crashes, the raw record is safe.
The UNIQUE(source_type, source_id) constraint prevents duplicate ingestion on restart.
Step 2: Background Cleaning and Enrichment
The Enrichment Pipeline (ENP) picks up all preprocessed=FALSE rows. For each Gmail item:
Remove HTML tags (<div>, <span>, etc.)
Remove tracking pixels (invisible images used to track opens)
Remove email signatures ('Best regards, John')
Remove quoted replies ('> On Tuesday, Jane wrote...')
Extract clean_body — readable text only
Construct embeddable_text = subject + clean_body + thread_keywords + sender_hint
Generate 384-dim embedding via all-MiniLM-L6-v2
Add embedding to FAISS index, insert into embedding_index table
Run 4-stage system group classifier (see Section 9)
Set preprocessed=TRUE
Step 3: Engagement Tracking (When Email is Opened)
The Chrome Extension detects when the user opens Gmail web and navigates to a specific email.
Timer rules are identical to Chrome module: foreground + browser focused + user not idle > 30 seconds.

The following table:
When email tab becomes active:,  UPDATE memory_engagement SET,    first_opened_at = NOW()  (if first time),    last_accessed_at = NOW(),,While reading (timer active):,  dwell_time_seconds accumulates in real time,,On tab switch / minimize / idle:,  Timer PAUSES,,On return:,"  Timer RESUMES, dwell_time_seconds continues accumulating",,Final record update:,  dwell_time_seconds = total foreground active time across all opens

5.3  Thread Context Enrichment
Problem: Reply emails often have subjects like 'Re:' with no context in the body.
A search for 'OS project' cannot find 'Re: ' even though the thread is about OS.
Solution: When enriching a reply email, the ENP queries previous emails in the same thread_id and prepends their extracted keywords to the reply's embeddable_text.
The reply becomes findable through the thread's context even without its own useful content.

The following table:
Thread example:,"  Email 1: 'Can you review my Operating Systems project?' → topics: [os, scheduling]",  Email 2: 'Re:' + 'Thanks! Looks good.' (the reply),,Without thread enrichment:,  Email 2 embeddable_text = 'Re: Thanks looks good',  → Not findable by 'OS project' query,,With thread enrichment:,"  Email 2 embeddable_text = 'Re: Thanks looks good [thread:os scheduling review project]'",  → Findable by 'OS project' query

5.4  Attachment Handling — Two-Tier System
Attachment types supported: PDF, DOC, DOCX, TXT, and image files (via Tesseract OCR).
Audio and video attachments are out of scope.

The following table:
,Tier 1 — Lightweight (Always),Tier 2 — Heavyweight (On Demand)
When,"At ingestion time, for every attachment",When email ranks ≤3 in search results AND attachment filename matches query OR file size < 500 KB
What,"filename, mime_type, file_size, first 500 characters extracted",Full text extracted from all pages. Binary file never stored permanently.
Storage,"Metadata in gmail_attachments table, lightweight_extract text, embedding in FAISS","Extracted text cached in Redis (TTL: 1 hour), then deleted"
Speed,100–200 milliseconds,2–5 seconds (only triggered on-demand)

5.5  Key Files (GMC Module)
gmail_connector.py — Gmail API polling/webhook handler
gmail_auth.py — OAuth 2.0 authentication and token refresh
email_processor.py — HTML cleaning, body extraction, embeddable text construction
thread_analyzer.py — Thread context extraction and enrichment

6.  Chrome Browsing Module (CHC)
6.1  Architecture — Chrome Extension, Not History API
Echo uses a custom Manifest V3 Chrome Extension, NOT the Chrome History API. This is an intentional design decision.
The chrome.history API only provides past visit logs with timestamps — it provides no engagement signals, no foreground time, no scroll depth, no interaction count.
The custom extension provides all of these in real time.
6.2  Two-Phase Intent Gate
Every page a user visits passes through two sequential filters before being saved.
Phase 1: Exposure Gate (Fires Immediately)

The following table:
Condition: Page must be in foreground for >= 5 seconds,"If NOT met → DISCARD immediately, nothing written to Redis or PostgreSQL","If met     → Record observation in Redis, proceed to Phase 2"

Phase 2: Intent Evaluation (Fires After Minimum Exposure)
At least ONE of the following must be true:
Foreground active time >= 10 seconds
Scroll depth >= 25% of page height
1 or more interactions: click on page content, text selection
Revisit signal from Redis: user visited this URL earlier today (even briefly)

The following table:
If ANY condition met → INTENT CONFIRMED → Save to PostgreSQL,If NONE met         → DISCARD

Intent Gate Examples

The following table:
Scenario,Phase 1,Phase 2,Result
"Open article, close after 3s",FAIL (< 5s),Not reached,DISCARDED
"Open article, keep open 6s, no interaction",PASS (> 5s),FAIL (no signals),DISCARDED
"Open article, read 15s, scroll 30%",PASS,PASS (scroll > 25%),SAVED
"Misclick, instant close < 2s",FAIL,Not reached,DISCARDED
"Open page 4s, come back same page later","Phase 1 fail, Redis notes brief visit",Revisit signal on return,SAVED on revisit
"Read for 12s, no scroll or click",PASS,PASS (foreground > 10s),SAVED

6.3  Time Tracking — Strict Rules
All three conditions must be simultaneously true for time to count:

The following table:
if (tabIsActiveTab && browserWindowHasFocus && !userIdleFor30Seconds) {,    dwell_time_seconds++; // count this second,}

Example timeline:

The following table:
14:00:00  Open article → tab active                    → TIMER STARTS,14:00:10  Switch to another tab                        → TIMER PAUSES,14:02:15  Switch back to article                       → TIMER RESUMES,14:05:00  Minimize browser window                      → TIMER PAUSES,14:08:30  Restore browser                              → TIMER RESUMES,14:10:00  No mouse/keyboard movement for 30s           → TIMER PAUSES,14:10:45  Mouse moves                                  → TIMER RESUMES,14:12:00  Close tab,,Total foreground time:,  14:00:00-14:00:10  =  10s,  14:02:15-14:05:00  = 165s,  14:08:30-14:10:00  =  90s,  14:10:45-14:12:00  =  75s,  TOTAL              = 340s (5 min 40 sec)  → saved as dwell_time_seconds=340

6.4  Revisit Handling
When a user returns to a URL already in PostgreSQL, no duplicate is created. The existing record is updated:

The following table:
UPDATE memory_engagement SET,"  last_accessed_at = new_timestamp,","  dwell_time_seconds = existing + new_time,",  play_sessions_count = existing + 1,,UPDATE chrome_metadata SET,"  revisit_count = existing + 1,","  scroll_depth = MAX(existing_scroll, new_scroll),",  interaction_count = existing + new_interactions

6.5  Application Pages — Session Duration Only
Web applications (Slack, Jira, Notion, Gmail Web as an app, Confluence) contain private, sensitive content.
Echo never extracts content from these pages. Only session duration is tracked — 'Used Slack for 45 minutes' — stored in a separate attention_sessions concept, not in canonical memory_items, not semantically indexed, only counted for wellbeing time.
6.6  Content Extraction (Background ENP)
Once a page is saved to PostgreSQL, the Enrichment Pipeline extracts main content using the Readability algorithm — the same library used by Firefox Reader View.
This strips navigation menus, sidebars, ads, footer links, and comment sections, leaving only the main article text. Headings are preserved.
Result is stored in memory_items.raw_text.
6.7  Privacy Rules
Incognito mode: Chrome Extension does not run in incognito mode by design
Typed text: no keylogging, no form inputs captured
Application page content: never captured (Slack messages, Jira tasks, etc.)
Passwords or credentials: never accessed or stored
6.8  Key Files (CHC Module)
extension/manifest.json — Manifest V3 config
extension/background.js — Service worker, tab focus and window focus tracking
extension/content.js — Injected into pages, scroll depth and interaction detection
extension/popup.html + popup.js — Extension popup UI
chrome_connector.py — Backend API receiving events from extension
intent_filter.py — Two-phase gate logic
revisit_tracker.py — Redis revisit detection

7.  YouTube Module (YTC)
7.1  Platform Scope and Honest Limitations

The following table:
WORKS:   YouTube videos watched in desktop Chrome browser (via extension),"NO:      YouTube mobile app — no API, no extension support on mobile",NO:      Background play or Picture-in-Picture mode,NO:      Complete YouTube watch history — tracking starts from extension install date,WHY:     YouTube provides no watch event API. Detection is extension-driven from URL patterns.

7.2  URL-Based Video Detection
The extension monitors browser URL changes. When a YouTube URL matches a known pattern, video handling is triggered.

The following table:
Pattern 1 — Regular video:,  URL: youtube.com/watch?v=VIDEO_ID,  Extract: VIDEO_ID = 11-character string,  Classification: Long-form video,,Pattern 2 — Shorts:,  URL: youtube.com/shorts/VIDEO_ID,  Extract: VIDEO_ID,  Classification: Short-form video (< 60 seconds)

7.3  Intent Gate — Three Conditions
ANY ONE of the following must be true to trigger a save:
Option A — Sustained Watch Time: video playing + tab foreground >= 20 seconds.
(Longer than ad skip threshold of 5–15 seconds)
Option B — Manual Interaction: user paused/resumed, seeked to timestamp, changed playback speed, liked, or added to playlist
Option C — Revisit: user watched this same video_id earlier today (Redis 24-hour memory)
Example Flows

The following table:
Scenario,Option A,Option B,Result
"Auto-play next video, close after 3s",FAIL (< 20s),None,DISCARDED
"Skip ad, watch 15s of wrong video",FAIL (< 20s),None,DISCARDED
Watch OS tutorial 5 minutes,PASS (300s),—,SAVED
"Pause to take notes, resume",—,PASS (manual),SAVED
"Watch 10s morning, 12s afternoon",FAIL individually,Revisit signal fires afternoon,SAVED
Seek to specific timestamp,—,PASS (seek),SAVED

7.4  Shorts vs Long-Form Classification
Classification uses URL pattern first, duration as fallback:

The following table:
"def classify_video_type(url, duration_seconds):",    if '/shorts/' in url:          return 'short',    if duration_seconds < 60:      return 'short',    return 'long',,Shorts: auto-assigned to 'entertainment' system_group,Shorts: regret system operates at SESSION level (not individual Short level),Shorts analytics: separate count and total time in wellbeing dashboard

7.5  Complete Storage Flow
Step 1: Intent Confirmed — Immediate Save

The following table:
"INSERT INTO memory_items: video_id, is_short, source_type='youtube'","INSERT INTO youtube_metadata: video_id, (metadata pending)",INSERT INTO memory_engagement: watch_time_seconds=0 (updating in real time)

Step 2: YouTube Data API — Metadata Fetch

The following table:
GET youtube.googleapis.com/youtube/v3/videos,"  ?id=VIDEO_ID&part=snippet,contentDetails",,Response parsed:,"  title, description, channelTitle, channelId","  publishedAt, duration (ISO 8601 → seconds)","  categoryId (27=Education, 10=Music, 24=Entertainment, ...)",,UPDATE youtube_metadata SET:,"  channel_name, duration_seconds, transcript_text (next step)"

Step 3: Transcript Fetch (Best-Effort)

The following table:
Try: Fetch transcript from YouTube's caption API,If available:   transcript_text stored in youtube_metadata,                transcript_available = TRUE in raw_text,If unavailable: transcript_available = FALSE,                title + description used for embedding instead,Note: Many videos have no captions. System handles both cases.

Step 4: Embeddable Text and Embedding

The following table:
embeddable_text = title + description + channel_name + category_hint + transcript_snippet,embedding = sentence_transformer.encode(embeddable_text)  # 384-dim,faiss_index.add(embedding),"INSERT INTO embedding_index: embedding_version, vector_dimension=384, is_active=TRUE",UPDATE memory_items SET preprocessed=TRUE

7.6  Watch Time Rules
Time counts ONLY when all four conditions are simultaneously true:

The following table:
if (videoIsPlaying &&,    tabIsInForeground &&,    browserWindowHasFocus &&,    userNotIdleFor30Seconds) {,    watch_time_seconds++;,},,Completion rate = watch_time_seconds / duration_seconds,  → stored and used in effort-based re-ranking

7.7  Key Files (YTC Module)
youtube_connector.py — Backend handler for YouTube events
playback_tracker.js — Chrome Extension component, monitors YouTube DOM for play/pause/seek
youtube_api_client.py — YouTube Data API v3 integration
video_classifier.py — Shorts vs long-form classification

8.  Enrichment Pipeline (ENP)
8.1  Role and Trigger
The Enrichment Pipeline is a background worker — never called on the hot path.
It polls PostgreSQL for records where preprocessed=FALSE and processes them asynchronously. The ingestion modules write raw data instantly;
ENP enriches it in the background. If ENP is slow or stopped, ingestion continues unaffected.
8.2  Processing Steps per Source
Gmail Items
Remove HTML tags, tracking pixels, email signatures, quoted replies
Construct clean_body from readable text only
Thread context: fetch keywords from prior emails in same thread_id
Embeddable text: subject + clean_body + thread_keywords + sender_domain_hint
Generate 384-dim embedding via all-MiniLM-L6-v2
Run 4-stage system group classifier (Section 9)
Add to FAISS, insert into embedding_index, set preprocessed=TRUE
Chrome Items
Apply Readability algorithm to extract main article content from HTML
Strip navigation, sidebars, ads, footers, comment sections
Extract headings as structured metadata
Embeddable text: page_title + headings + content_snippet + domain_hint
Generate embedding, add to FAISS, classify system group, set preprocessed=TRUE
YouTube Items
Fetch metadata from YouTube Data API (title, channel, duration, category)
Attempt transcript fetch (best-effort — not all videos have captions)
Embeddable text: title + description + channel + category + transcript_snippet
Generate embedding, add to FAISS, classify system group, set preprocessed=TRUE
8.3  Attachment Processing (On-Demand)
Triggered by the LangGraph RSE when a search result in the top 3 has an attachment and the query suggests attachment content is needed.
ENP then:
Fetch full file from Gmail API (PDF, DOC, DOCX, TXT, or image)
For PDF/DOC/DOCX/TXT: extract all text using PyPDF2/pdfplumber/python-docx
For images: run Tesseract OCR to extract text
Cache extracted text in Redis (TTL: 1 hour)
Binary file never stored permanently
8.4  Key Files (ENP Module)
enrichment_pipeline.py — Main worker loop, polls preprocessed=FALSE queue
text_cleaner.py — HTML removal, body normalization, signature stripping
topic_extractor.py — Keyword-based topic extraction and auto_keywords generation
embedding_generator.py — sentence-transformers wrapper, FAISS index management
attachment_processor.py — PDF/DOC/image extraction, Redis caching

9.  System Group Classification — 4-Stage Pipeline
Every memory item must be assigned exactly one of five system group labels: work | study | entertainment |
personal | misc. This assignment happens in the background via the ENP after ingestion.
Items are saved with classified_by='pending' and system_group_id=NULL initially.

The following table:
"Why not ML? System groups classify behavioural intent, not just content. A GeeksforGeeks article could be",'study' for a student or 'work' for a developer. Cascading deterministic logic with a semantic fallback,handles this correctly without training data. system_group_id must NEVER be NULL — NULL breaks all,dashboard aggregation queries silently. Fallback is always 'misc'.

9.1  Stage 1 — Structural Signals (Authoritative, ~50% of items)
These come directly from source APIs, not content analysis. When they fire, they are correct ~99% of the time.
No content reading required. Cost: free.
Gmail Structural Rules

The following table:
Gmail label = CATEGORY_PERSONAL     → 'personal',Gmail label = CATEGORY_PROMOTIONS   → 'misc',Gmail label = CATEGORY_UPDATES      → 'misc',Gmail label = CATEGORY_FORUMS       → 'misc',sender_domain.endswith('.edu')       → 'study',sender_domain.endswith('.ac.in')     → 'study',sender_domain.endswith('.ac.uk')     → 'study',sender_domain in KNOWN_WORK_DOMAINS  → 'work',,Note: Gmail's own ML assigns CATEGORY_* labels.,Echo inherits Google's classifier for free — just reads the labelIds array.

YouTube Structural Rules

The following table:
is_short = TRUE                            → 'entertainment' (always),YouTube categoryId = '27' (Education)      → 'study',YouTube categoryId = '28' (Science & Tech) → 'study',YouTube categoryId = '10' (Music)          → 'entertainment',YouTube categoryId = '24' (Entertainment)  → 'entertainment',YouTube categoryId = '20' (Gaming)         → 'entertainment',YouTube categoryId = '23' (Comedy)         → 'entertainment'

Chrome Structural Rules

The following table:
domain.endswith('.edu')           → 'study',domain.endswith('.ac.in')         → 'study',domain in KNOWN_WORK_DOMAINS      → 'work',domain in KNOWN_ENTERTAINMENT_DOMAINS → 'entertainment'

If Stage 1 fires: assign category, classified_by='structural', classification_confidence=1.0. DONE. Stop pipeline.
9.2  Stage 2 — Domain Lookup (Chrome Only, ~25% of items)
Broader domain database check using a pre-built domain-category map with 200+ entries. Covers the majority of common browsing destinations.
Ambiguous domains (rail deliberately return None — content is too varied. These fall to Stage 3.

The following table:
domain_category_map = {,"  'geeksforgeeks.org': 'study',","  'stackoverflow.com': 'study',","  'coursera.org':      'study',","  'github.com':        'work',","  'leetcode.com':      'study',","  'reddit.com':        'entertainment',","  'medium.com':        None,   # ambiguous → Stage 3","  'substack.com':      None,   # ambiguous → Stage 3",  ...200+ entries...,},,"If Stage 2 fires: classified_by='domain', confidence=0.95. DONE. Stop."

9.3  Stage 3 — Nearest Centroid (~15% of items)
Uses the same 384-dim embedding already computed for FAISS — zero additional compute cost.
Five centroid vectors represent the semantic centre of each category.
Item embedding is compared to all five centroids by cosine similarity.

The following table:
category_seeds = {,"  'work':          'meeting deadline client project deliverable sprint report',","  'study':         'lecture tutorial concept algorithm textbook exam university',","  'entertainment': 'funny comedy music gaming meme trailer reaction vlog viral',","  'personal':      'family friend birthday vacation message dinner wedding travel',","  'misc':          'general information article news update random browse read',",},,def nearest_centroid_classify(item_embedding):,"    scores = {cat: cosine_similarity(item_embedding, centroid)","              for cat, centroid in centroids.items()}","    best   = max(scores, key=scores.get)","    score  = scores[best]","    margin = score - sorted(scores.values())[-2]  # gap to 2nd best",,    if score > 0.55 and margin > 0.08:,"        return best, score, 'confident'","    return best, score, 'ambiguous'  # → Stage 4"

Margin check is critical: if 'study' scores 0.61 and 'work' scores 0.59, the item is genuinely on the boundary.
False confidence is worse than falling to Stage 4.
How Centroids Improve Over Time
Monthly, confirmed items (classified_by in 'structural', 'user_override') are used to recompute centroids as the mean embedding of all confirmed items per category.
The centroid for 'study' drifts from generic seed text toward the actual shape of this specific user's study content.
Passive learning — no retraining ever.

The following table:
def recompute_centroid(category):,    confirmed = fetch_embeddings_where(,"        system_group=category,","        classified_by IN ('structural', 'user_override')",    ),    if len(confirmed) >= 10:,"        centroids[category] = np.mean(confirmed, axis=0)"

9.4  Stage 4 — LLM Fallback (~10% of items, declining over time)
Only genuinely ambiguous items reach here. Items are batched (every few minutes) to minimise API calls — not one call per item.
LLM receives title + 300-char snippet and returns exactly one category label.

The following table:
prompt = ''',Classify the following content into exactly one category:,  work | study | entertainment | personal | misc,,Source: {source_type},"Title:  ""{title}""","Snippet:""{first_300_chars}""",,Reply with exactly one word. Nothing else.,''',,"Result: classified_by='llm', classification_confidence=0.90",LLM results also feed monthly centroid recomputation → Stage 3 improves

9.5  Pipeline Expected Distribution

The following table:
Stage,% of Items,Cost,Improves Over Time?
1 — Structural,~50%,Free — API metadata already available,No — rules are fixed
2 — Domain Lookup,~25%,Free — dictionary lookup (~200 entries),No — static map
3 — Nearest Centroid,~15%,Free — reuses FAISS embedding already computed,Yes — monthly centroid update from confirmed items
4 — LLM Fallback,~10%,"Small API cost — batched, not per-item",Shrinks as centroid matures toward 5–8%

9.6  Complete Pipeline Function

The following table:
"def classify_system_group(item, item_embedding):","    # Stage 1 — Structural (free, authoritative)",    result = structural_classify(item),"    if result: return result, 1.0, 'structural'",,    # Stage 2 — Domain lookup (Chrome only),    if item.source_type == 'chrome':,        result = domain_classify(item.domain),"        if result: return result, 0.95, 'domain'",,    # Stage 3 — Nearest centroid (uses existing embedding),"    cat, conf, clarity = nearest_centroid_classify(item_embedding)","    if clarity == 'confident': return cat, conf, 'centroid'",,    # Stage 4 — LLM fallback (batched),    result = llm_classify_batch(item),"    return result, 0.90, 'llm'",,# Fallback: if no stage fires (shouldn't happen) → 'misc',# system_group_id MUST NEVER be NULL


10.  LangGraph Retrieval & Synthesis Engine (RSE)

The following table:
THIS SECTION REFLECTS THE UPDATED ARCHITECTURE (v2.1).,The earlier 7-step linear fallback chain has been replaced by a LangGraph stateful graph.,"LLM is called exactly TWICE per query: once for intent parsing, once for answer synthesis.",All retrieval logic between parse and synthesize is deterministic Python.

10.1  Why LangGraph
The original linear fallback chain could not handle multi-turn conversational queries.
When a user says 'find Chrome pages about that topic after that email' on Turn 2, the pipeline had no way to resolve 'that email' from Turn 1. LangGraph solves this through:
Conversation memory: full chat history available at every node via PostgreSQL-backed ConversationBufferMemory
Stateful graph: a typed state object carries all data (query, results, quality flags, attempt count) between nodes
Self-correcting evaluation loop: results are evaluated after retrieval; if weak, scope is widened and retrieval retried up to 3 times before returning no-results
Deterministic structure: the graph is fixed and explicit — an examiner can see every node and every routing decision
10.2  The LangGraph State Object

The following table:
class EchoState(TypedDict):,    # Input,    user_query:          str,    conversation_history: List,,    # Parsed intent (from parse_intent node),    parsed_intent:       dict,    # parsed_intent fields:,"    #   sources:          List[str]   # ['gmail','chrome','youtube'] or ['all']",    #   time_filter:      str|None    # ISO datetime string or None,    #   fetch_attachment: bool        # True if user wants file content,    #   fetch_api:        bool        # True if live API call needed,    #   query_clean:      str         # stripped core topic,"    #   scope_level:      int         # 0=tight,1=wider,2=widest",    #   is_ambiguous:     bool,    #   original_query:   str,,    # Retrieval results,    postgres_results:    List,    faiss_results:       List,"    attachment_content:  Optional[str]",    api_results:         List,,    # Evaluation and control,    result_quality:      str     # 'strong' | 'weak' | 'empty',"    attempt_count:       int     # 0 to 3, tracks widening attempts",,    # Output,    final_answer:        str,    no_results:          bool

10.3  The Complete Graph — 9 Nodes
Node 1: parse_intent (LLM Call 1)
The only LLM call in the retrieval phase. Uses a cheap, fast model (Claude Haiku or Gemini Flash).
Receives the user query + full conversation history + few-shot examples in the system prompt.
Outputs structured JSON conforming to the EchoState parsed_intent schema.

The following table:
SYSTEM PROMPT (excerpt):,  You are Echo's query parser. Your only job is to output valid JSON.,  Always search sources unless explicitly specified.,"  Set fetch_attachment=true only if user says: paper, PDF, document, file, attachment, open fully.","  Set fetch_api=true only if user says: latest, today, just now, received today.",  Include time_filter only for explicit time references.,,  FEW-SHOT EXAMPLES (3–5 included in prompt):,  Query:  'find my TechCorp interview email',"  Output: {""sources"":[""gmail""],""time_filter"":null,""fetch_attachment"":false,","           ""fetch_api"":false,""query_clean"":""TechCorp interview"",""is_ambiguous"":false}",,  Query:  'OS material I read yesterday',"  Output: {""sources"":[""chrome""],""time_filter"":""2026-03-16"",","           ""fetch_attachment"":false,""fetch_api"":false,","           ""query_clean"":""operating systems"",""is_ambiguous"":false}",,  Query:  'get the full PDF from the offer letter email',"  Output: {""sources"":[""gmail""],""time_filter"":null,""fetch_attachment"":true,","           ""fetch_api"":false,""query_clean"":""offer letter"",""is_ambiguous"":false}"

Node 2: postgres_search (Pure Python)
Executes SQL queries against the 16-table schema based on parsed_intent fields. Returns up to 1,000 candidate records with relevance metadata.

The following table:
"-- Core query (simplified, actual has dynamic filter assembly)","SELECT m.memory_id, m.source_type, m.title, m.created_at,","       m.auto_keywords, m.system_group_id,","       me.dwell_time_seconds, me.watch_time_seconds,","       me.last_accessed_at,","       cm.url, cm.domain, cm.scroll_depth, cm.revisit_count,","       gm.sender, gm.subject, gm.has_attachments,","       ym.channel_name, ym.is_short",FROM memory_items m,LEFT JOIN memory_engagement me  ON m.memory_id = me.memory_id,LEFT JOIN chrome_metadata   cm  ON m.memory_id = cm.memory_id,LEFT JOIN gmail_metadata    gm  ON m.memory_id = gm.memory_id,LEFT JOIN youtube_metadata  ym  ON m.memory_id = ym.memory_id,WHERE m.is_deleted = FALSE,  AND m.preprocessed = TRUE,  AND ({source_filter}),  AND ({keyword_conditions}),  AND ({time_filter}),ORDER BY me.last_accessed_at DESC,LIMIT 1000

Node 3: faiss_search (Pure Python)
Converts the query_clean to a 384-dim embedding. Searches FAISS for the most semantically similar items among the Postgres candidate IDs.
Returns top 20 by cosine similarity.

The following table:
"query_embedding = sentence_transformer.encode(state.parsed_intent['query_clean'])","candidate_ids   = [r.memory_id for r in state.postgres_results]",,# Filtered search — only search among postgres candidates,faiss_results = faiss_index.search(,"    query_embedding,","    k=20,",    filter_ids=candidate_ids  # restrict to pre-filtered set,),,"# Returns: [(memory_id, cosine_similarity_score), ...]"

Node 4: evaluate_quality (Pure Python — No LLM)
Deterministic quality check. Five sequential checks. No LLM call. Executes in milliseconds.

The following table:
def evaluate_quality_node(state):,"    results = merge_results(state.postgres_results, state.faiss_results)",,    # Check 1: Any results at all?,    if len(results) == 0:,        state.result_quality = 'empty'; return state,,    # Check 2: Source match (when specific source requested),"    if state.parsed_intent['sources'] != ['all']:","        matched = [r for r in results","                   if r.source_type in state.parsed_intent['sources']]",        if len(matched) == 0:,            state.result_quality = 'empty'; return state,,    # Check 3: Time window match (when time filter set),    if state.parsed_intent.get('time_filter'):,"        filter_dt = datetime.fromisoformat(state.parsed_intent['time_filter'])","        in_window = [r for r in results if r.created_at >= filter_dt]",        if len(in_window) == 0:,            state.result_quality = 'weak'; return state,,    # Check 4: Minimum count,    if len(results) < 2:,        state.result_quality = 'weak'; return state,,    # Check 5: Top result semantic similarity,"    if results[0].similarity_score < 0.35:",        state.result_quality = 'weak'; return state,,    state.result_quality = 'strong',    return state

Node 5: widen_scope (Pure Python — Triggered on Weak/Empty)
Called only when quality is weak or empty AND attempt_count < 3. Each attempt widens one parameter and loops back to postgres_search.

The following table:
def widen_scope_node(state):,    attempt = state.attempt_count,    intent  = state.parsed_intent,,    if attempt == 1:,"        # Widen time window by 4 days, or open sources if time not set",        if intent.get('time_filter'):,"            original = datetime.fromisoformat(intent['time_filter'])","            intent['time_filter'] = (original - timedelta(days=4)).isoformat()","        elif intent['sources'] != ['all']:","            intent['sources'] = ['all']",        else:,"            intent['query_clean'] = intent['query_clean'].split()[0]",,    elif attempt == 2:,"        # Remove time filter, open all sources, core keyword only","        intent['time_filter'] = None","        intent['sources']     = ['all']","        intent['query_clean'] = intent['query_clean'].split()[0]",,    elif attempt == 3:,"        # Full FAISS scan, bypass postgres pre-filter","        intent['skip_postgres_filter'] = True","        intent['full_faiss_scan']       = True",,    state.attempt_count += 1,    return state,,# Graph routes: widen_scope → back to postgres_search

Node 6: check_attachments (Pure Python)
Inspects top results. Routes to fetch_attachment if: fetch_attachment flag is True AND a top-3 result has attachments.
Routes to fetch_api if: fetch_api flag is True. Otherwise routes directly to synthesize.
Node 7: fetch_attachment (Pure Python — Gmail API Call)
Triggered on-demand only. Fetches full file from Gmail API, extracts text, caches in Redis (1-hour TTL). Binary file never stored.
Node 8: synthesize (LLM Call 2)
The second and final LLM call per query. Uses a configurable model (same or stronger than parser).
Assembles context from all results, attachment content, and conversation history. Generates a readable answer with source citations and temporal context.

The following table:
context = {,"    'query':       state.user_query,","    'history':     state.conversation_history,","    'results':     format_results(top_10_reranked),",    'attachments': state.attachment_content,},,answer = synthesizer_llm.invoke(context),"# LLM synthesizes, cites sources, adds temporal context",# LLM does NOT query databases or make any routing decisions

Node 9: no_results_found (Pure Python)
Triggered after 3 failed widening attempts. Returns a structured message explaining what was searched and offering suggestions.
10.4  Complete Graph Topology

The following table:
"[parse_intent]          ← LLM Call 1 (Haiku / Gemini Flash)",      ↓,"[postgres_search]       ← Pure Python SQL",      ↓,"[faiss_search]          ← Pure Python FAISS",      ↓,"[evaluate_quality]      ← Pure Python (5 checks, no LLM)",      ↓,  ┌───┴────────────────────┐,"[strong]            [weak/empty]",  ↓                   ↓,"[check_attachments]  [attempt_count < 3?]",  ↓                   ↓         ↓,"  ├─[fetch_attachment] YES       NO","  ├─[fetch_api]        [widen_scope]  [no_results_found]","  └─[synthesize]       ↓              ↓","       ↑            [postgres_search] [END]",       └─────────────┘ (loop back),,"[synthesize]            ← LLM Call 2 (configurable model)",      ↓,"[END]",,Total LLM calls: EXACTLY 2 per query regardless of loop iterations

10.5  Multi-Signal Re-Ranking
Applied before the synthesize node. Combines four signals into a final_score per result.

The following table:
final_score =,    0.40 * semantic_similarity       # FAISS cosine score,  + 0.30 * engagement_strength       # dwell_time + scroll_depth normalised,  + 0.20 * recency_score             # exponential decay from last_accessed_at,  + 0.10 * effort_score              # revisit_count + completion_rate + topic_cluster

Effort Score Detail (Cognitive Memory Scoring)

The following table:
Effort Signal,What It Means
High dwell_time_seconds,User spent significant time reading — high attention
Multiple revisit_count,User kept coming back to this page — high reference value
Deep scroll_depth > 0.5,"User read most of the content, not just the headline"
High YouTube completion rate,User watched most of the video — genuine interest
Topic clustering,Multiple items with same topic close in time — sustained focus
Post-event proximity,Created within 48h of important email — context-relevant high attention

10.6  LLM Configuration — Plug and Play
Echo has zero dependency on any specific LLM provider. Switching providers requires one configuration line change and a different API key.

The following table:
# config.py — user sets this once at installation,LLM_CONFIG = {,"    'provider':    'anthropic',  # or 'google' or 'ollama'","    'api_key':     'user_provided',","    'parser_model':      'claude-haiku-4-20250514',",    'synthesizer_model': 'claude-haiku-4-20250514',},,# Supported providers (via LangChain wrappers):,"#   anthropic → ChatAnthropic(model=..., api_key=...)","#   google    → ChatGoogleGenerativeAI(model=..., google_api_key=...)","#   ollama    → ChatOllama(model=...)  # no API key, runs locally",,# Local Ollama — realistic options for student GTX GPU:,"#   Llama 3.1 8B  (4-bit quantized, ~5.5 GB VRAM) — GTX 1070/1080","#   Mistral 7B    (4-bit quantized, ~4.5 GB VRAM) — GTX 1060 6GB+","#   Llama 3.2 3B  (4-bit quantized, ~2.5 GB VRAM) — any GTX"

10.7  Conversation Memory
Multi-turn conversation history is stored in local PostgreSQL using LangChain's PostgresChatMessageHistory. Never sent to any cloud service.
Each session has a session_id. Full history is injected into every LLM call (parse and synthesize).

The following table:
from langchain.memory import ConversationBufferMemory,from langchain_community.chat_message_histories import PostgresChatMessageHistory,,def get_session_history(session_id: str):,    return PostgresChatMessageHistory(,"        connection_string='postgresql://localhost/echo',",        session_id=session_id,    ),,# History stored in local PostgreSQL — never in cloud,"# Table: message_store (session_id, message, additional_kwargs, type, created_at)"

10.8  Key Files (RSE Module)
retrieval_engine.py — LangGraph graph assembly, state management, entry point
graph_nodes.py — All 9 node function implementations
graph_routing.py — Conditional edge routing functions
query_parser.py — parse_intent LLM call, few-shot prompt management
search_coordinator.py — postgres_search and faiss_search node implementations
reranker.py — Multi-signal re-ranking with effort score computation
llm_synthesizer.py — synthesize node, context assembly, provider factory
conversation_memory.py — PostgresChatMessageHistory setup and session management

11.  Digital Wellbeing Module (WBA)
11.1  Philosophy

The following table:
Echo DOES,Echo DOES NOT
Show patterns: 'You spent 2 hours on Shorts this week',Score productivity: 'Wellbeing score: 6/10'
Reflect regret: 'You marked 5 items as regretful',Judge behaviour: 'You wasted time'
Explain correlations: 'Regret often happens after 11 PM',Predict problems: 'You may be becoming addicted'
Enable self-awareness through descriptive data,Block content or set limits
Acknowledge user's own regret markings neutrally,Use prescriptive or judgmental language

11.2  Time Calculation Rules
Time is calculated on-demand from source tables — NOT stored separately. This prevents sync bugs and double-counting.
Gmail: only opened emails (engagement_status = 'visited') count. Unread emails never count.
Chrome: all admitted pages count (they passed the intent gate).
YouTube: videos with watch_time_seconds > 0 count.

The following table:
-- Total time today across all sources (on-demand query),SELECT SUM(time_seconds) AS total_today FROM (,    SELECT me.dwell_time_seconds AS time_seconds,    FROM memory_items m JOIN memory_engagement me ON m.memory_id=me.memory_id,    WHERE m.source_type='gmail',      AND me.first_opened_at IS NOT NULL,      AND DATE(me.last_accessed_at) = CURRENT_DATE,    UNION ALL,    SELECT me.dwell_time_seconds FROM memory_items m,    JOIN memory_engagement me ON m.memory_id=me.memory_id,    WHERE m.source_type='chrome' AND DATE(me.last_accessed_at)=CURRENT_DATE,    UNION ALL,    SELECT me.watch_time_seconds FROM memory_items m,    JOIN memory_engagement me ON m.memory_id=me.memory_id,    WHERE m.source_type='youtube' AND me.watch_time_seconds>0,      AND DATE(me.last_accessed_at)=CURRENT_DATE,) AS all_time

11.3  Two-Layer Grouping System
Layer 1: System Groups (Automatic)
5 fixed categories: work | study | entertainment | personal | misc. Assigned by the 4-stage classification pipeline (Section 9).
Always assigned — never NULL. User can override any item's system group at any time; override sets classified_by='user_override'.
Layer 2: User Groups (Intent-Based)
User-defined goal groups like 'Capstone Project', 'Placement Prep 2025', 'Health Research'. System groups answer 'what type of content?'.
User groups answer 'what personal goal does this serve?' — a question only the user can answer.
Managed through a 4-phase lifecycle:
Phase 1: Manual Seeding (First 6+ items)
User manually tags items using 'Add to Group' in the UI. No automation.
These items are the ground truth the hybrid engine depends on. Minimum 6 manually labelled items required before auto-assignment activates.
Phase 2: Auto-Assignment (After 6 Seed Items)

The following table:
"def classify_for_user_groups(new_item, new_embedding):",    for group in active_user_groups:,        if group.confirmed_count < 6: continue  # not ready,,        # Rule check,        rules   = fetch_active_rules(group.group_id),"        matched = sum(1 for r in rules if rule_matches(r, new_item))",        rule_score = matched / len(rules) if rules else 0.0,,        # KNN check (top-3 average similarity),        group_embeddings = fetch_embeddings(group.group_id),"        similarities = [cosine_sim(new_embedding, e) for e in group_embeddings]","        knn_score = mean(sorted(similarities)[-3:])",,        # Hybrid score,        final_score = 0.5 * rule_score + 0.5 * knn_score,,        if final_score > 0.70 or rule_score > 0:,"            auto_assign(new_item, group, rule_score, knn_score)"

Phase 3: Weekly Batch Review (Every Sunday)
Echo shows a review screen: 'We added 14 items to Capstone Project this week. Remove any that don't belong.'
User scans the list and removes wrong ones. Removing triggers: DELETE from memory_user_groups, UPDATE group_suggestions SET decision='rejected'.
After review: SET reviewed=TRUE for all shown items.
Phase 4: KNN Improves Each Cycle
After weekly review, confirmed_count grows and wrong items are removed.
The next week's KNN comparison is against a larger, cleaner set. Accuracy naturally improves each cycle without retraining.
11.4  Group Rules — Persistent Rule Engine
When users repeatedly accept items matching the same pattern, Echo surfaces a rule suggestion. User approves → INSERT into group_rules.
Future matching items are classified with rule_score=1.0 — deterministic match.

The following table:
rule_type,Evaluated Against  —  Example rule_value
keyword,memory_items.title ILIKE '%{value}%'  —  'operating systems'
domain,chrome_metadata.domain = '{value}'  —  'geeksforgeeks.org'
channel,youtube_metadata.channel_name = '{value}'  —  'CS Tutorials'
sender,gmail_metadata.sender ILIKE '%@{value}'  —  'techcorp.com'
time_window,"memory_items.created_at BETWEEN start AND end  —  '2025-02-01,2025-03-31'"

11.5  Session Computation
A session = continuous block of activity where consecutive items are within 5 minutes of each other.
Gap > 5 minutes starts a new session. Stored in sessions table.

The following table:
Session definition: items within 5-minute gap → same session,,Session metrics:,  session_start         = timestamp of first item,  session_end           = timestamp of last item,  total_duration_seconds (stored redundantly for fast SUM queries),  dominant_group_id     = system group appearing most in session,  item_count            = items in session (via session_memory_map),,Derived analytics (on-demand):,  focus_score  = 1 / (unique_source_types_in_session)  # lower switch = higher focus,  fragmentation = sessions_per_day / average_session_duration

11.6  Regret System
What Regret Is
User-declared reflection: 'I spent time on this and wish I hadn't.' Echo never decides what is regretful. The user does.
Each regret marking creates a new row in regret_events (1:N design) preserving full mark/unmark history.
Odd row count for a memory_id = currently regretted.
Regret Granularity

The following table:
Source,Regret Unit
Gmail,Email-level. Rarely regretted individually.
Chrome,Page-level OR session-level. Common for distraction sites.
YouTube (long-form),Video-level. Occasional regret.
YouTube Shorts,Session-level (not individual Shorts). Most common regret type. Marking individual Shorts is impractical.

Regret Analytics

The following table:
-- Regret rate today,regret_rate = (total_time_on_regretted_items / total_time_today) * 100,,-- Regret by time of day (pattern detection),"SELECT EXTRACT(HOUR FROM re.marked_at) AS hour,",       COUNT(*) AS regret_count,FROM regret_events re,GROUP BY hour ORDER BY regret_count DESC,,-- Regret by category,"SELECT sg.group_name, COUNT(re.regret_id), SUM(me.dwell_time_seconds)",FROM regret_events re,JOIN memory_items m  ON re.memory_id = m.memory_id,JOIN system_groups sg ON m.system_group_id = sg.system_group_id,JOIN memory_engagement me ON m.memory_id = me.memory_id,GROUP BY sg.group_name ORDER BY SUM(me.dwell_time_seconds) DESC

Regret Reminders (Optional, Rate-Limited)
Fires ONLY when: similar regret pattern detected AND current session >= 15 min AND cooldown passed AND user hasn't disabled
Rate limits: max 1 reminder per content type per day, max 2 total per day
Options: Got it / Snooze for today / Disable permanently
Content: always the USER'S OWN prior note — Echo never generates the regret opinion
Reminders NEVER block content, force action, or use judgmental language
11.7  LLM-Generated Insights
Weekly insights sent to LLM for synthesis. Only aggregated numbers are sent — never raw email bodies, page content, or video titles.

The following table:
# What is sent to LLM (aggregated only),context = {,"    'total_hours': 42,","    'category_breakdown': {'study':45,'work':30,'entertainment':20,'personal':5},","    'regret_count': 5,","    'regret_pattern': 'all 5 were late-night YouTube Shorts sessions',","    'session_pattern': 'email → web research → video (appeared 4 times)',","    'peak_hours': '2–5 PM',","    'shorts_count': 23,",    'shorts_time_minutes': 85,},,# What is NOT sent,# ✗ email subjects or bodies,# ✗ specific page URLs or titles,# ✗ video titles or transcript excerpts


12.  User Interface Layer (UIL)
12.1  Components
React web dashboard — main interface for search, analytics, group management, settings
Chrome Extension popup — quick search and recent activity from extension icon
FastAPI gateway — api_gateway.py, all backend routes consumed by frontend
12.2  React Dashboard — Views

The following table:
View,What It Shows
Search Interface,"Natural language search bar, results with source icons, timestamps, engagement depth. Original email accessible via iframe or link. Retrieved web pages openable by URL click."
Daily Summary,"Total time today, breakdown by system category (bar chart), regret count, focus score"
Time Heatmap,Activity level by hour × day of week. R marker on cells where regret occurred.
Source Breakdown,Time by Gmail / Chrome / YouTube with sub-breakdowns by topic
User Groups,"Group list, item counts, rule management, weekly review queue"
Regret Analytics,"Regret rate trend, pattern by time-of-day, pattern by category, item list"
Settings,"LLM API key config, source enable/disable, domain exclusions, sender exclusions, data export, deletion controls"

12.3  Chrome Extension Popup
Quick search bar — full LangGraph RSE pipeline, same as dashboard
Recent items — last 5 items across all sources
Pause/resume tracking toggle
Link to open full React dashboard
12.4  FastAPI Routes

The following table:
"POST /api/query              → LangGraph RSE, returns answer + sources",GET  /api/analytics/daily    → Time aggregation for today,GET  /api/analytics/heatmap  → Activity by hour/day matrix,GET  /api/groups             → List all user groups,POST /api/groups             → Create user group,GET  /api/groups/{id}/review → Weekly review queue,POST /api/regret/{memory_id} → Mark item as regret,DELETE /api/regret/{memory_id}→ Remove regret mark,GET  /api/recent             → Last N items across all sources,"POST /api/settings           → Update LLM config, domain exclusions",DELETE /api/items/{memory_id}→ Soft-delete item,GET  /api/export             → Export all data as JSON or CSV

12.5  Key Files (UIL Module)
api_gateway.py — FastAPI app, all routes
frontend/Dashboard.jsx — Main dashboard shell
frontend/SearchInterface.jsx — Search bar, results with iframe/link
frontend/WellbeingView.jsx — Analytics charts and heatmaps
frontend/GroupManager.jsx — User groups CRUD and weekly review
extension/popup.html + popup.js — Extension popup

13.  Privacy & Ethics
13.1  Hard Boundaries — Never Captured

The following table:
Incognito / private browsing:     Extensions do not run in incognito mode by design.,Typed text:                        No keylogging. No form inputs. No messages in Slack/Jira/Gmail compose.,"Passwords or credentials:          Never accessed, never stored. OAuth tokens encrypted separately.",Background-only activity:          Only foreground + active time counts. Minimized/background = not tracked.,"Application page content:          Slack messages, Jira tasks, Notion pages — never read, never stored.",Screen recording / screenshots:    Never taken.,Audio/video file contents:         Not processed. Inline email images not extracted.

13.2  What Does Reach the LLM API
The LLM API (Claude/Gemini) receives only:
The user's natural language query text
Formatted snippets of retrieved results — titles, short excerpts, timestamps, source types
Aggregated numerical data for wellbeing insights — totals per category, regret counts, session stats
The user's conversation history for the current session — stored locally, sent per-query
The LLM API never receives: raw email bodies, full page content, video transcripts, attachment text, PostgreSQL rows, or FAISS vectors.
13.3  User Control Points
Deletion: single items, by source, by date range, or full reset (all soft-delete with CASCADE)
Privacy settings: disable specific sources, exclude domains (blacklist), exclude sender addresses
Wellbeing controls: disable regret reminders, snooze (1 day, 1 week, forever), delete regret history
Data export: JSON/CSV for backup or migration
13.4  Development vs Production Data
During development, the team uses neon cloud PostgreSQL (shared). This contains only test/development data — never real personal emails, browsing history, or watch history from personal accounts.
The final production installation runs entirely on the user's local machine.
Migration from neon to local at submission is one pg_dump command.

14.  Technology Stack — Complete

The following table:
Layer,Technology,Version,Purpose
Storage,PostgreSQL,15+,Main permanent database — single source of truth
Storage,FAISS,Latest (faiss-cpu),Local semantic similarity search
Storage,Redis,7+,Temporary cache — revisit detection + attachment cache
Backend,Python,3.10+,All server-side logic
Backend,FastAPI,Latest,API gateway for frontend and extension
Backend,SQLAlchemy,2.x,PostgreSQL ORM
Backend,LangGraph,Latest,Stateful retrieval graph (RSE module)
Backend,LangChain,Latest,"LLM wrappers, structured output, conversation memory"
Embeddings,sentence-transformers,Latest,"all-MiniLM-L6-v2, 384-dim local embeddings"
Enrichment,Readability,Latest,Article content extraction from HTML
Enrichment,PyPDF2 / pdfplumber,Latest,PDF text extraction
Enrichment,python-docx,Latest,DOC/DOCX text extraction
Enrichment,Tesseract + pytesseract,Latest,OCR for image-based attachments
Frontend,React,18+,Web dashboard
Frontend,TailwindCSS,3.x,Dashboard styling
Frontend,Recharts,Latest,Analytics charts and heatmaps
Extension,JavaScript (MV3),—,"Chrome Extension — tab tracking, intent detection"
LLM Cloud,Claude Haiku,claude-haiku-4-20250514,Default: parsing + synthesis
LLM Cloud,Gemini Flash,gemini-2.0-flash,Alternative: parsing + synthesis (free tier)
LLM Local,Ollama + Llama 3.1 8B,—,Offline option — GTX 1070/1080 (8 GB VRAM)
LLM Local,Ollama + Mistral 7B,—,Offline option — GTX 1060 6 GB (4-bit quant)
Dev DB,Neon (PostgreSQL),15,Shared cloud dev database during development
Dev Cache,Upstash / ,7+,Shared cloud Redis during development
Testing,pytest,Latest,Python backend unit tests
Testing,Jest,Latest,JavaScript/React tests
API Testing,Postman,—,API endpoint testing
Version Control,Git,—,Source control


1.  What is Echo?
1.1  The Problem
A user researches for an interview. They receive a confirmation email, then read articles about Operating Systems, then watch YouTube tutorials.
15.  Database Schema — Complete (16 Tables)
Executes SQL queries against the 16-table schema based on parsed_intent fields. Returns up to 1,000 candidate records with relevance metadata.
The ingestion pipeline has zero dependency on the RSE/LangGraph decision. All three connectors write to the same 16-table schema.
1,1–2,Foundation + Gmail,"Neon and upstash PostgreSQL+Redis setup, 16-table schema CREATE, FAISS local setup, Gmail API OAuth, email ingestion, ENP preprocessing, embeddings to FAISS"
"9.  16-TABLE SCHEMA: locked. UNIQUE(source_type, source_id) on memory_items. No users table.",    Single-user installation. group_rules and group_suggestions are permanent additions.,,10. LLM: plug-and-play via config. No fine-tuning on user data. Few-shot prompting for parser.,    Switching provider is one config line change.
Single-user installation. No users table. Tables ordered by dependency — run CREATE TABLE statements in this exact order.

The following table:
"UNIQUENESS CONSTRAINT: memory_items has UNIQUE(source_type, source_id).",This prevents duplicate ingestion on restart or re-run. The constraint fires silently — ON CONFLICT DO NOTHING.,,SOFT DELETE: memory_items.is_deleted=TRUE hides items from all queries without losing data.,All foreign keys use ON DELETE CASCADE — deleting a memory_item cascades to all 6 subtables.,,NULL RULE: system_group_id on memory_items MUST NEVER be NULL.,All aggregation queries depend on this. Fallback: always assign 'misc'.

The following table:
TABLE 1  ·  system_groups  ·  Strong Entity
Column,Type,Constraints,Notes
system_group_id,SERIAL,"PRIMARY KEY, CHECK constraint",Auto-increment 1–5. Never modified at runtime.
group_name,VARCHAR(20),UNIQUE NOT NULL,CHECK: work|study|entertainment|personal|misc only
INSERT INTO system_groups (group_name) VALUES,"  ('work'), ('study'), ('entertainment'), ('personal'), ('misc');","-- Run once at setup. 5 rows, never changed."

The following table:
TABLE 2  ·  memory_items  ·  Strong Entity  ·  Central Abstraction
Column,Type,Constraints,Notes
memory_id,UUID,PRIMARY KEY,UUID v4 generated at ingestion time
system_group_id,INTEGER,FK → system_groups,Assigned by 4-stage classifier. MUST NOT be NULL.
source_type,VARCHAR(20),"NOT NULL, CHECK('gmail','chrome','youtube')",Determines which subtype table has detail
source_id,TEXT,NOT NULL,email_id / video_id / canonical_url_hash from native source
title,TEXT,,"Subject (Gmail), page title (Chrome), video title (YouTube)"
raw_text,TEXT,,Email body / page main content / video description — embedding source
auto_keywords,"TEXT[]",DEFAULT '{}',Heuristic top-5 nouns from title. Not authoritative.
preprocessed,BOOLEAN,DEFAULT FALSE,Has ENP pipeline run on this item?
classified_by,VARCHAR(20),DEFAULT 'pending',structural|domain|centroid|llm|user_override|pending
classification_confidence,FLOAT,DEFAULT NULL,"1.0 structural/domain, 0.55-1.0 centroid, 0.90 llm"
created_at,TIMESTAMP,NOT NULL,Source creation time — NOT ingestion time
first_ingested_at,TIMESTAMP,DEFAULT NOW(),When Echo first stored this item
last_updated_at,TIMESTAMP,DEFAULT NOW(),Updated on engagement changes
is_deleted,BOOLEAN,DEFAULT FALSE,Soft delete — user-initiated privacy deletion
UNIQUE,CONSTRAINT,"(source_type, source_id)",Prevents duplicate ingestion on re-runs

The following table:
TABLE 3  ·  gmail_metadata  ·  Weak Subtype (1:1 with memory_items)
Column,Type,Constraints,Notes
memory_id,UUID,"PRIMARY KEY, FK → memory_items ON DELETE CASCADE",Shared PK — 1:1 with parent
email_id,TEXT,NOT NULL,Gmail native message ID (from API)
thread_id,TEXT,,Groups related emails in conversation thread
sender,TEXT,,From address — used in sender rules
recipients,"TEXT[]",,Array of To/CC addresses
subject,TEXT,,Email subject line
received_at,TIMESTAMP,,Maps to parent created_at
has_attachments,BOOLEAN,DEFAULT FALSE,Quick flag — full detail in gmail_attachments

The following table:
TABLE 4  ·  gmail_attachments  ·  Weak Entity (1:N from gmail_metadata)
Column,Type,Constraints,Notes
attachment_id,UUID,PRIMARY KEY,Own UUID — not shared with parent
memory_id,UUID,FK → gmail_metadata ON DELETE CASCADE,Which email this attachment belongs to
filename,TEXT,,e.g. interview_brief.pdf
mime_type,TEXT,,"e.g. application/pdf, application/vnd.openxmlformats-officedocument"
file_size,INTEGER,,Bytes from Gmail API header
lightweight_extract,TEXT,,First ~500 chars — always extracted at ingestion
last_extracted_at,TIMESTAMP,,When heavyweight extraction last ran

The following table:
TABLE 5  ·  chrome_metadata  ·  Weak Subtype (1:1 with memory_items)
Column,Type,Constraints,Notes
memory_id,UUID,"PRIMARY KEY, FK → memory_items ON DELETE CASCADE",
url,TEXT,NOT NULL,Full original URL with query parameters
canonical_url,TEXT,NOT NULL,UTM-stripped — used for revisit deduplication
domain,TEXT,,e.g. geeksforgeeks.org — used in domain rules and analytics
referrer,TEXT,,"How user arrived at this page (search, direct, link)"
scroll_depth,FLOAT,,0.0 to 1.0 — maximum scroll position recorded
interaction_count,INTEGER,DEFAULT 0,"Clicks on content, text selections"
revisit_count,INTEGER,DEFAULT 0,Total revisit count across all sessions

The following table:
TABLE 6  ·  youtube_metadata  ·  Weak Subtype (1:1 with memory_items)
Column,Type,Constraints,Notes
memory_id,UUID,"PRIMARY KEY, FK → memory_items ON DELETE CASCADE",
video_id,TEXT,NOT NULL,YouTube 11-character video ID
channel_name,TEXT,,Used in channel rules and analytics
duration_seconds,INTEGER,,Total video length from YouTube Data API
is_short,BOOLEAN,DEFAULT FALSE,TRUE → auto-assign 'entertainment' system_group
transcript_text,TEXT,,Full transcript — richest embedding source when available

The following table:
TABLE 7  ·  memory_engagement  ·  Weak Entity (1:1 Extension)
Column,Type,Constraints,Notes
memory_id,UUID,"PRIMARY KEY, FK → memory_items ON DELETE CASCADE",1:1 with parent — always created on ingestion
dwell_time_seconds,INTEGER,DEFAULT 0,Total foreground active reading time (Gmail/Chrome)
watch_time_seconds,INTEGER,DEFAULT 0,Total foreground playback time (YouTube)
first_opened_at,TIMESTAMP,,When user first actively engaged — NULL if never opened
last_accessed_at,TIMESTAMP,,Most recent access — drives recency ranking
play_sessions_count,INTEGER,DEFAULT 0,Number of separate watch/read sessions

Separated from memory_items because engagement updates frequently on every revisit. Prevents write amplification on memory_items.

The following table:
TABLE 8  ·  regret_events  ·  Weak Entity (1:N from memory_items)
Column,Type,Constraints,Notes
regret_id,UUID,PRIMARY KEY,
memory_id,UUID,FK → memory_items ON DELETE CASCADE,Which item was regretted
marked_at,TIMESTAMP,DEFAULT NOW(),When user pressed the regret button
regret_note,TEXT,NULLABLE,Optional note: 'watched before exam'

1:N is intentional — each mark/unmark creates a new row, preserving full toggle history.
Odd row count for a memory_id = currently regretted.

The following table:
TABLE 9  ·  user_groups  ·  Strong Entity
Column,Type,Constraints,Notes
group_id,UUID,PRIMARY KEY,UUID v4
group_name,TEXT,NOT NULL,"e.g. 'Capstone Project', 'Placement Prep 2025'"
is_active,BOOLEAN,DEFAULT TRUE,"Soft delete — FALSE hides from UI, data preserved"
created_at,TIMESTAMP,DEFAULT NOW(),
updated_at,TIMESTAMP,DEFAULT NOW(),Updated on every modification

The following table:
TABLE 10  ·  memory_user_groups  ·  Associative M:N Bridge
Column,Type,Constraints,Notes
memory_id,UUID,"PK (composite), FK → memory_items CASCADE",
group_id,UUID,"PK (composite), FK → user_groups CASCADE",
CRITICAL: No row ever enters memory_user_groups without a human setting decision='accepted',"in group_suggestions first. This is architecturally enforced, not a guideline."

The following table:
TABLE 11  ·  embedding_index  ·  Weak Entity (1:1)
Column,Type,Constraints,Notes
memory_id,UUID,"PRIMARY KEY, FK → memory_items CASCADE",1:1 with parent — created only after preprocessed=TRUE
embedding_version,TEXT,NOT NULL,e.g. 'all-MiniLM-L6-v2-v1'
vector_dimension,INTEGER,NOT NULL,"384 for MiniLM, 1536 for OpenAI"
indexed_at,TIMESTAMP,DEFAULT NOW(),When FAISS index entry was created
is_active,BOOLEAN,DEFAULT TRUE,FALSE = needs re-embedding on model upgrade

On model upgrade: UPDATE embedding_index SET is_active=FALSE. Background worker re-embeds all inactive rows. FAISS index rebuilt from scratch.
Zero data loss.

The following table:
TABLE 12  ·  sessions  ·  Strong Entity
Column,Type,Constraints,Notes
session_id,UUID,PRIMARY KEY,
session_start,TIMESTAMP,NOT NULL,Timestamp of first item in session
session_end,TIMESTAMP,NOT NULL,Timestamp of last item in session
total_duration_seconds,INTEGER,,Stored redundantly — enables fast SUM queries without timestamp arithmetic
dominant_group_id,INTEGER,FK → system_groups,Most frequent system group among session's items

The following table:
TABLE 13  ·  session_memory_map  ·  Associative M:N Bridge
Column,Type,Constraints,Notes
session_id,UUID,"PK (composite), FK → sessions CASCADE",
memory_id,UUID,"PK (composite), FK → memory_items CASCADE",

The following table:
"TABLE 14  ·  group_rules  ·  Weak Entity  [Rule Classifier Backbone]"
Column,Type,Constraints,Notes
rule_id,UUID,PRIMARY KEY,
group_id,UUID,FK → user_groups ON DELETE CASCADE,Which user group this rule targets
rule_type,VARCHAR(30),NOT NULL,keyword | domain | channel | sender | time_window
rule_value,TEXT,NOT NULL,Value to match — stored lowercase
is_active,BOOLEAN,DEFAULT TRUE,Pause a rule without deleting it
created_at,TIMESTAMP,DEFAULT NOW(),

Without this table: every approved rule evaporates on restart. This table makes rule-based classification persistent and deterministic across sessions.

The following table:
"TABLE 15  ·  group_suggestions  ·  Weak Entity  [Human-in-the-Loop Audit Trail]"
Column,Type,Constraints,Notes
suggestion_id,UUID,PRIMARY KEY,
memory_id,UUID,FK → memory_items CASCADE,Item being suggested for group
group_id,UUID,FK → user_groups CASCADE,Target group for the suggestion
rule_score,FLOAT,,0.0–1.0 — proportion of active rules matched
knn_score,FLOAT,,0.0–1.0 — cosine similarity from top-3 KNN
suggested_at,TIMESTAMP,DEFAULT NOW(),When suggestion was generated
decision,VARCHAR(20),DEFAULT 'auto_accepted',auto_accepted | rejected | pending (legacy)
reviewed,BOOLEAN,DEFAULT FALSE,FALSE = must appear in next weekly review screen
decided_at,TIMESTAMP,NULLABLE,When user acted on this in weekly review

15.1  Complete Relationship Map

The following table:
From Table,Card.,To Table,Participation Rule
system_groups,1:N,memory_items,Partial both — item may be ungrouped; group may have no items
system_groups,1:N,sessions,Partial — session may have no dominant group
memory_items,1:1,gmail_metadata,Total from gmail_metadata — Gmail item MUST have this
memory_items,1:1,chrome_metadata,Total from chrome_metadata — Chrome item MUST have this
memory_items,1:1,youtube_metadata,Total from youtube_metadata — YouTube item MUST have this
memory_items,1:1,memory_engagement,Total — every item MUST get an engagement row on creation
memory_items,1:0..1,embedding_index,Partial — created only after preprocessed=TRUE
memory_items,1:N,regret_events,Partial — item may have zero regrets
gmail_metadata,1:N,gmail_attachments,Partial — email may have zero attachments
memory_items,M:N,user_groups,Via memory_user_groups — item in multiple groups
sessions,M:N,memory_items,Via session_memory_map — item can span sessions
user_groups,1:N,group_rules,Partial — group may have zero rules (manual-only mode)
user_groups,1:N,group_suggestions,Partial — group may have no pending suggestions
memory_items,1:N,group_suggestions,Partial — item may be suggested to multiple groups

15.2  Critical Indexes

The following table:
Table,Index Column(s),Purpose
memory_items,"UNIQUE (source_type, source_id)",Dedup constraint — core ingestion safety
memory_items,system_group_id,Category filtering in all dashboard queries
memory_items,created_at DESC,Temporal range queries — after/before time filters
memory_items,preprocessed WHERE FALSE,Background worker partial index — ENP job queue
memory_items,is_deleted WHERE FALSE,Partial index — exclude deleted from all queries
gmail_metadata,email_id,Fast Gmail API message ID lookup
gmail_metadata,thread_id,Thread grouping and context enrichment queries
gmail_metadata,sender,Sender-based rule matching
chrome_metadata,canonical_url,Revisit dedup — O(log n)
chrome_metadata,domain,Domain rule matching and analytics
youtube_metadata,video_id,YouTube API ID lookup
youtube_metadata,channel_name,Channel rule matching
memory_engagement,last_accessed_at DESC,Recency ranking in search results
embedding_index,is_active WHERE FALSE,Rebuild queue — ENP finds stale embeddings
sessions,session_start DESC,Time-window session queries
group_rules,"(group_id, is_active)",Rule engine fetches active rules per group
group_suggestions,"(memory_id, decision)",Find pending suggestions for a given item
group_suggestions,"(group_id, decision)",Find all pending suggestions for a group

15.3  9-Step User Group Classification Flow

The following table:
Step,Action,Database Operation
1,New item ingested,INSERT INTO memory_items. UNIQUE constraint fires — rejects duplicate automatically.
2,Rule engine fires,SELECT * FROM group_rules WHERE is_active=TRUE. Match rule_type/rule_value against new item fields.
3,KNN check (≥6 items),Fetch embeddings from FAISS for group. Compute cosine similarity. Average top-3 = knn_score.
4,Hybrid scoring,rule_score = matched/total rules. final_score = 0.5×rule_score + 0.5×knn_score.
5,Threshold decision,"IF final_score > 0.70 OR rule_score > 0: INSERT INTO memory_user_groups AND group_suggestions (decision='auto_accepted', reviewed=FALSE)."
6,User sees batch review,Sunday: SELECT * FROM group_suggestions WHERE reviewed=FALSE AND decided_at > 7 days ago.
7,User removes wrong item,"DELETE FROM memory_user_groups for that (memory_id, group_id). UPDATE group_suggestions SET decision='rejected'."
8,User keeps item,No action. Item stays in memory_user_groups. UPDATE group_suggestions SET reviewed=TRUE.
9,Pattern → rule suggestion,If 5+ accepted items with same pattern: UI suggests INSERT INTO group_rules on user approval.


16.  Module Architecture — 8 Modules

The following table:
Code,Module Name,Core Responsibilities,Key Files
GMC,Gmail Connector,"Gmail API (OAuth, polling/webhooks), email ingestion, engagement tracking via extension, thread context, attachment metadata","gmail_connector.py, gmail_auth.py, email_processor.py, thread_analyzer.py"
CHC,Chrome Connector,"Chrome Extension MV3, two-phase intent gate, foreground time tracking, scroll/interaction detection, Redis revisit, application page classification","extension/(manifest, background.js, content.js), chrome_connector.py, intent_filter.py, revisit_tracker.py"
YTC,YouTube Connector,"URL-based video detection, playback state tracking, foreground watch time, YouTube Data API, transcript fetch, Shorts classification","youtube_connector.py, playback_tracker.js, youtube_api_client.py, video_classifier.py"
STE,Storage Engine,"PostgreSQL CRUD operations, FAISS index management, Redis cache coordination, ID mapping, transaction management, FAISS rebuild safety","storage_engine.py, postgresql_manager.py, faiss_manager.py, redis_manager.py, schema.sql"
ENP,Enrichment Pipeline,"Text cleaning, Readability extraction, 4-stage system group classifier, embedding generation, transcript handling, attachment on-demand processing","enrichment_pipeline.py, text_cleaner.py, topic_extractor.py, embedding_generator.py, attachment_processor.py"
RSE,Retrieval & Synthesis (LangGraph),"LangGraph stateful graph, parse_intent (LLM), postgres_search, faiss_search, evaluate_quality, widen_scope (3-attempt loop), fetch_attachment, synthesize (LLM), conversation memory","retrieval_engine.py, graph_nodes.py, graph_routing.py, query_parser.py, search_coordinator.py, reranker.py, llm_synthesizer.py, conversation_memory.py"
WBA,Wellbeing Analytics,"Time aggregation, session computation, 4-stage system group classifier, user groups + hybrid KNN+rule engine, weekly review queue, regret system, pattern detection, LLM insights","wellbeing_analytics.py, time_aggregator.py, group_manager.py, regret_system.py, pattern_detector.py, insight_generator.py"
UIL,UI Layer,"React dashboard (search, analytics, groups, settings), Chrome Extension popup, FastAPI gateway, data export/deletion","api_gateway.py, frontend/Dashboard.jsx, frontend/SearchInterface.jsx, frontend/WellbeingView.jsx, frontend/GroupManager.jsx, extension/popup.html"

16.1  Module Interaction Flow

The following table:
"INGESTION (parallel, independent):",  GMC → STE (raw Gmail records),  CHC → STE (raw Chrome records via Redis revisit check),  YTC → STE (raw YouTube records via Redis revisit check),,"ENRICHMENT (background, non-blocking):",  STE queues preprocessed=FALSE items,"  ENP polls queue → clean, classify, embed → STE (enriched) + FAISS (embeddings)",,RETRIEVAL:,  UIL → RSE (user query + session_id),  RSE.parse_intent → LLM (Call 1),  RSE.postgres_search → STE → PostgreSQL,  RSE.faiss_search → STE → FAISS,  RSE.evaluate_quality → pure Python,  RSE.widen_scope (if needed) → loop back,  RSE.fetch_attachment (if needed) → GMC → Gmail API → Redis cache,  RSE.synthesize → LLM (Call 2),  RSE → UIL (answer + sources),,ANALYTICS:,  UIL → WBA (analytics queries),  WBA → STE → PostgreSQL aggregation queries,  WBA → LLM (weekly insights from aggregated data only),"  WBA → UIL (charts, heatmaps, regret analytics)"


17.  Development Infrastructure
17.1  Cloud Development Setup (During Development)
Team development uses neon and upstash cloud for PostgreSQL and Redis.
This provides a shared database all team members access simultaneously, enabling parallel development of connectors.
FAISS always stays local on each developer's machine.

The following table:
Component,Setup
PostgreSQL,"Neon cloud — PostgreSQL 15. Free tier: 1 GB storage, adequate for development data. Standard connection string, works identically with SQLAlchemy."
Redis,"Upstash (serverless Redis) or  — Free tier: 256 MB, 10,000 commands/day. Standard Redis URL, works with redis-py directly."
FAISS,Local only on each developer's machine. Rebuilt from neon PostgreSQL via embedding_index table. No cloud equivalent.
Migration to local,"Single command at submission: pg_dump [neon_connection_string] > echo_final.sql | psql -U postgres -d echo_local < echo_final.sql. All schema, data, indexes, and constraints transfer exactly."

The following table:
IMPORTANT: Pin PostgreSQL version to 17 on neon.,Target local production installation must also use PostgreSQL 17.,Version mismatch between pg_dump (export) and psql (import) can cause minor syntax errors.,Agree on PostgreSQL 17 across the entire team before neon setup.

17.2  What Friends Can Build Right Now
The ingestion pipeline has zero dependency on the RSE/LangGraph decision. All three connectors write to the same 16-table schema.
They can start immediately:
GMC developer: Gmail Connector, gmail_auth.py, email_processor.py against Neon PostgreSQL
CHC developer: Chrome Extension, intent_filter.py, revisit_tracker.py against Neon
YTC developer: YouTube Connector, playback_tracker.js, youtube_api_client.py against neon
ENP developer: Enrichment Pipeline, text_cleaner.py, embedding_generator.py — FAISS stays local

18.  Implementation Roadmap — 12 Weeks

The following table:
Phase,Weeks,Goal,Deliverables
1,1–2,Foundation + Gmail,"Neon and upstash PostgreSQL+Redis setup, 16-table schema CREATE, FAISS local setup, Gmail API OAuth, email ingestion, ENP preprocessing, embeddings to FAISS"
2,3–4,Chrome Module,"Manifest V3 extension, two-phase intent gate, foreground time tracking, scroll depth, Redis revisit detection, Readability content extraction, Chrome embeddings"
3,5–6,YouTube Module,"URL detection, playback state tracking, foreground watch time, intent gate, YouTube Data API metadata, transcript fetch, Shorts classification, YouTube embeddings"
4,7–8,LangGraph RSE + LLM,"LangGraph graph assembly, all 9 nodes, parse_intent with few-shot prompt, postgres_search with joins, faiss_search, evaluate_quality, widen_scope loop, synthesize, conversation memory, plug-and-play LLM config"
5,9–10,Wellbeing + Groups,"Time aggregation queries, session computation, 4-stage system group classifier, user groups CRUD, hybrid KNN+rule engine, group_rules and group_suggestions, regret system, weekly review UI, LLM insights"
6,11–12,Dashboard + Polish,"React dashboard (daily summary, heatmap, regret analytics), group management UI, extension popup, end-to-end testing, performance testing, privacy audit, demo scenarios"

The following table:
Phase 4 note: LangGraph RSE replaces the earlier 7-step linear fallback chain.,Phases 1-3 (ingestion) have zero dependency on Phase 4 (retrieval) — parallel development is possible.,Phase 4 learning curve: budget 1 week of LangGraph familiarisation before productive node implementation.,Migration from neon and upstash to local at submission is one pg_dump command.


19.  Testing Scenarios
19.1  Gmail Module Tests
Test 1: Email Reception and Preprocessing

The following table:
Action:  Send test email with subject 'Test OS Topic',Expect:  Email in PostgreSQL within 30 seconds,"         Fields: sender, subject, body, received_time, preprocessed=FALSE",Wait 1 min for ENP,"Expect:  clean_body populated, auto_keywords includes 'OS', preprocessed=TRUE","         embedding_index row created, FAISS contains this memory_id"

Test 2: UNIQUE Constraint

The following table:
Action:  Trigger ingestion of same email_id twice (simulating restart),"Expect:  Second insert silently rejected by UNIQUE(source_type, source_id)",         No duplicate row in memory_items

Test 3: Engagement Tracking

The following table:
"Action:  Open email in Gmail web, read for 2 minutes, switch tabs, return for 1 minute",Expect:  engagement_status changes to 'visited',         dwell_time_seconds ≈ 180 (tab switch pauses timer),         last_accessed_at updated on return

19.2  Chrome Module Tests
Test 4: Intent Gate — Discard

The following table:
"Action:  Open article, close after 3 seconds",Expect:  NOT saved (< 5 second exposure gate),,"Action:  Open article, keep open 6 seconds, no scroll, no interaction","Expect:  NOT saved (exposure met, Phase 2 fails)"

Test 5: Intent Gate — Save

The following table:
"Action:  Open article, read 15 seconds, scroll 30%",Expect:  SAVED (foreground > 10s OR scroll > 25%)

Test 6: Revisit Detection

The following table:
"Action:  Visit page A for 4 seconds (below threshold), close","Expect:  NOT saved, but Redis stores brief visit",,Action:  Return to page A within 24 hours for 6 seconds,Expect:  SAVED (revisit signal from Redis fires)

19.3  YouTube Module Tests
Test 7: Intent Gate

The following table:
"Action:  Open video, close after 3 seconds (ad skip simulation)",Expect:  NOT saved,,Action:  Watch video 5 minutes,"Expect:  SAVED, watch_time_seconds ≈ 300"

Test 8: Shorts Classification

The following table:
Action:  Navigate to youtube.com/shorts/SOME_ID,Expect:  is_short=TRUE in youtube_metadata,         system_group_id → 'entertainment'

19.4  LangGraph RSE Tests
Test 9: Single-Turn Simple Query

The following table:
Query:  'find my TechCorp interview email',"Expect: parse_intent → sources=['gmail'], fetch_attachment=false",        postgres_search → gmail_metadata query fires,        faiss_search → semantic match,        evaluate_quality → STRONG,        synthesize → answer citing email with date and sender

Test 10: Self-Correcting Loop

The following table:
Query:  'find emails about quantum computing' (none in DB),Expect: Attempt 1 → empty → widen_scope (time window expanded),        Attempt 2 → empty → widen_scope (remove filters),        Attempt 3 → full FAISS scan → empty,        → no_results_found node fires,        → descriptive message with suggestions returned

Test 11: Multi-Turn Conversation

The following table:
Turn 1: 'find my TechCorp interview email',"        → returns email, memory stores email_id and timestamp",,Turn 2: 'find Chrome pages I read about that topic after that email',        → parse_intent resolves 'that topic' from Turn 1 (operating_systems),        → parse_intent resolves 'that email' timestamp from Turn 1,"        → sources=['chrome'], time_filter=T1",        → returns Chrome pages after T1 with OS topic

19.5  Performance Benchmarks

The following table:
Operation,Target
Email ingestion to PostgreSQL,< 500 milliseconds
Chrome intent gate decision,< 100 milliseconds
ENP preprocessing per item,"< 5 seconds (background, non-blocking)"
Postgres search query,"< 200 milliseconds for 10,000 items"
FAISS semantic search (20 candidates from 50k),< 50 milliseconds
LLM intent parsing (Haiku/Flash),1–3 seconds
LLM synthesis (Haiku/Flash),2–5 seconds
Full query end-to-end (no API call),< 8 seconds
Full query end-to-end (with attachment),< 15 seconds
pg_dump for 1 year of data (~450 MB),< 2 minutes


20.  Glossary

The following table:
Term,Definition
Admission,Decision to save an item permanently in Echo (vs discarding it after intent evaluation)
auto_keywords,Heuristic top-5 nouns from title stored in memory_items. Not authoritative — used for fast pre-filtering only.
Canonical URL,"Normalized URL with UTM parameters, session tokens, and tracking suffixes removed. Used for revisit deduplication."
classified_by,Column on memory_items tracking which stage classified the item: structural|domain|centroid|llm|user_override|pending
Centroid,"Mean embedding vector representing a system category. Computed from seed text initially, updated monthly from confirmed items."
Conversation Memory,LangChain PostgresChatMessageHistory storing the full chat history per session in local PostgreSQL.
Dwell Time,"Total foreground active time spent on content (not background, not idle). Timer pauses on tab switch, minimize, or user idle > 30s."
Effort Score,"Supplementary 10% re-ranking signal based on engagement depth: dwell time, revisits, scroll depth, watch completion, topic clustering."
Embedding,384-number vector representing semantic meaning of text. Similar meaning = similar vectors = similar cosine similarity score.
engagement_status,Whether an item was actively opened: 'visited' (opened) or 'not_visited' (never opened). Determines wellbeing time inclusion.
FAISS,Facebook AI Similarity Search — library for fast approximate nearest-neighbour search using embedding vectors.
Few-Shot Prompting,Including 3–5 example query → JSON pairs in the parse_intent system prompt. Achieves fine-tuning-level reliability at zero cost.
Group Rules,Stored deterministic rules (keyword/domain/channel/sender/time_window) for auto-classifying items into user groups.
Group Suggestions,"Audit trail table for hybrid KNN+rule auto-assignments. decision='auto_accepted' by default, reviewed weekly in batch."
Hybrid Engine,Combination of rule-based matching + KNN cosine similarity for user group classification. final_score = 0.5×rule + 0.5×knn.
Intent Gate,The filtering logic that determines if browsing or viewing activity should be saved. Different thresholds per source.
KNN,K-Nearest Neighbours — finds K most similar embeddings in FAISS to compute average similarity score for group suggestions.
LangGraph,LangChain's graph-based execution framework. Defines retrieval as an explicit stateful directed graph with nodes and conditional edges.
Memory ID,UUID v4 identifying each saved item in Echo's database (memory_items.memory_id).
Nearest Centroid,Classification: assign item to category whose centroid embedding is most similar to item's embedding.
Preprocessed,"Boolean flag on memory_items. FALSE = ENP has not yet run. TRUE = cleaned, classified, embedded, added to FAISS."
Progressive Enrichment,"Save raw data instantly, enrich in background. Crash-safe ingestion, non-blocking hot path."
Regret,User-declared reflection that they wish they hadn't spent time on something. Echo never decides — user does.
Session,Continuous block of activity where consecutive items are within 5 minutes of each other.
System Group,Automatic coarse category (work/study/entertainment/personal/misc) assigned by 4-stage classifier.
User Group,"Manual intent-based category defined by the user (e.g. Capstone Project, Placement Prep). Reflects personal goals."
Weekly Review,Batch UI screen shown Sunday showing all auto-assigned group_suggestions from the past 7 days for user cleanup.
Wellbeing Time,"Time counted toward analytics — only engaged items: opened Gmail emails, admitted Chrome pages, watched YouTube videos."


21.  Non-Negotiable Architecture Decisions
These decisions are locked. Any change requires explicit architectural review, version increment, and team agreement.

The following table:
"1.  THREE DATABASES: PostgreSQL (permanent truth), FAISS (semantic search), Redis (temp cache).",    Each does one thing well. This separation is fundamental — not optional.,,"2.  INTENT FILTERING: no filter for Gmail, strict two-phase for Chrome, 20s threshold for YouTube.",    Thresholds may be tuned during development but the filter structure is fixed.,,3.  SYSTEM GROUPS: 4-stage cascading classifier. Structural → Domain → Centroid → LLM.,    system_group_id MUST NEVER be NULL. Fallback is always 'misc'.,,4.  USER GROUPS: human-in-the-loop via group_suggestions. No item enters memory_user_groups,    without human approval. This is architecturally enforced by the 9-step classification flow.,,5.  LANGGRAPH RSE: 2 LLM calls per query maximum. parse_intent + synthesize.,    All retrieval logic between them is deterministic Python. Self-correcting loop: 3 attempts max.,,"6.  EFFORT SCORING: 10% weight in re-ranking. Relevance AND importance, not just relevance.",,7.  WELLBEING: descriptive only. No productivity scores. No content blocking.,    Regret is user-declared. LLM insights use aggregated data only — never raw content.,,8.  PRIVACY: local-first. LLM API receives only query + snippets. No full database ever leaves device.,    Incognito never tracked. Typed text never captured. Application page content never stored.,,"9.  16-TABLE SCHEMA: locked. UNIQUE(source_type, source_id) on memory_items. No users table.",    Single-user installation. group_rules and group_suggestions are permanent additions.,,10. LLM: plug-and-play via config. No fine-tuning on user data. Few-shot prompting for parser.,    Switching provider is one config line change.

— Architecture Locked. Build Echo. —
Echo_System_Architecture_v2.1  ·  Echo_DB_Final_v4  ·  Design Session Records Jan–Mar 2026