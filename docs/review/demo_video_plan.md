# Demo video plan — presentation Thu 5 Nov 2026, about 3 pm

Written Tue 3 Nov 2026. The video replaces the live demo, so it is recorded, edited and checked BEFORE Thursday.
Everything is recorded on the TEST ACCOUNT only (`capstone0092`). The data that already exists in it (Brightwave interview + offer PDF,
Flipkart internship, Komprise placement mail, campus placements page, OS pages, hackathon, Hyderabad flight) IS a placement-season story,
so no new data is needed for the story; only the screens below have to be warmed up.

Target length: **about 7 minutes** (5-minute cut: drop scenes 6 and 8). One continuous voice-over recorded AFTER the screen capture, not while clicking.

---

## The story (one sentence for the intro)
"It is placement season. In three weeks I read about companies, studied for the interviews, got mails, an interview call and an offer, and
my laptop scattered all of it over Gmail, Chrome and YouTube. Echo keeps it in one memory I can ask, and shows me how I spent the time."

## Scenes

| # | Scene | What is on screen | Seconds | What it proves (say this) |
|---|---|---|---|---|
| 1 | Problem + architecture | One PPT slide (the pipeline diagram) while you speak | 25 | Three sources, one memory, local-first |
| 2 | Capture | Extension popup (pause toggle, recent 5) -> a short pre-recorded clip: open a page, read ~15 s, scroll -> it appears on **Timeline** after the next ENP cycle (10 s). Then one sentence: "a 3-second misclick is not kept" (no demo needed) | 50 | Intent gates: only engaged content is saved, nothing typed, no incognito |
| 3 | Timeline | **Timeline** page: items grouped by day, source badges (Gmail / Chrome / YouTube) | 20 | One unified memory, three sources |
| 4 | Recall, 4 questions | **Recall** page, in this order (all are in the evaluation set, so they are known to work): <br>1. "When is my interview with Brightwave?" <br>2. follow-up: "And what did they offer me afterwards?" <br>3. "What stipend does the Brightwave offer letter mention?" (answer is inside the PDF) <br>4. "Everything I saved about Komprise" (Gmail + Chrome + YouTube pills) | 150 | Follow-ups resolve earlier turns; attachment text is read; one answer cites all sources; progress lines show the agent searching |
| 5 | Past chats | Open an earlier chat from the list: bubbles and memory pills come back | 15 | Conversation memory is stored |
| 6 | Wellbeing: Patterns + regret | **Timeline**: press the regret mark on one late-night item. **Patterns**: step to a day with data; show category bars, the heatmap (the regret cell is flagged) and the one-sentence insight | 55 | Time patterns reflected back without scores or "wasted time" wording |
| 7 | Groups (the weekly review) | **Patterns** -> groups card with a pre-made group (e.g. "Placement Prep") and 2-3 PRE-SEEDED suggestions: press Keep on one, Remove on an auto-accepted one. Then 8 s on the Rules page | 45 | Items enter a group only through a suggestion; confident ones are auto-accepted and can be undone |
| 8 | Reflections + reminder | **Reflections**: regret by hour, by category, marked items. Show the regret reminder card if it is due | 30 | The user's own marks, descriptive language |
| 9 | Privacy controls | **Settings**: capture toggles, retention, pause. Delete one item on Timeline. (Export: just say it exists) | 30 | User stays in control; data stays on the laptop |
| 10 | MCP in Claude Desktop | Claude Desktop: "Search my Echo memory for the Brightwave offer" -> tool call -> answer | 40 | Echo is also a tool for other assistants (E1) |
| 11 | Close | Slide: 563 tests, ~94 % coverage, CI green, ~2.3 LLM calls per query, retrieval numbers from `docs/review/eval/RESULTS_2026-10-26.md` | 20 | Engineering quality |

Sum: about 8 min with everything, about 5.5 min without scenes 8 and 10. Trim by editing, not by rushing the narration.

## What to SHOW vs what to only SAY

**Show (they look good, are quick, and are reliable):** Recall, Timeline, Patterns, regret mark, Reflections, one group-suggestion click, Settings toggles, MCP.

**Do not show, just say one sentence:**
- The Sunday auto-assign run: say "once a week Echo scores new items against my groups and puts the suggestions in this review screen." Show the
  finished review screen with suggestions that already exist. Do NOT run `auto-assign` or create the group live.
- Gmail OAuth, `setup_db`, migrations, schema: one slide at most.
- Enrichment internals (4-stage classifier, embedding, FAISS, RRF, cross-encoder): the architecture slide covers them; the narration names them in one breath.
- Thresholds, calibration, centroid recompute, retention cleanup, OCR (Tesseract not installed, so OCR stays off).
- Delete-chat, add-rule, pick-items-by-hand (D6): they were built 2 Nov and are not yet checked live. Show them ONLY if the dry run passes (see below); otherwise skip.
- Export: mention only.
- Time-based queries ("today", "yesterday"): they depend on the date of recording and scored lowest (0.71). If you want one, "Which emails did I get after the Brightwave interview email?" works on any day.

## Before recording (Tue night / Wed morning)

1. Start Redis (`wsl -d Ubuntu -u root -- sleep infinity`), PostgreSQL, backend (:8000), frontend (:3000). Confirm the Recall page is NOT in the "demo mode" badge (the badge means the backend is unreachable).
2. **Dry run of every scene once** without recording. Note any slow or broken step. The first Recall query after a start is slow (model load): ask one throwaway question first. Median answer time was 8 s, p95 24 s, so expect waits; cut them in editing and keep the progress lines.
3. Seed the screens that need data: at least 3-4 regret marks on real late-night items (so Patterns heatmap and Reflections are not empty), one group with 2-3 pending suggestions and one auto-accepted one.
4. Browser: 1920x1080, zoom 110-125 %, hide bookmarks bar and other extensions, dark Echo theme as is, no real inbox tab visible. Windows: turn on Do Not Disturb so no notification pops up.
5. Record with OBS (or the Windows Game Bar, Win+Alt+R). 1080p, 30 fps, one scene per file so a bad take only costs that scene. Mic only for a separate voice-over pass.
6. Scene 2 (capture) cannot be done on camera in one go, because a visit needs real reading time and the poller/ENP need a cycle: record it as its own clip and speed it up where nothing happens.

## Schedule

| When | What |
|---|---|
| Tue 3 Nov, evening | Dry run, fix anything broken, seed data (steps 1-3). Run `pytest` once, so slide numbers are fresh. |
| Wed 4 Nov, morning | Record the scenes (about 1.5 h with retakes). |
| Wed 4 Nov, afternoon | Voice-over, cut, export MP4 (H.264, 1080p). Watch the whole video once, with sound, on another device. |
| Wed 4 Nov, evening | Your own review of your part and of the code; update the PPT with the video slide and the final numbers. |
| Thu 5 Nov, morning | Copy the video to the laptop, a pen drive AND Google Drive. Test the video file on the presentation machine if you can. |
| Thu 5 Nov, ~3 pm | Present. Backup 1: the MP4. Backup 2: frontend in demo mode (clearly badged, seeded data) if the backend will not start. |

## Voice-over rules
- Say "what this shows" in one line per scene (the "What it proves" column), not how the code works. Questions about code come after the video.
- Wellbeing wording stays descriptive: "time pattern", "marked as regret", never "score" or "wasted".
- Do not read the numbers off the screen; state the one that matters per scene.
- Correction to remember: the audit claim is "no database row and no text beyond the excerpt caps reaches the model", NOT "no complete email" (a short mail goes in whole; see `RESULTS_2026-10-26.md` section 5).
