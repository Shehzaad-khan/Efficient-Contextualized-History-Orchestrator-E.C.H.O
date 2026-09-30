# Approved content — E.C.H.O. Capstone Phase 3 Review 2 deck

The owner approved this slide plan. The deck must match it. Owner decisions are marked **[OWNER]**.

## Slide order (23 slides)
1 Title · 2 Guide Approval Mail · 3 Outline · 4 Abstract · 5 Project Scope · 6 Suggestions from Review 1 ·
7–8 Proposed Approach / Methodology · 9 Design Approach · 10 Design Constraints, Assumptions & Dependencies ·
11 Master Class Diagram · 12 Use Case Diagram · 13–14 Sequence Diagram (1/2, 2/2) · 15 Packaging & Deployment ·
16 Algorithm & Pseudocode – Retrieval · 17 Algorithm & Pseudocode – Wellbeing · 18 Project Progress So Far ·
19 Project Demonstration · 20 Timeline · 21 Conclusion · 22 References · 23 Thank You.
Template slide 16 ("Any other information") is deliberately dropped **[OWNER]**.

## Rules
- Styling must follow the official PES template (`REVIEW - 2 - UE23CS441A  –Capstone Project Phase – 2.pptx`): red title right-aligned over a cyan rule, blue Trebuchet MS body, PES logo top-right.
- Slide 1: title, ID 92, guide Dr. M. Shalima Sulthana, 4 members with SRNs — copied from `ECHO.pptx` slide 1.
- Slide 2: heading + logo only, otherwise EMPTY (mentor approval mail goes here later) **[OWNER]**.
- Slide 3: outline kept exactly as the template **[OWNER]**.
- Slides 4 and 5: copied from `ECHO.pptx` slides 5 and 6 — keep both slides **[OWNER]**. (One truncated word "exp" in the source was completed to "export".)
- Slide 6: copied exactly from `ECHO.pptx` slide 7 (3 panel suggestions) PLUS a one-line "Response" under each **[OWNER]**. Responses must be truthful: retention and spam model are NOT implemented (planned/future work); Gmail spam-folder mail is not captured (connector uses messages.list without includeSpamTrash); delete and export endpoints exist.
- Slides 7–10: methodology, design approach, constraints — from Phase II decks/report, updated to the current code.
- Slides 11–12: class and use case diagrams reused from Phase II as-is.
- Slides 13–15: sequence and deployment diagrams REDRAWN to match the current system **[OWNER]** (hybrid keyword+semantic search, RRF, cross-encoder; no "Effort-Aware Ranker").
- Slides 16–17: pseudocode must match the current code.
- Slide 18: 80% complete. Remaining 20% = 10% UI + 10% cleanup/refactoring. WBA is implemented but NOT validated on live data — the deck must say so **[OWNER]**.
- Slide 19: demo plan; data is created live (no static dataset); WBA is the post-Review-1 addition.
- Slide 20: 4 October weeks; 100% in the LAST WEEK OF OCTOBER; no exact final date **[OWNER]**. Retrieval quality check and privacy check sit in the final polishing week **[OWNER]**.
- Slide 21: summary of the above.
- Slide 22: references [1]–[4] copied exactly from `ECHO.pptx` slide 26; reference [5] (A. Thorp) REMOVED **[OWNER]**.
- Slide 23: template "Thank You".

## Facts that must be right (verified against code)
- Exactly 2 LLM calls per query (parse_intent + synthesize).
- Retrieval graph has 14 nodes (`rse/retrieval_engine.py`).
- RRF k = 60; final score = 0.8 × cross-encoder + 0.2 × effort (`rse/config.py`); MIN_TOP_SIMILARITY 0.35; MAX_WIDEN_ATTEMPTS 3.
- Quality gate: 5 checks → strong / weak / empty (`rse/graph_nodes.py` node_evaluate_quality); weak/empty after 3 widenings → no_results_found (`rse/graph_routing.py`).
- Sessions: 5-minute gap; focus = 1 / distinct sources; fragmentation = sessions per day / avg session minutes (`wba/wellbeing_analytics.py`).
- Groups: need ≥ 6 confirmed items; score = 0.5·rule + 0.5·knn; knn = mean cosine of top 3 members; suggest if score > 0.70 or rule score > 0; suggestions are 'pending' until the user accepts (`wba/group_manager.py`).
- Regret reminders: at most 2 per day total (`wba/regret_system.py`).
- Recency weighting was deliberately removed; Phase II planned 40/30/20/10 weighting.
- Extension posts to localhost:8000. The ENP worker runs as a background thread inside the uvicorn process
  (`backend/main.py` `start_enp_worker`, started at boot); `enp/enrichment_pipeline.py` can also run standalone.
- The LLM is OpenAI gpt-4.1-mini (`rse/config.py` `LLM_CONFIG`, switched from Gemini on 2026-07-09) — older decks
  still say Gemini; trust the config, not the decks.
- Recall answers carry inline [1], [2] citations; there are no separate "source cards" in the API response yet.

## Changes after independent review (2026-09-30)
Fixed: LLM provider, ENP-as-thread, "source cards" wording, slide-17 footer (manual add is also a user action),
methodology diagram moved to its own slide, sequence diagram split into 3 slides (deck is now 25 slides).
Open: whether WBA is re-enabled before the review (demo slide assumes yes — see RESTORE.md).

## Owner edits (2026-09-30, second pass) **[OWNER]**
- Slide 6 responses: retention = "planned as future work" only (no soft-delete line); focused presentation =
  "kept to the point, only what is needed, up to the Digital Wellbeing module" (no "four-step demo").
- No "[1], [2] inline citations" wording anywhere (results, demo, sequence diagram) — too specific, will change.
- Wellbeing pseudocode slide: ONLY the user-group hybrid rules + KNN algorithm; sessions algorithm removed.
- Progress slide "New since Review 1": only regret marking, user groups, wellbeing API routes.
- Timeline week 1: "Implement wellbeing screens and popup", not "turn back on".
