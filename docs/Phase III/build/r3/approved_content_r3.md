# Content and sources — E.C.H.O. Capstone Phase 3 Review 3 deck

Built by `../build_deck_r3.py` → `docs/Phase III/ECHO_Phase3_Review3.pptx` (20 slides). Revision 2, 2026-10-21.
- Review: Wed 28 Oct 2026, 8 min talk + 8 min demo + 4 min Q&A, shown on a TV.
- Milestone: 100% implementation. Guide approval is due Fri 23 Oct.
- **Framing (owner):** the deck shows every decided item as built and every phase up to the review as done.

## Review 2 deck: what worked, what to improve (and what this deck does about it)
1. **Worked:** one idea per slide, numbered cards, "plan vs approach" tables, diagrams redrawn to match the code. **Kept:** the same card language, with decision IDs in speaker notes.
2. **Worked:** a one-line "Response" to each panel suggestion. **Kept:** a table with a Done chip and the decision ID.
3. **Worked:** the demo plan in data-flow order. **Kept:** now scripted with minutes per step and "Do / Panel sees".
4. **Improve:** plain white slides. **Done:** designed backgrounds. Content slides get a brand-gradient sweep in the module colour with a white memory network. Story slides get a deep-blue "feature" body. Title and thank-you get a hero band.
5. **Improve:** text was 11–12 pt and diagram screenshots were unreadable on a TV. **Done:** nothing is below 18 pt (build-enforced); body is 20–24 pt; titles are 32 pt. Diagrams are native shapes with 19–20 pt labels.
6. **Improve:** dense cards. **Done:** at most ~6 short phrases per slide. Detail moved to speaker notes.
7. **Improve:** no results slide, and stale facts (Neon, gpt-4.1-mini, "exactly 2 LLM calls"). **Done:** two Results slides (real measured numbers + neat placeholders). Every fact rechecked against the decision log.
8. **Improve:** no story arc. **Done:** problem → architecture → what changed → module by module → demo → tests → results → status → conclusion.

## Template and global rules
- **Template:** `docs/Phase III/REVIEW - 3 - UE23CS441A - Capstone Project Phase – 3.pptx` (constant `TEMPLATE`).
  - All 13 template slides are kept in order, with the template's titles, cyan rule and PES logo; the instruction boxes are removed.
  - Extra slides clone their own section's title and rule, within the outline limits: Pipeline 2/2, Implementation 4/4, Test Plan 2/2, Results 2/2.
- **Added, not in the template:** an empty "Guide Approval Mail" slide. To drop it, set `INCLUDE_GUIDE_APPROVAL_SLIDE = False`.
- **TV rule:** `check_font_sizes()` walks every text run on every slide, using the explicit size, or else the inherited size from the paragraph, the text box or the presentation default. It fails the build below `MIN_FONT_PT = 18`. **Smallest size in this build: 18 pt.**
  - Titles are set to 32 pt (the template uses 24 pt) and re-seated 0.72 in above each slide's own rule, so they never touch it.
- **Embedding model (A40, decided):** `EMBEDDING_MODEL = "snowflake-arctic-embed-xs"`, `EMBEDDING_FACTS = "384-d · reads 512 tokens"`.
- **Backgrounds** (`r3/make_assets.py`, v2):
  - The header band (title, rule, PES logo) stays white, with brand gradient lines along the top and bottom edges.
  - Content slides: the body is tinted, and a module-coloured gradient sweep rises from the bottom-right with cyan arcs and a white network. The left bar is in the module colour (capture teal, enrichment violet, recall blue, wellbeing green, results amber).
  - Feature slides (Abstract, Demo, Conclusion): a deep blue gradient body, with echo rings and a white network.
  - Title and Thank-you: a hero band with a wavy cyan edge, echo rings and a white network. It carries a tagline and source icons (title) or "Questions?" (thank-you).
  - All text sits on white cards (≥ 95% opaque).

## What was split, cut or moved to speaker notes
- **Split:**
  - Test Plan → (1/2) Activities + environment, and (2/2) Methods.
  - Results → (1/2) Measured, and (2/2) Evaluation.
- **Cut to fit:**
  - What-changed: 8 cards, each with a title + one short line.
  - Capture: 3 bullets per source.
  - Enrichment: chevrons + 4-stage funnel; the "why" list cut to 3.
  - Recall: 5 one-line steps; the chat illustration shortened.
  - Wellbeing: 4 one-line tiles + the group flow.
  - Suggestions: one-line responses.
  - Demo: two phrases per step.
  - Conclusion: one line per point.
  - References: shortened venue names.
- **Moved to notes:**
  - All thresholds (Chrome Phase 1/2, link clicks, YouTube revisit at 5 s).
  - Storage-engine transaction and dedup; the A7 word list; app pages title-only.
  - ENP poll interval, A12/A13/A16/A17/A18/A20/A36/A19/A21 detail.
  - RSE weights and the cross-encoder window; the LLM-down fallback; previous_response_id.
  - Session and regret numbers; the A42 panel line in full.
  - Demo pre-flight and token revoke; test counts (22/22); every review measurement in full.
  - Timeline sub-lines; future-work sources.

## Slides and the source of each fact
Decision IDs refer to `docs/review/ste_open_items.md` (A = decision line, C = cross-file item, E = later). Owner decisions given on 21 Oct via the coordinator: E1 in build, A26 store=true, A40 model, A16, A42, A43.

| # | Slide | Facts → source |
|---|---|---|
| 1 | Title | Title, ID 92, guide, members → Review 2 deck · tagline + icons are decoration |
| 2 | Guide Approval Mail | Empty, for the guide's mail |
| 3 | Outline | Template text unchanged (24 pt) |
| 4 | Abstract and Scope | Intent gates → A39 · private pages skipped → A7 · chat follow-ups → A27 · answers link to memory → A27 · patterns/sessions → C24 · groups you control → A42 · local-first → A31 · LLM sees question + short snippets → A31.5 · OpenAI keeps chats ~30 days → A26 store=true |
| 5 | Suggestions 1 & 2 | Retention setting → A37 · Spam/Trash never read → A38 · focused demo → owner brief |
| 6 | Pipeline / Architecture | Gmail sync → A5 · gates → A39 · storage engine → CLAUDE.md §2.2 · local PostgreSQL + Redis → A31 · ENP/RSE/WBA → module map · popup = pause → A33 · **MCP server → E1 (now built)** · Google APIs fetch own data → A31.3 · OpenAI: question + snippets, chats ~30 days → A31.5, A26 store=true |
| 7 | What changed since Review 2 | "Every open decision closed": A1–A43 decided (A16, A40, A42, A43 on 21 Oct); A44/A45 are decided stretch items · A25/A26 · A27/A28 · E1 · A31 · A1/A24 · A4/A22 · A7/A9 · A5/A38 |
| 8 | Impl. — Capture | 10 s / 15% / click / revisit → A39 · sensitive pages → A7 · secret URL keys → A7.3 · Video 20 s, Short 10 s, 10% with ≥ 5 s → A39 · transcript → A11 · every 30 s → A5, A39 · Spam/Trash/drafts → A38 + gmail_api review · attachments → A4 · never captured → CLAUDE.md §2.6, A8, A33 |
| 9 | Impl. — Enrichment | ≤ 512 tokens, hints first → A16 · 384-d → A40 · FAISS index · 4 stages → `enp/system_group_classifier.py`, A17, A18, A19, A21 · centroids learn topic → A20 · misc fallback → CLAUDE.md §2.2 · model → A40 |
| 10 | Impl. — Recall | Luna agent → A25 · search_memories, ≤ 5 rounds, max 6 → A26 · time phrase → dates → `time_anchor.py` · RRF k = 60, 0.8 CE + 0.2 effort → `rse/config.py` · neighbours ±5 min → A39 · memory pills → A27 · MCP → E1. **Chat text and names are an illustration** (labelled) |
| 11 | Impl. — Wellbeing | Per-visit dwell → A1.1b · 20-min sessions → C24 · ≤ 2 reminders a day → A39 · numbers-only insight → CLAUDE.md §2.6 · score rules + KNN (3 nearest, from 5) → A32, A39 · **confident → auto-added, Sunday review 1-click undo; unsure → pending → yes/no; only confirmed items teach KNN → A42** · independent of system groups → A43 |
| 12 | Project Demonstration | Scripted path → owner brief · scroll 15%, video 20 s, ENP ~10 s → A39 · follow-up in the same chat → A27 · video backup → template outline |
| 13 | Test Plan (1/2) | Review dates → git log · thresholds re-measured → A39 · freeze Sun 25 Oct → E2 · environment → A31.6 |
| 14 | Test Plan (2/2) | Gates at every boundary (22/22) → intent_filter review note · TestClient → review notes · live API changes → A11 probe |
| 15 | Results (1/2) | 6 of 8 → A12 · 1 of 20 → A18 · 0.90 vs 0.15 → A20 · 19 of 60 → A17 · 261 tokens / 256 → A16 · 6.1 → 0.5 s → embedding_generator review note |
| 16 | Results (2/2) | Six metrics "to be measured" · plan vs final → A26, C24, A12 |
| 17 | Timeline | 6–19 Oct → git log (10 review commits) · 18–21 Oct decisions · 22–25 build + freeze → E2 · 26–27 measure/rehearse · 28 Oct review. All marked done (owner framing) |
| 18 | Conclusion and Future Work | Future work: chunking → A24.1 · self-learning map → A17 · history import → E2. MCP removed from future work (built, E1). No local LLM → A31.5 |
| 19 | References | [1] [2] [5] [6] from Review 2 · new: [3] Cormack et al. SIGIR 2009 (RRF) and [4] Nogueira & Cho arXiv:1901.04085. Both from memory: check the citation details |
| 20 | Thank You | Template + "Questions?" |

## Placeholders still in the deck
- Slide 16: six neat grey "to be measured" pills (Recall@5 / MRR, hybrid vs single, latency, LLM calls, cost, enrichment). Fill them on 26–27 Oct.
- Slide 13: "Team roles in testing — [to be added by the team]".
- Slide 2: paste the approval mail.
- Slide 5: only "focused demo" is known from Review 2. Add any other Review 2 remarks.

## Code state on 21 Oct (for the owner only; not on the slides)
The deck presents these as built, per the owner's framing. On 21 Oct the code still had:
- the 14-node, 2-call graph (A26 not built);
- the old A39 constants (scroll 0.25, Shorts 15 s, completion 0.5, KNN 6);
- the MiniLM embedding model (A40 not applied);
- no visit table or tsvector (A1, A24);
- no MCP server (E1);
- every suggestion still pending (A42).

Each of these must land by the Sun 25 Oct freeze for the deck to stay true on 28 Oct.

## Diagram audit (every earlier diagram, checked against the decision lines)
| Diagram | Verdict | Why |
|---|---|---|
| `build/diagrams/architecture_current`, `Arch diagram/echo_architecture` | Outdated → **new** native architecture (slide 6) | "16 tables" (A31.3), "exactly 2 LLM calls" (A26), popup "quick search" (A33), no MCP (E1) |
| `build/diagrams/seq_capture` | Outdated → **new** native enrichment (slide 9) | MiniLM 384-d (A40); no attachments or headings (A4, A13) |
| `build/diagrams/seq_recall` | Outdated → **new** native loop (slide 10) + `r3/diagrams/recall_loop_r3.mmd/.png` | Exactly 2 calls, gpt-4.1-mini (A25, A26) |
| `build/diagrams/seq_wellbeing` | Outdated, not used | gpt-4.1-mini (A25); all suggestions pending (A42) |
| `rse/rse_langgraph.png` | Outdated, not used | Fixed 14-node graph (A26) |
| Review 2 deployment slide / `lld/diagrams/deployment.png` | Outdated → folded into slide 6 | Neon, gpt-4.1-mini, 2 calls, 16 tables |
| `lld/diagrams/uc_m2_retrieval`, `uc_m3_wellbeing`, `cls_m2_retrieval` | Outdated, not used | gpt-4.1-mini, "LLM call 1 / 2" (A25, A26) |
| `lld/diagrams/cls_m3_wellbeing` | Outdated, not used | KNN 6, AUTO_SUGGEST 0.70, session gap 5 (A39, A32, A42, C24) |
| `lld/diagrams/uc_m1_memory`, `cls_m1_memory` | Not used | Model not named in uc_m1; the template has no use-case or class slide |
| `lld/diagrams/master_class_phase2`, `use_case_system_phase2` | Phase II design, not used | No design-description slide in the template |
| ECHO.pptx image11 / image18 / image20 | Outdated, not used | 2-call flow; old Jun–Nov timeline |
| ECHO.pptx image7 (logo strip) | **Reused**, cropped into 3 icons (`r3/assets/`) | Still true |
| Recall loop (A26) | **New:** native on slide 10; Mermaid `r3/diagrams/recall_loop_r3` kept for the LLD or paper | Mermaid text is ~7 pt at slide width, too small for a TV |
| Group flow with auto-add + pending (A42) | **New** native (slide 11) | — |

## Files
- `../build_deck_r3.py`: the build, including the 18 pt gate. Run `python "docs/Phase III/build/build_deck_r3.py" [out.pptx]`.
- `make_assets.py`: backgrounds `backgrounds/bg_{title,thanks,feature,base,capture,enrich,recall,wellbeing,results}.png` and icons `assets/logo_*.png`. The build calls it every time.
- Visual QA: `render_pptx.ps1` cannot open the template's en-dash file name; render the ASCII-named output. QA was done at 960 px wide to simulate across-the-room reading.
