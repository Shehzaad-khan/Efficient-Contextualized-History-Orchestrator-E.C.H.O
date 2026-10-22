# Draft 4 (his vocabulary): notes

Draft 4 is Draft 3 with the prose reworded a second time. Draft 3 fixed his rhythm. Draft 4 changes his word
choice and the way each argument is built. Nothing else changed: the facts, numbers, citations, labels,
placeholders, floats, equations, comments and bibliography are the same as in Draft 3 (Section 5).

- **Build:** `latexmk -pdf main.tex` gives **8 pages, 0 errors** (Draft 3 was 8 pages too). The log has no
  LaTeX warnings.
- **Pages:** I checked all 8 pages as images. Two layout fixes were needed (Section 6).
- **Files:**
  - `VOCAB_MAP.md` has the vocabulary map with frequencies.
  - Scratch scripts and raw outputs are in `C:\Users\mdshe\AppData\Local\Temp\echo_agents\draft4\`, not in
    the repo. `build_d4.py` regenerates `main.tex` from Draft 3, and `pass2.py` to `pass5.py` are the
    revision passes.

## 1. What ran

| Step | Ran? | Detail |
|---|---|---|
| Own topic and style search | **Ran** on all 64 Draft 3 prose paragraphs | Cosine over his 601 explanatory messages, 0.6 × MiniLM topic z-score (`embeddings.npy`) + 0.4 × Wegmann style z-score (`wegmann_vectors.npy`). Top 8 kept per paragraph (`retrieval_d3.json`). |
| App Stage 3 `retrieve(use_cache=False)` | **Ran** on all 64 paragraphs | 5-signal fusion, top 5 kept (`retrieval_d3_s3.json`). 0 errors and no cache writes. |
| Stage 1 features, explanatory subset | Reused from Draft 3 | `his_indices.json`: 1,725 kept messages, of which 601 are explanatory. |
| Stage 4/5 LLM rewrite | **Not run** | Draft 3 already ran Stage 5 on one paragraph, and its output was chat register. The brief made this optional, so I did the whole rewrite myself. |
| Stage 6 / AI detection | **Not run, on purpose** | As the brief says. |

- `.env`, keys and tokens were never opened.
- Nothing was written into the style project, the app code, or Drafts 1 to 3.

## 2. Which messages anchored each section

- **"Own"** means my topic-and-style search. The z-score shown is the best one for that message within the
  section.
- **"Stage 3"** means the app's hybrid score.
- The message ids are the corpus `id` field.

| Section | Paragraphs | Own search (most frequent top 5) | Stage 3 (most frequent top 5) |
|---|---|---|---|
| Abstract + keywords | 2 | 683 (z 2.89), 1052 (z 2.81), 684 (z 2.41), 1749 (z 2.11) | 755 (0.840), 1052 (0.724), 368 (0.681) |
| I Introduction | 8 | 683 (z 3.15), 684 (z 2.61), 1052 (z 3.08), 1017 (z 2.43) | 755 (0.711), 852 (0.718), 750 (0.718) |
| II Related Work | 5 | 847 (z 2.67), 1014 (z 2.62), 684 (z 2.53), 1125 (z 1.92) | 2004 (0.673), 1014 (0.857), 684 (0.644) |
| III System Overview | 3 | 78 (z 2.77), 127 (z 3.04), 1052 (z 2.65), 822 (z 2.35) | 78 (0.752), 750 (0.724), 92 (0.719) |
| IV.A Capture | 4 | 1052 (z 2.83), 683 (z 3.10), 164 (z 2.23), 1906 (z 1.95) | 753 (0.643), 683 (0.703), 1052 (0.786) |
| IV.B Storage | 3 | 1052 (z 2.92), 127 (z 2.45), 1740 (z 2.05), 683 (z 2.02) | 852 (0.641), 1052 (0.832), 77 (0.762) |
| IV.C Enrichment | 6 | 683 (z 2.56), 1630 (z 2.30), 1055 (z 2.51), 1949 (z 2.18) | 384 (0.690), 1058 (0.626), 94 (0.678) |
| IV.D to IV.E Retrieval, Conversation | 10 | 1795 (z 2.28), 684 (z 2.69), 309 (z 2.93), 1749 (z 2.58) | 859 (0.716), 33 (0.724), 309 (0.804) |
| V Wellbeing | 7 | 717 (z 3.41), 1749 (z 2.80), 1052 (z 2.66), 1023 (z 2.43) | 2004 (0.797), 2005 (0.723), 717 (0.703) |
| VI Privacy | 4 | 683 (z 2.73), 684 (z 2.60), 1052 (z 2.57), 1795 (z 2.26) | 1014 (0.696), 683 (0.700), 1052 (0.710) |
| VII Implementation | 2 | 127 (z 2.64), 78 (z 2.23), 72 (z 2.20), 762 (z 2.06) | 760 (0.672), 78 (0.642), 762 (0.628) |
| VIII Evaluation | 7 | 1014 (z 2.68), 356 (z 2.13), 1831 (z 1.95), 1977 (z 1.82) | 1370 (0.669), 1052 (0.669), 1495 (0.671) |
| IX Limitations, Future Work | 2 | 1052 (z 2.66), 1125 (z 2.41), 683 (z 3.11), 1740 (z 2.50) | 1052 (0.842), 1056 (0.685), 683 (0.679) |
| X Conclusion | 1 | 1125 (z 2.45), 1052 (z 2.42), 1737 (z 2.12), 684 (z 2.09) | 1052 (0.849), 1055 (0.695), 1014 (0.695) |

**Most useful source, found in this pass.** Messages 1729 to 1754 are his own Phase I report, dictated as
written prose, so they are his closest register to a paper. Examples:

- "We found out that there is no real solution that actually tackles this problem" (1742).
- "While many other upcoming solutions like Comet and Atlas try to do it, they are not unifying it like Echo. Instead, they are able to fetch one browsing history at a time." (1740).

Together with his Phase 2 review speech (683, 684), these set the report register. His explanation messages set
the connector rates.

## 3. Four before/after samples

### Introduction, opening paragraph

*Anchors:*

- Own search: 683 (z 3.15), 1052 (z 3.09), 1740 (z 2.89), 684 (z 2.32).
- Stage 3: 750 (0.707), 1056 (0.699).
- Applied: the *while … instead* contrast from 1740, including "one … at a time". The punchline "That is three
  apps and three search boxes" is gone.

**Draft 3**

> Suppose a student is preparing for a job interview. The company sends an email, and over the following days the student reads articles about operating systems in Chrome and watches a few tutorials on YouTube. A week later, they want that material back. Now the email sits in Gmail, the articles are buried in the browser history, and the videos are in the YouTube watch list. That is three apps and three search boxes, and none of them knows about the other two. So the student either searches all three or starts again from scratch.

**Draft 4**

> Suppose a student is preparing for a job interview. The company sends an email, and over the following days the student reads articles about operating systems in Chrome and watches a few tutorials on YouTube. A week later, the student wants that material back. Now the email sits in Gmail and the videos sit in the YouTube watch list, while the articles are buried in the browser history. Each of these apps has its own search box, and none of them knows about the other two. So the student either searches all three, one at a time, or starts again from scratch.

### Methodology, IV.A Gmail paragraph

*Anchors:*

- Own search: 683 (z 3.10), 1949 (z 2.42), 684 (z 2.28).
- Stage 3: 1055 (0.844), 683 (0.703).
- Applied: reason before decision, as in 89 and 683. Each reason gets its own sentence, then "So …". *also* and
  *already* are used at his rates. Draft 3's two uses of *instead* and one *never* are gone.

**Draft 3**

> Gmail is treated differently from the other two sources. Mail arrives on its own instead of being chosen by the user, the daily volume is small, and even an unread message may matter later. So every message is stored when it arrives. The connector polls every 30 seconds, but each poll asks Gmail only for the changes since a saved history position, instead of listing the entire mailbox. If that position has expired, the connector lists the mailbox once, and the uniqueness rule on stored items absorbs the overlap. Mail that Gmail has filed as spam or trash is never read, so Gmail's own spam filter acts as the spam filter of E.C.H.O. When the user opens a message in Gmail on the web, the extension reports the message id and the seconds spent reading, and nothing else.

**Draft 4**

> Gmail is treated differently from the other two sources. Mail arrives on its own and is not chosen by the user. Also, the daily volume is small, and even an unread message may matter later. So every message is stored when it arrives. The connector polls every 30 seconds, but each poll asks Gmail only for the changes since a saved history position. It does not list the entire mailbox each time. If that position has expired, the connector lists the mailbox once, and the uniqueness rule on stored items absorbs the overlap. Mail that Gmail has already filed as spam or trash is not read at all, so Gmail's own spam filter also acts as the spam filter of E.C.H.O. When the user opens a message in Gmail on the web, the extension reports the message id and the seconds spent reading, and nothing else.

### Wellbeing, V Sessions

*Anchors:*

- Own search: 1052 (z 2.42), 1603 (z 2.41), 1630 (z 2.24).
- Stage 3: 49 (0.741), 1052 (0.695).
- Applied: the problem is stated first, then turned into a concrete "suppose" case, as in 684 and 1587. The
  numbers are unchanged: the two 15s and "five minutes".

**Draft 3**

> \textit{Sessions.} A session is a run of activity in which consecutive items are less than 20 minutes apart. The architecture first proposed five minutes. But each item is recorded at one point in time, so a 15 minute read followed by the next page looked like a 15 minute gap, and one sitting was split in two. For each session, the module records its length, the system group with the most time, and how often the user switched between sources.

**Draft 4**

> \textit{Sessions.} A session is a run of activity in which consecutive items are less than 20 minutes apart. The architecture first proposed five minutes. But suppose the user reads a page for 15 minutes and then opens the next page. As each item is recorded at one point in time, that read looks like a 15 minute gap. So the earlier rule would split one sitting in two. For each session, the module records its length, the system group with the most time, and how often the user switched between sources.

### Conclusion

*Anchors:*

- Stage 3: 1052, his own feature walkthrough of E.C.H.O (0.849).
- Own search: 1125 (z 2.45), 1052 (z 2.42), 1737 (z 2.12).
- Applied: the three-adjective close "visible, bounded, and cheap" became a plain *So* sentence, and "Around
  this loop" became his "Apart from".

**Draft 3**

> E.C.H.O turns email, browsing, and video history into one personal memory that can be searched in plain language. It keeps all legitimate mail, but only the pages and videos the user paid attention to. It stores them on the user's laptop and prepares them for search in the background. Questions are answered by a loop in which a language model plans the search and deterministic code performs it. This keeps the role of the model visible, bounded, and cheap. Around this loop, a wellbeing module describes how time is spent and keeps the user in charge of every personal grouping. The next version of this paper will report the measurements laid out above.

**Draft 4**

> E.C.H.O turns email, browsing, and video history into one personal memory that can be searched in plain language. It keeps all legitimate mail, but only the pages and videos that the user paid attention to. These items are stored on the user's laptop and prepared for search in the background. Questions are answered by a loop in which a language model plans the search and deterministic code performs it. So the role of the model stays visible and bounded, and its cost stays low. Apart from retrieval, a wellbeing module describes how time is spent and keeps the user in charge of every personal grouping. The next version of this paper will report the measurements laid out above.

## 4. Metrics: his corpus, Draft 3, Draft 4

**How the numbers are counted.**

- All counts come from one script (`compare.py`, with `metrics.py`).
- "His" is the 601-message explanatory subset (2,180 sentences). Draft 3 has 349 prose sentences and Draft 4
  has 369.
- Prose means the abstract through the conclusion. Floats, equations and comments are removed, and
  `\item` text is kept.
- Rates are per 100 sentences. The count is in brackets.
- This sentence splitter gives Draft 3 a mean of 16.7, against 17.1 with the Draft 3 agent's splitter. Both are
  the same text.

**Sentence length**

| Measure | His | Draft 3 | Draft 4 |
|---|---|---|---|
| Mean sentence length (words) | 15.7 | 16.7 | **16.4** |
| Median | 14 | 16 | 16 |
| Sentences of 10 words or fewer | 37.2% | 22.9% | 20.3% |
| Sentences of 25 words or more | 16.7% | 14.6% | 10.0% |

**Connectors (per 100 sentences)**

| Connector | His | Draft 3 | Draft 4 |
|---|---|---|---|
| so | 14.77 | 10.89 (38) | 10.30 (38) |
| so that | 2.48 | 1.43 (5) | 2.17 (8) |
| such that | 1.28 | 0.29 (1) | 1.08 (4) |
| but | 7.20 | 3.44 (12) | 4.07 (15) |
| also | 6.83 | 2.01 (7) | 5.42 (20) |
| then | 5.50 | 2.01 (7) | 5.15 (19) |
| first | 4.13 | 4.30 (15) | 4.61 (17) |
| next | 2.57 | 1.43 (5) | 2.44 (9) |
| while | 3.39 | 2.58 (9) | 3.25 (12) |
| instead | 0.73 | 2.58 (9) | 1.08 (4) |
| , meaning / meaning | 1.06 | 2.29 (8) | 1.63 (6) |
| because | 0.78 | 1.15 (4) | 0.81 (3) |
| since | 0.87 | 1.43 (5) | 0.81 (3) |
| already | 1.51 | 0.86 (3) | 1.36 (5) |
| directly | 0.83 | 0.00 (0) | 1.08 (4) |
| particular | 0.73 | 0.00 (0) | 0.81 (3) |
| this way | 0.05 | 0.57 (2) | 0.54 (2) |

**Sentence openers (per 100 sentences)**

| Opener | His | Draft 3 | Draft 4 |
|---|---|---|---|
| So | 3.35 | 2.29 | 3.52 |
| Also | 1.51 | 0.00 | 1.63 |
| Next | 1.28 | 0.00 | 1.08 |
| First | 0.96 | 0.29 | 0.81 |
| Then | 0.69 | 0.29 | 0.81 |
| While | 0.83 | 0.29 | 0.54 |
| As (reason first) | 1.47 | 0.00 | 1.36 |
| Now | 1.15 | 0.29 | 0.27 |
| For | 1.24 | 3.15 | 2.98 |

**His top 15 signature words (per 100 sentences)**

These are the 15 most distinctive words in `vocab_profile.json` after removing topic nouns, verbs aimed at an
assistant (give, add, make, tell, explain) and chat markers (right, guess, basically, hey).

| Word | His | Draft 3 | Draft 4 |
|---|---|---|---|
| complete | 3.72 | 0.00 (0) | 1.08 (4) |
| first | 4.13 | 4.30 (15) | 4.61 (17) |
| better | 2.34 | 0.00 (0) | 0.27 (1) |
| meaning | 1.06 | 2.29 (8) | 1.63 (6) |
| simple | 1.51 | 0.00 (0) | 0.27 (1) |
| again | 1.51 | 1.72 (6) | 1.63 (6) |
| best | 1.47 | 1.15 (4) | 1.36 (5) |
| entire | 1.47 | 0.29 (1) | 0.81 (3) |
| exact | 1.15 | 0.29 (1) | 0.54 (2) |
| exactly | 0.92 | 1.15 (4) | 1.08 (4) |
| suppose | 1.15 | 0.57 (2) | 0.81 (3) |
| single | 1.01 | 2.29 (8) | 2.17 (8) |
| main | 0.69 | 1.15 (4) | 1.08 (4) |
| later | 1.10 | 1.15 (4) | 1.63 (6) |
| apart | 0.96 | 0.86 (3) | 1.36 (5) |

**Model habits and chat markers (counts)**

| Pattern | His (2,180 sentences) | Draft 3 | Draft 4 |
|---|---|---|---|
| "never" | 3 | 22 | 3 (one is the fixed heading "What is never captured") |
| Anaphoric tricolons (the same non-article word opening three parallel items, e.g. "never X, never Y, and never Z") | 4 | 3 | 0 |
| Aphoristic one-liners (11 phrases found in Draft 3, listed in `compare.py`) | 0 | 11 | 0 |
| Furthermore / Moreover / Additionally / In essence / Notably / crucial / robust / seamless / leverage / delve | 0 (crucial 1) | 0 | 0 |
| Semicolons | 62 | 0 | 0 |
| right? / basically / just / and all | 25 / 9 / 145 / 27 | 0 | 0 |
| Contractions | 377 | 2 | 2 (both in the name "Stuff I've Seen") |
| Post-positional "only" | 26 | 4 | 1 (the term "read only") |
| Question marks in prose | 270 | 5 | 5 (same three signpost questions plus two quoted user questions) |

**Where Draft 4 is below his rate, and why I left it there.**

- *complete*, *better* and *simple* are mostly used in his chat instructions, such as "give the complete
  document", "better results" and "in simple terms".
- In the paper, "better" would add comparative claims that Draft 2 does not make. So it appears once, in the
  signature trade-off.
- *so* and *but* stay below him for the same reason: part of his use is chat ("so basically", "but yeah").

**Above his rate.** *single*, *main*, *For* openers and *such as* are higher than his. This comes from
the paper's content: per-source lists, "single transaction" and "single user". Changing those would mean
changing terms.

## 5. Invariant diff (Draft 3 against Draft 4)

`check_invariants_d4.py` is the Draft 3 checker pointed at the new pair. Output (left column = Draft 3, right =
Draft 4):

```
lines changed: D3 62, D4 64 (expected: 64 prose lines + 2 added header comment lines (+ any layout line))
\cite keys                                 D3=  23 D4=  23  SAME
\ref targets                               D3=  16 D4=  16  SAME
\label names                               D3=  16 D4=  16  SAME
\tbd{...} placeholders                     D3=  13 D4=  13  SAME
[TBD] cells                                D3=  10 D4=  10  SAME
\embmodel uses                             D3=   2 D4=   2  SAME
\embdim uses                               D3=   1 D4=   1  SAME
\rerankmodel uses                          D3=   2 D4=   2  SAME
\llmname uses                              D3=   2 D4=   2  SAME
inline math $...$                          D3=  16 D4=  16  SAME
equation blocks                            D3=   2 D4=   2  SAME
figure blocks                              D3=   2 D4=   2  SAME
table blocks                               D3=   3 D4=   3  SAME
bibliography block                         D3=   1 D4=   1  SAME
preamble (to \begin{document})             D3=   1 D4=   1  SAME
comment lines (incl. % TODO / % VERIFY)    D3= 110 D4= 112  removed=0 added=2
    added: %  Draft 4 (his vocabulary), 21 Oct 2026: same content as Draft 3, word choice
    added: %  and argument shape moved to his own corpus (see VOCAB_MAP.md, NOTES.md).
% TODO / % VERIFY lines                    D3=   6 D4=   6  SAME
section/subsection headings                D3=  27 D4=  27  SAME
numerals in prose                          D3=  41 D4=  41  SAME
number words in prose                      D3= 174 D4= 171  DIFF (soft)
    only in D3: {'one': 2, 'seventh': 1, 'three': 4}
    only in D4: {'six': 1, 'first': 2, 'second': 1}
quoted phrases ``...''                     D3=   9 D4=   9  SAME
\texttt{...}                               D3=   1 D4=   1  SAME
named systems/tools in prose               D3= 116 D4= 117  DIFF (soft)
    only in D3: {}
    only in D4: {'PostgreSQL': 1}
hyphens/dashes in prose (style rule)       D3=   0 D4=   0
RESULT: PASS
```

**Identical sets.** All of these match Draft 3 exactly:

- citations, refs and labels
- the 13 `\tbd{}` placeholders and the 10 `[TBD]` cells
- every macro use, including `\embmodel`
- inline math, both equations, both figures, all three tables, the bibliography and the preamble
- the section headings and the 41 numerals in prose
- the 9 quoted phrases
- all 110 comment lines, including every `% TODO` and `% VERIFY`

**Added.** There are 2 header comment lines after line 1.

**One layout line changed: `\IEEEtriggeratref{7}` became `{5}`.**

- The comment above it says to adjust it, and Draft 3 changed it too.
- It only balances the two columns on page 8.

**The soft number-word difference is wording only. Each change keeps the fact:**

- "the code refuses a seventh" became "the code allows at most six". The cap of six calls is unchanged.
- The punchline "That is three apps and three search boxes" became "Each of these apps has its own search box".
- "a retention setting offers three choices: keep forever, …" became "lets the user keep items forever, for one
  year, or for six months". The same three options remain.
- "All three run on the user's laptop" became "This entire storage layer runs on the user's laptop".
- "has one job" became "each has a separate job".
- "one rule set per source" became "a separate rule set for each source".
- "First, … Second, …" was added to "Two kinds of traffic leave the machine".
- "First, data comes in through three connectors" was added in Section III.

**The added name.** The extra "PostgreSQL" is "the FAISS index can be rebuilt from PostgreSQL" (Draft 3 had
"from it").

**Meaning check.** I reread every changed paragraph against Draft 2, sentence by sentence. Five places were
corrected back toward Draft 2:

1. **Abstract.**
   - "such as the inbox, the browser, and video sites" went back to Draft 2's colon list.
   - "When the results look weak, it widens the search, and it then reorders…" could read as reranking only weak
     results. It is now "It widens the search when the results look weak, and it reorders the survivors…",
     because reranking always happens.
2. **Introduction.** "A search box finds an item only when it is given the right keyword" was stronger than
   Draft 2. It went back to "A search box needs the right keyword".
3. **Digital wellbeing tools.** "Instead of working toward such a goal" clashed with E.C.H.O's own personal
   goals (user groups). It is now "E.C.H.O is different from these tools", followed by Draft 2's three facts.
4. **Quality check.** The five rules are now in three sentences. I counted that all five are still present and
   the 0.35 cutoff is unchanged.
5. **Sessions.** The "suppose" case keeps the same reading as Draft 2: the earlier five-minute rule would split
   one sitting in two.

**One place states an implied trade-off openly.** In Cleaning, "as a little noise is better than lost body
text" puts into words what Draft 2's two facts imply: six of eight messages lost real text, while a signature
only adds a little noise. This is also the reason recorded with decision A12. If a reviewer reads it as a new
claim, delete that clause.

## 6. Layout

**Fig. 2 moved.**

- In Draft 3, Fig. 2 sat on page 4. The slightly longer Draft 4 text first pushed it to page 5, which left a
  stretched white gap before IV.D on page 4.
- I removed that gap by trimming wording only (`pass5.py`). Examples: "In the case of web pages" became "For
  web pages", and "without rebuilding the index" became "without a rebuild".
- Fig. 2 now sits at the top of page 5, and page 4 is full.

**Page 8 columns are now balanced** (the `\IEEEtriggeratref` change).

I checked every page as an image. There is no overflow, no missing reference and no `??`. Both tables and both
figures are intact.

## 7. Risks and open points

**Measured against chat, not academic writing.** His voice is measured on chat messages to AI assistants, with
no academic writing in them.

- Phase I report messages 1729 to 1754 are the closest register, and I leaned on them.
- If a venue finds sentence openers like "So" and "Also" too plain, a reviewer will notice them in Sections IV
  and VI. Each is a one-word change.

**Signature words are below his rate.** *complete*, *better* and *simple* stay well below his rate on purpose.
Raising them would mean either chat phrasing or new comparative claims.

**Not changed in this pass.** As the coordinator asked, I made no content changes. These newly decided items are
still to be applied in a separate pass:

- the embedding model name (`\embmodel` still reads granite)
- A16 and A42
- MCP
- `store=true`

Some Draft 4 sentences describe those exact spots:

- *Embedding*, the `\tbd{token budget, decision A16}` sentence.
- *User groups*, "No suggestion is applied on its own…". A42 now adds an auto-accept path, so this sentence
  must change.
- *Future work*, the auto-accept and MCP sentences.
- *Conversation memory*, the store=true sentence.
