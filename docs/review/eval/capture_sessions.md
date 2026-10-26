# Capture-gate sessions: results (paper "Capture Accuracy"; design doc §6.2, §7.3, §19)

Run 2026-10-26, 18:56 to 19:09 IST, in the test-account Chrome profile with the E.C.H.O extension, backend running.
Expected results are the decided rules in `docs/Timeless_docs/Echo_System_Design.md`, written BEFORE running.
"Actual" comes from the database (`memory_items`, `chrome_metadata`) and `logs/echo.log`, not from the assistant's belief.

**Result: 13 of 13 clean sessions agree with the rules (100 %). Two rows are excluded (C7a, C7b), see the notes.**

| # | Source | Scripted session (page) | Expected | Actual | Agree |
|---|---|---|---|---|---|
| C1 | Chrome | Hash table article, close after about 3 s | NOT STORED, no revisit flag | not stored; no flag in Redis | yes |
| C2 | Chrome | Bloom filter, about 6 s, no scroll, no click | NOT STORED (revisit flag written) | not stored; revisit check logged; flag present | yes |
| C3 | Chrome | Radix tree, about 8 s, scroll to 20 % | STORED | stored (dwell 7 s) | yes |
| C4 | Chrome | Skip list, about 6 s, real mouse click on an in-text link, leave | NOT STORED | not stored | yes |
| C5 | Chrome | B-tree, about 7 s, triple-click to select a paragraph | STORED | stored (dwell 6 s) | yes |
| C6 | Chrome | Merkle tree, 13 s, no scroll | STORED | stored (dwell 10 s at first save) | yes |
| C7a | Chrome | Treap, first visit, about 6 s, no scroll, no click | NOT STORED (flag written) | stored (dwell 7 s) | excluded (owner was using the browser) |
| C7b | Chrome | Treap again, about 6 s | STORED (revisit) | already stored by C7a; dwell rose to 12 s | excluded (not a clean test) |
| C8 | Chrome | `https://example.com/reset?token=abc`, 13 s | NOT STORED | not stored | yes |
| C9 | Chrome | Fenwick tree with `?token=abc`, 13 s | STORED, URL without the token key | stored, URL saved as `.../Fenwick_tree` | yes |
| Y1 | YouTube | Sintel, 3 s, leave | NOT STORED | not stored (no detection logged) | yes |
| Y2 | YouTube | "Me at the zoo" (19 s video, not a Short), watched; rule B | STORED | stored; watch time was 18 s, not 5 s (see notes) | yes |
| Y3a | YouTube | Big Buck Bunny (635 s), first visit, 7 s | NOT STORED | not stored (no detection logged) | yes |
| Y3b | YouTube | Big Buck Bunny again, 7 s | STORED (revisit) | stored (detected on the second visit, watch 7 s) | yes |
| Y4 | YouTube | Sintel (888 s), watched 25 s | STORED | stored (watch 25 s) | yes |

Chrome rows: 8 of 8 clean rows agree. YouTube rows: 5 of 5 agree. The revisit rule is tested cleanly only by Y3a/Y3b (YouTube).

## Notes and deviations (state these honestly in the paper)
- **C7a and C7b are excluded.** The owner states he was interacting with the browser during C7a, so real clicks or movement counted as intent; the page was stored for that reason and the session is not a clean test (not a gate bug). C7b then re-visited a page that was already stored, so it cannot show that the revisit rule saves a new page.
- **Synthetic user.** The extension counts time only while Chrome is the focused window and the user is not idle for 30 s. The owner was away, so a helper script sent a 1 pixel mouse move every 5 s and blocked sleep. The pages and clicks were driven through the browser-automation tools; durations are accurate to about 2 s (tool latency).
- **One tab only.** Tabs opened by the automation tool were ignored by the extension, so every session ran in the same single tab, parked on `mail.google.com` (always discarded by the server, reason `gmail_web_excluded`) between sessions.
- **Y2** used a 19 s regular video, not the 30 s Short of the original row, and it played about 18 s because of tool latency between steps. At 18 s it is below the 20 s regular-video rule, so the completion rule (watch of at least 5 s and at least 10 % of the length) is the one that stored it.
- **Y1 and Y4** used the same video (Sintel); Y1 (3 s) writes no flag by design, so Y4 does not depend on it.
- **Side effects in the test data:** 8 new items stored (Chrome: Merkle tree, Radix tree, Treap, B-tree, Fenwick tree; YouTube: Me at the zoo, Big Buck Bunny, Sintel), now 127 items in all. One stray page (`example.org`, stored while the tab was parked there during setup) was soft-deleted (`is_deleted = TRUE`).
- Four log lines `Chrome page discarded: reason=intent_not_confirmed domain=en.wikipedia.org` appeared shortly after stored pages; they did not create rows and were not explained.
