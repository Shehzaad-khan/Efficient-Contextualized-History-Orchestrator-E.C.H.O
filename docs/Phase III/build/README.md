# Review 2 deck build

Rebuilds `../ECHO_Phase3_Review2.pptx` from the official PES template (`../REVIEW - 2 - …pptx`).
Edit the content in `build_deck.py` and rebuild. Don't hand-edit the output if you plan to rebuild,
because a rebuild overwrites it.

## Files
| File | What it does |
|---|---|
| `build_deck.py` | Builds the deck. Each `slide_*` function is one slide. |
| `approved_content.md` | The content the owner approved, plus facts checked against the code. Use it as the review checklist. |
| `render_pptx.ps1` | Exports every slide to PNG through real PowerPoint (read-only, hidden window). |
| `grid.py` | Tiles those PNGs into contact sheets for a quick visual check. |
| `dump_pptx.py` | Prints each slide's text (stdlib only). |
| `diagrams/*.mmd` | Mermaid sources for the redrawn diagrams; `*.png` are their renders, which the build uses. |
| `diagrams/render_mermaid.py` | Renders a `.mmd` to a cropped PNG with headless Chrome (needs internet access for the Mermaid CDN). |

The deployment diagram is drawn from native PowerPoint shapes in `slide_deployment()`, not Mermaid,
so it can be edited in PowerPoint.

## Rebuild
```powershell
python -m pip install --user python-pptx pymupdf pillow lxml   # one-time
python "docs\Phase III\build\build_deck.py"                   # writes ../ECHO_Phase3_Review2.pptx
python "docs\Phase III\build\build_deck.py" "$env:TEMP\test.pptx"  # or somewhere else (deck open in PowerPoint)
# change a diagram: edit the .mmd, then (width and height are the browser window in px)
python "docs\Phase III\build\diagrams\render_mermaid.py" "docs\Phase III\build\diagrams\seq_recall.mmd" 3000 1500
# visual check
& "docs\Phase III\build\render_pptx.ps1" -PptxPath "<absolute path to deck>" -OutDir "<some temp dir>" -Width 1600
```

## Gotchas
- **A shape with zero or negative size makes PowerPoint refuse the whole file**, even though python-pptx and the
  XML validator accept it. `_check_frame()` raises an error before that can happen, so don't remove it.
- The Phase II class and use-case diagrams are pulled from `docs/Phase II/phase 2 review 1.pdf` by pixel size
  (`pdf_embedded_image`). If that PDF changes, update the sizes.
- Close the deck in PowerPoint before rebuilding. Saving over an open file fails.

# Review 3 deck build

Builds `../ECHO_Phase3_Review3.pptx` from the official Review 3 template (`TEMPLATE` constant in
`build_deck_r3.py`). `build_deck.py` and the Review 2 outputs are untouched. Content, sources, placeholders and the
diagram audit are in `r3/approved_content_r3.md`.

```powershell
python "docs\Phase III\build\build_deck_r3.py"                       # also regenerates r3/backgrounds + r3/assets
python "docs\Phase III\build\build_deck_r3.py" "$env:TEMP\r3.pptx"   # deck open in PowerPoint
```
- Template slides are kept in order; their instruction boxes are removed. Extra section slides clone the section's
  own title and rule (snapshotted before anything is drawn).
- Backgrounds are set as slide backgrounds (`p:bg`), so the master's PES logo stays on top.
- `EMBEDDING_MODEL` and `INCLUDE_GUIDE_APPROVAL_SLIDE` are one-line switches at the top of the script.
- `render_pptx.ps1` cannot open the template's en-dash file name; render the ASCII-named output, or a copy.

# Review 2 LLD document build

Rebuilds `../ECHO_Phase3_Review2_LLD.docx` (Low Level Design & Implementation Document) from the official template
`../UE23CS441A _LOW LEVEL DESIGN & IMPLEMENTATION DOCUMENT.docx`. The template itself is never modified.

| File | What it does |
|---|---|
| `lld/build_lld.py` | All document content — one function per section, `class_block()` per class. Edit here and rebuild. |
| `lld/docx_writer.py` | Fills the template by cloning the template's own headings, bullets and tables, so the format stays the template's. |
| `render_docx.ps1` | Exports a .docx to PDF through real Word; `-OutlinePath` also lists every heading with its page. |
| `lld/diagrams/*.mmd` | Mermaid sources for the 3 level-1 use case and 3 class diagrams (one per module). |
| `lld/diagrams/*.png` | Their renders, plus reused images: Phase II master class + level-0 use case, and the deck's deployment slide. |

The sequence diagrams are the deck's (`diagrams/seq_*.png`), reused as is.

```powershell
python "docs\Phase III\build\lld\build_lld.py"                  # 2 passes: build, ask Word for pages, rebuild the TOC
python "docs\Phase III\build\lld\build_lld.py" --single-pass    # quick build, TOC page column empty
python "docs\Phase III\build\diagrams\render_mermaid.py" "docs\Phase III\build\lld\diagrams\cls_m2_retrieval.mmd" 3200 2400
```

Gotchas:
- The table of contents is a manual table in the template, so its page numbers come from Word (`render_docx.ps1`).
  Always use the two-pass build for the final file; it warns if pages move after the TOC is filled.
- The template footer had the page total typed as "10"; the build replaces it with a NUMPAGES field.
- `TemplateDocument` checks that the template's prototype elements still hold the expected text and stops if the
  template changes. Update `_PROTOTYPES` if you swap in a new template.
- Pseudo-code wider than the text column is reported as a WARNING (Courier 9 pt, about 72 characters).
- Close the document in Word before rebuilding.
