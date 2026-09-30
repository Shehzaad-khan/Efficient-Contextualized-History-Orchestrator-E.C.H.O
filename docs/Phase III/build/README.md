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
