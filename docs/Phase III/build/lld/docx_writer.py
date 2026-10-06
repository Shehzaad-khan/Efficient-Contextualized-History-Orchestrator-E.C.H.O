"""Low-level writer that fills the PES LLD template by cloning the template's own paragraphs and tables.

Every new heading, paragraph, bullet and table is a deep copy of an element that already exists in the
template, so fonts, numbering and spacing stay exactly the template's. Only text, widths and images change.
"""
from __future__ import annotations

import copy
import pathlib
import re
from dataclasses import dataclass, field

import docx
from docx.oxml.ns import qn
from docx.shared import Inches
from docx.text.paragraph import Paragraph
from lxml import etree

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
W14_NS = "http://schemas.microsoft.com/office/word/2010/wordml"
CODE_FONT = "Courier New"
BODY_SIZE = 24          # half-points: 12 pt, the template's body size
TABLE_BODY_SIZE = 22    # 11 pt inside table bodies
CODE_SIZE = 18          # 9 pt for pseudo-code
CAPTION_SIZE = 20       # 10 pt
TEXT_WIDTH_DXA = 9360   # 8.5 in page − 2 × 1 in margins
CONTENT_INDENT = 1224   # indent the template uses for text under level-3/4 headings
SECTION_INDENT = 720    # Normal style indent

# Inline markup accepted in every text string: **bold** and `code`.
_MARKUP = re.compile(r"(\*\*.+?\*\*|`.+?`)")


def w(tag: str) -> str:
    return qn(f"w:{tag}")


@dataclass
class TocEntry:
    level: int          # 1–4 for numbered headings, 0 for appendix headings
    number: str         # "3.2.3.1" — empty for level 1 (the TOC numbers those itself) and appendices
    title: str


@dataclass
class HeadingCounter:
    counts: list[int] = field(default_factory=lambda: [0, 0, 0, 0])

    def next(self, level: int) -> str:
        self.counts[level - 1] += 1
        for deeper in range(level, 4):
            self.counts[deeper] = 0
        return ".".join(str(c) for c in self.counts[:level])


class TemplateDocument:
    """Opens the template, keeps prototype elements, and appends content before the final sectPr."""

    # Body element indexes in the untouched template, checked against their text so a changed
    # template fails loudly instead of silently cloning the wrong element.
    _PROTOTYPES = {
        "h1": (41, "Introduction"),
        "h2": (45, "Purpose"),
        "h3": (68, "Use Case Diagram"),
        "h4": (111, "Class Name 1"),
        "body": (43, "[This section provides the overview of the low-level design.]"),
        "bullet": (123, "Purpose"),
        "subhead": (209, "4.1 Algorithm and Pseudocode"),
        "appendix": (221, "Appendix A:  Definitions, Acronyms and Abbreviations"),
        "table": (118, None),
        "change_table": (228, None),
        "trace_table": (232, None),
    }
    _FIRST_REPLACED = 41       # "Introduction" — everything from here on is regenerated
    _TOC_FILLER = range(6, 38)  # blank paragraphs that push the Note onto a new page

    def __init__(self, template: pathlib.Path) -> None:
        self.doc = docx.Document(str(template))
        self.body = self.doc.element.body
        elements = list(self.body.iterchildren())
        self.proto: dict[str, etree._Element] = {}
        for name, (index, expected) in self._PROTOTYPES.items():
            element = elements[index]
            if expected is not None:
                actual = self._text(element)
                if actual.strip() != expected.strip():
                    raise ValueError(f"Template changed: element {index} is {actual!r}, expected {expected!r}")
            self.proto[name] = copy.deepcopy(element)
        self.cover_table = elements[2]
        self.toc_table = elements[5]
        self._replace_toc_filler(elements)
        self.sect_pr = elements[-1]
        if self.sect_pr.tag != w("sectPr"):
            raise ValueError("Template changed: last body element is not sectPr")
        for element in elements[self._FIRST_REPLACED:-1]:
            self.body.remove(element)
        self.counter = HeadingCounter()
        self.toc: list[TocEntry] = []
        self.figure_count = 0
        self.warnings: list[str] = []

    # ── text helpers ──────────────────────────────────────────────────────────
    @staticmethod
    def _text(element: etree._Element) -> str:
        return "".join(t.text or "" for t in element.iter(w("t")))

    @staticmethod
    def _clean_clone(element: etree._Element) -> etree._Element:
        clone = copy.deepcopy(element)
        for tag in ("bookmarkStart", "bookmarkEnd", "proofErr", "lastRenderedPageBreak"):
            for node in list(clone.iter(w(tag))):
                node.getparent().remove(node)
        # Duplicate w14:paraId values make Word repair the file — drop them from clones.
        for node in clone.iter():
            for attr in (f"{{{W14_NS}}}paraId", f"{{{W14_NS}}}textId"):
                node.attrib.pop(attr, None)
        return clone

    def _replace_toc_filler(self, elements: list[etree._Element]) -> None:
        filler = [elements[i] for i in self._TOC_FILLER]
        if any(self._text(p).strip() for p in filler):
            raise ValueError("Template changed: expected only blank paragraphs after the TOC")
        page_break = self._clean_clone(filler[0])
        for run in page_break.findall(w("r")):
            page_break.remove(run)
        run = etree.SubElement(page_break, w("r"))
        etree.SubElement(run, w("br")).set(w("type"), "page")
        filler[0].addprevious(page_break)
        for p in filler:
            self.body.remove(p)

    @staticmethod
    def _base_rpr(paragraph: etree._Element) -> etree._Element:
        run = paragraph.find(w("r"))
        rpr = run.find(w("rPr")) if run is not None else None
        if rpr is None:
            ppr = paragraph.find(w("pPr"))
            rpr = ppr.find(w("rPr")) if ppr is not None else None
        return copy.deepcopy(rpr) if rpr is not None else etree.Element(w("rPr"))

    @staticmethod
    def _set_flag(rpr: etree._Element, tag: str, on: bool) -> None:
        for node in rpr.findall(w(tag)):
            rpr.remove(node)
        if on:
            flag = etree.Element(w(tag))
            # rPr children have a schema order: rStyle, rFonts, b, bCs, i, iCs, ... — insert after the
            # last of those that precede this flag.
            before = {"b": ("rStyle", "rFonts"), "i": ("rStyle", "rFonts", "b", "bCs")}[tag]
            anchor = None
            for child in rpr:
                if etree.QName(child).localname in before:
                    anchor = child
            if anchor is not None:
                anchor.addnext(flag)
            else:
                rpr.insert(0, flag)

    @staticmethod
    def _set_size(rpr: etree._Element, half_points: int) -> None:
        for tag in ("sz", "szCs"):
            node = rpr.find(w(tag))
            if node is None:
                node = etree.SubElement(rpr, w(tag))
            node.set(w("val"), str(half_points))
        # keep sz before szCs and after b/i (schema order); re-append in order
        sz, szcs = rpr.find(w("sz")), rpr.find(w("szCs"))
        rpr.remove(sz); rpr.remove(szcs)
        anchor_tags = ("vertAlign", "u", "effect", "highlight", "shd", "fitText", "lang", "eastAsianLayout", "specVanish", "oMath")
        anchor = next((n for n in rpr if etree.QName(n).localname in anchor_tags), None)
        if anchor is not None:
            anchor.addprevious(sz); anchor.addprevious(szcs)
        else:
            rpr.append(sz); rpr.append(szcs)

    @staticmethod
    def _set_font(rpr: etree._Element, font: str) -> None:
        fonts = rpr.find(w("rFonts"))
        if fonts is None:
            fonts = etree.Element(w("rFonts"))
            rpr.insert(0, fonts)
        for attr in ("ascii", "hAnsi", "cs"):
            fonts.set(w(attr), font)

    def _fill(self, paragraph: etree._Element, text: str, *, bold: bool | None = None, italic: bool = False,
              size: int | None = None, font: str | None = None) -> etree._Element:
        """Replace the paragraph's runs with runs built from `text` (supports **bold** and `code`)."""
        base = self._base_rpr(paragraph)
        if size is not None:
            self._set_size(base, size)
        if font is not None:
            self._set_font(base, font)
        for child in list(paragraph):
            if child.tag != w("pPr"):
                paragraph.remove(child)
        for piece in _MARKUP.split(text):
            if not piece:
                continue
            rpr = copy.deepcopy(base)
            if piece.startswith("**") and piece.endswith("**"):
                piece, piece_bold, piece_code = piece[2:-2], True, False
            elif piece.startswith("`") and piece.endswith("`"):
                piece, piece_bold, piece_code = piece[1:-1], bool(bold), True
            else:
                piece_bold, piece_code = bool(bold), False
            if bold is not None or piece_bold:
                self._set_flag(rpr, "b", piece_bold)
            if italic:
                self._set_flag(rpr, "i", True)
            if piece_code:
                self._set_font(rpr, CODE_FONT)
            run = etree.SubElement(paragraph, w("r"))
            run.append(rpr)
            t = etree.SubElement(run, w("t"))
            t.text = piece
            t.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
        return paragraph

    @staticmethod
    def _ppr(paragraph: etree._Element) -> etree._Element:
        ppr = paragraph.find(w("pPr"))
        if ppr is None:
            ppr = etree.Element(w("pPr"))
            paragraph.insert(0, ppr)
        return ppr

    def _set_ppr_child(self, paragraph: etree._Element, tag: str, attrs: dict[str, str]) -> None:
        """Set a pPr child (ind, jc, spacing, keepNext) respecting the schema order of pPr children."""
        order = ["pStyle", "keepNext", "keepLines", "pageBreakBefore", "framePr", "widowControl", "numPr",
                 "suppressLineNumbers", "pBdr", "shd", "tabs", "suppressAutoHyphens", "kinsoku", "wordWrap",
                 "overflowPunct", "topLinePunct", "autoSpaceDE", "autoSpaceDN", "bidi", "adjustRightInd",
                 "snapToGrid", "spacing", "ind", "contextualSpacing", "mirrorIndents", "suppressOverlap", "jc",
                 "textDirection", "textAlignment", "textboxTightWrap", "outlineLvl", "divId", "cnfStyle", "rPr",
                 "sectPr", "pPrChange"]
        ppr = self._ppr(paragraph)
        existing = ppr.find(w(tag))
        if existing is not None:
            ppr.remove(existing)
        node = etree.Element(w(tag))
        for key, value in attrs.items():
            node.set(w(key), value)
        rank = order.index(tag)
        for child in ppr:
            name = etree.QName(child).localname
            if name in order and order.index(name) > rank:
                child.addprevious(node)
                return
        ppr.append(node)

    def _append(self, element: etree._Element) -> etree._Element:
        self.sect_pr.addprevious(element)
        return element

    # ── headings ──────────────────────────────────────────────────────────────
    def _heading(self, proto: str, level: int, title: str, *, toc: bool) -> etree._Element:
        p = self._clean_clone(self.proto[proto])
        num_pr = p.find(w("pPr")).find(w("numPr"))
        num_pr.find(w("ilvl")).set(w("val"), str(level - 1))
        num_pr.find(w("numId")).set(w("val"), "1")   # one list for every numbered heading
        self._set_ppr_child(p, "keepNext", {})
        self._fill(p, title)
        number = self.counter.next(level)
        if toc:
            self.toc.append(TocEntry(level, number if level > 1 else "", title))
        return self._append(p)

    def h1(self, title: str) -> None:
        self._heading("h1", 1, title, toc=True)

    def h2(self, title: str) -> None:
        self._heading("h2", 2, title, toc=True)

    def h3(self, title: str) -> None:
        self._heading("h3", 3, title, toc=True)

    def h4(self, title: str, *, toc: bool = False) -> None:
        self._heading("h4", 4, title, toc=toc)

    def appendix(self, title: str) -> None:
        p = self._clean_clone(self.proto["appendix"])
        self._set_ppr_child(p, "keepNext", {})
        self._fill(p, title)
        self.toc.append(TocEntry(0, "", title))
        self._append(p)

    def subhead(self, title: str, indent: int = SECTION_INDENT) -> None:
        p = self._clean_clone(self.proto["subhead"])
        self._set_ppr_child(p, "keepNext", {})
        self._set_ppr_child(p, "spacing", {"before": "160", "after": "60"})
        self._set_ppr_child(p, "ind", {"left": str(indent)})
        self._fill(p, title, bold=True)
        self._append(p)

    # ── body content ──────────────────────────────────────────────────────────
    def para(self, text: str, indent: int = SECTION_INDENT, *, keep_next: bool = False) -> None:
        p = self._clean_clone(self.proto["body"])
        self._set_ppr_child(p, "ind", {"left": str(indent)})
        if keep_next:
            self._set_ppr_child(p, "keepNext", {})
            self._set_ppr_child(p, "keepLines", {})
        self._fill(p, text)
        self._append(p)

    def bullets(self, items: list[str], indent: int | None = None, *, align_left: bool = False,
                keep_last_with_next: bool = False) -> None:
        for index, item in enumerate(items):
            p = self._clean_clone(self.proto["bullet"])
            if indent is not None:
                self._set_ppr_child(p, "ind", {"left": str(indent + 360), "hanging": "360"})
            if align_left:
                self._set_ppr_child(p, "jc", {"val": "left"})
            if keep_last_with_next and index == len(items) - 1:
                self._set_ppr_child(p, "keepNext", {})
            self._fill(p, item)
            self._append(p)

    def code(self, lines: list[str], indent: int = CONTENT_INDENT + 360) -> None:
        max_chars = int((TEXT_WIDTH_DXA - indent) / 20 / 5.4)
        for line in lines:
            if len(line) > max_chars:
                self.warnings.append(f"code line wraps ({len(line)} > {max_chars} chars): {line!r}")
        for index, line in enumerate(lines):
            p = self._clean_clone(self.proto["body"])
            self._set_ppr_child(p, "keepLines", {})
            if index < len(lines) - 1:
                self._set_ppr_child(p, "keepNext", {})
            self._set_ppr_child(p, "spacing", {"before": "0", "after": "0"})
            self._set_ppr_child(p, "ind", {"left": str(indent)})
            self._set_ppr_child(p, "jc", {"val": "left"})
            # a literal code line — no markup parsing
            base = self._base_rpr(p)
            self._set_font(base, CODE_FONT)
            self._set_size(base, CODE_SIZE)
            for child in list(p):
                if child.tag != w("pPr"):
                    p.remove(child)
            run = etree.SubElement(p, w("r"))
            run.append(base)
            t = etree.SubElement(run, w("t"))
            t.text = line if line else " "
            t.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
            self._append(p)

    def figure(self, image: pathlib.Path, caption: str, width_in: float, height_in: float | None = None) -> None:
        p = self._clean_clone(self.proto["body"])
        for child in list(p):
            if child.tag != w("pPr"):
                p.remove(child)
        self._set_ppr_child(p, "keepNext", {})
        self._set_ppr_child(p, "spacing", {"before": "120", "after": "60"})
        self._set_ppr_child(p, "ind", {"left": "0"})
        self._set_ppr_child(p, "jc", {"val": "center"})
        self._append(p)
        run = Paragraph(p, self.doc._body).add_run()
        if height_in is None:
            run.add_picture(str(image), width=Inches(width_in))
        else:
            run.add_picture(str(image), height=Inches(height_in))
        self.figure_count += 1
        cap = self._clean_clone(self.proto["body"])
        self._set_ppr_child(cap, "spacing", {"before": "0", "after": "200"})
        self._set_ppr_child(cap, "ind", {"left": "0"})
        self._set_ppr_child(cap, "jc", {"val": "center"})
        self._fill(cap, f"Figure {self.figure_count}: {caption}", italic=True, size=CAPTION_SIZE)
        self._append(cap)

    def page_break(self) -> None:
        p = self._clean_clone(self.proto["body"])
        for child in list(p):
            if child.tag != w("pPr"):
                p.remove(child)
        run = etree.SubElement(p, w("r"))
        etree.SubElement(run, w("br")).set(w("type"), "page")
        self._append(p)

    # ── tables ────────────────────────────────────────────────────────────────
    def _fill_cell(self, cell: etree._Element, width: int, text: str, *, header: bool) -> None:
        cell.find(w("tcPr")).find(w("tcW")).set(w("w"), str(width))
        paragraphs = cell.findall(w("p"))
        for extra in paragraphs[1:]:
            cell.remove(extra)
        proto_p = paragraphs[0]
        lines = text.split("\n") if text else [""]
        for index, line in enumerate(lines):
            p = proto_p if index == 0 else copy.deepcopy(proto_p)
            if index:
                cell.append(p)
            if header:
                self._fill(p, line, bold=True)
            else:
                self._set_ppr_child(p, "jc", {"val": "left"})
                self._fill(p, line, size=TABLE_BODY_SIZE)

    def _build_rows(self, table: etree._Element, header_proto: etree._Element, body_proto: etree._Element,
                    headers: list[str] | None, rows: list[list[str]], widths: list[int]) -> None:
        grid = table.find(w("tblGrid"))
        for col in list(grid):
            grid.remove(col)
        for width in widths:
            etree.SubElement(grid, w("gridCol")).set(w("w"), str(width))
        table.find(w("tblPr")).find(w("tblW")).set(w("w"), str(sum(widths)))
        all_rows = ([(headers, header_proto, True)] if headers else []) + [(r, body_proto, False) for r in rows]
        for values, proto_row, is_header in all_rows:
            if len(values) != len(widths):
                raise ValueError(f"Row has {len(values)} cells, table has {len(widths)} columns: {values}")
            row = self._clean_clone(proto_row)
            cells = row.findall(w("tc"))
            pristine = copy.deepcopy(cells[0])
            for extra in cells[1:]:
                row.remove(extra)
            for index, value in enumerate(values):
                cell = cells[0] if index == 0 else copy.deepcopy(pristine)
                if index:
                    row.append(cell)
                self._fill_cell(cell, widths[index], value, header=is_header)
            if is_header:
                tr_pr = row.find(w("trPr"))
                if tr_pr is not None and tr_pr.find(w("tblHeader")) is None:
                    tr_pr.insert(0, etree.Element(w("tblHeader")))
            else:
                # let a long row break across pages rather than leaving large gaps
                tr_pr = row.find(w("trPr"))
                if tr_pr is not None:
                    for node in tr_pr.findall(w("trHeight")):
                        tr_pr.remove(node)
                    self._add_cant_split(tr_pr)
            table.append(row)

    @staticmethod
    def _add_cant_split(tr_pr: etree._Element) -> None:
        if tr_pr.find(w("cantSplit")) is None:
            # trPr children: cnfStyle, divId, gridBefore, gridAfter, wBefore, wAfter, cantSplit, ...
            node = etree.Element(w("cantSplit"))
            later = [c for c in tr_pr if etree.QName(c).localname in
                     ("trHeight", "tblHeader", "tblCellSpacing", "jc", "hidden", "ins", "del", "trPrChange")]
            if later:
                later[0].addprevious(node)
            else:
                tr_pr.append(node)

    def table(self, headers: list[str], rows: list[list[str]], widths: list[int]) -> None:
        table = self._clean_clone(self.proto["table"])
        proto_rows = table.findall(w("tr"))
        header_proto, body_proto = proto_rows[0], proto_rows[1]
        for row in proto_rows:
            table.remove(row)
        self._build_rows(table, header_proto, body_proto, headers, rows, widths)
        self._append(table)
        self._spacer()

    def template_table(self, name: str, rows: list[list[str]]) -> None:
        """Fill one of the template's own appendix tables, keeping its header row and column widths."""
        table = self._clean_clone(self.proto[name])
        proto_rows = table.findall(w("tr"))
        header, body_proto = proto_rows[0], proto_rows[1]
        widths = [int(col.get(w("w"))) for col in table.find(w("tblGrid")).findall(w("gridCol"))]
        for row in proto_rows[1:]:
            table.remove(row)
        grid_widths = widths
        body_rows = []
        for values in rows:
            if len(values) != len(grid_widths):
                raise ValueError(f"{name}: row has {len(values)} cells, table has {len(grid_widths)}")
            body_rows.append(values)
        for values in body_rows:
            row = self._clean_clone(body_proto)
            tr_pr = row.find(w("trPr"))
            if tr_pr is None:
                tr_pr = etree.Element(w("trPr"))
                row.insert(0 if row.find(w("tblPrEx")) is None else 1, tr_pr)
            self._add_cant_split(tr_pr)
            for cell, value, width in zip(row.findall(w("tc")), values, grid_widths):
                paragraphs = cell.findall(w("p"))
                for extra in paragraphs[1:]:
                    cell.remove(extra)
                p = paragraphs[0]
                # appendix C numbers its "#" column with an auto-list; keep that paragraph as it is
                if p.find(w("pPr")) is not None and p.find(w("pPr")).find(w("numPr")) is not None:
                    continue
                self._set_ppr_child(p, "jc", {"val": "left"})
                self._fill(p, value, size=TABLE_BODY_SIZE)
            table.append(row)
        self._append(table)
        self._spacer()

    def _spacer(self) -> None:
        p = self._clean_clone(self.proto["body"])
        for child in list(p):
            if child.tag != w("pPr"):
                p.remove(child)
        self._set_ppr_child(p, "spacing", {"before": "0", "after": "0"})
        self._append(p)

    # ── cover and table of contents ──────────────────────────────────────────
    def replace_paragraph_text(self, scope: etree._Element, old: str, new: str, *, drop_highlight: bool = False) -> None:
        matches = [p for p in scope.iter(w("p")) if self._text(p).strip() == old]
        if len(matches) != 1:
            raise ValueError(f"Expected exactly one paragraph {old!r} in scope, found {len(matches)}")
        p = matches[0]
        runs = p.findall(w("r"))
        keep = next(r for r in runs if (r.findtext(w("t")) or "").strip())
        for child in list(p):
            if child.tag != w("pPr") and child is not keep:
                p.remove(child)
        for t in keep.findall(w("t")):
            keep.remove(t)
        t = etree.SubElement(keep, w("t"))
        t.text = new
        t.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
        if drop_highlight:
            for node in list(p.iter(w("highlight"))):
                node.getparent().remove(node)

    def set_nested_table_widths(self, scope: etree._Element, old_widths: list[int], new_widths: list[int]) -> None:
        """Change the column widths of the one nested table in `scope` whose grid is `old_widths`."""
        matches = []
        for table in scope.iter(w("tbl")):
            grid = [int(c.get(w("w"))) for c in table.find(w("tblGrid")).findall(w("gridCol"))]
            if grid == old_widths:
                matches.append(table)
        if len(matches) != 1:
            raise ValueError(f"Expected one nested table with grid {old_widths}, found {len(matches)}")
        table = matches[0]
        for col, width in zip(table.find(w("tblGrid")).findall(w("gridCol")), new_widths):
            col.set(w("w"), str(width))
        for row in table.findall(w("tr")):
            for cell, width in zip(row.findall(w("tc")), new_widths):
                cell.find(w("tcPr")).find(w("tcW")).set(w("w"), str(width))

    def fix_footer_page_total(self) -> None:
        """The template footer reads "Page {PAGE} of10" with the total typed in; make it a NUMPAGES field."""
        footer = self.doc.sections[0].footer._element
        runs = footer.findall(f".//{w('r')}")
        texts = [(r, r.findtext(w("t"))) for r in runs]
        of_run = next((r for r, t in texts if t == " of"), None)
        total_run = next((r for r, t in texts if t == "10"), None)
        if of_run is None or total_run is None:
            raise ValueError("Template footer changed: expected ' of' followed by a typed '10'")
        of_run.find(w("t")).text = " of "
        rpr = total_run.find(w("rPr"))
        parts = []
        for kind, payload in (("begin", None), (None, " NUMPAGES   \\* MERGEFORMAT "), ("separate", None),
                              ("text", "1"), ("end", None)):
            run = etree.Element(w("r"))
            if rpr is not None:
                run.append(copy.deepcopy(rpr))
            if kind in ("begin", "separate", "end"):
                etree.SubElement(run, w("fldChar")).set(w("fldCharType"), kind)
            elif kind == "text":
                etree.SubElement(run, w("t")).text = payload
            else:
                instr = etree.SubElement(run, w("instrText"))
                instr.text = payload
                instr.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
            parts.append(run)
        for run in parts:
            total_run.addprevious(run)
        total_run.getparent().remove(total_run)

    def build_toc(self, pages: dict[str, str]) -> None:
        """Rebuild the TOC table rows from the recorded headings, cloning the template's own row styles."""
        rows = self.toc_table.findall(w("tr"))
        texts = [self._text(r).strip() for r in rows]

        def find_row(prefix: str) -> etree._Element:
            for row, text in zip(rows, texts):
                if text.startswith(prefix):
                    return row
            raise ValueError(f"TOC prototype row starting {prefix!r} not found")

        proto = {1: find_row("Introduction"), 2: find_row("1.1"), 3: find_row("3.2.1"),
                 4: find_row("3.2.3.1"), 0: find_row("Appendix B")}
        proto = {k: self._clean_clone(v) for k, v in proto.items()}
        for row in rows:
            self.toc_table.remove(row)
        spaces = {2: "", 3: "         ", 4: "                  "}
        for entry in self.toc:
            row = copy.deepcopy(proto[entry.level])
            label_cell, page_cell = row.findall(w("tc"))
            label_p = label_cell.find(w("p"))
            for extra in label_cell.findall(w("p"))[1:]:
                label_cell.remove(extra)
            if entry.level in (1, 0):
                label = entry.title
            else:
                label = f"{spaces[entry.level]}{entry.number}   {entry.title}"
            self._fill(label_p, label)
            page_p = page_cell.find(w("p"))
            for extra in page_cell.findall(w("p"))[1:]:
                page_cell.remove(extra)
            key = entry.number or entry.title
            self._fill(page_p, pages.get(key, ""))
            self.toc_table.append(row)

    def save(self, output: pathlib.Path) -> None:
        self.doc.save(str(output))
