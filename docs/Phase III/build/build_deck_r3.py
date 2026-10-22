"""
Build the E.C.H.O. Capstone Phase 3 Review 3 deck from the official PES Review 3 template.

How it works (same idea as build_deck.py for Review 2, which is left untouched):
  - Every template slide is kept, in the template's order. Its instruction text box is removed and our
    content is drawn in its place; the template's own red title, cyan rule and PES logo stay.
  - Sections that need more than one slide (Pipeline max 2, Implementation max 4, Test Plan max 2,
    Results max 2) get extra slides that clone that template slide's own title box and rule.
  - Every slide gets a generated background (r3/make_assets.py) set as the SLIDE background, so the
    master's PES logo is still drawn on top of it. Text sits on white cards over the richer background.
  - TV readability (owner rule, 2026-10-21): no text below 18 pt anywhere, body 20-24 pt, titles 32 pt.
    check_font_sizes() walks every run of every slide and FAILS the build if anything is smaller.
  - Diagrams are native PowerPoint shapes (editable, big boxes, labels >= 18 pt), colour-coded per module.

Content and its sources: r3/approved_content_r3.md. Detail that does not fit at TV size lives in each slide's
speaker notes. Edit content here and rebuild; do not hand-edit the output if you plan to rebuild.

    python "docs/Phase III/build/build_deck_r3.py"                       # writes ../ECHO_Phase3_Review3.pptx
    python "docs/Phase III/build/build_deck_r3.py" "%TEMP%/r3_test.pptx"  # elsewhere (deck open in PowerPoint)
"""
from __future__ import annotations

import copy
import io
import pathlib
import re
import sys
from dataclasses import dataclass
from typing import Optional, Union

from lxml import etree
from PIL import Image
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.dml import MSO_LINE_DASH_STYLE
from pptx.enum.shapes import MSO_CONNECTOR, MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.oxml import parse_xml
from pptx.oxml.ns import qn
from pptx.util import Emu, Inches, Pt

BUILD_DIR = pathlib.Path(__file__).resolve().parent
PHASE3_DIR = BUILD_DIR.parent
R3_DIR = BUILD_DIR / "r3"
sys.path.insert(0, str(R3_DIR))
import make_assets  # noqa: E402  (r3/make_assets.py — backgrounds and source icons)

# ── The values most likely to change ───────────────────────────────────────────
# Official Review 3 template (from the course email of 17 Oct 2026). Swapping templates = change this line.
TEMPLATE = PHASE3_DIR / "REVIEW - 3 - UE23CS441A - Capstone Project Phase – 3.pptx"
# Embedding model — decision A40 (owner, 2026-10-21). One line to change if the model ever changes.
EMBEDDING_MODEL = "snowflake-arctic-embed-xs"
EMBEDDING_FACTS = "384-d · reads 512 tokens"
# The Review 3 template has no "Guide Approval Mail" slide; the Review 2 template had one and the owner
# pasted the approval mail there. True = add an empty one after the title slide (owner can delete it).
INCLUDE_GUIDE_APPROVAL_SLIDE = True
# TV readability: the build fails if any text run on a slide is smaller than this.
MIN_FONT_PT = 18.0
TITLE_PT = 32.0

OUTPUT = PHASE3_DIR / "ECHO_Phase3_Review3.pptx"
BACKGROUNDS = R3_DIR / "backgrounds"
ASSETS = R3_DIR / "assets"

# ── Palette (template colours + one accent per module) ─────────────────────────
FONT = "Trebuchet MS"
MONO = "Consolas"
BLUE = RGBColor(0x00, 0x33, 0xCC)       # template body text
RED = RGBColor(0xFF, 0x00, 0x00)        # template titles
NAVY = RGBColor(0x10, 0x20, 0x4A)
GREY = RGBColor(0x45, 0x50, 0x68)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
LINE_SOFT = RGBColor(0xC5, 0xD0, 0xEA)


@dataclass(frozen=True)
class Accent:
    main: RGBColor      # bars, chips, headings
    tint: RGBColor      # light fill behind text
    dark: RGBColor      # text on the tint


CAPTURE = Accent(RGBColor(0x0E, 0x8C, 0x8C), RGBColor(0xE3, 0xF6, 0xF6), RGBColor(0x08, 0x4E, 0x4E))
ENRICH = Accent(RGBColor(0x6B, 0x4F, 0xD8), RGBColor(0xEE, 0xEA, 0xFD), RGBColor(0x2D, 0x1F, 0x66))
RECALL = Accent(RGBColor(0x1F, 0x4F, 0xE0), RGBColor(0xE6, 0xED, 0xFF), RGBColor(0x0B, 0x22, 0x72))
WELL = Accent(RGBColor(0x1F, 0x8A, 0x4C), RGBColor(0xE6, 0xF5, 0xEC), RGBColor(0x0F, 0x4D, 0x2A))
STORE = Accent(RGBColor(0x4A, 0x55, 0x6E), RGBColor(0xEE, 0xF1, 0xF7), RGBColor(0x1E, 0x26, 0x3A))
CLOUD = Accent(RGBColor(0xE0, 0x30, 0x1E), RGBColor(0xFF, 0xF1, 0xEE), RGBColor(0x7A, 0x1A, 0x10))
AMBER = Accent(RGBColor(0xC2, 0x6A, 0x00), RGBColor(0xFF, 0xF4, 0xE3), RGBColor(0x6B, 0x3A, 0x00))
BASE = RECALL

# Content frame (inches). The header (title, rule, PES logo) sits above Y0; a brand line runs below Y1.
X0, X1 = 0.7, 12.63
Y0, Y1 = 2.02, 7.12
W = X1 - X0


# ── Text model ────────────────────────────────────────────────────────────────

@dataclass
class Run:
    text: str
    bold: bool = False
    italic: bool = False
    color: Optional[RGBColor] = None
    size: Optional[float] = None
    font: Optional[str] = None


@dataclass
class Para:
    runs: list[Run]
    bullet: bool = False
    size: Optional[float] = None
    color: Optional[RGBColor] = None
    align: Optional[PP_ALIGN] = None
    space_after: float = 4
    font: Optional[str] = None


ParaLike = Union[str, Para]


def P(*parts: Union[str, Run], bullet: bool = False, size: Optional[float] = None,
      color: Optional[RGBColor] = None, align: Optional[PP_ALIGN] = None,
      space_after: float = 4, font: Optional[str] = None) -> Para:
    runs = [Run(p) if isinstance(p, str) else p for p in parts]
    return Para(runs, bullet, size, color, align, space_after, font)


def B(text: str, color: Optional[RGBColor] = None) -> Run:
    return Run(text, bold=True, color=color)


def I(text: str, color: Optional[RGBColor] = None) -> Run:  # noqa: E743 — italic run
    return Run(text, italic=True, color=color)


def _add_bullet(paragraph, indent_in: float = 0.28) -> None:
    p_pr = paragraph._p.get_or_add_pPr()
    p_pr.set("marL", str(int(Inches(indent_in))))
    p_pr.set("indent", str(-int(Inches(indent_in))))
    bu_font = etree.SubElement(p_pr, qn("a:buFont"))
    bu_font.set("typeface", "Arial")
    bu_char = etree.SubElement(p_pr, qn("a:buChar"))
    bu_char.set("char", "•")


def fill_text_frame(tf, paras: list[ParaLike], size: float, color: RGBColor, font: str,
                    align: PP_ALIGN, anchor: MSO_ANCHOR, margin: float) -> None:
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    tf.margin_left = tf.margin_right = Inches(margin)
    tf.margin_top = tf.margin_bottom = Inches(margin * 0.6)
    for i, item in enumerate(paras):
        para = P(item) if isinstance(item, str) else item
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = para.align or align
        p.space_after = Pt(para.space_after)
        if para.bullet:
            _add_bullet(p)
        for run in para.runs:
            r = p.add_run()
            r.text = run.text
            f = r.font
            f.name = run.font or para.font or font
            f.size = Pt(run.size or para.size or size)
            f.bold = run.bold
            f.italic = run.italic
            f.color.rgb = run.color or para.color or color
        # The empty end-of-paragraph mark gets the same size, so no hidden small text remains.
        end = p._p.get_or_add_endParaRPr()
        end.set("sz", str(int((para.size or size) * 100)))


def _check_frame(x: float, y: float, w: float, h: float) -> None:
    """PowerPoint refuses the whole file if any shape has a non-positive size or leaves the slide."""
    if w <= 0 or h <= 0:
        raise ValueError(f"Shape at ({x:.2f}, {y:.2f}) has non-positive size {w:.2f} x {h:.2f}")
    if x < 0 or y < 0 or x + w > 13.34 or y + h > 7.51:
        raise ValueError(f"Shape ({x:.2f}, {y:.2f}, {w:.2f} x {h:.2f}) extends past the 13.33 x 7.5 slide")


def _set_fill_alpha(shape, opacity: float) -> None:
    """Make a solid fill partly transparent (opacity 0..1)."""
    srgb = shape._element.spPr.find(qn("a:solidFill")).find(qn("a:srgbClr"))
    alpha = etree.SubElement(srgb, qn("a:alpha"))
    alpha.set("val", str(int(opacity * 100000)))


def box(slide, x: float, y: float, w: float, h: float, *, fill: Optional[RGBColor] = WHITE,
        line: Optional[RGBColor] = None, shape: MSO_SHAPE = MSO_SHAPE.RECTANGLE,
        radius: float = 0.06, line_w: float = 1.0, opacity: float = 1.0, dash: bool = False):
    _check_frame(x, y, w, h)
    sh = slide.shapes.add_shape(shape, Inches(x), Inches(y), Inches(w), Inches(h))
    sh.shadow.inherit = False
    if fill is None:
        sh.fill.background()
    else:
        sh.fill.solid()
        sh.fill.fore_color.rgb = fill
        if opacity < 1.0:
            _set_fill_alpha(sh, opacity)
    if line is None:
        sh.line.fill.background()
    else:
        sh.line.color.rgb = line
        sh.line.width = Pt(line_w)
        if dash:
            sh.line.dash_style = MSO_LINE_DASH_STYLE.DASH
    if shape in (MSO_SHAPE.ROUNDED_RECTANGLE, MSO_SHAPE.CHEVRON, MSO_SHAPE.PENTAGON):
        sh.adjustments[0] = radius
    # Shapes without text still carry an empty text body; give it a legal size for the font check.
    return sh


def text(slide, x: float, y: float, w: float, h: float, paras: list[ParaLike], *,
         size: float = 20, color: RGBColor = NAVY, font: str = FONT,
         align: PP_ALIGN = PP_ALIGN.LEFT, anchor: MSO_ANCHOR = MSO_ANCHOR.TOP,
         fill: Optional[RGBColor] = None, line: Optional[RGBColor] = None,
         shape: MSO_SHAPE = MSO_SHAPE.RECTANGLE, margin: float = 0.08, radius: float = 0.06,
         opacity: float = 1.0, dash: bool = False, line_w: float = 1.0):
    _check_frame(x, y, w, h)
    if fill is None and line is None:
        sh = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    else:
        sh = box(slide, x, y, w, h, fill=fill, line=line, shape=shape, radius=radius, opacity=opacity,
                 dash=dash, line_w=line_w)
    fill_text_frame(sh.text_frame, paras, size, color, font, align, anchor, margin)
    return sh


def arrow(slide, x1: float, y1: float, x2: float, y2: float, color: RGBColor = GREY, width: float = 2.25,
          dash: bool = False):
    c = slide.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, Inches(x1), Inches(y1), Inches(x2), Inches(y2))
    c.line.color.rgb = color
    c.line.width = Pt(width)
    if dash:
        c.line.dash_style = MSO_LINE_DASH_STYLE.DASH
    tail = etree.SubElement(c.line._get_or_add_ln(), qn("a:tailEnd"))
    tail.set("type", "triangle")
    return c


def picture(slide, source: Union[pathlib.Path, bytes], x: float, y: float, max_w: float, max_h: float):
    """Fit an image inside the (x, y, max_w, max_h) frame, centred, aspect preserved."""
    data = source if isinstance(source, bytes) else source.read_bytes()
    with Image.open(io.BytesIO(data)) as im:
        iw, ih = im.size
    scale = min(max_w / iw, max_h / ih)
    w, h = iw * scale, ih * scale
    _check_frame(x + (max_w - w) / 2, y + (max_h - h) / 2, w, h)
    return slide.shapes.add_picture(io.BytesIO(data), Inches(x + (max_w - w) / 2), Inches(y + (max_h - h) / 2),
                                    Inches(w), Inches(h))


def logo(slide, name: str, x: float, y: float, size: float):
    return picture(slide, ASSETS / f"logo_{name}.png", x, y, size, size)


def panel(slide, x: float, y: float, w: float, h: float, *, opacity: float = 0.97):
    """Plain white rounded card (the readable surface over the designed background)."""
    return box(slide, x, y, w, h, fill=WHITE, line=LINE_SOFT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.05,
               line_w=0.75, opacity=opacity)


def card(slide, x: float, y: float, w: float, h: float, heading: str, paras: list[ParaLike], *,
         accent: Accent = BASE, size: float = 20, heading_size: Optional[float] = None,
         anchor: MSO_ANCHOR = MSO_ANCHOR.TOP) -> None:
    """White card with a module-coloured bar on the left and a bold heading."""
    panel(slide, x, y, w, h)
    box(slide, x, y + 0.12, 0.08, h - 0.24, fill=accent.main)
    body = ([P(B(heading), color=accent.main, size=heading_size or size + 3, space_after=6)] if heading else [])
    text(slide, x + 0.16, y + 0.04, w - 0.22, h - 0.08, body + list(paras), size=size, anchor=anchor, margin=0.1)


def chip(slide, x: float, y: float, w: float, h: float, label: str, accent: Accent, *, size: float = 18,
         solid: bool = True):
    return text(slide, x, y, w, h, [P(B(label), align=PP_ALIGN.CENTER, space_after=0)], size=size,
                color=WHITE if solid else accent.dark, fill=accent.main if solid else accent.tint,
                line=None if solid else accent.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.5,
                anchor=MSO_ANCHOR.MIDDLE, margin=0.03)


def node(slide, x: float, y: float, w: float, h: float, title: str, sub: str, accent: Accent, *,
         size: float = 20, shape: MSO_SHAPE = MSO_SHAPE.ROUNDED_RECTANGLE, dash: bool = False):
    """A diagram box: bold title + optional detail line, module-tinted. Detail is never below 18 pt."""
    paras = [P(B(title), align=PP_ALIGN.CENTER, space_after=1, color=accent.dark)]
    if sub:
        paras.append(P(sub, align=PP_ALIGN.CENTER, space_after=0, size=max(MIN_FONT_PT, size - 2),
                       color=accent.dark))
    return text(slide, x, y, w, h, paras, size=size, fill=accent.tint, line=accent.main, shape=shape,
                anchor=MSO_ANCHOR.MIDDLE, margin=0.05, radius=0.12, line_w=2, dash=dash)


def notes(slide, body: str) -> None:
    slide.notes_slide.notes_text_frame.text = body


def set_background(slide, image: pathlib.Path) -> None:
    """Use `image` as the slide background (p:bg). Master shapes such as the PES logo stay on top."""
    _, rid = slide.part.get_or_add_image_part(str(image))
    c_sld = slide._element.find(qn("p:cSld"))
    old = c_sld.find(qn("p:bg"))
    if old is not None:
        c_sld.remove(old)
    bg = parse_xml(
        '<p:bg xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" '
        'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<p:bgPr><a:blipFill dpi="0" rotWithShape="1"><a:blip r:embed="{rid}"/><a:srcRect/>'
        '<a:stretch><a:fillRect/></a:stretch></a:blipFill><a:effectLst/></p:bgPr></p:bg>')
    c_sld.insert(0, bg)


def bg(name: str) -> pathlib.Path:
    return BACKGROUNDS / f"bg_{name}.png"


def send_to_back(shape) -> None:
    tree = shape._element.getparent()
    tree.remove(shape._element)
    tree.insert(2, shape._element)   # after nvGrpSpPr and grpSpPr


# ── TV-readability gate ───────────────────────────────────────────────────────

def _default_size_pt(prs) -> float:
    """Inherited size for a text box with no explicit size: presentation defaultTextStyle, else 18 pt."""
    dts = prs.part._element.find(qn("p:defaultTextStyle"))
    if dts is not None:
        for tag in ("a:lvl1pPr", "a:defPPr"):
            lvl = dts.find(qn(tag))
            if lvl is not None:
                d = lvl.find(qn("a:defRPr"))
                if d is not None and d.get("sz"):
                    return int(d.get("sz")) / 100
    return 18.0


def _run_size_pt(run, paragraph, text_frame, default_pt: float) -> float:
    if run.font.size is not None:
        return run.font.size.pt
    p_pr = paragraph._p.pPr
    if p_pr is not None:
        d = p_pr.find(qn("a:defRPr"))
        if d is not None and d.get("sz"):
            return int(d.get("sz")) / 100
    lst = text_frame._txBody.find(qn("a:lstStyle"))
    if lst is not None:
        lvl = lst.find(qn(f"a:lvl{paragraph.level + 1}pPr"))
        if lvl is not None:
            d = lvl.find(qn("a:defRPr"))
            if d is not None and d.get("sz"):
                return int(d.get("sz")) / 100
    return default_pt


def _shapes(shapes):
    for sh in shapes:
        if sh.shape_type == 6:          # group
            yield from _shapes(sh.shapes)
        else:
            yield sh


def check_font_sizes(prs, slides: list) -> float:
    """Fail if any visible text run on any slide is below MIN_FONT_PT. Returns the smallest size found."""
    default_pt = _default_size_pt(prs)
    smallest = 1000.0
    problems: list[str] = []
    for n, slide in enumerate(slides, 1):
        for sh in _shapes(slide.shapes):
            if not sh.has_text_frame:
                continue
            tf = sh.text_frame
            for para in tf.paragraphs:
                for run in para.runs:
                    if not run.text.strip():
                        continue
                    pt = _run_size_pt(run, para, tf, default_pt)
                    smallest = min(smallest, pt)
                    if pt < MIN_FONT_PT:
                        problems.append(f"slide {n}: {pt:g} pt in {sh.name!r}: {run.text[:40]!r}")
    if problems:
        raise RuntimeError("Text below the TV minimum of %g pt:\n  %s" % (MIN_FONT_PT, "\n  ".join(problems)))
    return smallest


# ── Template plumbing ─────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())


INSTRUCTION_BOXES = {"Content Placeholder 2", "TextBox 4", "Google Shape;26;p3"}


def _is_rule(sh) -> bool:
    return sh.shape_type == 1 and sh.height < Inches(0.1)


class Deck:
    def __init__(self) -> None:
        if not TEMPLATE.exists():
            raise FileNotFoundError(f"Template not found: {TEMPLATE}")
        self.prs = Presentation(str(TEMPLATE))
        self.template_slides = list(self.prs.slides)
        self.blank = self.template_slides[1].slide_layout
        self.order: list = []
        # Snapshot each template slide's own rule + title BEFORE anything is drawn on it, so clone() never
        # copies our content (a thin coloured bar would otherwise look like a rule).
        self._header: dict[int, list] = {}
        self._orig_title: dict[int, str] = {}
        for s in self.template_slides:
            t = self._title_shape(s)
            self._orig_title[s.slide_id] = t.text_frame.text if t is not None else ""
        for s in self.template_slides:
            self._header[s.slide_id] = [copy.deepcopy(sh._element) for sh in s.shapes
                                        if _is_rule(sh) or sh.name == "Text Box 34"]

    @staticmethod
    def _title_shape(slide):
        return next((s for s in slide.shapes if s.name == "Text Box 34"), None)

    def find(self, title: str):
        """The template slide whose title starts with `title` (dash/space/case-insensitive)."""
        for s in self.template_slides:
            if _norm(self._orig_title[s.slide_id]).startswith(_norm(title)):
                return s
        raise LookupError(f"Template has no slide titled {title!r} — has the template changed?")

    @staticmethod
    def _style_title(slide, title: Optional[str]) -> None:
        """Retitle (optional), set 32 pt, widen to the content frame and seat it just above the slide's rule."""
        t = Deck._title_shape(slide)
        rule = next(sh for sh in slide.shapes if _is_rule(sh))
        el = t._element
        paras = el.findall(".//" + qn("a:p"))
        for extra_p in paras[1:]:
            extra_p.getparent().remove(extra_p)
        runs = paras[0].findall(qn("a:r"))
        if title:
            runs[0].find(qn("a:t")).text = title
            for extra in runs[1:]:
                extra.getparent().remove(extra)
            runs = runs[:1]
        for r in runs:
            r_pr = r.find(qn("a:rPr"))
            if r_pr is None:
                r_pr = etree.SubElement(r, qn("a:rPr"))
                r.insert(0, r_pr)
            r_pr.set("sz", str(int(TITLE_PT * 100)))
        right = t.left + t.width
        t.left = Inches(X0)
        t.width = right - Inches(X0)
        t.height = Inches(0.66)
        t.top = rule.top - Inches(0.72)

    def keep(self, slide, title: Optional[str] = None, *, strip: bool = True):
        """Keep a template slide (in order). Remove its instruction text; optionally retitle it."""
        if strip:
            for sh in list(slide.shapes):
                if sh.name in INSTRUCTION_BOXES:
                    sh._element.getparent().remove(sh._element)
        if self._title_shape(slide) is not None:
            self._style_title(slide, title)
        self.order.append(slide)
        return slide

    def clone(self, source, title: str):
        """A new slide with `source`'s own title box and cyan rule (exact template styling)."""
        slide = self.prs.slides.add_slide(self.blank)
        tree = slide.shapes._spTree
        header = self._header[source.slide_id]
        if len(header) != 2:
            raise RuntimeError(f"Expected a rule and a title on the template slide, found {len(header)} shapes")
        for el in header:
            tree.append(copy.deepcopy(el))
        self._style_title(slide, title)
        self.order.append(slide)
        return slide

    def save(self, path: pathlib.Path) -> float:
        smallest = check_font_sizes(self.prs, self.order)
        sld_id_lst = self.prs.slides._sldIdLst
        id_by_rid = {s.get(qn("r:id")): s for s in sld_id_lst}
        rid_by_part = {self.prs.part.related_part(rid): rid for rid in id_by_rid}
        wanted = [rid_by_part[s.part] for s in self.order]
        for sld_id in list(sld_id_lst):
            sld_id_lst.remove(sld_id)
        for rid in wanted:
            sld_id_lst.append(id_by_rid[rid])
        for rid in set(id_by_rid) - set(wanted):
            self.prs.part.drop_rel(rid)
        self.prs.save(str(path))
        return smallest


# ── Slides ────────────────────────────────────────────────────────────────────

def slide_title(deck: Deck) -> None:
    s = deck.keep(deck.template_slides[0])
    set_background(s, bg("title"))
    labels = ["Project Title", "Project ID", "Project Guide", "Project Team"]
    values = [
        "E.C.H.O. – Efficient Contextualized History Orchestrator",
        "92",
        "Dr. M. Shalima Sulthana",
        "Mrigank Jain — PES2UG23CS352",
        "Mohammed Mir Fazlai Ali — PES2UG23CS346",
        "Mohammed Shehzaad Khan — PES2UG23CS349",
        "Arjun Sai N — PES2UG23CS091",
    ]
    top, h = 2.72, 2.85
    text(s, 1.3, top, 2.3, h, [P(l, space_after=2) for l in labels], size=20, color=BLUE)
    text(s, 3.45, top, 0.3, h, [P(":", space_after=2) for _ in labels], size=20, color=BLUE)
    text(s, 3.75, top, 8.6, h, [P(v, space_after=2) for v in values], size=20, color=BLUE)
    # Hero band content.
    text(s, X0, 6.32, 8.4, 0.62, [P(B("Your private, searchable memory."), space_after=0)],
         size=24, color=WHITE, anchor=MSO_ANCHOR.MIDDLE)
    for i, name in enumerate(["gmail", "chrome", "youtube"]):
        x = 9.55 + i * 1.0
        box(s, x, 6.22, 0.82, 0.82, fill=WHITE, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.2)
        logo(s, name, x + 0.11, 6.33, 0.6)
    notes(s, "Title. 8-minute talk + 8-minute demo + 4 minutes Q&A. Milestone: 100% implementation.")


def slide_guide_approval(deck: Deck) -> None:
    s = deck.clone(deck.find("Outline"), "Guide Approval Mail")
    set_background(s, bg("base"))
    notes(s, "Left empty on purpose: paste the guide's approval mail here (due Fri 23 Oct 2026). "
             "Not part of the Review 3 template — delete this slide if not wanted "
             "(build_deck_r3.py: INCLUDE_GUIDE_APPROVAL_SLIDE = False).")


def slide_outline(deck: Deck) -> None:
    s = deck.keep(deck.find("Outline"), strip=False)   # the outline text is kept exactly as the template
    set_background(s, bg("base"))
    send_to_back(panel(s, 1.45, 2.05, 10.95, 4.9, opacity=0.95))
    notes(s, "Outline exactly as the template asks.")


def slide_abstract(deck: Deck) -> None:
    s = deck.keep(deck.find("Abstract and Scope"))
    set_background(s, bg("feature"))
    panel(s, X0, Y0 + 0.03, W, 1.25)
    for i, name in enumerate(["gmail", "chrome", "youtube"]):
        logo(s, name, X0 + 0.25 + i * 0.9, Y0 + 0.28, 0.75)
    arrow(s, X0 + 3.05, Y0 + 0.65, X0 + 3.6, Y0 + 0.65, color=BLUE, width=3)
    text(s, X0 + 3.7, Y0 + 0.08, W - 3.8, 1.15, [
        P("You remember ", I("that"), " you saw it — not ", I("where"), " or ", I("when"), ".", space_after=4),
        P(B("E.C.H.O.: "), "one private memory you can ask.", color=BLUE, space_after=0),
    ], size=22, anchor=MSO_ANCHOR.MIDDLE)
    pillars = [
        ("Capture", CAPTURE, "Only what you engaged with", ["Intent gate per source", "Private pages skipped"]),
        ("Recall", RECALL, "Ask like a chat", ["Follow-up questions", "Answers link to the memory"]),
        ("Reflect", WELL, "Your time, no judgement", ["Patterns and sessions", "Groups you control"]),
    ]
    top, ph = Y0 + 1.48, 2.55
    pw = (W - 2 * 0.25) / 3
    for i, (name, acc, lead, points) in enumerate(pillars):
        x = X0 + i * (pw + 0.25)
        card(s, x, top, pw, ph, name, [P(B(lead), size=21, color=acc.dark, space_after=8)]
             + [P(p, bullet=True, space_after=6) for p in points], accent=acc, size=20, heading_size=26)
    y = top + ph + 0.18
    text(s, X0, y, W, Y1 - y, [
        P(B("Local-first: "), "storage and processing on your laptop. The LLM sees only the question and short "
          "snippets (OpenAI keeps chats ~30 days so follow-ups continue).", space_after=0),
    ], size=20, fill=WHITE, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.15, anchor=MSO_ANCHOR.MIDDLE, margin=0.18,
        opacity=0.97)
    notes(s, "~45 s. Problem in one line; then capture, recall, reflect. Scope: one user, desktop Chrome, "
             "Gmail + Chrome + YouTube. Privacy wording A31.5; chat retention from A26 (store=true, "
             "previous_response_id).")


def slide_suggestions(deck: Deck) -> None:
    s = deck.keep(deck.find("Suggestions from Review"), "Suggestions from Review – 1 & 2")
    set_background(s, bg("base"))
    rows = [
        ("Review 1", "Data retention", "Setting: keep forever, 1 year or 6 months; older items deleted", "A37"),
        ("Review 1", "Spam filtering", "Gmail's own filter: Spam and Trash are never read", "A38"),
        ("Review 1", "Focused talk", "One idea per slide, large text, native diagrams", "—"),
        ("Review 2", "Focused demo", "One scripted 8-minute path; demo video as backup", "Demo"),
    ]
    cols = [1.55, 2.75, 5.85, 1.78]
    xs = [X0]
    for c in cols[:-1]:
        xs.append(xs[-1] + c)
    head_y = Y0 + 0.03
    for x, w, label in zip(xs, cols, ["Review", "Suggestion", "Our response", "Status"]):
        text(s, x, head_y, w, 0.55, [P(B(label))], size=20, color=WHITE, fill=BLUE, anchor=MSO_ANCHOR.MIDDLE,
             margin=0.12)
    y = head_y + 0.65
    rh = (Y1 - y - 3 * 0.1) / 4
    for rev, name, resp, ref in rows:
        panel(s, X0, y, W, rh)
        chip(s, xs[0] + 0.12, y + rh / 2 - 0.25, cols[0] - 0.24, 0.5, rev, RECALL if rev == "Review 2" else STORE)
        text(s, xs[1], y, cols[1], rh, [P(B(name), space_after=0)], size=21, anchor=MSO_ANCHOR.MIDDLE, margin=0.12)
        text(s, xs[2], y, cols[2], rh, [P(resp, space_after=0)], size=20, anchor=MSO_ANCHOR.MIDDLE, margin=0.12)
        chip(s, xs[3] + 0.1, y + rh / 2 - 0.42, cols[3] - 0.2, 0.46, "Done", WELL)
        text(s, xs[3], y + rh / 2 + 0.06, cols[3], 0.38, [P(ref, align=PP_ALIGN.CENTER, space_after=0)], size=18,
             color=GREY, margin=0.02)
        y += rh + 0.1
    notes(s, "~40 s. Review 1 asked for (1) a retention period — now a user setting, default keep forever; a "
             "startup job hard-deletes older items from PostgreSQL, FAISS and Redis (A37). (2) A spam model — "
             "Gmail's spam filter is our model; Spam/Trash never read, also in the incremental sync; Promotions "
             "and Social stay by choice (A38). (3) Focused presentation. Review 2 asked for a focused demo.")


def slide_architecture(deck: Deck) -> None:
    s = deck.keep(deck.find("Complete pipeline"), "Complete Pipeline / Architecture of Project")
    set_background(s, bg("base"))
    lap_x, lap_top, lap_bot = 1.95, Y0 + 0.02, 6.08
    box(s, lap_x, lap_top, X1 - lap_x, lap_bot - lap_top, fill=WHITE, line=BLUE, shape=MSO_SHAPE.ROUNDED_RECTANGLE,
        radius=0.03, opacity=0.96, dash=True, line_w=1.75)
    text(s, lap_x + 0.15, lap_top + 0.03, 8.0, 0.42, [P(B("YOUR LAPTOP — everything stays local"))], size=20,
         color=BLUE, margin=0.02)
    col_w, step = 1.77, 2.15
    cxs = [2.1 + i * step for i in range(5)]
    heads = [("Capture", CAPTURE), ("API", STORE), ("Local data", STORE), ("Processing", RECALL),
             ("Interface", RECALL)]
    for x, (h, acc) in zip(cxs, heads):
        text(s, x - 0.1, lap_top + 0.45, col_w + 0.2, 0.4, [P(B(h), align=PP_ALIGN.CENTER, space_after=0)],
             size=18, color=acc.dark, margin=0.0)
    rows = [2.95, 3.98, 5.01]
    rh = 0.88
    # Sources (outside the laptop).
    for name, y in zip(["gmail", "chrome", "youtube"], rows):
        box(s, X0 + 0.05, y, 1.05, rh, fill=WHITE, line=LINE_SOFT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.12,
            line_w=0.75)
        logo(s, name, X0 + 0.22, y + 0.1, 0.68)
    for (t, acc), y in zip([("Gmail sync", CAPTURE), ("Chrome gate", CAPTURE), ("YouTube gate", CAPTURE)], rows):
        node(s, cxs[0], y, col_w, rh, t, "", acc, size=19)
        arrow(s, X0 + 1.12, y + rh / 2, cxs[0] - 0.02, y + rh / 2)
    node(s, cxs[1], rows[0], col_w, rh, "FastAPI", "", STORE, size=19)
    node(s, cxs[1], 4.45, col_w, 1.1, "Storage engine", "", STORE, size=19)
    for y in rows:
        arrow(s, cxs[0] + col_w + 0.02, y + rh / 2, cxs[1] - 0.02, rows[0] + rh / 2)
    arrow(s, cxs[1] + col_w / 2, rows[0] + rh + 0.02, cxs[1] + col_w / 2, 4.43)
    for t, y in zip(["PostgreSQL", "FAISS", "Redis"], rows):
        node(s, cxs[2], y, col_w, rh, t, "", STORE, size=19, shape=MSO_SHAPE.CAN)
    arrow(s, cxs[1] + col_w + 0.02, 5.0, cxs[2] - 0.02, rows[0] + rh / 2)
    arrow(s, cxs[1] + col_w + 0.02, 5.0, cxs[2] - 0.02, rows[2] + rh / 2)
    for (t, acc), y in zip([("ENP enrich", ENRICH), ("RSE recall", RECALL), ("WBA wellbeing", WELL)], rows):
        node(s, cxs[3], y, col_w, rh, t, "", acc, size=19)
    arrow(s, cxs[2] + col_w + 0.02, rows[0] + rh / 2, cxs[3] - 0.02, rows[0] + rh / 2)
    arrow(s, cxs[2] + col_w + 0.02, rows[1] + rh / 2, cxs[3] - 0.02, rows[1] + rh / 2)
    arrow(s, cxs[2] + col_w + 0.02, rows[0] + rh * 0.8, cxs[3] - 0.02, rows[2] + rh / 2)
    for (t, acc), y in zip([("Dashboard", RECALL), ("Popup: pause", CAPTURE), ("MCP server", RECALL)], rows):
        node(s, cxs[4], y, col_w, rh, t, "", acc, size=19)
    arrow(s, cxs[3] + col_w + 0.02, rows[1] + rh / 2, cxs[4] - 0.02, rows[0] + rh / 2)
    arrow(s, cxs[3] + col_w + 0.02, rows[1] + rh / 2, cxs[4] - 0.02, rows[2] + rh / 2)
    arrow(s, cxs[3] + col_w + 0.02, rows[2] + rh / 2, cxs[4] - 0.02, rows[0] + rh * 0.8)
    # Cloud row — the only things that leave the laptop.
    cy = 6.24
    text(s, X0, cy, 5.05, Y1 - cy, [P(B("Google APIs → "), "your own mail and video details", space_after=0)],
         size=20, color=STORE.dark, fill=WHITE, line=STORE.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.2,
         anchor=MSO_ANCHOR.MIDDLE, margin=0.14, dash=True, line_w=1.5)
    text(s, 5.95, cy, X1 - 5.95, Y1 - cy, [P(B("OpenAI Luna ← "), "question + short snippets only; chats kept ~30 days",
                                           space_after=0)],
         size=20, color=CLOUD.dark, fill=CLOUD.tint, line=CLOUD.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.2,
         anchor=MSO_ANCHOR.MIDDLE, margin=0.14, dash=True, line_w=1.5)
    arrow(s, cxs[3] + col_w / 2, lap_bot + 0.01, cxs[3] + col_w / 2, cy - 0.02, color=CLOUD.main, dash=True)
    arrow(s, cxs[0] + col_w / 2, lap_bot + 0.01, cxs[0] + col_w / 2, cy - 0.02, color=STORE.main, dash=True)
    notes(s, "~60 s. Left to right = the life of one memory. Gmail sync = incremental history.list every 30 s (A5); "
             "Chrome and YouTube gates (A39). Storage engine: one transaction per item, dedup on (source, id). "
             "Local PostgreSQL 16 + Redis in WSL (A31). ENP = clean, embed, classify; RSE = Luna agent + "
             "search_memories tool (A25/A26); WBA = time, sessions, regret, groups, insights. MCP server: Claude "
             "Desktop or any MCP client can call search_memories, get_memory, recent_activity over stdio — Echo "
             "stays the engine (E1). Only HTTPS to Google (your own data) and OpenAI (question + short snippets; "
             "chats kept ~30 days so follow-ups continue via previous_response_id, A26 store=true) leaves the laptop.")


def slide_changes(deck: Deck) -> None:
    s = deck.clone(deck.find("Complete pipeline"), "Pipeline – What Changed Since Review 2")
    set_background(s, bg("base"))
    text(s, X0, Y0 + 0.03, W, 0.55, [P(B("Every open decision closed — the eight that changed the pipeline"),
                                       space_after=0)],
         size=22, color=BLUE, fill=WHITE, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.3, anchor=MSO_ANCHOR.MIDDLE,
         margin=0.18, opacity=0.97)
    items = [
        ("A25 · A26", RECALL, "Agentic recall", "Luna + search tool, ≤ 6 calls"),
        ("A27 · A28", RECALL, "Memory-pill answers", "click a pill → the original"),
        ("E1", RECALL, "Echo as an MCP server", "Claude Desktop can ask it"),
        ("A31", STORE, "Fully local stack", "own PostgreSQL + Redis"),
        ("A1 · A24", STORE, "Per-visit table", "time filters by visit"),
        ("A4 · A22", ENRICH, "Searchable attachments", "PDF, Office, OCR images"),
        ("A7 · A9", CAPTURE, "Privacy at capture", "38 words, URL secrets cut"),
        ("A5 · A38", CAPTURE, "Incremental Gmail", "history.list; no Spam"),
    ]
    gx, gy = 0.25, 0.13
    cw = (W - gx) / 2
    top = Y0 + 0.72
    ch = (Y1 - top - 3 * gy) / 4
    for i, (ref, acc, head, sub) in enumerate(items):
        x = X0 + (i % 2) * (cw + gx)
        y = top + (i // 2) * (ch + gy)
        panel(s, x, y, cw, ch)
        box(s, x, y + 0.1, 0.08, ch - 0.2, fill=acc.main)
        chip(s, x + 0.22, y + (ch - 0.48) / 2, 1.75, 0.48, ref, acc)
        text(s, x + 2.1, y, cw - 2.15, ch, [P(B(head), size=21, color=acc.dark, space_after=0),
                                             P(sub, size=19, space_after=0)], anchor=MSO_ANCHOR.MIDDLE, margin=0.04)
    notes(s, "~50 s. A25 GPT-5.6 Luna on the Responses API via LangChain (provider still swappable). A26 Luna calls "
             "search_memories up to 5 rounds: usually 2, max 6 LLM calls, cap in code; chats continue via "
             "previous_response_id (store=true, ~30 days). A27/A28 chat answers with memory pills + live progress. "
             "E1 MCP server (search_memories, get_memory, recent_activity over stdio). A31 local PostgreSQL + Redis. "
             "A1 per-visit table, A24 stored full-text index over the whole text. A4/A22/A23 attachment text "
             "extracted at ingestion (PDF, DOCX, XLSX, PPTX, EML, OCR); only query-matched passages reach the LLM. "
             "A7/A9 whole-token sensitive words, form check, secret URL keys stripped, app pages title only. "
             "A5 history.list; A38 Spam/Trash never read. Also: A19-A21 typed Stage-4 classifier (Decisions API).")


def slide_impl_capture(deck: Deck) -> None:
    s = deck.keep(deck.find("Implementation Details"), "Implementation – Capture & Intent Gates")
    set_background(s, bg("capture"))
    cols = [
        ("chrome", "Chrome", ["10 s · 15% scroll · a click · a revisit", "Sensitive pages skipped",
                              "Secret URL keys removed"]),
        ("youtube", "YouTube", ["Video 20 s · Short 10 s", "or 10% watched (≥ 5 s)", "Title, channel, transcript"]),
        ("gmail", "Gmail", ["New mail every 30 s", "Spam, Trash, drafts never read", "Attachment text extracted"]),
    ]
    top, ch = Y0 + 0.03, 3.72
    cw = (W - 2 * 0.25) / 3
    for i, (lg, name, bullets) in enumerate(cols):
        x = X0 + i * (cw + 0.25)
        panel(s, x, top, cw, ch)
        box(s, x, top, cw, 0.1, fill=CAPTURE.main)
        logo(s, lg, x + 0.22, top + 0.28, 0.72)
        text(s, x + 1.1, top + 0.25, cw - 1.2, 0.8, [P(B(name), space_after=0)], size=26, color=CAPTURE.dark,
             anchor=MSO_ANCHOR.MIDDLE, margin=0.02)
        text(s, x + 0.12, top + 1.2, cw - 0.24, ch - 1.25, [P(b, bullet=True, space_after=10) for b in bullets],
             size=20, margin=0.08)
    y = top + ch + 0.2
    card(s, X0, y, W, Y1 - y, "Never captured", [
        P("Incognito tabs · typed text · text of app pages · sensitive pages · anything while paused",
          space_after=0)], accent=CLOUD, size=20, heading_size=22)
    notes(s, "~45 s. Chrome: Phase 1 = 5 s in focus (records the visit); Phase 2 passes on 10 s, scroll >= 15%, "
             "one interaction (link clicks don't count) or a revisit within 24 h (A39). Sensitive pages: 38 words as "
             "whole tokens + password / personal-form check (A7); secret URL keys (token, code, state, ...) stripped "
             "(A7.3). App pages (Slack, Notion, Jira): title only (A8). YouTube: regular >= 20 s, Shorts >= 10 s, "
             "completion >= 10% with >= 5 s watched, revisit check at 5 s (A39, A34); transcript via "
             "youtube-transcript-api (A11); watch time = how far you got (A2). Gmail: history.list from the saved "
             "historyId, full listing as fallback (A5); drafts skipped; attachments extracted in the background (A4); "
             "reading time = email id + seconds for wellbeing (A41). Storage engine: one transaction per item, "
             "UNIQUE(source_type, source_id). Pause switch in the extension popup (A33).")


def _chevron_row(s, x: float, y: float, w: float, h: float, steps: list[tuple[str, str]], acc: Accent) -> None:
    n = len(steps)
    overlap = 0.14
    sw = (w + overlap * (n - 1)) / n
    for i, (head, sub) in enumerate(steps):
        sx = x + i * (sw - overlap)
        shp = MSO_SHAPE.PENTAGON if i == 0 else MSO_SHAPE.CHEVRON
        box(s, sx, y, sw, h, fill=WHITE if i % 2 == 0 else acc.tint, line=acc.main, shape=shp, radius=0.2,
            line_w=2)
        inset = 0.12 if i == 0 else 0.34
        text(s, sx + inset, y, sw - inset - 0.3, h, [P(B(head), size=21, color=acc.dark, space_after=0,
                                                       align=PP_ALIGN.CENTER),
                                                     P(sub, size=18, align=PP_ALIGN.CENTER, space_after=0)],
             anchor=MSO_ANCHOR.MIDDLE, margin=0.0)


def slide_impl_enrich(deck: Deck) -> None:
    s = deck.clone(deck.find("Implementation Details"), "Implementation – Enrichment (ENP)")
    set_background(s, bg("enrich"))
    _chevron_row(s, X0, Y0 + 0.05, W, 1.05, [
        ("Clean", "HTML → text"), ("Build text", "≤ 512 tokens"), ("Embed", "384-d vector"),
        ("Index", "into FAISS"), ("Classify", "5 groups"),
    ], ENRICH)
    top = Y0 + 1.3
    stages = [
        ("1 · Rules: labels, Shorts, .edu", ENRICH),
        ("2 · Curated domain map", ENRICH),
        ("3 · Nearest centroid", ENRICH),
        ("4 · LLM: title + 300 chars", CLOUD),
    ]
    fh, fgap, full = 0.72, 0.1, 6.7
    for i, (label, acc) in enumerate(stages):
        inset = i * 0.3
        text(s, X0 + inset, top + i * (fh + fgap), full - 2 * inset, fh,
             [P(B(label), align=PP_ALIGN.CENTER, space_after=0)], size=20, color=acc.dark, fill=acc.tint,
             line=acc.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.25, anchor=MSO_ANCHOR.MIDDLE, margin=0.05,
             line_w=2)
    rx = X0 + full + 0.3
    rw = X1 - rx
    text(s, rx, top, rw, 1.25, [
        P(B("Output: system group"), size=21, color=WHITE, space_after=2),
        P("work · study · entertainment · personal · misc", size=19, color=WHITE, space_after=0),
    ], fill=ENRICH.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.15, anchor=MSO_ANCHOR.MIDDLE, margin=0.18)
    fun_bot = top + 4 * fh + 3 * fgap
    card(s, rx, top + 1.4, rw, fun_bot - top - 1.4, "Why this order", [
        P("Cheapest stage first", bullet=True, space_after=4),
        P("Centroids learn topic, not source", bullet=True, space_after=4),
        P("Unsure → 'misc', failed → retried", bullet=True, space_after=0),
    ], accent=ENRICH, size=20, heading_size=22)
    y = fun_bot + 0.15
    text(s, X0, y, W, Y1 - y, [P(B("Embedding model: "), EMBEDDING_MODEL + " · " + EMBEDDING_FACTS, space_after=0)], size=20,
         fill=WHITE, line=LINE_SOFT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.3, anchor=MSO_ANCHOR.MIDDLE,
         margin=0.18, opacity=0.97)
    notes(s, "~45 s. enp/ runs as a background thread in the backend and picks up new rows every 10 s; it never "
             "blocks capture. Clean: HTML to text, quoted replies cut, signatures kept (A12). Build text: short hints "
             "first, then the body, cut to the model's 512-token window with its own tokenizer, so what is stored is exactly "
             "what was embedded (A16); Chrome headings (A13), attachment text (A4). Model snowflake-arctic-embed-xs, "
             "384-d (A40). Stage 1 structural rules; Stage 2 curated "
             "domain map grown from real data (A17); Stage 3 nearest centroid with richer seeds, balanced across "
             "sources (A18, A20), recomputed at startup when > 30 days old (A36); Stage 4 OpenAI Decisions API, "
             "sees title + 300-char snippet only (A19, A21). system_group never NULL ('misc').")


def slide_impl_recall(deck: Deck) -> None:
    s = deck.clone(deck.find("Implementation Details"), "Implementation – Recall (RSE)")
    set_background(s, bg("recall"))
    lw = 7.35
    ty, th = Y0 + 0.05, 0.78
    node(s, X0, ty, 1.85, th, "Question", "", STORE)
    node(s, X0 + 2.25, ty, 2.6, th, "Luna agent", "", CLOUD)
    node(s, X0 + 5.25, ty, lw - 5.25, th, "Answer", "", WELL)
    arrow(s, X0 + 1.87, ty + th / 2, X0 + 2.23, ty + th / 2)
    arrow(s, X0 + 4.87, ty + th / 2, X0 + 5.23, ty + th / 2, color=WELL.main)
    tb_y, tb_h = ty + th + 0.5, 2.95
    lx = X0 + 2.25 + 1.3
    arrow(s, lx - 0.35, ty + th + 0.02, lx - 0.35, tb_y - 0.02, color=CLOUD.main, width=2.75)
    arrow(s, lx + 0.35, tb_y - 0.02, lx + 0.35, ty + th + 0.02, color=RECALL.main, width=2.75)
    text(s, lx + 0.5, ty + th + 0.04, 3.4, 0.42, [P("≤ 5 search rounds", space_after=0)], size=18,
         color=RECALL.dark, margin=0.0)
    box(s, X0, tb_y, lw, tb_h, fill=WHITE, line=RECALL.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.04,
        line_w=2, opacity=0.97)
    text(s, X0 + 0.15, tb_y + 0.06, lw - 0.3, 0.45, [P(B("search_memories — plain Python, no LLM"), space_after=0)],
         size=20, color=RECALL.dark, margin=0.02)
    steps = ["Time phrase → dates", "Keyword + semantic search", "RRF merge (k = 60)",
             "Rerank: 0.8 cross-encoder + 0.2 effort", "Add thread / ±5 min neighbours"]
    sy, sh_ = tb_y + 0.58, 0.45
    for i, st in enumerate(steps):
        y = sy + i * (sh_ + 0.02)
        text(s, X0 + 0.2, y + 0.03, 0.4, 0.4, [P(B(str(i + 1)), align=PP_ALIGN.CENTER, space_after=0)], size=18,
             color=WHITE, fill=RECALL.main, shape=MSO_SHAPE.OVAL, anchor=MSO_ANCHOR.MIDDLE, margin=0.0)
        text(s, X0 + 0.72, y, lw - 0.85, sh_, [P(st, space_after=0)], size=20, anchor=MSO_ANCHOR.MIDDLE, margin=0.02)
    st_y = tb_y + tb_h + 0.15
    stats = [("usually 2", "LLM calls"), ("max 6", "5 rounds + answer"), ("+ MCP", "Claude Desktop")]
    sw2 = (lw - 0.3) / 3
    for i, (big, small) in enumerate(stats):
        text(s, X0 + i * (sw2 + 0.15), st_y, sw2, Y1 - st_y, [
            P(B(big), size=24, color=RECALL.main, align=PP_ALIGN.CENTER, space_after=0),
            P(small, size=18, align=PP_ALIGN.CENTER, space_after=0, color=GREY)],
            fill=WHITE, line=LINE_SOFT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.15, anchor=MSO_ANCHOR.MIDDLE,
            margin=0.04, opacity=0.97, line_w=0.75)
    # Right: chat illustration.
    rx = X0 + lw + 0.3
    rw = X1 - rx
    panel(s, rx, Y0 + 0.05, rw, Y1 - Y0 - 0.05)
    text(s, rx + 0.15, Y0 + 0.1, rw - 0.3, 0.42, [P(B("Answer style "), I("(illustration)", GREY), space_after=0)],
         size=20, color=RECALL.dark, margin=0.02)
    text(s, rx + 0.6, Y0 + 0.62, rw - 0.75, 0.85, [P("Did Priya send the checklist?", space_after=0)], size=20,
         color=WHITE, fill=RECALL.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.3, anchor=MSO_ANCHOR.MIDDLE,
         margin=0.14)
    text(s, rx + 0.15, Y0 + 1.62, rw - 0.6, 1.2, [P("Yes — ", B("Priya"), " emailed it on ", B("Tue 14 Oct"),
                                                    ", PDF attached.", space_after=0)],
         size=20, fill=RECALL.tint, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.15, anchor=MSO_ANCHOR.MIDDLE,
         margin=0.14)
    for j, (lbl, solid) in enumerate([("Priya · 14 Oct", True), ("HR · 15 Oct", False)]):
        py = Y0 + 2.98 + j * 0.62
        box(s, rx + 0.15, py, 2.65, 0.5, fill=WHITE, line=RECALL.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.5,
            line_w=1.75, dash=not solid)
        logo(s, "gmail", rx + 0.28, py + 0.1, 0.3)
        text(s, rx + 0.65, py, 2.1, 0.5, [P(lbl, space_after=0)], size=18, color=RECALL.dark,
             anchor=MSO_ANCHOR.MIDDLE, margin=0.02)
    text(s, rx + 0.15, Y0 + 4.32, rw - 0.3, Y1 - Y0 - 4.4, [
        P(B("Click a pill"), " → the original mail", space_after=0)], size=20, anchor=MSO_ANCHOR.MIDDLE, margin=0.04)
    notes(s, "~75 s — the core. Luna (GPT-5.6, reasoning medium, A25) decides WHAT to search; search_memories "
             "(query, sources, time_phrase, sender_or_domain) is fixed Python: time_anchor resolves the phrase, "
             "PostgreSQL full text + FAISS in parallel, RRF k = 60, rerank 0.8 cross-encoder + 0.2 effort "
             "(rse/config.py; the cross-encoder ms-marco-MiniLM-L-6-v2 reads the title + the best-matching 500-char "
             "window), same-thread / ±5 min neighbours, numbered short snippets back. Max 5 rounds, then the "
             "next call must answer — cap in code (A26). LLM down → plain deterministic search on the raw query. "
             "Chats continue via previous_response_id (store=true, OpenAI keeps ~30 days). Answers are chat-style "
             "with memory pills (who / where / when); solid = the item says it, dashed = related; pills the search "
             "did not return are dropped (A27); live progress while searching (A28). MCP server (E1): "
             "search_memories, get_memory, recent_activity over stdio — Claude Desktop uses its own model, Echo "
             "stays the engine. The chat on the right is an illustration of the format, not a screenshot.")


def slide_impl_wellbeing(deck: Deck) -> None:
    s = deck.clone(deck.find("Implementation Details"), "Implementation – Wellbeing (WBA)")
    set_background(s, bg("wellbeing"))
    lw = 6.2
    tiles = [
        ("Time", "Dwell per visit, by day and group"),
        ("Sessions", "Gaps ≤ 20 min = one sitting"),
        ("Regret", "Mark any item; ≤ 2 reminders a day"),
        ("Weekly insight", "LLM sees numbers only; no scores"),
    ]
    tw = (lw - 0.22) / 2
    th = (Y1 - Y0 - 0.2) / 2
    for i, (head, body) in enumerate(tiles):
        x = X0 + (i % 2) * (tw + 0.22)
        y = Y0 + 0.03 + (i // 2) * (th + 0.17)
        card(s, x, y, tw, th, head, [P(body, space_after=0)], accent=WELL, size=21, heading_size=25,
             anchor=MSO_ANCHOR.MIDDLE)
    rx = X0 + lw + 0.3
    rw = X1 - rx
    panel(s, rx, Y0 + 0.03, rw, Y1 - Y0 - 0.03)
    text(s, rx + 0.15, Y0 + 0.1, rw - 0.3, 0.45, [P(B("User groups — you stay in control"), space_after=0)],
         size=22, color=WELL.dark, margin=0.02)
    nx, nw = rx + 0.25, rw - 0.5
    half = (nw - 0.2) / 2
    lx_, rx_ = nx + half / 2, nx + half + 0.2 + half / 2
    node(s, nx, Y0 + 0.66, nw, 0.62, "New item", "", STORE)
    node(s, nx, Y0 + 1.5, nw, 0.62, "Score: rules + KNN", "", STORE)
    arrow(s, nx + nw / 2, Y0 + 1.3, nx + nw / 2, Y0 + 1.48)
    node(s, nx, Y0 + 2.62, half, 0.7, "Auto-added", "", WELL)
    node(s, nx + half + 0.2, Y0 + 2.62, half, 0.7, "Pending", "", AMBER)
    arrow(s, nx + nw / 2 - 0.3, Y0 + 2.14, lx_, Y0 + 2.6, color=WELL.main)
    arrow(s, nx + nw / 2 + 0.3, Y0 + 2.14, rx_, Y0 + 2.6, color=AMBER.main)
    text(s, nx, Y0 + 2.14, half - 0.35, 0.42, [P("confident", space_after=0)], size=18, color=WELL.dark, margin=0.0)
    text(s, nx + half + 0.55, Y0 + 2.14, half - 0.35, 0.42, [P("unsure", align=PP_ALIGN.RIGHT, space_after=0)],
         size=18, color=AMBER.dark, margin=0.0)
    node(s, nx, Y0 + 3.55, half, 0.95, "Sunday review", "1-click undo", WELL)
    node(s, nx + half + 0.2, Y0 + 3.55, half, 0.95, "Your yes / no", "accept or reject", AMBER)
    arrow(s, lx_, Y0 + 3.34, lx_, Y0 + 3.53, color=WELL.main)
    arrow(s, rx_, Y0 + 3.34, rx_, Y0 + 3.53, color=AMBER.main)
    text(s, rx + 0.15, Y0 + 4.6, rw - 0.3, Y1 - Y0 - 4.65, [
        P("Never decides alone when unsure", space_after=0)], size=19, color=WELL.dark, anchor=MSO_ANCHOR.MIDDLE,
        align=PP_ALIGN.CENTER, margin=0.04)
    notes(s, "~45 s. Time = per-visit dwell summed by local day (A1.1b). Sessions: activity joins one sitting while "
             "gaps stay <= 20 min (C24; doc says 5). Regret: mark/unmark any item, patterns by hour and category, at "
             "most 2 gentle reminders a day after a >= 15 min session (A39). Weekly insight: the LLM sees aggregated "
             "numbers only; wording stays descriptive — no scores, no 'wasted time'. Groups (A42): each item is scored "
             "by rules + KNN (mean cosine of the 3 nearest confirmed members; KNN starts at 5 confirmed items, A39, "
             "A32). Confident (a rule matches AND KNN >= the bar) → added automatically and still shown in the Sunday "
             "review, where one click undoes it. Anything less certain waits as pending for the user's yes. Only "
             "items the user confirmed teach KNN. Panel line: 'Echo never decides alone when it is unsure — "
             "confident items are added and shown for one-click undo (human-on-the-loop); uncertain ones wait for "
             "your yes (human-in-the-loop).' User groups stay independent of system groups (A43).")


def slide_demo(deck: Deck) -> None:
    s = deck.keep(deck.find("Project Demonstration"))
    set_background(s, bg("feature"))
    text(s, X0, Y0 + 0.03, W, 0.6, [P(B("8-minute scripted path "), "· recorded demo video ready as backup",
                                      space_after=0)],
         size=22, color=BLUE, fill=WHITE, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.3, anchor=MSO_ANCHOR.MIDDLE,
         margin=0.18, opacity=0.97)
    steps = [
        ("1", "Capture", "1.5 min", CAPTURE, "Read an article, play a video", "New rows in Timeline"),
        ("2", "Enrichment", "1 min", ENRICH, "Wait about 10 s", "Item processed, group set"),
        ("3", "Recall", "3.5 min", RECALL, "Ask, then follow up", "Answer with memory pills"),
        ("4", "Wellbeing", "2 min", WELL, "Mark a regret", "Patterns; accept a group"),
    ]
    top = Y0 + 0.82
    sh = Y1 - top
    cw = (W - 3 * 0.3) / 4
    for i, (n, name, mins, acc, do, see) in enumerate(steps):
        x = X0 + i * (cw + 0.3)
        panel(s, x, top, cw, sh, opacity=0.98)
        box(s, x, top, cw, 0.1, fill=acc.main)
        text(s, x + 0.18, top + 0.3, 0.75, 0.75, [P(B(n), align=PP_ALIGN.CENTER, space_after=0)], size=26,
             color=WHITE, fill=acc.main, shape=MSO_SHAPE.OVAL, anchor=MSO_ANCHOR.MIDDLE, margin=0.0)
        text(s, x + 1.0, top + 0.25, cw - 1.05, 0.85, [P(B(name), size=22, color=acc.dark, space_after=0),
                                                       P(mins, size=18, color=GREY, space_after=0)],
             anchor=MSO_ANCHOR.MIDDLE, margin=0.02)
        text(s, x + 0.12, top + 1.3, cw - 0.24, sh - 1.35, [
            P(B("Do"), size=18, color=acc.main, space_after=2), P(do, space_after=14),
            P(B("Panel sees"), size=18, color=acc.main, space_after=2), P(see, space_after=0),
        ], size=20, margin=0.06)
        if i < 3:
            arrow(s, x + cw + 0.03, top + 0.68, x + cw + 0.27, top + 0.68, color=WHITE, width=3)
    notes(s, "Hand over to the demo (8 min). 1 Capture: read a fresh article and scroll past 15%; play a video for "
             "20 s; show the new rows in Timeline and the popup pause switch. 2 Enrichment: one ENP cycle (~10 s); "
             "the item turns processed with its system group. 3 Recall: ask about the article, then 'Did anyone email "
             "me about it?' in the same chat; show live steps and memory pills; click a pill. 4 Wellbeing: Patterns "
             "and Reflections; mark one item regretted; accept a group suggestion. Optional: one query from Claude "
             "Desktop through the MCP server. Pre-flight: local PostgreSQL + Redis (WSL) running, backend + ENP up, "
             "extension loaded in the browser used, one fresh mail in the test inbox. If OpenAI is unreachable, "
             "recall falls back to plain search (A26). Revoke the Google token shown on screen after the demo (A35).")


def slide_test_plan(deck: Deck) -> None:
    s = deck.keep(deck.find("Test Plan and Strategy"), "Test Plan and Strategy (1/2) – Activities")
    set_background(s, bg("base"))
    lw = 7.25
    panel(s, X0, Y0 + 0.03, lw, Y1 - Y0 - 0.03)
    tl = [
        ("6 – 19 Oct", "Every file reviewed + simulation tests"),
        ("21 – 22 Oct", "Thresholds re-measured on local data"),
        ("22 – 25 Oct", "Unit + integration checks; freeze 25 Oct"),
        ("26 – 27 Oct", "End-to-end run, evaluation, rehearsal"),
    ]
    y = Y0 + 0.25
    line_x = X0 + 0.42
    box(s, line_x - 0.025, y + 0.2, 0.05, 3.75, fill=WELL.main)
    for when, what in tl:
        box(s, line_x - 0.17, y + 0.08, 0.34, 0.34, fill=WELL.main, shape=MSO_SHAPE.OVAL)
        text(s, line_x + 0.35, y - 0.02, lw - 0.95, 1.15, [P(B(when), size=22, color=WELL.dark, space_after=2),
                                                         P(what, space_after=0)], size=20, margin=0.02)
        y += 1.22
    rx = X0 + lw + 0.3
    rw = X1 - rx
    card(s, rx, Y0 + 0.03, rw, 3.0, "Test environment", [
        P("Windows 11 laptop", bullet=True, space_after=3),
        P("PostgreSQL 16 + Redis (WSL)", bullet=True, space_after=3),
        P("Chrome + extension", bullet=True, space_after=3),
        P("Test Gmail / YouTube", bullet=True, space_after=0),
    ], accent=STORE, size=20, heading_size=22)
    card(s, rx, Y0 + 3.2, rw, Y1 - Y0 - 3.2, "Team roles in testing", [
        P(Run("[to be added by the team]", color=GREY, italic=True), space_after=0)], accent=STORE, size=20,
        heading_size=22)
    notes(s, "~30 s. 6-19 Oct: every file reviewed with offline simulation tests (fake DB, fake Redis, FastAPI "
             "TestClient) and thread-safety tests — e.g. intent gates 22/22 boundary cases. 21-22 Oct: cosine "
             "thresholds (quality gate 0.35, Stage-3 centroid, KNN-only) re-measured on local data after the model "
             "choice (A39). 22-25 Oct: build with unit + integration checks; code freeze Sun 25 Oct (E2). "
             "26-27 Oct: end-to-end run of all 3 sources, retrieval evaluation set, privacy check, demo rehearsal + "
             "video. PLACEHOLDER: the template asks for each member's role in testing — fill in.")


def slide_test_methods(deck: Deck) -> None:
    s = deck.clone(deck.find("Test Plan and Strategy"), "Test Plan and Strategy (2/2) – Methods")
    set_background(s, bg("base"))
    half = (W - 0.25) / 2
    h1 = 2.75
    card(s, X0, Y0 + 0.03, half, h1, "Functional", [
        P(B("Unit: "), "gates at every boundary", bullet=True, space_after=5),
        P(B("Integration: "), "routes via TestClient", bullet=True, space_after=5),
        P(B("End-to-end: "), "capture → recall", bullet=True, space_after=0),
    ], accent=CAPTURE, size=20, heading_size=23)
    card(s, X0 + half + 0.25, Y0 + 0.03, half, h1, "Non-functional", [
        P(B("Performance: "), "latency, LLM calls", bullet=True, space_after=5),
        P(B("Privacy: "), "inspect every LLM payload", bullet=True, space_after=5),
        P(B("Reliability: "), "fallback when down", bullet=True, space_after=0),
    ], accent=CLOUD, size=20, heading_size=23)
    y2 = Y0 + 0.03 + h1 + 0.2
    card(s, X0, y2, W, 1.05, "", [P(B("Tools: "), "Python test scripts · FastAPI TestClient · tsc + vite build · "
                                     "DevTools", space_after=0)], accent=STORE, size=20, anchor=MSO_ANCHOR.MIDDLE)
    y3 = y2 + 1.25
    card(s, X0, y3, half, Y1 - y3, "Why", [P("Fast; never touches personal data", space_after=0)], accent=WELL,
         size=20, heading_size=22, anchor=MSO_ANCHOR.MIDDLE)
    card(s, X0 + half + 0.25, y3, half, Y1 - y3, "Drawback", [P("Can miss live API changes", space_after=0)],
         accent=AMBER, size=20, heading_size=22, anchor=MSO_ANCHOR.MIDDLE)
    notes(s, "~30 s. Unit: intent gates at every boundary (22/22), URL rules, text cleaning, time phrases. "
             "Integration: routes through FastAPI TestClient; connectors with fake Redis / DB. End-to-end: live "
             "capture → enrichment → recall on the test account. Performance: query latency, LLM calls per question, "
             "embedding speed. Privacy: inspect every LLM payload; sensitive-page probe URLs. Reliability: LLM, "
             "Redis or backend down → defined fallback. Drawback example: YouTube's caption endpoint was found empty "
             "only by a live probe (A11).")


def slide_results(deck: Deck) -> None:
    s = deck.keep(deck.find("Results and Discussion"), "Results and Discussion (1/2) – Measured")
    set_background(s, bg("results"))
    finds = [
        ("6 of 8", "mails lost text to signature removal", "step removed"),
        ("1 of 20", "items classified by seed centroids", "richer seeds"),
        ("0.90 vs 0.15", "same topic vs same source (cosine)", "learns topic"),
        ("19 of 60", "student domains in the domain map", "map grown"),
        ("261 tokens", "1,346-char mail; model reads 256", "new model"),
        ("6.1 → 0.5 s", "model load with offline cache", "faster start"),
    ]
    y = Y0 + 0.03
    rh = (Y1 - y - 5 * 0.1) / 6
    for big, what, change in finds:
        panel(s, X0, y, W, rh)
        text(s, X0 + 0.1, y, 2.75, rh, [P(B(big), align=PP_ALIGN.CENTER, space_after=0)], size=24,
             color=AMBER.main, anchor=MSO_ANCHOR.MIDDLE, margin=0.02)
        text(s, X0 + 2.95, y, 6.3, rh, [P(what, space_after=0)], size=20, anchor=MSO_ANCHOR.MIDDLE, margin=0.04)
        chip(s, X1 - 2.55, y + (rh - 0.48) / 2, 2.4, 0.48, "→ " + change, AMBER, size=18, solid=False)
        y += rh + 0.1
    notes(s, "~40 s. Real numbers from the code review. 6 of 8 test mails lost real body text to the signature "
             "heuristic → step removed (A12). Seed centroids fired on 1 of 20 realistic items at the doc's 0.55 "
             "cut-off → richer seeds + measured cut-off (A18). Cosine 0.90 for the same topic across sources vs 0.15 "
             "for the same source and another topic → centroids learn the topic, balanced per source (A20). The "
             "domain map covered 19 of 60 typical student domains → curated map grown from real data (A17). A "
             "1,346-char mail was 261 tokens but the model reads 256, so the sender hint was cut → hints first, "
             "model change (A16, A40). Model load 6.1 s → 0.5 s with the offline cache (embedding_generator review).")


def slide_results_eval(deck: Deck) -> None:
    s = deck.clone(deck.find("Results and Discussion"), "Results and Discussion (2/2) – Evaluation")
    set_background(s, bg("results"))
    lw = 7.5
    metrics = [
        ("Retrieval quality", "Recall@5 · MRR"),
        ("Hybrid vs single", "keyword · semantic · RRF"),
        ("Latency", "median · p95 seconds"),
        ("LLM calls", "mean per question"),
        ("Cost", "₹ per question"),
        ("Enrichment", "items per minute"),
    ]
    y = Y0 + 0.03
    rh = (Y1 - y - 5 * 0.1) / 6
    for name, how in metrics:
        panel(s, X0, y, lw, rh)
        text(s, X0 + 0.1, y, lw - 2.75, rh, [P(B(name), size=20, space_after=0), P(how, size=18, color=GREY,
                                                                                    space_after=0)],
             anchor=MSO_ANCHOR.MIDDLE, margin=0.08)
        text(s, X0 + lw - 2.55, y + (rh - 0.5) / 2, 2.4, 0.5, [P("to be measured", align=PP_ALIGN.CENTER,
                                                                 space_after=0)],
             size=18, color=GREY, fill=RGBColor(0xF3, 0xF5, 0xFA), line=LINE_SOFT, shape=MSO_SHAPE.ROUNDED_RECTANGLE,
             radius=0.5, anchor=MSO_ANCHOR.MIDDLE, margin=0.02, line_w=1)
        y += rh + 0.1
    rx = X0 + lw + 0.3
    rw = X1 - rx
    card(s, rx, Y0 + 0.03, rw, Y1 - Y0 - 0.03, "Plan vs final", [
        P(B("2 → up to 6"), " LLM calls", space_after=2), P("agentic search (A26)", size=18, color=GREY,
                                                             space_after=14),
        P(B("5 → 20 min"), " session gap", space_after=2), P("long reads split (C24)", size=18, color=GREY,
                                                             space_after=14),
        P(B("Signatures kept"), space_after=2), P("removal cut real text (A12)", size=18, color=GREY, space_after=0),
    ], accent=AMBER, size=20, heading_size=23)
    notes(s, "~30 s. PLACEHOLDERS: measure on the local dataset on 26-27 Oct and replace every 'to be measured' "
             "(Recall@5 and MRR on the evaluation set; keyword-only vs semantic-only vs RRF + rerank; median and "
             "p95 latency; mean LLM calls per question (cap 6); cost per question; enrichment throughput). Do not "
             "present estimates as results.")


def slide_timeline(deck: Deck) -> None:
    s = deck.keep(deck.find("Timeline"))
    set_background(s, bg("base"))
    lw = 3.75
    panel(s, X0, Y0 + 0.03, lw, Y1 - Y0 - 0.03)
    text(s, X0, Y0 + 0.15, lw, 1.1, [P(B("100%"), align=PP_ALIGN.CENTER, space_after=0)], size=60, color=BLUE,
         anchor=MSO_ANCHOR.MIDDLE)
    text(s, X0, Y0 + 1.2, lw, 0.45, [P("implemented", align=PP_ALIGN.CENTER, space_after=0)], size=20, color=NAVY)
    modules = [("Capture", CAPTURE), ("Storage", STORE), ("Enrichment", ENRICH), ("Recall + MCP", RECALL),
               ("Wellbeing", WELL), ("Dashboard", RECALL)]
    y = Y0 + 1.8
    for name, acc in modules:
        box(s, X0 + 0.22, y + 0.13, 0.22, 0.22, fill=acc.main, shape=MSO_SHAPE.OVAL)
        text(s, X0 + 0.55, y, lw - 1.75, 0.48, [P(name, space_after=0)], size=20, anchor=MSO_ANCHOR.MIDDLE,
             margin=0.02)
        chip(s, X0 + lw - 1.15, y + 0.04, 1.0, 0.42, "done", WELL)
        y += 0.53
    rx = X0 + lw + 0.3
    rw = X1 - rx
    tasks = [
        ("6 – 19 Oct", "Every file reviewed", WELL),
        ("18 – 21 Oct", "All decisions closed", WELL),
        ("22 – 25 Oct", "Decisions built, frozen", WELL),
        ("26 – 27 Oct", "Measured, rehearsed", WELL),
        ("Wed 28 Oct", "Review 3", AMBER),
    ]
    y = Y0 + 0.03
    th = (Y1 - y - 4 * 0.12) / 5
    for when, head, acc in tasks:
        text(s, rx, y, 2.15, th, [P(B(when), align=PP_ALIGN.CENTER, space_after=0)], size=20, color=WHITE,
             fill=acc.main, shape=MSO_SHAPE.PENTAGON, radius=0.25, anchor=MSO_ANCHOR.MIDDLE, margin=0.04)
        panel(s, rx + 2.25, y, rw - 2.25, th)
        text(s, rx + 2.4, y, rw - 3.85, th, [P(B(head), space_after=0)], size=21, anchor=MSO_ANCHOR.MIDDLE,
             margin=0.02)
        chip(s, X1 - 1.4, y + th / 2 - 0.22, 1.25, 0.44, "today" if acc is AMBER else "done",
             AMBER if acc is AMBER else WELL)
        y += th + 0.12
    notes(s, "~30 s. 6-19 Oct: every file reviewed (10 review commits: logging, thread safety, dead code, fixes). "
             "18-21 Oct: every open design decision closed (A1-A43; A44/A45 are stretch). 22-25 Oct: decisions built; code freeze Sun 25 Oct. "
             "26-27 Oct: evaluation numbers, end-to-end run, demo video. Wed 28 Oct: Review 3 (8 min talk, 8 min "
             "demo, 4 min Q&A).")


def slide_conclusion(deck: Deck) -> None:
    s = deck.keep(deck.find("Conclusion"), "Conclusion and Future Work")
    set_background(s, bg("feature"))
    lw = 7.0
    points = [
        ("One private memory", "Gmail, Chrome, YouTube — all local", CAPTURE),
        ("Ask, don't search", "Explainable search, clickable sources", RECALL),
        ("Time, reflected kindly", "Patterns and groups you control", WELL),
    ]
    y, h = Y0 + 0.03, 1.28
    for i, (head, body, acc) in enumerate(points):
        text(s, X0, y, 0.9, h, [P(B(str(i + 1)), align=PP_ALIGN.CENTER, space_after=0)], size=30, color=WHITE,
             fill=acc.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.15, anchor=MSO_ANCHOR.MIDDLE)
        text(s, X0 + 1.05, y, lw - 1.05, h, [P(B(head), size=23, color=acc.dark, space_after=3),
                                             P(body, space_after=0)],
             size=20, fill=WHITE, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.1, anchor=MSO_ANCHOR.MIDDLE,
             margin=0.2, opacity=0.98)
        y += h + 0.15
    text(s, X0, y, lw, Y1 - y, [P(I("LLM sees only the question + short snippets"), align=PP_ALIGN.CENTER,
                                  space_after=0)],
         size=20, color=BLUE, fill=WHITE, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.3, anchor=MSO_ANCHOR.MIDDLE,
         margin=0.15, opacity=0.98)
    rx = X0 + lw + 0.3
    rw = X1 - rx
    text(s, rx, Y0 + 0.03, rw, 0.6, [P(B("Future work"), space_after=0)], size=24, color=BLUE, fill=WHITE,
         shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.3, anchor=MSO_ANCHOR.MIDDLE, margin=0.18, opacity=0.98)
    future = [
        ("Long documents", "several vectors per item"),
        ("Self-learning domain map", "rules learned from use"),
        ("Import your history", "Takeout + Chrome History"),
    ]
    y = Y0 + 0.8
    fh = (Y1 - y - 2 * 0.15) / 3
    for head, body in future:
        card(s, rx, y, rw, fh, head, [P(body, space_after=0)], accent=ENRICH, size=20, heading_size=21,
             anchor=MSO_ANCHOR.MIDDLE)
        y += fh + 0.15
    notes(s, "~30 s. Future work: chunking long documents into several vectors per item (A24.1); a self-learning "
             "domain map — a domain becomes a rule after the classifier agrees N times (A17); importing history from "
             "Google Takeout (YouTube) and the Chrome History file, plus an export button (E2). Echo as an MCP "
             "server (E1) is now built, not future work. A local LLM is deliberately NOT listed (A31.5). Privacy "
             "line: OpenAI keeps chats ~30 days so follow-ups continue (A26 store=true).")


def slide_references(deck: Deck) -> None:
    s = deck.keep(deck.find("References"))
    set_background(s, bg("base"))
    refs = [
        "[1] N. Reimers and I. Gurevych, \"Sentence-BERT: Sentence embeddings using Siamese BERT-networks,\" "
        "in Proc. EMNLP, 2019.",
        "[2] J. Johnson, M. Douze, and H. Jégou, \"Billion-scale similarity search with GPUs,\" IEEE Trans. "
        "Big Data, vol. 7, no. 3, pp. 535–547, 2021.",
        "[3] G. V. Cormack, C. L. A. Clarke, and S. Büttcher, \"Reciprocal rank fusion outperforms Condorcet and "
        "individual rank learning methods,\" in Proc. ACM SIGIR, 2009, pp. 758–759.",
        "[4] R. Nogueira and K. Cho, \"Passage re-ranking with BERT,\" arXiv:1901.04085, 2019.",
        "[5] LangChain and LangGraph Documentation, https://python.langchain.com, 2024.",
        "[6] M. Kleppmann, A. Wiggins, P. van Hardenberg, and M. McGranaghan, \"Local-first software: You own your "
        "data, in spite of the cloud,\" in Proc. ACM Onward!, 2019.",
    ]
    panel(s, X0, Y0 + 0.03, W, Y1 - Y0 - 0.03)
    text(s, X0 + 0.25, Y0 + 0.15, W - 0.5, Y1 - Y0 - 0.25, [P(r, space_after=10) for r in refs], size=19,
         color=BLUE)
    notes(s, "[1], [2], [5], [6] are the Review 2 references (shortened venue names). [3] RRF and [4] cross-encoder "
             "re-ranking are new: they back the two ranking steps.")


def slide_thanks(deck: Deck) -> None:
    s = deck.keep(deck.template_slides[-1], strip=False)
    set_background(s, bg("thanks"))
    text(s, 3.2, 6.1, 6.9, 0.8, [P(B("Questions?"), align=PP_ALIGN.CENTER, space_after=0)], size=32, color=WHITE,
         anchor=MSO_ANCHOR.MIDDLE)
    notes(s, "Thank you — questions (4 minutes).")


def build(output: pathlib.Path = OUTPUT) -> pathlib.Path:
    make_assets.ensure()
    deck = Deck()
    slide_title(deck)
    if INCLUDE_GUIDE_APPROVAL_SLIDE:
        slide_guide_approval(deck)
    slide_outline(deck)
    slide_abstract(deck)
    slide_suggestions(deck)
    slide_architecture(deck)
    slide_changes(deck)
    slide_impl_capture(deck)
    slide_impl_enrich(deck)
    slide_impl_recall(deck)
    slide_impl_wellbeing(deck)
    slide_demo(deck)
    slide_test_plan(deck)
    slide_test_methods(deck)
    slide_results(deck)
    slide_results_eval(deck)
    slide_timeline(deck)
    slide_conclusion(deck)
    slide_references(deck)
    slide_thanks(deck)
    if len(deck.order) != len({id(s) for s in deck.order}):
        raise RuntimeError("A slide was added twice")
    smallest = deck.save(output)
    print(f"Smallest font on any slide: {smallest:g} pt (minimum allowed {MIN_FONT_PT:g} pt)")
    return output


if __name__ == "__main__":
    # Optional argument: write somewhere else (e.g. a temp file while the real deck is open in PowerPoint).
    print(build(pathlib.Path(sys.argv[1]).resolve()) if len(sys.argv) > 1 else build())
