"""
Build the E.C.H.O. Capstone Phase 3 Review 3 deck from the official PES Review 3 template.

How it works (same idea as build_deck.py for Review 2, which is left untouched):
  - Every template slide is kept, in the template's order. Its instruction text box is removed and our
    content is drawn in its place; the template's own red title, cyan rule and PES logo stay.
  - Sections that need more than one slide (Pipeline max 2, Implementation max 4, Test Plan max 2,
    Results max 2) get extra slides that clone that template slide's own title box and rule.
  - Every slide gets a generated background (r3/make_assets.py) set as the SLIDE background, so the
    master's PES logo is still drawn on top of it. Backgrounds are near-white with one blue accent (v3, restrained).
  - TV readability (owner rule, 2026-10-21): no text below 18 pt anywhere, body 20-24 pt, titles 32 pt.
    check_font_sizes() walks every run of every slide and FAILS the build if anything is smaller.
  - Diagrams are native PowerPoint shapes (editable, big boxes, labels >= 18 pt), one accent colour.

Content and its sources: r3/slide_text_draft.md (v3 text) and r3/approved_content_r3.md. Detail that does not fit at TV size lives in each slide's
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


PRIMARY = Accent(RGBColor(0x1F, 0x4F, 0xE0), RGBColor(0xE9, 0xEF, 0xFD), RGBColor(0x0B, 0x22, 0x72))
# Owner feedback 2026-10-22: "too colourful". One accent for every module; grey for stores and detail.
CAPTURE = ENRICH = RECALL = WELL = PRIMARY
STORE = Accent(RGBColor(0x4A, 0x55, 0x6E), RGBColor(0xEE, 0xF1, 0xF7), RGBColor(0x1E, 0x26, 0x3A))
CLOUD = Accent(RGBColor(0x5B, 0x6B, 0x8C), RGBColor(0xF3, 0xF5, 0xFA), RGBColor(0x24, 0x2F, 0x4A))
AMBER = STORE
BASE = PRIMARY

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
          dash: bool = False, head: bool = True):
    c = slide.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, Inches(x1), Inches(y1), Inches(x2), Inches(y2))
    c.line.color.rgb = color
    c.line.width = Pt(width)
    if dash:
        c.line.dash_style = MSO_LINE_DASH_STYLE.DASH
    if head:
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


# ── Slide helpers ─────────────────────────────────────────────────────────────

CODE_FILL = RGBColor(0xF1, 0xF4, 0xFB)
MODULE_1 = "Module 1 · Memory Capture and Enrichment"
MODULE_2 = "Module 2 · Retrieval and Synthesis Engine (RSE)"
MODULE_3 = "Module 3 · Digital Wellbeing and Analytics (WBA)"


def header_strip(s, y: float, h: float, module: str, tech: str) -> None:
    """The template asks for 'module name, technology used': both go on one white card at the top."""
    panel(s, X0, y, W, h)
    box(s, X0, y + 0.1, 0.08, h - 0.2, fill=PRIMARY.main)
    text(s, X0 + 0.2, y + 0.03, W - 0.3, h - 0.06, [
        P(B(module), size=22, color=PRIMARY.dark, space_after=2),
        P(B("Technology: "), tech, size=18, color=NAVY, space_after=0),
    ], anchor=MSO_ANCHOR.MIDDLE, margin=0.06)


def code_block(s, x: float, y: float, w: float, h: float, title: str, lines: list[str]) -> None:
    paras = [P(B(title), size=18, color=GREY, space_after=3)]
    paras += [P(line, font=MONO, size=18, color=NAVY, space_after=0) for line in lines]
    text(s, x, y, w, h, paras, size=18, fill=CODE_FILL, line=LINE_SOFT, shape=MSO_SHAPE.ROUNDED_RECTANGLE,
         radius=0.04, anchor=MSO_ANCHOR.MIDDLE, margin=0.16)


def step_row(s, x: float, y: float, w: float, h: float, n: int, label: str, *, size: float = 20) -> None:
    panel(s, x, y, w, h)
    text(s, x + 0.14, y + (h - 0.46) / 2, 0.46, 0.46, [P(B(str(n)), align=PP_ALIGN.CENTER, space_after=0)], size=18,
         color=WHITE, fill=PRIMARY.main, shape=MSO_SHAPE.OVAL, anchor=MSO_ANCHOR.MIDDLE, margin=0.0)
    text(s, x + 0.72, y, w - 0.82, h, [P(label, space_after=0)], size=size, anchor=MSO_ANCHOR.MIDDLE, margin=0.04)


def white_pill(s, x: float, y: float, w: float, h: float, paras: list[ParaLike], size: float = 20) -> None:
    text(s, x, y, w, h, paras, size=size, fill=WHITE, line=LINE_SOFT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.12,
         anchor=MSO_ANCHOR.MIDDLE, margin=0.18)


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
    # Navy band content.
    text(s, X0, 6.32, 8.4, 0.62, [P(B("Your private, searchable memory."), space_after=0)],
         size=24, color=WHITE, anchor=MSO_ANCHOR.MIDDLE)
    for i, name in enumerate(["gmail", "chrome", "youtube"]):
        x = 9.55 + i * 1.0
        box(s, x, 6.22, 0.82, 0.82, fill=WHITE, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.2)
        logo(s, name, x + 0.11, 6.33, 0.6)
    notes(s, "Title. 8-minute talk, 8-minute demo, 4 minutes of questions.")


def slide_guide_approval(deck: Deck) -> None:
    s = deck.clone(deck.find("Outline"), "Guide Approval Mail")
    set_background(s, bg("plain"))
    picture(s, ASSETS / "guide_approval_mail.png", X0, Y0 + 0.03, W, Y1 - Y0 - 0.03)
    notes(s, "Screenshot of the guide's approval mail (Fri 23 Oct 2026, 7:32 PM): 'Dear Students your PPT is approved'. "
             "It is not part of the Review 3 template; delete this slide if it is not wanted "
             "(build_deck_r3.py: INCLUDE_GUIDE_APPROVAL_SLIDE = False).")


def slide_outline(deck: Deck) -> None:
    s = deck.keep(deck.find("Outline"), strip=False)   # the template's agenda; its slide-count notes are dropped
    set_background(s, bg("base"))
    for sh in s.shapes:
        if sh.has_text_frame and "Abstract and Scope" in sh.text_frame.text:
            for para in sh.text_frame.paragraphs:
                for run in para.runs:
                    run.text = re.sub(r"\s*\((?:max )?\d slides?\)|\s*\(keep the demo video ready\)", "", run.text)
    notes(s, "Outline exactly as the template asks.")


def slide_abstract(deck: Deck) -> None:
    s = deck.keep(deck.find("Abstract and Scope"))
    set_background(s, bg("base"))
    top_h = 1.05
    ty = Y0 + 0.03
    panel(s, X0, ty, W, top_h)
    for i, name in enumerate(["gmail", "chrome", "youtube"]):
        logo(s, name, X0 + 0.2 + i * 0.85, ty + (top_h - 0.7) / 2, 0.7)
    arrow(s, X0 + 2.8, ty + top_h / 2, X0 + 3.3, ty + top_h / 2, color=BLUE, width=3)
    text(s, X0 + 3.45, ty, W - 3.55, top_h, [
        P(B("The problem. "), "A student gets an interview email, reads articles and watches tutorials. A week later "
          "it is spread over Gmail, Chrome and YouTube, and no tool searches all three.", space_after=0)],
        size=19, anchor=MSO_ANCHOR.MIDDLE)
    pillars = [
        ("Capture", ["Every email is kept", "Pages and videos only if you engaged", "Sensitive pages never stored"]),
        ("Recall", ["One question, all sources", "Links to the original", "Follow-ups keep context"]),
        ("Reflect", ["Time per category", "Sessions and regret you mark", "Groups change only with your yes"]),
    ]
    top, ph = ty + top_h + 0.12, 2.55
    pw = (W - 2 * 0.25) / 3
    for i, (name, points) in enumerate(pillars):
        card(s, X0 + i * (pw + 0.25), top, pw, ph, name, [P(p, bullet=True, space_after=5) for p in points],
             accent=PRIMARY, size=19, heading_size=24)
    by = top + ph + 0.12
    half = (W - 0.25) / 2
    white_pill(s, X0, by, half, Y1 - by, [P(B("Scope. "), "One user, one laptop: desktop Chrome, Gmail and YouTube. "
                                           "Not a cloud service, a score or a blocker.", space_after=0)], size=19)
    white_pill(s, X0 + half + 0.25, by, half, Y1 - by, [
        P(B("Local-first. "), "Storage and processing run on your laptop. The language model gets your question and "
          "limited excerpts of the best matches, never database rows.", space_after=0)], size=19)
    notes(s, "~45 s. The problem in one line (interview email, articles, tutorials, spread over three places). Then "
             "the three things E.C.H.O. does. Scope: one user, one laptop, desktop Chrome plus the Gmail API and "
             "YouTube. Privacy: storage and processing are local; the language model gets the question and "
             "limited excerpts (the first 8,000 characters of the top 3 results, 2,000 of the others), never database rows or "
             "the whole store; the Gmail and YouTube APIs fetch your own data.")


def slide_suggestions(deck: Deck) -> None:
    s = deck.keep(deck.find("Suggestions from Review"), "Suggestions from Review – 1 & 2")
    set_background(s, bg("base"))
    rows = [
        ("Review 1", "Data retention", "A setting: keep forever, 1 year or 6 months. Older items are deleted from the "
                                       "database, the vector index and the cache."),
        ("Review 1", "Spam filtering", "Gmail's own filter is our spam filter. Spam, Trash and draft mail is never read."),
    ]
    cols = [1.55, 2.5, 6.1, 1.78]
    xs = [X0]
    for c in cols[:-1]:
        xs.append(xs[-1] + c)
    head_y = Y0 + 0.03
    for x, w, label in zip(xs, cols, ["Review", "Suggestion", "Our response", "Status"]):
        text(s, x, head_y, w, 0.55, [P(B(label))], size=20, color=WHITE, fill=BLUE, anchor=MSO_ANCHOR.MIDDLE,
             margin=0.12)
    y = head_y + 0.7
    rh = 1.9
    for rev, name, resp in rows:
        panel(s, X0, y, W, rh)
        chip(s, xs[0] + 0.12, y + rh / 2 - 0.25, cols[0] - 0.24, 0.5, rev, STORE)
        text(s, xs[1], y, cols[1], rh, [P(B(name), space_after=0)], size=22, anchor=MSO_ANCHOR.MIDDLE, margin=0.12)
        text(s, xs[2], y, cols[2], rh, [P(resp, space_after=0)], size=20, anchor=MSO_ANCHOR.MIDDLE, margin=0.12)
        chip(s, xs[3] + 0.15, y + rh / 2 - 0.25, cols[3] - 0.3, 0.5, "Done", PRIMARY)
        y += rh + 0.12
    notes(s, "~40 s. Review 1 asked for (1) a retention period: now a user setting, default keep forever; a job at "
             "startup deletes older items from PostgreSQL, the FAISS index and Redis. (2) A spam filter: Gmail's own "
             "spam filter is our filter, so Spam and Trash are never read, also in the incremental sync; drafts are "
             "skipped; Promotions and Social mail are kept by choice.")


def slide_architecture(deck: Deck) -> None:
    """Redrawn from the system design document, section 3.1: same boxes, same arrows, one style."""
    s = deck.keep(deck.find("Complete pipeline"), "Complete Pipeline / Architecture of Project")
    set_background(s, bg("base"))
    lap_x, lap_top, lap_bot = 1.95, Y0 + 0.02, 6.05
    box(s, lap_x, lap_top, X1 - lap_x, lap_bot - lap_top, fill=WHITE, line=BLUE, shape=MSO_SHAPE.ROUNDED_RECTANGLE,
        radius=0.03, opacity=0.96, dash=True, line_w=1.75)
    text(s, lap_x + 0.15, lap_top + 0.03, 8.0, 0.42, [P(B("YOUR LAPTOP — everything stays local"))], size=20,
         color=BLUE, margin=0.02)
    cw = [1.5, 1.4, 1.35, 1.7, 1.6, 1.55]
    gap = 0.25
    xs = [lap_x + 0.2]
    for w_ in cw[:-1]:
        xs.append(xs[-1] + w_ + gap)
    heads = ["Browser", "Connectors", "Write path", "Local stores", "Processing", "Interfaces"]
    for x, w_, h in zip(xs, cw, heads):
        text(s, x - 0.1, lap_top + 0.45, w_ + 0.2, 0.4, [P(B(h), align=PP_ALIGN.CENTER, space_after=0)],
             size=18, color=STORE.dark, margin=0.0)
    rows = [3.0, 3.98, 4.96]
    rh = 0.85
    mid = [r + rh / 2 for r in rows]
    right = [x + w_ for x, w_ in zip(xs, cw)]
    # Sources (outside the laptop) → extension.
    for name, y in zip(["gmail", "chrome", "youtube"], rows):
        box(s, X0 + 0.05, y, 1.05, rh, fill=WHITE, line=LINE_SOFT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.12,
            line_w=0.75)
        logo(s, name, X0 + 0.22, y + 0.09, 0.68)
        arrow(s, X0 + 1.12, y + rh / 2, xs[0] - 0.02, y + rh / 2)
    node(s, xs[0], rows[0], cw[0], rows[2] + rh - rows[0], "Extension", "trackers + gate", PRIMARY,
         size=18)
    # Connectors.
    for t, y in zip(["Gmail connector", "Chrome connector", "YouTube connector"], rows):
        node(s, xs[1], y, cw[1], rh, t, "", PRIMARY, size=18)
        arrow(s, right[0] + 0.02, y + rh / 2, xs[1] - 0.02, y + rh / 2)
    # Storage engine (rows 1-2) and the three connectors writing into it.
    st_h = rows[2] + rh - rows[0]
    node(s, xs[2], rows[0], cw[2], st_h, "Storage engine", "", PRIMARY, size=19)
    arrow(s, right[1] + 0.02, mid[0], xs[2] - 0.02, mid[0])
    arrow(s, right[1] + 0.02, mid[1], xs[2] - 0.02, mid[1])
    arrow(s, right[1] + 0.02, mid[2], xs[2] - 0.02, mid[2])
    # Local stores panel: PostgreSQL, FAISS, Redis.
    sp_h = rows[2] + rh - rows[0]
    box(s, xs[3], rows[0], cw[3], sp_h, fill=STORE.tint, line=STORE.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE,
        radius=0.05, line_w=2)
    for i, t in enumerate(["PostgreSQL", "FAISS", "Redis"]):
        node(s, xs[3] + 0.12, rows[0] + 0.12 + i * 0.9, cw[3] - 0.24, 0.78, t, "", STORE, size=18,
             shape=MSO_SHAPE.CAN)
    arrow(s, right[2] + 0.02, rows[0] + st_h / 2, xs[3] - 0.02, rows[0] + st_h / 2)             # write
    # Processing.
    for t, sub, y in zip(["Enrichment worker", "Recall engine", "Wellbeing"], ["", "", ""], rows):
        node(s, xs[4], y, cw[4], rh, t, sub, PRIMARY, size=19)
    arrow(s, xs[4] - 0.02, mid[0] - 0.2, right[3] + 0.02, mid[0] - 0.2)      # ENP reads PostgreSQL rows
    arrow(s, right[3] + 0.02, mid[0] + 0.2, xs[4] - 0.02, mid[0] + 0.2)      # ENP writes vectors and groups
    arrow(s, xs[4] - 0.02, mid[1], right[3] + 0.02, mid[1])                  # recall reads PostgreSQL and FAISS
    arrow(s, xs[4] - 0.02, mid[2], right[3] + 0.02, mid[2])                  # wellbeing reads PostgreSQL
    # Interfaces: dashboard and MCP server call the recall engine and wellbeing.
    node(s, xs[5], rows[0], cw[5], rh, "MCP server", "", PRIMARY, size=19)
    node(s, xs[5], rows[1], cw[5], rows[2] + rh - rows[1], "Dashboard", "React", PRIMARY, size=19)
    arrow(s, xs[5] - 0.02, mid[0] + 0.2, right[4] + 0.02, mid[1] - 0.2)          # MCP -> recall
    arrow(s, xs[5] - 0.02, mid[1], right[4] + 0.02, mid[1])                       # dashboard -> recall
    arrow(s, xs[5] - 0.02, mid[2], right[4] + 0.02, mid[2])                       # dashboard -> wellbeing
    # Cloud row: the only things that leave the laptop.
    cy = 6.2
    text(s, X0, cy, 5.2, Y1 - cy, [P(B("Gmail and YouTube APIs → "), "your own mail and video details", space_after=0)],
         size=19, color=STORE.dark, fill=WHITE, line=STORE.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.2,
         anchor=MSO_ANCHOR.MIDDLE, margin=0.14, dash=True, line_w=1.5)
    text(s, 6.15, cy, X1 - 6.15, Y1 - cy, [P(B("OpenAI language model ← "), "your question and limited excerpts, never database rows",
                                            space_after=0)],
         size=19, color=CLOUD.dark, fill=CLOUD.tint, line=CLOUD.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.2,
         anchor=MSO_ANCHOR.MIDDLE, margin=0.14, dash=True, line_w=1.5)
    arrow(s, xs[1] + cw[1] / 2, lap_bot + 0.01, xs[1] + cw[1] / 2, cy - 0.02, color=STORE.main, dash=True)
    arrow(s, xs[4] + cw[4] / 2, lap_bot + 0.01, xs[4] + cw[4] / 2, cy - 0.02, color=CLOUD.main, dash=True)
    notes(s, "~60 s. Read it left to right: the life of one memory. Mail comes in through the Gmail API (checked every "
             "30 s); pages and videos come in through the Chrome extension, whose background script is the only way "
             "data leaves the browser. Each connector writes through ONE storage engine, one transaction per item, "
             "into PostgreSQL (the permanent truth); FAISS holds the vectors and Redis the revisit flags and "
             "settings. The enrichment worker runs in the background: it reads new rows, writes vectors and groups, "
             "and asks the language model only about unclear items. The recall engine and the wellbeing module only "
             "read. The dashboard and the MCP server (Claude Desktop) call them. Only two things leave the laptop: "
             "the Google APIs, which return your own data, and the language model, which gets the question and limited "
             "excerpts. Wellbeing sends it weekly totals only.")


def slide_impl_capture(deck: Deck) -> None:
    s = deck.keep(deck.find("Implementation Details"), "Implementation – Module 1: Capture (1/2)")
    set_background(s, bg("base"))
    hs_y, hs_h = Y0 + 0.03, 1.1
    header_strip(s, hs_y, hs_h, MODULE_1 + " — Part A: Capture",
                 "Chrome extension (Manifest V3) · FastAPI · Gmail API and YouTube Data API · PostgreSQL · Redis")
    cols = [
        ("chrome", "Chrome", ["5 s seen, then any one of:", "10 s, 15 % scroll, a click", "or a return within 24 h"]),
        ("youtube", "YouTube", ["Short 10 s · video 20 s", "or 10 % watched (≥ 5 s)", "or a return, pause or seek"]),
        ("gmail", "Gmail", ["Every mail is kept", "New mail every 30 s", "No Spam, Trash or drafts"]),
    ]
    top, ch = hs_y + hs_h + 0.12, 2.0
    cw = (W - 2 * 0.25) / 3
    for i, (lg, name, bullets) in enumerate(cols):
        x = X0 + i * (cw + 0.25)
        panel(s, x, top, cw, ch)
        box(s, x, top, cw, 0.08, fill=PRIMARY.main)
        logo(s, lg, x + 0.2, top + 0.2, 0.55)
        text(s, x + 0.95, top + 0.17, cw - 1.05, 0.62, [P(B(name), space_after=0)], size=24, color=PRIMARY.dark,
             anchor=MSO_ANCHOR.MIDDLE, margin=0.02)
        text(s, x + 0.12, top + 0.82, cw - 0.24, ch - 0.86, [P(b, bullet=True, space_after=2) for b in bullets],
             size=19, margin=0.06)
    by = top + ch + 0.12
    bh = Y1 - by
    lw = 6.3
    code_block(s, X0, by, lw, bh, "Chrome gate (extension)", [
        "if seconds_in_front >= 5:      # phase 1",
        "    keep = active >= 10 s or scroll >= 15 %",
        "        or clicked or opened_again_in_24h"])
    card(s, X0 + lw + 0.2, by, W - lw - 0.2, bh, "Never captured", [
        P("Incognito windows · typed text, passwords, forms · sensitive pages · app-page text (title only) · "
          "secret URL keys", space_after=0)], accent=STORE, size=19, heading_size=21)
    notes(s, "~45 s. Chrome: the extension counts only active time (tab in front, window focused, not idle for 30 s). "
             "Phase 1: 5 s in front. Phase 2, any one of: 10 s active, scrolled 15 %, a click or text selection (link "
             "clicks do not count), or the same page again within 24 h. The server repeats the same checks because it "
             "never trusts the client. YouTube: watched 20 s (Short 10 s), or at least 10 % with 5 s watched, or seen "
             "again within 24 h, or paused/seeked by you. Gmail: every mail is kept; the poll runs every 30 s and asks "
             "only for new mail; Spam, Trash and drafts are skipped; reading time is recorded when you open a mail. "
             "Sensitive pages are matched by whole words (login, payment, settings ...); app pages (Slack, Notion, "
             "Jira) keep only the title; secret keys (token, code, key ...) are removed from URLs before storing. "
             "Each item is stored in one atomic transaction, and the same item is never stored twice.")


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
    s = deck.clone(deck.find("Implementation Details"), "Implementation – Module 1: Enrichment (2/2)")
    set_background(s, bg("base"))
    hs_y, hs_h = Y0 + 0.03, 0.95
    header_strip(s, hs_y, hs_h, MODULE_1 + " — Part B: Enrichment",
                 "Python worker thread · sentence-transformers · FAISS · PostgreSQL · OpenAI")
    cy = hs_y + hs_h + 0.1
    _chevron_row(s, X0, cy, W, 0.9, [
        ("Clean", "mail HTML → text"), ("Build text", "hints first"), ("Embed", "384-d vector"),
        ("Classify", "5 groups"), ("Index", "into FAISS"),
    ], PRIMARY)
    top = cy + 0.9 + 0.12
    stages = [
        "1 · Structural signals: labels, .edu",
        "2 · Curated domain list (Chrome)",
        "3 · Nearest centroid: meaning",
        "4 · Language model: unclear items",
    ]
    fh, fgap, full = 0.5, 0.07, 6.5
    for i, label in enumerate(stages):
        inset = i * 0.25
        text(s, X0 + inset, top + i * (fh + fgap), full - 2 * inset, fh,
             [P(B(label), align=PP_ALIGN.CENTER, space_after=0)], size=19, color=PRIMARY.dark, fill=PRIMARY.tint,
             line=PRIMARY.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.25, anchor=MSO_ANCHOR.MIDDLE,
             margin=0.05, line_w=2)
    fun_h = 4 * fh + 3 * fgap
    rx = X0 + full + 0.25
    card(s, rx, top, X1 - rx, fun_h, "Example", [
        P("Interview mail → work-list sender → ", B("work"), space_after=3),
        P("geeksforgeeks.org → domain list → ", B("study"), space_after=3),
        P("Unlisted blog → centroid, else the model", space_after=0)], accent=PRIMARY, size=18, heading_size=20)
    y = top + fun_h + 0.1
    white_pill(s, X0, y, W, Y1 - y, [
        P(B("The first stage that is sure wins. "), "Output: work · study · entertainment · personal · misc.", size=19,
          space_after=2),
        P(B("Embedding model: "), EMBEDDING_MODEL + " · " + EMBEDDING_FACTS, size=19, space_after=0)], size=19)
    notes(s, "~45 s. The worker runs as a background thread in the backend: every 10 s it picks up to 10 new rows, "
             "oldest first, and never blocks capture. Clean: mail HTML to text and quoted replies cut (signatures are "
             "kept on purpose: removing them cut real text in 6 of 8 test mails); page text arrives already extracted "
             "with its headings; attachment text is extracted here. Build text: short hints first (subject, keywords, "
             "sender domain or title, channel), long text last, cut at the model's own 512-token window; what is "
             "stored is exactly what was embedded. Classify: 1 structural signals; 2 curated domain list; 3 nearest "
             "centroid (meaning compared with example vectors per group, refreshed from confirmed items when the "
             "worker starts and the file is over 30 days old); 4 the language model sees the title and a 300-character "
             "snippet and answers with one of the five names. Unsure or failed -> misc; a row never has an empty "
             "group. The FAISS file is saved before rows are marked done, so a crash loses nothing. The embedding "
             "model was chosen over all-MiniLM-L6-v2 after a 39-question comparison on our own test set (right item "
             "first: 37 of 39 vs 31 of 39).")


def slide_impl_recall(deck: Deck) -> None:
    s = deck.clone(deck.find("Implementation Details"), "Implementation – Module 2: Recall (1/2)")
    set_background(s, bg("base"))
    hs_y, hs_h = Y0 + 0.03, 1.1
    header_strip(s, hs_y, hs_h, MODULE_2 + " — how a question is answered",
                 "LangGraph + LangChain · OpenAI gpt-6-luna · PostgreSQL full-text search · FAISS · local "
                 "cross-encoder · FastAPI · React · MCP")
    steps = [
        "Model calls the search tool (≤ 5 rounds)", "Time phrase → hard date filter",
        "Keyword and meaning search together", "Merge by rank (RRF, k = 60)",
        "Weak results? Widen, up to 3 times", "Re-rank: cross-encoder + effort",
        "Add thread / ±5-minute context", "Answer with numbered citations",
    ]
    top = hs_y + hs_h + 0.12
    rh, rg = 0.56, 0.06
    cw = (W - 0.2) / 2
    for i, label in enumerate(steps):
        col, row = divmod(i, 4)
        step_row(s, X0 + col * (cw + 0.2), top + row * (rh + rg), cw, rh, i + 1, label, size=19)
    by = top + 4 * rh + 3 * rg + 0.12
    bh = Y1 - by
    lw = 7.3
    code_block(s, X0, by, lw, bh, "Ranking (rse/fusion.py, rse/reranker.py)", [
        "rrf(item)   = sum(1 / (60 + rank)) per list",
        "final(item) = 0.8 * cross_encoder + 0.2 * effort"])
    tw = W - lw - 0.2
    for i, (big, small) in enumerate([("2.3 mean", "model calls per question (2 – 6)")]):
        text(s, X0 + lw + 0.2 + i * (tw + 0.2), by, tw, bh, [
            P(B(big), size=24, color=PRIMARY.main, align=PP_ALIGN.CENTER, space_after=0),
            P(small, size=18, align=PP_ALIGN.CENTER, space_after=0, color=GREY)],
            fill=WHITE, line=LINE_SOFT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.12, anchor=MSO_ANCHOR.MIDDLE,
            margin=0.06, line_w=0.75)
    notes(s, "~60 s, the core. The model decides WHAT to search for; HOW the search runs is fixed Python, and the "
             "model never touches the database. It calls one read-only tool, search_memories (query, sources, a time "
             "phrase such as 'after the interview email', an optional sender). Rounds are capped at 5, then it must "
             "answer: usually 2 model calls, at most 6, enforced in code. Python resolves the time phrase to a hard "
             "date filter, runs PostgreSQL full-text search and FAISS meaning search (three rephrasings) at the same "
             "time, and merges by rank with Reciprocal Rank Fusion. A quality check can widen the search up to three "
             "times. A local cross-encoder (ms-marco-MiniLM-L-6-v2) reads the question and each item together; the "
             "final score is 0.8 x cross-encoder + 0.2 x effort (time spent, scroll, revisits). Then the thread or "
             "the items within 5 minutes are added as context, and the model writes the answer from the top 10 "
             "results (8,000 characters of text for the best 3, 2,000 for the rest). Recency boost and diversity re-ranking are deliberately not used. If the "
             "model is down, a deterministic search runs on the raw question.")


def slide_impl_recall_example(deck: Deck) -> None:
    s = deck.clone(deck.find("Implementation Details"), "Implementation – Module 2: Recall in action (2/2)")
    set_background(s, bg("base"))
    lw = 6.5
    ty = Y0 + 0.03
    white_pill(s, X0, ty, lw, 0.62, [P(B("Example: "), "“OS material after the interview email”",
                                       space_after=0)], size=19)
    steps = [
        "Model: query = operating systems, time phrase = after the interview email",
        "Python finds the interview email; its time is the filter",
        "Keyword and meaning search run on later items only",
        "Rank fusion merges; the cross-encoder re-orders",
        "Answer cites the two best items as pills",
    ]
    sy = ty + 0.62 + 0.1
    rh, rg = 0.66, 0.05
    for i, label in enumerate(steps):
        step_row(s, X0, sy + i * (rh + rg), lw, rh, i + 1, label, size=18)
    list_bottom = sy + 5 * rh + 4 * rg
    # Right: chat illustration.
    rx = X0 + lw + 0.25
    rw = X1 - rx
    ph = list_bottom - ty
    panel(s, rx, ty, rw, ph)
    text(s, rx + 0.15, ty + 0.05, rw - 0.3, 0.42, [P(B("Answer format "), I("(illustration)", GREY), space_after=0)],
         size=19, color=PRIMARY.dark, margin=0.02)
    text(s, rx + 0.5, ty + 0.55, rw - 0.65, 0.62, [P("Did Priya send the checklist?", space_after=0)], size=19,
         color=WHITE, fill=PRIMARY.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.3, anchor=MSO_ANCHOR.MIDDLE,
         margin=0.14)
    text(s, rx + 0.15, ty + 1.3, rw - 0.45, 1.0, [P("Yes — ", B("Priya"), " emailed it on ", B("Tue 14 Oct"),
                                                    ", PDF attached.", space_after=0)],
         size=19, fill=PRIMARY.tint, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.15, anchor=MSO_ANCHOR.MIDDLE,
         margin=0.14)
    for j, (lbl, solid) in enumerate([("Priya · 14 Oct", True), ("HR · 15 Oct", False)]):
        py = ty + 2.45 + j * 0.58
        box(s, rx + 0.15, py, 2.65, 0.46, fill=WHITE, line=PRIMARY.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE,
            radius=0.5, line_w=1.75, dash=not solid)
        logo(s, "gmail", rx + 0.28, py + 0.09, 0.28)
        text(s, rx + 0.65, py, 2.1, 0.46, [P(lbl, space_after=0)], size=18, color=PRIMARY.dark,
             anchor=MSO_ANCHOR.MIDDLE, margin=0.02)
    text(s, rx + 0.15, ty + 3.65, rw - 0.3, ph - 3.7, [P(B("Click a pill"), " → the original mail", space_after=0)],
         size=18, anchor=MSO_ANCHOR.MIDDLE, margin=0.04)
    by = list_bottom + 0.12
    white_pill(s, X0, by, W, Y1 - by, [
        P(B("What the model sees: "), "your question and limited excerpts of the best matches (first 8,000 characters of the top 3), never database rows.", space_after=0)], size=19)
    notes(s, "~45 s. The example question is traced from the design; it is not a recorded run, and the chat on the "
             "right is an illustration of the answer format, not a screenshot. Answers are short chat replies; each "
             "citation becomes a memory pill (source logo, sender or site, date). Solid = the item says it, dashed = "
             "related context. Clicking a pill opens the original mail or page. Live progress ('Searching Gmail...') "
             "streams while it works. Follow-ups: each chat has its own session, so the next question continues from "
             "the earlier turns. An attachment question adds the attachment text (up to 40,000 characters). "
             "The MCP server lets Claude Desktop call the same read-only tools: search_memories, get_memory, "
             "recent_activity.")


def slide_impl_wellbeing(deck: Deck) -> None:
    s = deck.clone(deck.find("Implementation Details"), "Implementation – Module 3: Wellbeing (1/2)")
    set_background(s, bg("base"))
    hs_y, hs_h = Y0 + 0.03, 1.1
    header_strip(s, hs_y, hs_h, MODULE_3 + " — Part A: what it measures",
                 "FastAPI router · PostgreSQL (SQLAlchemy) · Redis · OpenAI (weekly numbers only) · React + "
                 "TypeScript + Vite + Tailwind")
    tiles = [
        ("Time", "Per category and source, added up per visit, by day"),
        ("Sessions", "Items under 20 minutes apart form one sitting"),
        ("Regret", "You mark an item yourself. Never scored, never judged"),
        ("Weekly insight", "The model sees only totals and hours. Judging words are rejected"),
    ]
    lw = 6.3
    top = hs_y + hs_h + 0.12
    avail = Y1 - top
    tw = (lw - 0.2) / 2
    th = (avail - 0.15) / 2
    for i, (head, body) in enumerate(tiles):
        card(s, X0 + (i % 2) * (tw + 0.2), top + (i // 2) * (th + 0.15), tw, th, head, [P(body, space_after=0)],
             accent=PRIMARY, size=19, heading_size=22)
    rx = X0 + lw + 0.25
    rw = X1 - rx
    ch = 1.9
    code_block(s, rx, top, rw, ch, "Session rule (wba/wellbeing_analytics.py)", [
        "for item in items_in_time_order:",
        "    if gap_to_previous > 20 min:",
        "        start a new session",
        "    else: add to current session"])
    card(s, rx, top + ch + 0.15, rw, avail - ch - 0.15, "Example", [
        P("09:00 · 09:12 · 09:25 · 09:40 → session 1", space_after=3),
        P("50-minute gap", I("", GREY), space_after=3),
        P("10:30 · 10:45 → session 2", space_after=0)], accent=PRIMARY, size=19, heading_size=21)
    notes(s, "~45 s. Time is computed on request from the source tables and never stored as a total: mails count "
             "only if you opened them, pages count when stored, videos count their watch time, and each visit is "
             "added on the day it happened. Sessions: items less than 20 minutes apart belong to one sitting; the "
             "example uses made-up times run through the real function. Regret: you mark or unmark an item yourself; "
             "each mark is a new event (odd count = currently regretted); optional reminders (at most 2 a day, only "
             "for a habit) quote your own earlier note. Weekly insight: the model receives only totals, counts and "
             "hours, never titles or URLs; a reply containing judging words is replaced by a plain summary.")


def slide_impl_groups(deck: Deck) -> None:
    s = deck.clone(deck.find("Implementation Details"), "Implementation – Module 3: Groups you control (2/2)")
    set_background(s, bg("base"))
    ty = Y0 + 0.03
    white_pill(s, X0, ty, W, 0.6, [P(B(MODULE_3 + " — Part B: your goal groups"), space_after=0)], size=21)
    top = ty + 0.6 + 0.12
    lw = 6.4
    panel(s, X0, top, lw, Y1 - top)
    nx, nw = X0 + 0.25, lw - 0.5
    half = (nw - 0.2) / 2
    lx_, rx_ = nx + half / 2, nx + half + 0.2 + half / 2
    node(s, nx, top + 0.1, nw, 0.58, "New item", "", STORE, size=20)
    node(s, nx, top + 0.93, nw, 0.58, "Scored: rules + similarity", "", STORE, size=19)
    arrow(s, nx + nw / 2, top + 0.7, nx + nw / 2, top + 0.91)
    text(s, nx, top + 1.55, 1.3, 0.36, [P("confident", align=PP_ALIGN.LEFT, space_after=0)], size=18,
         color=PRIMARY.dark, margin=0.0)
    text(s, nx + nw - 1.0, top + 1.55, 1.0, 0.36, [P("unsure", align=PP_ALIGN.RIGHT, space_after=0)], size=18,
         color=STORE.dark, margin=0.0)
    node(s, nx, top + 2.0, half, 0.75, "Added automatically", "", PRIMARY, size=19)
    node(s, nx + half + 0.2, top + 2.0, half, 0.75, "Waits for you", "", STORE, size=19)
    arrow(s, nx + nw / 2 - 0.3, top + 1.53, lx_, top + 1.98, color=PRIMARY.main)
    arrow(s, nx + nw / 2 + 0.3, top + 1.53, rx_, top + 1.98, color=STORE.main)
    node(s, nx, top + 3.0, half, 0.85, "Sunday review", "one-click undo", PRIMARY, size=19)
    node(s, nx + half + 0.2, top + 3.0, half, 0.85, "Your yes or no", "", STORE, size=19)
    arrow(s, lx_, top + 2.77, lx_, top + 2.98, color=PRIMARY.main)
    arrow(s, rx_, top + 2.77, rx_, top + 2.98, color=STORE.main)
    text(s, nx, top + 3.9, nw, Y1 - top - 3.95, [P(I("Echo never decides alone when it is unsure"),
                                                    align=PP_ALIGN.CENTER, space_after=0)],
         size=19, color=PRIMARY.dark, anchor=MSO_ANCHOR.MIDDLE, margin=0.04)
    rx = X0 + lw + 0.25
    card(s, rx, top, X1 - rx, Y1 - top, "How a suggestion is made", [
        P("Rules you write: keyword, domain, channel, sender, dates", bullet=True, space_after=6),
        P("Similarity: mean of the 3 nearest confirmed items", bullet=True, space_after=6),
        P("Suggestions start after 5 confirmed items", bullet=True, space_after=6),
        P("Confident = a rule matches and similarity is above 0.85", bullet=True, space_after=6),
        P("Only items you confirmed teach the similarity", bullet=True, space_after=0)],
        accent=PRIMARY, size=19, heading_size=22)
    notes(s, "~45 s. You create a goal group ('Capstone project'), add a few items, and write rules. Every new item "
             "is scored by rules (do any match?) and similarity (mean cosine to its 3 nearest confirmed members). "
             "Confident means a rule matches AND similarity is above the bar: the item is added at once, marked "
             "'added automatically', and shown in the Sunday review where one click undoes it (human-on-the-loop). "
             "Anything less certain waits as pending for your yes or no (human-in-the-loop). Only items you confirmed "
             "teach the similarity. User groups are independent of the five system groups. Panel line: 'Echo never "
             "decides alone when it is unsure.'")


def slide_demo(deck: Deck) -> None:
    s = deck.keep(deck.find("Project Demonstration"))
    set_background(s, bg("base"))
    ty = Y0 + 0.03
    white_pill(s, X0, ty, W, 0.62, [P(B("How the demo runs "), "· recorded demo video ready as backup", space_after=0)],
               size=21)
    stages = [
        ("1", "Ingestion", "Gmail, Chrome and YouTube activity is captured through the intent gates"),
        ("2", "Enrichment", "The item is cleaned, embedded, classified into a group and indexed"),
        ("3", "Recall", "A question is answered in chat with memory pills; a follow-up continues the same chat"),
        ("4", "Wellbeing", "Time and patterns, a regret marked, a group suggestion reviewed"),
    ]
    top = ty + 0.62 + 0.15
    sh = Y1 - top
    cw = (W - 3 * 0.35) / 4
    for i, (n, name, body) in enumerate(stages):
        x = X0 + i * (cw + 0.35)
        panel(s, x, top, cw, sh)
        box(s, x, top, cw, 0.08, fill=PRIMARY.main)
        text(s, x + 0.2, top + 0.3, 0.65, 0.65, [P(B(n), align=PP_ALIGN.CENTER, space_after=0)], size=24,
             color=WHITE, fill=PRIMARY.main, shape=MSO_SHAPE.OVAL, anchor=MSO_ANCHOR.MIDDLE, margin=0.0)
        text(s, x + 0.95, top + 0.3, cw - 1.0, 0.65, [P(B(name), size=21, color=PRIMARY.dark, space_after=0)],
             anchor=MSO_ANCHOR.MIDDLE, margin=0.02)
        text(s, x + 0.15, top + 1.2, cw - 0.3, sh - 1.3, [P(body, space_after=0)], size=21, margin=0.06)
        if i < 3:
            arrow(s, x + cw + 0.03, top + 0.68, x + cw + 0.32, top + 0.68, color=BLUE, width=3)
    notes(s, "Hand over to the demo (8 min). 1 Ingestion: read a fresh article and scroll past 15 %; play a video "
             "for 20 s; show the new rows in Timeline. 2 Enrichment: one worker cycle (about 10 s); the item turns "
             "processed and gets a group. 3 Recall: ask about the article, then 'Did anyone email me about it?' in "
             "the same chat; show live steps and memory pills; click a pill. 4 Wellbeing: Patterns and Reflections; "
             "mark one item regretted; accept or undo a group suggestion. Optional: one query from Claude Desktop "
             "through the MCP server. Before starting: local PostgreSQL and Redis running, backend and worker up, "
             "extension loaded in the browser used, one fresh mail in the test inbox, sessions rebuilt and group "
             "auto-assign run once. Revoke the Google token shown on screen after the demo.")


def slide_test_plan(deck: Deck) -> None:
    s = deck.keep(deck.find("Test Plan and Strategy"), "Test Plan and Strategy (1/2) – Activities")
    set_background(s, bg("base"))
    lw = 5.3
    entries = [
        ("6 – 19 Oct", "Done", PRIMARY, True,
         "Every file reviewed, with 30 backend and 56 extension offline checks"),
        ("22 – 25 Oct", "Done", PRIMARY, True, "Final build and code freeze on 25 Oct"),
        ("26 – 27 Oct", "Done", PRIMARY, True,
         "563 tests; 38 questions, 15 capture sessions and 3 thresholds measured"),
    ]
    eh = (Y1 - Y0 - 0.03 - 2 * 0.15) / 3
    y = Y0 + 0.03
    for when, status, acc, solid, what in entries:
        panel(s, X0, y, lw, eh)
        box(s, X0, y + 0.1, 0.08, eh - 0.2, fill=PRIMARY.main)
        text(s, X0 + 0.25, y + 0.1, 2.4, 0.5, [P(B(when), space_after=0)], size=22, color=PRIMARY.dark,
             anchor=MSO_ANCHOR.MIDDLE, margin=0.02)
        chip(s, X0 + lw - 1.65, y + 0.14, 1.5, 0.42, status, acc, solid=solid)
        text(s, X0 + 0.25, y + 0.68, lw - 0.4, eh - 0.74, [P(what, space_after=0)], size=20, margin=0.02)
        y += eh + 0.15
    rx = X0 + lw + 0.3
    rw = X1 - rx
    card(s, rx, Y0 + 0.03, rw, 1.75, "Test environment", [
        P("Windows 11 laptop, local PostgreSQL 16 + Redis", bullet=True, space_after=2),
        P("Chrome with the extension; test Gmail and YouTube accounts", bullet=True, space_after=0)],
        accent=STORE, size=18, heading_size=20)
    ty = Y0 + 0.03 + 1.75 + 0.12
    cols = [2.1, 2.4, rw - 4.5]
    cx = [rx]
    for c in cols[:-1]:
        cx.append(cx[-1] + c)
    for x, c, label in zip(cx, cols, ["Member", "Owns", "Cross-checks"]):
        text(s, x, ty, c, 0.46, [P(B(label))], size=18, color=WHITE, fill=BLUE, anchor=MSO_ANCHOR.MIDDLE, margin=0.1)
    team = [
        ("Mrigank Jain", "Capture gates and privacy rules", "Enrichment"),
        ("Mohammed Mir Fazlai Ali", "Enrichment and classification", "Recall"),
        ("Mohammed Shehzaad Khan", "Recall quality and citations", "Wellbeing"),
        ("Arjun Sai N", "Wellbeing, groups, dashboard", "Capture"),
    ]
    ry = ty + 0.5
    rh = (Y1 - ry - 3 * 0.05) / 4
    for row in team:
        panel(s, rx, ry, rw, rh)
        for x, c, val, bold in zip(cx, cols, row, (True, False, False)):
            text(s, x, ry, c, rh, [P(B(val) if bold else val, space_after=0)], size=18, anchor=MSO_ANCHOR.MIDDLE,
                 margin=0.1)
        ry += rh + 0.05
    notes(s, "~30 s. 6-19 Oct: every file reviewed with offline simulation tests (fake database, fake Redis, FastAPI "
             "test client; a Node harness with a fake browser for the extension): 30 and 56 checks, recorded in the "
             "commit messages. 22-25 Oct: final build and code freeze on 25 Oct. 26 Oct: 563 pytest tests (333 unit, 220 integration, 10 "
             "system, about 94 % line coverage, docs/review/ci_cd_guide.md; a re-run on a laptop without the mcp 2.x "
             "package ran 547 of them) and the live evaluation on the test account: 119 items, 38 questions, 15 scripted "
             "capture sessions, thresholds measured (docs/review/eval/RESULTS_2026-10-26.md). 27 Oct: rehearsal. Team: four equal shares; each member owns one area and "
             "cross-checks one area of another member (four equal shares).")


def slide_test_methods(deck: Deck) -> None:
    s = deck.clone(deck.find("Test Plan and Strategy"), "Test Plan and Strategy (2/2) – Methods")
    set_background(s, bg("base"))
    half = (W - 0.25) / 2
    h1 = 2.1
    card(s, X0, Y0 + 0.03, half, h1, "Functional", [
        P(B("Unit: "), "gates, session gap, rank fusion", bullet=True, space_after=5),
        P(B("Integration: "), "routes via the test client", bullet=True, space_after=5),
        P(B("End to end: "), "mail → browse → ask → citation", bullet=True, space_after=0),
    ], accent=PRIMARY, size=19, heading_size=22)
    card(s, X0 + half + 0.25, Y0 + 0.03, half, h1, "Non-functional", [
        P(B("Performance: "), "time, model calls", bullet=True, space_after=5),
        P(B("Privacy: "), "inspect model payloads", bullet=True, space_after=5),
        P(B("Reliability: "), "Redis or model down", bullet=True, space_after=0),
    ], accent=STORE, size=19, heading_size=22)
    y2 = Y0 + 0.03 + h1 + 0.12
    mid_h = 1.5
    card(s, X0, y2, W, mid_h, "", [
        P(B("Test cases: "), "563 automated tests (333 unit · 220 integration · 10 system), 94 % line coverage, run on "
          "every push · 38 recall questions · 15 capture sessions", space_after=4),
        P(B("Tools: "), "Python test scripts (pytest) · FastAPI test client · Node harness (fake browser) · "
          "TypeScript compiler + Vite build · DevTools", space_after=0)],
        accent=STORE, size=19, anchor=MSO_ANCHOR.MIDDLE)
    y3 = y2 + mid_h + 0.12
    card(s, X0, y3, half, Y1 - y3, "Why", [P("Fast, repeatable, no personal data", space_after=0)],
         accent=PRIMARY, size=19, heading_size=21, anchor=MSO_ANCHOR.MIDDLE)
    card(s, X0 + half + 0.25, y3, half, Y1 - y3, "Drawback", [P("Simulated services can miss API changes",
                                                                   space_after=0)],
         accent=STORE, size=19, heading_size=21, anchor=MSO_ANCHOR.MIDDLE)
    notes(s, "~30 s. Unit: intent gates at every boundary (3 s, 6 s, 8 s with 20 % scroll, a login page), the 20-minute "
             "session gap (exactly 20 min vs 20 min 1 s), rank fusion on a small fixed list. Integration: routes "
             "through the FastAPI test client with a fake database; capture -> stored row -> enriched -> searchable. "
             "End to end: live capture, enrichment and recall on the test account. Performance: query time, model "
             "calls per question, enrichment speed. Privacy: inspect every model payload; sensitive-page probe URLs; "
             "check that incognito, Spam and Trash never create a row. Reliability: each failure and its fallback "
             "(design document section 23). Drawback example: YouTube's caption endpoint was found empty only by a "
             "live probe.")


def slide_results(deck: Deck) -> None:
    s = deck.keep(deck.find("Results and Discussion"), "Results and Discussion – Evaluation")
    set_background(s, bg("base"))
    ty = Y0 + 0.03
    bh = 1.5
    white_pill(s, X0, ty, W, bh, [
        P(B("Test set: "), "119 items, 38 questions, test account. ",
          B("Finding: "), "meaning search alone ranked best (MRR 0.99); the cross-encoder step lowered it to 0.90 on "
          "this small set.", space_after=3),
        P(B("Also: "), "13 of 13 clean capture sessions matched the design; no text past the 8,000-character cap "
          "was sent to the model.", space_after=0)], size=18)
    metrics = [
        ("Retrieval quality", "Recall@5 0.91 · MRR 0.90", "full agent loop; Recall@10 0.93"),
        ("Search methods", "Recall@10: 0.70 · 0.96 · 0.96", "keyword · meaning · hybrid (rank fusion)"),
        ("Embedding model", "Right item first: 37 vs 31 of 39", "new model vs MiniLM, earlier 39-question test"),
        ("Time per question", "8.1 s median · 23.8 s p95", "44 turns, a memory-starved laptop"),
        ("Model calls and cost", "2.3 calls · about $0.002", "per question; the price is an assumption"),
        ("Enrichment", "19 items per minute", "71 % sorted by rules, 16 % by the model"),
    ]
    top = ty + bh + 0.12
    cw = (W - 0.25) / 2
    rh = (Y1 - top - 2 * 0.1) / 3
    for i, (name, value, how) in enumerate(metrics):
        col, row = i // 3, i % 3
        x = X0 + col * (cw + 0.25)
        y = top + row * (rh + 0.1)
        panel(s, x, y, cw, rh)
        box(s, x, y + 0.1, 0.08, rh - 0.2, fill=PRIMARY.main)
        text(s, x + 0.2, y, cw - 0.3, rh, [P(B(name), size=19, color=GREY, space_after=0),
                                           P(B(value), size=21, color=PRIMARY.dark, space_after=0),
                                           P(how, size=18, color=GREY, space_after=0)],
             anchor=MSO_ANCHOR.MIDDLE, margin=0.04)
    notes(s, "~60 s. Everything here was measured on 26 Oct on the test account (docs/review/eval/RESULTS_2026-10-26.md). "
             "Questions and gold items were written by the team's assistant and are still to be checked by the owner; "
             "say 'first test set' if asked. Retrieval, 38 questions, 81 gold items: keyword only Recall@10 0.70, "
             "meaning only 0.96, rank fusion 0.96 (the keyword branch changed no top-10), full pipeline 0.93, MRR 0.99 "
             "for meaning only against 0.90 for the full pipeline. Cause found in q28 and q30: the cross-encoder scores "
             "are close to 0 for every candidate on these short questions, so the effort score decides the order. "
             "Answers contained the right fact in 11 of 11 questions that have one. Time: median 8.1 s, 95th percentile "
             "23.8 s over 44 turns, on a laptop with about 1 GB free RAM, so likely pessimistic. Model calls 2.34 per "
             "turn (33 turns used 2, none needed all 5 searches). Cost: about $0.0021 per turn at an ASSUMED $0.20 and "
             "$1.20 per million input and output tokens (copied from an older model comment; gpt-6-luna's price is not "
             "checked). Embedding model: the 37/39 against 31/39 is from the model-choice test of 21 Oct "
             "(Echo_System_Design.md A40), a different 39-question set, not repeated on tonight's 38. Enrichment: 3.1 s "
             "per item, 19.3 items per minute including PDF text extraction; stages over all 119 items: structural 84, "
             "model 19, centroid 11, domain 5. Capture: 13 clean scripted sessions, all agree (Chrome 8 of 8, YouTube "
             "5 of 5), run with a synthetic user in one tab. Privacy: the largest request was 38,469 characters; no "
             "item text beyond the cap appeared (a probe over only 2 long items). Thresholds measured: KNN bar 0.65 "
             "-> 0.85, seed lead 0.08 -> 0.05, search quality gate 0.55 kept.")


def slide_timeline(deck: Deck) -> None:
    s = deck.keep(deck.find("Timeline"))
    set_background(s, bg("base"))
    rows = [
        ("Apr – Jun 2026", "Capture, enrichment, retrieval and storage built", "Completed"),
        ("Jul 2026", "Wellbeing, retrieval engine, first dashboard", "Completed"),
        ("Aug 2026", "Review 1: scope focused on the core recall path", "Completed"),
        ("Sep 2026", "Wellbeing and dashboard restored; Review 2", "Completed"),
        ("1 – 21 Oct", "File-by-file review; all design decisions closed", "Completed"),
        ("22 – 25 Oct", "Final build and freeze on 25 Oct", "Completed"),
        ("26 – 27 Oct", "Evaluation, tests and thresholds measured; rehearsal", "Completed"),
        ("Wed 28 Oct", "Review 3", "Review"),
    ]
    y = Y0 + 0.03
    rh = (Y1 - y - 7 * 0.07) / 8
    for when, what, status in rows:
        text(s, X0, y, 2.2, rh, [P(B(when), align=PP_ALIGN.CENTER, space_after=0)], size=19, color=WHITE,
             fill=PRIMARY.main, shape=MSO_SHAPE.PENTAGON, radius=0.25,
             anchor=MSO_ANCHOR.MIDDLE, margin=0.04)
        panel(s, X0 + 2.3, y, W - 2.3, rh)
        text(s, X0 + 2.45, y, W - 4.5, rh, [P(what, space_after=0)], size=19, anchor=MSO_ANCHOR.MIDDLE, margin=0.02)
        chip(s, X1 - 1.95, y + (rh - 0.4) / 2, 1.8, 0.4, status, PRIMARY if status in ("Completed", "Review") else STORE,
             solid=status in ("Completed", "Review"))
        y += rh + 0.07
    notes(s, "~30 s. Month-level, taken from the project's commit history. Apr-Jun: capture, enrichment, retrieval and "
             "the storage engine merged and hardened. Jul: wellbeing module, finished retrieval engine, first "
             "dashboard. Aug: Review 1, scope focused on recall. Sep: wellbeing and dashboard restored, Review 2. "
             "1-21 Oct: every file reviewed (logging, thread safety, privacy fixes) and every design decision "
             "closed. 22-25 Oct: final build (local stack, new embedding model, agent recall, memory pills, MCP "
             "server, retention, auto-grouping) and code freeze. 26-27 Oct: end-to-end run and rehearsal. Wed 28 Oct: "
             "Review 3.")


def slide_conclusion(deck: Deck) -> None:
    s = deck.keep(deck.find("Conclusion"), "Conclusion and Future Work")
    set_background(s, bg("base"))
    lw = 7.0
    points = [
        ("One private memory", "Gmail, Chrome and YouTube on your laptop, sorted into five groups"),
        ("Ask, don't search", "Merged, re-ranked search; answers cite the original items"),
        ("Reflect without judging", "Time, sessions, your own regrets; groups change only with your yes"),
        ("Privacy by design", "Sensitive pages and typed text never captured; the model never gets database rows"),
    ]
    gap = 0.1
    h = (Y1 - Y0 - 0.03 - 3 * gap) / 4
    y = Y0 + 0.03
    for i, (head, body) in enumerate(points):
        text(s, X0, y, 0.85, h, [P(B(str(i + 1)), align=PP_ALIGN.CENTER, space_after=0)], size=28, color=WHITE,
             fill=PRIMARY.main, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.15, anchor=MSO_ANCHOR.MIDDLE)
        text(s, X0 + 0.95, y, lw - 0.95, h, [P(B(head), size=22, color=PRIMARY.dark, space_after=2),
                                             P(body, size=19, space_after=0)],
             size=19, fill=WHITE, line=LINE_SOFT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.1,
             anchor=MSO_ANCHOR.MIDDLE, margin=0.2)
        y += h + gap
    rx = X0 + lw + 0.3
    rw = X1 - rx
    text(s, rx, Y0 + 0.03, rw, 0.6, [P(B("Future work"), space_after=0)], size=24, color=PRIMARY.dark, fill=WHITE,
         line=LINE_SOFT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.3, anchor=MSO_ANCHOR.MIDDLE, margin=0.18)
    future = [
        ("Import and export your data", "Past history from Gmail, Chrome and YouTube in; your memories out as JSON or CSV"),
        ("Publish the Chrome extension", "A public release on the Chrome Web Store"),
        ("More apps", "Google Calendar, Google Drive and others"),
    ]
    y = Y0 + 0.78
    fh = (Y1 - y - 2 * 0.12) / 3
    for head, body in future:
        card(s, rx, y, rw, fh, head, [P(body, space_after=0)], accent=PRIMARY, size=19, heading_size=21,
             anchor=MSO_ANCHOR.MIDDLE)
        y += fh + 0.12
    notes(s, "~30 s. Four points: one private memory; ask, don't search; reflect without judging; privacy by design. "
             "Future work: import past history (Google Takeout for YouTube, the Chrome History file) and export "
             "your memories (an export route for JSON and CSV already exists); publish the extension on the Chrome "
             "Web Store; extend to more apps such as Google Calendar and Google Drive. Other ideas kept in reserve: "
             "several vectors per long document, and a self-learning domain list.")


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
        "[5] L. Merrick et al., \"Arctic-Embed: Scalable, efficient, and accurate text embedding models,\" "
        "arXiv:2405.05374, 2024.",
        "[6] LangChain and LangGraph Documentation, https://python.langchain.com, 2024.",
        "[7] M. Kleppmann, A. Wiggins, P. van Hardenberg, and M. McGranaghan, \"Local-first software: You own your "
        "data, in spite of the cloud,\" in Proc. ACM Onward!, 2019.",
    ]
    panel(s, X0, Y0 + 0.03, W, Y1 - Y0 - 0.03)
    text(s, X0 + 0.25, Y0 + 0.12, W - 0.5, Y1 - Y0 - 0.2, [P(r, space_after=7) for r in refs], size=18, color=BLUE)
    notes(s, "[1], [2], [6], [7] are the Review 2 references. [3] rank fusion and [4] cross-encoder re-ranking back the "
             "two ranking steps. [5] Arctic-Embed backs the embedding model: CHECK the author list, title and arXiv "
             "number against arxiv.org before presenting (typed from memory).")


def slide_thanks(deck: Deck) -> None:
    s = deck.keep(deck.template_slides[-1], strip=False)
    set_background(s, bg("thanks"))
    for sh in s.shapes:                                   # the template's "Thank You" box sits left of centre
        if sh.has_text_frame and sh.text_frame.text.strip() == "Thank You":
            sh.left = int((deck.prs.slide_width - sh.width) / 2)
    text(s, 2.4, 4.62, 8.5, 0.75, [P("We would be glad to take your questions and feedback.", align=PP_ALIGN.CENTER,
                                     space_after=0)], size=24, color=NAVY, anchor=MSO_ANCHOR.MIDDLE)
    notes(s, "Thank you. Questions (4 minutes).")


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
    slide_impl_capture(deck)
    slide_impl_enrich(deck)
    slide_impl_recall(deck)
    slide_impl_recall_example(deck)
    slide_impl_wellbeing(deck)
    slide_impl_groups(deck)
    slide_demo(deck)
    slide_test_plan(deck)
    slide_test_methods(deck)
    slide_results(deck)
    slide_timeline(deck)
    slide_conclusion(deck)
    slide_references(deck)
    slide_thanks(deck)
    if len(deck.order) != len({id(s) for s in deck.order}):
        raise RuntimeError("A slide was added twice")
    smallest = deck.save(output)
    print(f"Slides: {len(deck.order)} · smallest font on any slide: {smallest:g} pt (minimum allowed {MIN_FONT_PT:g} pt)")
    return output


if __name__ == "__main__":
    # Optional argument: write somewhere else (e.g. a temp file while the real deck is open in PowerPoint).
    print(build(pathlib.Path(sys.argv[1]).resolve()) if len(sys.argv) > 1 else build())
