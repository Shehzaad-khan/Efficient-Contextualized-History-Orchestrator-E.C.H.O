"""
Build the E.C.H.O. Capstone Phase 3 Review 2 deck from the official PES template.

Template slides 1, 2, 3 and 17 are kept (slide 1 filled in); content slides are new
slides that clone the template's own title box and cyan rule so styling matches exactly.
Template slides 4-16 (instruction placeholders) are dropped from the output.
"""
from __future__ import annotations

import copy
import io
import pathlib
import zipfile
from dataclasses import dataclass
from typing import Optional, Union

import fitz  # PyMuPDF — pulls the Phase II diagrams out of the review PDF
from lxml import etree
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_CONNECTOR, MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.oxml.ns import qn
from pptx.util import Inches, Pt

# Paths resolve from this file's location: <repo>/docs/Phase III/build/build_deck.py
BUILD_DIR = pathlib.Path(__file__).resolve().parent
PHASE3_DIR = BUILD_DIR.parent
ROOT = PHASE3_DIR.parent.parent
TEMPLATE = PHASE3_DIR / "REVIEW - 2 - UE23CS441A  –Capstone Project Phase – 2.pptx"
REVIEW1_DECK = PHASE3_DIR / "ECHO.pptx"
PHASE2_REVIEW_PDF = ROOT / "docs" / "Phase II" / "phase 2 review 1.pdf"
GRAPH_PNG = ROOT / "rse" / "rse_langgraph.png"
DIAGRAMS = BUILD_DIR / "diagrams"
OUTPUT = PHASE3_DIR / "ECHO_Phase3_Review2.pptx"

# Template palette and type (read from the template's own runs).
FONT = "Trebuchet MS"
MONO = "Consolas"
BLUE = RGBColor(0x00, 0x33, 0xCC)
RED = RGBColor(0xFF, 0x00, 0x00)
NAVY = RGBColor(0x10, 0x20, 0x4A)
GREY = RGBColor(0x5A, 0x64, 0x7A)
TINT = RGBColor(0xEA, 0xF0, 0xFF)
TINT_SOFT = RGBColor(0xF5, 0xF7, 0xFD)
CODE_BG = RGBColor(0xF3, 0xF5, 0xFA)
BORDER = RGBColor(0x8F, 0xA3, 0xD9)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
BLUE_MID = RGBColor(0x6F, 0x8C, 0xE6)
BLUE_LIGHT = RGBColor(0xB9, 0xC8, 0xF2)

# Content frame: left margin to the right end of the template's cyan rule.
X0, X1 = 1.0, 11.66
Y0, Y1 = 2.02, 7.15
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


def _add_bullet(paragraph, indent_in: float = 0.2) -> None:
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


def text(slide, x: float, y: float, w: float, h: float, paras: list[ParaLike], *,
         size: float = 14, color: RGBColor = BLUE, font: str = FONT,
         align: PP_ALIGN = PP_ALIGN.LEFT, anchor: MSO_ANCHOR = MSO_ANCHOR.TOP,
         fill: Optional[RGBColor] = None, line: Optional[RGBColor] = None,
         shape: MSO_SHAPE = MSO_SHAPE.RECTANGLE, margin: float = 0.08, radius: float = 0.06):
    _check_frame(x, y, w, h)
    if fill is None and line is None:
        sh = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    else:
        sh = box(slide, x, y, w, h, fill=fill, line=line, shape=shape, radius=radius)
    fill_text_frame(sh.text_frame, paras, size, color, font, align, anchor, margin)
    return sh


def _check_frame(x: float, y: float, w: float, h: float) -> None:
    """PowerPoint refuses the whole file if any shape has a non-positive size or leaves the slide."""
    if w <= 0 or h <= 0:
        raise ValueError(f"Shape at ({x:.2f}, {y:.2f}) has non-positive size {w:.2f} x {h:.2f}")
    if x < 0 or y < 0 or x + w > 13.34 or y + h > 7.51:
        raise ValueError(f"Shape ({x:.2f}, {y:.2f}, {w:.2f} x {h:.2f}) extends past the 13.33 x 7.5 slide")


def box(slide, x: float, y: float, w: float, h: float, *, fill: Optional[RGBColor] = TINT,
        line: Optional[RGBColor] = None, shape: MSO_SHAPE = MSO_SHAPE.RECTANGLE,
        radius: float = 0.06, line_w: float = 1.0):
    _check_frame(x, y, w, h)
    sh = slide.shapes.add_shape(shape, Inches(x), Inches(y), Inches(w), Inches(h))
    sh.shadow.inherit = False
    if fill is None:
        sh.fill.background()
    else:
        sh.fill.solid()
        sh.fill.fore_color.rgb = fill
    if line is None:
        sh.line.fill.background()
    else:
        sh.line.color.rgb = line
        sh.line.width = Pt(line_w)
    if shape == MSO_SHAPE.ROUNDED_RECTANGLE:
        sh.adjustments[0] = radius
    return sh


def arrow(slide, x1: float, y1: float, x2: float, y2: float, color: RGBColor = NAVY, width: float = 1.5):
    c = slide.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, Inches(x1), Inches(y1), Inches(x2), Inches(y2))
    c.line.color.rgb = color
    c.line.width = Pt(width)
    tail = etree.SubElement(c.line._get_or_add_ln(), qn("a:tailEnd"))
    tail.set("type", "triangle")
    return c


def picture(slide, source: Union[pathlib.Path, bytes], x: float, y: float, max_w: float, max_h: float,
            border: bool = False):
    """Fit an image inside the (x, y, max_w, max_h) frame, centred, aspect preserved."""
    from PIL import Image
    data = source if isinstance(source, bytes) else source.read_bytes()
    with Image.open(io.BytesIO(data)) as im:
        iw, ih = im.size
    scale = min(max_w / iw, max_h / ih)
    w, h = iw * scale, ih * scale
    pic = slide.shapes.add_picture(io.BytesIO(data), Inches(x + (max_w - w) / 2), Inches(y + (max_h - h) / 2),
                                   Inches(w), Inches(h))
    if border:
        pic.line.color.rgb = BORDER
        pic.line.width = Pt(0.75)
    return pic


def card(slide, x: float, y: float, w: float, h: float, heading: str, paras: list[ParaLike], *,
         size: float = 13, heading_color: RGBColor = BLUE, fill: RGBColor = TINT) -> None:
    """Tinted rounded card with a bold heading line followed by body paragraphs."""
    body = [P(B(heading), color=heading_color, size=size + 2, space_after=6)] + list(paras)
    text(slide, x, y, w, h, body, size=size, fill=fill, shape=MSO_SHAPE.ROUNDED_RECTANGLE,
         margin=0.14, radius=0.05)


# ── Template plumbing ─────────────────────────────────────────────────────────

class Deck:
    def __init__(self) -> None:
        self.prs = Presentation(str(TEMPLATE))
        self.template_slides = list(self.prs.slides)
        self.blank = self.template_slides[3].slide_layout
        header_src = self.template_slides[3]  # "Abstract and Scope" slide
        self.title_el = next(s._element for s in header_src.shapes if s.name == "Text Box 34")
        self.rule_el = next(s._element for s in header_src.shapes if s.name == "Rectangle 4")
        self.order: list = []

    def keep(self, template_index: int):
        slide = self.template_slides[template_index - 1]
        self.order.append(slide)
        return slide

    def new(self, title: str):
        slide = self.prs.slides.add_slide(self.blank)
        tree = slide.shapes._spTree
        tree.append(copy.deepcopy(self.rule_el))
        title_el = copy.deepcopy(self.title_el)
        tree.append(title_el)
        title_shape = slide.shapes[-1]
        # Widen the title box to the rule's span; keep the template's right alignment.
        title_shape.left, title_shape.width = Inches(2.2), Inches(11.66 - 2.2)
        runs = title_el.findall(".//" + qn("a:r"))
        runs[0].find(qn("a:t")).text = title
        for extra in runs[1:]:
            extra.getparent().remove(extra)
        self.order.append(slide)
        return slide

    def save(self, path: pathlib.Path) -> None:
        sld_id_lst = self.prs.slides._sldIdLst
        id_by_rid = {s.get(qn("r:id")): s for s in sld_id_lst}
        rid_by_slide = {}
        for rid, sld_id in id_by_rid.items():
            rid_by_slide[self.prs.part.related_part(rid)] = rid
        wanted_rids = [rid_by_slide[s.part] for s in self.order]
        for sld_id in list(sld_id_lst):
            sld_id_lst.remove(sld_id)
        for rid in wanted_rids:
            sld_id_lst.append(id_by_rid[rid])
        for rid in set(id_by_rid) - set(wanted_rids):
            self.prs.part.drop_rel(rid)
        self.prs.save(str(path))


def review1_media(name: str) -> bytes:
    with zipfile.ZipFile(REVIEW1_DECK) as z:
        return z.read(f"ppt/media/{name}")


def pdf_embedded_image(pdf: pathlib.Path, page_no: int, size: tuple[int, int]) -> bytes:
    """Return the embedded image of the given pixel size on a PDF page (the diagram, not the backdrop)."""
    doc = fitz.open(str(pdf))
    for img in doc[page_no - 1].get_images(full=True):
        info = doc.extract_image(img[0])
        if (info["width"], info["height"]) == size:
            return info["image"]
    raise LookupError(f"No {size} image on page {page_no} of {pdf.name}")


# ── Slides ────────────────────────────────────────────────────────────────────

def slide_title(deck: Deck) -> None:
    s = deck.keep(1)
    old = next(sh for sh in s.shapes if sh.name == "Google Shape;26;p3")
    old._element.getparent().remove(old._element)
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
    text(s, 1.55, 3.25, 2.35, 3.6, [P(f"{l}", space_after=8) for l in labels], size=20)
    text(s, 3.55, 3.25, 0.3, 3.6, [P(":", space_after=8) for _ in labels], size=20)
    text(s, 3.9, 3.25, 8.4, 3.6, [P(v, space_after=8) for v in values], size=20)


def slide_abstract(deck: Deck) -> None:
    s = deck.new("Abstract and Scope")
    picture(s, review1_media("image7.jpg"), X0 + (W - 6.2) / 2, Y0, 6.2, 1.12)
    text(s, X0, 3.3, W, 2.7, [
        P(B("ABSTRACT:"), size=16, space_after=8),
        P("E.C.H.O. addresses fragmented personal digital history across Gmail, Chrome, and YouTube. "
          "Instead of forcing users to remember which application contained a previous interaction, "
          "the system creates a unified, searchable memory layer.", space_after=10),
        P("The system captures meaningful activity, stores source-aware canonical records, enriches them "
          "for semantic search, retrieves context through hybrid deterministic retrieval, and presents "
          "results through a React dashboard and Chrome extension popup."),
    ], size=15)
    text(s, X0, 6.1, W, 0.85, [
        P(B("Phase III focus: "), "system integration, reliable enrichment/indexing, retrieval-quality "
          "improvements, digital wellbeing analytics, and UI completion."),
    ], size=14, fill=TINT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, anchor=MSO_ANCHOR.MIDDLE, margin=0.18)


def slide_scope(deck: Deck) -> None:
    s = deck.new("Abstract and Scope – Project Scope")
    text(s, X0, Y0, W, 0.45, ["The E.C.H.O. system encompasses:"], size=16)
    items = [
        "Gmail, Chrome, and YouTube activity capture",
        "Canonical memory storage with source-specific metadata",
        "Intent-aware capture and revisit detection",
        "Background enrichment, classification, and FAISS semantic indexing",
        "Natural-language hybrid retrieval with conversational context",
        "Digital wellbeing analytics and user-controlled reflections",
        "React dashboard, browser extension popup, settings, export",
    ]
    col_w, row_h, gap = (W - 0.3) / 2, 0.78, 0.16
    for i, item in enumerate(items):
        col, row = (0, i) if i < 4 else (1, i - 4)
        x = X0 + col * (col_w + 0.3)
        y = 2.6 + row * (row_h + gap)
        text(s, x, y, 0.62, row_h, [P(B(f"{i + 1:02d}"), align=PP_ALIGN.CENTER)], size=16, color=WHITE,
             fill=BLUE, shape=MSO_SHAPE.ROUNDED_RECTANGLE, anchor=MSO_ANCHOR.MIDDLE, radius=0.12)
        text(s, x + 0.72, y, col_w - 0.72, row_h, [item], size=14, fill=TINT,
             shape=MSO_SHAPE.ROUNDED_RECTANGLE, anchor=MSO_ANCHOR.MIDDLE, margin=0.14, radius=0.12)
    text(s, X0 + col_w + 0.3, 2.6 + 3 * (row_h + gap), col_w, row_h, [
        P(Run("All functionality is designed with privacy-first principles and local-first architecture.",
              italic=True)),
    ], size=14, anchor=MSO_ANCHOR.MIDDLE)


def slide_suggestions(deck: Deck) -> None:
    s = deck.new("Suggestions from Review - 1")
    text(s, X0, Y0, W, 0.45, [P(B("Suggestions and remarks given by the panel members:"))], size=16)
    rows = [
        ("Data retention timeline:",
         " keep captured data for a fixed period (6 months or 1 year), then delete it automatically to save "
         "storage, as production systems do",
         "Planned as future work — a configurable auto-delete window (6 or 12 months)."),
        ("Spam filtering:",
         " add a model that filters out spam emails before they are captured",
         "Mail that Gmail already flags as spam is never captured, since the connector reads mail without "
         "the Spam folder; a dedicated spam model is planned as future work."),
        ("Focused presentation:",
         " frame the demo to show only what is needed and leave out what isn't",
         "We have kept this review to the point — it shows only what is needed, up to the Digital "
         "Wellbeing module."),
    ]
    y, h = 2.6, 1.42
    for label, rest, response in rows:
        text(s, X0, y, W, h, [
            P(B(label), rest, size=15, space_after=8),
            P(B("Response: ", RED), response, size=14),
        ], fill=TINT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, anchor=MSO_ANCHOR.MIDDLE, margin=0.2, radius=0.08)
        y += h + 0.14


def slide_method_framework(deck: Deck) -> None:
    slide_diagram(deck, "Proposed Approach / Methodology – Framework", DIAGRAMS / "architecture_current.png",
                  "E.C.H.O. layered architecture — capture → storage → enrichment → retrieval & wellbeing → interface")
    s = deck.new("Proposed Approach / Methodology")
    top = Y0 + 0.1
    h = Y1 - top
    col_w = (W - 0.3) / 2
    card(s, X0, top, col_w, h, "Basic approach", [
        P(B("1 "), "Capture only engaged activity (intent gates)", space_after=10),
        P(B("2 "), "Store atomically in PostgreSQL — the source of truth", space_after=10),
        P(B("3 "), "Enrich in the background: clean → embed → classify", space_after=10),
        P(B("4 "), "Answer with hybrid search and exactly 2 LLM calls", space_after=10),
        P(B("5 "), "Reflect on time use — descriptive, never judgmental", space_after=10),
    ], size=16)
    card(s, X0 + col_w + 0.3, top, col_w, h, "Results obtained", [
        P("All three sources captured into one PostgreSQL store", bullet=True, space_after=10),
        P("Enrichment tracks pending / processed / failed; failed items are retried on demand", bullet=True,
          space_after=10),
        P("Multi-turn natural-language questions answered", bullet=True, space_after=10),
        P("Wellbeing analytics implemented — live-data validation pending", bullet=True, space_after=10),
    ], size=16)


def slide_method_change(deck: Deck) -> None:
    s = deck.new("Proposed Approach / Methodology")
    text(s, X0, Y0, W, 0.45, [P(B("Is there a need for changing the approach? "),
                                "Yes — three changes since Phase II.")], size=16)
    col = [1.55, 3.6, W - 1.55 - 3.6 - 0.2]
    xs = [X0, X0 + col[0] + 0.1, X0 + col[0] + col[1] + 0.2]
    head_y = 2.55
    for x, w, label in zip(xs, col, ["", "Phase II plan", "Phase III approach — and why"]):
        if label:
            text(s, x, head_y, w, 0.38, [P(B(label))], size=13, color=WHITE, fill=BLUE,
                 anchor=MSO_ANCHOR.MIDDLE, margin=0.12)
    rows = [
        ("Ranking", "One weighted sum: 40% semantic + 30% engagement + 20% recency + 10% effort",
         [P("Merge keyword and semantic lists by rank (RRF, k = 60), then re-score: "
            "0.8 × cross-encoder + 0.2 × effort.", space_after=3),
          P(Run("Why: the two searches score on different scales, so adding raw scores is unfair.",
                italic=True))]),
        ("Recency", "20% boost for recent items",
         [P("Dropped on purpose, so older but still-relevant memories are not pushed down.")]),
        ("User groups", "Suggested items auto-accepted",
         [P("Suggestions start as pending; an item joins a group only after the user accepts.")]),
    ]
    y = head_y + 0.46
    heights = [1.02, 0.62, 0.62]
    for (aspect, old, new), h in zip(rows, heights):
        text(s, xs[0], y, col[0], h, [P(B(aspect))], size=13, fill=TINT, anchor=MSO_ANCHOR.MIDDLE, margin=0.12)
        text(s, xs[1], y, col[1], h, [old], size=12.5, color=GREY, fill=TINT_SOFT, anchor=MSO_ANCHOR.MIDDLE,
             margin=0.12)
        text(s, xs[2], y, col[2], h, new, size=12.5, fill=TINT_SOFT, anchor=MSO_ANCHOR.MIDDLE, margin=0.12)
        y += h + 0.08
    top = y + 0.14
    h = Y1 - top
    half = (W - 0.25) / 2
    card(s, X0, top, half, h, "Benefits", [
        P("Fairer merge of keyword and semantic results", bullet=True, space_after=2),
        P("Candidates re-scored against the actual question", bullet=True, space_after=2),
        P("Still exactly 2 LLM calls; user stays in control of groups", bullet=True, space_after=2),
    ], size=12.5)
    card(s, X0 + half + 0.25, top, half, h, "Drawbacks", [
        P("Cross-encoder adds CPU time to every query", bullet=True, space_after=2),
        P("One more local model to download and load", bullet=True, space_after=2),
    ], size=12.5, heading_color=RED)


def slide_design_approach(deck: Deck) -> None:
    s = deck.new("Design Approach")
    text(s, X0, Y0, W, 1.12, [
        P(B("Approach: "), "a layered, modular monolith — private, intent-aware memory with deterministic "
          "retrieval-augmented generation (fixed Python search steps; the LLM only writes the final answer).",
          space_after=5),
        P(B("Why: "), "keeps data on the device, cuts noise, unifies search across sources, makes results "
          "explainable, and let four people build modules in parallel."),
    ], size=13.5, fill=TINT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, anchor=MSO_ANCHOR.MIDDLE, margin=0.18)
    top, h = 3.3, 2.28
    half = (W - 0.25) / 2
    card(s, X0, top, half, h, "Benefits", [
        P(B("Privacy-preserving: "), "sensitive data stays on the device", bullet=True, space_after=1),
        P(B("Relevant results: "), "meaning-based search plus engagement signals", bullet=True, space_after=1),
        P(B("Explainable: "), "deterministic ranking, every step visible", bullet=True, space_after=1),
        P(B("Modular: "), "independent modules, easy to maintain and extend", bullet=True, space_after=1),
        P(B("Low-latency design: "), "in-memory vector index and caching", bullet=True, space_after=1),
    ], size=12.5, fill=TINT_SOFT)
    card(s, X0 + half + 0.25, top, half, h, "Drawbacks", [
        P("Tight coupling within the monolith", bullet=True, space_after=1),
        P("Limited horizontal scalability", bullet=True, space_after=1),
        P("Deterministic pipeline limits autonomy", bullet=True, space_after=1),
        P("Desktop dependency", bullet=True, space_after=1),
    ], size=12.5, heading_color=RED, fill=TINT_SOFT)
    text(s, X0, 5.66, W, 0.36, [P(B("Alternate design approaches considered and rejected"))], size=13.5)
    alts = [
        ("Cloud-centric logging", "Easier sync — rejected for privacy risk"),
        ("Keyword-only search", "Simple — rejected for poor semantic understanding"),
        ("Agentic LLM retrieval", "Flexible — rejected as unpredictable, not explainable"),
        ("Capture everything", "Maximum recall — rejected for noise and privacy"),
    ]
    aw = (W - 3 * 0.18) / 4
    for i, (name, why) in enumerate(alts):
        text(s, X0 + i * (aw + 0.18), 6.04, aw, Y1 - 6.04, [P(B(name), size=12.5, space_after=3), why],
             size=11.5, fill=TINT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, margin=0.12)


def slide_constraints(deck: Deck) -> None:
    s = deck.new("Design Constraints, Assumptions & Dependencies")
    cw = (W - 2 * 0.2) / 3
    h = Y1 - Y0
    card(s, X0, Y0, cw, h, "Design constraints", [
        P("Local-first; no permanent cloud copy of personal data", bullet=True),
        P("No capture of typed text, incognito tabs or app pages (Slack, Notion)", bullet=True),
        P("Exactly 2 LLM calls per query", bullet=True),
        P("LLM sees only the question, prior turns, short snippets and aggregated numbers", bullet=True),
        P("16-table schema is locked", bullet=True),
        P("Wellbeing language stays descriptive — no scores, no \"wasted time\"", bullet=True),
        P("No item joins a user group without the user's approval", bullet=True),
    ], size=14)
    card(s, X0 + cw + 0.2, Y0, cw, h, "Assumptions", [
        P("A single user on desktop Chrome", bullet=True),
        P("Gmail, Chrome and YouTube cover most relevant activity", bullet=True),
        P("Engagement (time, scroll, revisits) approximates effort", bullet=True),
        P("Google APIs stay available and stable", bullet=True),
    ], size=14)
    card(s, X0 + 2 * (cw + 0.2), Y0, cw, h, "Dependencies → impact", [
        P(B("Gmail API, YouTube Data API v3: "), "capture stops if an API or its OAuth token fails", bullet=True),
        P(B("Chrome MV3 extension APIs: "), "Chrome and YouTube capture rely on them", bullet=True),
        P(B("PostgreSQL, Redis, FAISS: "), "storage layers; FAISS can be rebuilt from PostgreSQL", bullet=True),
        P(B("Local models (MiniLM, cross-encoder): "), "one-time download on first run", bullet=True),
        P(B("OpenAI gpt-4.1-mini (swappable): "), "paid API, a small cost per query; switching provider is one config change", bullet=True),
    ], size=14)


def slide_diagram(deck: Deck, title: str, image: Union[pathlib.Path, bytes], caption: str) -> None:
    s = deck.new(title)
    picture(s, image, 0.8, Y0, 13.333 - 1.6, Y1 - Y0 - 0.42, border=True)
    text(s, 0.8, Y1 - 0.36, 13.333 - 1.6, 0.36, [P(Run(caption, italic=True), align=PP_ALIGN.CENTER)],
         size=12, color=GREY)


def slide_deployment(deck: Deck) -> None:
    s = deck.new("Design Description – Packaging & Deployment")

    def group(x, y, w, h, label):
        box(s, x, y, w, h, fill=TINT_SOFT, line=BORDER, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.03)
        text(s, x + 0.08, y + 0.03, w - 0.16, 0.3, [P(B(label))], size=11.5, color=NAVY)

    def pkg(x, y, w, h, name, desc, shape=MSO_SHAPE.RECTANGLE):
        text(s, x, y, w, h, [P(B(name), size=12, space_after=1, align=PP_ALIGN.CENTER),
                             P(desc, align=PP_ALIGN.CENTER, space_after=0)],
             size=11, color=NAVY, fill=TINT, line=BLUE, shape=shape, anchor=MSO_ANCHOR.MIDDLE, margin=0.05)

    # Browser
    group(0.6, 2.75, 2.35, 3.0, "Chrome browser")
    pkg(0.75, 3.15, 2.05, 1.05, "«package» extension/", "Manifest V3 · intent gate")
    pkg(0.75, 4.45, 2.05, 1.05, "«package» frontend/", "React + TypeScript · port 3000")
    # Laptop runtime
    group(3.55, 2.0, 5.2, 5.15, "User's laptop — local-first runtime")
    group(3.7, 2.38, 4.9, 2.9, "uvicorn process — FastAPI :8000 request handlers")
    pkg(3.85, 2.75, 4.6, 0.5, "«package» backend/", "API gateway · routes")
    pkg(3.85, 3.35, 2.25, 0.85, "«package» ingestion/", "Gmail · Chrome · YouTube connectors")
    pkg(6.2, 3.35, 2.25, 0.85, "«package» ste/", "storage engine · Redis · settings")
    pkg(3.85, 4.3, 2.25, 0.85, "«package» rse/", "LangGraph retrieval · cross-encoder")
    pkg(6.2, 4.3, 2.25, 0.85, "«package» wba/", "time · sessions · groups · regret · insights")
    group(3.7, 5.45, 2.35, 1.55, "Background thread")
    pkg(3.85, 5.83, 2.05, 1.02, "«package» enp/", "ENP worker: clean · embed · classify")
    group(6.25, 5.45, 2.35, 1.55, "Local files")
    pkg(6.38, 5.8, 2.1, 0.55, "FAISS index", "384-d vectors", shape=MSO_SHAPE.CAN)
    pkg(6.38, 6.4, 2.1, 0.55, "Local models", "MiniLM · cross-encoder", shape=MSO_SHAPE.CAN)
    # Data stores and external APIs
    group(9.3, 2.0, 3.45, 2.65, "Data stores")
    pkg(9.45, 2.38, 3.15, 1.15, "PostgreSQL 16 · 16 tables", "source of truth (Neon during development)",
        shape=MSO_SHAPE.CAN)
    pkg(9.45, 3.63, 3.15, 0.9, "Redis", "revisit · cache · settings", shape=MSO_SHAPE.CAN)
    group(9.3, 4.85, 3.45, 2.3, "External APIs")
    pkg(9.45, 5.22, 3.15, 0.5, "Gmail API", "OAuth 2.0")
    pkg(9.45, 5.82, 3.15, 0.5, "YouTube Data API v3", "video metadata")
    pkg(9.45, 6.42, 3.15, 0.6, "OpenAI gpt-4.1-mini", "2 calls per query · swappable")
    # Connections
    arrow(s, 2.95, 4.0, 3.7, 4.0)
    text(s, 2.9, 4.05, 0.85, 0.5, [P("HTTP JSON", align=PP_ALIGN.CENTER, space_after=0)], size=10, color=GREY,
         margin=0.0)
    arrow(s, 8.75, 3.3, 9.3, 3.3)
    arrow(s, 8.75, 6.0, 9.3, 6.0)
    arrow(s, 7.4, 5.28, 7.4, 5.45)
    arrow(s, 5.9, 6.34, 6.25, 6.34)


def slide_algo_retrieval(deck: Deck) -> None:
    s = deck.new("Algorithm & Pseudocode – Retrieval")
    code = [
        "function answer(question, history):",
        "  intent = LLM.parse_intent(question, history)   # LLM call 1",
        "  intent = resolve_time_anchor(intent)  # 'after that email' → date",
        "  attempts = 0",
        "  loop:",
        "    in parallel:",
        "      kw  = postgres_fulltext_search(intent)",
        "      sem = faiss_search(embed(intent.query))",
        "    pool = RRF(kw, sem)       # score = Σ 1 / (60 + rank)",
        "    quality = evaluate(pool)  # 5 checks → strong/weak/empty",
        "    if quality == strong: break",
        "    if attempts == 3: return no_results_found()",
        "    intent = widen_scope(intent); attempts += 1",
        "  top = rerank(pool)   # 0.8·cross-encoder + 0.2·effort",
        "  top = add_neighbours(top)    # same thread / session",
        "  ctx = fetch_attachment_or_live_gmail(top)  # if asked",
        "  return LLM.synthesize(question, top, ctx)  # LLM call 2",
    ]
    code_w = 7.55
    text(s, X0 - 0.2, Y0, code_w, Y1 - Y0, [P(line, space_after=0) for line in code], size=12.5, color=NAVY,
         font=MONO, fill=CODE_BG, line=BORDER, shape=MSO_SHAPE.ROUNDED_RECTANGLE, margin=0.2, radius=0.03,
         anchor=MSO_ANCHOR.MIDDLE)
    gx = X0 - 0.2 + code_w + 0.25
    picture(s, GRAPH_PNG, gx, Y0, 12.55 - gx, Y1 - Y0 - 0.35, border=True)
    text(s, gx, Y1 - 0.32, 12.55 - gx, 0.32,
         [P(Run("Live LangGraph: 14 nodes", italic=True), align=PP_ALIGN.CENTER)], size=11, color=GREY)


def slide_algo_wellbeing(deck: Deck) -> None:
    s = deck.new("Algorithm & Pseudocode – Wellbeing")
    x_left = X0 - 0.2
    groups = [
        "# User-group suggestions — hybrid rules + KNN",
        "for each recent item, for each active group:",
        "  if group has < 6 confirmed items: skip     # not enough examples yet",
        "  rule  = rules matched / total rules",
        "  knn   = mean cosine similarity of the 3 most similar confirmed members",
        "  score = 0.5·rule + 0.5·knn",
        "  if score > 0.70 or rule > 0:",
        "    add suggestion with decision = 'pending'",
        "",
        "# weekly review:  accept → item joins the group",
        "#                 reject → item is never added",
    ]
    text(s, x_left, Y0, W + 0.4, 4.25, [P(line, space_after=2) for line in groups],
         size=15, color=NAVY, font=MONO, fill=CODE_BG, line=BORDER, shape=MSO_SHAPE.ROUNDED_RECTANGLE,
         margin=0.3, radius=0.03, anchor=MSO_ANCHOR.MIDDLE)
    text(s, x_left, Y0 + 4.42, W + 0.4, Y1 - Y0 - 4.42, [
        P(B("Human-in-the-loop by design: "), "an item enters a user group only through an explicit user "
          "action — adding it by hand, or accepting a suggestion in the review queue."),
    ], size=14, fill=TINT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, anchor=MSO_ANCHOR.MIDDLE, margin=0.18)


def slide_progress(deck: Deck) -> None:
    s = deck.new("Project Progress So Far")
    lw = 3.5
    text(s, X0, Y0 + 0.05, lw, 1.25, [P(B("80%"), align=PP_ALIGN.CENTER)], size=66, anchor=MSO_ANCHOR.MIDDLE)
    text(s, X0, Y0 + 1.3, lw, 0.4, [P("of E.C.H.O. complete", align=PP_ALIGN.CENTER)], size=15, color=NAVY)
    bar_y, bar_h = Y0 + 1.9, 0.42
    box(s, X0, bar_y, lw * 0.8, bar_h, fill=BLUE)
    box(s, X0 + lw * 0.8, bar_y, lw * 0.1, bar_h, fill=BLUE_MID)
    box(s, X0 + lw * 0.9, bar_y, lw * 0.1, bar_h, fill=BLUE_LIGHT)
    legend = [(BLUE, "Done — 80%"), (BLUE_MID, "UI — 10%"), (BLUE_LIGHT, "Cleanup & hardening — 10%")]
    for i, (c, label) in enumerate(legend):
        y = bar_y + 0.6 + i * 0.38
        box(s, X0 + 0.1, y + 0.07, 0.22, 0.22, fill=c)
        text(s, X0 + 0.42, y, lw - 0.42, 0.36, [label], size=13, color=NAVY)
    text(s, X0, Y1 - 1.05, lw, 1.05, [
        P("Wellbeing module implemented; validation on live data is pending (scheduled in October)."),
    ], size=12, color=BLUE, fill=TINT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, anchor=MSO_ANCHOR.MIDDLE, margin=0.14)

    rx = X0 + lw + 0.35
    rw = X1 - rx
    card(s, rx, Y0, rw, 0.98, "Done — shown at Review 1", [
        P("Capture (Gmail, Chrome, YouTube) · Storage engine · Enrichment · 14-node hybrid retrieval engine"),
    ], size=12.5)
    card(s, rx, Y0 + 1.1, rw, 2.72, "New since Review 1 — Wellbeing module (WBA)", [
        P("Regret marking, patterns by hour and category, gentle reminders (max 2 a day)", bullet=True,
          space_after=10),
        P("User groups: rules + KNN suggestions, weekly review queue, suggested rules", bullet=True,
          space_after=10),
        P("Wellbeing API routes for the dashboard", bullet=True, space_after=10),
    ], size=15, fill=TINT)
    card(s, rx, Y0 + 3.95, rw, Y1 - Y0 - 3.95, "Remaining — 20%", [
        P(B("UI (10%): "), "finish the dashboard screens and the extension popup", bullet=True, space_after=2),
        P(B("Cleanup (10%): "), "fix breaking paths, add logging, clean code, remove dead code", bullet=True,
          space_after=2),
    ], size=12.5, heading_color=RED, fill=TINT_SOFT)


def slide_demo(deck: Deck) -> None:
    s = deck.new("Project Demonstration")
    text(s, X0, Y0, W, 0.75, [
        P(B("Demo plan — "), "each part is shown on its own, in the order data flows through the system. "
          "There is no static dataset: data is created live by the user's own activity."),
    ], size=14)
    steps = [
        ("Dataset creation", "Open a web page, watch a YouTube video, receive an email.",
         "Each engaged item appears as a new row in PostgreSQL."),
        ("Enrichment", "The background worker picks up new items.",
         "Show an item move from pending to processed, with its category."),
        ("Recall", "Ask a question, then a follow-up that refers to it.",
         "Show the answer — 2 LLM calls."),
        ("Wellbeing (WBA)", "Today's summary and sessions; mark an item as regretted.",
         "Accept a group suggestion; view a weekly insight."),
    ]
    cw = (W - 3 * 0.2) / 4
    top = Y0 + 0.95
    for i, (name, what, show) in enumerate(steps):
        x = X0 + i * (cw + 0.2)
        box(s, x, top, cw, Y1 - top, fill=TINT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=0.05)
        text(s, x + cw / 2 - 0.36, top + 0.25, 0.72, 0.72, [P(B(str(i + 1)), align=PP_ALIGN.CENTER)],
             size=22, color=WHITE, fill=RED if i == 3 else BLUE, shape=MSO_SHAPE.OVAL, anchor=MSO_ANCHOR.MIDDLE,
             margin=0.0)
        text(s, x + 0.12, top + 1.12, cw - 0.24, 0.45, [P(B(name), align=PP_ALIGN.CENTER)], size=15)
        text(s, x + 0.15, top + 1.62, cw - 0.3, Y1 - top - 1.75, [
            P(what, space_after=10), P(Run(show, color=NAVY)),
        ], size=14.5)
        if i < 3:
            arrow(s, x + cw + 0.02, top + 0.61, x + cw + 0.18, top + 0.61, color=BLUE, width=2)
    text(s, X0 + 3 * (cw + 0.2), top - 0.36, cw, 0.32, [P(B("NEW since Review 1"), align=PP_ALIGN.CENTER)],
         size=11, color=RED)


def slide_timeline(deck: Deck) -> None:
    s = deck.new("Timeline – Update on Pending Tasks")
    text(s, X0, Y0, W, 0.42, [P(B("Target: 100% complete in the last week of October"))], size=15)
    label_w = 2.1
    grid_x = X0 + label_w + 0.1
    col_w = (X1 - grid_x) / 4
    head_y = Y0 + 0.55
    for i, week in enumerate(["October · Week 1", "October · Week 2", "October · Week 3",
                              "October · Week 4 · final"]):
        text(s, grid_x + i * col_w + 0.03, head_y, col_w - 0.06, 0.4, [P(B(week), align=PP_ALIGN.CENTER)],
             size=12, color=WHITE, fill=RED if i == 3 else BLUE, anchor=MSO_ANCHOR.MIDDLE, margin=0.04)
    rows = [
        ("UI completion", "10%", [
            (0, 1, "Implement wellbeing screens and popup; connect to the API"),
            (1, 1, "Citation cards, source-breakdown chart, group-review polish"),
        ]),
        ("Refactor & hardening", "10%", [
            (2, 1, "Fix breaking paths, add logging, remove dead code"),
        ]),
        ("Validation & finish", "", [
            (2, 1, "End-to-end run of all 3 sources; WBA on live data"),
            (3, 1, "Final testing, retrieval quality check, privacy check, documentation"),
        ]),
    ]
    row_h, y = 1.0, head_y + 0.52
    for label, pct, bars in rows:
        text(s, X0, y, label_w, row_h, [P(B(label), space_after=2), P(pct)] if pct else [P(B(label))],
             size=13, fill=TINT, anchor=MSO_ANCHOR.MIDDLE, margin=0.12)
        box(s, grid_x, y, X1 - grid_x, row_h, fill=TINT_SOFT)
        for start, span, task in bars:
            text(s, grid_x + start * col_w + 0.05, y + 0.1, span * col_w - 0.1, row_h - 0.2, [task],
                 size=12.5, color=WHITE, fill=BLUE_MID if label != "Validation & finish" else BLUE,
                 shape=MSO_SHAPE.ROUNDED_RECTANGLE, anchor=MSO_ANCHOR.MIDDLE, margin=0.08, radius=0.12)
        y += row_h + 0.1
    text(s, X0, y + 0.05, W, Y1 - y - 0.05, [
        P(B("Carried forward from Review 1: "), "wellbeing validation, dashboard refinements, end-to-end "
          "validation, retrieval quality measurement, privacy audit and documentation — all covered above."),
    ], size=12, color=BLUE, fill=TINT, shape=MSO_SHAPE.ROUNDED_RECTANGLE, anchor=MSO_ANCHOR.MIDDLE, margin=0.16)


def slide_conclusion(deck: Deck) -> None:
    s = deck.new("Conclusion")
    points = [
        ("80% complete", "Capture, storage, enrichment, hybrid retrieval and — new since Review 1 — the "
                         "Wellbeing module are implemented."),
        ("Two deliberate design changes", "Rank-based fusion with cross-encoder re-ranking, and user approval "
                                          "before any item joins a group."),
        ("Remaining 20%", "UI completion and code hardening, finishing in the last week of October."),
    ]
    y, h = Y0 + 0.15, 1.3
    for i, (head, body) in enumerate(points):
        text(s, X0, y, 0.95, h, [P(B(f"{i + 1}"), align=PP_ALIGN.CENTER)], size=28, color=WHITE, fill=BLUE,
             shape=MSO_SHAPE.ROUNDED_RECTANGLE, anchor=MSO_ANCHOR.MIDDLE, radius=0.1)
        text(s, X0 + 1.1, y, W - 1.1, h, [P(B(head), size=18, space_after=6), body], size=16, fill=TINT,
             shape=MSO_SHAPE.ROUNDED_RECTANGLE, anchor=MSO_ANCHOR.MIDDLE, margin=0.2, radius=0.08)
        y += h + 0.2
    text(s, X0, y + 0.05, W, Y1 - y - 0.05, [
        P(Run("E.C.H.O. stays local-first and private: the LLM only ever sees the question, short snippets and "
              "aggregated numbers.", italic=True), align=PP_ALIGN.CENTER),
    ], size=15, color=BLUE, anchor=MSO_ANCHOR.MIDDLE)


def slide_references(deck: Deck) -> None:
    s = deck.new("References")
    refs = [
        "[1] N. Reimers and I. Gurevych, \"Sentence-BERT: Sentence embeddings using Siamese BERT-networks,\" "
        "in Proceedings of the 2019 Conference on Empirical Methods in Natural Language Processing, 2019.",
        "[2] J. Johnson, M. Douze, and H. Jégou, \"Billion-scale similarity search with GPUs,\" IEEE "
        "Transactions on Big Data, vol. 7, no. 3, pp. 535–547, 2021.",
        "[3] LangChain and LangGraph Documentation, \"Building language model applications with composable "
        "components,\" https://python.langchain.com, 2024.",
        "[4] M. Kleppmann, A. Wiggins, P. van Hardenberg, and M. McGranaghan, \"Local-first software: You own "
        "your data, in spite of the cloud,\" in Proceedings of the 2019 ACM SIGPLAN International Symposium "
        "on New Ideas, New Paradigms, and Reflections on Programming and Software, 2019.",
    ]
    text(s, X0, Y0, W, Y1 - Y0, [P(B("REFERENCES (IEEE Format):"), size=16, space_after=12)]
         + [P(r, space_after=14) for r in refs], size=15)


def build(output: pathlib.Path = OUTPUT) -> pathlib.Path:
    deck = Deck()
    slide_title(deck)
    deck.keep(2)   # Guide Approval Mail — left empty for the mentor's approval
    deck.keep(3)   # Outline — unchanged
    slide_abstract(deck)
    slide_scope(deck)
    slide_suggestions(deck)
    slide_method_framework(deck)
    slide_method_change(deck)
    slide_design_approach(deck)
    slide_constraints(deck)
    slide_diagram(deck, "Design Description – Master Class Diagram",
                  pdf_embedded_image(PHASE2_REVIEW_PDF, 27, (1453, 958)),
                  "Master class diagram (from Phase II design)")
    slide_diagram(deck, "Design Description – Use Case Diagram",
                  pdf_embedded_image(PHASE2_REVIEW_PDF, 26, (1570, 832)),
                  "Use case diagram (from Phase II design)")
    slide_diagram(deck, "Design Description – Sequence Diagram (1/3)", DIAGRAMS / "seq_capture.png",
                  "Capture and enrichment")
    slide_diagram(deck, "Design Description – Sequence Diagram (2/3)", DIAGRAMS / "seq_recall.png",
                  "Recall — exactly 2 LLM calls per query")
    slide_diagram(deck, "Design Description – Sequence Diagram (3/3)", DIAGRAMS / "seq_wellbeing.png",
                  "Wellbeing — descriptive analytics, human-in-the-loop groups")
    slide_deployment(deck)
    slide_algo_retrieval(deck)
    slide_algo_wellbeing(deck)
    slide_progress(deck)
    slide_demo(deck)
    slide_timeline(deck)
    slide_conclusion(deck)
    slide_references(deck)
    deck.keep(17)  # Thank You
    deck.save(output)
    return output


if __name__ == "__main__":
    import sys
    # Optional argument: write somewhere else (e.g. a temp file while the real deck is open in PowerPoint).
    print(build(pathlib.Path(sys.argv[1]).resolve()) if len(sys.argv) > 1 else build())
