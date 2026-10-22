"""
Generate the Review 3 deck's backgrounds and source icons (PIL + numpy, deterministic).

Backgrounds go to r3/backgrounds/*.png (16:9, 2400 x 1350). They are SLIDE backgrounds (p:bg), so the
template's PES logo — a picture on the slide master — is still drawn on top of them.

Design (v2, owner feedback 2026-10-21: "too dim, make it visibly designed"):
  - header band (top 1.8 in: red title, cyan rule, PES logo) stays white, with a thin brand line on the top edge;
  - content slides: a clearly tinted body, a confident brand-gradient "sweep" panel rising from the bottom-right
    corner in the module colour, a memory-network motif with real presence, a module-coloured bar on the left and
    a brand line along the bottom. Text sits on white cards drawn by the build, so contrast stays high;
  - feature slides (abstract, demo, conclusion): the whole body is a deep brand gradient with a white network —
    visibly different from content slides; again all text sits on white cards;
  - title and thank-you slides: a strong hero band (deep gradient, echo rings, network) in the lower part, with the
    template's own text kept on the white upper part.

Icons: the Gmail / Chrome / YouTube logos are cropped from the Review 1 deck's logo strip
(ECHO.pptx, ppt/media/image7.jpg); their grey backdrop is made transparent.

Run directly to regenerate:  python "docs/Phase III/build/r3/make_assets.py"
"""
from __future__ import annotations

import io
import math
import pathlib
import random
import zipfile

import numpy as np
from PIL import Image, ImageDraw, ImageFilter

R3_DIR = pathlib.Path(__file__).resolve().parent
BACKGROUNDS = R3_DIR / "backgrounds"
ASSETS = R3_DIR / "assets"
REVIEW1_DECK = R3_DIR.parent.parent / "ECHO.pptx"

W, H = 2400, 1350
PPI = W / 13.333           # pixels per inch on the 13.33 x 7.5 in slide
HEADER_IN = 1.8            # header band height (title + rule + logo) kept white

RGB = tuple[int, int, int]
# Module accent colours — the same values build_deck_r3.py uses for cards and diagram boxes.
ACCENTS: dict[str, RGB] = {
    "base": (0x1F, 0x4F, 0xE0),
    "capture": (0x0E, 0x8C, 0x8C),
    "enrich": (0x6B, 0x4F, 0xD8),
    "recall": (0x1F, 0x4F, 0xE0),
    "wellbeing": (0x1F, 0x8A, 0x4C),
    "results": (0xC2, 0x6A, 0x00),
}
CYAN: RGB = (0x33, 0xCC, 0xCC)
DEEP: RGB = (0x06, 0x1A, 0x6E)     # deep navy-blue for hero areas
BRAND: RGB = (0x00, 0x33, 0xCC)    # template body blue
WHITE: RGB = (255, 255, 255)


def _mix(a: RGB, b: RGB, t: float) -> RGB:
    return tuple(int(round(x + (y - x) * t)) for x, y in zip(a, b))  # type: ignore[return-value]


def _gradient(stops: list[tuple[float, RGB]], t: np.ndarray) -> np.ndarray:
    """Piecewise-linear colour ramp; t in [0, 1] (any shape) -> (..., 3) float array."""
    out = np.zeros(t.shape + (3,), dtype=np.float32)
    t = np.clip(t, 0.0, 1.0)
    for (t0, c0), (t1, c1) in zip(stops, stops[1:]):
        m = (t >= t0) & (t <= t1)
        f = ((t - t0) / max(t1 - t0, 1e-6))[m][:, None]
        out[m] = np.array(c0, np.float32) + (np.array(c1, np.float32) - np.array(c0, np.float32)) * f
    return out


def _network(size: tuple[int, int], box: tuple[float, float, float, float], color: RGB, n: int, seed: int,
             link_dist: float, node_alpha: int, link_alpha: int,
             fade_from: tuple[float, float] | None = None) -> Image.Image:
    """Nodes and links in `box`; optional alpha fade with distance from `fade_from`."""
    rnd = random.Random(seed)
    x0, y0, x1, y1 = box
    pts = [(rnd.uniform(x0, x1), rnd.uniform(y0, y1)) for _ in range(n)]
    reach = math.hypot(x1 - x0, y1 - y0)

    def fade(p: tuple[float, float]) -> float:
        if fade_from is None:
            return 1.0
        return max(0.0, 1.0 - math.hypot(p[0] - fade_from[0], p[1] - fade_from[1]) / reach) ** 0.8

    layer = Image.new("RGBA", size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    for i, a in enumerate(pts):
        for b in pts[i + 1:]:
            dist = math.hypot(a[0] - b[0], a[1] - b[1])
            if dist < link_dist:
                alpha = int(link_alpha * min(fade(a), fade(b)) * (1 - 0.6 * dist / link_dist))
                if alpha > 3:
                    d.line((a, b), fill=color + (alpha,), width=3)
    for p in pts:
        f = fade(p)
        r = rnd.choice((5, 6, 8, 11))
        alpha = int(node_alpha * f)
        if alpha > 3:
            d.ellipse((p[0] - r, p[1] - r, p[0] + r, p[1] + r), fill=color + (alpha,))
            if r == 11:   # a few "memories" get a halo ring
                d.ellipse((p[0] - r * 2.3, p[1] - r * 2.3, p[0] + r * 2.3, p[1] + r * 2.3),
                          outline=color + (alpha // 2,), width=3)
    return layer


def _rings(size: tuple[int, int], center: tuple[float, float], r0: int, r1: int, step: int, color: RGB,
           alpha0: int, width: int = 3) -> Image.Image:
    layer = Image.new("RGBA", size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    cx, cy = center
    for i, r in enumerate(range(r0, r1, step)):
        alpha = max(8, alpha0 - i * 7)
        d.ellipse((cx - r, cy - r, cx + r, cy + r), outline=color + (alpha,), width=width)
    return layer


def _brand_lines(img: Image.Image, accent: RGB, left_bar: bool) -> None:
    d = ImageDraw.Draw(img)
    top = int(0.07 * PPI)
    bottom = int(0.09 * PPI)
    for x in range(W):
        c = _mix(CYAN, accent, x / W)
        d.line((x, 0, x, top), fill=c + (255,))
        d.line((x, H - bottom, x, H), fill=c + (255,))
    if left_bar:
        y0 = int(1.73 * PPI)
        for y in range(y0, H - bottom):
            d.line((0, y, int(0.12 * PPI), y), fill=_mix(CYAN, accent, (y - y0) / (H - y0)) + (255,))


def _body_mask(fade_in: float = 0.35) -> np.ndarray:
    """0 in the header band, ramping to 1 just below it."""
    ys = np.arange(H, dtype=np.float32)[:, None] * np.ones((1, W), np.float32)
    top = HEADER_IN * PPI
    return np.clip((ys - top) / (fade_in * PPI), 0.0, 1.0)


def content_background(accent_name: str, seed: int) -> Image.Image:
    accent = ACCENTS[accent_name]
    ys, xs = np.mgrid[0:H, 0:W].astype(np.float32)
    mask = _body_mask()
    # Body tint: clearly visible, light enough for the few labels drawn straight on it.
    t = 0.45 * (ys / H) + 0.55 * (xs / W)
    body = _gradient([(0.0, (0xF2, 0xF5, 0xFC)), (1.0, _mix(WHITE, accent, 0.20))], t)
    pixels = 255.0 * (1 - mask[..., None]) + body * mask[..., None]
    img = Image.fromarray(np.round(pixels).astype(np.uint8), "RGB").convert("RGBA")
    # Brand sweep: a large disc rising from the bottom-right corner, deep → module colour → cyan.
    cx, cy, r = W * 1.02, H * 1.55, W * 0.52
    dist = np.sqrt((xs - cx) ** 2 + (ys - cy) ** 2)
    inside = dist < r
    sweep_t = np.clip((xs - (cx - r)) / (r * 1.1), 0, 1)
    sweep = _gradient([(0.0, _mix(accent, WHITE, 0.15)), (0.55, accent), (1.0, DEEP)], sweep_t)
    alpha = np.where(inside, 235, 0).astype(np.uint8)
    edge = np.clip((r - dist) / 6.0, 0, 1)                     # anti-aliased rim
    alpha = (alpha * np.where(inside, edge, 0)).astype(np.uint8)
    sweep_img = Image.fromarray(np.dstack([np.round(sweep).astype(np.uint8), alpha]), "RGBA")
    img.alpha_composite(sweep_img)
    # A thin cyan arc just outside the sweep, and a second fainter one.
    arcs = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ad = ImageDraw.Draw(arcs)
    for k, (dr, a) in enumerate([(26, 200), (70, 110), (130, 60)]):
        rr = r + dr
        ad.ellipse((cx - rr, cy - rr, cx + rr, cy + rr), outline=CYAN + (a,), width=4 if k == 0 else 3)
    img.alpha_composite(arcs)
    # Network: white on the sweep, module colour over the light body (left edge zone).
    img.alpha_composite(_network((W, H), (W * 0.62, H * 0.66, W - 20, H - 30), WHITE, 30, seed, 260, 230, 140))
    img.alpha_composite(_network((W, H), (30, HEADER_IN * PPI + 60, W * 0.20, H - 30), accent, 18, seed + 5,
                                 240, 150, 80, fade_from=(0, H)))
    _brand_lines(img, accent, left_bar=True)
    return img.convert("RGB")


def feature_background(seed: int) -> Image.Image:
    """Deep brand-gradient body for the story slides (abstract, demo, conclusion)."""
    ys, xs = np.mgrid[0:H, 0:W].astype(np.float32)
    mask = _body_mask(0.12)
    t = 0.35 * (ys / H) + 0.65 * (xs / W)
    body = _gradient([(0.0, DEEP), (0.55, BRAND), (1.0, (0x1E, 0x8F, 0xC8))], t)
    pixels = 255.0 * (1 - mask[..., None]) + body * mask[..., None]
    img = Image.fromarray(np.round(pixels).astype(np.uint8), "RGB").convert("RGBA")
    body_top = HEADER_IN * PPI + 30
    img.alpha_composite(_rings((W, H), (W * 1.0, H * 1.08), 180, 1500, 120, CYAN, 120))
    img.alpha_composite(_network((W, H), (40, body_top, W - 40, H - 40), WHITE, 46, seed, 250, 170, 85))
    _brand_lines(img, BRAND, left_bar=False)
    return img.convert("RGB")


def _hero_band(img: Image.Image, top_in: float, seed: int, ring_center: tuple[float, float]) -> None:
    """Deep gradient band with a wavy top edge from `top_in` down, plus echo rings and a white network."""
    ys, xs = np.mgrid[0:H, 0:W].astype(np.float32)
    wave = top_in * PPI + 38 * np.sin(xs / W * 2 * math.pi * 1.0 + 0.6) - 20 * (xs / W)
    inside = ys > wave
    edge = np.clip((ys - wave) / 5.0, 0, 1)
    t = 0.3 * ((ys - top_in * PPI) / (H - top_in * PPI)) + 0.7 * (xs / W)
    band = _gradient([(0.0, DEEP), (0.5, BRAND), (1.0, (0x1E, 0x8F, 0xC8))], t)
    alpha = (255 * np.where(inside, edge, 0)).astype(np.uint8)
    img.alpha_composite(Image.fromarray(np.dstack([np.round(band).astype(np.uint8), alpha]), "RGBA"))
    # Cyan line riding the wave.
    line = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ld = ImageDraw.Draw(line)
    pts = [(x, float(top_in * PPI + 38 * math.sin(x / W * 2 * math.pi + 0.6) - 20 * (x / W)) - 9)
           for x in range(0, W + 1, 8)]
    ld.line(pts, fill=CYAN + (230,), width=6)
    img.alpha_composite(line)
    rings = _rings((W, H), ring_center, 120, 1200, 95, CYAN, 150)
    clip = Image.fromarray(alpha, "L")
    rings.putalpha(Image.fromarray(np.minimum(np.array(rings.getchannel("A")), np.array(clip)), "L"))
    img.alpha_composite(rings)
    net = _network((W, H), (30, top_in * PPI + 70, W - 30, H - 30), WHITE, 44, seed, 250, 190, 95)
    net.putalpha(Image.fromarray(np.minimum(np.array(net.getchannel("A")), np.array(clip)), "L"))
    img.alpha_composite(net)


def title_background() -> Image.Image:
    ys, xs = np.mgrid[0:H, 0:W].astype(np.float32)
    t = 0.6 * (xs / W) + 0.4 * (ys / H)
    top = _gradient([(0.0, WHITE), (1.0, (0xE6, 0xEE, 0xFD))], t)
    img = Image.fromarray(np.round(top).astype(np.uint8), "RGB").convert("RGBA")
    img.alpha_composite(_network((W, H), (20, 20, W * 0.22, H * 0.55), BRAND, 16, 12, 230, 120, 60,
                                 fade_from=(0, 0)))
    _hero_band(img, 5.85, 31, (W * 0.93, H * 1.02))
    _brand_lines(img, BRAND, left_bar=False)
    return img.convert("RGB")


def thanks_background() -> Image.Image:
    ys, xs = np.mgrid[0:H, 0:W].astype(np.float32)
    t = 0.6 * (xs / W) + 0.4 * (ys / H)
    top = _gradient([(0.0, WHITE), (1.0, (0xE6, 0xEE, 0xFD))], t)
    img = Image.fromarray(np.round(top).astype(np.uint8), "RGB").convert("RGBA")
    img.alpha_composite(_rings((W, H), (6.15 * PPI, 4.05 * PPI), 260, 1300, 110, BRAND, 70, width=2))
    _hero_band(img, 5.05, 41, (W * 0.5, H * 1.25))
    _brand_lines(img, BRAND, left_bar=False)
    return img.convert("RGB")


# Pixel boxes of each logo in the 2043 x 366 Review 1 logo strip.
LOGO_BOXES = {"gmail": (372, 4, 852, 362), "chrome": (874, 4, 1252, 362), "youtube": (1282, 4, 1732, 362)}


def source_icons() -> dict[str, pathlib.Path]:
    with zipfile.ZipFile(REVIEW1_DECK) as z:
        strip = Image.open(io.BytesIO(z.read("ppt/media/image7.jpg"))).convert("RGB")
    bg = strip.getpixel((5, 5))
    out: dict[str, pathlib.Path] = {}
    for name, box in LOGO_BOXES.items():
        im = strip.crop(box).convert("RGBA")
        data = [(r, g, b, 0) if abs(r - bg[0]) + abs(g - bg[1]) + abs(b - bg[2]) < 36 else (r, g, b, 255)
                for r, g, b, _ in im.getdata()]
        im.putdata(data)
        bbox = im.getbbox()
        if bbox is None:
            raise RuntimeError(f"{name}: crop is empty — check LOGO_BOXES against the logo strip")
        path = ASSETS / f"logo_{name}.png"
        im.crop(bbox).save(path)
        out[name] = path
    return out


def ensure() -> None:
    """Generate every background and icon (a few seconds)."""
    BACKGROUNDS.mkdir(parents=True, exist_ok=True)
    ASSETS.mkdir(parents=True, exist_ok=True)
    for i, name in enumerate(ACCENTS):
        content_background(name, seed=100 + i).save(BACKGROUNDS / f"bg_{name}.png", optimize=True)
    feature_background(seed=7).save(BACKGROUNDS / "bg_feature.png", optimize=True)
    title_background().save(BACKGROUNDS / "bg_title.png", optimize=True)
    thanks_background().save(BACKGROUNDS / "bg_thanks.png", optimize=True)
    source_icons()


if __name__ == "__main__":
    ensure()
    print("backgrounds:", sorted(p.name for p in BACKGROUNDS.glob("*.png")))
    print("icons:", sorted(p.name for p in ASSETS.glob("*.png")))
