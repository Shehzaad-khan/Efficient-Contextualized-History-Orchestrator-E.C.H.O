"""
Generate the Review 3 deck's backgrounds and source icons (PIL + numpy, deterministic).

Backgrounds go to r3/backgrounds/*.png (16:9, 2400 x 1350). They are SLIDE backgrounds (p:bg), so the
template's PES logo — a picture on the slide master — is still drawn on top of them.

Design (v3, owner feedback 2026-10-22: "too colourful, not professional enough"):
  - header band (title, rule, PES logo) stays white;
  - content slides: a near-white body with one thin blue line along the bottom and, on most slides, very faint arcs
    in the bottom-right corner. The "plain" variant has no decoration at all (used where a screenshot goes);
  - title and thank-you slides: a white upper area and a slim deep-navy band at the bottom carrying a quiet network.
  One accent colour (the template blue) is used everywhere; cards drawn by the build sit on these surfaces.

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
CYAN: RGB = (0x33, 0xCC, 0xCC)
NAVY: RGB = (0x0B, 0x1C, 0x4F)     # slim band on the title and thank-you slides
NAVY2: RGB = (0x14, 0x30, 0x86)
SOFT: RGB = (0xF6, 0xF8, 0xFD)     # body tint, top-left
SOFT2: RGB = (0xEA, 0xEF, 0xFA)    # body tint, bottom-right
LINE_BLUE: RGB = (0x5B, 0x8D, 0xEF)
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


def _bottom_line(img: Image.Image) -> None:
    """One thin blue line along the bottom edge (the only colour accent on the surface)."""
    d = ImageDraw.Draw(img)
    h = int(0.05 * PPI)
    for x in range(W):
        d.line((x, H - h, x, H), fill=_mix(BRAND, LINE_BLUE, x / W) + (255,))


def _body_mask(fade_in: float = 0.35) -> np.ndarray:
    """0 in the header band, ramping to 1 just below it."""
    ys = np.arange(H, dtype=np.float32)[:, None] * np.ones((1, W), np.float32)
    top = HEADER_IN * PPI
    return np.clip((ys - top) / (fade_in * PPI), 0.0, 1.0)


def content_background(decorated: bool) -> Image.Image:
    ys, xs = np.mgrid[0:H, 0:W].astype(np.float32)
    mask = _body_mask()
    body = _gradient([(0.0, SOFT), (1.0, SOFT2)], 0.5 * (ys / H) + 0.5 * (xs / W))
    pixels = 255.0 * (1 - mask[..., None]) + body * mask[..., None]
    img = Image.fromarray(np.round(pixels).astype(np.uint8), "RGB").convert("RGBA")
    if decorated:
        rings = _rings((W, H), (W * 1.02, H * 1.12), 380, 1500, 130, BRAND, 30, width=3)
        alpha = np.array(rings.getchannel("A"), dtype=np.float32) * mask      # never over the header band
        rings.putalpha(Image.fromarray(alpha.astype(np.uint8), "L"))
        img.alpha_composite(rings)
    _bottom_line(img)
    return img.convert("RGB")


def _navy_band(img: Image.Image, top_in: float, seed: int, net_from: float = 0.45) -> None:
    """Slim deep-navy band from `top_in` to the bottom edge, a thin light-blue line on top, a quiet network inside."""
    ys, xs = np.mgrid[0:H, 0:W].astype(np.float32)
    top = int(top_in * PPI)
    band = _gradient([(0.0, NAVY), (1.0, NAVY2)], 0.8 * (xs / W) + 0.2 * (ys / H))
    alpha = np.where(ys >= top, 255, 0).astype(np.uint8)
    img.alpha_composite(Image.fromarray(np.dstack([np.round(band).astype(np.uint8), alpha]), "RGBA"))
    d = ImageDraw.Draw(img)
    d.rectangle((0, top, W, top + 6), fill=LINE_BLUE + (255,))
    net = _network((W, H), (W * net_from, top + 30, W - 30, H - 24), WHITE, 22, seed, 300, 90, 50)
    img.alpha_composite(net)


def title_background() -> Image.Image:
    ys, xs = np.mgrid[0:H, 0:W].astype(np.float32)
    top = _gradient([(0.0, WHITE), (1.0, (0xEE, 0xF3, 0xFC))], 0.6 * (xs / W) + 0.4 * (ys / H))
    img = Image.fromarray(np.round(top).astype(np.uint8), "RGB").convert("RGBA")
    _navy_band(img, 6.05, 31)
    return img.convert("RGB")


def thanks_background() -> Image.Image:
    ys, xs = np.mgrid[0:H, 0:W].astype(np.float32)
    top = _gradient([(0.0, WHITE), (1.0, (0xEE, 0xF3, 0xFC))], 0.6 * (xs / W) + 0.4 * (ys / H))
    img = Image.fromarray(np.round(top).astype(np.uint8), "RGB").convert("RGBA")
    _navy_band(img, 5.85, 41, net_from=0.78)
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
    content_background(decorated=True).save(BACKGROUNDS / "bg_base.png", optimize=True)
    content_background(decorated=False).save(BACKGROUNDS / "bg_plain.png", optimize=True)
    title_background().save(BACKGROUNDS / "bg_title.png", optimize=True)
    thanks_background().save(BACKGROUNDS / "bg_thanks.png", optimize=True)
    source_icons()


if __name__ == "__main__":
    ensure()
    print("backgrounds:", sorted(p.name for p in BACKGROUNDS.glob("*.png")))
    print("icons:", sorted(p.name for p in ASSETS.glob("*.png")))
