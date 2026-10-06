"""Render a .mmd file to a tightly cropped PNG using headless Chrome + Mermaid from jsdelivr."""
import base64
import pathlib
import subprocess
import sys

from PIL import Image, ImageChops

CHROME = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

HTML = """<!doctype html><html><head><meta charset="utf-8">
<style>body{margin:0;padding:24px;background:#fff;font-family:'Trebuchet MS',Arial,sans-serif}</style>
</head><body><pre class="mermaid">%s</pre>
<script type="module">
import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";
mermaid.initialize({startOnLoad:true, theme:"base", securityLevel:"loose",
  themeVariables:{fontFamily:"Trebuchet MS, Arial, sans-serif", fontSize:"17px",
    primaryColor:"#EAF0FF", primaryBorderColor:"#0033CC", primaryTextColor:"#10204A",
    lineColor:"#3B4A6B", clusterBkg:"#F7F9FF", clusterBorder:"#8FA3D9",
    actorBkg:"#EAF0FF", actorBorder:"#0033CC", noteBkgColor:"#FFFFFF", noteBorderColor:"#8FA3D9"},
  sequence:{useMaxWidth:false, mirrorActors:false, actorMargin:40, messageFontSize:16, noteFontSize:15},
  flowchart:{useMaxWidth:false, curve:"basis", nodeSpacing:40, rankSpacing:70}});
</script></body></html>"""


# UML actor (stick figure) for use case diagrams. Mermaid has no actor shape in flowcharts, so a .mmd file
# writes an image node instead:  U@{ img: "{{STICKMAN}}", label: "User", pos: "b", w: 36, h: 60 }
_STICKMAN_SVG = (
    "<svg xmlns='http://www.w3.org/2000/svg' width='36' height='60' viewBox='0 0 36 60'>"
    "<g fill='none' stroke='#10204A' stroke-width='2.4' stroke-linecap='round'>"
    "<circle cx='18' cy='9' r='7' fill='#EAF0FF'/>"
    "<line x1='18' y1='16' x2='18' y2='38'/><line x1='5' y1='24' x2='31' y2='24'/>"
    "<line x1='18' y1='38' x2='7' y2='57'/><line x1='18' y1='38' x2='29' y2='57'/></g></svg>"
)
STICKMAN_URI = "data:image/svg+xml;base64," + base64.b64encode(_STICKMAN_SVG.encode("utf-8")).decode("ascii")


def render(mmd_path: pathlib.Path, width: int, height: int) -> pathlib.Path:
    html_path = mmd_path.with_suffix(".html")
    raw_png = mmd_path.with_name(mmd_path.stem + "_raw.png")
    out_png = mmd_path.with_suffix(".png")
    source = mmd_path.read_text(encoding="utf-8").replace("{{STICKMAN}}", STICKMAN_URI)
    source = source.replace("<", "&lt;").replace(">", "&gt;")
    html_path.write_text(HTML % source, encoding="utf-8")
    subprocess.run(
        [CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars",
         "--force-device-scale-factor=2", f"--window-size={width},{height}",
         "--virtual-time-budget=15000", f"--screenshot={raw_png}", html_path.as_uri()],
        check=True, capture_output=True, timeout=120,
    )
    with Image.open(raw_png) as raw:
        img = raw.convert("RGB")
    # The HTML page and uncropped screenshot are scratch files — only the cropped PNG is kept.
    html_path.unlink()
    raw_png.unlink()
    bbox = ImageChops.difference(img, Image.new("RGB", img.size, "white")).getbbox()
    if bbox is None:
        raise RuntimeError(f"{mmd_path.name}: rendered page is blank (Mermaid failed or CDN unreachable)")
    pad = 16
    box = (max(bbox[0] - pad, 0), max(bbox[1] - pad, 0), min(bbox[2] + pad, img.width), min(bbox[3] + pad, img.height))
    img.crop(box).save(out_png)
    touches_edge = bbox[2] >= img.width - 2 or bbox[3] >= img.height - 2
    print(f"{out_png.name}: {img.crop(box).size} {'WARNING: touches window edge, enlarge window' if touches_edge else ''}")
    return out_png


if __name__ == "__main__":
    render(pathlib.Path(sys.argv[1]).resolve(), int(sys.argv[2]), int(sys.argv[3]))
