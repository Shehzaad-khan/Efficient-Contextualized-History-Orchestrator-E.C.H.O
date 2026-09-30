"""Tile slide PNGs into labeled contact sheets (cols x rows per sheet)."""
import sys, glob, os
from PIL import Image, ImageDraw

def make(src_dir: str, prefix: str, cols: int = 3, rows: int = 3, w: int = 640) -> None:
    files = sorted(glob.glob(os.path.join(src_dir, "slide-*.png")))
    per = cols * rows
    for sheet in range(0, len(files), per):
        chunk = files[sheet:sheet + per]
        ims = [Image.open(f) for f in chunk]
        h = int(w * ims[0].height / ims[0].width)
        canvas = Image.new("RGB", (cols * (w + 10) + 10, ((len(ims) + cols - 1) // cols) * (h + 34) + 10), "white")
        d = ImageDraw.Draw(canvas)
        for i, (f, im) in enumerate(zip(chunk, ims)):
            x, y = 10 + (i % cols) * (w + 10), 10 + (i // cols) * (h + 34)
            canvas.paste(im.resize((w, h)), (x, y + 24))
            d.text((x, y + 4), os.path.basename(f), fill="black")
        out = f"{prefix}-{sheet // per + 1}.png"
        canvas.save(out)
        print(out)

if __name__ == "__main__":
    make(sys.argv[1], sys.argv[2])
