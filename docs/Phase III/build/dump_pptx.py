"""Dump slide text (in presentation order) plus image names from a .pptx, stdlib only."""
import re
import sys
import zipfile
from xml.dom import minidom

NS_A = "http://schemas.openxmlformats.org/drawingml/2006/main"


def slide_order(z: zipfile.ZipFile) -> list[str]:
    pres = minidom.parseString(z.read("ppt/presentation.xml"))
    rels = minidom.parseString(z.read("ppt/_rels/presentation.xml.rels"))
    rid_to_target = {r.getAttribute("Id"): r.getAttribute("Target") for r in rels.getElementsByTagName("Relationship")}
    order = []
    for sid in pres.getElementsByTagName("p:sldId"):
        rid = sid.getAttribute("r:id")
        order.append("ppt/" + rid_to_target[rid].lstrip("/").replace("ppt/", ""))
    return order


def slide_images(z: zipfile.ZipFile, slide_path: str) -> list[str]:
    rel_path = slide_path.replace("slides/", "slides/_rels/") + ".rels"
    if rel_path not in z.namelist():
        return []
    rels = minidom.parseString(z.read(rel_path))
    return [r.getAttribute("Target").split("/")[-1] for r in rels.getElementsByTagName("Relationship")
            if r.getAttribute("Type").endswith("/image")]


def main(path: str) -> None:
    with zipfile.ZipFile(path) as z:
        for n, sp in enumerate(slide_order(z), 1):
            doc = minidom.parseString(z.read(sp))
            print(f"\n===== Slide {n} ({sp.split('/')[-1]}) images={slide_images(z, sp)}")
            for p in doc.getElementsByTagNameNS(NS_A, "p"):
                text = "".join(t.firstChild.data for t in p.getElementsByTagNameNS(NS_A, "t") if t.firstChild)
                if text.strip():
                    print("  " + text)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main(sys.argv[1])
