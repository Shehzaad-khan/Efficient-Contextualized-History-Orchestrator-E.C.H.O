"""
Round trip between the paper's LaTeX (Draft 5) and a plain text file that can be rewritten (for example by a humanizer).

    python scripts/paper_text.py to-txt --tex main.tex --out paper.txt
    python scripts/paper_text.py to-tex --tex main.tex --txt paper.txt --out main_new.tex

to-txt  writes the editable text: title, abstract, keywords, headings, paragraphs and bullets. Figures, tables, equations, the
        author block and the reference list are shown inside [[KEEP ...]] blocks and are NOT read back; they come from the template.
to-tex  keeps the template main.tex and replaces only the editable blocks, matched by order. It refuses to run when the number or
        kind of blocks differs, and it says where.

Plain text conventions: *italic* for a lead in, `code` for tool names, [n] for citations (the printed numbers), "Section VIII",
"Fig. 2", "Table III", "Eq. (2)" for cross references, and $...$ stays as LaTeX for short inline math.
Scripts keep print() (owner's rule for scripts/).
"""
import argparse
import io
import re
import sys
from pathlib import Path
from typing import Any

CITE_NUMBER = {"b1": 1, "b2": 2, "b3": 3, "b4": 4, "b5": 5, "b13": 6, "b6": 7, "b16": 8, "b8": 9, "b9": 10, "b10": 11,
               "b7": 12, "b11": 13, "b12": 14, "b14": 15, "b15": 16, "b17": 17}
NUMBER_CITE = {number: key for key, number in CITE_NUMBER.items()}
SECTION_REF = {"related": "II", "overview": "III", "method": "IV", "wba": "V", "privacy": "VI", "impl": "VII", "eval": "VIII",
               "limits": "IX", "conc": "X"}
ROMAN_SECTION = {roman: key for key, roman in SECTION_REF.items()}
TABLE_REF = {"gates": "I", "stack": "II", "results": "III"}
ROMAN_TABLE = {roman: key for key, roman in TABLE_REF.items()}
FIG_REF = {"arch": "1", "graph": "2"}
NUM_FIG = {number: key for key, number in FIG_REF.items()}
EQ_REF = {"rrf": "1", "final": "2"}
NUM_EQ = {number: key for key, number in EQ_REF.items()}
MACRO_PLAIN = {"\\embmodel{}": "snowflake-arctic-embed-xs", "\\rerankmodel{}": "ms-marco-MiniLM-L-6-v2",
               "\\llmname{}": "GPT-6 Luna", "\\embdim{}": "384"}
PLAIN_MACRO = {"snowflake-arctic-embed-xs": "\\embmodel{}", "ms-marco-MiniLM-L-6-v2": "\\rerankmodel{}", "GPT-6 Luna": "\\llmname{}"}
EDITABLE = {"title", "abstract", "keywords", "para", "bullet"}


# ── LaTeX to plain text (inline) ─────────────────────────────────────────────

def tex_inline_to_plain(tex: str) -> str:
    text = tex
    for macro, plain in MACRO_PLAIN.items():
        text = text.replace(macro, plain)
        text = text.replace(macro[:-2], plain)  # macro without the trailing {}
    text = re.sub(r"\\texttt\{([^{}]*)\}", lambda m: "`" + m.group(1).replace("\\_", "_").replace("\\allowbreak", "") + "`", text)
    text = re.sub(r"\\textit\{([^{}]*)\}", r"*\1*", text)
    text = re.sub(r"\\cite\{([^{}]*)\}", lambda m: ", ".join(f"[{CITE_NUMBER[k.strip()]}]" for k in m.group(1).split(",")), text)
    text = re.sub(r"Section~\\ref\{sec:(\w+)\}", lambda m: f"Section {SECTION_REF[m.group(1)]}", text)
    text = re.sub(r"Fig\.~\\ref\{fig:(\w+)\}", lambda m: f"Fig. {FIG_REF[m.group(1)]}", text)
    text = re.sub(r"Table~\\ref\{tab:(\w+)\}", lambda m: f"Table {TABLE_REF[m.group(1)]}", text)
    text = re.sub(r"Eq\.~\\(?:eq)?ref\{eq:(\w+)\}", lambda m: f"Eq. ({EQ_REF[m.group(1)]})", text)
    text = text.replace("``", "\u201c").replace("''", "\u201d")
    text = text.replace("\\%", "%").replace("\\&", "&").replace("\\nobreakdash-", "-").replace("\\allowbreak", "")
    text = text.replace("---", "\u2014").replace("~", " ")
    text = text.replace("\\newline ", "").replace("\\newline", "").replace("\\ ", " ")  # "et al.\ " is a control space
    return text


# ── plain text to LaTeX (inline) ─────────────────────────────────────────────

def plain_inline_to_tex(plain: str) -> str:
    holders: list[str] = []

    def hold(value: str) -> str:
        holders.append(value)
        return f"\u0001{len(holders) - 1}\u0002"

    text = plain
    text = re.sub(r"\$[^$]*\$", lambda m: hold(m.group(0)), text)  # inline math stays as LaTeX
    text = re.sub(r"`([^`]*)`", lambda m: hold("\\texttt{" + m.group(1).replace("_", "\\_") + "}"), text)
    text = re.sub(r"\[(\d{1,2})\]", lambda m: hold("\\cite{" + NUMBER_CITE[int(m.group(1))] + "}") if int(m.group(1)) in NUMBER_CITE else m.group(0), text)
    text = re.sub(r"Section ([IVX]+)\b", lambda m: hold(f"Section~\\ref{{sec:{ROMAN_SECTION[m.group(1)]}}}") if m.group(1) in ROMAN_SECTION else m.group(0), text)
    text = re.sub(r"Fig\. (\d)\b", lambda m: hold(f"Fig.~\\ref{{fig:{NUM_FIG[m.group(1)]}}}") if m.group(1) in NUM_FIG else m.group(0), text)
    text = re.sub(r"Table ([IVX]+)\b", lambda m: hold(f"Table~\\ref{{tab:{ROMAN_TABLE[m.group(1)]}}}") if m.group(1) in ROMAN_TABLE else m.group(0), text)
    text = re.sub(r"Eq\. \((\d)\)", lambda m: hold(f"Eq.~\\eqref{{eq:{NUM_EQ[m.group(1)]}}}") if m.group(1) in NUM_EQ else m.group(0), text)
    for plain_name, macro in PLAIN_MACRO.items():
        text = text.replace(plain_name, hold(macro))
    text = text.replace("\u2019", "'").replace("\u2018", "'")
    text = re.sub(r"\u201c", "``", text)
    text = re.sub(r"\u201d", "''", text)
    text = re.sub(r"\"([^\"]*)\"", r"``\1''", text)
    text = text.replace("\u2014", "---").replace("\u2013", "--")
    text = text.replace("et al. ", "et al.\\ ")  # control space after the abbreviation, as in the template
    text = text.replace("%", "\\%").replace("&", "\\&").replace("#", "\\#").replace("_", "\\_")
    text = re.sub(r"\*([^*]+)\*", r"\\textit{\1}", text)
    return re.sub(r"\u0001(\d+)\u0002", lambda m: holders[int(m.group(1))], text)


# ── segmentation of the template (shared by both directions) ─────────────────

def segment(lines: list[str]) -> list[dict[str, Any]]:
    """Blocks in document order, each with its line range [start, end) in `lines`."""
    start_body = next(i for i, l in enumerate(lines) if l.startswith("\\maketitle")) + 1
    end_body = next(i for i, l in enumerate(lines) if l.startswith("\\IEEEtriggeratref") or l.startswith("\\begin{thebibliography}"))
    blocks: list[dict[str, Any]] = []
    for i, l in enumerate(lines):
        if l.startswith("\\title{"):
            blocks.append({"type": "title", "start": i, "end": i + 1, "tex": l[len("\\title{"):-1]})
            break

    def add(kind: str, start: int, end: int, tex: str, **extra: Any) -> None:
        blocks.append({"type": kind, "start": start, "end": end, "tex": tex, **extra})

    i = start_body
    para_start = None
    para: list[str] = []

    def flush(upto: int) -> None:
        nonlocal para_start, para
        if para:
            add("para", para_start, upto, " ".join(s.strip() for s in para))
        para_start, para = None, []

    while i < end_body:
        line = lines[i]
        s = line.strip()
        if not s or s.startswith("%"):
            flush(i)
            i += 1
        elif s.startswith("\\section*{") or s.startswith("\\section{") or s.startswith("\\subsection{"):
            flush(i)
            level = 2 if s.startswith("\\subsection{") else 1
            title = re.match(r"\\(?:sub)?section\*?\{([^}]*)\}", s).group(1)
            add("heading", i, i + 1, title, level=level)
            i += 1
        elif s.startswith("\\begin{abstract}") or s.startswith("\\begin{IEEEkeywords}"):
            flush(i)
            kind = "abstract" if "abstract" in s else "keywords"
            env = "abstract" if kind == "abstract" else "IEEEkeywords"
            j = i + 1
            while not lines[j].strip().startswith(f"\\end{{{env}}}"):
                j += 1
            add(kind, i + 1, j, " ".join(x.strip() for x in lines[i + 1:j]))
            i = j + 1
        elif re.match(r"\\begin\{(figure|table|equation)\}", s):
            flush(i)
            env = re.match(r"\\begin\{(\w+)\}", s).group(1)
            j = i
            while not lines[j].strip().startswith(f"\\end{{{env}}}"):
                j += 1
            add("keep", i, j + 1, "\n".join(lines[i:j + 1]), env=env)
            i = j + 1
        elif s.startswith("\\begin{itemize}"):
            flush(i)
            j = i + 1
            while not lines[j].strip().startswith("\\end{itemize}"):
                if lines[j].strip().startswith("\\item"):
                    k = j + 1
                    while not (lines[k].strip().startswith("\\item") or lines[k].strip().startswith("\\end{itemize}")):
                        k += 1
                    body = " ".join(x.strip() for x in lines[j:k])[len("\\item"):].strip()
                    add("bullet", j, k, body)
                    j = k
                else:
                    j += 1
            i = j + 1
        elif s.startswith("\\"):
            flush(i)
            i += 1
        else:
            if not para:
                para_start = i
            para.append(line)
            i += 1
    flush(end_body)
    return blocks


# ── to-txt ───────────────────────────────────────────────────────────────────

NOTE = ("[[NOTE FOR EDITORS: Rewrite the wording of the paragraphs only. Keep every number, name, citation like [3], cross reference "
        "like Section VIII, and the *italic lead in* and `code` marks. Keep the headings, keep the same number of paragraphs and bullets "
        "in the same order, and leave the blocks that start with KEEP (inside double square brackets) untouched. Style: US spelling, plain words, no hyphens or dashes in prose.]]")


def keep_block_text(block: dict[str, Any]) -> str:
    tex = block["tex"]
    env = block["env"]
    caption = re.search(r"\\caption\{(.*)\}", tex)
    cap = tex_inline_to_plain(caption.group(1)) if caption else ""
    if env == "figure":
        return f"[[KEEP FIGURE: {cap}]]"
    if env == "equation":
        eq = re.sub(r"\\label\{[^}]*\}", "", tex.replace("\\begin{equation}", "").replace("\\end{equation}", "")).strip()
        return f"[[KEEP EQUATION: {' '.join(eq.split())}]]"
    rows = [r for r in tex.split("\n") if "&" in r]
    shown = "\n".join(tex_inline_to_plain(re.sub(r"\\textbf\{([^{}]*)\}", r"\1", r.replace("\\\\", "").replace("&", "|"))).strip() for r in rows)
    return f"[[KEEP TABLE: {cap}\n{shown}\n]]"


def to_txt(tex_path: Path, out_path: Path) -> None:
    lines = io.open(tex_path, encoding="utf-8", newline="").read().replace("\r\n", "\n").split("\n")
    blocks = segment(lines)
    authors = re.findall(r"\\IEEEauthorblockN\{\d\\textsuperscript\{\w+\} ([^}]*)\}", "\n".join(lines))
    out = [NOTE, ""]
    section = 0
    sub = 0
    for block in blocks:
        kind = block["type"]
        if kind == "title":
            out += ["TITLE", tex_inline_to_plain(block["tex"]), "", "AUTHORS (keep)", "; ".join(authors) + " (Dept. of Computer Science and Engineering, PES University, Bengaluru, India)", ""]
        elif kind == "abstract":
            out += ["ABSTRACT", tex_inline_to_plain(block["tex"]), ""]
        elif kind == "keywords":
            out += ["KEYWORDS", tex_inline_to_plain(block["tex"]), ""]
        elif kind == "heading":
            if block["level"] == 1:
                if block["tex"] == "Acknowledgment":
                    out += ["## Acknowledgment", ""]
                else:
                    section += 1
                    sub = 0
                    out += [f"## {section}. {tex_inline_to_plain(block['tex'])}", ""]
            else:
                sub += 1
                out += [f"### {section}.{sub} {tex_inline_to_plain(block['tex'])}", ""]
        elif kind == "para":
            out += [tex_inline_to_plain(block["tex"]), ""]
        elif kind == "bullet":
            out += ["- " + tex_inline_to_plain(block["tex"]), ""]
        elif kind == "keep":
            out += [keep_block_text(block), ""]
    refs = re.findall(r"\\bibitem\{(\w+)\}(.*)", "\n".join(lines))
    out += ["## References (keep)", ""]
    for key, entry in sorted(refs, key=lambda r: CITE_NUMBER[r[0]]):
        entry = re.sub(r"\\textit\{([^{}]*)\}", r"\1", entry)
        entry = re.sub(r"\\url\{([^{}]*)\}", r"\1", entry)
        entry = entry.replace("\\\"{u}", "\u00fc").replace("\\\"{a}", "\u00e4").replace("\\'{e}", "\u00e9")
        out += [f"[{CITE_NUMBER[key]}] " + tex_inline_to_plain(entry).replace("--", "-").strip(), ""]
    out_path.write_text("\n".join(out).rstrip() + "\n", encoding="utf-8", newline="\n")
    print(f"wrote {out_path} ({len(blocks)} blocks)")


# ── to-tex ───────────────────────────────────────────────────────────────────

def parse_txt(path: Path) -> list[dict[str, str]]:
    raw = io.open(path, encoding="utf-8").read().replace("\r\n", "\n")
    raw = re.sub(r"\[\[NOTE FOR EDITORS:.*?\]\]", "", raw, flags=re.S)
    raw = re.sub(r"\[\[KEEP .*?\n?\]\]", "[[KEEP]]", raw, flags=re.S)
    raw = raw.split("## References (keep)")[0]
    items: list[dict[str, str]] = []
    chunks = [c.strip() for c in re.split(r"\n\s*\n", raw) if c.strip()]
    i = 0
    while i < len(chunks):
        c = chunks[i]
        label, _, rest = c.partition("\n")
        if label == "TITLE":
            items.append({"type": "title", "text": " ".join(rest.split())})
            i += 1
        elif label == "AUTHORS (keep)":
            i += 1
        elif label == "ABSTRACT":
            items.append({"type": "abstract", "text": " ".join(rest.split())})
            i += 1
        elif label == "KEYWORDS":
            items.append({"type": "keywords", "text": " ".join(rest.split())})
            i += 1
        elif c.startswith("## ") or c.startswith("### "):
            items.append({"type": "heading", "text": c})
            i += 1
        elif c == "[[KEEP]]":
            items.append({"type": "keep", "text": ""})
            i += 1
        elif c.startswith("- "):
            items.append({"type": "bullet", "text": c[2:].strip()})
            i += 1
        else:
            items.append({"type": "para", "text": " ".join(c.split())})
            i += 1
    return items


def to_tex(tex_path: Path, txt_path: Path, out_path: Path) -> None:
    lines = io.open(tex_path, encoding="utf-8", newline="").read().replace("\r\n", "\n").split("\n")
    blocks = segment(lines)
    items = parse_txt(txt_path)
    if len(blocks) != len(items):
        kinds_t = [b["type"] for b in blocks]
        kinds_x = [x["type"] for x in items]
        first = next((n for n, (a, b) in enumerate(zip(kinds_t, kinds_x)) if a != b), min(len(kinds_t), len(kinds_x)))
        sys.exit(f"STOP: the template has {len(blocks)} blocks, the text has {len(items)}. First difference at block {first + 1}: "
                 f"template {kinds_t[first:first + 2]} vs text {kinds_x[first:first + 2]} "
                 f"(text near it: {items[first]['text'][:60] if first < len(items) else '-'!r}).")
    for n, (block, item) in enumerate(zip(blocks, items), start=1):
        if block["type"] != item["type"]:
            sys.exit(f"STOP: block {n} is a {block['type']} in the template but a {item['type']} in the text: {item['text'][:60]!r}")
    for block, item in reversed(list(zip(blocks, items))):
        if block["type"] not in EDITABLE:
            continue
        new = plain_inline_to_tex(item["text"])
        if block["type"] == "bullet":
            new = "\\item " + new
        if block["type"] == "title":
            new = "\\title{" + new + "}"
        lines[block["start"]:block["end"]] = [new]
    out_path.write_text("\n".join(lines), encoding="utf-8", newline="\n")
    print(f"wrote {out_path}")


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    a = commands.add_parser("to-txt")
    a.add_argument("--tex", required=True, type=Path)
    a.add_argument("--out", required=True, type=Path)
    b = commands.add_parser("to-tex")
    b.add_argument("--tex", required=True, type=Path, help="template main.tex")
    b.add_argument("--txt", required=True, type=Path)
    b.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "to-txt":
        to_txt(args.tex, args.out)
    else:
        to_tex(args.tex, args.txt, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
