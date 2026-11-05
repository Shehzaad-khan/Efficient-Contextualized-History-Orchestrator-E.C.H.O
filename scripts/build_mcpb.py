"""
Build the E.C.H.O Desktop Extension for Claude Desktop: dist/echo.mcpb.

    python scripts/build_mcpb.py

A .mcpb file is a zip with manifest.json at its root. This one holds only three files:
    manifest.json   mcp_server/bundle/manifest.json  (display name, logo, how to start the server)
    launch.py       mcp_server/bundle/launch.py      (runs the project's mcp_server/server.py)
    icon.png        mcp_server/echo-icon.png         (the E.C.H.O logo)
The server code stays in the project. Install: double-click dist/echo.mcpb, or Claude Desktop -> Settings -> Extensions.
"""
import json
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BUNDLE_DIR = ROOT / "mcp_server" / "bundle"
OUTPUT = ROOT / "dist" / "echo.mcpb"

FILES = {
    "manifest.json": BUNDLE_DIR / "manifest.json",
    "launch.py": BUNDLE_DIR / "launch.py",
    "icon.png": ROOT / "mcp_server" / "echo-icon.png",
}

REQUIRED_FIELDS = ("manifest_version", "name", "version", "description", "author", "server")


def check_manifest(manifest: dict) -> list[str]:
    """The fields the MCPB spec requires, and that the files the manifest names are in the bundle."""
    problems = [f"missing field: {field}" for field in REQUIRED_FIELDS if field not in manifest]
    if "name" not in manifest.get("author", {}):
        problems.append("missing field: author.name")
    for named in (manifest.get("icon"), manifest.get("server", {}).get("entry_point")):
        if named and named not in FILES:
            problems.append(f"manifest names {named}, which is not in the bundle")
    return problems


def main() -> int:
    missing = [str(path) for path in FILES.values() if not path.is_file()]
    if missing:
        print("Missing source files:", *missing, sep="\n  ")
        return 1
    manifest = json.loads(FILES["manifest.json"].read_text(encoding="utf-8"))
    problems = check_manifest(manifest)
    if problems:
        print("manifest.json is not valid:", *problems, sep="\n  ")
        return 1

    OUTPUT.parent.mkdir(exist_ok=True)
    with zipfile.ZipFile(OUTPUT, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, path in FILES.items():
            bundle.write(path, arcname=name)
    print(f"Built {OUTPUT} ({OUTPUT.stat().st_size:,} bytes): {', '.join(FILES)}")
    print(f"{manifest['display_name']} {manifest['version']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
