"""
Starter for the E.C.H.O Desktop Extension (.mcpb). Claude Desktop runs:

    <project>/.venv/Scripts/python.exe <extension folder>/launch.py <project>

The server code is NOT copied into the extension: it lives in the project, because it imports rse/, ste/ and enp/
and reads the FAISS index and .env from there. This file only checks the folder and runs the project's
mcp_server/server.py as if it were started directly. Anything it prints goes to stderr: stdout is the MCP stream.
"""
import runpy
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("E.C.H.O extension: expected one argument, the project folder")
    server_file = Path(sys.argv[1]).resolve() / "mcp_server" / "server.py"
    if not server_file.is_file():
        sys.exit(f"E.C.H.O extension: {server_file} not found; check the project folder in the extension's settings")
    sys.argv = [str(server_file)]
    runpy.run_path(str(server_file), run_name="__main__")


if __name__ == "__main__":
    main()
