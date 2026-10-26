"""
E1 — Echo as an MCP server (Claude Desktop or any MCP client).

Echo stays the engine, the client is the brain: the client's own model decides what to search and writes the
answer; Echo runs the same deterministic search as Luna's tool (rse/search_tool.py) and returns bounded text
excerpts. Entry point: `python mcp_server/server.py` (or `python -m mcp_server`); see mcp_server/README.md.
"""
