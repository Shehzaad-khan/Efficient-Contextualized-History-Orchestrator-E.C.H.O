# Echo as an MCP server (E1)

Lets **Claude Desktop** (or any MCP client) search your Echo memory. Echo stays the engine and the client is the
brain: the client's own model decides what to search for and writes the answer. Echo only runs the search and
returns bounded text excerpts. No Luna / OpenAI call is made and nothing listens on a network port; the client
starts this process and talks to it over stdin/stdout.

```
inside the Echo app:   your question → Luna (OpenAI) ⇄ search_memories → answer
through MCP:           your question → Claude (Anthropic, in Claude Desktop) ⇄ search_memories → answer
                                                         └── the same deterministic search in both
```

## Tools (both read-only)

| Tool | What it does |
|---|---|
| `search_memories` | The same search and the same arguments as Luna's tool: `query` (a few keywords), `alternative_phrasings`, `sources` (`gmail` / `chrome` / `youtube`), `since` (start date), `anchor_item` + `anchor_relation` ("after the interview email"), `read_attachment`. Returns up to 10 numbered matches, best first, each with its `memory_id`, source, sender / site / channel, time, a text excerpt and related context. Every result starts with today's date so the client can work out `since`. |
| `get_memory` | More of **one** item by the `memory_id` a search showed: details, link to the original, the first 8,000 characters of its text, attachment names. `read_attachment=true` adds the extracted text of the email's attachments (up to 40,000 characters). |

Not included, on purpose: a "recent activity" tool (a search with `since` set to today covers it when the question has
a topic) and the wellbeing tools (read-only summaries were optional in the decision and are not built). No tool writes
or deletes anything.

## Install and connect (Windows)

1. In the project's virtual environment: `pip install "mcp>=2.3,<3"` (it is also in `requirements.txt`).
2. Make sure **PostgreSQL** is running, and **Redis** for attachment text (WSL: `wsl -d Ubuntu -u root -- sleep infinity`
   keeps it up). The Echo backend does not need to be running; the server reads the database and the FAISS file itself.
3. Open `%APPDATA%\Claude\claude_desktop_config.json` (Claude Desktop → Settings → Developer → Edit Config) and add the
   server. Use the full paths of your venv's `python.exe` and of this file:

```json
{
  "mcpServers": {
    "echo": {
      "command": "<PROJECT>\\.venv\\Scripts\\python.exe",
      "args": ["<PROJECT>\\mcp_server\\server.py"]
    }
  }
}
```

   (`<PROJECT>` is the folder that contains `rse/`, `ste/` and this folder. Backslashes are doubled in JSON.)
4. **Fully quit and restart Claude Desktop** (tray icon → Quit, not just closing the window).
5. Try: *"Search my Echo memory for the TechCorp interview email"*, then *"open the first one"* (uses `get_memory`).
   Claude Desktop asks you to allow each tool call; the tools appear under the tools/connectors icon of the chat box.

The first search after start can take a few seconds longer: the server loads the search models in the background
once it is up, so it answers Claude Desktop's start-up handshake immediately.

## Privacy

- The tools return the same **bounded text excerpts** the app gives Luna: up to 8,000 characters for each of the best 3
  results, 2,000 for the others, 40,000 for an attachment. Never database rows, never the whole store.
- **The connected client's model sees whatever the tools return.** In Claude Desktop that is Anthropic's model, under
  your Claude account's terms, not OpenAI. Don't connect a client whose provider you don't want to see those excerpts.
- Incognito pages, typed text and Slack / Notion / Jira content were never captured, so no tool can return them.
- Everything a tool returns is your own captured data. The tool descriptions tell the model to treat it as data, never as
  instructions, but a malicious mail or page could still try to steer a model; the tools only read, and Claude Desktop
  asks you before each call.
- Errors come back as short text with no connection strings, paths or exception detail.

## When something is off

- **Tools don't show up:** check the config path and JSON, then fully restart Claude Desktop. Logs are in
  `%APPDATA%\Claude\logs\mcp-server-echo.log` (this server logs to stderr only, which Claude Desktop keeps there).
- **"Echo could not search just now":** PostgreSQL is probably not running.
- **Results look old:** the server reloads the FAISS index whenever the backend saves a newer one, but only the running
  backend (its enrichment worker) indexes new captures.
- **Run it by hand** to see start-up errors: `python mcp_server/server.py` (it waits for a client on stdin; Ctrl+C stops it).
- **Tests:** `python -m pytest tests/test_mcp_server.py` (no database, models or Claude Desktop needed; includes a real
  child process over stdio pipes).

## Files

`server.py` registers the two tools and runs over stdio (warm-up of the models starts once it is serving).
`tools.py` holds the logic as plain functions (search, get a memory, reload the index when the backend saved a new one).
The search itself is `rse/search_tool.py` `run_search`, shared with the app.
