# MCP toolbox quickstart

Give an agent tools served by an [MCP](https://modelcontextprotocol.io) server —
deployed as its own node and used by a **separately-deployed agent that
references it by name**. This is the end-to-end version of the
[How to give agents MCP tools](../../docs/mcp-tool-discovery.md) guide.

The toolbox here fronts the official **`fetch`** reference server, giving the
agent one tool — `fetch` (URL → markdown) — so it can read the live web.

## What's here

| File | Role |
| --- | --- |
| `fetch_toolbox.py` | The toolbox node — spawns `uvx mcp-server-fetch` and advertises its tools on the control plane. |
| `research_agent.py` | An agent that references the toolbox **by name** (`MCPToolbox("fetcher")`) — no shared import, no connection config. |
| `ask.py` | Sends the agent a question that needs a web fetch. |

## Prerequisites

- A running [Calfkit broker](../../README.md#start-a-calfkit-broker) on `localhost:9092`.
- The CLI extra: `pip install "calfkit[cli]"`.
- [`uv`](https://docs.astral.sh/uv/) on your PATH — the toolbox runs the fetch
  server with `uvx`, which fetches it on first use (no manual install).
- An LLM API key: `export OPENAI_API_KEY=sk-...`.

## Run it (three terminals)

```console
# 1) the toolbox node (spawns the MCP fetch server)
$ calfkit run fetch_toolbox:fetcher

# 2) the agent — references the toolbox by name
$ calfkit run research_agent:agent

# 3) ask it something that needs the web
$ python ask.py
```

The agent's worker discovers the toolbox's `fetch` tool over the control plane
and calls it to read the page. Bring the toolbox and agent up in any order —
selections re-resolve every turn.

## Offline alternative: the `time` server

No network for the fetch demo? Swap the toolbox to the official `time` server
(current time + timezone conversion) by changing one line in `fetch_toolbox.py`:

```python
connection_params=StdioServerParameters(command="uvx", args=["mcp-server-time"]),
```

then ask something like `"What time is it in Asia/Tokyo right now?"`.
