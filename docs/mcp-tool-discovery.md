# How to give agents MCP tools

An `MCPToolbox` node fronts one MCP server: it owns the live MCP session,
services tool calls sent to its topic, and automatically advertises its tools
to agents anywhere in the cluster. You deploy it like any node and pass it to
agents like any tool — there is no discovery configuration in the happy path.

## Deploy a toolbox

```python
from calfkit.mcp import StreamableHttpParameters
from calfkit.mcp.mcp_toolbox import MCPToolbox

docs = MCPToolbox("docs_server", connection_params=StreamableHttpParameters(url="https://docs.example.com/mcp"))

client = Client.connect("kafka:9092")
worker = Worker(client, nodes=[docs])
await worker.run()
```

On startup the toolbox connects to the MCP server, lists its tools, and
advertises them on the capability topic (`mcp.capabilities`), re-advertising
whenever the server's tool list changes and as a periodic heartbeat. If the
MCP server is unreachable at startup, the worker fails to boot — fix the
connection rather than running dark.

## Give the tools to an agent — same or different process

Pass the toolbox object in `tools=[...]`, exactly like a tool node. Passing it
never touches the MCP session and does not deploy the toolbox — agents in
another process just import the same definition:

```python
from my_service.toolboxes import docs   # shared module; deployed elsewhere

agent = Agent(
    "researcher",
    subscribe_topics="researcher.input",
    model_client=model,
    tools=[weather_tool, docs],          # all of the toolbox's tools
)
worker = Worker(client, nodes=[agent])   # capability view auto-registers
```

The agent's worker detects the declaration and maintains a local view of the
capability topic (gated at boot so the first turn already sees it). Tools
resolve fresh at the start of every agent turn, so a toolbox that comes up
later, or changes its tools, is picked up on the next turn — no restarts, no
bring-up order.

## Scope or require the selection

```python
tools=[docs.select(include=["search", "fetch"])]      # only these tools
tools=[docs.select(include=["search"], strict=True)]  # fail the turn if unavailable
```

`include` pins the exact tool names the agent may see — a server suddenly
advertising new tools cannot enlarge the agent's surface. By default an
unresolved selection logs a warning and the turn runs with the tools that did
resolve; `strict=True` raises before the model runs instead.

If a toolbox-provided tool name collides with a locally configured tool, the
local tool wins and an error is logged — a remote server never silently
shadows your own tools.

## Operational notes

- **Outages:** a crashed toolbox keeps its advertisement (calls to it fail
  visibly; agents keep last-known tools). A *clean shutdown* removes the
  advertisement until the toolbox restarts.
- **Topic creation:** with provisioning enabled (dev/CI), toolbox and agent
  workers both create the compacted capability topic idempotently — no
  bring-up order. In production, create it out-of-band (`cleanup.policy=compact`,
  RF≥3) like any governed topic; see [topic-provisioning](./topic-provisioning.md).
- **Tuning (all optional):** `Worker(..., mcp_discovery=MCPDiscoveryConfig(...))`
  and `MCPToolbox(..., discovery=...)` — topic name, boot catch-up timeout,
  heartbeat interval, and a bootstrap-servers override for running the
  capability topic on a separate cluster.
