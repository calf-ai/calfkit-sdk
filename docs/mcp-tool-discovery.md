# How to give agents MCP tools

To let agents call tools served by an MCP server, deploy one `MCPToolboxNode` per
server and reference it from each agent by name. Discovery is automatic and
cross-process — no wiring, and no bring-up order. (For how it works under the
hood, see the [design spec](designs/mcp-capability-discovery-spec.md).)

A complete, runnable version of this guide — a toolbox fronting a real MCP server
plus a separately-deployed agent that uses it by name — lives in
[`examples/quickstart_mcp/`](../examples/quickstart_mcp/).

## Deploy a toolbox

Wrap one `MCPToolboxNode` per MCP server in a worker and run it:

```python
from calfkit.client import Client
from calfkit.mcp import MCPToolboxNode, StreamableHttpParameters
from calfkit.worker import Worker

docs = MCPToolboxNode(
    "docs_server",
    connection_params=StreamableHttpParameters(url="https://docs.example.com/mcp"),
)

client = Client.connect("kafka:9092")
worker = Worker(client, nodes=[docs])
await worker.run()
```

Connect over HTTP with `StreamableHttpParameters`, or spawn a local stdio server
with `StdioServerParameters` (see the [reference](api.md#mcp-toolboxes)). If the
MCP server is unreachable at startup, the toolbox fails to boot — fix the
connection rather than running dark.

## Give the tools to an agent

Reference the toolbox **by name** with an `MCPToolbox` handle in `tools=[...]`.
This is the default pattern: it works whether the agent shares a process with the
toolbox or runs as a separate deployment, and the agent host never needs the
toolbox's connection config — secrets stay on the toolbox host.

```python
from calfkit.mcp import MCPToolbox

agent = Agent(
    "researcher",
    subscribe_topics="researcher.input",
    model_client=model,
    tools=[MCPToolbox("docs_server")],   # all of the toolbox's tools; scope with include= (below)
)
worker = Worker(client, nodes=[agent])
```

An `MCPToolbox` is an identity-only handle: it carries no connection config, and
deploying one fails immediately with a pointer to the hosting form. A toolbox
that comes up later — or changes its tools — is picked up automatically on the
agent's next turn. No restarts, no bring-up order.

### Pass the toolbox object directly (when the agent shares the definition)

If the agent's process already imports the toolbox definition — the same codebase
— you can pass the `MCPToolboxNode` object itself instead of a name handle:

```python
from my_service.toolboxes import docs   # shared module; deployed elsewhere

agent = Agent(
    "researcher",
    subscribe_topics="researcher.input",
    model_client=model,
    tools=[weather_tool, docs],          # all of the toolbox's tools
)
```

Both forms behave the same; prefer the name handle unless you specifically want
to share the definition.

## Scope the selection

```python
tools=[MCPToolbox("docs_server", include=("search", "fetch"))]   # only these tools
```

Use `include` to pin the exact tool names the agent may see — a server that
starts offering new tools cannot enlarge the agent's surface. (For the object
form, `docs.select(include=("search", "fetch"))` returns the same handle.) If a
requested tool isn't available — the toolbox is offline, or doesn't offer it —
the turn proceeds with whatever tools are available and logs a warning.

If a toolbox tool name collides with a locally configured tool, the local tool
wins and an error is logged.

## Handle outages and topic creation

- If a toolbox **crashes**, agents stop being offered its tools shortly after, so
  they degrade rather than dispatch to a dead toolbox; a **clean shutdown** drops
  them immediately. Either way the toolbox reappears automatically once it's back.
- In dev/CI with provisioning enabled, the control-plane topic is created for you
  — bring up either side first. In production, create it out-of-band as a
  compacted topic (`calf.capabilities`, `cleanup.policy=compact`, RF≥3) like any
  governed topic; see [topic provisioning](topic-provisioning.md).

## Tune it (optional)

The control plane's timings and an alternate Kafka cluster are set with
`Worker(control_plane=ControlPlaneConfig(...))` — every setting has a working
default. See the [`ControlPlaneConfig` reference](api.md#mcp-toolboxes).
