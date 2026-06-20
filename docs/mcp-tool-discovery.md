# How to give agents MCP tools

To let agents call tools served by an MCP server, deploy one `MCPToolboxNode`
per server and give each agent a name-only `MCPToolbox` handle in `tools=[...]`
— like a tool node. Discovery is automatic and cross-process: the toolbox advertises
its tools on a control-plane topic, and each agent's worker keeps a local view
of it. There is no discovery configuration in the happy path. (For why it
works this way, see the
[design spec](designs/mcp-capability-discovery-spec.md).)

## Deploy a toolbox

```python
from calfkit.client.client import Client
from calfkit.mcp.mcp_toolbox import MCPToolboxNode
from calfkit.mcp.mcp_transport import StreamableHttpParameters
from calfkit.worker.worker import Worker

docs = MCPToolboxNode(
    "docs_server",
    connection_params=StreamableHttpParameters(url="https://docs.example.com/mcp"),
)

client = Client.connect("kafka:9092")
worker = Worker(client, nodes=[docs])
await worker.run()
```

On startup the toolbox connects to the MCP server and lists its tools; its host
worker then advertises them on the capability control-plane topic
(`calf.capabilities`), refreshes them whenever the server reports a tool-list
change, and re-publishes periodically as a liveness heartbeat (a tool-list change
lands on the next heartbeat). If the MCP server is unreachable at startup, the
worker fails to boot — fix the connection rather than running dark.

## Give the tools to an agent — same or different process

Pass the toolbox object in `tools=[...]`. Passing it never contacts the MCP
session and does not deploy the toolbox; an agent in another process imports
the same definition:

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

If the agent host doesn't import the toolbox definition — or must not hold
its connection config at all (secrets stay on the toolbox host) — reference
the toolbox **by name** instead:

```python
from calfkit.mcp import MCPToolbox

agent = Agent(
    "researcher",
    subscribe_topics="researcher.input",
    model_client=model,
    tools=[MCPToolbox("docs_server", include=("search",))],
)
```

An `MCPToolbox` is a frozen, identity-only handle: it can never carry connection
params, and deploying one fails immediately with a pointer to the hosting
form. (`MCPToolboxNode.select(...)` returns the same type.)

The agent's worker detects the declaration and maintains the local capability
view, gated at boot so the first turn already sees it. Selections re-resolve
at the start of every agent turn, so a toolbox that comes up later — or
changes its tools — is picked up on the next turn. No restarts, no bring-up
order.

## Scope the selection

```python
tools=[docs.select(include=["search", "fetch"])]      # only these tools
```

Use `include` to pin the exact tool names the agent may see — a server that
starts advertising new tools cannot enlarge the agent's surface. An unresolved
selection (the toolbox is offline, or a requested tool isn't advertised) logs a
warning and the turn runs with whatever resolved.

If a toolbox tool name collides with a locally configured tool, the local tool
wins and an error is logged.

## Handle outages and topic creation

- A **crashed** toolbox stops heartbeating; its advertisement goes **stale**
  after roughly three heartbeat intervals, and the view then hides it — so
  agents stop being offered its tools (the selection degrades) rather than
  dispatching to a dead toolbox. A **clean shutdown** removes the advertisement
  immediately. Either way the toolbox reappears on the next turn once it is back
  and heartbeating.
- With provisioning enabled (dev/CI), toolbox and agent workers both create
  the compacted capability topic idempotently — bring up either side first.
  In production, create the topic out-of-band (`cleanup.policy=compact`,
  RF≥3) like any governed topic; see
  [topic provisioning](topic-provisioning.md).

## Tune it (optional)

Every knob has a working default, and there is one config surface:
`Worker(..., control_plane=ControlPlaneConfig(...))` — the boot catch-up
timeout, the heartbeat interval, a staleness threshold, and a `bootstrap_servers`
override for running the control plane on a separate Kafka cluster. The
capability topic name is fixed (`calf.capabilities`). Toolboxes and agents
inherit the config of the worker that hosts them.
