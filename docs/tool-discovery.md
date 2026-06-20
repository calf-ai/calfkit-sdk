# How to give agents discoverable tool nodes

A function tool node (`@agent_tool`) can be given to an agent two ways. Pass the
live node in `tools=[...]` and the agent bakes its schema in at construction (the
**eager** path). Or reference it **by name** with a `Tools(...)` handle and the
agent discovers its schema at runtime from the capability control plane (the
**discovered** path) — so the agent's deployment never imports the tool's code.
This mirrors how MCP toolboxes work (see [MCP tool discovery](mcp-tool-discovery.md));
the two share one plane. (For why it works this way, see the
[design spec](designs/runtime-tool-discoverability-spec.md).)

## Deploy a tool node

```python
from calfkit import Client, Worker, agent_tool


@agent_tool
def add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b


client = Client.connect("kafka:9092")
worker = Worker(client, nodes=[add])   # the tool node advertises automatically
await worker.run()
```

A deployed tool node advertises its single tool on the capability control-plane
topic (`calf.capabilities`) automatically: its host worker publishes the schema
and re-publishes periodically as a liveness heartbeat. The tool name is the
node's identity — both the name the LLM calls and the name an agent references.

## Reference it by name

```python
from calfkit import Agent, Tools

agent = Agent(
    "researcher",
    subscribe_topics="researcher.input",
    model_client=model,
    tools=[Tools("add", "subtract")],   # by name only — no import, no schema
)
worker = Worker(client, nodes=[agent])  # the capability view auto-registers
```

`Tools` is a frozen, identity-only handle — just the tool names (`Tools("add", "subtract")`
or `Tools(names=[...])`). The agent's worker keeps a local view of the capability
plane (gated at boot, so the first turn already sees it) and resolves each name at
the start of every turn, so a tool node that comes up later — or runs in another
process — is picked up on the next turn. No restarts, no bring-up order.

**Which handle?** Reach for `Tools(...)` when you want the agent's deployment
decoupled from the tool's code — the schema travels over the plane. Pass the live
node (`tools=[add]`) when you'd rather bake the schema in and validate arguments
locally *before* dispatch, at the cost of importing the tool. Same node, either
handle. (A discovered binding defers argument validation to the tool node; see the
[design spec](designs/runtime-tool-discoverability-spec.md) for the full trade-off.)

## Names are cluster-wide identities

A tool node's name is its key on the shared capability topic, so names must be
unique across the cluster (like MCP toolbox names). Override the name without
renaming the function — the disambiguation knob:

```python
@agent_tool(name="issues_search")   # the tool name, not the function name "search"
def search(query: str) -> list[str]:
    """Search the issue tracker."""
    ...


# ... then reference it: tools=[Tools("issues_search")]
```

`Tools` binds only tool-node records, so `Tools("github")` pointed at a multi-tool
MCP toolbox (or any non-tool-node record) resolves nothing rather than absorbing
the whole toolbox. If a discovered tool name collides with a tool the agent
already has, the existing one wins and an error is logged.

## Outages and topic creation

Same as the MCP plane: a crashed tool node's advertisement goes **stale** after
roughly three heartbeat intervals and the view hides it (the selection degrades);
a clean shutdown removes it immediately; it reappears on the next turn once it is
back. With provisioning enabled (dev/CI) either worker creates the compacted
`calf.capabilities` topic idempotently; in production create it out-of-band
(`cleanup.policy=compact`, RF≥3). See [topic provisioning](topic-provisioning.md).

## Tune it (optional)

One config surface, shared with the MCP plane:
`Worker(..., control_plane=ControlPlaneConfig(...))` — the boot catch-up timeout,
heartbeat interval, staleness threshold, and a `bootstrap_servers` override for
running the control plane on a separate Kafka cluster. The capability topic name
is fixed (`calf.capabilities`).
