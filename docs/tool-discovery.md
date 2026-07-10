# How to let agents discover and use tools at runtime, without importing their code

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
node (`tools=[add]`) when you'd rather import the tool and bake its schema in. Either
way the agent validates the model's arguments *before* dispatch: a live node uses the
tool's own signature validator (with coercion and any custom field validators), while a
discovered binding checks them against the advertised JSON schema — a non-coercing
subset check, with the tool node staying authoritative on receipt.

## Discover every tool node

To give an agent *all* the tool nodes on the cluster instead of naming each one,
pass `Tools(discover=True)`. It resolves every live tool node from the capability
view at the start of each turn, so a node deployed later is picked up automatically —
no names to maintain:

```python
from calfkit import Agent, Tools

agent = Agent(
    "researcher",
    subscribe_topics="researcher.input",
    model_client=model,
    tools=[Tools(discover=True)],   # every live tool node — no names
)
```

Discover mode takes no names and **owns the agent's tool-node surface**: you cannot
pair `Tools(discover=True)` with an eager tool node or a named `Tools(...)` — that
raises at construction, so pick one or the other. It still composes with an
`MCPToolbox` (a different kind of provider), and it binds only function tool nodes,
never an MCP toolbox's tools.

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
the whole toolbox. An agent's tool surface must also have no duplicate tool names:
referencing the same tool node both eagerly and by name, or naming it twice, raises
at construction — each tool is referenced once.

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
