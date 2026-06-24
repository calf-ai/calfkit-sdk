---
status: accepted
---

# Agent-to-agent messaging is a first-class capability, surfaced as one runtime-rendered tool

We want an agent to discover and message other agents at runtime without importing
their code or holding a reference to their objects. The two obvious shapes both
fail. Making peers entries in `tools=` doesn't fit: a `tools=` entry resolves to a
`ToolBinding` with a **static** `dispatch_topic` serviced by a deployed node
(`tool_dispatch.py:46`), which matches neither a single one-to-all dispatcher nor a
target the model chooses at runtime. Packaging it as a "plugin" fails too: calfkit
has no plugin tier, the load-bearing piece (dynamic per-call routing) lives in the
agent core where no existing seam reaches, and a one-off plugin tier would be
net-negative over-engineering.

We therefore make agent-to-agent messaging a **first-class agent capability**,
configured by a `Messaging(...)` handle in the unified `peers=` surface (curated
names XOR `discover=True`; the `Handoff(...)` handle is its sibling — ADR-0019), and
surfaced to the model as **one built-in tool**, `message_agent(name, message)`. The tool def is
sourced **outside `tools_registry`** but injected as a *real member* of the model's
`ExternalToolset` — it must be (a name absent from the toolset is classified
`unknown` and handled as a retry *inside* the model run, never reaching the agent's
dispatch fork). The dispatch sites fork on the well-known name *before* the
`tools_registry` lookup. Because it is never in `tools_registry`, it is **orthogonal
to `override_agent_tools` by construction**.

The peer **directory is rendered into the tool's description fresh every turn** from
a `ControlPlaneView[AgentCard]` over the compacted `calf.agents` topic. Because tool
definitions are ephemeral request-time data that never persist in message history,
the directory self-heals as peers deploy and go down. Calling the tool is a **Peer
message**: the resolver validates the target against the view at dispatch, derives
the peer's input topic from its name (ADR-0017 — the card carries no topic), seeds a
*fresh* sub-conversation (the peer sees only `message`; `deps`/`correlation_id` ride
forward), dispatches over the standard call path with `callback_topic` = the caller's
return slot and `tag` = the `tool_call_id`, and folds the peer's reply into
`tool_results[tag]`.

The peer needs **no special handler** — it is "a client invocation minus the
client". Because the reply slot and fault rail shipped (ADR-0006), slot *routing* is
generic and peer *faults* run the caller's `on_callee_error` for free. The one
peer-message-specific bit: a peer's reply may carry multiple parts (a text preamble
plus structured data), so the result **serializes all parts into one string** (text
verbatim, data JSON-encoded) rather than the lenient single-part extraction used for
ordinary tool results — discriminated at fold time by `state.tool_calls[tag].tool_name`
(the state-carried signal, since no in-process memory survives the Kafka round-trip).

Advertising is **default-on for every agent, with no opt-out**. We considered a
`discoverable=False` knob, but because every node is reachable on its derived input
topic (ADR-0017), "not advertised" would not mean "not reachable" — it would be a
*visibility* control masquerading as access control. Rather than ship that
half-measure, we defer **both visibility and access control whole to the ACL plane
(#231)**. The card is minimal — name (the wire key) + an optional length-bounded
description; no topic, no system prompt, no tool list — so internals never leak, and
the inbound `message` is treated as untrusted input (it lands as a user-role turn).

Considered and rejected: peers as `tools=` bindings (static topic vs. dynamic
target); a plugin tier (none exists; can't reach per-call dispatch); per-peer tools
(the dispatcher is one-to-all); a callee-side `ToolCallRef` handler (leaks the
caller's conversation or still needs caller-side seeding); a separate `find_agents`
tool (the per-turn directory subsumes it; kept as the large-fleet hatch);
advertising the peer's topic on the card (redundant once derivable, ADR-0017); a
`discoverable=False` knob (visibility-without-access half-measure — deferred to #231).

Consequences: caller-side seeding preserves peer conversation-isolation; the caller
resumes from its own snapshot. Peer messages ride the existing parallel fan-out
mixed with tool calls — and a *lone* peer message (N=1) rides a degenerate
one-element durable batch as well, triggered by an explicit `isolate_state` flag on
its `Call` (the snapshot/restore the round-trip fast path would skip), so the caller
always resumes on its own conversation rather than the peer's authored history; a
lone tool keeps the no-snapshot fast path (spec §5.4/L13). Every agent-hosting worker has a `calf.agents` boot
dependency (default-on advertise). The model-facing verb (`message_agent`, chosen
for breadth over `ask_*`) is intentionally distinct from the mechanism name ("Peer
message") and the config (the `Messaging` handle in `peers=`). Full design in
`docs/designs/agent-mesh-spec.md`.
