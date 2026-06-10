# Compacted control-plane topic for MCP tool discovery, not live queries

MCP tools are only discoverable through an async `list_tools` call on a live
session owned by the bridge node, but agents — possibly in other worker
processes — need those tools as `ToolBinding`s every turn. The obvious
approach (the one pydantic-ai and the OpenAI Agents SDK use in-process) is to
query the bridge for its tool list at run time. We rejected it: over Kafka, a
per-turn query makes every bridge a synchronous availability dependency of
every agent turn, adds request/reply hops before each model call, and needs
new fan-in state machinery in the agent's chain-of-responsibility handler —
and the "cache it" variant converges back to needing a change-notification
topic anyway.

Instead, all bridges publish their current tool list (a versioned,
calfkit-owned `CapabilityRecord`) to **one cluster-wide compacted topic**,
keyed by bridge id, re-publishing on `tools/list_changed` and as a ~30s
heartbeat. Each agent-hosting worker replays that topic into a small
last-write-wins dict (the Capability View) and agents resolve `MCPTools`
selectors against it per turn. This is event-carried state transfer — the same
shape as Kafka Connect's config topic — and it separates the control plane
(what tools exist, where) from the data plane (the tool calls themselves), so
a bridge outage degrades only the calls routed to it, never the agents' view
of the world.

Consequences worth remembering: records outlive deploys indefinitely
(compaction keeps the latest per key), which is why the wire format is a
calfkit-owned versioned model rather than the vendored `ToolDefinition`;
every capability-consuming worker holds the whole cluster's (tiny) tool map;
and "bridge down" ≠ "tools gone" — removal is an explicit tombstone on clean
shutdown, while crashes only show up as heartbeat staleness. Full design:
`docs/designs/mcp-capability-discovery-spec.md`.
