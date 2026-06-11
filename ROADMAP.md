# Roadmap

An index of potential features and changes under consideration for Calf SDK. Each entry links to a detailed design document in `docs/designs/`. Inclusion here does not imply commitment — items may be reshaped, deferred, or dropped after review.

## Accepted

- [Deps-as-dict refactor](docs/designs/deps-as-dict-refactor.md) — `ctx.deps`/`result.deps` are plain dicts and `correlation_id` is a top-level context attribute (closes #144; breaking)
- [Fire-and-forget emit](docs/designs/fire-and-forget-emit.md) — true one-way Client.send; nullable callback terminal; opt-in reply TTL (closes #132)
- [Topic Provisioning](docs/topic-provisioning.md) — EXPERIMENTAL opt-in `ProvisioningConfig` to best-effort create Kafka topics for dev/CI (off by default; review for prod)
- [Agent-POV History Projection](docs/designs/agent-pov-projection.md) — always-on POV projection over `message_history` + `ModelResponse.name` stamping for shared-channel multi-agent; portable content-prefix attribution (PR #185; closes #154)

## Proposed

- [Calfkit 1.0](docs/designs/calfkit-v1-design.md) — 1.0 rewrite proposal covering node, agent, and result shape changes
- [Hook System](docs/designs/hooks-design.md) — two-layer middleware proposal for pre/post-run extensibility on nodes
- [ConsumerContext](docs/designs/consumer-context.md) — node-side context for `@consumer` sinks (moves `resources` off the client-facing `NodeResult`); includes a future-direction note on unifying Tool + Consumer contexts on a shared `NodeContext` base (breaking)
- [Durable Fan-Out Aggregator] — replace in-process `_pending_batches` with a Kafka-backed compacted-state aggregator for parallel tool calls. Design-time leads (from the 2026-06 Kafka-state survey in `docs/designs/mcp-capability-discovery-spec.md` §7): this is *sharded* per-key state (frame_id-keyed, partition-local, seconds-lived) — the KTable shape, not the capability-view GlobalKTable shape — so **evaluate quix-streams first** (the field's clear maintenance leader: sharded RocksDB stores + changelog + recovery = exactly this problem; caveat: owns its process loop and runs librdkafka, so it would have to be a standalone aggregator service between tool outputs and agent inputs, not embedded in a FastStream worker). Alternatives to weigh against it: batch state carried in the spawning call frame (`internal_workflow_state`; most calf-native, no new infra), or a hand-rolled frame_id-keyed changelog topic with replay-on-rebalance.

### Deferred from MCP capability discovery (deliberate v1 boundaries)

Shipped in PRs #209/#210/#211; each edge below was consciously cut and is
recorded with rationale in
[the design spec](docs/designs/mcp-capability-discovery-spec.md):

- [Behavioral staleness](docs/designs/mcp-capability-discovery-spec.md) — v1 only *logs* when a toolbox's capability record goes stale (§2 decision 8, §8.4); v2 would hide stale advertisements from the model and/or let `strict` selectors fail on staleness. The heartbeat data it needs already flows.
- [Toolbox decommission tooling](docs/designs/mcp-capability-discovery-spec.md) — removal currently happens only via clean shutdown (§6, including the accepted deploy-window tool-gap trade-off); permanent retirement without a final clean shutdown needs an explicit operator command, and the §6 trade-off should be revisited if deploy gaps hurt.
- [Secured control plane](docs/designs/mcp-capability-discovery-spec.md) — v1 assumes the capability topic is reachable with bootstrap servers alone (§8.3 security boundary: addressing data may be retained on the client, credentials never); SASL/SSL support needs an explicit security pass-through on `MCPDiscoveryConfig`.
- [MCP session read-timeout backstop](docs/designs/mcp-capability-discovery-spec.md) — the discovery-path deadlock is fixed (§8.5 review notes), but individual `call_tool`/`list_tools` RPCs against a hung MCP server still wait unboundedly; a configurable `read_timeout_seconds` on the `ClientSession` would bound them (interacts with tool-call failure semantics — design before bolting on).
- [Native toolbox node] — the non-MCP twin of `MCPToolbox`: one deployable node hosting many `@tool`-decorated methods (registry-collected), advertising N bindings on one topic. Sketched during the ToolBinding design sessions; would reuse `ToolProvider` as-is.

