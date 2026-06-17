# Roadmap

An index of features and changes for Calfkit SDK, grouped by state. **Accepted** items are decided and shipped (with the PR that landed them). **In progress** items are actively being built. **Proposed** items are under consideration — inclusion here does not imply commitment; they may be reshaped, deferred, or dropped after review. Most entries link to a detailed design document or ADR under `docs/`.

## Accepted

- [Deps-as-dict refactor](docs/designs/deps-as-dict-refactor.md) — `ctx.deps`/`result.deps` are plain dicts and `correlation_id` is a top-level context attribute (closes #144; breaking)
- [Fire-and-forget emit](docs/designs/fire-and-forget-emit.md) — true one-way Client.send; nullable callback terminal; opt-in reply TTL (closes #132)
- [Topic Provisioning](docs/topic-provisioning.md) — EXPERIMENTAL opt-in `ProvisioningConfig` to best-effort create Kafka topics for dev/CI (off by default; review for prod)
- [Agent-POV History Projection](docs/designs/agent-pov-projection.md) — always-on POV projection over `message_history` + `ModelResponse.name` stamping for shared-channel multi-agent; portable content-prefix attribution (PR #185; closes #154)
- [Run/handler unification](docs/designs/run-handler-unification.md) — `run()` folded into the `@handler` registry as a declining `@handler('*')`; `input_args` removed (PR #200; breaking)
- [ConsumerContext](docs/designs/consumer-context.md) — node-side context for `@consumer` sinks; moves `resources` off the client-facing result (now `InvocationResult`) (PR #203). A shared `NodeContext` base unifying Tool + Consumer contexts is noted in the design as a future direction (breaking).
- [In-Node Durable Fan-Out Aggregation](docs/adr/0008-durable-in-node-fanout-store-and-self-published-re-entry.md) — parallel tool-call fan-out folded **in the node** over two node-scoped compacted ktables (no separate aggregator loop, no events topic); single-writer-per-batch comes free from correlation-keyed returns + `max_workers=1`, freshness via a small `ktables` `barrier()` addition. Supersedes the rejected separate-aggregator / quix-streams designs (PR #233; ADR-0008; follow-ups #228/#234).

## In progress

- **Fault Rail & Policy Seams** — typed faults travel the per-delivery reply slot and escalate to callers, with policy seams as the catch-all error surface. The wire model (`ErrorReport` / `FaultMessage` / `NodeFaultError`) shipped in #229; the rail wiring and its ADRs (0003/0004) land with the in-flight fault-rail PR. Builds on the reply slot ([ADR-0006](docs/adr/0006-reply-rides-a-per-delivery-envelope-slot.md)).

## Proposed

- [Calfkit 1.0](docs/designs/calfkit-v1-design.md) — 1.0 rewrite proposal covering node, agent, and result shape changes; folds the extensibility model into three primitives (§12), superseding the standalone two-layer Hook System middleware proposal
- [Node Presence Control Plane](docs/designs/node-presence-substrate-spec.md) — compacted-Kafka presence plane materializing a live view of every node, plus a reusable `ControlPlaneView`/`ControlPlanePublisher` primitive extracted from the MCP capability plane; the substrate for discovery and capability-plane migration (design)
- [Capability-Plane Migration & Ops CLI](docs/designs/capability-plane-migration-and-ops-spec.md) — move the shipped MCP capability plane onto the shared presence primitive (fixing replica-flap / staleness-masking / frozen-view defects) and add a read-only `calfkit nodes` fleet view (design)
- [Agent Discovery & Ask](docs/designs/agent-discovery-and-ask-spec.md) — opt-in agent registry (`calf.agents` / `AgentCard`), peer discovery (`find_agents`), and a caller-side peer-as-tool Ask adapter for agent-to-agent request/reply on the presence substrate (design)
- **Concurrency Model** — future per-node `concurrency=N` knob: N in-process Kafka group members (partition-sharded, serial-per-partition, safe by construction) instead of FastStream `max_workers>1` (which races the await-spanning fold). Shares one batch-store materialization across the N consumers, amortizing node-scoped-table RAM. Deferred from the in-node fan-out work to keep that scoped (design doc not yet committed).

### Deferred from MCP capability discovery (deliberate v1 boundaries)

Shipped in PRs #209/#210/#211; each edge below was consciously cut and is
recorded with rationale in
[the design spec](docs/designs/mcp-capability-discovery-spec.md):

- [Behavioral staleness](docs/designs/mcp-capability-discovery-spec.md) — v1 only *logs* when a toolbox's capability record goes stale (§2 decision 8, §8.4); v2 would hide stale advertisements from the model and/or let `strict` selectors fail on staleness. The heartbeat data it needs already flows.
- [Toolbox decommission tooling](docs/designs/mcp-capability-discovery-spec.md) — removal currently happens only via clean shutdown (§6, including the accepted deploy-window tool-gap trade-off); permanent retirement without a final clean shutdown needs an explicit operator command, and the §6 trade-off should be revisited if deploy gaps hurt.
- [Secured control plane](docs/designs/mcp-capability-discovery-spec.md) — v1 assumes the capability topic is reachable with bootstrap servers alone (§8.3 security boundary: addressing data may be retained on the client, credentials never); SASL/SSL support needs an explicit security pass-through on `MCPDiscoveryConfig`.
- [MCP session read-timeout backstop](docs/designs/mcp-capability-discovery-spec.md) — the discovery-path deadlock is fixed (§8.5 review notes), but individual `call_tool`/`list_tools` RPCs against a hung MCP server still wait unboundedly; a configurable `read_timeout_seconds` on the `ClientSession` would bound them (interacts with the fault-rail tool-call failure semantics — design before bolting on).
- **Native toolbox node** — the non-MCP twin of `MCPToolbox`: one deployable node hosting many `@tool`-decorated methods (registry-collected), advertising N bindings on one topic. Sketched during the ToolBinding design sessions; would reuse `ToolProvider` as-is.
