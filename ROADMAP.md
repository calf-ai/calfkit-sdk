# Roadmap

An index of potential features and changes under consideration for Calf SDK. Each entry links to a detailed design document in `docs/designs/`. Inclusion here does not imply commitment — items may be reshaped, deferred, or dropped after review.

## Accepted

- [Deps-as-dict refactor](docs/designs/deps-as-dict-refactor.md) — `ctx.deps`/`result.deps` are plain dicts and `correlation_id` is a top-level context attribute (closes #144; breaking)
- [Fire-and-forget emit](docs/designs/fire-and-forget-emit.md) — true one-way Client.emit_to_node; nullable callback terminal; opt-in reply TTL (closes #132)
- [Topic Provisioning](docs/topic-provisioning.md) — EXPERIMENTAL opt-in `ProvisioningConfig` to best-effort create Kafka topics for dev/CI (off by default; review for prod)
- [Agent-POV History Projection](docs/designs/agent-pov-projection.md) — always-on POV projection over `message_history` + `ModelResponse.name` stamping for shared-channel multi-agent; portable content-prefix attribution (PR #185; closes #154)

## Proposed

- [Calfkit 1.0](docs/designs/calfkit-v1-design.md) — 1.0 rewrite proposal covering node, agent, and result shape changes
- [Hook System](docs/designs/hooks-design.md) — two-layer middleware proposal for pre/post-run extensibility on nodes
- [ConsumerContext](docs/designs/consumer-context.md) — node-side context for `@consumer` sinks (moves `resources` off the client-facing `NodeResult`); includes a future-direction note on unifying Tool + Consumer contexts on a shared `NodeContext` base (breaking)
- [Durable Fan-Out Aggregator] — replace in-process `_pending_batches` with a Kafka-backed compacted-state aggregator for parallel tool calls. Design-time leads (from the 2026-06 Kafka-state survey in `docs/designs/mcp-capability-discovery-spec.md` §7): this is *sharded* per-key state (frame_id-keyed, partition-local, seconds-lived) — the KTable shape, not the capability-view GlobalKTable shape — so **evaluate quix-streams first** (the field's clear maintenance leader: sharded RocksDB stores + changelog + recovery = exactly this problem; caveat: owns its process loop and runs librdkafka, so it would have to be a standalone aggregator service between tool outputs and agent inputs, not embedded in a FastStream worker). Alternatives to weigh against it: batch state carried in the spawning call frame (`internal_workflow_state`; most calf-native, no new infra), or a hand-rolled frame_id-keyed changelog topic with replay-on-rebalance.
