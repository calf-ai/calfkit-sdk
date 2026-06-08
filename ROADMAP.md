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
- [Durable Fan-Out Aggregator] — replace in-process `_pending_batches` with a Kafka-backed compacted-state aggregator for parallel tool calls
