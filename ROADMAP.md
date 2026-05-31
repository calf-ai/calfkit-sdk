# Roadmap

An index of potential features and changes under consideration for Calf SDK. Each entry links to a detailed design document in `docs/`. Inclusion here does not imply commitment — items may be reshaped, deferred, or dropped after review.

## Proposed

- [Calfkit 1.0](docs/calfkit-v1-design.md) — 1.0 rewrite proposal covering node, agent, and result shape changes
- [Hook System](docs/hooks-design.md) — two-layer middleware proposal for pre/post-run extensibility on nodes
- [Durable Fan-Out Aggregator](docs/durable-fanout-aggregator.md) — replace in-process `_pending_batches` with a Kafka-backed compacted-state aggregator for parallel tool calls
- [MCP-over-Kafka RPC Discovery](docs/mcp-discovery-rpc-design.md) — runtime tool discovery alternative to codegen; bridge exposes MCP methods as Kafka request/reply topics; generalizes to resources and prompts
