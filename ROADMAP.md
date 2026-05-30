# Roadmap

An index of potential features and changes under consideration for Calf SDK. Each entry links to a detailed design document in `docs/`. Inclusion here does not imply commitment — items may be reshaped, deferred, or dropped after review.

## Proposed

- [Calfkit 1.0](docs/calfkit-v1-design.md) — 1.0 rewrite proposal covering node, agent, and result shape changes
- [Hook System](docs/hooks-design.md) — two-layer middleware proposal for pre/post-run extensibility on nodes
- [Durable Fan-Out Aggregator](docs/durable-fanout-aggregator.md) — replace in-process `_pending_batches` with a Kafka-backed compacted-state aggregator for parallel tool calls
- [MCP Adaptor](docs/mcp-v1-plan.md) — first-class wrapper exposing any MCP server's tools as native calfkit tools, with `mcp.json` interop and codegen-generated schemas ([design doc](docs/mcp-adaptor-design.md), [impl plan](docs/mcp-adaptor-implementation-plan.md), [Phase 0 kickoff](docs/mcp-phase-0-kickoff.md))
- [MCP-over-Kafka RPC Discovery](docs/mcp-discovery-rpc-design.md) — runtime tool discovery alternative to codegen; bridge exposes MCP methods as Kafka request/reply topics; generalizes to resources and prompts
