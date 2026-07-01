
# Documentation

## Getting started

**New to building agent teams?** Start with the tutorial **[Build a multi-agent support desk](docs/multi-agent-support-desk.md)** — build and run three agents that discover each other and collaborate by messaging and handoff.

## How-to guides

- **[How to call agents from a client](docs/client-features.md)** — the `agent(name)` gateway and its `send` / `start` / `execute` triad, multi-turn conversations, runtime dependency injection (`deps`), temporary instructions, streaming a run's intermediate steps live with `handle.stream()`, the `events()` firehose, and the typed client errors.
- **[How to chat with an agent from the terminal](docs/chat-with-agents.md)** — discover the agents online, pick one (or name it), and hold a multi-turn conversation in an interactive `ck chat` REPL, watching each turn's tool calls and results stream live.
- **[How to run a local mesh with `ck dev`](docs/local-dev-mesh.md)** — run and iterate with zero broker setup: `ck dev run`/`ck dev chat` connect to (or spawn) a managed in-memory dev broker, with provisioning and reload preset on.
- **[How to guard and transform node invocations](docs/policy-seams.md)** — guard an invocation with `before_node` (transform the input, short-circuit the body, or raise to block), and validate or reshape its output with `after_node`.
- **[How to handle errors and faults](docs/error-handling.md)** — recover from a failed node or callee with `on_node_error` / `on_callee_error`, mint typed faults with `NodeFaultError`, and inspect an `ErrorReport`.
- **[How to let agents discover and use tools at runtime](docs/tool-discovery.md)** — reference deployed function tool nodes by name (or every live one with `discover=True`) with `Tools`; agents discover their schemas at runtime, so an agent's deployment never imports the tool's code.
- **[How to give agents MCP tools](docs/mcp-tool-discovery.md)** — deploy an `MCPToolboxNode` fronting an MCP server and pass it to agents like a tool node; tools are discovered and kept fresh across processes automatically.
- **[How to let agents find and reach each other at runtime](docs/agent-peers.md)** — agents discover each other by name (no hardcoded addresses) and collaborate two ways: consult a peer and keep control (`Messaging`), or transfer control to a specialist (`Handoff`).
- **[Worker lifecycle & embedding](docs/worker-lifecycle.md)** — open long-lived resources at startup and close them on shutdown, publish presence events, and run with `run()`, the embeddable `start()`/`stop()`, or `async with worker:`.
- **[How to tap a topic with a consumer node](docs/consumer-nodes.md)** — terminal sinks that run arbitrary Python against every event on a topic; tap an agent's `publish_topic` to log, persist, or fan out.

## References

- **[API reference](docs/api.md)** — the public surface re-exported from the top-level `calfkit` package, with the key entry-point signatures.
- **[CLI reference](docs/cli.md)** — the `ck run`, `ck chat`, `ck dev`, and `ck topics` commands.
- **[Topic provisioning](docs/topic-provisioning.md)** — the experimental, opt-in topic-creation helper for dev/CI.
