# Calfkit MCP Adaptor — Design Document

**Status:** Draft proposal
**Document version:** 1.0
**Last updated:** 2026-05-27
**Tracking issue:** [#158](https://github.com/calf-ai/calfkit-sdk/issues/158)

---

## 1. Summary

This document proposes a first-class adaptor that turns any [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server into a set of native calfkit tools — usable by any agent, deployable as part of any worker, and consistent with calfkit's existing distributed tool model.

The design centres on a single user-facing object, `McpServer`, that behaves as **both** a node-group (when passed to `Worker(nodes=[...])`) and a tool-source (when passed to `Agent(tools=[...])`). The same import works in single-process dev setups and split agent/tool deployments without modification. Drop-in compatibility with the de facto `mcp.json` configuration format means users can copy working configs from Claude Desktop, Cursor, Cline, and Gemini CLI.

The design preserves every existing calfkit invariant:

- Each MCP tool maps to one logical `ToolNodeDef` with its own deterministic topic.
- Tool schemas are visible to the LLM at per-tool granularity (no `mcp_call` mega-tool).
- Tool errors flow through `RetryPromptPart` (LLM-retryable) and `FailedToolCall` (operator-visible) — no new exception types.
- Concurrency leverages MCP's native JSON-RPC request-ID pipelining; no locks or serial dispatch.

Lifecycle, schema discovery, content adaptation, and credential propagation are designed to be invisible to the typical user but configurable for advanced cases.

---

## 2. Motivation

The MCP ecosystem has become the de facto standard for exposing tools to AI agents. Production-quality MCP servers exist for Gmail, Google Workspace, GitHub, Slack, Linear, Notion, Postgres, filesystems, browsers, and hundreds of others — many maintained by the original vendors. Today, using any of these from a calfkit agent requires hand-writing a `@agent_tool` wrapper, duplicating effort across the community and gating calfkit from the broader MCP catalogue.

A first-class adaptor turns the entire MCP catalogue into calfkit's tool catalogue by extension, without compromising the event-driven, microservice-shaped runtime that distinguishes calfkit from in-process agent SDKs.

---

## 3. Goals and non-goals

### 3.1 Goals

- **DX parity with in-process SDKs.** Adding an MCP server should be three lines or fewer. The user should never construct a `ToolNodeDef` by hand for MCP.
- **Zero new mental models.** MCP tools are tools. They appear in `Agent(tools=[...])` and `Worker(nodes=[...])` the same way native `@agent_tool`s do.
- **One source of truth across processes.** A single shared module defines the MCP server; agent workers and tool workers both import it.
- **Standard config interop.** Accept `mcp.json`-shaped configs verbatim.
- **Honest error semantics.** MCP-level tool errors (`isError: true`) become LLM-retryable retry prompts; transport-level errors become operator-visible failures.
- **Concurrency by default.** Multiple agents calling MCP tools in parallel pipeline through the underlying session via JSON-RPC request IDs — no per-call serialisation.
- **Multi-server-per-deployment.** Gmail + GitHub + Postgres can live in the same worker, addressed by the same agent, with no special multi-server mode.
- **Multi-tenant credential propagation.** Per-invocation OAuth tokens / user context flow through the existing `Deps` plumbing.

### 3.2 Non-goals (v1)

- **Resources and prompts.** MCP servers also expose `resources/*` and `prompts/*` primitives. v1 covers tools only. Resources and prompts require new node abstractions and a wider rewire of LLM-context construction.
- **Server-initiated sampling.** Routing an MCP server's `sampling/createMessage` request back to a calfkit agent is feasible but requires reverse-topic protocol design. v1 declines the capability at `initialize`; servers that strictly require it will fail fast.
- **Elicitation.** Server-initiated user prompts are not meaningful in a server-side, automated multi-agent system.
- **Hot reload via `notifications/tools/list_changed`.** v1 logs and ignores; v2 may rebuild the registry.
- **`outputSchema` validation.** Pass-through of `structuredContent` as-is; v2 may add Pydantic-side validation.
- **Codegen for IDE-completable `gmail.search(...)`.** A separate `calfkit mcp codegen` roadmap item.

---

## 4. Background: MCP protocol primer (relevant subset)

This section is the minimum protocol context required to evaluate the design. It is intentionally narrow.

### 4.1 Transports

- **stdio.** The server runs as a subprocess; JSON-RPC frames pass over stdin/stdout. One client per process. Local only.
- **Streamable HTTP.** The server is an independent HTTP service. Client→server uses POST; server→client uses optional SSE on the same endpoint. Stateful via `Mcp-Session-Id` header. Multi-client. OAuth 2.1 Bearer tokens.
- **SSE (legacy).** Being phased out in favour of Streamable HTTP. v1 supports stdio and Streamable HTTP; legacy SSE is out of scope.

### 4.2 Lifecycle

A client→server connection is opened, then `initialize` is sent with `protocolVersion`, `capabilities`, and `clientInfo`. The server responds with its own version, capabilities, and `serverInfo`. The client then sends `notifications/initialized`. After this handshake, `tools/list` returns the catalogue.

### 4.3 Tool schema

Each tool returned by `tools/list` has:

- `name` (string), `title` (optional, display)
- `description`
- `inputSchema` (JSON Schema for arguments)
- `outputSchema` (optional JSON Schema for structured return)
- `annotations` (`readOnlyHint`, `destructiveHint` *defaulting to true*, `idempotentHint`, `openWorldHint`)
- `_meta` (free-form metadata)

### 4.4 Tool result

A `tools/call` response carries:

- `content`: array of content blocks (`text`, `image`, `audio`, `resource_link`, `embedded_resource`)
- `structuredContent` (optional): typed object matching `outputSchema`
- `isError` (optional bool): semantic tool error — distinct from a JSON-RPC transport error
- `_meta`

### 4.5 Concurrency model

JSON-RPC requests carry IDs. A single `ClientSession` can have many in-flight requests; responses correlate by ID. **No client-side locking is required for concurrent `call_tool`.**

### 4.6 Bidirectional capabilities (out of v1 scope)

Servers can request *sampling* (call the client's LLM) and *elicitation* (ask the user). Both are negotiated at `initialize`. v1 explicitly does not advertise these capabilities.

---

## 5. Proposed API

### 5.1 Construction

```python
from calfkit.mcp import McpServer

# stdio transport (local subprocess)
gmail = McpServer.stdio("npx", "-y", "@modelcontextprotocol/server-gmail")

# Streamable HTTP transport (remote server)
github = McpServer.http(
    "https://api.github.com/mcp",
    auth="bearer $GITHUB_TOKEN",  # env-substituted at construction
)
```

Construction is **cheap, synchronous, and I/O-free**. No subprocess is spawned and no network call is made. Schemas are not yet known.

### 5.2 Usage in an agent

```python
agent = Agent(
    "scribe",
    tools=[
        get_weather,                                  # native @agent_tool
        gmail,                                        # all tools from gmail
        github.only("create_issue", "list_issues"),   # explicit subset
    ],
    model_client=...,
)
```

`McpServer` behaves as a `ToolNodeDef`-yielding iterable when passed to `Agent(tools=...)`. The agent's existing `tools_registry` construction in `calfkit/nodes/agent.py:163-167` is preserved unchanged; the registry is built from the discovered set after the startup pass described in §6.3.

### 5.3 Usage in a worker

```python
worker = Worker(client, nodes=[gmail, github, agent])  # one Worker
await worker.run()
```

Or split across two workers without code changes:

```python
# tools_worker.py
worker = Worker(client, nodes=[gmail, github])
await worker.run()

# agent_worker.py
worker = Worker(client, nodes=[agent])
await worker.run()
```

In both cases, the underlying long-lived MCP session lives in **only** the worker that hosts `gmail` / `github` as a node. The agent worker performs a separate short-lived `initialize` + `tools/list` connection at startup purely for schema resolution, then disconnects. See §6.

### 5.4 Filtering and renaming

```python
gmail                                              # all tools
gmail.only("search", "send")                       # explicit allowlist
gmail.exclude("delete_draft")                      # blocklist
gmail.where(readonly=True)                         # by MCP annotation
gmail.where(lambda t: "draft" in t.name)           # predicate
gmail.prefix("inbox")                              # gmail.search → inbox.search
gmail.rename({"search": "find"})                   # explicit rename
```

`.only(...)` and `.exclude(...)` accept tool names as exported by the server (pre-rename). `.where(...)` takes either an annotation keyword (`readonly`, `destructive`, `idempotent`, `open_world`) or a predicate `(ToolMeta) -> bool`. Multiple filters compose: `gmail.exclude("delete_draft").where(readonly=False)`.

The filtered/renamed object is itself an `McpServer` — operations chain.

### 5.5 Per-call context (multi-tenant credentials)

```python
gmail = McpServer.http(
    "https://gmail-mcp.acme.com/mcp",
    per_call=lambda ctx: {
        "headers": {"Authorization": f"Bearer {ctx.deps['gmail_token']}"},
        "meta": {"user_id": ctx.deps["user_id"]},
    },
)
```

`per_call` receives the same `ToolContext` that native `@agent_tool` functions see (`calfkit/models/tool_context.py`). For HTTP transports, `headers` are merged into the outgoing request. For stdio transports, `headers` is silently ignored (no transport-level header concept). For both, `meta` is merged into the call's MCP `_meta` field. Returning `None` or an empty dict from `per_call` is a no-op.

### 5.6 `mcp.json` interop

```python
from calfkit.mcp import McpServers

# From a file
servers = McpServers.from_file("./mcp.json")

# Or from a dict (identical shape)
servers = McpServers.from_config({
    "mcpServers": {
        "gmail":  {"command": "npx", "args": ["-y", "@modelcontextprotocol/server-gmail"]},
        "github": {"type": "http", "url": "https://api.github.com/mcp",
                    "headers": {"Authorization": "Bearer $GH_TOKEN"}},
    }
})

# servers is a Mapping[str, McpServer]
agent  = Agent("scribe", tools=[*servers.values()], ...)
worker = Worker(client, nodes=[*servers.values(), agent])
```

`McpServers.from_config()` accepts both the canonical `{"mcpServers": {...}}` envelope and the bare inner dict for convenience. Environment-variable substitution (`$VAR` and `${VAR}`) applies inside `command`, `args`, `env`, `url`, and `headers` values. Unset variables raise at construction time rather than silently producing empty strings.

---

## 6. Wire model and internal architecture

### 6.1 Topic naming

Each MCP tool receives a deterministic topic pair derived from `(server_name, tool_name)`:

- Input: `mcp.{server_name}.{tool_name}.input`
- Output: `mcp.{server_name}.{tool_name}.output`

The `mcp.*` prefix distinguishes MCP-backed tools from native `tool.*` tools at the wire/observability layer. The names are deterministic so the agent worker and tool worker derive identical topics without any coordination channel.

Renaming (§5.4) affects only the **agent-facing name** the LLM sees; the underlying topic uses the server-exported name to keep wire identity stable across renames.

### 6.2 Worker lifecycle (subprocess management)

A `Worker` running with one or more `McpServer` nodes performs the following on `run()`:

1. **Pre-subscribe startup phase** (before any Kafka subscriber registers):
   - For each `McpServer` in `nodes`, open the underlying transport (spawn subprocess for stdio, open HTTP session for streamable HTTP).
   - Send `initialize` with calfkit's `clientInfo` and capabilities (notably, **`sampling` and `elicitation` are not declared**).
   - Send `notifications/initialized`.
   - Call `tools/list`, apply filters/renames, populate the in-memory tool registry.
   - If the server declares `sampling` or `elicitation` as required, fail fast with a clear error referencing the v1 non-goal.
2. **Subscriber registration phase** (existing `register_handlers()` flow). Each MCP-backed tool gets one subscriber on its `mcp.<server>.<tool>.input` topic, sharing the worker's `group_id`.
3. **Steady state.** Subscribers dispatch envelopes; each handler awaits `session.call_tool(name, args, _meta=...)` and replies via the existing `ReturnCall` path.
4. **Shutdown (SIGTERM, exception, or `asyncio.CancelledError`).** All MCP sessions close cleanly via the Python SDK's async context managers; subprocesses are sent SIGTERM, then SIGKILL after a grace period (default 5s, configurable per `McpServer`).

The startup phase is implemented as a FastStream lifespan hook on the existing `FastStream(...).run()` call in `calfkit/worker/worker.py:67-71`.

### 6.3 Schema discovery for agent-only workers

A worker that hosts only `agent` (and not the MCP server) still needs the tool schemas to populate the agent's `tools_registry`. The same `McpServer` object, when iterated in agent-worker context, performs a **short-lived discovery connection** during the worker's startup phase:

- Open transport, `initialize`, `tools/list`, close.
- Apply filters/renames.
- Produce the same set of `ToolNodeDef`-shaped objects, but with `_tool=None` (no callable body — this worker will never invoke them).

For Streamable HTTP servers, this is a single ephemeral HTTP exchange. For stdio servers, it briefly spawns the subprocess on the agent worker as well. This is the price of stdio's local-only nature; for multi-tenant production deployments, Streamable HTTP is recommended.

A caching escape hatch (`McpServer.cache_to("/path/to/snapshot.json")`) is provided for production deployments that want fully reproducible startup with no boot-time MCP contact from the agent worker. The snapshot is consumed identically to a live discovery result.

### 6.4 Single-process worker

When `gmail` and `agent` are in the same `Worker(nodes=[gmail, agent])`:

- The startup phase opens the long-lived session **once** (the node-group path, not the discovery-only path).
- The agent's tool registry consumes the same in-memory registry the subscribers use.
- No second subprocess; no duplicate `initialize`.

### 6.5 Concurrency

A single `ClientSession` handles N in-flight `call_tool` requests via JSON-RPC request IDs. The worker's existing per-subscriber concurrency (FastStream `max_workers`) drives parallelism — each consumer task awaits its own `call_tool` and the session multiplexes underneath. **No locks, no pools, no per-tool queues.**

The only synchronisation point is `initialize` itself, which is awaited once at startup before any subscriber accepts traffic.

---

## 7. Error mapping

MCP exposes two distinct error layers; calfkit maps them to its two existing tool-error paths.

### 7.1 Tool-semantic errors (`CallToolResult.isError: true`)

These are errors the LLM is meant to see and react to: "permission denied", "no results", "invalid argument value", etc. They map to `RetryPromptPart`, the existing path in `calfkit/nodes/agent.py:316-321` used for arg validation and unknown tool names.

The MCP `content` field is concatenated into the retry prompt's `content` (text blocks only; non-text blocks are summarised as `[image omitted]`, etc.). The LLM sees the error message and can retry with adjusted arguments.

### 7.2 Transport / protocol errors

Connection lost, JSON-RPC parse error, unexpected method response, subprocess died, HTTP 5xx, OAuth token revoked — these are operational failures. They flow through the existing `FailedToolCall` marker (`calfkit/models/state.py`) and raise as `ToolExecutionError` at the agent (`calfkit/exceptions.py`), consistent with native tool exceptions.

This split mirrors the existing native-tool handling in `calfkit/nodes/tool.py:122-184`: `ModelRetry` exceptions become `RetryPromptPart`, all other exceptions become `FailedToolCall`.

### 7.3 Initialization-time errors

`initialize` failure (server died on launch, version mismatch, required capability calfkit doesn't support) fails `Worker.run()` before any subscriber is registered. This is preferred over partial startup — operators see the error immediately on deploy rather than discovering it on first traffic.

---

## 8. Content and return-value mapping

### 8.1 Output preference

The agent's view of an MCP tool's return is constructed as follows:

1. If `structuredContent` is present, **return that** (passed through as a dict). The text content array is dropped; `structuredContent` is the typed primary surface.
2. Otherwise, **concatenate text blocks** into a single string and return.
3. Non-text content blocks (`image`, `audio`, `resource_link`, `embedded_resource`) are **summarised as placeholders** in v1: `[image: 512x512 image/png]`, `[resource: example://path.pdf]`. The full content is not surfaced to the LLM.

The placeholder behaviour is honest about a v1 limitation and avoids the failure mode of silently dropping content the user expected.

### 8.2 Wire serialisation

The return value rides the existing `ToolReturn(return_value=..., metadata={"tool_call_id": ...})` path in `calfkit/nodes/tool.py:120-121`. `structuredContent` is JSON-serialisable by construction (it conforms to its `outputSchema`). The eager `pydantic_core.to_json` check on `tool.py:121` continues to guard against any wire-unsafe values.

### 8.3 Rich content in v2

A future revision can extend `ToolReturn` to a multi-block result that maps directly to calfkit's `ContentPart` hierarchy (`TextPart`, `DataPart`, `FilePart`) — at which point images and embedded resources can ride through to providers that support multi-modal inputs. v1 surfacing this would require changes to `pydantic-ai`'s return path; deferred.

---

## 9. Multi-tenant context

The `per_call` hook (§5.5) receives the same `ToolContext` instance native tools see. It is invoked **per envelope**, immediately before `session.call_tool(...)`. The hook is sync; for now, async hooks are out of scope (the contract is "compute a small dict from already-resolved deps", which is sync-shaped).

```python
gmail = McpServer.stdio(
    "node", "gmail-mcp.js",
    per_call=lambda ctx: {"meta": {"user_id": ctx.deps["user_id"]}},
)
```

Exceptions raised inside `per_call` are caught and surface as `FailedToolCall` (transport-layer fault — the call never went over the wire). The LLM does not see them; operators do.

---

## 10. The `McpServer` object — single polymorphic role

`McpServer` deliberately serves two functions:

- **In `Worker(nodes=[mcp_server, ...])`**, it expands at registration time into N subscribers, one per discovered tool, plus a lifespan handle for the underlying session.
- **In `Agent(tools=[mcp_server, ...])`**, it expands at registry-construction time into N `ToolNodeDef`-shaped objects bearing only schemas + topic refs.

Polymorphism is a deliberate DX choice. The alternative — explicit `gmail.as_node_group()` + `gmail.as_tool_refs()` — is more honest but adds boilerplate to every call site, undermining the "three lines or fewer" goal. The polymorphism is local (only `McpServer`) and clearly named (`mcp.McpServer`), and the two roles cannot accidentally collide because `Worker` and `Agent` consume different attributes.

The construction-time / discovery-time / runtime separation prevents subprocess-on-import: importing a module that defines an `McpServer` triggers no I/O. Subprocesses spawn only inside `Worker.run()`.

---

## 11. Testing

The existing test infrastructure (`TestKafkaBroker` for synchronous Kafka simulation, per the project memory) extends to MCP via a `FakeMcpServer` shipped under `calfkit/mcp/_testing.py`:

- Implements the same `McpServer` public surface.
- Accepts a static dict of tool definitions and a callable `(name, args) -> result` for invocation.
- Skips transport entirely; the `Worker` startup phase calls into the fake's in-memory registry directly.

This pattern lets tests verify the full envelope flow (agent → MCP tool topic → handler → reply) without spawning real subprocesses or HTTP servers. Live-integration tests against real MCP servers (e.g. `@modelcontextprotocol/server-everything`) run in a separate CI lane.

---

## 12. Configuration matrix

| Concern | Default | Override |
|---|---|---|
| Transport | inferred from constructor (`stdio` / `http`) | explicit constructor call |
| Subprocess shutdown grace period | 5s | `McpServer.stdio(..., shutdown_grace=10)` |
| HTTP client timeout | 30s read | `McpServer.http(..., timeout=...)` |
| Reconnect on session loss | best-effort, 3 attempts, exp. backoff | `McpServer(..., reconnect=False)` |
| Tool list caching across restarts | off | `mcp.cache_to("./snapshot.json")` |
| Per-call hook | none | `per_call=lambda ctx: {...}` |
| Filtering | none (all tools exposed) | `.only / .exclude / .where` |
| Renaming | none | `.prefix(...)` / `.rename(...)` |
| Concurrency cap per server | unbounded (driven by FastStream `max_workers`) | (no override in v1 — add if needed) |

---

## 13. Out of scope (v1)

| Item | Why deferred |
|---|---|
| Resources (`resources/*`) | Requires new node abstractions; LLM-context pipeline rewire. |
| Prompts (`prompts/*`) | Same. |
| Sampling callback (server→client `sampling/createMessage`) | Needs reverse-topic protocol; potentially valuable enough to warrant its own RFC. |
| Elicitation (server→client `elicitation/create`) | Not meaningful in automated agent runtime. |
| `notifications/tools/list_changed` hot reload | Long-lived worker requirement; v2. |
| `outputSchema` validation | Pass-through is sufficient for v1; v2 may add Pydantic-level. |
| Codegen (`calfkit mcp codegen gmail > gmail.pyi`) | Separate roadmap entry; orthogonal to runtime DX. |
| Roots advertisement | Tied to filesystem MCP servers' security model; needs design discussion. |
| Multi-modal LLM passthrough of `image` / `audio` content | Needs changes to `ToolReturn` and provider passthroughs. |

---

## 14. Open questions

The following decisions in this draft reflect the author's recommendation but are flagged for explicit review before implementation begins. Each cites the section that would change if the decision flips.

1. **§10 — Polymorphism of `McpServer`.** Confirm we accept the dual `nodes=[...]` / `tools=[...]` role. The alternative (explicit `.as_node_group()` / `.as_tool_refs()`) is more honest but more verbose.
2. **§6.3 — Agent-worker schema discovery.** The proposal opens a short-lived MCP connection from the agent worker at startup. The alternative is a Kafka-mediated discovery RPC against the tool worker. The proposed path is simpler; the alternative is more decoupled.
3. **§5.5 / §9 — Shape of `per_call`.** Single callable returning `{headers, meta}` vs separate `headers_resolver` / `meta_resolver` vs pydantic-ai-style `process_tool_call` that can also rewrite args.
4. **§8.1 — Content-block mapping.** v1 surfaces `structuredContent` or concatenated text, summarising non-text blocks. Confirm this is acceptable as a starting point (vs holding v1 until full multi-modal passthrough lands).
5. **§4.1 — Streamable HTTP in v1.** Recommended in. Confirm or restrict v1 to stdio only.

---

## 15. References

- [MCP specification, current draft (2025-11-25)](https://modelcontextprotocol.io/specification/2025-11-25/)
- [MCP Python SDK — `ClientSession`, `stdio_client`, `streamablehttp_client`](https://github.com/modelcontextprotocol/python-sdk)
- [pydantic-ai `MCPServerStdio`](https://ai.pydantic.dev/mcp/client/) — closest in-process analogue; informed the per-call and lifecycle ergonomics.
- [OpenAI Agents SDK MCP support](https://openai.github.io/openai-agents-python/mcp/) — informed the filter / annotation handling.
- [Google ADK `MCPToolset`](https://github.com/google/adk-python) — informed the `AsyncExitStack` lifecycle pattern.
- [langchain-mcp-adapters `MultiServerMCPClient`](https://github.com/langchain-ai/langchain-mcp-adapters) — informed the multi-server config interop. Note its explicit caveat about stdio in server contexts, addressed by recommending Streamable HTTP for production multi-tenant deployments.
- Calfkit issue [#158](https://github.com/calf-ai/calfkit-sdk/issues/158)
