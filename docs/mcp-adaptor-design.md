# Calfkit MCP Adaptor — Design Document

**Status:** Draft proposal
**Document version:** 1.3
**Last updated:** 2026-05-30
**Tracking issue:** [#158](https://github.com/calf-ai/calfkit-sdk/issues/158)

---

## 1. Summary

This document proposes a first-class adaptor that turns any [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server into a set of native calfkit tools — usable by any agent, deployable as part of any worker, and consistent with calfkit's existing distributed tool model.

The design centres on a single user-facing object, `McpServer`, that behaves as **both** a node-group (when passed to `Worker(nodes=[...])`) and a tool-source (when passed to `Agent(tools=[...])`). The same import works in single-process dev setups and split agent/tool deployments without modification. Drop-in compatibility with the de facto `mcp.json` configuration format means users can copy working configs from Claude Desktop, Cursor, Cline, and Gemini CLI.

**v1 uses codegen for schema declarations.** Users run `calfkit mcp codegen` once per MCP server to generate a Python module containing the tool schemas, then import that module and pass the tools to `mcp(..., tools=[...])`. This aligns with the dominant event-driven schema management pattern (protobuf/Avro/Confluent Schema Registry — schemas as versioned artifacts, not runtime introspection). Runtime dynamic discovery is on the roadmap as a complementary path (see [`docs/mcp-discovery-rpc-design.md`](./mcp-discovery-rpc-design.md)).

The design preserves every existing calfkit invariant:

- Each MCP tool maps to one logical `ToolNodeDef` with its own deterministic topic.
- Tool schemas are visible to the LLM at per-tool granularity (no `mcp_call` mega-tool).
- Tool errors flow through `RetryPromptPart` (LLM-retryable) and `FailedToolCall` (operator-visible) — no new exception types.
- Concurrency leverages MCP's native JSON-RPC request-ID pipelining; no locks or serial dispatch.

Lifecycle, content adaptation, and credential propagation are designed to be invisible to the typical user but configurable for advanced cases.

---

## 2. Motivation

The MCP ecosystem has become the de facto standard for exposing tools to AI agents. Production-quality MCP servers exist for Gmail, Google Workspace, GitHub, Slack, Linear, Notion, Postgres, filesystems, browsers, and hundreds of others — many maintained by the original vendors. Today, using any of these from a calfkit agent requires hand-writing a `@agent_tool` wrapper, duplicating effort across the community and gating calfkit from the broader MCP catalogue.

A first-class adaptor turns the entire MCP catalogue into calfkit's tool catalogue by extension, without compromising the event-driven, microservice-shaped runtime that distinguishes calfkit from in-process agent SDKs.

---

## 3. Goals and non-goals

### 3.1 Goals

- **DX parity with in-process SDKs (for the runtime path).** Adding an MCP server to a running agent is a single line of code; the schema acquisition step is a one-time CLI invocation. The user should never construct a `ToolNodeDef` by hand for MCP.
- **Zero new mental models.** MCP tools are tools. They appear in `Agent(tools=[...])` and `Worker(nodes=[...])` the same way native `@agent_tool`s do.
- **One source of truth across processes.** A single shared module defines the MCP server; agent workers and tool workers both import it.
- **Standard config interop.** Accept `mcp.json`-shaped configs verbatim.
- **Honest error semantics.** MCP-level tool errors (`isError: true`) become LLM-retryable retry prompts; transport-level errors become operator-visible failures.
- **Concurrency by default.** Multiple agents calling MCP tools in parallel pipeline through the underlying session via JSON-RPC request IDs — no per-call serialisation.
- **Multi-server-per-deployment.** Gmail + GitHub + Postgres can live in the same worker, addressed by the same agent, with no special multi-server mode.
- **Multi-tenant identity propagation.** Per-invocation user identity flows through MCP's `_meta` field via the existing `Deps` plumbing. Credentials remain session-scoped per the MCP spec; the MCP server maps identity to credentials. This is the protocol-canonical pattern (§10).

### 3.2 Non-goals (v1)

- **Runtime tool discovery (no codegen).** Schemas come from a codegen-generated module that users import. Skipping codegen and discovering schemas from a running MCP server at agent boot is the v1.x roadmap entry ([`docs/mcp-discovery-rpc-design.md`](./mcp-discovery-rpc-design.md)). v1 ships only the codegen path.
- **Resources and prompts.** MCP servers also expose `resources/*` and `prompts/*` primitives. v1 covers tools only. Resources and prompts require new node abstractions and a wider rewire of LLM-context construction.
- **Server-initiated sampling.** Routing an MCP server's `sampling/createMessage` request back to a calfkit agent is feasible but requires reverse-topic protocol design. v1 declines the capability at `initialize`; servers that strictly require it will fail fast.
- **Elicitation.** Server-initiated user prompts are not meaningful in a server-side, automated multi-agent system.
- **Hot reload via `notifications/tools/list_changed`.** v1 logs and ignores; v2 may rebuild the registry.
- **`outputSchema` validation.** Pass-through of `structuredContent` as-is; v2 may add Pydantic-level validation.
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

## 5. Comparison to peer SDK adaptors

The MCP-adaptor design space splits into three tiers. The proposed design is positioned with reference to each.

### 5.1 Tier 1 — in-process agent SDKs

| Concern | pydantic-ai `MCPServerStdio` | OpenAI Agents `MCPServer*` | Google ADK `MCPToolset` | **Calfkit (proposed)** |
|---|---|---|---|---|
| Toolset object | `Agent(toolsets=[server])` | `mcp_servers=[server]` | `from_server()` or `async with` | `Agent(tools=[server])` |
| Lifecycle | `async with agent:` | same | `AsyncExitStack` | `Worker.run()` lifespan hook |
| Where session lives | agent process | agent process | agent process | **tool worker process** (split from agent) |
| Filtering | `tool_prefix` | `tool_filter` static + dynamic | none documented | `.only / .exclude / .where / .prefix / .rename` |
| Per-call hook | `process_tool_call` (can rewrite args + meta) | `tool_meta_resolver` (meta only) | none | `per_call → {headers, meta}` |
| Error split | tool errors → `ModelRetry`; everything else raises | similar | similar | **explicit:** `isError` → `RetryPromptPart`; transport → `FailedToolCall` |
| Config interop | inline ctor only | inline ctor only | inline ctor only | **`mcp.json` file or dict accepted verbatim** |

The proposed API is feature-equivalent to the most ergonomic in-process peer (pydantic-ai) and strictly richer on filtering and configuration interop. The honest two-layer error split is more explicit than any in-process peer documents.

### 5.2 Tier 2 — multi-server agent SDKs

| | langchain-mcp-adapters `MultiServerMCPClient` | lastmile `mcp-agent` | **Calfkit (proposed)** |
|---|---|---|---|
| Server definition | dict literal | YAML file + `MCPServerSettings` | dict, `mcp.json` file, or inline ctor |
| Agent attachment | `tools = await client.get_tools()` | `Agent(server_names=["fetch"])` (string refs) | `Agent(tools=[server])` (object refs) |
| Filtering per server | manual list comprehension | not documented | first-class chainable methods |
| Per-call OAuth | not documented | app-level OAuth only | `per_call=lambda ctx: ...` |
| Durable execution | none | optional Temporal backend | none (see §14, gaps 1–3) |

lastmile's `mcp-agent` is the closest spiritual analogue — it has the optional-Temporal angle that nods toward calfkit's distributed nature. But its API ties servers to agents by string name (`server_names=["fetch"]`), which is a weaker DX than object references and pushes configuration into YAML. The proposed object-based API gives IDE navigability and avoids the typo-at-runtime failure mode of string lookups.

### 5.3 Tier 3 — distributed / durable runtimes

#### 5.3.1 Temporal

Temporal does **not** ship an "MCP-as-Temporal-tools" adapter. The published pattern is the inverse: an MCP server stands in front of Temporal Workflows so AI clients can invoke durable workflows as MCP tools. Concretely:

- **Mapping:** one MCP tool ⇒ one Temporal Workflow ⇒ N Activities.
- **Subprocess location:** MCP server is a thin stdio bridge; Temporal Workers are separate processes polling the task queue.
- **DX:** developer writes `@activity.defn`, `@workflow.defn`, and `@mcp.tool()` separately and stitches them in the MCP handler.

Temporal's split-process structure (MCP bridge process + worker pool process) is similar in shape to calfkit's split (MCP-hosting worker + agent worker). But Temporal solves the converse problem — *Temporal-backs-MCP* — whereas calfkit needs *MCP-fronts-calfkit*.

References: [Learn Temporal — Durable MCP Tools](https://learn.temporal.io/tutorials/ai/building-mcp-tools-with-temporal/introducing-mcp-temporal/), [Bitovi — Building Durable MCP Tools with Temporal](https://www.bitovi.com/blog/building-durable-mcp-tools-with-temporal).

#### 5.3.2 Restate

Restate's [`ai-examples/mcp/`](https://github.com/restatedev/ai-examples) directory is the closest precedent for this design. The pattern is:

- Each MCP tool is wrapped as an **individual Restate handler** (`tool({description, input}, async (ctx, args) => ...)`).
- A thin MCP server process **fronts** the Restate runtime and dispatches to per-tool handlers.
- Resilience (retries, suspend/resume, durable state) comes from the Restate runtime, not from the MCP layer.

The proposed `mcp.<server>.<tool>.input` topic-per-tool routing (§7.1) is **structurally identical** to Restate's "one MCP tool = one handler" pattern. The differences are: Restate's runtime owns retries and durability; calfkit's runtime owns Kafka envelope routing. Both treat MCP as a fan-out boundary, not a session-encapsulating monolith. This convergence with an unrelated distributed runtime is corroborative evidence for the per-tool decomposition.

Restate also publishes [`restate-mcp-server`](https://github.com/restatedev) for exposing Restate's admin API as MCP — the inverse direction, like Temporal's adapter. Reference: [`restatedev/ai-examples`](https://github.com/restatedev/ai-examples).

### 5.4 What is novel in the proposed design

- **`McpServer` as polymorphic node-group and tool-source.** No peer SDK exposes one object that works in both `Worker(nodes=[...])` and `Agent(tools=[...])`. This polymorphism is a calfkit-specific affordance for its split-process model and removes the need for users to construct ToolNodeDef-shaped wrappers by hand.
- **Deterministic topic naming for cross-process schema agreement.** Restate solves the equivalent problem via runtime service registration. Calfkit's static topic naming (`mcp.<server>.<tool>.input`) lets the agent worker and tool worker derive identical wire addresses without any coordination channel.
- **`mcp.json` drop-in compatibility.** Only langchain-mcp-adapters comes close, with a dict-based config; none accept the standard `mcp.json` file format verbatim. Reusing the format users already maintain for Claude Desktop / Cursor / Cline removes a duplicate source of truth.

---

## 6. Proposed API

### 6.1 Construction and schema acquisition

v1 has two steps: (1) generate schemas once via the CLI, (2) declare the server in code with those schemas.

**Step 1 — Generate schemas:**

```bash
calfkit mcp codegen gmail \
    --command "npx -y @modelcontextprotocol/server-gmail" \
    --output gmail_schemas.py
```

The CLI spawns the MCP server, runs `initialize` + `tools/list`, and writes a Python module:

```python
# gmail_schemas.py — generated by `calfkit mcp codegen`; do not edit by hand.
from calfkit.mcp import McpToolDef

SEARCH = McpToolDef(
    name="search",
    description="Search emails matching a query",
    input_schema={...},
    annotations={"read_only_hint": True, ...},
)
SEND = McpToolDef(name="send", ...)

class Gmail:
    SEARCH = SEARCH
    SEND = SEND
    ALL = [SEARCH, SEND]
```

The generated file is committed to the repo. A scheduled CI job can run `calfkit mcp codegen --check` to detect drift and open a PR if the upstream MCP server has changed.

**Step 2 — Declare the server:**

```python
from calfkit.mcp import McpServer
from gmail_schemas import Gmail

# stdio transport (local subprocess)
gmail = McpServer.stdio(
    "npx", "-y", "@modelcontextprotocol/server-gmail",
    tools=Gmail.ALL,
)

# Streamable HTTP transport (remote server)
github = McpServer.http(
    "https://api.github.com/mcp",
    auth="bearer $GITHUB_TOKEN",
    tools=Github.ALL,
)
```

`McpServer` construction is **cheap, synchronous, and I/O-free**. No subprocess is spawned and no network call is made at import time. The bridge worker spawns the MCP session at `Worker.run()` startup (see §7); the agent worker never connects to the MCP server.

Users who prefer inline declarations over codegen can pass `McpToolDef` instances directly:

```python
gmail = McpServer.stdio("npx", "-y", "@modelcontextprotocol/server-gmail", tools=[
    McpToolDef(name="search", description="...", input_schema={...}),
    McpToolDef(name="send", description="...", input_schema={...}),
])
```

Both paths produce the same result.

### 6.2 Usage in an agent

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

`McpServer` behaves as a `ToolNodeDef`-yielding iterable when passed to `Agent(tools=...)`. The agent's existing `tools_registry` construction in `calfkit/nodes/agent.py:163-167` is preserved unchanged; the registry is built from the discovered set after the startup pass described in §7.3.

### 6.3 Usage in a worker

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

In both cases, the underlying long-lived MCP session lives in **only** the worker that hosts `gmail` / `github` as a node. The agent worker imports the codegen-generated schemas (§6.1) and never connects to the MCP server at any point.

### 6.4 Filtering and renaming

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

### 6.5 Per-call context (multi-tenant identity)

```python
gmail = McpServer.http(
    "https://gmail-mcp.acme.com/mcp",
    token="$CALFKIT_SERVICE_TOKEN",                     # session-static auth to MCP server
    meta=lambda ctx: {"user_id": ctx.deps["user_id"]},  # per-call user identity
)
```

`token=` and `headers=` are **session-static** — resolved at construction time (with `$VAR` env substitution), baked into the underlying httpx client, and held for the lifetime of the MCP session. They are not per-call. `token` is sugar for `headers={"Authorization": f"Bearer {value}"}`.

`meta=` accepts either a constant `dict` or a `Callable[[ToolContext], dict | Awaitable[dict]]`. The callable runs once per envelope before `session.call_tool` is invoked. Its return value is placed in MCP's `_meta` field on the tool call, where it travels with the message body — race-free with no shared-state mutation. The `ToolContext` is the same one native `@agent_tool` functions see (`calfkit/models/tool_context.py`).

This shape is intentional: it expresses **Pattern 1** from §10 (identity in `_meta`, credential mapping server-side), which is the protocol-canonical pattern for multi-tenant MCP deployments. Per-call HTTP header rotation is not supported in v1 — see §10 for the reasoning and §14 for the explicit non-goal.

Exceptions raised inside `meta=` surface as `FailedToolCall` (transport-layer fault — the call never went over the wire). The LLM does not see them; operators do.

### 6.6 `mcp.json` interop

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

## 7. Wire model and internal architecture

### 7.1 Topic naming

Each MCP tool receives a deterministic topic pair derived from `(server_name, tool_name)`:

- Input: `mcp.{server_name}.{tool_name}.input`
- Output: `mcp.{server_name}.{tool_name}.output`

The `mcp.*` prefix distinguishes MCP-backed tools from native `tool.*` tools at the wire/observability layer. The names are deterministic so the agent worker and tool worker derive identical topics without any coordination channel.

Renaming (§6.4) affects only the **agent-facing name** the LLM sees; the underlying topic uses the server-exported name to keep wire identity stable across renames.

### 7.2 Worker lifecycle (subprocess management)

A `Worker` running with one or more `McpServer` nodes performs the following on `run()`:

1. **Pre-subscribe startup phase** (before any Kafka subscriber registers):
   - For each `McpServer` in `nodes`, open the underlying transport (spawn subprocess for stdio, open HTTP session for streamable HTTP).
   - Send `initialize` with calfkit's `clientInfo` and capabilities (notably, **`sampling` and `elicitation` are not declared**).
   - Send `notifications/initialized`.
   - Call `tools/list` as a **sanity check**: validate that every tool declared in `tools=` actually exists on the MCP server. Log a warning identifying drift (declared tools not on server, or server tools not declared). Calls to undeclared tools are not registered as subscribers.
   - If the server declares `sampling` or `elicitation` as required, fail fast with a clear error referencing the v1 non-goal.
2. **Subscriber registration phase** (existing `register_handlers()` flow). Each MCP-backed tool gets one subscriber on its `mcp.<server>.<tool>.input` topic, sharing the worker's `group_id`.
3. **Steady state.** Subscribers dispatch envelopes; each handler awaits `session.call_tool(name, args, _meta=...)` and replies via the existing `ReturnCall` path.
4. **Shutdown (SIGTERM, exception, or `asyncio.CancelledError`).** All MCP sessions close cleanly via the Python SDK's async context managers; subprocesses are sent SIGTERM, then SIGKILL after a grace period (default 5s, configurable per `McpServer`).

The startup phase is implemented as a FastStream lifespan hook on the existing `FastStream(...).run()` call in `calfkit/worker/worker.py:67-71`.

### 7.3 Schema sourcing for agent-only workers

A worker that hosts only `agent` (and not the MCP server) gets schemas from the **codegen-generated module** the user imports (§6.1). The agent worker never connects to the MCP server at any point — no subprocess spawning on the agent host, no credentials required, no network call to the MCP server.

Concretely: `gmail = McpServer.stdio(..., tools=Gmail.ALL)` carries the schemas inside the `McpServer` instance. When iterated by the agent (`Agent(tools=[gmail])`), it yields `BaseToolNodeSchema` instances built from the declared tools. Filters and renames apply as normal.

For users who want runtime discovery (no codegen step), the future MCP-over-Kafka RPC path will offer it as an opt-in alternative — see [`docs/mcp-discovery-rpc-design.md`](./mcp-discovery-rpc-design.md). v1 ships codegen only.

### 7.4 Single-process worker

When `gmail` and `agent` are in the same `Worker(nodes=[gmail, agent])`:

- The startup phase opens the long-lived session **once** (the node-group path, not the discovery-only path).
- The agent's tool registry consumes the same in-memory registry the subscribers use.
- No second subprocess; no duplicate `initialize`.

### 7.5 Concurrency

A single `ClientSession` handles N in-flight `call_tool` requests via JSON-RPC request IDs. The worker's existing per-subscriber concurrency (FastStream `max_workers`) drives parallelism — each consumer task awaits its own `call_tool` and the session multiplexes underneath. **No locks, no pools, no per-tool queues.**

The only synchronisation point is `initialize` itself, which is awaited once at startup before any subscriber accepts traffic.

---

## 8. Error mapping

MCP exposes two distinct error layers; calfkit maps them to its two existing tool-error paths.

### 8.1 Tool-semantic errors (`CallToolResult.isError: true`)

These are errors the LLM is meant to see and react to: "permission denied", "no results", "invalid argument value", etc. They map to `RetryPromptPart`, the existing path in `calfkit/nodes/agent.py:316-321` used for arg validation and unknown tool names.

The MCP `content` field is concatenated into the retry prompt's `content` (text blocks only; non-text blocks are summarised as `[image omitted]`, etc.). The LLM sees the error message and can retry with adjusted arguments.

### 8.2 Transport / protocol errors

Connection lost, JSON-RPC parse error, unexpected method response, subprocess died, HTTP 5xx, OAuth token revoked — these are operational failures. They flow through the existing `FailedToolCall` marker (`calfkit/models/state.py`) and raise as `ToolExecutionError` at the agent (`calfkit/exceptions.py`), consistent with native tool exceptions.

This split mirrors the existing native-tool handling in `calfkit/nodes/tool.py:122-184`: `ModelRetry` exceptions become `RetryPromptPart`, all other exceptions become `FailedToolCall`.

### 8.3 Initialization-time errors

`initialize` failure (server died on launch, version mismatch, required capability calfkit doesn't support) fails `Worker.run()` before any subscriber is registered. This is preferred over partial startup — operators see the error immediately on deploy rather than discovering it on first traffic.

---

## 9. Content and return-value mapping

### 9.1 Output preference

The agent's view of an MCP tool's return is constructed as follows:

1. If `structuredContent` is present, **return that** (passed through as a dict). The text content array is dropped; `structuredContent` is the typed primary surface.
2. Otherwise, **concatenate text blocks** into a single string and return.
3. Non-text content blocks (`image`, `audio`, `resource_link`, `embedded_resource`) are **summarised as placeholders** in v1: `[image: 512x512 image/png]`, `[resource: example://path.pdf]`. The full content is not surfaced to the LLM.

The placeholder behaviour is honest about a v1 limitation and avoids the failure mode of silently dropping content the user expected.

### 9.2 Wire serialisation

The return value rides the existing `ToolReturn(return_value=..., metadata={"tool_call_id": ...})` path in `calfkit/nodes/tool.py:120-121`. `structuredContent` is JSON-serialisable by construction (it conforms to its `outputSchema`). The eager `pydantic_core.to_json` check on `tool.py:121` continues to guard against any wire-unsafe values.

### 9.3 Rich content in v2

A future revision can extend `ToolReturn` to a multi-block result that maps directly to calfkit's `ContentPart` hierarchy (`TextPart`, `DataPart`, `FilePart`) — at which point images and embedded resources can ride through to providers that support multi-modal inputs. v1 surfacing this would require changes to `pydantic-ai`'s return path; deferred.

---

## 10. Multi-tenant context

The MCP protocol separates two concerns:

- **Identity** — which user / tenant is making the call. Travels in the message body via the `_meta` field on `tools/call`. Per-call by design.
- **Credentials** — the OAuth tokens / API keys used to authenticate to upstream services. Session-scoped by spec. The [MCP authorization spec](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization) states that the Authorization header "must be included in every HTTP request, even within the same logical session" — implying one identity per session.

Three canonical patterns satisfy these constraints. v1 implements Pattern 1; Patterns 2 and 3 are deployment or future-version choices.

### 10.1 Pattern 1 — Identity in `_meta`, credentials mapped server-side (v1)

The calfkit bridge passes user identity in `_meta`. The MCP server holds its own credential store (or fronts one — Stytch, Composio, Pipedream, vendor OAuth, etc.) and maps the identity to upstream tokens. Calfkit never sees the user's external-service credentials.

```python
gmail = McpServer.http(
    "https://gmail-mcp.acme.com/mcp",
    token="$CALFKIT_SERVICE_TOKEN",                     # auths calfkit ↔ MCP server
    meta=lambda ctx: {"user_id": ctx.deps["user_id"]},  # per-call user identity
)
```

This pattern matches how every multi-tenant MCP server in the production ecosystem is built. It is also how `pydantic-ai`'s `process_tool_call`, OpenAI Agents' `tool_meta_resolver`, and every other major MCP-aware agent SDK handles per-call context — they all converge on `_meta` for the same protocol-level reason.

The trust boundary is clean: calfkit holds one service-level credential per MCP server; the MCP server holds per-user OAuth tokens. Compromise of a calfkit worker exposes user identities but not their upstream tokens.

### 10.2 Pattern 2 — One session per tenant (v2 — session pool)

When the MCP server cannot map identity to credentials server-side (for instance a generic MCP server that requires the calling client to pass its own OAuth token), the protocol-canonical answer is one session per tenant. Each session is opened with that tenant's credentials at `initialize` time and reused for all of that tenant's calls.

v1 does not implement this. v2 will add a session-pool option to `McpServer`:

```python
# v2 sketch — not in v1
gmail = McpServer.http(
    "https://gmail-mcp.acme.com/mcp",
    session_key=lambda ctx: ctx.deps["tenant"],
    session_token=lambda tenant: vault.get_oauth_token(tenant),
)
```

The bridge maintains a pool of `McpSession`s keyed by `session_key(ctx)`. Each session is opened lazily with credentials from `session_token(key)`. Idle sessions are evictable by LRU policy. There is no async-coherence race because credentials are bound to the session at construction, not mutated per call. Roadmap entry; explicitly out of v1 scope — see §14.

### 10.3 Pattern 3 — One bridge process per credential set (deployment-time)

The simplest operational answer: deploy one calfkit bridge per credential set. Each process has static credentials. Tenancy is expressed by which Kafka partition (and therefore which bridge process) handles which envelope. No SDK changes; works with v1 today.

This is appropriate when (a) the number of credential sets is small and fixed, (b) operational separation is desired anyway (per-tenant compliance boundaries, per-tenant observability), or (c) the deployment uses a scheduler that can spin bridges up and down per tenant.

### 10.4 What is intentionally not supported

**Per-call HTTP header rotation against a shared MCP session is not supported in v1 and is not on the v2 roadmap.** Two reasons:

1. The MCP spec binds credentials to sessions, not calls. Per-call HTTP authorization is not a protocol-supported concept; supporting it would mean fighting the protocol's design.
2. The implementation has a genuine async-coherence trap: a shared `httpx.AsyncClient.headers` mutated in the calling task is read later by MCP's transport task, after the calling task has yielded the event loop — creating a window where another task's mutation can overwrite the first. No peer SDK solves this; the protocol-clean answers are Patterns 1, 2, or 3 above.

Users with this need should fall back to Pattern 3 (one bridge per credential set) or wait for Pattern 2 (session pool, v2).

---

## 11. The `McpServer` object — single polymorphic role

`McpServer` deliberately serves two functions:

- **In `Worker(nodes=[mcp_server, ...])`**, it expands at registration time into N subscribers, one per discovered tool, plus a lifespan handle for the underlying session.
- **In `Agent(tools=[mcp_server, ...])`**, it expands at registry-construction time into N `ToolNodeDef`-shaped objects bearing only schemas + topic refs.

Polymorphism is a deliberate DX choice. The alternative — explicit `gmail.as_node_group()` + `gmail.as_tool_refs()` — is more honest but adds boilerplate to every call site, undermining the "three lines or fewer" goal. The polymorphism is local (only `McpServer`) and clearly named (`mcp.McpServer`), and the two roles cannot accidentally collide because `Worker` and `Agent` consume different attributes.

The construction-time / discovery-time / runtime separation prevents subprocess-on-import: importing a module that defines an `McpServer` triggers no I/O. Subprocesses spawn only inside `Worker.run()`.

---

## 12. Testing

The existing test infrastructure (`TestKafkaBroker` for synchronous Kafka simulation, per the project memory) extends to MCP via a `FakeMcpServer` shipped under `calfkit/mcp/_testing.py`:

- Implements the same `McpServer` public surface.
- Accepts a static dict of tool definitions and a callable `(name, args) -> result` for invocation.
- Skips transport entirely; the `Worker` startup phase calls into the fake's in-memory registry directly.

This pattern lets tests verify the full envelope flow (agent → MCP tool topic → handler → reply) without spawning real subprocesses or HTTP servers. Live-integration tests against real MCP servers (e.g. `@modelcontextprotocol/server-everything`) run in a separate CI lane.

---

## 13. Configuration matrix

| Concern | Default | Override |
|---|---|---|
| Transport | inferred from constructor (`stdio` / `http`) | explicit constructor call |
| Subprocess shutdown grace period | 5s | `McpServer.stdio(..., shutdown_grace=10)` |
| HTTP client timeout | 30s read | `McpServer.http(..., timeout=...)` |
| Reconnect on session loss | not supported — bridge worker fails loudly, orchestrator restarts | (no override; intentional design choice — cattle-not-pets) |
| Tool list source | codegen-generated module (recommended) | inline `McpToolDef` declarations |
| Session-static token (HTTP) | none (anonymous) | `token="$VAR"` — sugar for `Authorization: Bearer …` header |
| Session-static headers (HTTP) | none | `headers={"X-…": "…"}` — `$VAR` substitution applied |
| Per-call MCP `_meta` | none | `meta=lambda ctx: {...}` — placed in tool call's `_meta` field (both transports) |
| Filtering | none (all tools exposed) | `.only / .exclude / .where` |
| Renaming | none | `.prefix(...)` / `.rename(...)` |
| Concurrency cap per server | unbounded (driven by FastStream `max_workers`) | (no override in v1 — add if needed) |

---

## 14. Out of scope (v1)

### 14.1 Protocol features deferred

| Item | Why deferred |
|---|---|
| Resources (`resources/*`) | Requires new node abstractions; LLM-context pipeline rewire. |
| Prompts (`prompts/*`) | Same. |
| Sampling callback (server→client `sampling/createMessage`) | Needs reverse-topic protocol; potentially valuable enough to warrant its own RFC. |
| Elicitation (server→client `elicitation/create`) | Not meaningful in automated agent runtime. |
| `notifications/tools/list_changed` hot reload | Long-lived worker requirement; v2. |
| `outputSchema` validation | Pass-through is sufficient for v1; v2 may add Pydantic-level. |
| Codegen (`calfkit mcp codegen gmail > gmail.pyi`) | Separate roadmap entry; orthogonal to runtime DX. |
| Per-call HTTP header rotation | Not a protocol-supported concept (credentials are session-scoped per MCP spec). Pattern 2 (session pool, §10.2) covers the rare case where credentials truly must vary per tenant client-side; Pattern 3 (§10.3) is the deployment-time workaround that works today. |
| Roots advertisement | Tied to filesystem MCP servers' security model; needs design discussion. |
| Multi-modal LLM passthrough of `image` / `audio` content | Needs changes to `ToolReturn` and provider passthroughs. |

### 14.2 Durability gaps relative to Temporal / Restate

The proposed design inherits calfkit's at-least-once Kafka delivery semantics and does not add a workflow-level durability layer over MCP calls. Three concrete consequences, called out explicitly so reviewers do not assume they were missed:

1. **In-flight calls are not durable across worker crashes.** A worker that dies mid-`tools/call` loses the call. Kafka redelivery will re-drive the inbound envelope, but the MCP server may have already executed the call once — duplicate execution risk for non-idempotent tools. Temporal Workflows and Restate handlers both survive crashes; this design does not.
2. **No idempotency-key dedup.** MCP's `idempotentHint` annotation could feed a worker-side dedup cache keyed by `(tool_call_id, args_hash)`, but v1 does not implement this. A redelivered envelope can double-execute a destructive tool.
3. **Long-running tool calls pin a worker handler.** The MCP spec's experimental `tasks` extension (`call_tool_as_task`, `poll_task`) allows tools to run for minutes/hours with polling for status. v1 only supports synchronous `call_tool`; a long call ties up a FastStream consumer task for its full duration. Restate's suspend/resume handles this natively; v2 may add tasks-extension support.

These gaps are real and should be weighed against the v1 implementation cost. They are deliberately deferred because (a) most MCP servers in the current ecosystem ship short, idempotent-by-construction tools where the gaps don't bite, and (b) closing them requires either a calfkit-wide durability layer (which would more generally benefit native tools) or coupling to an external workflow runtime (Temporal/Restate) — both of which are larger discussions than this adaptor warrants.

---

## 15. Open questions

The following decisions in this draft reflect the author's recommendation but are flagged for explicit review before implementation begins. Each cites the section that would change if the decision flips.

1. **§11 — Polymorphism of `McpServer`.** Confirm we accept the dual `nodes=[...]` / `tools=[...]` role. The alternative (explicit `.as_node_group()` / `.as_tool_refs()`) is more honest but more verbose.
2. **§7.3 — Agent-worker schema discovery.** The proposal opens a short-lived MCP connection from the agent worker at startup. The alternative is a Kafka-mediated discovery RPC against the tool worker. The proposed path is simpler; the alternative is more decoupled.
3. **§9.1 — Content-block mapping.** v1 surfaces `structuredContent` or concatenated text, summarising non-text blocks. Confirm this is acceptable as a starting point (vs holding v1 until full multi-modal passthrough lands).
4. **§4.1 — Streamable HTTP in v1.** Recommended in. Confirm or restrict v1 to stdio only.
5. **§14.2 — Durability gaps.** Confirm that the three durability gaps are acceptable for v1, or that one of them (most likely #2, idempotency-key dedup) should be promoted into v1 scope.

---

## 16. References

### 16.1 MCP protocol

- [MCP specification, current draft (2025-11-25)](https://modelcontextprotocol.io/specification/2025-11-25/)
- [MCP Python SDK — `ClientSession`, `stdio_client`, `streamablehttp_client`](https://github.com/modelcontextprotocol/python-sdk)

### 16.2 In-process agent SDK adaptors

- [pydantic-ai `MCPServerStdio`](https://ai.pydantic.dev/mcp/client/) — closest in-process analogue; informed the per-call and lifecycle ergonomics.
- [OpenAI Agents SDK MCP support](https://openai.github.io/openai-agents-python/mcp/) — informed the filter / annotation handling.
- [Google ADK `MCPToolset`](https://github.com/google/adk-python) — informed the `AsyncExitStack` lifecycle pattern.

### 16.3 Multi-server adaptors

- [langchain-mcp-adapters `MultiServerMCPClient`](https://github.com/langchain-ai/langchain-mcp-adapters) — informed the multi-server config interop. Note its explicit caveat about stdio in server contexts, addressed by recommending Streamable HTTP for production multi-tenant deployments.
- [lastmile-ai/mcp-agent](https://github.com/lastmile-ai/mcp-agent) — closest spiritual analogue with an optional Temporal backend; informed the recognition that server-as-object beats server-as-string-name.

### 16.4 Distributed / durable runtime patterns

- [Learn Temporal — Building Durable MCP Tools (Part 1)](https://learn.temporal.io/tutorials/ai/building-mcp-tools-with-temporal/introducing-mcp-temporal/) — Temporal-backs-MCP pattern; inverse direction from this design but corroborates the split-process structure.
- [Bitovi — Building Durable MCP Tools with Temporal](https://www.bitovi.com/blog/building-durable-mcp-tools-with-temporal) — concrete workflow-per-tool mapping reference.
- [restatedev/ai-examples (`mcp/`)](https://github.com/restatedev/ai-examples) — closest precedent for the proposed design: each MCP tool wrapped as an individual handler fronted by a thin MCP server. Structurally converges with this design's topic-per-tool routing despite Restate and calfkit having unrelated runtime models.

### 16.5 Project context

- Calfkit issue [#158](https://github.com/calf-ai/calfkit-sdk/issues/158)
