# MCP Adaptor

How to expose any [Model Context Protocol](https://modelcontextprotocol.io/)
server's tools as native calfkit tools — what it does, how to use it,
how to deploy it.

---

## What it is

The MCP adaptor turns any MCP server — Gmail, GitHub, Postgres,
filesystems, browsers, and hundreds of others — into native calfkit
tools. The same MCP server can be used by any `Agent` over standard
Kafka envelopes, with no MCP knowledge on the agent side and no extra
plumbing on the bridge side.

## When to use it

- **Plug in a third-party tool catalog** without writing custom adapters.
- **Separate concerns by worker**: agent worker holds LLM credentials;
  bridge worker holds MCP-server credentials. They share nothing except
  the Kafka topic contract.
- **Pass per-user identity** to a multi-tenant MCP server without
  rotating its session credentials (Pattern 1 — identity in `_meta`).
- **Tap MCP tool outputs** with `@consumer` for audit logs, metrics,
  or downstream pipelines.

If you only need calfkit-internal tools (`@agent_tool`), skip this — the
MCP adaptor adds value when crossing the calfkit ↔ MCP boundary.

## Under the hood

- One `McpServer` object plays two roles: it iterates as tool schemas
  for the agent's LLM, and acts as a node group for the bridge worker
  that spawns the MCP subprocess (or opens the HTTP session).
- Each MCP tool gets its own Kafka topic pair —
  `mcp.<server>.<tool>.input` / `.output` — so tool calls are routed,
  observed, and replayed like any other calfkit dispatch.
- Schemas come from offline **codegen** (`calfkit mcp codegen`) —
  committed, version-controlled, and drift-checked in CI. Agents never
  need network access to discover what tools an MCP server exposes.

---

## Functionality at a glance

| Feature | Status | Section |
|---|---|---|
| stdio + Streamable HTTP transports | ✅ v1 | [Quickstart](#quickstart) |
| Codegen-generated schemas (`calfkit mcp codegen`) | ✅ v1 | [Step 2](#2-generate-schemas-one-time-committed) |
| `mcp.json` drop-in (Claude Desktop / Cursor / Cline format) | ✅ v1 | [mcp.json](#many-mcp-servers-via-mcpjson) |
| Filters: `.only()` / `.exclude()` / `.where(...)` | ✅ v1 | [Filtering](#filtering-renaming-and-selecting-tools) |
| Renames: `.prefix()` / `.rename({...})` | ✅ v1 | [Filtering](#filtering-renaming-and-selecting-tools) |
| Pattern 1 multi-tenancy (`meta=lambda ctx: {...}`) | ✅ v1 | [Per-tenant](#per-tenant-identity) |
| `@consumer` taps on per-tool topics | ✅ v1 | [Observability](#observability) |
| Per-worker idempotency cache (LRU + TTL) | ✅ v1 | [Idempotency](#idempotency) |
| `$VAR` env-var expansion (tokens, headers, env, args) | ✅ v1 | construction-time |
| Two-layer error semantics | ✅ v1 | [Errors](#error-semantics) |
| MCP resources, prompts, sampling, elicitation | ❌ out of v1 | [What's NOT](#whats-not-in-v1) |
| Runtime tool discovery (RPC-style) | 🛣 v1.x roadmap | [`mcp-discovery-rpc-design.md`](./mcp-discovery-rpc-design.md) |

---

## Quickstart

~5 minutes if you already have a Kafka broker running; ~15 minutes from
a cold start.

### Prerequisites

- Python 3.10+
- Node.js 18+ (the example MCP server is `npx`-installed)
- A Kafka broker — the [calfkit-broker](https://github.com/calf-ai/calfkit-broker)
  container is the easiest local option
- `OPENAI_API_KEY` exported in the agent worker's environment

### 1. Install

```bash
pip install calfkit[mcp-codegen]   # codegen extra adds the typer-based CLI
```

### 2. Generate schemas (one-time, committed)

```bash
# stdio (most common — npx-installable servers)
calfkit mcp codegen gmail \
    --command "npx -y @modelcontextprotocol/server-gmail" \
    --output gmail_schemas.py

# Streamable HTTP
calfkit mcp codegen github \
    --url "https://api.github.com/mcp" \
    --token "$GITHUB_TOKEN" \
    --output github_schemas.py
```

The CLI spawns the MCP server, runs `tools/list`, and writes a Python
module with one `McpToolDef` constant per tool plus a `Gmail.ALL` list,
sorted by name for deterministic diffs. **Commit the generated file** —
it's source code now.

### 3. Declare the server in a shared module

```python
# shared.py — imported by both agent and bridge workers
from calfkit import mcp
from gmail_schemas import Gmail

# Auto-detects from the first arg: starts with http(s):// → HTTP;
# any other "scheme://" prefix raises (use `mcp.http(url)` for that);
# otherwise treats as a stdio command line. Server name is inferred
# from the npm package; pass `name=` to override.
gmail = mcp("npx -y @modelcontextprotocol/server-gmail", tools=Gmail.ALL)
```

By default the bridge passes the worker's full `os.environ` to the MCP
subprocess. For multi-tenant or untrusted MCP servers, opt into the MCP
SDK's safe allowlist instead:

```python
gmail = mcp.stdio(
    "npx", "-y", "@modelcontextprotocol/server-gmail",
    tools=Gmail.ALL,
    safe_env_only=True,           # only HOME, PATH, USER, LANG, ... + your explicit env=
    env={"GMAIL_OAUTH": "$OAUTH"},
)
```

### 4. Wire into a worker

This is the **single-process dev topology** — agent and bridge in one
process. For production, see [Deployment topologies](#deployment-topologies)
below.

```python
# main.py
import asyncio
from calfkit import Agent, Client, Worker
from calfkit.providers import OpenAIResponsesModelClient
from shared import gmail

agent = Agent(
    "scribe",
    subscribe_topics="scribe.input",
    publish_topic="scribe.output",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-nano"),
    tools=[gmail],
)

async def main() -> None:
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[gmail, agent])
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
```

Invoke from another process:

```python
import asyncio
from calfkit.client import Client

async def main() -> None:
    client = Client.connect("localhost:9092")
    result = await client.execute_node(
        "Find emails from sam@example.com this week",
        "scribe.input",
    )
    print(result.output)

asyncio.run(main())
```

That's it — the agent's LLM sees Gmail's tools, calls them through
calfkit's standard dispatch path, and you get the final response back.

---

## Deployment topologies

### Single-process (dev)

```python
worker = Worker(client, nodes=[gmail, github, agent])
```

One worker hosts both the agent and the MCP subprocesses. Simplest
setup, no Kafka partitioning concerns, ideal for local dev.

### Split agent / bridge (prod)

Same code, two processes:

```python
# agent_service.py — needs LLM credentials, NOT MCP subprocess
worker = Worker(client, nodes=[agent])
```

```python
# bridge_service.py — needs MCP-server credentials, NOT LLM
worker = Worker(client, nodes=[gmail, github])
```

The same `McpServer` object lives in both processes (imported from a
shared module). The agent worker iterates it for tool schemas; the
bridge worker spawns the underlying MCP subprocess. They communicate
over the `mcp.<server>.<tool>.input/output` topics — no in-process
coupling.

Run multiple bridge replicas for horizontal scale: each Kafka partition
routes to one replica. The idempotency cache is per-process — it covers
single-worker-crash redelivery but does NOT dedup across replicas (see
[Idempotency](#idempotency)).

### Many MCP servers via `mcp.json`

Drop a Claude Desktop / Cursor / Cline `mcp.json` verbatim:

```json
{
  "mcpServers": {
    "gmail":    {"command": "npx", "args": ["-y", "@modelcontextprotocol/server-gmail"]},
    "github":   {"type": "http", "url": "https://api.github.com/mcp",
                  "headers": {"Authorization": "Bearer $GH_TOKEN"}},
    "postgres": {"type": "http", "url": "https://postgres-mcp.acme.com/mcp"}
  }
}
```

```python
from calfkit.mcp import McpServers
from gmail_schemas import Gmail
from github_schemas import Github
from postgres_schemas import Postgres

servers = McpServers.from_file("./mcp.json", schemas={
    "gmail":    Gmail.ALL,
    "github":   Github.ALL,
    "postgres": Postgres.ALL,
})

agent  = Agent("scribe", tools=[*servers.values()], ...)
worker = Worker(client, nodes=[*servers.values(), agent])
```

`$VAR` substitution is applied at load time; unset variables raise
`McpConfigError` immediately. A server in the JSON without a matching
entry in `schemas=` also raises — misconfig fails loudly at startup.

When the config comes from env vars / Vault / a K8s ConfigMap rather
than a file on disk, pass the parsed dict directly:

```python
servers = McpServers.from_config(my_config_dict, schemas={...})
```

---

## Filtering, renaming, and selecting tools

`McpServer` is chainable and immutable — each call returns a new view;
the original is untouched.

```python
gmail.only("search", "send")            # allowlist
gmail.exclude("delete_draft")           # blocklist

# Filter by MCP annotation hints
gmail.where(read_only_hint=True)        # only annotated read-only
gmail.where(destructive_hint=False)     # only annotated non-destructive
gmail.where(idempotent_hint=True)
gmail.where(predicate=lambda t: "search" in t.name.lower())

# Combine (AND semantics)
gmail.only("search", "send").where(read_only_hint=True)

# Rename for the LLM (topic paths use original names — wire stable)
gmail.prefix("inbox")                   # search → inbox.search
gmail.rename({"search": "find"})        # explicit mapping
```

Annotation filters apply MCP spec defaults: `destructive_hint=True` and
`open_world_hint=True` are the conservative defaults when an annotation
is absent, so `.where(destructive_hint=False)` excludes tools without
explicit annotations.

---

## Per-tenant identity

The MCP protocol binds credentials to the connection — per-call HTTP
credential rotation is not protocol-supported. Pass user **identity**
(not credentials) in MCP's `_meta` field on every call, and let the MCP
server resolve identity → upstream tokens server-side. The agent worker
holds no per-user secrets.

```python
gmail = mcp(
    "https://gmail-mcp.acme.com/mcp",
    token="$CALFKIT_SERVICE_TOKEN",            # session-static
    meta=lambda ctx: {"user_id": ctx.deps.provided_deps["user_id"]},
    tools=Gmail.ALL,
)
# user_id arrives via Client.execute_node(deps={"user_id": ...})
```

The `meta=` callable receives the same `ToolContext` that native
`@agent_tool` functions see and runs once per envelope, just before
`session.call_tool`. Sync and async callables both work.

If the MCP server can't map identity → credentials server-side (rare),
run one bridge process per credential set — tenancy is then expressed
by which Kafka partition routes which envelope.

---

## Observability

Per-tool Kafka topics make tap-points trivial — the existing
`@consumer` decorator works without modification:

```python
from calfkit.client import NodeResult
from calfkit.nodes import consumer

@consumer(subscribe_topics="mcp.gmail.send.output")
async def audit_sent_emails(result: NodeResult) -> None:
    print(f"Sent: {result.output}")

# Tap multiple tools across servers
@consumer(subscribe_topics=["mcp.gmail.send.output", "mcp.github.create_issue.output"])
async def audit_destructive_actions(result: NodeResult) -> None:
    ...
```

Topic naming: `mcp.<normalized-server>.<original-tool-name>.<input|output>`.
Server names with `.` or `-` get normalized to `_` (e.g.
`my-srv.v2` → `my_srv_v2`).

---

## CI drift detection

`calfkit mcp codegen --check` re-generates in memory and compares to
the committed file. Exit codes:

- `0` — file matches the upstream MCP server's tools (no drift)
- `1` — drift detected (diff printed to stderr; file NOT overwritten)
- `2` — error (MCP server failed to start, file I/O failure, etc.)

Wire into CI:

```yaml
# .github/workflows/mcp-drift.yml
- name: Check MCP schema drift
  run: |
    calfkit mcp codegen gmail \
        --command "npx -y @modelcontextprotocol/server-gmail" \
        --output gmail_schemas.py \
        --check
```

On drift, re-run without `--check` to refresh, commit the diff, and
open a PR. The diff shows exactly which tools / fields changed.

---

## Idempotency

Kafka delivers at-least-once. If a bridge worker crashes between
executing an MCP tool call and committing the offset, the envelope can
be redelivered — calling the tool again.

v1 ships a **per-worker LRU + TTL cache** keyed on
`(tool_call_id, args_hash)`. Successful results are served from cache
on redelivery without re-dispatching. Defaults: 1 hr TTL, 10k entries
max, in-memory only. Tools annotated `idempotentHint=True` bypass the
cache (safe to re-run).

Supply a custom cache (e.g. different TTL) on the Worker:

```python
from calfkit.mcp._dedup import IdempotencyCache

worker = Worker(client, nodes=[...],
                idempotency_cache=IdempotencyCache(ttl_seconds=1800))
```

**Limitation**: the cache is per-process. Multi-replica bridge
deployments don't share state — a redelivery to a different replica
will re-execute. Cross-process idempotency is tracked in
[#161](https://github.com/calf-ai/calfkit-sdk/issues/161).

---

## Error semantics

Two distinct error layers, two distinct mappings:

| MCP situation | Calfkit surface | Visibility |
|---|---|---|
| Tool ran but reported error (`isError=True`) | `RetryPromptPart` | LLM-visible; LLM can retry with adjusted args |
| Transport / RPC error (subprocess died, HTTP 5xx, timeout, `McpError`) | `FailedToolCall` → `ToolExecutionError` at agent | Operator-visible; agent run halts |
| `meta=` hook raised | `FailedToolCall` | Operator-visible (hook is calfkit-internal config) |

The split mirrors calfkit's native-tool failure handling: `ModelRetry`
exceptions → `RetryPromptPart`; everything else → `FailedToolCall`.

---

## Content adaptation

MCP tool results can include text, images, audio, embedded resources,
and a typed `structuredContent` field. v1 adapts as follows:

1. **`structuredContent` is preferred** when present — passed through
   as the `ToolReturn.return_value`.
2. **Otherwise, text content blocks are concatenated** and used as the
   return value.
3. **Non-text content** (image, audio, resource_link, embedded_resource)
   is summarised as a placeholder string (e.g. `[image: image/png]`).
   v1 does not pass multi-modal payloads to LLM providers; full
   multi-modal passthrough is on the roadmap.

---

## What's NOT in v1

Documented limits so they don't surprise:

- **Resources and prompts** — MCP's `resources/*` and `prompts/*`
  primitives are out of v1 scope; v1 covers tools only.
- **Server-initiated sampling / elicitation** — calfkit's MCP client
  does not advertise these capabilities; servers requiring them fail at
  `initialize`.
- **Long-running tool calls** — MCP's experimental `tasks` extension is
  not implemented. A long call pins the bridge worker handler for its
  duration.
- **Per-call HTTP credentials** — not protocol-supported; use Pattern 1
  (identity in `_meta`) or one bridge per credential set.
- **Hot reload via `notifications/tools/list_changed`** — bridges don't
  react to this notification; restart workers to pick up new tools.
- **Cross-process idempotency** — see [Idempotency](#idempotency).

---

## Try it locally

A runnable end-to-end example using the reference
[`@modelcontextprotocol/server-everything`](https://github.com/modelcontextprotocol/servers/tree/main/src/everything)
server lives in
[`examples/quickstart_mcp/`](../examples/quickstart_mcp/) — codegen +
shared module + split agent / bridge workers + one-shot invoker, with
its own README.

---

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `calfkit mcp` exits with "command not found" | Install with the `[mcp-codegen]` extra |
| `npx` not found / codegen hangs at spawn | Install Node.js 18+ (the MCP server is `npx`-installed) |
| `Connection refused` on `Client.connect("localhost:9092")` | Broker not running — start [`calfkit-broker`](https://github.com/calf-ai/calfkit-broker) |
| LLM call fails with `AuthenticationError` | `OPENAI_API_KEY` not exported in the agent worker's environment |
| Agent never receives tool results | Both workers must share the same broker AND the same `McpServer` instance (imported from a shared module) |
| `McpConfigError: scheme '<x>' is not http(s)://` | `mcp("ws://...")` etc.; for non-HTTP-MCP transports use `mcp.stdio(...)` explicitly |
| `McpConfigError: environment variable '<X>' is unset` | A `$VAR` in a token / header / URL / env value didn't resolve — export the variable or fix the spelling |

---

## Further reading

- **Runnable example**: [`examples/quickstart_mcp/`](../examples/quickstart_mcp/)
- **Design rationale**: [`mcp-adaptor-design.md`](./mcp-adaptor-design.md) —
  peer-SDK comparison, multi-tenancy patterns, content fidelity
- **v1 plan**: [`mcp-v1-plan.md`](./mcp-v1-plan.md) — signed-off
  decisions with sourced rationale per design question
- **v1.x roadmap**: [`mcp-discovery-rpc-design.md`](./mcp-discovery-rpc-design.md) —
  runtime tool discovery (alternative to codegen)
- **MCP protocol spec**: [`modelcontextprotocol.io`](https://modelcontextprotocol.io/specification/2025-11-25/)
