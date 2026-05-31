# MCP Adaptor — Overview

A concise overview of the calfkit MCP adaptor: what it is, when to reach
for it, and a working quickstart. For the full reference, see
[`mcp-guide.md`](./mcp-guide.md).

---

## What it is

The MCP adaptor turns any [Model Context Protocol](https://modelcontextprotocol.io/)
server — Gmail, GitHub, Postgres, filesystems, browsers, and hundreds of
others — into native calfkit tools. The same MCP server can be used by
any `Agent` over standard Kafka envelopes, with no MCP knowledge on the
agent side and no extra plumbing on the bridge side.

## When to use it

Reach for the MCP adaptor when you want to:

- **Plug in a third-party tool catalog** without writing custom adapters
  (Gmail/GitHub/etc. MCP servers exist; just point at them).
- **Separate concerns by worker**: the agent worker holds LLM
  credentials; the bridge worker holds MCP-server credentials. They
  share nothing except the Kafka topic contract.
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

| Feature | Status | Reference |
|---|---|---|
| stdio + Streamable HTTP transports | ✅ v1 | [`mcp-guide.md` §Setup](./mcp-guide.md#step-by-step-setup) |
| Codegen-generated schemas (`calfkit mcp codegen`) | ✅ v1 | [`mcp-guide.md` §CI drift](./mcp-guide.md#ci-drift-detection) |
| `mcp.json` drop-in (Claude Desktop / Cursor / Cline format) | ✅ v1 | [`mcp-guide.md` §mcp.json](./mcp-guide.md#mcpjson-drop-in) |
| Filters: `.only()` / `.exclude()` / `.where(read_only_hint=...)` | ✅ v1 | [`mcp-guide.md` §Filtering](./mcp-guide.md#filtering-renaming-and-selecting-tools) |
| Renames: `.prefix()` / `.rename({...})` | ✅ v1 | [`mcp-guide.md` §Filtering](./mcp-guide.md#filtering-renaming-and-selecting-tools) |
| Pattern 1 multi-tenancy (`meta=lambda ctx: {...}`) | ✅ v1 | [`mcp-guide.md` §Per-tenant](./mcp-guide.md#per-tenant-identity-pattern-1-multi-tenancy) |
| `@consumer` taps on per-tool topics | ✅ v1 | [`mcp-guide.md` §Observability](./mcp-guide.md#observability--tap-any-tools-outputs) |
| Per-worker idempotency cache (LRU + TTL) | ✅ v1 | [`mcp-guide.md` §Idempotency](./mcp-guide.md#idempotency--duplicate-execution) |
| `$VAR` env-var expansion (tokens, headers, env, args) | ✅ v1 | [`mcp-guide.md` §Pattern 1](./mcp-guide.md#per-tenant-identity-pattern-1-multi-tenancy) |
| Two-layer error semantics (tool-error → RetryPromptPart; transport → FailedToolCall) | ✅ v1 | [`mcp-guide.md` §Errors](./mcp-guide.md#error-semantics) |
| MCP resources, prompts, sampling, elicitation | ❌ out of v1 | [`mcp-guide.md` §What's NOT](./mcp-guide.md#whats-not-in-v1) |
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
calfkit mcp codegen gmail \
    --command "npx -y @modelcontextprotocol/server-gmail" \
    --output gmail_schemas.py
```

The CLI spawns the MCP server, runs `tools/list`, and writes a Python
module with one `McpToolDef` constant per tool plus a `Gmail.ALL` list.
**Commit the generated file** — it's source code now.

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
single-worker-crash redelivery but does NOT dedup across replicas.

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
`McpConfigError` immediately.

---

## Per-tenant identity

The MCP protocol binds credentials to the connection. The recommended
shape is to pass user **identity** (not credentials) in MCP's `_meta`
field on every call, and let the MCP server resolve identity → upstream
tokens server-side. The agent worker holds no per-user secrets.

```python
gmail = mcp(
    "https://gmail-mcp.acme.com/mcp",
    token="$CALFKIT_SERVICE_TOKEN",                          # session-static
    meta=lambda ctx: {"user_id": ctx.deps.provided_deps["user_id"]},
    tools=Gmail.ALL,
)
# user_id arrives via Client.execute_node(deps={"user_id": ...})
```

Sync or async `meta=` callables both work. Full pattern + alternatives
(one bridge per credential set, etc.):
[`mcp-guide.md` §Per-tenant identity](./mcp-guide.md#per-tenant-identity-pattern-1-multi-tenancy).

## Observability

Per-tool Kafka topics make tap-points trivial — the existing
`@consumer` decorator works without modification:

```python
from calfkit.client import NodeResult
from calfkit.nodes import consumer

@consumer(subscribe_topics="mcp.gmail.send.output")
async def audit_sent_emails(result: NodeResult) -> None:
    print(f"Sent: {result.output}")
```

Topic naming: `mcp.<normalized-server>.<original-tool-name>.<input|output>`.
Server names with `.` or `-` get normalized to `_` (e.g.
`my-srv.v2` → `my_srv_v2`).

## CI drift detection

Re-run codegen with `--check` to fail builds when the upstream MCP
server's tool catalog drifts from your committed schema. Re-run without
`--check` to refresh and commit the diff.

---

## Try it locally

A runnable end-to-end example using the reference
[`@modelcontextprotocol/server-everything`](https://github.com/modelcontextprotocol/servers/tree/main/src/everything)
server lives in
[`examples/quickstart_mcp/`](../examples/quickstart_mcp/) — codegen +
shared module + split agent / bridge workers + one-shot invoker, with
its own README.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `calfkit mcp` exits with "command not found" | Install with the `[mcp-codegen]` extra |
| `npx` not found / codegen hangs at spawn | Install Node.js 18+ (the MCP server is `npx`-installed) |
| `Connection refused` on `Client.connect("localhost:9092")` | Broker not running — start [`calfkit-broker`](https://github.com/calf-ai/calfkit-broker) |
| LLM call fails with `AuthenticationError` | `OPENAI_API_KEY` not exported in the agent worker's environment |
| Agent never receives tool results | Both workers must share the same broker AND the same `McpServer` instance (imported from a shared module) |
| `McpConfigError: scheme '<x>' is not http(s)://` | `mcp("ws://...")` etc.; for non-HTTP-MCP transports use `mcp.stdio(...)` explicitly |

## Where to go next

- **Full reference**: [`mcp-guide.md`](./mcp-guide.md) — filters, renames,
  multi-tenancy alternatives, mcp.json, idempotency, error semantics,
  content adaptation, what's NOT in v1.
- **Design rationale**: [`mcp-adaptor-design.md`](./mcp-adaptor-design.md) —
  peer-SDK comparison, multi-tenancy patterns, content fidelity.
- **v1 plan**: [`mcp-v1-plan.md`](./mcp-v1-plan.md) — signed-off
  decisions with sourced rationale per design question.
- **Runnable example**: [`examples/quickstart_mcp/`](../examples/quickstart_mcp/).
- **MCP protocol spec**: [`modelcontextprotocol.io`](https://modelcontextprotocol.io/specification/2025-11-25/).
