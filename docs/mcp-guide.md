# MCP Adaptor Guide

Comprehensive reference for using the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) adaptor in calfkit. For the minimal quickstart, see the README. For design rationale, see [`docs/mcp-adaptor-design.md`](./mcp-adaptor-design.md).

---

## Concept overview

The MCP adaptor turns any MCP server (Gmail, GitHub, Postgres, filesystem, etc.) into calfkit tool nodes that any agent can invoke through standard Kafka envelopes. The model:

- An `McpServer` is a polymorphic object that lives in both the **agent worker** (where it advertises N tool schemas to the LLM) and the **tool/bridge worker** (where it spawns the MCP subprocess / HTTP session and dispatches calls).
- Each MCP tool gets its own Kafka topic pair: `mcp.<server>.<tool>.input` / `.output`.
- Schemas come from **codegen** (`calfkit mcp codegen`) — committed to your repo, version-controlled, and refreshed via the same CLI when the upstream MCP server changes.

This matches the dominant event-driven schema-management pattern (protobuf / Avro / Confluent Schema Registry — schemas as artifacts) and means agent workers never need MCP credentials or subprocess capabilities.

---

## Step-by-step setup

### 1. Install with the codegen extra

```bash
pip install calfkit[mcp-codegen]
```

The CLI dep (`typer`) is optional because most calfkit users won't need codegen at runtime — only at schema-refresh time. Production agent workers can omit the extra.

### 2. Generate schemas

Pick any MCP server. The CLI accepts either stdio or Streamable HTTP transports:

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

The CLI:
- Spawns the MCP server (or opens the HTTP connection).
- Runs MCP `initialize` then `tools/list`.
- Renders a Python module with `McpToolDef` constants sorted by tool name (deterministic output → CI-friendly `--check` mode).
- Writes it to `--output` (default: `schemas.py` in CWD).

**Commit the generated file.** It's version-controlled like any other source; the `do not edit by hand` banner reminds reviewers it's regenerated.

### 3. Declare the server in a shared module

Both the agent worker and the bridge worker need the same `McpServer` instance, so put it in a shared module:

```python
# shared.py
from calfkit import mcp
from gmail_schemas import Gmail
from github_schemas import Github

gmail = mcp("npx -y @modelcontextprotocol/server-gmail", tools=Gmail.ALL)
github = mcp("https://api.github.com/mcp", tools=Github.ALL, token="$GITHUB_TOKEN")
```

`mcp(...)` auto-detects the transport from its first argument (`http://` / `https://` → HTTP; otherwise → stdio). Use `mcp.stdio(...)` / `mcp.http(...)` for explicit form.

### 4. Wire into your workers

**Agent worker** (uses MCP servers as tool sources — never spawns the subprocess):

```python
# agent_worker.py
import asyncio
from calfkit import Agent, Client, Worker
from calfkit.providers import OpenAIResponsesModelClient
from shared import gmail, github

agent = Agent(
    "scribe",
    subscribe_topics="scribe.input",
    publish_topic="scribe.output",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-nano"),
    tools=[gmail, github],
)

async def main():
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[agent])
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
```

**Tools/bridge worker** (hosts the MCP sessions, dispatches calls):

```python
# tools_worker.py
import asyncio
from calfkit import Client, Worker
from shared import gmail, github

async def main():
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[gmail, github])
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
```

Run the two scripts. The agent worker sees Gmail + GitHub tools and can call them; the bridge worker handles the actual MCP dispatch.

**Single-process dev mode**: put both `agent` and the MCP servers in one Worker:

```python
worker = Worker(client, nodes=[gmail, github, agent])
```

Same code; the bridge worker just happens to also be the agent worker.

---

## Filtering, renaming, and selecting tools

`McpServer` is chainable and immutable. Each call returns a new server view; the original is untouched.

```python
# Only specific tools
gmail.only("search", "send")

# Exclude tools
gmail.exclude("delete_draft")

# Filter by MCP annotation hints
gmail.where(read_only_hint=True)           # only annotated read-only
gmail.where(destructive_hint=False)         # only annotated non-destructive
gmail.where(idempotent_hint=True)           # only annotated idempotent
gmail.where(open_world_hint=False)          # only annotated closed-world

# Custom predicate
gmail.where(predicate=lambda t: "search" in t.name.lower())

# Combine filters (AND semantics)
gmail.only("search", "send").where(read_only_hint=True)
gmail.where(read_only_hint=True).where(idempotent_hint=True)

# Rename for the LLM (topic paths use original names — wire identity stable)
gmail.prefix("inbox")                       # gmail.search → inbox.search
gmail.rename({"search": "find"})            # explicit mapping
```

Annotation filters apply the MCP spec defaults: `destructive_hint=True` and `open_world_hint=True` are the conservative defaults when an annotation is absent. So `.where(destructive_hint=False)` excludes tools without an explicit annotation, since the spec assumes they're destructive.

---

## Per-tenant identity (Pattern 1 multi-tenancy)

The MCP protocol binds credentials to the connection: "the Authorization header must be included in every HTTP request, even within the same logical session." Per-call HTTP credential rotation is not a protocol-supported concept.

**Pattern 1** — calfkit's recommended approach — passes user **identity** in MCP's `_meta` field on each tool call. The MCP server holds its own credential store (vendor OAuth, Stytch, Composio, Pipedream, etc.) and maps the identity to upstream tokens. Calfkit never sees the user's external-service credentials:

```python
from calfkit import mcp
from gmail_schemas import Gmail

gmail = mcp(
    "https://gmail-mcp.acme.com/mcp",
    # Calfkit ↔ MCP server: one service-level credential
    token="$CALFKIT_SERVICE_TOKEN",
    # Per-call: user identity passed in MCP _meta field
    meta=lambda ctx: {"user_id": ctx.deps.provided_deps["user_id"]},
    tools=Gmail.ALL,
)
```

The `meta` callable receives the same `ToolContext` that native `@agent_tool` functions see. It runs once per envelope, just before `session.call_tool`. Sync and async callables both work.

For deployments that genuinely need per-tenant client-side credentials (rare — usually only when the MCP server can't map identity), the cleanest approach is one bridge process per credential set. Each process has fixed credentials; tenancy is expressed by which Kafka partition routes which envelope.

---

## mcp.json drop-in

The MCP ecosystem uses a standard `mcp.json` config format (Claude Desktop, Cursor, Cline, Gemini CLI). Use it verbatim:

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

`$VAR` substitution in the JSON values is applied at load time. Unset env vars raise `McpConfigError` immediately (no silent empty-string production). A server in the JSON without a matching entry in `schemas=` also raises — misconfiguration fails loudly at startup, not at first traffic.

---

## CI drift detection

`calfkit mcp codegen --check` re-generates in memory and compares to the committed file. Exit codes:

- `0` — file matches the upstream MCP server's tools (no drift)
- `1` — drift detected (diff printed to stderr; file NOT overwritten)
- `2` — error (MCP server failed to start, file I/O failure, etc.)

Add to your CI:

```yaml
# .github/workflows/mcp-drift.yml
- name: Check MCP schema drift
  run: |
    calfkit mcp codegen gmail \
        --command "npx -y @modelcontextprotocol/server-gmail" \
        --output gmail_schemas.py \
        --check
```

On drift, run without `--check` to refresh, commit the regenerated file, and open a PR. The diff in the PR shows exactly which tools / fields changed.

---

## Observability — tap any tool's outputs

Because MCP tools live on standard calfkit topics, the existing `@consumer` decorator works without modification:

```python
from calfkit.client import NodeResult
from calfkit.nodes import consumer

# Tap one specific tool
@consumer(subscribe_topics="mcp.gmail.send.output")
async def audit_sent_emails(result: NodeResult) -> None:
    print(f"Sent: {result.output}")

# Tap multiple tools across servers
@consumer(subscribe_topics=["mcp.gmail.send.output", "mcp.github.create_issue.output"])
async def audit_destructive_actions(result: NodeResult) -> None:
    ...
```

Topic naming convention: `mcp.<normalized-server>.<original-tool-name>.<input|output>`. Server names with `.` or `-` are normalized to `_` in topic paths to avoid delimiter conflicts (e.g. `my-srv.v2` → `my_srv_v2`); the LLM-facing name is preserved separately.

---

## Idempotency / duplicate execution

Kafka delivers at-least-once. If a bridge worker crashes between executing an MCP tool call and committing the offset, the envelope can be redelivered — calling the tool again.

**v1 ships a worker-local LRU+TTL cache** keyed on `(tool_call_id, args_hash)`. Successful results are served from cache on redelivery without re-dispatching. Defaults: 1hr TTL, 10k entries max, in-memory only. Tools annotated `idempotentHint=True` bypass the cache (safe to re-run).

This is a hot fix; a generalized BaseNodeDef-level idempotency mechanism is tracked in [#161](https://github.com/calf-ai/issue/161).

You can supply a custom cache (e.g. with different TTL) via `Worker(..., idempotency_cache=IdempotencyCache(ttl_seconds=1800))`.

---

## Error semantics

Two distinct error layers, two distinct mappings:

| MCP situation | calfkit surface | Visibility |
|---|---|---|
| Tool ran but reported error (`isError=True`) | `RetryPromptPart` | LLM-visible; LLM can retry with adjusted args |
| Transport / RPC error (subprocess died, HTTP 5xx, timeout, MCPError) | `FailedToolCall` → `ToolExecutionError` at agent | Operator-visible; agent run halts |
| `meta=` hook raised | `FailedToolCall` | Operator-visible (hook is calfkit-internal config) |

The split mirrors calfkit's existing native-tool failure handling: `ModelRetry` exceptions → `RetryPromptPart`; everything else → `FailedToolCall`.

---

## Content adaptation

MCP tool results can include text, images, audio, embedded resources, and a typed `structuredContent` field. v1's adaptation:

1. **`structuredContent` is preferred** when present. It's a dict matching the tool's declared `outputSchema` and gets passed through as the `ToolReturn.return_value`.
2. **Otherwise, text content blocks are concatenated** and used as the return value.
3. **Non-text content** (image, audio, resource_link, embedded_resource) is summarised as a placeholder string (e.g. `[image: image/png]`). v1 does not pass multi-modal payloads to LLM providers — that's a v1.5+ enhancement.

Full content fidelity (multi-modal passthrough) is on the roadmap.

---

## What's NOT in v1

Documented limits so they don't surprise:

- **Resources and prompts** — MCP's `resources/*` and `prompts/*` primitives are out of v1 scope. v1 covers tools only.
- **Server-initiated sampling / elicitation** — calfkit's MCP client does not advertise these capabilities. Servers requiring them will fail at `initialize`.
- **Long-running tool calls** — MCP's experimental `tasks` extension is not implemented. A long call pins the bridge worker handler for its duration.
- **Per-call HTTP credentials** — not protocol-supported; use Pattern 1 (identity in `_meta`) or one bridge per credential set.
- **Hot reload via `notifications/tools/list_changed`** — bridges don't react to this notification in v1; users restart workers to pick up new tools.
- **Cross-process idempotency** — the dedup cache is per-process. Multi-replica bridges don't share state. Tracked in [#161](https://github.com/calf-ai/issue/161).

---

## Reference

- [`docs/mcp-v1-plan.md`](./mcp-v1-plan.md) — actionable v1 plan with all signed-off decisions
- [`docs/mcp-adaptor-design.md`](./mcp-adaptor-design.md) — design rationale, peer-SDK comparison, multi-tenancy patterns
- [`docs/mcp-adaptor-implementation-plan.md`](./mcp-adaptor-implementation-plan.md) — class-level implementation details
- [`docs/mcp-discovery-rpc-design.md`](./mcp-discovery-rpc-design.md) — v1.x roadmap entry for runtime discovery (alternative to codegen)
- [Model Context Protocol specification](https://modelcontextprotocol.io/specification/2025-11-25/)
