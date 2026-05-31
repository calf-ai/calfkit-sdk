# MCP Support in Calfkit — v1 Plan

**Status:** Ready for implementation. All open questions signed off (§11).
**Document version:** 1.1
**Last updated:** 2026-05-30
**Tracking issue:** [#158](https://github.com/calf-ai/calfkit-sdk/issues/158)
**Companion docs (deeper detail):**
- [`docs/mcp-adaptor-design.md`](./mcp-adaptor-design.md) — design intent, peer-SDK comparison, prior art
- [`docs/mcp-adaptor-implementation-plan.md`](./mcp-adaptor-implementation-plan.md) — class-level code, file-by-file changes
- [`docs/mcp-discovery-rpc-design.md`](./mcp-discovery-rpc-design.md) — v1.x runtime discovery roadmap
- [`docs/mcp-catalog-discovery-plan.md`](./mcp-catalog-discovery-plan.md) — archived alternative

This document is the **actionable v1 plan**: scope, decisions, timeline, sign-off checklist. Read this to understand what v1 ships and when. Read the companion docs for design rationale and code-level detail.

---

## 1. v1 at a glance

| | |
|---|---|
| **What ships** | First-class MCP server adaptor: declare any MCP server via codegen-generated schemas, expose its tools as native calfkit tool nodes, agents call them through standard Kafka envelopes. Both stdio and Streamable HTTP transports. |
| **What doesn't** | Runtime tool discovery (v1.x roadmap), resources/prompts (v2), sampling/elicitation (v2), per-call HTTP credentials (Pattern 2/3 alternatives), in-flight call durability across crashes (deferred), long-running task polling (v2). |
| **User effort** | One CLI invocation per MCP server (`calfkit mcp codegen`), plus a few lines of Python to declare and use it. |
| **Implementation effort** | ~17 working days, single engineer (compressible to ~12 calendar days with phases 5–7 running in parallel). ~9 new files under `calfkit/mcp/` + CLI subcommand, ~50-line patch to `Worker.run()`, zero agent code changes, zero FastStream-internals access. See §7 for the per-phase breakdown. |
| **New deps** | `mcp>=1.20.0`, `httpx` (already transitive). |
| **Pin compatibility** | Uses only stable FastStream public API — no version-pin restriction beyond the existing one. |

---

## 2. The user experience

### 2.1 Minimal three-line setup

```bash
# One-time per MCP server: generate the schema declarations
calfkit mcp codegen gmail \
    --command "npx -y @modelcontextprotocol/server-gmail" \
    --output gmail_schemas.py
```

```python
# shared.py — imported by both processes
from calfkit.mcp import McpServer
from gmail_schemas import Gmail

gmail = McpServer.stdio(
    "npx", "-y", "@modelcontextprotocol/server-gmail",
    tools=Gmail.ALL,
)
```

```python
# agent_worker.py
from calfkit import Agent, Client, Worker
from shared import gmail

agent = Agent("scribe", subscribe_topics="scribe.input",
              tools=[gmail], model_client=...)
asyncio.run(Worker(Client.connect("..."), nodes=[agent]).run())
```

```python
# tools_worker.py
from calfkit import Client, Worker
from shared import gmail

asyncio.run(Worker(Client.connect("..."), nodes=[gmail]).run())
```

That's it. One CLI call + four imports. No subscriptions, no topic configs, no credentials in the agent process.

### 2.2 Multi-server via `mcp.json`

```python
from calfkit.mcp import McpServers
from my_schemas import Gmail, Github, Postgres

servers = McpServers.from_file("./mcp.json", schemas={
    "gmail": Gmail.ALL,
    "github": Github.ALL,
    "postgres": Postgres.ALL,
})

agent = Agent("scribe", tools=[*servers.values()], ...)
worker = Worker(client, nodes=[*servers.values(), agent])
```

Standard `mcp.json` shape (Claude Desktop / Cursor / Cline-compatible):

```json
{
  "mcpServers": {
    "gmail":    {"command": "npx", "args": ["-y", "@mcp/server-gmail"], "env": {"GMAIL_OAUTH": "$GMAIL_OAUTH"}},
    "github":   {"command": "npx", "args": ["-y", "@mcp/server-github"]},
    "postgres": {"type": "http", "url": "https://postgres-mcp.acme.com/mcp", "headers": {"Authorization": "Bearer $PG_TOKEN"}}
  }
}
```

### 2.3 Per-tenant identity (Pattern 1)

```python
gmail = McpServer.http(
    "https://gmail-mcp.acme.com/mcp",
    token="$CALFKIT_SERVICE_TOKEN",                     # session-static auth
    meta=lambda ctx: {"user_id": ctx.deps.provided_deps["user_id"]},  # per-call identity
    tools=Gmail.ALL,
)
```

Credentials are session-scoped; user identity travels in MCP `_meta` field. The MCP server handles credential mapping per user.

### 2.4 Filtering, renaming, observability

```python
# Filter to a subset
gmail = McpServer.stdio(..., tools=Gmail.ALL).where(read_only_hint=True)

# Rename for the LLM
gmail = McpServer.stdio(..., tools=Gmail.ALL).prefix("inbox")

# Tap any tool's outputs — works because MCP tools live on standard calfkit topics
@consumer(subscribe_topics="mcp.gmail.send.output")
async def audit_sent_email(result):
    ...
```

### 2.5 Drift detection in CI

```yaml
# .github/workflows/mcp-drift.yml
- run: calfkit mcp codegen gmail --command "..." --check
```

`--check` exits non-zero if regenerating would change the file. Run on a schedule; if it fails, open a PR with the refreshed schemas.

---

## 3. v1 scope — what's in

| Feature | In v1? | Notes |
|---|---|---|
| stdio transport | ✓ | Subprocess managed by the bridge worker |
| Streamable HTTP transport | ✓ | Headers/token session-static; no per-call mutation |
| `McpServer` polymorphic object (works in `nodes=` and `tools=`) | ✓ | Schema-only registration via existing `BaseToolNodeSchema` path |
| Per-tool Kafka topics (`mcp.<server>.<tool>.input/.output`) | ✓ | One subscriber per tool; shared session in the bridge |
| `calfkit mcp codegen` CLI | ✓ | Generates Python module with `McpToolDef` instances |
| `calfkit mcp codegen --check` (drift detection) | ✓ | For CI guardrails |
| Inline `McpToolDef` declarations | ✓ | Escape hatch for users who don't want codegen |
| `mcp.json` config interop (`McpServers.from_file`) | ✓ | Schemas supplied separately via `schemas=` kwarg |
| Pattern 1 multi-tenancy (`meta=` per-call hook) | ✓ | Identity in `_meta`; credentials session-static |
| Filtering: `.only()`, `.exclude()`, `.where()` | ✓ | Operates on user-declared `tools=` list |
| Renaming: `.prefix()`, `.rename()` | ✓ | Affects LLM-facing name only; topic name unchanged |
| Tool-semantic errors → `RetryPromptPart` (LLM-retryable) | ✓ | `CallToolResult.is_error=True` mapping |
| Transport errors → `FailedToolCall` (operator-visible) | ✓ | MCP `MCPError` and timeout mapping |
| Sanity-check `tools/list` on bridge boot (declared vs server) | ✓ | Logs warning on drift |
| Idempotency-key dedup for non-idempotent tools | ✓ | Worker-side cache keyed on `(tool_call_id, args_hash)`, honors `idempotentHint` |
| `structuredContent` preferred over text in tool returns | ✓ | Non-text content blocks summarized as placeholders |
| `FakeMcpServer` for tests | ✓ | Bypasses transport entirely |
| Observability via per-tool publish topics | ✓ | Native to calfkit's consumer model |

## 4. v1 scope — what's out (and why)

| Feature | Why deferred | Roadmap |
|---|---|---|
| Runtime tool discovery (no codegen) | Codegen aligns with dominant event-driven schema patterns (protobuf/Avro/Confluent SR); runtime RPC discovery is an opt-in alternative for users who prefer it | [v1.x — RPC design](./mcp-discovery-rpc-design.md) |
| `resources/*` MCP primitive | Requires new node abstraction; LLM-context pipeline rewire | v2 |
| `prompts/*` MCP primitive | Same | v2 |
| Server-initiated sampling | Needs reverse-topic protocol design | v2 (own RFC) |
| Server-initiated elicitation | Not meaningful in automated agent runtime | (not planned) |
| `notifications/tools/list_changed` hot reload | Long-lived worker requirement; trivial after RPC discovery lands | v2 |
| Pydantic-level `outputSchema` validation | Pass-through is sufficient for v1 | v2 |
| Per-call HTTP credentials (callable `token=`) | Not a protocol-supported concept; introduces async-coherence trap. Pattern 2/3 covers genuine multi-credential needs | (not planned at this layer) |
| Multi-modal LLM passthrough (image/audio content) | Requires changes to pydantic-ai `ToolReturn` and provider integrations | v1.5+ |
| In-flight call durability across worker crashes | Requires calfkit-wide durability layer | post-v1 |
| Long-running tool calls via MCP `tasks` extension | Experimental protocol feature; pins worker handlers | v2 |
| Standalone MCP-bridge container image | Operational improvement; SDK API is forward-compatible | v1.5+ |

---

## 5. Design decisions made (consolidated reference)

Each decision is settled. Companion docs carry the full reasoning.

| # | Decision | Rationale (short) | Detailed reasoning |
|---|---|---|---|
| **D1** | **Schemas via codegen, not runtime discovery** | Aligns with protobuf/Avro/Confluent SR pattern; eliminates FastStream-internals access; reviewable schema changes via git diff; bridge availability decouples from agent boot | Design doc §10; this doc §2 of [archived alternative](./mcp-catalog-discovery-plan.md) |
| **D2** | **Pattern 1 multi-tenancy: identity in `_meta`, credentials session-static** | Protocol-canonical (matches MCP spec on session-scoped auth); no async-coherence races; trust boundary stays clean | Design doc §10 |
| **D3** | **Per-tool Kafka topics (one per MCP tool)** | Routing symmetry with native calfkit tools; per-tool observability, ordering, retry policy; zero agent code changes | Design doc §7.1 |
| **D4** | **`McpServer` is polymorphic — works in `Worker(nodes=)` and `Agent(tools=)`** | DX consistency with native `@agent_tool` pattern; one import, two contexts | Design doc §11 |
| **D5** | **Both stdio and Streamable HTTP transports in v1** | stdio is most-shipped in the ecosystem; HTTP is right for production multi-tenant; ~30% additional code; same MCP SDK path | Design doc §15 Q4 |
| **D6** | **`structuredContent` preferred over text** | Most ecosystem MCP servers return text; non-text content as placeholders is honest about v1 limit | Design doc §9 |
| **D7** | **Schema-only registration via existing `BaseToolNodeSchema` path** | Agent loop already supports this (agent.py:163-167, 374); zero changes to agent code | Impl plan §1 |
| **D8** | **FastStream `on_startup` hook is the bridge initialization seam** | Fires before `broker.start()`; no race with incoming messages; cancellation-safe with `BaseException` cleanup | Impl plan §6 |
| **D9** | **`mcp.json` drop-in interop** | Reuses the de facto config format from Claude Desktop / Cursor / Cline; schemas supplied separately via `schemas=` | Design doc §6.6 |
| **D10** | **Idempotency-key dedup promoted into v1** | ~50 LOC; closes the destructive-tool double-execute case under Kafka redelivery; honors `idempotentHint` annotation | DD-5 from design doc §15 |
| **D11** | **`from calfkit.mcp import McpServer` canonical; `from calfkit import mcp` factory shortcut** | Class form is IDE-discoverable; lowercase factory is the README-leading convenience | Impl plan §15.1 |
| **D12** | **stdio environment merges `os.environ`** | Matches Docker/subprocess defaults; documented behavior; trust-domain mitigation (operator-deployed MCP servers) | Impl plan §15.2 |
| **D13** | **`Agent(tools=)` type widened via `ToolLike` Union alias** | Single line; future-proof for additional shapes | Impl plan §15.3 |
| **D14** | **`meta=` accepts both sync and async callables** | Trivial to support; mirrors existing calfkit gates | Impl plan §15.4 |
| **D15** | **Codegen output is versioned JSON-equivalent Python module** | Human-readable, diff-friendly, IDE-completable | Impl plan §15.5 |
| **D16** | **README minimal example + separate `docs/mcp-guide.md` for advanced patterns** | Easy onboarding + thorough reference | Impl plan §15.6 |

---

## 6. Implementation overview

### 6.1 Module layout

```
calfkit/
├── __init__.py                 (modified — re-export `mcp` factory and McpServer)
├── mcp/
│   ├── __init__.py             (public surface — mcp, McpServer, McpServers, McpToolDef)
│   ├── _server.py              (McpServer class + factory functions)             ~250 LOC
│   ├── _bridge.py              (McpBridge: per-tool dispatch node)               ~180 LOC
│   ├── _session.py             (McpSession: ClientSession + transport lifecycle) ~220 LOC
│   ├── _tool_def.py            (McpToolDef dataclass — codegen target)            ~80 LOC
│   ├── _adapt.py               (MCP types ↔ calfkit types)                       ~120 LOC
│   ├── _config.py              (mcp.json parsing + env substitution)              ~80 LOC
│   ├── _codegen.py             (codegen renderer)                                ~120 LOC
│   ├── _dedup.py               (idempotency-key cache)                            ~50 LOC
│   ├── _testing.py             (FakeMcpServer for tests)                         ~120 LOC
│   └── exceptions.py           (McpConfigError, etc.)                             ~30 LOC
├── cli/
│   └── mcp.py                  (`calfkit mcp codegen` typer command)              ~80 LOC
└── worker/worker.py            (modified — McpServer segregation + on_startup)   ~50 LOC diff
```

**Total new code: ~1,500 LOC** plus ~800 LOC of tests. Zero changes to `calfkit/nodes/agent.py`. Zero FastStream-internals access (no `broker.config.*`). No new heavy dependencies.

### 6.2 Worker.run() patch (the only existing-file change)

```python
async def run(self, **extra_run_args: Any) -> None:
    app = FastStream(
        self._client._connection,
        on_startup=[self._on_startup],
        on_shutdown=[self._on_shutdown],
    )
    await app.run(**extra_run_args)

async def _on_startup(self) -> None:
    spawned: list[McpServer] = []
    try:
        for server in self._mcp_servers:
            await server._open_bridge_session()  # spawn + initialize + sanity-check
            spawned.append(server)
            for tool_def in server._apply_filters(server._tools):
                self._mcp_bridges.append(McpBridge(server, tool_def))
        self._register_handlers_now()
    except BaseException:
        for s in spawned:
            if s._session is not None:
                try: await s._session.aclose()
                except Exception: pass
                s._session = None
        raise
```

### 6.3 Dependencies added

```toml
# pyproject.toml
[project]
dependencies = [
    # ...existing...
    "mcp>=1.20.0",       # official MCP Python SDK
    # httpx is already transitive via existing LLM provider deps
]

[project.optional-dependencies]
mcp-codegen = ["typer>=0.12"]   # for the CLI (only needed when running codegen)
```

---

## 7. Phased implementation

Sequential phases 1–4; phases 5–7 parallelizable; phase 8 is release.

| Phase | Days | Deliverables |
|---|---|---|
| **0 — Pre-work** | 1 | Add `mcp>=1.20.0` to deps; `calfkit/mcp/` skeleton; sanity test that the `BaseToolNodeSchema` path works as advertised |
| **1 — Core types + session** | 3 | `_tool_def.py` (McpToolDef); `_session.py` (transports + session wrapper); `_adapt.py` (error/content mapping); `_config.py` (`mcp.json` parse + env expand); unit tests for each |
| **2 — McpServer frontend** | 2 | `_server.py` (McpServer with filters/renames; trivial `__iter__` over user-supplied tools); `_testing.py` (FakeMcpServer); filter composition tests |
| **3 — McpBridge runtime** | 3 | `_bridge.py` (BaseNodeDef subclass dispatching to shared McpSession); `_dedup.py` (idempotency cache); integration tests with `FakeMcpServer` + `TestKafkaBroker` |
| **4 — Worker integration** | 1 | Patch `worker.py`: McpServer segregation, `_on_startup`/`_on_shutdown`, FastStream hook wiring, BaseException cleanup. Single-process + split-process lifecycle tests. |
| **5 — Codegen CLI** | 2 | `_codegen.py` renderer; `cli/mcp.py` typer command (`codegen`, `codegen --check`); tests using FakeMcpServer + tempdirs |
| **6 — Public API polish** | 1 | Module-level `mcp` factory + `.stdio` / `.http` namespacers; auto-detect logic; `McpServers.from_file` / `from_config`; end-to-end fixture |
| **7 — Documentation + examples** | 2 | README MCP section; `docs/mcp-guide.md` (multi-server, Pattern 1, observability tap); `examples/quickstart_mcp/` (4 files + 1 generated schema file) |
| **8 — E2E + release** | 2 | Integration tests against `@modelcontextprotocol/server-everything` + an HTTP MCP fixture (separate CI lane); release-please config; smoke test against a real OAuth server |

**Total: 17 working days** for a single engineer, plus integration buffer. Parallelization can compress this to ~12 calendar days if phases 5–7 run alongside phase 8 prep.

---

## 8. Test strategy

### 8.1 Three test layers

| Layer | What it covers | Infrastructure |
|---|---|---|
| **Unit** (`tests/mcp/test_*.py`) | Each module's pure logic — McpToolDef serde, content adaptation, error classification, config parsing, filter composition, codegen renderer | pytest only |
| **Integration** (`tests/mcp/test_bridge_integration.py`) | Full envelope flow: agent → McpBridge → FakeMcpServer → reply, including error paths and per-call meta resolution | `TestKafkaBroker` + `FakeMcpServer` |
| **E2E** (`tests/mcp/test_real_servers.py`, separate CI lane) | Actual `@modelcontextprotocol/server-everything` over stdio + a containerized HTTP MCP fixture | Docker Compose |

### 8.2 Critical test scenarios (the 10 we must pass)

1. Single-process worker round-trip — agent constructs `Agent(tools=[gmail])` with `FakeMcpServer`, makes a tool call, gets correct response.
2. Split-process round-trip — bridge worker hosts `gmail`, agent worker references `gmail` from shared module, full envelope flow.
3. `CallToolResult.is_error=True` → `RetryPromptPart` (LLM sees error, can retry).
4. `MCPError` from `call_tool` → `FailedToolCall` → `ToolExecutionError` at agent.
5. Per-call `meta=` callable is invoked with correct `ToolContext`; result placed in MCP `_meta`; exceptions surface as `FailedToolCall`.
6. Parallel tool calls to one bridge pipeline correctly (no serial dispatch).
7. `Worker._on_startup` failure during MCP `initialize` aborts boot cleanly with no zombie subprocesses.
8. `mcp.json` parse with `$VAR` substitution; unset variable raises at parse time.
9. Filter composition: `.only("search").exclude("delete").where(read_only_hint=True)` produces the correct subset.
10. Codegen `--check` exits 0 when up-to-date, non-zero on drift with diff output.

### 8.3 Test infrastructure additions

- `tests/mcp/conftest.py` — shared `FakeMcpServer` fixtures, sample `McpToolDef` lists.
- `tests/mcp/fixtures/mcp.json` — sample config for parser tests.
- `tests/mcp/fixtures/gmail_schemas_expected.py` — golden file for codegen output tests.
- Docker Compose file under `tests/mcp/docker/` for the E2E lane.

---

## 9. Risk register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| MCP Python SDK API churn (e.g. another rename like `streamablehttp_client` → `streamable_http_client` in v1.20) | Medium | Medium | Pin to `mcp>=1.20.0,<2`. Wrap SDK imports in `_session.py` so a future bump touches one file. Track upstream releases. |
| stdio environment passthrough surprise (users expect their env to propagate; MCP SDK uses allowlist by default) | Medium | Medium | Document loudly. Calfkit's default merges `os.environ`. Add `safe_env_only=True` opt-in flag for users who want the allowlist behavior. |
| Codegen output drift from MCP server upgrades | Medium | Low | `calfkit mcp codegen --check` in CI; schedules drift-detection job opens PR if upstream changed. |
| Long-running MCP tools pin worker handlers | Medium | Medium | Documented as v1 limitation. v2 implements MCP `tasks` extension support. |
| Worker crash mid-call risks duplicate MCP execution (Kafka redelivery) | Medium | High (for non-idempotent tools) | **v1 hot fix**: idempotency-key dedup cache keyed on `(tool_call_id, args_hash)` in `calfkit/mcp/_dedup.py`. Honors MCP `idempotentHint`. ~50 LOC. **Tracked for generalization** to a node-level abstraction in issue [#161](https://github.com/calf-ai/calfkit-sdk/issues/161). The MCP-local cache will migrate to the general mechanism when that lands. |
| MCP server requires `sampling` capability | Low | Medium | We don't advertise sampling/elicitation; server gets `INVALID_REQUEST` on attempted use. Surface as a startup-time check: log warning if `InitializeResult.capabilities` indicates required sampling. |
| LLM emits args with `additionalProperties: false` violations (schema-only path skips validation) | High | Low | No client-side validation in v1 (documented). MCP server rejects; LLM sees `RetryPromptPart` and adjusts. Acceptable. |

---

## 10. Release plan

### 10.1 Calfkit versioning

- v1 ships as **`calfkit 0.4.0`** — minor bump because of additive feature, no breaking changes.
- The MCP adaptor is opt-in at the API level (no behavior changes to existing nodes), but the underlying `mcp` SDK is now a **required** dependency. The factory shortcut (`from calfkit import mcp`) is re-exported at the top level so the SDK is imported eagerly when calfkit is imported; the install footprint grows by `mcp>=1.20.0` plus its transitive deps (httpx, anyio — both of which are already pulled in by existing LLM provider deps). Pure-Kafka users without MCP needs pay the disk/import cost but nothing else.
- `calfkit[mcp-codegen]` extra installs the CLI dependency (`typer`).

### 10.2 Release-please configuration

- Add `calfkit/mcp/` and `calfkit/cli/` to release-please paths.
- Tag generated CHANGELOG entries with `mcp:` prefix for the v1 release.

### 10.3 Migration / deprecation notes

- None — purely additive feature.
- Users on `0.3.x` who don't use MCP see no change.

### 10.4 Documentation deliverables

- README: top-level MCP section (10–15 lines), copy-pasteable quickstart, link to `docs/mcp-guide.md`.
- `docs/mcp-guide.md` (new): comprehensive user guide — multi-server, Pattern 1 OAuth, filtering, observability, CI drift detection.
- `docs/mcp-adaptor-design.md` & `docs/mcp-adaptor-implementation-plan.md` stay as deeper reference for contributors.

---

## 11. Open questions — all signed off

All Tier 1 and Tier 2 questions have been resolved. The decisions below are locked for v1 implementation.

### Tier 1 — pre-Phase-0 sign-offs

| # | Question | Decision | Notes |
|---|---|---|---|
| Q1 | stdio environment passthrough | **Full passthrough** (`{**os.environ, **user_env}`) | Matches Docker/subprocess defaults. `safe_env_only=True` opt-in available for users wanting MCP SDK's allowlist behavior. |
| Q2 | Idempotency-key cache | **In-process, 1hr TTL, LRU 10k entries** | Ships as a **temporary hot fix** inside `calfkit/mcp/_dedup.py`. Tracked for generalization to a node-level abstraction in issue [#161](https://github.com/calf-ai/calfkit-sdk/issues/161). The MCP-local cache will be migrated to that general mechanism when it lands. |
| Q3 | Codegen output format | **Python module** with `McpToolDef` instances + a per-server class with `.ALL` and per-tool attributes | IDE autocomplete, version-control diff-friendly. |
| Q4 | `McpServer.__iter__` with `tools=[]` | **Empty iterator + log warning** | Friendlier for users scaffolding MCP integration. |
| Q5 | `mcp.json` schemas kwarg shape | **Separate `schemas={"gmail": Gmail.ALL, ...}` kwarg on `McpServers.from_file`** | Preserves standard `mcp.json` format. |
| Q6 | CLI dependency | **`typer` via optional extra `calfkit[mcp-codegen]`** | Off the critical install path. |
| Q7 | Sanity-check behavior on declared-vs-server-tools drift | **Warn-only** | Drift is normal between MCP server upgrades; hard-failing in production is too disruptive. Strict mode is a v1.1 enhancement. |
| Q8 | CLI command structure | **`calfkit mcp codegen <name> --command "..." --output <path>`** with `--check` for drift detection | `--check` prints diff to stderr and exits non-zero. No auto-write. |
| Q9 | Generated class naming | **Capitalize server name** (`gmail` → `Gmail`) | Reads cleanest at the call site (`tools=Gmail.ALL`). Non-Python-identifier characters CamelCased. |

### Tier 2 — answered during implementation planning

| # | Question | Decision | Notes |
|---|---|---|---|
| Q10 | Reconnect backoff for MCP sessions | **Dropped** — `reconnect=` kwarg removed | No calfkit precedent for in-process reconnect. Cattle-not-pets: if MCP session dies, bridge worker fails loudly and orchestrator restarts. Aligns with calfkit's existing philosophy. |
| Q11 | HTTP transport custom SSL / proxy config | **Pass-through to `httpx`** via `httpx_client_kwargs={...}` escape hatch | Documented. |
| Q12 | MCP `roots` capability handling | **Not advertised in v1.** Servers requiring roots will fail loudly on capability negotiation. | Documented v1 limitation. |
| Q13 | Topic naming for servers with special characters | **Normalize** `.` / `-` to `_` in server-name component of topic paths. Original name preserved for LLM-facing strings. | Standard pattern used by Kubernetes labels, DNS labels, Avro schema names. |
| Q14 | Subprocess SIGTERM grace period scope | **Per-server** via `McpServer.stdio(..., shutdown_grace_seconds=N)` | Different MCP servers have different shutdown profiles; one global default would force everyone to the slowest case. |

### Tier 3 — explicitly out of scope or trivially answered

| # | Question | Decision |
|---|---|---|
| Q15 | Hot reload of generated schemas during dev | Out of v1 scope. Users restart workers manually. |
| Q16 | Concurrent codegen invocations | Documented as not concurrency-safe; users serialize in CI. |
| Q17 | `--check` exit code conventions | Standard: 0 = no drift, 1 = drift, 2 = error. |
| Q18 | Telemetry / instrumentation | Out of v1. Revisit when calfkit gets a broader observability story. |

---

## 12. Definition of done

v1 is considered shipped when:

1. **All 10 critical test scenarios** (§8.2) pass in CI.
2. **E2E test against `@modelcontextprotocol/server-everything`** passes in the separate CI lane.
3. **`examples/quickstart_mcp/`** is runnable end-to-end via the README instructions.
4. **README MCP section** is reviewed and merged.
5. **`docs/mcp-guide.md`** is reviewed and merged.
6. **Manual smoke test** against a real OAuth-protected MCP server (Gmail or GitHub) succeeds.
7. **`calfkit mcp codegen --check`** is wired into calfkit's own CI as a self-test against the example.
8. **Release notes** clearly describe the new feature and `calfkit[mcp-codegen]` extra.

---

## 13. After v1 — what comes next

| Roadmap | Doc / issue | Trigger |
|---|---|---|
| Generic node-level idempotency mechanism | [Issue #161](https://github.com/calf-ai/calfkit-sdk/issues/161) | When v1 lands — generalize the MCP-local dedup hot fix |
| v1.x MCP-over-Kafka RPC discovery | [`mcp-discovery-rpc-design.md`](./mcp-discovery-rpc-design.md) | First production user requests "no codegen" workflow |
| v1.x Strict-mode bridge tool-drift handling (Q7 strict variant) | (not yet drafted) | If users want hard-fail on declared-vs-server drift |
| v1.5 Multi-modal content passthrough | (not yet drafted) | Needs pydantic-ai `ToolReturn` extension |
| v1.5 Standalone `calfkit-mcp-bridge` container image | (not yet drafted) | If operators prefer config-driven bridge deployments |
| v2 MCP `resources/*` and `prompts/*` primitives | (not yet drafted) | When use cases emerge — currently low demand vs tools |
| v2 Server-initiated sampling (reverse-topic protocol) | (not yet drafted) | Standalone RFC; nontrivial protocol design |
| v2 MCP `tasks` extension for long-running tools | (not yet drafted) | When users hit handler-pinning issue |
| post-v1 In-flight call durability across crashes | (not yet drafted) | Calfkit-wide durability layer (broader than MCP) |

---

## 14. Sign-off checklist

Before kicking off Phase 0:

- [x] §11 open questions confirmed (all signed off; see Tier 1/2/3 tables)
- [ ] §10 release plan reviewed
- [ ] Capacity assigned: ~3 weeks single engineer (or split across two)
- [ ] CI infrastructure: real-Kafka lane available, Docker Compose for E2E
- [ ] Stakeholder review of `docs/mcp-v1-plan.md`, `docs/mcp-adaptor-design.md`, `docs/mcp-adaptor-implementation-plan.md`

---

## 15. Companion docs

| Doc | Purpose | When to read |
|---|---|---|
| [`mcp-v1-plan.md`](./mcp-v1-plan.md) | THIS DOC. Actionable v1 plan: scope, decisions, timeline. | Read first to understand v1. |
| [`mcp-phase-0-kickoff.md`](./mcp-phase-0-kickoff.md) | Day-1 executable checklist: deps, skeleton, baseline test. | Read on day 1 of Phase 0 implementation. |
| [`mcp-adaptor-design.md`](./mcp-adaptor-design.md) | Design intent, peer-SDK comparison, prior art, multi-tenancy patterns. | Read for design rationale, "why we chose X over Y." |
| [`mcp-adaptor-implementation-plan.md`](./mcp-adaptor-implementation-plan.md) | Class-level code, file-by-file changes, full test strategy. | Read when implementing each phase. |
| [`mcp-discovery-rpc-design.md`](./mcp-discovery-rpc-design.md) | v1.x roadmap entry — runtime discovery via MCP-over-Kafka. | Read when scoping v1.x. |
| [`mcp-catalog-discovery-plan.md`](./mcp-catalog-discovery-plan.md) | Archived alternative — compacted-topic discovery. | Read for historical context on why codegen was chosen. |

---

## 16. Implementation kickoff

**Status: ready to start Phase 0.**

### 16.1 Day-1 actions

See [`docs/mcp-phase-0-kickoff.md`](./mcp-phase-0-kickoff.md) — a copy-paste-runnable checklist. End-state of day 1: three commits on a feature branch, dependency + skeleton + load-bearing baseline test, ready to merge.

The **single most important Phase 0 deliverable** is `tests/mcp/test_baseline_schema_only_dispatch.py`, which empirically validates D7 (the agent loop accepts `BaseToolNodeSchema` instances via `Agent(tools=...)` without modification). If that test fails, Phase 1 cannot start and `agent.py:163-167` needs a code change first.

### 16.2 Pre-Phase-0 audit pass

A cross-document consistency audit was performed (62 findings across all planning docs). The **P0 blockers** (stale code examples that would mislead the day-1 engineer) have been fixed:

- Impl plan §3.5 Patterns 1–4 now use the correct `McpServer.stdio(...)` / `.http(...)` factories with required `tools=` kwarg
- Impl plan §3.5 Pattern 4 now uses Pattern 1 multi-tenancy (`token="$VAR"` session-static + `meta=lambda ctx: ...` per-call) rather than the rejected callable `token=`
- Impl plan §3.4 `McpServers.from_file/from_config` signatures now include the required `schemas=` kwarg
- Impl plan §15 "open questions" replaced with pointer to v1 plan §11 (all signed off)
- Impl plan version bumped to 1.3 to align with design doc

**Remaining audit cleanup (P1/P2)** — to be addressed during the relevant phase, not blocking Phase 0:

- Design doc §3.2 / §14.1 / §14.2 / §15 still carry v1.2-era language (Codegen as "non-goal", "No idempotency-key dedup", `per_call` references). Sweep during Phase 7 (docs).
- Design doc §6.4 filter annotation kwarg names should be `read_only_hint` / `destructive_hint` / etc. (matching MCP spec) — sweep in Phase 2 when `where()` is implemented.
- Helper definitions referenced but not shown (`_retry_prompt`, `_flatten_text`, `_annotation_predicate`, `build_session_headers`, etc.) — write during the corresponding implementation phase.
- Topic normalization helper (Q13) — implement in Phase 2's `McpServer._build_schema`.
- `_dedup.py`, `_codegen.py`, CLI design sketches — write at the start of Phases 3 and 5 respectively.
- `safe_env_only=` and `httpx_client_kwargs=` kwargs — add to impl plan §3.2 kwarg table during Phase 1.

### 16.3 Phase progression

| Phase | Days | Key deliverable | Blocked on |
|---|---|---|---|
| 0 — Pre-work | 1 | Deps + skeleton + **baseline test** (see kickoff doc) | (sign-off complete) |
| 1 — Core types + session | 3 | `_tool_def.py`, `_session.py`, `_adapt.py`, `_config.py` | Phase 0 baseline test passing |
| 2 — McpServer frontend | 2 | `_server.py` + `_testing.py` (`FakeMcpServer`) | Phase 1 |
| 3 — McpBridge runtime | 3 | `_bridge.py` + `_dedup.py` + integration tests | Phase 2 |
| 4 — Worker integration | 1 | `worker.py` patch + lifecycle tests | Phase 3 |
| 5 — Codegen CLI | 2 | `_codegen.py` + `cli/mcp.py` | Phase 1 (can run parallel to 2–4) |
| 6 — Public API polish | 1 | `mcp` factory + `McpServers.from_file` | Phase 4 |
| 7 — Docs + examples | 2 | README, `mcp-guide.md`, `examples/quickstart_mcp/` | Phase 6 |
| 8 — E2E + release | 2 | Real-Kafka lane, real MCP servers, release-please | Phase 7 |

Phases 5, 6, 7 can compress to ~3 calendar days if parallelised. Total: **17 working days serial / ~12 calendar days with parallelism**.

### 16.4 The first PR

Phase 0 ships as a single PR titled "Phase 0: MCP adaptor skeleton + BaseToolNodeSchema sanity test", three commits (deps, skeleton, sanity test), referencing tracking issue #158.

Tracking issue #161 (generic idempotency mechanism) is independent and can be picked up by a separate engineer at any time post-Phase 3.
