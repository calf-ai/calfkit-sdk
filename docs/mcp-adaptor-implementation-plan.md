# Calfkit MCP Adaptor — Implementation Plan

**Status:** Implementation-ready (v1 plan §11 signed off; pre-Phase-0 audit fixes applied)
**Document version:** 1.3
**Last updated:** 2026-05-30
**Companion to:** [`docs/mcp-adaptor-design.md`](./mcp-adaptor-design.md) (v1.3 — codegen schemas + Pattern 1 multi-tenancy)
**Tracking issue:** [#158](https://github.com/calf-ai/calfkit-sdk/issues/158)

This document is the implementation plan: exact module layout, class signatures, file-level changes, phased build order, test strategy, and risks. It assumes the design doc has been accepted and proceeds to the *how*.

---

## 1. Executive summary

The MCP adaptor adds a new top-level subpackage, `calfkit.mcp`, that exposes a single user-facing object — `McpServer` — which behaves polymorphically as a node group (for `Worker(nodes=[...])`) and as a tool source (for `Agent(tools=[...])`). The implementation is built on four foundations:

1. **Schema-only tool registration** (`calfkit/models/node_schema.py:29-32` + `calfkit/nodes/agent.py:163-167, :374`). The agent loop already supports tools that have no executable body — it dispatches by topic and skips validation. This is what `BaseToolNodeSchema` is for. We use it directly. **No agent code changes are required.**
2. **FastStream `on_startup` hooks** as the bridge-side MCP session setup seam. The hook fires *before* `broker.start()`, so MCP sessions can be initialized with zero races against incoming messages.
3. **The official `mcp` Python SDK** for transport, session, and content types. We do not reimplement the protocol; we wrap `ClientSession` + `stdio_client` / `streamable_http_client`.
4. **Codegen-generated schema declarations.** v1 ships a `calfkit mcp codegen` CLI that spawns an MCP server once, calls `tools/list`, and writes a Python module containing `McpToolDef` instances. Users import this module and pass the schemas to `McpServer(..., tools=[...])`. **No runtime discovery between bridge and agent workers** — schemas are static deploy-time artifacts.

The net implementation is small: ~7 new files under `calfkit/mcp/`, a small CLI (~150 LOC), a ~20-line patch to `Worker.run()`, no changes to the agent loop, no FastStream-internals access. Test coverage uses a `FakeMcpServer` that bypasses transport entirely; integration tests use `@modelcontextprotocol/server-everything`.

The adaptor implements **Pattern 1** from the design doc (§10): per-call **identity** travels in MCP's `_meta` field; **credentials** are session-static and baked into the underlying httpx client at `initialize` time. Per-call HTTP header rotation is intentionally not supported.

Runtime discovery (the MCP-over-Kafka RPC pattern) is on the roadmap as a complementary path — see [`docs/mcp-discovery-rpc-design.md`](./mcp-discovery-rpc-design.md). v1 ships codegen only.

---

## 2. Architectural overview

```
  ┌─────────────────────────────────────────────────────────────┐
  │ Step 1: Operator runs codegen (once per MCP server)         │
  │                                                              │
  │   $ calfkit mcp codegen gmail \                              │
  │       --command "npx -y @mcp/server-gmail" \                 │
  │       --output gmail_schemas.py                              │
  │                                                              │
  │   (commits gmail_schemas.py to repo)                         │
  └─────────────────────────────────────────────────────────────┘

                       ┌──────────────────────────────────────────┐
shared.py              │ from calfkit import mcp                  │
                       │ from gmail_schemas import Gmail          │
                       │                                          │
                       │ gmail = mcp("npx -y @mcp/server-gmail",  │
                       │             tools=Gmail.ALL)             │
                       └────────────┬─────────────────────────────┘
                                    │  same import in both processes
                ┌───────────────────┴───────────────────┐
                │                                       │
                ▼                                       ▼
   ┌──────────────────────┐                ┌──────────────────────┐
   │ tools_worker.py      │                │ agent_worker.py       │
   │                      │                │                       │
   │ Worker(nodes=[gmail])│                │ Agent(tools=[gmail])  │
   │  ↓ McpBridge mode    │                │ Worker(nodes=[agent]) │
   │                      │                │  ↓ tool-ref mode      │
   │  on_startup:         │                │                       │
   │    spawn subprocess  │                │  no MCP contact;      │
   │    initialize        │                │  schemas come from    │
   │    tools/list (sanity│                │  Gmail.ALL import.    │
   │      check vs decl)  │                │                       │
   │    register N        │                │  Agent routes         │
   │    subscribers       │                │  Call[State] to       │
   │                      │                │  mcp.gmail.<tool>.    │
   │  steady state:       │                │  input                │
   │    handler →         │                │                       │
   │    session.call_tool │                │                       │
   └──────────────────────┘                └──────────────────────┘
              ▲                                       │
              │  mcp.gmail.search.input  ◄────────────┘
              │  mcp.gmail.search.output ────────────► tap with @consumer
              │
       Kafka (one topic per MCP tool)
```

Key invariants:

- **One MCP session per `McpServer` per process.** Multiple workers may each spawn their own session; they do not share one cross-process.
- **One subscriber per MCP tool.** All subscribers in a single bridge process share the same `ClientSession`. Concurrency comes from JSON-RPC request-ID multiplexing on the session.
- **Topic naming is deterministic.** Agent worker and tool worker derive identical `mcp.<server>.<tool>.input/output` strings from the `McpServer` object's `name` plus the discovered tool name — no coordination protocol required.

---

## 3. Public API specification

All types live under `calfkit.mcp`. The module-level entry point is re-exported from `calfkit` as `mcp` for the canonical `from calfkit import mcp` import.

### 3.1 The `mcp` entry point

`mcp` is a callable + namespace. Three call shapes:

```python
# Auto-detect transport from first arg
mcp(cmd_or_url: str | list[str], /, *, name: str | None = None, **kwargs) -> McpServer

# Explicit stdio
mcp.stdio(command: str, *args: str, name: str | None = None, **kwargs) -> McpServer

# Explicit Streamable HTTP
mcp.http(url: str, *, name: str | None = None, **kwargs) -> McpServer
```

Auto-detection rule for the bare-call form:

- If the string starts with `http://` or `https://` → HTTP transport.
- If a list is passed, or a string containing whitespace → stdio transport (command + args).
- Otherwise stdio with no args.

`name=` defaults to the basename of the command (e.g. `"server-gmail"` from the `@modelcontextprotocol/server-gmail` package) or the URL host (e.g. `"api.github.com"`). Users override to disambiguate.

### 3.2 `McpServer` — full kwargs reference

The complete kwarg set (every option is keyword-only after the first positional):

| Kwarg | Type | Default | Applies to | Description |
|---|---|---|---|---|
| `tools` | `list[McpToolDef]` | (required) | both | Tool schemas. Generated by `calfkit mcp codegen` (recommended) or declared inline. |
| `name` | `str \| None` | inferred | both | Server identifier used in topic names (`mcp.<name>.<tool>.input`) and logs. |
| `env` | `dict[str, str] \| None` | `None` | stdio | Extra env vars merged into the subprocess environment. See §13 for the credential-passthrough caveat. |
| `cwd` | `str \| Path \| None` | `None` | stdio | Working directory for the subprocess. |
| `shutdown_grace_seconds` | `float` | `5.0` | stdio | Wait this long after closing stdin before SIGTERM. |
| `token` | `str \| None` | `None` | HTTP | Bearer token; session-static. `$VAR` substitution applied at construction. Sugar for `headers={"Authorization": f"Bearer {value}"}`. |
| `headers` | `dict[str, str] \| None` | `None` | HTTP | Custom HTTP headers; session-static. `$VAR` substitution applied at construction. Merged with `token=` if both are set. |
| `meta` | `dict[str, Any] \| Callable[[ToolContext], dict \| Awaitable[dict]] \| None` | `None` | both | MCP `_meta` field on tool calls. **Per-call** — constant dict or callable. The callable receives the same `ToolContext` native tools see, and its return value is placed in the JSON-RPC message body. |
| `read_timeout_seconds` | `float` | `120.0` | both | Per-call MCP read timeout. |
| `connect_timeout_seconds` | `float` | `30.0` | both | Initialize handshake timeout. |
| `client_info_version` | `str \| None` | calfkit version | both | The `Implementation.version` advertised in `initialize`. |

*Note: no `reconnect=` kwarg in v1.* If the MCP session dies (subprocess crashes, HTTP server unreachable), the bridge worker fails loudly and the orchestrator (Kubernetes, systemd) restarts it. This matches calfkit's existing philosophy — no in-process self-healing.

**Note on filters/renames.** The chainable filter methods (`.only()`, `.exclude()`, `.where()`, `.prefix()`, `.rename()`) still apply but now operate on the user-declared `tools=` list rather than runtime-discovered tools. They're equivalent in semantics — just earlier in the pipeline.

### 3.3 `McpServer` instance methods

```python
class McpServer:
    # Chainable filter/transform — each returns a new McpServer with the
    # filter applied. Originals are immutable; chaining composes by AND.
    def only(self, *tool_names: str) -> Self: ...
    def exclude(self, *tool_names: str) -> Self: ...
    def where(self, *,
              read_only_hint: bool | None = None,
              destructive_hint: bool | None = None,
              idempotent_hint: bool | None = None,
              open_world_hint: bool | None = None,
              predicate: Callable[[ToolMeta], bool] | None = None) -> Self: ...
    def prefix(self, value: str) -> Self: ...
    def rename(self, mapping: dict[str, str]) -> Self: ...

    # Polymorphic protocol — consumed by Worker / Agent at registration time.
    def __iter__(self) -> Iterator[BaseToolNodeSchema]: ...

    # Cache control
    # (cache_to removed — runtime schema discovery is not a v1 feature)

    # Discovery — typically called by Worker.run(), not by users.
    # (No runtime discovery in v1 — tools come from the user-supplied `tools=` list.)
```

### 3.4 `McpServers` — bulk constructor

```python
class McpServers(Mapping[str, McpServer]):
    @classmethod
    def from_config(
        cls,
        config: dict | str | Path,
        *,
        schemas: dict[str, list["McpToolDef"]],
    ) -> Self:
        """Accept either an mcp.json envelope ({"mcpServers": {...}})
        or the bare inner dict. Strings/paths are treated as file paths.

        `schemas` is required: maps server name (matching the mcp.json key)
        to a list of McpToolDef instances (typically from a codegen-generated
        module). Each McpServer gets the schemas associated with its name.
        Server names present in `config` but missing from `schemas` raise
        McpConfigError at parse time.
        """

    @classmethod
    def from_file(
        cls,
        path: str | Path,
        *,
        schemas: dict[str, list["McpToolDef"]],
    ) -> Self: ...
```

The dict shape conforms to the de facto `mcp.json` standard:

```json
{
  "mcpServers": {
    "gmail":  {"command": "npx", "args": ["-y", "@mcp/server-gmail"], "env": {...}},
    "github": {"type": "http", "url": "https://api.github.com/mcp",
                "headers": {"Authorization": "Bearer $GH_TOKEN"}}
  }
}
```

### 3.5 Usage examples (canonical README form)

```python
# Pattern 1 — single file, dev setup
# Prerequisite: run `calfkit mcp codegen gmail --command "..." --output gmail_schemas.py`
from calfkit import Agent, Client, Worker
from calfkit.mcp import McpServer
from gmail_schemas import Gmail

gmail = McpServer.stdio(
    "npx", "-y", "@modelcontextprotocol/server-gmail",
    tools=Gmail.ALL,
)

agent = Agent(
    "scribe",
    subscribe_topics="scribe.input",
    model_client=...,
    tools=[gmail],
)

worker = Worker(Client.connect("..."), nodes=[gmail, agent])
asyncio.run(worker.run())
```

```python
# Pattern 2 — split deployment, shared import
# shared.py
from calfkit.mcp import McpServer
from gmail_schemas import Gmail
from github_schemas import Github

gmail  = McpServer.stdio(
    "npx", "-y", "@modelcontextprotocol/server-gmail",
    env={"OAUTH_TOKEN": os.environ["GMAIL_OAUTH"]},
    tools=Gmail.ALL,
)
github = McpServer.http(
    "https://api.github.com/mcp",
    token="$GITHUB_TOKEN",
    tools=Github.ALL,
)

# tools_worker.py
from shared import gmail, github
asyncio.run(Worker(Client.connect("..."), nodes=[gmail, github]).run())

# agent_worker.py
from shared import gmail, github
agent = Agent("scribe", subscribe_topics="scribe.input",
              tools=[gmail, github.where(read_only_hint=True)],
              model_client=...)
asyncio.run(Worker(Client.connect("..."), nodes=[agent]).run())
```

```python
# Pattern 3 — mcp.json drop-in (schemas supplied separately via `schemas=`)
from calfkit import Agent, Client, Worker
from calfkit.mcp import McpServers
from gmail_schemas import Gmail
from github_schemas import Github
from postgres_schemas import Postgres

servers = McpServers.from_file("./mcp.json", schemas={
    "gmail": Gmail.ALL,
    "github": Github.ALL,
    "postgres": Postgres.ALL,
})
agent   = Agent("scribe", tools=[*servers.values()], ...)
worker  = Worker(Client.connect("..."), nodes=[*servers.values(), agent])
```

```python
# Pattern 4 — per-tenant identity (Pattern 1 multi-tenancy)
# Credentials are session-static; per-call IDENTITY (not credentials) travels in _meta.
# See design doc §10 for the protocol-level reasoning.
from calfkit.mcp import McpServer
from gmail_schemas import Gmail

gmail = McpServer.http(
    "https://gmail-mcp.acme.com/mcp",
    token="$CALFKIT_SERVICE_TOKEN",                     # session-static auth to MCP server
    meta=lambda ctx: {"user_id": ctx.deps["user_id"]},  # per-call user identity
    tools=Gmail.ALL,
)
```

```python
# Pattern 5 — observability tap, free with topic-per-tool
@consumer(subscribe_topics="mcp.gmail.send.output")
async def audit_sent_email(result):
    await audit_log.write(result)
```

---

## 4. Module structure

New package layout under `calfkit/`:

```
calfkit/
├── __init__.py                 (modified — re-export `mcp`)
└── mcp/
    ├── __init__.py             (public surface — mcp factory, McpServer, McpServers, McpToolDef)
    ├── _server.py              (McpServer class + factory functions)
    ├── _bridge.py              (McpBridge — runtime tool dispatch nodes)
    ├── _session.py             (McpSession — wraps mcp.ClientSession + transport lifecycle)
    ├── _config.py              (mcp.json parsing + env substitution)
    ├── _adapt.py               (MCP types ↔ calfkit types: ToolDefinition, ToolReturn, errors)
    ├── _tool_def.py            (McpToolDef dataclass — the schema-bearing type users import)
    ├── _codegen.py             (CLI codegen: spawn MCP, list_tools, render Python module)
    ├── _testing.py             (FakeMcpServer for tests)
    └── exceptions.py           (MCP-specific exceptions surfaced to users)
```

Plus a CLI entry point:

```
calfkit/
└── cli/
    └── mcp.py                  (typer/click subcommand registering `calfkit mcp codegen`)
```

### 4.1 Existing files modified

| File | Lines | Change |
|---|---|---|
| `calfkit/__init__.py` | new export | Add `mcp` (from `.mcp`) and `__all__` entry. |
| `calfkit/worker/worker.py` | 13-71 | Add `mcp_servers` discovery from `nodes`, `on_startup`/`on_shutdown` hooks, pass to `FastStream(...)`. |
| `calfkit/nodes/agent.py` | (none) | **No changes.** The schema-only path already exists at line 163-167 and 374. |

### 4.2 New dependencies

- `mcp >= 1.20.0` (the official Python SDK; the streamable_http_client signature stabilised at this version).
- `httpx` (already a transitive dependency via the LLM providers; pin to a version compatible with `mcp`).
- `jsonschema` (optional, dev-extra; for opt-in client-side schema validation in v1.5+).

---

## 5. Class design

### 5.1 `McpServer` — frontend object

```python
# calfkit/mcp/_server.py

from __future__ import annotations
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Iterator, Self
from pathlib import Path

from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.models.node_schema import BaseToolNodeSchema
from calfkit.models.tool_context import ToolContext

from ._session import McpTransport, StdioTransport, HttpTransport, McpSession
from ._bridge import McpBridge


@dataclass(frozen=True)
class _Filters:
    only: frozenset[str] | None = None
    exclude: frozenset[str] = frozenset()
    annotation_predicates: tuple[Callable[["McpToolMeta"], bool], ...] = ()
    custom_predicates: tuple[Callable[["McpToolMeta"], bool], ...] = ()


@dataclass(frozen=True)
class _Renames:
    prefix: str | None = None
    explicit: dict[str, str] = field(default_factory=dict)


class McpServer:
    """Polymorphic MCP server reference.

    Behaves as a node-group when passed to `Worker(nodes=[...])`:
    the bridge process spawns the subprocess / opens the HTTP session and
    registers one Kafka subscriber per declared tool.

    Behaves as a schema-only tool source when passed to `Agent(tools=[...])`:
    yields BaseToolNodeSchema instances built from the user-declared `tools=`
    list (typically imported from a codegen-generated module). No MCP contact
    from the agent worker.
    """

    def __init__(
        self,
        transport: McpTransport,
        *,
        tools: list[McpToolDef],
        name: str | None = None,
        token: str | None = None,
        headers: dict[str, str] | None = None,
        meta: dict | Callable | None = None,
        read_timeout_seconds: float = 120.0,
        connect_timeout_seconds: float = 30.0,
        # reconnect dropped — bridge worker fails loudly on session loss;
        # orchestrator (k8s/systemd) restarts it. No in-process self-healing.
        client_info_version: str | None = None,
        # Filtering / renaming applied to the declared tools:
        filters: _Filters = _Filters(),
        renames: _Renames = _Renames(),
    ) -> None:
        self._transport = transport
        self._name = name or transport.infer_name()
        self._tools = tools                    # codegen-supplied or inline
        self._token = token
        self._headers = headers
        self._meta = meta
        self._read_timeout = read_timeout_seconds
        self._connect_timeout = connect_timeout_seconds
        # (no reconnect state — see comment above)
        self._client_version = client_info_version
        self._filters = filters
        self._renames = renames

        self._session: McpSession | None = None  # set only in bridge-mode workers
        self._initialize_result = None           # populated at bridge startup

    # ----- Chainable transformations (immutable) -----

    def only(self, *names: str) -> McpServer:
        return self._copy(filters=replace(self._filters, only=frozenset(names)))

    def exclude(self, *names: str) -> McpServer:
        return self._copy(filters=replace(
            self._filters,
            exclude=self._filters.exclude | frozenset(names),
        ))

    def where(self, *, predicate=None, **annotation_kwargs) -> McpServer:
        new_ann = self._filters.annotation_predicates + tuple(
            _annotation_predicate(k, v) for k, v in annotation_kwargs.items()
        )
        new_custom = (
            self._filters.custom_predicates + (predicate,)
            if predicate is not None else self._filters.custom_predicates
        )
        return self._copy(filters=replace(
            self._filters,
            annotation_predicates=new_ann,
            custom_predicates=new_custom,
        ))

    def prefix(self, value: str) -> McpServer:
        return self._copy(renames=replace(self._renames, prefix=value))

    def rename(self, mapping: dict[str, str]) -> McpServer:
        return self._copy(renames=replace(
            self._renames,
            explicit={**self._renames.explicit, **mapping},
        ))

    def _copy(self, **changes) -> McpServer:
        new = object.__new__(McpServer)
        new.__dict__.update(self.__dict__)
        new.__dict__.update(changes)
        new._session = None  # filtered/renamed copy gets its own session if deployed
        return new

    # ----- Polymorphic iter — yields BaseToolNodeSchema -----

    def __iter__(self) -> Iterator[BaseToolNodeSchema]:
        """Yield one BaseToolNodeSchema per declared tool, with filters applied.

        Trivially synchronous — schemas come from `self._tools` (codegen output
        or inline), not from a running MCP server. Works in both sync and async
        contexts; safe to call at module import time.
        """
        for tool_def in self._apply_filters(self._tools):
            yield self._build_schema(tool_def)

    def _build_schema(self, tool: McpToolDef) -> BaseToolNodeSchema:
        agent_facing_name = self._rename(tool.name)
        tool_definition = ToolDefinition(
            name=agent_facing_name,
            description=tool.description,
            parameters_json_schema=tool.input_schema,
            metadata={
                "mcp_server": self._name,
                "mcp_tool_name": tool.name,            # original, used for dispatch
                "annotations": tool.annotations,
                "output_schema": tool.output_schema,
                "_meta": tool.meta,
                "source": "mcp",
            },
        )
        return BaseToolNodeSchema(
            node_id=f"mcp_{self._name}_{tool.name}",
            subscribe_topics=[f"mcp.{self._name}.{tool.name}.input"],
            publish_topic=f"mcp.{self._name}.{tool.name}.output",
            tool_schema=tool_definition,
        )

    def _rename(self, original: str) -> str:
        if original in self._renames.explicit:
            return self._renames.explicit[original]
        if self._renames.prefix:
            return f"{self._renames.prefix}.{original}"
        return original

    def _apply_filters(self, metas: list[McpToolMeta]) -> Iterator[McpToolMeta]:
        for m in metas:
            if self._filters.only is not None and m.name not in self._filters.only:
                continue
            if m.name in self._filters.exclude:
                continue
            if not all(p(m) for p in self._filters.annotation_predicates):
                continue
            if not all(p(m) for p in self._filters.custom_predicates):
                continue
            yield m

    # ----- Bridge session lifecycle — called by Worker._on_startup -----

    async def _open_bridge_session(self) -> None:
        """Spawn the MCP session and validate declared tools against the server.

        Called only on bridge workers (workers that have this McpServer in
        their nodes= list). Agent-only workers skip this entirely — they
        already have schemas from self._tools.
        """
        self._session = await McpSession.open(
            transport=self._transport,
            client_info_name="calfkit",
            client_info_version=self._client_version or _calfkit_version(),
            connect_timeout=self._connect_timeout,
            read_timeout=self._read_timeout,
        )
        self._initialize_result = await self._session.initialize()

        # Sanity check: ensure declared tools actually exist on the MCP server.
        server_tools = {t.name for t in await self._session.list_tools()}
        declared_tools = {t.name for t in self._tools}
        missing = declared_tools - server_tools
        extra = server_tools - declared_tools
        if missing:
            logger.warning(
                "McpServer %r: declared tools missing on server: %s. "
                "These will register subscribers that will fail on dispatch. "
                "Run `calfkit mcp codegen` to refresh.",
                self._name, sorted(missing),
            )
        if extra:
            logger.info(
                "McpServer %r: server exposes %d tools not in declarations: %s. "
                "These will not be dispatchable. Run `calfkit mcp codegen` to add them.",
                self._name, len(extra), sorted(extra),
            )
```

### 5.2 `McpBridge` — the deployable node group

`McpBridge` is what `Worker` registers. It is **not** user-facing; users only see `McpServer`. The Worker detects `McpServer` instances in its `nodes` list and converts them to a single `McpBridge` per server, plus N per-tool subscriber bindings.

```python
# calfkit/mcp/_bridge.py

from typing import Any
from faststream.kafka.annotations import KafkaBroker as BrokerAnnotation
from faststream import Context

from calfkit._protocol import NodeKind
from calfkit._vendor.pydantic_ai.exceptions import ModelRetry
from calfkit.models import Call, NodeResult, ReturnCall, SessionRunContext, State, ToolContext
from calfkit.models.actions import Silent
from calfkit.models.state import FailedToolCall
from calfkit.nodes.base import BaseNodeDef
from calfkit.nodes.tool import _safe_exc_message

from ._adapt import adapt_call_tool_result, classify_mcp_error
from ._session import McpSession


class McpBridge(BaseNodeDef):
    """Internal: per-MCP-tool subscriber that proxies to a shared McpSession.

    One instance per (server, tool) pair. All instances for a given server
    share the same self._session reference (populated at Worker._on_startup).
    """

    _node_kind: ClassVar[NodeKind] = "tool"  # advertised on the wire as a tool

    def __init__(
        self,
        server: "McpServer",
        tool_meta: "McpToolMeta",
    ) -> None:
        self._server = server
        self._tool_meta = tool_meta
        super().__init__(
            node_id=f"mcp_{server._name}_{tool_meta.name}",
            subscribe_topics=[f"mcp.{server._name}.{tool_meta.name}.input"],
            publish_topic=f"mcp.{server._name}.{tool_meta.name}.output",
        )

    async def run(self, ctx: SessionRunContext, tool_call_id: str) -> NodeResult[State]:
        tool_call_part = ctx.state.get_tool_call(tool_call_id)
        if tool_call_part is None:
            return Silent()

        tool_call_ctx = ToolContext(
            deps=ctx.deps,
            agent_name=ctx.emitter_node_id,
            tool_call_id=tool_call_part.tool_call_id,
            tool_name=tool_call_part.tool_name,
            messages=ctx.state.message_history,
            run_id=ctx.deps.correlation_id,
        )

        try:
            args = tool_call_part.args_as_dict()
            session = self._server._session
            assert session is not None, "bridge run() before Worker._on_startup completed"

            # Per-call meta resolution (Pattern 1: identity in _meta)
            meta = await _resolve_meta(self._server, tool_call_ctx)

            mcp_result = await session.call_tool(
                self._tool_meta.name,
                args,
                read_timeout_seconds=self._server._read_timeout,
                meta=meta,
            )
        except ModelRetry as e:
            # Bridge or per-call hook explicitly requested a retry.
            ctx.state.add_tool_result(
                tool_call_part.tool_call_id,
                _retry_prompt(e.message, tool_call_part),
            )
            return ReturnCall[State](state=ctx.state)
        except Exception as e:
            # Transport-level: connection lost, parse error, JSON-RPC error, timeout.
            # Map to FailedToolCall (operator-visible, NOT LLM-retryable).
            failure = classify_mcp_error(
                e,
                tool_name=tool_call_part.tool_name,
                tool_call_id=tool_call_part.tool_call_id,
            )
            ctx.state.add_tool_result(tool_call_part.tool_call_id, failure)
            return ReturnCall[State](state=ctx.state)

        # Tool-semantic error: result.is_error == True → RetryPromptPart
        if mcp_result.is_error:
            ctx.state.add_tool_result(
                tool_call_part.tool_call_id,
                _retry_prompt(
                    _flatten_text(mcp_result.content),
                    tool_call_part,
                ),
            )
            return ReturnCall[State](state=ctx.state)

        # Happy path: adapt content/structured to ToolReturn
        tool_return = adapt_call_tool_result(
            mcp_result,
            tool_call_id=tool_call_part.tool_call_id,
        )
        ctx.state.add_tool_result(tool_call_part.tool_call_id, tool_return)
        return ReturnCall[State](state=ctx.state)
```

Why subclass `BaseNodeDef` instead of `BaseToolNodeDef`:

- `BaseToolNodeDef` requires a `_tool: Tool` field used by `validate_call_args` (`nodes/tool.py:47-59`).
- We don't want client-side schema validation in v1 (skip-then-let-MCP-server-validate path; see design §8).
- The agent only calls `validate_call_args` if `isinstance(tool_node, BaseToolNodeDef)` (`agent.py:374`); on a `BaseNodeDef` subclass it skips this — same behavior as `BaseToolNodeSchema`.
- The `tool_schema` for advertising to the LLM is held by `BaseToolNodeSchema` instances yielded by `McpServer.__iter__`, *not* by `McpBridge`. The bridge is purely the wire-side handler.

This split is intentional: the agent worker registers `BaseToolNodeSchema` instances (from `McpServer.__iter__`); the tool worker registers `McpBridge` instances (from the Worker's introspection of `McpServer` in `nodes=`). Both reference the same deterministic topic, so the agent's `Call[State](mcp.gmail.search.input, ...)` lands on the bridge.

### 5.3 `McpSession` — transport + session wrapper

```python
# calfkit/mcp/_session.py

import asyncio
import os
from contextlib import AsyncExitStack
from typing import Any
import httpx
from mcp import ClientSession, StdioServerParameters
from mcp import types as mcp_types
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client


class McpTransport:
    """Abstract base. Concrete: StdioTransport, HttpTransport."""
    def infer_name(self) -> str: ...


class StdioTransport(McpTransport):
    def __init__(self, command: str, args: list[str], env: dict | None,
                 cwd: str | None, shutdown_grace_seconds: float):
        ...
    def infer_name(self) -> str:
        # Best-effort: last meaningful arg, stripped of @scope/ prefix
        return _basename_from_command(self.command, self.args)


class HttpTransport(McpTransport):
    def __init__(self, url: str):
        self.url = url
    def infer_name(self) -> str:
        return httpx.URL(self.url).host or "mcp"


class McpSession:
    """Owns one MCP ClientSession + its transport context.

    Reconnect logic and the Python SDK's environment passthrough quirk
    live here. HTTP credentials are baked into the httpx client at
    construction time (Pattern 1 from design doc §10).
    """

    def __init__(self, transport: McpTransport, **session_kwargs):
        self._transport = transport
        self._session_kwargs = session_kwargs
        self._stack = AsyncExitStack()
        self._session: ClientSession | None = None

    @classmethod
    async def open(cls, transport: McpTransport, **kwargs) -> "McpSession":
        self = cls(transport, **kwargs)
        await self._connect()
        return self

    async def _connect(self) -> None:
        if isinstance(self._transport, StdioTransport):
            params = StdioServerParameters(
                command=self._transport.command,
                args=self._transport.args,
                # CRITICAL: mcp's stdio_client uses get_default_environment() which is
                # an allowlist. We must explicitly merge calfkit's process env with
                # the user-supplied env to pass credentials through.
                env={**os.environ, **(self._transport.env or {})},
                cwd=self._transport.cwd,
            )
            read, write = await self._stack.enter_async_context(stdio_client(params))
        else:
            assert isinstance(self._transport, HttpTransport)
            # Build the httpx.AsyncClient with session-static headers baked in.
            # streamable_http_client v1.20+ has no `headers=` kwarg; the
            # client's headers are the authoritative source for every request.
            # Per Pattern 1, these headers do NOT change after construction.
            client = await self._stack.enter_async_context(
                httpx.AsyncClient(
                    headers=self._transport.build_session_headers(),
                    timeout=httpx.Timeout(30.0, connect=10.0),
                )
            )
            read, write = await self._stack.enter_async_context(
                streamable_http_client(self._transport.url, http_client=client)
            )

        self._session = await self._stack.enter_async_context(
            ClientSession(
                read, write,
                read_timeout_seconds=self._session_kwargs["read_timeout"],
                client_info=mcp_types.Implementation(
                    name=self._session_kwargs["client_info_name"],
                    version=self._session_kwargs["client_info_version"],
                ),
                # NOT passing sampling/elicitation/roots callbacks → capabilities
                # not advertised, which is correct for v1.
            )
        )

    async def initialize(self) -> mcp_types.InitializeResult:
        assert self._session is not None
        return await self._session.initialize()

    async def list_tools(self) -> list[mcp_types.Tool]:
        assert self._session is not None
        all_tools: list[mcp_types.Tool] = []
        cursor: str | None = None
        while True:
            params = (
                mcp_types.PaginatedRequestParams(cursor=cursor)
                if cursor is not None else None
            )
            result = await self._session.list_tools(params=params)
            all_tools.extend(result.tools)
            if not result.next_cursor:
                break
            cursor = result.next_cursor
        return all_tools

    async def call_tool(
        self, name: str, args: dict, *,
        read_timeout_seconds: float | None = None,
        meta: dict | None = None,
    ) -> mcp_types.CallToolResult:
        # Pure delegation. `meta` rides in the JSON-RPC message body via
        # the SDK's `meta=` kwarg — race-free because the data is frozen
        # into the message bytes inside our task before any await.
        assert self._session is not None
        return await self._session.call_tool(
            name, args,
            read_timeout_seconds=read_timeout_seconds,
            meta=meta,
        )

    async def aclose(self) -> None:
        await self._stack.aclose()
```

Per-call HTTP header injection is intentionally absent. The design doc §10 explains the protocol-level reason (MCP binds credentials to sessions, not calls) and the async-coherence trap that any in-process workaround would have to navigate. Session-static `token=` / `headers=` are baked into the httpx client at construction. Per-call identity travels in the `meta=` kwarg of `call_tool`, which the SDK serializes into the JSON-RPC message body before any await — race-free with no shared-state mutation.

### 5.4 `McpToolMeta` — discovery output

```python
# calfkit/mcp/_session.py (continued)

@dataclass(frozen=True)
class McpToolMeta:
    """Snapshot of one tool from MCP tools/list. Pure data, serializable."""
    name: str
    description: str | None
    input_schema: dict[str, Any]
    output_schema: dict[str, Any] | None
    annotations: dict[str, Any] | None  # serialized ToolAnnotations
    meta: dict[str, Any] | None         # MCP _meta

    @property
    def read_only(self) -> bool:
        return bool((self.annotations or {}).get("read_only_hint", False))

    @property
    def destructive(self) -> bool:
        # MCP spec: destructiveHint defaults to TRUE
        ann = self.annotations or {}
        return bool(ann.get("destructive_hint", True))

    @property
    def idempotent(self) -> bool:
        return bool((self.annotations or {}).get("idempotent_hint", False))

    @property
    def open_world(self) -> bool:
        # MCP spec: openWorldHint defaults to TRUE
        ann = self.annotations or {}
        return bool(ann.get("open_world_hint", True))
```

---

## 6. Worker lifecycle integration

### 6.1 Worker patch — file changes

**File:** `/Users/ryan/Projects/calf-sdk/calfkit/worker/worker.py`

Current `__init__` (lines 13-28) adds no MCP-specific state. The new logic detects `McpServer` instances in `self._nodes` and segregates them.

Diff sketch:

```python
# calfkit/worker/worker.py
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from faststream import FastStream, ContextRepo

from calfkit.client import Client
from calfkit.nodes import BaseNodeDef
from calfkit.mcp._server import McpServer
from calfkit.mcp._bridge import McpBridge


class Worker:
    def __init__(self, client, nodes=None, max_workers=1, group_id=None,
                 extra_publish_kwargs={}, extra_subscribe_kwargs={}):
        self._client = client
        self._max_workers = max_workers
        self._group_id = group_id
        self._extra_publish_kwargs = extra_publish_kwargs
        self._extra_subscribe_kwargs = extra_subscribe_kwargs
        self._prepared = False

        # Segregate MCP servers from regular nodes.
        # MCP servers expand into McpBridge instances after _on_startup.
        self._mcp_servers: list[McpServer] = []
        self._nodes: list[BaseNodeDef] = []
        for node in (nodes or []):
            if isinstance(node, McpServer):
                self._mcp_servers.append(node)
            else:
                self._nodes.append(node)
        self._mcp_bridges: list[McpBridge] = []  # populated by _on_startup

    def add_nodes(self, *nodes: BaseNodeDef) -> None:
        for node in nodes:
            if isinstance(node, McpServer):
                self._mcp_servers.append(node)
            else:
                self._nodes.append(node)

    async def _on_startup(self) -> None:
        """FastStream startup hook — runs before broker.start().

        - For each McpServer in self._mcp_servers (bridge-mode):
          spawn MCP subprocess / open HTTP session, initialize, sanity-check
          declared tools against the server's tools/list, register McpBridge
          per declared tool.
        - For agent-only McpServer references (in Agent.tools but NOT in
          self._mcp_servers): nothing needed — schemas are already in
          server._tools (from the user-supplied codegen module).
        """
        for server in self._mcp_servers:
            await server._open_bridge_session()
            for tool_def in server._apply_filters(server._tools):
                self._mcp_bridges.append(McpBridge(server, tool_def))

        # Register handlers for both regular nodes AND McpBridges.
        self._register_handlers_now()

    def _collect_agent_only_servers(self) -> list[McpServer]:
        seen_in_nodes = {id(s) for s in self._mcp_servers}
        collected: dict[int, McpServer] = {}
        for node in self._nodes:
            tools = getattr(node, "tools", None) or []
            for t in tools:
                if isinstance(t, McpServer) and id(t) not in seen_in_nodes:
                    collected.setdefault(id(t), t)
        return list(collected.values())

    def _register_handlers_now(self) -> None:
        """Subscriber registration — was previously the body of register_handlers."""
        if self._prepared:
            return
        all_nodes: list[BaseNodeDef] = [*self._nodes, *self._mcp_bridges]
        for node in all_nodes:
            group_id = self._group_id or node.name
            topics = list(dict.fromkeys([*node.subscribe_topics, node._return_topic]))
            subscriber = self._client._connection.subscriber(
                *topics,
                group_id=group_id,
                max_workers=self._max_workers,
                **self._extra_subscribe_kwargs,
            )
            handler = subscriber(node.handler)
            if node.publish_topic:
                self._client._connection.publisher(
                    node.publish_topic, **self._extra_publish_kwargs
                )(handler)
        self._prepared = True

    async def _on_shutdown(self) -> None:
        """Tear down MCP sessions. Broker still up; in-flight handlers can finish."""
        for server in self._mcp_servers:
            if server._session is not None:
                await server._session.aclose()
                server._session = None

    async def run(self, **extra_run_args: Any) -> None:
        logger.info("worker starting with %d regular node(s), %d MCP server(s)",
                    len(self._nodes), len(self._mcp_servers))

        app = FastStream(
            self._client._connection,
            on_startup=[self._on_startup],
            on_shutdown=[self._on_shutdown],
        )
        await app.run(**extra_run_args)
```

The critical change is dropping the eager `register_handlers()` call from `run()` and moving it into `_on_startup` so it executes *after* MCP discovery (otherwise we'd register subscribers for the regular nodes before MCP bridges exist).

### 6.2 Why `on_startup` and not `lifespan`

FastStream lifespans wrap the entire app — including the keep-alive loop. The startup hook fires before `broker.start()` (subscribers go live) and runs to completion before any message can be delivered. This is precisely the window we need.

We could equivalently use a lifespan asynccontextmanager — they're symmetric. `on_startup` reads more naturally as "boot this stuff" in the `Worker` class shape; switching to lifespan is a one-line refactor if a future version needs to wrap shutdown more tightly.

### 6.3 Cancellation safety

`_on_startup` must catch `BaseException` (not just `Exception`) so that a SIGTERM during boot doesn't orphan subprocesses:

```python
async def _on_startup(self) -> None:
    spawned: list[McpServer] = []
    try:
        for server in self._mcp_servers:
            await server._open_bridge_session()
            spawned.append(server)
            for tool_def in server._apply_filters(server._tools):
                self._mcp_bridges.append(McpBridge(server, tool_def))
        # Agent-only McpServer references need no startup action —
        # schemas are already in server._tools from the codegen module.
        self._register_handlers_now()
    except BaseException:
        # Cancellation, OSError, anything — close what we already opened.
        for s in spawned:
            if s._session is not None:
                try:
                    await s._session.aclose()
                except Exception:
                    pass
                s._session = None
        raise
```

This handles `asyncio.CancelledError` and `KeyboardInterrupt` too — both of which inherit from `BaseException`, not `Exception`.

---

## 7. Wire protocol

### 7.1 Topic naming

| Topic | Direction | Notes |
|---|---|---|
| `mcp.<server>.<tool>.input` | agent → bridge | one per (server, tool). Subscribed by `McpBridge`. |
| `mcp.<server>.<tool>.output` | bridge → consumers | publish_topic of the bridge; observability tap point. |
| `mcp_<server>_<tool>.private.return` | framework → bridge | the bridge's `_return_topic`, per existing convention (`nodes/base.py:301-324`). Only relevant if the bridge itself emits `Call`s (it doesn't in v1). |

Names use the **MCP-server-exported** tool name, *not* the renamed agent-facing name. Rename/prefix changes affect only the `tool_schema.name` the LLM sees. This decouples wire identity from display identity — a tool renamed `gmail.search` → `inbox_search` still uses `mcp.gmail.search.input` on the wire, so renaming on one side of a deployment doesn't require coordination with the other.

### 7.2 Envelope flow

Identical to native tools — calfkit's `Call[State]` → `ReturnCall[State]` dance carries through unchanged:

```
Agent (run.py:269-281):
  return Call[State](
      tools_registry["gmail.search"].subscribe_topics[0],   # "mcp.gmail.search.input"
      ctx.state,
      target_tool_call.tool_call_id,
  )

  ↓ Kafka envelope, key=correlation_id

McpBridge.run(ctx, tool_call_id):
  args = ctx.state.get_tool_call(tool_call_id).args_as_dict()
  result = await session.call_tool("search", args, ...)
  ctx.state.add_tool_result(tool_call_id, _adapt(result))
  return ReturnCall[State](state=ctx.state)

  ↓ Kafka envelope, target=callback_topic from frame

Agent (run.py:283-296):
  ... continues agent loop with tool_results populated
```

### 7.3 Startup flow (schemas come from codegen, not discovery)

```
Operator runs once per MCP server (committed to repo):
  $ calfkit mcp codegen gmail --command "npx ..." --output gmail_schemas.py

At Worker.run():
  └─ FastStream.run()
     └─ _on_startup()
        ├─ For each McpServer in nodes (bridge mode):
        │     • spawn / connect MCP session
        │     • initialize
        │     • tools/list (sanity check vs declared tools; log drift)
        │     • register N McpBridge subscribers (one per declared tool)
        │     • keep session open for runtime dispatch
        ├─ For each McpServer referenced via Agent.tools but NOT in nodes:
        │     • NO ACTION — schemas already in server._tools from codegen
        ├─ _register_handlers_now()  ← subscribers wired up
        └─ return
     └─ broker.start()              ← subscribers begin consuming
     └─ keep-alive loop
```

The agent worker performs zero MCP contact: it constructs `McpServer` instances with `tools=Gmail.ALL` (codegen-supplied), iterates them to produce `BaseToolNodeSchema` entries, and feeds those to the agent. No subprocess spawn, no HTTP request, no credentials needed on the agent host.

### 7.4 Concurrency

`ClientSession.send_request` allocates a JSON-RPC request ID and stores a per-request response stream (per MCP SDK research, §7). Concurrent calls from sibling `McpBridge.run()` tasks are multiplexed transparently. No locks needed.

For HTTP transports, headers (including `token=`) are baked into the `httpx.AsyncClient` at session construction and never mutated per call (Pattern 1 from design doc §10). No async-coherence concerns.

---

## 8. MCP-to-calfkit adaptation

### 8.1 Tool schema (MCP `Tool` → calfkit `ToolDefinition`)

Done in `McpServer._build_schema`. Raw JSON Schema passes through unchanged. No validator synthesized (the existing schema-only path at `agent.py:374` skips client-side validation; the MCP server is the validator of last resort).

Reference: research output recipe §6 "PRIMARY — `BaseToolNodeSchema` (no callable, no validator)".

### 8.2 Tool result (MCP `CallToolResult` → calfkit `ToolReturn` or `RetryPromptPart`)

```python
# calfkit/mcp/_adapt.py

from mcp import types as mcp_types
from calfkit._vendor.pydantic_ai.messages import ToolReturn

def adapt_call_tool_result(
    result: mcp_types.CallToolResult,
    *,
    tool_call_id: str,
) -> ToolReturn:
    """MCP CallToolResult → calfkit ToolReturn.

    Preference order:
      1. structured_content (dict, matches outputSchema if declared)
      2. concatenated text content
      3. placeholder strings for non-text content
    """
    if result.structured_content is not None:
        return_value: Any = result.structured_content
    else:
        return_value = _flatten_content(result.content)

    return ToolReturn(
        return_value=return_value,
        metadata={
            "tool_call_id": tool_call_id,
            "mcp_meta": result.meta,
            "is_error": False,
        },
    )


def _flatten_content(blocks: list[mcp_types.ContentBlock]) -> str | list[Any]:
    if not blocks:
        return ""
    parts = []
    for b in blocks:
        if b.type == "text":
            parts.append(b.text)
        elif b.type == "image":
            parts.append(f"[image: {b.mime_type}]")
        elif b.type == "audio":
            parts.append(f"[audio: {b.mime_type}]")
        elif b.type == "resource_link":
            parts.append(f"[resource: {b.uri}]")
        elif b.type == "resource":  # EmbeddedResource
            res = b.resource
            if isinstance(res, mcp_types.TextResourceContents):
                parts.append(res.text)
            else:
                parts.append(f"[resource: {res.uri} ({res.mime_type})]")
    return parts[0] if len(parts) == 1 else parts
```

### 8.3 Error mapping (MCP errors → calfkit errors)

```python
# calfkit/mcp/_adapt.py (continued)

from mcp.shared.exceptions import MCPError
from calfkit.models.state import FailedToolCall


def classify_mcp_error(
    exc: BaseException, *,
    tool_name: str,
    tool_call_id: str,
) -> FailedToolCall:
    """Map MCP exceptions to FailedToolCall.

    All MCP-side failures from this function are operator-visible
    (NOT LLM-retryable). LLM-retryable errors come via CallToolResult.is_error
    handled in McpBridge.run(), not here.
    """
    if isinstance(exc, MCPError):
        return FailedToolCall(
            tool_name=tool_name,
            tool_call_id=tool_call_id,
            exc_type=f"McpError({exc.error.code})",
            exc_message=str(exc.error.message),
        )
    if isinstance(exc, asyncio.TimeoutError):
        return FailedToolCall(
            tool_name=tool_name,
            tool_call_id=tool_call_id,
            exc_type="McpTimeout",
            exc_message=str(exc),
        )
    # RuntimeError from MCP's output-schema validator counts as transport-layer
    # (server's outputSchema lies about its results).
    return FailedToolCall(
        tool_name=tool_name,
        tool_call_id=tool_call_id,
        exc_type=type(exc).__name__,
        exc_message=_safe_exc_message(exc),
    )
```

Error classification table:

| MCP situation | calfkit surface | Path |
|---|---|---|
| `CallToolResult.is_error=True` | `RetryPromptPart` (LLM-visible, retryable) | `McpBridge.run` happy-path branch |
| `MCPError(code=*)` (transport/RPC) | `FailedToolCall` → `ToolExecutionError` | `McpBridge.run` `except Exception` |
| `RuntimeError` from output-schema validation | `FailedToolCall` | same |
| `asyncio.TimeoutError` (per-call timeout exceeded) | `FailedToolCall` | same |
| `ModelRetry` raised by per-call hook | `RetryPromptPart` | `McpBridge.run` explicit `except ModelRetry` |
| `initialize` failure | `Worker.run()` aborts before broker starts | `_on_startup` raises |

### 8.4 Per-call meta resolution

Only one per-call hook exists in v1: `meta=`. There is no headers / token / per-call-auth hook (see §8.5 and design doc §10.4).

```python
# calfkit/mcp/_bridge.py (continued)

async def _resolve_meta(server: McpServer, ctx: ToolContext) -> dict | None:
    if server._meta is None:
        return None
    if callable(server._meta):
        result = server._meta(ctx)
        if inspect.isawaitable(result):
            result = await result
        return result
    return server._meta
```

Sync and async callables both supported. The resolved dict is passed to `session.call_tool(..., meta=...)` and serialized into the JSON-RPC message body inside the calling task before any await — no shared mutable state involved. Exceptions raised inside `meta=` propagate to the `except Exception` block of `McpBridge.run` and surface as `FailedToolCall` (operator-visible, NOT LLM-retryable — a misconfigured hook is a bug, not a tool error).

### 8.5 Session-static credentials

`token=` and `headers=` are resolved once at `McpServer` construction time. `$VAR` substitution is performed by `expand_env` (§9.1). The resolved values are stored on the `HttpTransport` instance and merged into the `httpx.AsyncClient(headers=...)` constructor argument when `McpSession.open()` runs at Worker startup. After that point, they are immutable for the lifetime of the session.

This is Pattern 1 from design doc §10. For Pattern 2 (session pool keyed by tenant) and Pattern 3 (one bridge per credential set), see the design doc — neither requires changes to this implementation.

---

## 9. Configuration & environment

### 9.1 `mcp.json` parsing

```python
# calfkit/mcp/_config.py

import os
import json
import re
from pathlib import Path

ENV_PATTERN = re.compile(r"\$(\{(\w+)\}|(\w+))")


def expand_env(value: Any) -> Any:
    if isinstance(value, str):
        def _sub(m):
            var = m.group(2) or m.group(3)
            if var not in os.environ:
                raise ConfigError(f"environment variable {var!r} is unset")
            return os.environ[var]
        return ENV_PATTERN.sub(_sub, value)
    if isinstance(value, dict):
        return {k: expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(x) for x in value]
    return value


def parse_mcp_config(config: dict | str | Path) -> dict[str, McpServer]:
    if isinstance(config, (str, Path)):
        config = json.loads(Path(config).read_text())
    if "mcpServers" in config:
        config = config["mcpServers"]
    config = expand_env(config)

    result: dict[str, McpServer] = {}
    for name, spec in config.items():
        if "type" in spec and spec["type"] == "http":
            result[name] = McpServer(
                HttpTransport(url=spec["url"]),
                name=name,
                headers=spec.get("headers"),
            )
        else:
            # stdio is the default when type is absent
            result[name] = McpServer(
                StdioTransport(
                    command=spec["command"],
                    args=spec.get("args", []),
                    env=spec.get("env"),
                    cwd=spec.get("cwd"),
                ),
                name=name,
            )
    return result
```

### 9.2 stdio environment passthrough

MCP's `stdio_client` uses `get_default_environment()` (a safe allowlist) when `env=None`. This silently drops things like `GMAIL_OAUTH`. v1 explicitly merges `os.environ`:

```python
env={**os.environ, **(self._transport.env or {})}
```

Documented as the standard behavior; users who want the allowlist-only mode can pass `env={}` explicitly.

---

## 10. Filtering & renaming semantics

| Filter | Pre-rename or post? | Composes with |
|---|---|---|
| `only(*names)` | pre-rename | exclude, where |
| `exclude(*names)` | pre-rename | only, where |
| `where(read_only_hint=True)` | n/a | other where |
| `where(predicate=fn)` | n/a | other where |
| `prefix(value)` | rename | rename |
| `rename({a: b})` | rename | prefix |

Chains return new immutable `McpServer` instances. Equality is structural for caching (e.g. avoiding duplicate discoveries when the same logical server is referenced in `nodes=` and `tools=`).

Filter evaluation order:

1. `only` (whitelist) — short-circuits.
2. `exclude` (blacklist).
3. `where` annotation predicates (read_only_hint, destructive_hint, idempotent_hint, open_world_hint) — AND.
4. `where` custom predicates — AND.

Renames apply only to the agent-facing name (`tool_schema.name`), not topic names or `node_id`. This keeps wire identity stable; only the LLM sees the rename.

---

## 11. Testing strategy

### 11.1 `FakeMcpServer`

A drop-in replacement for `McpServer` that bypasses transport entirely.

```python
# calfkit/mcp/_testing.py

from .__init__ import McpServer
from ._session import McpToolMeta


class FakeMcpServer(McpServer):
    """In-memory MCP server stub for tests.

    Skips subprocess + transport. The Worker's _on_startup pathway treats it
    identically to a real McpServer (same _open_bridge_session, same iter,
    same bridge), but call_tool dispatches to a user-supplied callable
    instead of going over MCP.
    """

    def __init__(
        self,
        name: str,
        tools: list[McpToolDef],
        invoker: Callable[[str, dict], CallToolResult | Awaitable[CallToolResult]],
    ):
        # Minimal init, skipping real transport setup
        self._name = name
        self._tools = tools                      # codegen-style declarations
        self._invoker = invoker
        # ... fill in default filters / renames / etc.

    async def _open_bridge_session(self) -> None:
        # Install a fake session that routes call_tool to self._invoker.
        # Skip sanity-check (no real list_tools to compare against).
        self._session = _FakeSession(self._invoker)
        self._initialize_result = _FakeInitializeResult(self._name)
```

Used in tests:

```python
from calfkit.mcp._testing import FakeMcpServer, mcp_tool_meta

gmail = FakeMcpServer(
    "gmail",
    tools=[
        mcp_tool_meta("search", input_schema={"type": "object",
                      "properties": {"q": {"type": "string"}}}),
        mcp_tool_meta("send", ...),
    ],
    invoker=lambda name, args: CallToolResult(
        content=[TextContent(text=f"{name}({args})")],
        is_error=False,
    ),
)

# Plug into existing TestKafkaBroker-based pipeline
```

### 11.2 Test layers

| Layer | Coverage | Infrastructure |
|---|---|---|
| Unit (`tests/mcp/test_server.py`) | filter/rename composition, `McpServer.__iter__` validation, env substitution, `mcp.json` parsing | pytest |
| Unit (`tests/mcp/test_adapt.py`) | content-block flattening, error classification, structured_content preference | pytest |
| Unit (`tests/mcp/test_session.py`) | `McpSession` lifecycle, per-call header injection lock | pytest + monkeypatched httpx |
| Integration (`tests/mcp/test_bridge.py`) | full envelope flow agent → bridge → MCP → reply | `TestKafkaBroker` + `FakeMcpServer` |
| Integration (`tests/mcp/test_worker_lifecycle.py`) | `Worker._on_startup` ordering, shutdown cleanup, cancellation during boot | `TestApp` from FastStream |
| E2E (`tests/mcp/test_real_servers.py`, separate CI lane) | actual `@modelcontextprotocol/server-everything` + a stub HTTP MCP server | docker compose |

### 11.3 Key test scenarios

1. **Agent worker constructs `Agent(tools=[gmail])` where `gmail` is a FakeMcpServer; tools registry populates correctly after `_on_startup`.**
2. **Bridge worker handles a `Call[State]` envelope, dispatches to `session.call_tool`, returns `ReturnCall[State]`.**
3. **`CallToolResult.is_error=True` becomes `RetryPromptPart`; LLM-visible error message contains content.**
4. **`MCPError` raised by `call_tool` becomes `FailedToolCall`; agent escalates as `ToolExecutionError`.**
5. **Per-call hook exception surfaces as `FailedToolCall` (NOT `RetryPromptPart`).**
6. **Parallel tool calls to one bridge pipeline correctly (no serial dispatch).**
7. **`Worker._on_startup` failure aborts before subscribers register; no zombie subprocesses on retry.**
8. **`mcp.json` parse with `$VAR` substitution; unset variable raises at parse time.**
9. **Filter composition: `gmail.only("search").exclude("delete")` is equivalent to a single-step allowlist.**
10. **Prefix rename: agent sees `inbox.search`, but Kafka topic is `mcp.gmail.search.input`.**

---

## 12. File-by-file change list

| File | Change | Lines |
|---|---|---|
| `calfkit/__init__.py` | add `from .mcp import mcp` and `"mcp"` to `__all__` | +2 |
| `calfkit/mcp/__init__.py` | new — exports `mcp`, `McpServer`, `McpServers` | ~30 |
| `calfkit/mcp/_server.py` | new — `McpServer` class | ~250 |
| `calfkit/mcp/_bridge.py` | new — `McpBridge` (BaseNodeDef subclass) | ~180 |
| `calfkit/mcp/_session.py` | new — `McpSession`, `StdioTransport`, `HttpTransport`, `McpToolMeta` | ~220 |
| `calfkit/mcp/_config.py` | new — `parse_mcp_config`, `expand_env` | ~80 |
| `calfkit/mcp/_adapt.py` | new — `adapt_call_tool_result`, `classify_mcp_error` | ~100 |
| `calfkit/mcp/_testing.py` | new — `FakeMcpServer`, `_FakeSession` | ~120 |
| `calfkit/mcp/exceptions.py` | new — `McpConfigError`, etc. | ~30 |
| `calfkit/worker/worker.py` | modify — segregate MCP nodes, add `_on_startup`/`_on_shutdown`, pass to FastStream | ~80 net diff |
| `pyproject.toml` | add `mcp>=1.20.0` to deps | +1 |
| `tests/mcp/*.py` | new test modules | ~800 |
| `examples/quickstart_mcp/*.py` | new — minimal MCP example | ~100 |
| `README.md` | new MCP section | ~60 |
| `docs/mcp-adaptor-design.md` | already exists | — |

Approximate total: ~2,200 new lines + ~80 modified lines.

---

## 13. Phased implementation

### Phase 0 — Pre-work (1 day)

- Pin `mcp>=1.20.0` in `pyproject.toml`. Run existing test suite to confirm no transitive conflicts.
- Add `calfkit/mcp/` package skeleton (empty `__init__.py`, exceptions module).
- Verify the schema-only path works as documented: add an exploratory test that builds a `BaseToolNodeSchema` directly and runs the existing agent flow against it (no MCP involved). This sanity-checks that the documented override-mode path (`agent.py:163-167, :374`) behaves as the research described.

### Phase 1 — Core types and session (3 days)

- `_tool_def.py`: `McpToolDef` dataclass (the codegen target). Field equivalence with `mcp.types.Tool`. Pure data + serde.
- `_session.py`: `StdioTransport`, `HttpTransport`, `McpSession`, `McpToolMeta`. Unit tests via mocked `ClientSession`.
- `_adapt.py`: `adapt_call_tool_result`, `classify_mcp_error`. Pure unit tests.
- `_config.py`: `mcp.json` parser + env expansion. Pure unit tests.

### Phase 2 — `McpServer` frontend (2 days)

- `_server.py`: `McpServer` class accepting `tools=` (list of `McpToolDef`). Chainable filter methods. `__iter__` yields `BaseToolNodeSchema` instances directly from `self._tools` (no discovery needed).
- `_testing.py`: `FakeMcpServer` for tests without subprocess.

### Phase 3 — `McpBridge` runtime (3 days)

- `_bridge.py`: `McpBridge` extending `BaseNodeDef`. Per-call `meta=` hook resolution. Error mapping integration.
- Integration tests using `FakeMcpServer` + `TestKafkaBroker`: full envelope round-trip.

### Phase 4 — Worker integration (1 day)

- Patch `calfkit/worker/worker.py` to spawn MCP sessions at startup, run sanity-check `tools/list` (validate user-declared tools exist), register subscribers. **No catalog publish/read — schemas come from user-provided `tools=`.**
- Cancellation safety with `BaseException` cleanup.
- Tests for: lifecycle ordering, cancellation cleanup, mixed worker (bridges + agents), single-process worker.

### Phase 5 — Codegen CLI (2 days)

- `_codegen.py` + `cli/mcp.py`: `calfkit mcp codegen <name> --command "..." --output <path>` command.
- Spawns MCP server via `McpSession`, runs `initialize` + `tools/list`, renders Python module with `McpToolDef` instances + a per-server class with `.ALL` and per-tool attributes.
- `--check` mode for CI drift detection: re-generates in-memory and exits non-zero if it would differ from the existing file.
- Tests using `FakeMcpServer` + temp directories.

### Phase 6 — Public API polish (1 day)

- Module-level `mcp` callable + `.stdio` / `.http` namespacers. Auto-detect logic. Default name inference.
- `McpServers.from_file` / `from_config`. End-to-end test using a real `mcp.json` fixture file.

### Phase 7 — Documentation + examples (2 days)

- README section: lead with the codegen workflow (3 commands: `pip install`, `calfkit mcp codegen`, `python agent.py`).
- `examples/quickstart_mcp/`: shared.py + tools_worker.py + agent_worker.py + invoke.py + `gmail_schemas.py` (generated, committed).
- The `@consumer(subscribe_topics="mcp.gmail.send.output")` observability tap example.

### Phase 8 — E2E + release (2 days)

- E2E tests in separate CI lane against `@modelcontextprotocol/server-everything` and a containerized HTTP MCP fixture.
- Release-please configuration for the new mcp subpackage.
- Manual smoke test against a real OAuth server (Gmail or GitHub).

**Total estimate: ~13 working days for a single engineer.** Phases 1–4 are sequential; phases 5, 6, 7 can parallelize. Two days less than the previous estimate thanks to removing the catalog topic mechanism.

---

## 14. Risk register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| **`mcp` SDK API churn** — function name changed from `streamablehttp_client` to `streamable_http_client` in v1.20. Could change again. | Medium | High | Pin to a known-good version range. Wrap SDK imports in `_session.py` so a future version bump touches one file. Track upstream releases. |
| **stdio env passthrough surprise** — users won't expect calfkit to bypass MCP SDK's allowlist | Medium | Medium | Document loudly. Add a `safe_env_only=True` opt-in for users who want the allowlist behavior. |
| **Long-running MCP tools pin worker handlers** | Medium | Medium | Documented as v1 limitation. v2 implements `tasks` extension support. |
| **Worker crash mid-call duplicates MCP execution** | Medium | High for non-idempotent tools | Documented gap. v2 may add idempotency-key cache keyed on `(tool_call_id, args_hash)` using MCP's `idempotentHint`. |
| **MCP server requires `sampling` capability** | Low | Medium | `initialize` succeeds (we don't advertise sampling); if server requires it server-side, the first `tools/call` fails. Surface as a startup-time check: log warning if `InitializeResult.capabilities` indicates a required-sampling server. |
| **JSON Schema with `additionalProperties: false` + LLM emits extra fields** | High | Low | No client-side validation in v1 (documented). MCP server rejects, returns `is_error=True`, LLM sees `RetryPromptPart` and adjusts. Acceptable. |
| **Codegen output drift from MCP server upgrades** | Medium | Low | CI job runs `calfkit mcp codegen --check` on schedule; opens PR if upstream MCP server changed. Standard schema-as-artifact workflow. |
| **Cancellation during boot orphans subprocess** | Low | Medium | Explicit `BaseException` catch in `_on_startup` (see §6.3). Tested. |

---

## 15. Open questions — resolved

All open questions for v1 are signed off. See [`docs/mcp-v1-plan.md`](./mcp-v1-plan.md) §11 for the authoritative decision record (Tier 1 / Tier 2 / Tier 3 tables).

Key decisions of note for the implementer:
- **Canonical import**: `from calfkit.mcp import McpServer` (class form) is canonical. `from calfkit import mcp` (factory) is a convenience shortcut. (v1 plan D11.)
- **`Agent(tools=...)` typing**: widened via a `ToolLike = ToolNodeDef | BaseToolNodeSchema | McpServer` Union alias. (v1 plan D13.)
- **`reconnect=` kwarg**: dropped. Bridge fails loudly on session loss; orchestrator restarts. (v1 plan Q10.)
- **Idempotency dedup in v1**: in-process LRU 10k, 1hr TTL, in `calfkit/mcp/_dedup.py`. Hot fix tracked for generalization in [#161](https://github.com/calf-ai/calfkit-sdk/issues/161). (v1 plan Q2 / D10.)

---

## 16. References

### Internal

- [`docs/mcp-adaptor-design.md`](./mcp-adaptor-design.md) — design intent, peer-SDK comparison, durability gaps.
- `calfkit/nodes/agent.py:163-167, :374` — agent loop's schema-only tool path.
- `calfkit/models/node_schema.py:29-32` — `BaseToolNodeSchema` definition.
- `calfkit/worker/worker.py:33-71` — current Worker lifecycle (target of patch).
- `calfkit/_vendor/pydantic_ai/tools.py:288-429, :473-542` — `Tool`, `ToolDefinition`, `Tool.from_schema`.
- `calfkit/_vendor/pydantic_ai/toolsets/external.py:13-47` — `ExternalToolset`, the data-only consumer of `ToolDefinition`.
- `tests/test_tool_errors.py:687-723, :1118-1157` — existing tests for schema-only override-mode path.

### External

- [`mcp` Python SDK v1.27](https://github.com/modelcontextprotocol/python-sdk) — `ClientSession`, `stdio_client`, `streamable_http_client`, types, errors.
- [MCP specification (2025-11-25)](https://modelcontextprotocol.io/specification/2025-11-25/) — protocol-level reference.
- [FastStream lifecycle docs](https://faststream.ag2.ai/latest/) — `on_startup`, `lifespan`, hook ordering.

### Companion design docs

- [`docs/hooks-design.md`](./hooks-design.md) — referenced for the broader pre/post-run extensibility direction; the MCP adaptor lives below the hook layer (hooks could wrap MCP tool dispatch in the same way they wrap native tools).
