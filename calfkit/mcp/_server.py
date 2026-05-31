"""User-facing ``McpServer`` — the polymorphic frontend.

``McpServer`` is the single object users pass to both ``Worker(nodes=[...])``
and ``Agent(tools=[...])``. It carries:

- A transport descriptor (stdio or HTTP)
- A list of declared :class:`McpToolDef` schemas (typically from a
  codegen-generated module — see Phase 5 CLI)
- Filter / rename chains that scope which tools the agent sees
- A per-call ``meta=`` hook for Pattern 1 multi-tenancy identity

Construction is **cheap, synchronous, and I/O-free**. No subprocess is
spawned and no network call is made at import time. The MCP session is
opened lazily in ``_open_bridge_session`` (called by ``Worker._on_startup``
in Phase 4).

Polymorphism: ``__iter__`` yields one :class:`BaseToolNodeSchema` per
filtered tool. The agent's existing ``tools_registry`` machinery
(``calfkit/nodes/agent.py:163-167``) consumes these directly via the
schema-only path — empirically validated by Phase 0's baseline test.
The Worker (Phase 4) detects ``McpServer`` instances in ``nodes=`` and
constructs one ``McpBridge`` per tool from the same iteration.

See ``docs/mcp-v1-plan.md`` §6.1 and ``docs/mcp-adaptor-implementation-plan.md``
§5.1 for design context.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.mcp._config import expand_env
from calfkit.mcp._session import HttpTransport, McpSession, McpTransport, StdioTransport
from calfkit.mcp._tool_def import McpToolDef
from calfkit.mcp.exceptions import McpConfigError
from calfkit.models.node_schema import BaseToolNodeSchema

if TYPE_CHECKING:
    from calfkit.models.tool_context import ToolContext

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Filter and rename state (frozen, chainable)
# ---------------------------------------------------------------------------


# Type aliases for hook signatures. The annotation-keyword predicates take
# an McpToolDef and return bool; users can also pass arbitrary predicates.
_ToolPredicate = Callable[[McpToolDef], bool]
_MetaHook = dict[str, Any] | Callable[["ToolContext"], dict[str, Any] | Any]


@dataclass(frozen=True)
class _Filters:
    """Immutable filter state.

    ``only`` of ``None`` means "no allowlist applied". An empty frozenset
    is a *real* empty allowlist that legitimately filters everything out
    (and surfaces as the empty-tools warning via :meth:`McpServer.__iter__`).
    Distinguishing these matters because users can compose
    ``.only("x").exclude("x")`` and we must honor the intent.
    """

    only: frozenset[str] | None = None
    exclude: frozenset[str] = frozenset()
    predicates: tuple[_ToolPredicate, ...] = ()


@dataclass(frozen=True)
class _Renames:
    """Immutable rename state.

    ``prefix`` is applied first; ``explicit`` mapping then overrides per-tool.
    Either or both may be set. Renames affect only the agent-facing tool
    name (``ToolDefinition.name``); topic paths use the original MCP tool
    name to keep wire identity stable across user-side renames.
    """

    prefix: str | None = None
    explicit: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Topic / name normalization
# ---------------------------------------------------------------------------


def _normalize_topic_component(s: str) -> str:
    """Make a string safe to embed inside a dot-separated Kafka topic path.

    Replaces ``.`` and ``-`` with ``_`` (both common in npm package names,
    URLs, etc.). The result is used in ``mcp.<normalized>.<tool>.input`` —
    if we left dots inside the server name, our topic-parsing convention
    would break for downstream tooling that splits on ``.``.

    Per v1 plan Q13. Matches the conventions used by Kubernetes labels,
    DNS labels, Avro schema names, and AsyncAPI channel paths.
    """
    return s.replace(".", "_").replace("-", "_")


# ---------------------------------------------------------------------------
# McpServer
# ---------------------------------------------------------------------------


class McpServer:
    """Polymorphic MCP server reference.

    Behaves as:

    - A **tool source** when passed to ``Agent(tools=[server])`` — iteration
      yields one :class:`BaseToolNodeSchema` per filtered tool.
    - A **node group** when passed to ``Worker(nodes=[server])`` (Phase 4) —
      the worker iterates the same way and constructs an ``McpBridge`` per
      yielded tool, plus opens the long-lived MCP session.

    Construct via :meth:`stdio` or :meth:`http` classmethods, not the bare
    constructor (which is internal).

    Immutable: filter / rename methods return new instances via
    ``dataclasses.replace`` semantics. The ``_session`` field is the one
    mutable piece — populated by :meth:`_open_bridge_session` at Worker
    startup and consumed by Phase 3's ``McpBridge`` for runtime dispatch.
    """

    # ----- Construction -----

    def __init__(
        self,
        transport: McpTransport,
        *,
        tools: list[McpToolDef],
        name: str | None = None,
        meta: _MetaHook | None = None,
        read_timeout_seconds: float = 120.0,
        client_info_version: str | None = None,
        filters: _Filters | None = None,
        renames: _Renames | None = None,
    ) -> None:
        # Internal constructor; users go through stdio() / http() classmethods.
        if not isinstance(tools, list):
            # Tuple, frozen iterable, etc. all OK at runtime, but a list
            # is the documented surface so we coerce + freeze.
            tools = list(tools)

        raw_name = name if name is not None else transport.infer_name()
        if not raw_name:
            raise McpConfigError("McpServer: 'name' is empty and could not be inferred from the transport")

        self._transport = transport
        self._tools: tuple[McpToolDef, ...] = tuple(tools)
        self._raw_name = raw_name
        self._name = _normalize_topic_component(raw_name)
        self._meta = meta
        self._read_timeout = read_timeout_seconds
        self._client_info_version = client_info_version
        self._filters = filters or _Filters()
        self._renames = renames or _Renames()

        # Populated by _open_bridge_session at Worker.run() startup.
        # The McpBridge (Phase 3) reads self._session to dispatch tool calls.
        self._session: McpSession | None = None
        self._initialize_result: Any = None

    @classmethod
    def stdio(
        cls,
        command: str,
        *args: str,
        tools: list[McpToolDef],
        name: str | None = None,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        shutdown_grace_seconds: float = 5.0,
        safe_env_only: bool = False,
        meta: _MetaHook | None = None,
        read_timeout_seconds: float = 120.0,
        client_info_version: str | None = None,
    ) -> McpServer:
        """Construct an MCP server with the stdio transport.

        ``command`` is the executable; ``*args`` are extra command-line args
        (e.g. ``McpServer.stdio("npx", "-y", "@mcp/server-gmail", tools=...)``).

        ``tools`` is required: a list of :class:`McpToolDef` instances
        (typically imported from a ``calfkit mcp codegen``-generated module).
        Empty ``tools=[]`` is allowed but logs a warning on iteration.

        Env merge: by default, ``{**os.environ, **(env or {})}`` is passed
        to the subprocess (full passthrough, matching Docker / subprocess
        conventions). Pass ``safe_env_only=True`` to defer to the MCP SDK's
        allowlist instead.

        ``$VAR`` / ``${VAR}`` substitution is applied to every string value
        (command, args, env values, cwd) at construction time so the same
        idiom works for inline construction and ``mcp.json`` loading.
        Unset variables raise :class:`McpConfigError`.
        """
        transport = StdioTransport(
            command=expand_env(command, where="McpServer.stdio(command=)"),
            args=tuple(expand_env(a, where="McpServer.stdio(args=)") for a in args),
            env=expand_env(env, where="McpServer.stdio(env=)") if env is not None else None,
            cwd=expand_env(cwd, where="McpServer.stdio(cwd=)") if cwd is not None else None,
            shutdown_grace_seconds=shutdown_grace_seconds,
            safe_env_only=safe_env_only,
        )
        return cls(
            transport,
            tools=tools,
            name=name,
            meta=meta,
            read_timeout_seconds=read_timeout_seconds,
            client_info_version=client_info_version,
        )

    @classmethod
    def http(
        cls,
        url: str,
        *,
        tools: list[McpToolDef],
        name: str | None = None,
        token: str | None = None,
        headers: dict[str, str] | None = None,
        httpx_client_kwargs: dict[str, Any] | None = None,
        timeout_seconds: float = 30.0,
        sse_read_timeout_seconds: float = 300.0,
        meta: _MetaHook | None = None,
        read_timeout_seconds: float = 120.0,
        client_info_version: str | None = None,
    ) -> McpServer:
        """Construct an MCP server with the Streamable HTTP transport.

        ``token`` is sugar for an ``Authorization: Bearer <token>`` header;
        if both ``token`` and an explicit ``Authorization`` header in
        ``headers`` are set, the explicit header wins (user's intent).

        Per Pattern 1 (design doc §10), credentials are session-static.
        For per-call user identity, use ``meta=lambda ctx: {...}``.

        ``$VAR`` / ``${VAR}`` substitution is applied to ``url``, ``token``,
        and every header value at construction time so the same idiom works
        for inline construction and ``mcp.json`` loading. Unset variables
        raise :class:`McpConfigError`. The expanded URL must start with
        ``http://`` or ``https://`` — a misconfigured template like
        ``$MCP_BASE`` resolving to a scheme-less value fails loudly here
        rather than later inside httpx.
        """
        expanded_url = expand_env(url, where="McpServer.http(url=)")
        if not (expanded_url.startswith("http://") or expanded_url.startswith("https://")):
            raise McpConfigError(
                f"McpServer.http(url=): expanded URL {expanded_url!r} does not start with "
                f"'http://' or 'https://'. Check that any $VAR in {url!r} resolves to a full URL."
            )
        transport = HttpTransport(
            url=expanded_url,
            token=expand_env(token, where="McpServer.http(token=)") if token is not None else None,
            headers=expand_env(dict(headers), where="McpServer.http(headers=)") if headers is not None else {},
            httpx_client_kwargs=dict(httpx_client_kwargs) if httpx_client_kwargs is not None else {},
            timeout_seconds=timeout_seconds,
            sse_read_timeout_seconds=sse_read_timeout_seconds,
        )
        return cls(
            transport,
            tools=tools,
            name=name,
            meta=meta,
            read_timeout_seconds=read_timeout_seconds,
            client_info_version=client_info_version,
        )

    # ----- Chainable filter / rename operations (immutable) -----

    def only(self, *tool_names: str) -> McpServer:
        """Allowlist: only the given tool names are exposed.

        Composes by intersection with any existing allowlist.
        """
        new_only = frozenset(tool_names)
        if self._filters.only is not None:
            new_only = self._filters.only & new_only
        return self._copy(_filters=replace(self._filters, only=new_only))

    def exclude(self, *tool_names: str) -> McpServer:
        """Blocklist: the given tool names are removed."""
        return self._copy(_filters=replace(self._filters, exclude=self._filters.exclude | frozenset(tool_names)))

    def where(
        self,
        predicate: _ToolPredicate | None = None,
        *,
        read_only_hint: bool | None = None,
        destructive_hint: bool | None = None,
        idempotent_hint: bool | None = None,
        open_world_hint: bool | None = None,
    ) -> McpServer:
        """Filter by predicate and/or annotation hints.

        Annotation hints are matched against the *effective* value (with
        MCP spec defaults applied via :class:`McpToolDef`'s convenience
        properties). Combine multiple ``where`` calls to AND filters.

        Examples::

            gmail.where(read_only_hint=True)        # only read-only tools
            gmail.where(destructive_hint=False)     # exclude destructive
            gmail.where(predicate=lambda t: "search" in t.name.lower())
        """
        new_predicates = list(self._filters.predicates)
        if read_only_hint is not None:
            new_predicates.append(_annotation_predicate("read_only", read_only_hint))
        if destructive_hint is not None:
            new_predicates.append(_annotation_predicate("destructive", destructive_hint))
        if idempotent_hint is not None:
            new_predicates.append(_annotation_predicate("idempotent", idempotent_hint))
        if open_world_hint is not None:
            new_predicates.append(_annotation_predicate("open_world", open_world_hint))
        if predicate is not None:
            new_predicates.append(predicate)
        return self._copy(_filters=replace(self._filters, predicates=tuple(new_predicates)))

    def prefix(self, value: str) -> McpServer:
        """Prepend ``value.`` to every agent-facing tool name.

        Affects only the LLM-visible name (``tool_schema.name``); topic
        paths still use the original MCP tool names.
        """
        return self._copy(_renames=replace(self._renames, prefix=value))

    def rename(self, mapping: dict[str, str]) -> McpServer:
        """Explicit per-tool rename. Maps original name → agent-facing name."""
        merged = {**self._renames.explicit, **mapping}
        return self._copy(_renames=replace(self._renames, explicit=merged))

    def _copy(self, **changes: Any) -> McpServer:
        """Return a new McpServer with field overrides applied.

        Used internally by the chainable filter / rename methods. The
        session reference is intentionally NOT carried — a filtered/renamed
        view is a new logical server; if deployed, it gets its own session.
        """
        new = object.__new__(McpServer)
        new.__dict__.update(self.__dict__)
        new.__dict__.update(changes)
        # Reset session-bound mutable state on filtered/renamed copies.
        new._session = None
        new._initialize_result = None
        return new

    # ----- Iteration (the polymorphic surface) -----

    def __iter__(self) -> Iterator[BaseToolNodeSchema]:
        """Yield one BaseToolNodeSchema per filtered tool.

        Trivially synchronous — schemas come from ``self._tools`` (codegen
        output or inline), not from a running MCP server. Safe to call at
        module import time, in agent constructor, or anywhere.

        Empty result (no tools after filtering) logs a warning at WARNING
        level so users notice missing schemas without an exception
        (per v1 plan Q4).
        """
        filtered = list(self._apply_filters(self._tools))
        if not filtered:
            logger.warning(
                "McpServer %r yielded no tools — did you forget to run `calfkit mcp codegen` or pass tools=? "
                "(filters: only=%s, exclude=%s, predicates=%d; raw tool count=%d)",
                self._raw_name,
                self._filters.only,
                sorted(self._filters.exclude),
                len(self._filters.predicates),
                len(self._tools),
            )
            return
        for tool in filtered:
            yield self._build_schema(tool)

    def _apply_filters(self, tools: tuple[McpToolDef, ...]) -> Iterator[McpToolDef]:
        """Apply the chained filter operations in order: only → exclude → predicates."""
        for tool in tools:
            if self._filters.only is not None and tool.name not in self._filters.only:
                continue
            if tool.name in self._filters.exclude:
                continue
            if not all(p(tool) for p in self._filters.predicates):
                continue
            yield tool

    def _build_schema(self, tool: McpToolDef) -> BaseToolNodeSchema:
        """Construct one ``BaseToolNodeSchema`` for the agent's registry.

        - LLM-facing name: applies prefix + explicit renames.
        - Topic path: uses the ORIGINAL MCP tool name (not renamed) so
          wire identity stays stable across rename changes.
        - Topic path: uses the NORMALIZED server name (Q13) so dots/hyphens
          in server names don't conflict with our dot-separated convention.
        """
        agent_facing_name = self._rename(tool.name)
        topic_base = f"mcp.{self._name}.{tool.name}"
        tool_definition = ToolDefinition(
            name=agent_facing_name,
            description=tool.description,
            parameters_json_schema=tool.input_schema,
            metadata={
                "mcp_server": self._raw_name,
                "mcp_server_normalized": self._name,
                "mcp_tool_name": tool.name,  # original; used by McpBridge for dispatch
                "annotations": tool.annotations.to_dict() if tool.annotations is not None else None,
                "output_schema": tool.output_schema,
                "_meta": tool.meta,
                "source": "mcp",
            },
        )
        return BaseToolNodeSchema(
            node_id=f"mcp_{self._name}_{tool.name}",
            subscribe_topics=[f"{topic_base}.input"],
            publish_topic=f"{topic_base}.output",
            tool_schema=tool_definition,
        )

    def _rename(self, original: str) -> str:
        """Apply prefix and explicit-rename mapping to one tool name."""
        if original in self._renames.explicit:
            return self._renames.explicit[original]
        if self._renames.prefix is not None:
            return f"{self._renames.prefix}.{original}"
        return original

    # ----- Bridge session lifecycle (consumed by Worker in Phase 4) -----

    async def _open_bridge_session(self) -> None:
        """Spawn the MCP session and validate declared tools against the server.

        Called only on bridge workers (workers that include this ``McpServer``
        in their ``nodes=`` list — Phase 4). Agent-only workers skip this
        entirely; they already have schemas from ``self._tools``.

        After successful return:
        - ``self._session`` holds a live :class:`McpSession`.
        - ``self._initialize_result`` holds the MCP ``InitializeResult``.
        - Drift between declared and server-side tools is logged at
          WARNING (declared-missing) and INFO (server-extra). Per v1 plan
          Q7, v1 does NOT hard-fail on drift; the strict variant is a
          v1.1 enhancement.

        Raises whatever the underlying transport/initialize raise — the
        Worker patch (Phase 4) catches ``BaseException`` to ensure
        spawned processes get cleaned up on cancellation.
        """
        self._session = await McpSession.open(
            self._transport,
            client_info_version=self._client_info_version,
            read_timeout_seconds=self._read_timeout,
        )
        self._initialize_result = await self._session.initialize()

        # Sanity-check declared tools vs server's actual tools.
        try:
            server_tools = await self._session.list_tools()
        except Exception:
            # Don't let a list_tools failure prevent the bridge from
            # starting — call_tool failures will surface naturally if
            # the agent calls an unknown tool. Log at ERROR so operators
            # alert on the missed drift check.
            logger.error(
                "McpServer %r: tools/list sanity-check failed; skipping drift detection. "
                "Tool dispatch may fail at runtime if declared tools don't exist on the server.",
                self._raw_name,
                exc_info=True,
            )
            return

        server_names = {t.name for t in server_tools}
        declared_names = {t.name for t in self._tools}
        missing = declared_names - server_names
        extra = server_names - declared_names
        if missing:
            logger.warning(
                "McpServer %r: %d declared tool(s) missing on server: %s. "
                "Subscribers will register but calls will fail at dispatch. "
                "Run `calfkit mcp codegen` to refresh the schema module.",
                self._raw_name,
                len(missing),
                sorted(missing),
            )
        if extra:
            logger.info(
                "McpServer %r: server exposes %d tool(s) not in your declared schemas: %s. "
                "These will not be dispatchable. Run `calfkit mcp codegen` to add them.",
                self._raw_name,
                len(extra),
                sorted(extra),
            )

    async def _close_bridge_session(self) -> None:
        """Tear down the bridge session. Safe to call multiple times."""
        if self._session is not None:
            try:
                await self._session.aclose()
            finally:
                self._session = None
                self._initialize_result = None

    # ----- Accessors (Phase 3 + tests) -----

    @property
    def name(self) -> str:
        """The normalised name used in topic paths (e.g. ``gmail_v2``)."""
        return self._name

    @property
    def raw_name(self) -> str:
        """The original un-normalised name (e.g. ``gmail.v2``)."""
        return self._raw_name

    @property
    def transport(self) -> McpTransport:
        return self._transport

    @property
    def tools(self) -> tuple[McpToolDef, ...]:
        """The user-declared tools (pre-filter)."""
        return self._tools

    @property
    def session(self) -> McpSession | None:
        """The live MCP session (set after ``_open_bridge_session``)."""
        return self._session

    @property
    def meta_hook(self) -> _MetaHook | None:
        """The per-call meta hook (resolved per-envelope by Phase 3 McpBridge)."""
        return self._meta


# ---------------------------------------------------------------------------
# Annotation predicate helper
# ---------------------------------------------------------------------------


def _annotation_predicate(prop_name: str, expected: bool) -> _ToolPredicate:
    """Build a tool predicate that matches an annotation hint's *effective* value.

    Uses :class:`McpToolDef`'s convenience properties (``read_only``,
    ``destructive``, ``idempotent``, ``open_world``) which apply MCP
    spec defaults — so ``where(destructive_hint=False)`` correctly
    excludes tools without an explicit annotation (since the spec default
    is destructive=True).
    """

    def _pred(tool: McpToolDef) -> bool:
        return bool(getattr(tool, prop_name)) is expected

    _pred.__name__ = f"annotation_{prop_name}_eq_{expected}"
    return _pred
