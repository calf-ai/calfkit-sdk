"""``McpBridge`` — per-tool dispatch node.

One ``McpBridge`` instance per (server, tool) pair. All bridges for a single
:class:`McpServer` share the same underlying ``McpSession`` (set on the
server by :meth:`McpServer._open_bridge_session` at Worker startup — Phase 4).

Wire model:

- Subscribes to ``mcp.<server>.<tool>.input`` (the agent's ``Call[State]`` lands here).
- Publishes to ``mcp.<server>.<tool>.output`` (observability tap point).
- Run loop: read tool call from state → resolve meta hook → check
  idempotency cache → dispatch to ``session.call_tool`` → adapt result →
  store in state → return ``ReturnCall[State]``.

Error mapping (see ``calfkit/mcp/_adapt.py``):

- ``CallToolResult.isError=True`` → ``RetryPromptPart`` (LLM-visible, retryable)
- Raised ``MCPError`` / timeout / other exception → ``FailedToolCall``
  (operator-visible, becomes ``ToolExecutionError`` at the agent)
- ``meta=`` hook raising → ``FailedToolCall`` (transport-layer fault;
  the call never went over the wire)

Idempotency cache (Phase 3 hot fix tracked in #161):

- Successful ``ToolReturn`` results are cached on ``(tool_call_id,
  args_hash)``. Redeliveries served from cache without re-dispatching.
- Tools annotated ``idempotentHint=True`` skip the cache (safe to re-run).
- Failures and tool-errors are NOT cached — they should retry naturally.

This module subclasses ``BaseNodeDef`` directly (NOT ``BaseToolNodeDef``)
because we don't want pydantic-ai's ``validate_call_args`` — the MCP
server is the schema authority. The agent's existing
``isinstance(tool_node, BaseToolNodeDef)`` gate at ``agent.py:374``
correctly skips client-side validation for our bare ``BaseNodeDef``.
"""

from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Any, ClassVar

from calfkit._protocol import NodeKind
from calfkit.mcp._adapt import (
    adapt_call_tool_result,
    build_retry_prompt_from_error_result,
    classify_mcp_error,
)
from calfkit.mcp._dedup import IdempotencyCache, make_cache_key
from calfkit.models import NodeResult, ReturnCall, SessionRunContext, State, ToolContext
from calfkit.models.actions import Silent
from calfkit.nodes.base import BaseNodeDef

if TYPE_CHECKING:
    from calfkit.mcp._server import McpServer
    from calfkit.mcp._tool_def import McpToolDef

logger = logging.getLogger(__name__)


class McpBridge(BaseNodeDef):
    """One per-(server, tool) Kafka subscriber that dispatches to a shared MCP session.

    Constructed by ``Worker._on_startup`` (Phase 4) from each McpServer's
    declared tools. Not user-facing — users construct McpServer; the
    Worker derives bridges from it.
    """

    _node_kind: ClassVar[NodeKind] = "tool"

    def __init__(
        self,
        *,
        server: McpServer,
        tool_def: McpToolDef,
        dedup_cache: IdempotencyCache | None = None,
    ) -> None:
        """Initialise one bridge for one tool.

        Args:
            server: The parent ``McpServer``. We hold a reference because
                the live MCP session lives on the server (set by
                ``_open_bridge_session``) and is shared across all bridges
                for the same server. The meta-hook also lives on the server.
            tool_def: The specific ``McpToolDef`` this bridge dispatches.
                We carry the original MCP tool name (not any renamed
                LLM-facing alias) for SDK dispatch.
            dedup_cache: Optional shared idempotency cache. If omitted, a
                fresh per-bridge cache is constructed with default params
                — suitable for unit tests; production Worker construction
                (Phase 4) should pass a shared cache so multi-tool
                redeliveries dedup correctly.
        """
        self._server = server
        self._tool_def = tool_def
        self._dedup_cache = dedup_cache if dedup_cache is not None else IdempotencyCache()

        # Topic / node identity derived from the server's normalised name
        # (Q13 — dots/hyphens already substituted) and the original MCP tool
        # name (NOT the LLM-facing rename — wire identity stable).
        topic_base = f"mcp.{server.name}.{tool_def.name}"
        super().__init__(
            node_id=f"mcp_{server.name}_{tool_def.name}",
            subscribe_topics=[f"{topic_base}.input"],
            publish_topic=f"{topic_base}.output",
        )

    @property
    def tool_name(self) -> str:
        """The original MCP tool name (used for SDK dispatch)."""
        return self._tool_def.name

    @property
    def server(self) -> McpServer:
        return self._server

    @property
    def dedup_cache(self) -> IdempotencyCache:
        return self._dedup_cache

    async def run(self, ctx: SessionRunContext, tool_call_id: str) -> NodeResult[State]:
        """Dispatch one MCP tool call.

        Mirrors the structure of :meth:`calfkit.nodes.tool.ToolNodeDef.run`:
        defensive against missing tool calls, builds a ToolContext for the
        meta hook, and uses the existing FailedToolCall / RetryPromptPart
        envelope-storage idiom for error surfacing.

        Returns:
            ``ReturnCall[State]`` after storing the result (success, retry
            prompt, or failure marker) in ``ctx.state.tool_results``. The
            agent's existing reply pipeline picks up the new entry.

            ``Silent()`` only if the matching tool call is absent from
            state — same defensive branch as the native tool node.
        """
        logger.debug(
            "[%s] mcp_bridge run entered server=%s tool=%s tool_call_id=%s",
            ctx.deps.correlation_id[:8],
            self._server.raw_name,
            self._tool_def.name,
            tool_call_id,
        )

        tool_call_part = ctx.state.get_tool_call(tool_call_id)
        if tool_call_part is None:
            # Defensive: same shape as ToolNodeDef.run's missing-call guard.
            logger.warning(
                "McpBridge reached but no matching tool call found in state for tool_call_id=%s",
                tool_call_id,
            )
            return Silent()

        # Parse args. If args_as_dict raises, the agent's earlier dispatch
        # path would have caught it — but defensive in case state was
        # constructed differently. Surface as FailedToolCall.
        try:
            args = tool_call_part.args_as_dict()
        except Exception as e:
            failure = classify_mcp_error(e, tool_name=tool_call_part.tool_name, tool_call_id=tool_call_part.tool_call_id)
            ctx.state.add_tool_result(tool_call_part.tool_call_id, failure)
            return ReturnCall[State](state=ctx.state)

        # Idempotency check — only for non-idempotent tools. Idempotent
        # tools are safe to re-run; we save memory by not tracking them.
        cache_key = make_cache_key(tool_call_id, args)
        if not self._tool_def.idempotent:
            cached = self._dedup_cache.get(cache_key)
            if cached is not None:
                logger.info(
                    "[%s] mcp_bridge dedup HIT tool=%s tool_call_id=%s — serving cached result",
                    ctx.deps.correlation_id[:8],
                    self._tool_def.name,
                    tool_call_id,
                )
                ctx.state.add_tool_result(tool_call_part.tool_call_id, cached)
                return ReturnCall[State](state=ctx.state)

        # Resolve the per-call meta hook. Failures here are operator-visible
        # (the hook is calfkit-internal config, not LLM-visible).
        tool_call_ctx = ToolContext(
            deps=ctx.deps,
            agent_name=ctx.emitter_node_id,
            tool_call_id=tool_call_part.tool_call_id,
            tool_name=tool_call_part.tool_name,
            messages=ctx.state.message_history,
            run_id=ctx.deps.correlation_id,
        )
        try:
            meta = await _resolve_meta(self._server, tool_call_ctx)
        except Exception as e:
            logger.exception(
                "[%s] mcp_bridge meta hook raised for tool=%s tool_call_id=%s",
                ctx.deps.correlation_id[:8],
                self._tool_def.name,
                tool_call_id,
            )
            failure = classify_mcp_error(e, tool_name=tool_call_part.tool_name, tool_call_id=tool_call_part.tool_call_id)
            ctx.state.add_tool_result(tool_call_part.tool_call_id, failure)
            return ReturnCall[State](state=ctx.state)

        # Verify session is open. Without this, the dispatch line would
        # raise AttributeError which surfaces as a misleading FailedToolCall.
        if self._server.session is None:
            logger.error(
                "[%s] mcp_bridge tool=%s reached run() before McpServer._open_bridge_session ran",
                ctx.deps.correlation_id[:8],
                self._tool_def.name,
            )
            failure = classify_mcp_error(
                RuntimeError(f"McpServer({self._server.raw_name!r}) has no open session — bridge run() before Worker startup completed"),
                tool_name=tool_call_part.tool_name,
                tool_call_id=tool_call_part.tool_call_id,
            )
            ctx.state.add_tool_result(tool_call_part.tool_call_id, failure)
            return ReturnCall[State](state=ctx.state)

        # Dispatch to MCP. The session is shared with sibling bridges; the
        # MCP SDK pipelines via JSON-RPC request IDs so concurrent calls
        # from different bridges are fine.
        try:
            mcp_result = await self._server.session.call_tool(
                self._tool_def.name,
                args,
                meta=meta,
            )
        except (MemoryError, RecursionError, SystemError):
            # Process-level health issues must propagate as the operator's
            # signal that the worker is sick; converting to FailedToolCall
            # would mask them as a tool-level fault.
            raise
        except Exception as e:
            logger.exception(
                "[%s] mcp_bridge dispatch failed tool=%s tool_call_id=%s raised %s",
                ctx.deps.correlation_id[:8],
                self._tool_def.name,
                tool_call_id,
                type(e).__name__,
            )
            failure = classify_mcp_error(e, tool_name=tool_call_part.tool_name, tool_call_id=tool_call_part.tool_call_id)
            ctx.state.add_tool_result(tool_call_part.tool_call_id, failure)
            return ReturnCall[State](state=ctx.state)

        # Branch on isError: tool-semantic errors → RetryPromptPart;
        # successes → ToolReturn (and cached for dedup if applicable).
        if mcp_result.isError:
            retry_prompt = build_retry_prompt_from_error_result(
                mcp_result,
                tool_name=tool_call_part.tool_name,
                tool_call_id=tool_call_part.tool_call_id,
            )
            ctx.state.add_tool_result(tool_call_part.tool_call_id, retry_prompt)
            return ReturnCall[State](state=ctx.state)

        tool_return = adapt_call_tool_result(mcp_result, tool_call_id=tool_call_part.tool_call_id)

        # Cache successful results for dedup (skip if tool is idempotent —
        # see _dedup.py docstring for rationale).
        if not self._tool_def.idempotent:
            self._dedup_cache.put(cache_key, tool_return)

        ctx.state.add_tool_result(tool_call_part.tool_call_id, tool_return)
        logger.debug(
            "[%s] mcp_bridge completed tool=%s tool_call_id=%s",
            ctx.deps.correlation_id[:8],
            self._tool_def.name,
            tool_call_id,
        )
        return ReturnCall[State](state=ctx.state)


# ---------------------------------------------------------------------------
# Per-call meta hook resolution
# ---------------------------------------------------------------------------


async def _resolve_meta(server: McpServer, ctx: ToolContext) -> dict[str, Any] | None:
    """Resolve the per-call ``meta=`` hook for one dispatch.

    Supports:
    - ``None`` → no meta sent
    - ``dict`` → sent as-is (constant for the session's lifetime)
    - Sync callable ``(ToolContext) -> dict``
    - Async callable ``(ToolContext) -> Awaitable[dict]``

    Hook return type is not strictly validated — if the user returns
    something other than a dict / None, the MCP SDK's call_tool will
    raise downstream (and we'll surface as FailedToolCall).
    """
    hook = server.meta_hook
    if hook is None:
        return None
    if callable(hook):
        result = hook(ctx)
        if inspect.isawaitable(result):
            result = await result
        return result
    # Constant dict.
    return hook
