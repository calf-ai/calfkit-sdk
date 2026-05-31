"""Unit + integration tests for ``calfkit.mcp._bridge``.

Drives ``McpBridge.run`` directly with ``FakeMcpServer`` as the underlying
session — same pattern as ``tests/test_tool_errors.py`` for native tool nodes.

Coverage focus:
- Topic naming derives from normalised server name + original tool name
- Successful dispatch stores ToolReturn in state, returns ReturnCall
- isError=True stores RetryPromptPart (LLM-visible, retryable)
- Raised MCPError stores FailedToolCall (operator-visible)
- Missing tool call in state returns Silent (no crash)
- meta hook (constant + sync callable + async callable) flows to MCP
- meta hook raising surfaces as FailedToolCall
- Idempotency cache: hit serves cached result without re-dispatching
- Idempotent tools (idempotentHint=True) bypass cache
- Failed/error results NOT cached (let them retry naturally)
- Session-not-open guard fails cleanly with FailedToolCall
"""

from __future__ import annotations

from typing import Any

from mcp.shared.exceptions import McpError
from mcp.types import CallToolResult, ErrorData, TextContent

from calfkit._vendor.pydantic_ai.messages import RetryPromptPart, ToolCallPart, ToolReturn
from calfkit.mcp._bridge import McpBridge
from calfkit.mcp._dedup import IdempotencyCache, make_cache_key
from calfkit.mcp._testing import FakeMcpServer
from calfkit.mcp._tool_def import McpToolAnnotations, McpToolDef
from calfkit.models import SessionRunContext, State
from calfkit.models.actions import ReturnCall, Silent
from calfkit.models.session_context import Deps
from calfkit.models.state import FailedToolCall

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _td(name: str = "t", *, idempotent: bool | None = None) -> McpToolDef:
    ann = McpToolAnnotations(idempotent_hint=idempotent) if idempotent is not None else None
    return McpToolDef(
        name=name,
        input_schema={"type": "object", "properties": {}},
        annotations=ann,
    )


def _make_ctx(state: State, correlation_id: str = "cid-bridge-test") -> SessionRunContext:
    return SessionRunContext(state=state, deps=Deps(correlation_id=correlation_id, provided_deps={}))


def _add_pending_tool_call(state: State, *, tool_name: str, tool_call_id: str, args: dict[str, Any] | None = None) -> ToolCallPart:
    """Register a tool call in state so bridge.run can find it."""
    part = ToolCallPart(tool_name=tool_name, args=args or {}, tool_call_id=tool_call_id)
    state.add_tool_call(part)
    return part


def _ok_result(text: str = "ok", *, structured: dict[str, Any] | None = None) -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text=text)],
        structuredContent=structured,
        isError=False,
    )


def _err_result(text: str = "permission denied") -> CallToolResult:
    return CallToolResult(content=[TextContent(type="text", text=text)], isError=True)


async def _open_fake(fake: FakeMcpServer) -> None:
    """Convenience: open the bridge session for a fake server."""
    await fake._open_bridge_session()


# ---------------------------------------------------------------------------
# Topic + identity wiring
# ---------------------------------------------------------------------------


def test_bridge_topic_naming_uses_normalized_server_name() -> None:
    fake = FakeMcpServer(name="my-srv", tools=[_td("search")], invoker=lambda n, a, m: _ok_result())
    bridge = McpBridge(server=fake, tool_def=_td("search"))
    assert bridge.subscribe_topics == ["mcp.my_srv.search.input"]
    assert bridge.publish_topic == "mcp.my_srv.search.output"
    assert bridge.node_id == "mcp_my_srv_search"


def test_bridge_uses_original_tool_name_for_dispatch() -> None:
    """tool_name property returns the MCP-server-side name (not LLM-facing rename)."""
    fake = FakeMcpServer(name="gmail", tools=[_td("send")], invoker=lambda n, a, m: _ok_result())
    bridge = McpBridge(server=fake, tool_def=_td("send"))
    assert bridge.tool_name == "send"


def test_bridge_creates_default_cache_if_none_provided() -> None:
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    bridge = McpBridge(server=fake, tool_def=_td())
    assert isinstance(bridge.dedup_cache, IdempotencyCache)


def test_bridge_accepts_shared_cache() -> None:
    """When Phase 4 Worker passes a shared cache, the bridge uses it."""
    shared = IdempotencyCache(max_entries=100)
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    bridge = McpBridge(server=fake, tool_def=_td(), dedup_cache=shared)
    assert bridge.dedup_cache is shared


# ---------------------------------------------------------------------------
# Happy path — successful dispatch
# ---------------------------------------------------------------------------


async def test_successful_dispatch_stores_tool_return() -> None:
    fake = FakeMcpServer(
        name="gmail",
        tools=[_td("search")],
        invoker=lambda n, a, m: _ok_result(text=f"results for {a.get('q')}"),
    )
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td("search"))

    state = State()
    _add_pending_tool_call(state, tool_name="search", tool_call_id="tc-1", args={"q": "hello"})
    ctx = _make_ctx(state)

    result = await bridge.run(ctx, "tc-1")

    assert isinstance(result, ReturnCall)
    stored = state.tool_results.get("tc-1")
    assert isinstance(stored, ToolReturn)
    assert stored.return_value == "results for hello"


async def test_successful_dispatch_prefers_structured_content() -> None:
    fake = FakeMcpServer(
        name="gmail",
        tools=[_td("search")],
        invoker=lambda n, a, m: _ok_result(text="human-readable", structured={"hits": [1, 2]}),
    )
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td("search"))

    state = State()
    _add_pending_tool_call(state, tool_name="search", tool_call_id="tc-1", args={"q": "x"})
    await bridge.run(_make_ctx(state), "tc-1")

    stored = state.tool_results.get("tc-1")
    assert isinstance(stored, ToolReturn)
    assert stored.return_value == {"hits": [1, 2]}  # structured wins


async def test_dispatch_forwards_args_and_meta_to_session() -> None:
    """The bridge forwards args and meta (resolved from server.meta_hook) to call_tool."""
    captured: list[tuple[str, dict[str, Any], dict[str, Any] | None]] = []

    def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        captured.append((name, args, meta))
        return _ok_result()

    fake = FakeMcpServer(
        name="gmail",
        tools=[_td("send")],
        invoker=invoker,
    )
    fake._meta = {"trace_id": "abc"}  # constant meta hook
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td("send"))

    state = State()
    _add_pending_tool_call(state, tool_name="send", tool_call_id="tc-1", args={"to": "a@b.c"})
    await bridge.run(_make_ctx(state), "tc-1")

    assert captured == [("send", {"to": "a@b.c"}, {"trace_id": "abc"})]


async def test_dispatch_with_sync_meta_callable() -> None:
    captured_meta: list[dict[str, Any] | None] = []

    def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        captured_meta.append(meta)
        return _ok_result()

    fake = FakeMcpServer(name="gmail", tools=[_td("send")], invoker=invoker)
    fake._meta = lambda ctx: {"user_id": ctx.deps.correlation_id}
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td("send"))

    state = State()
    _add_pending_tool_call(state, tool_name="send", tool_call_id="tc-1", args={})
    await bridge.run(_make_ctx(state, correlation_id="cid-xyz"), "tc-1")

    assert captured_meta == [{"user_id": "cid-xyz"}]


async def test_dispatch_with_async_meta_callable() -> None:
    captured_meta: list[dict[str, Any] | None] = []

    async def async_meta(ctx: Any) -> dict[str, Any]:
        return {"async_resolved": True}

    def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        captured_meta.append(meta)
        return _ok_result()

    fake = FakeMcpServer(name="gmail", tools=[_td("send")], invoker=invoker)
    fake._meta = async_meta
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td("send"))

    state = State()
    _add_pending_tool_call(state, tool_name="send", tool_call_id="tc-1", args={})
    await bridge.run(_make_ctx(state), "tc-1")

    assert captured_meta == [{"async_resolved": True}]


async def test_dispatch_with_no_meta_hook_passes_none() -> None:
    captured_meta: list[dict[str, Any] | None] = []

    def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        captured_meta.append(meta)
        return _ok_result()

    fake = FakeMcpServer(name="gmail", tools=[_td("send")], invoker=invoker)
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td("send"))

    state = State()
    _add_pending_tool_call(state, tool_name="send", tool_call_id="tc-1", args={})
    await bridge.run(_make_ctx(state), "tc-1")

    assert captured_meta == [None]


# ---------------------------------------------------------------------------
# Tool-semantic errors (isError=True) → RetryPromptPart
# ---------------------------------------------------------------------------


async def test_is_error_result_stores_retry_prompt() -> None:
    fake = FakeMcpServer(
        name="gmail",
        tools=[_td("search")],
        invoker=lambda n, a, m: _err_result("permission denied: needs scope X"),
    )
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td("search"))

    state = State()
    _add_pending_tool_call(state, tool_name="search", tool_call_id="tc-err", args={"q": "x"})
    result = await bridge.run(_make_ctx(state), "tc-err")

    assert isinstance(result, ReturnCall)
    stored = state.tool_results.get("tc-err")
    assert isinstance(stored, RetryPromptPart)
    assert stored.tool_name == "search"
    assert stored.tool_call_id == "tc-err"
    assert "permission denied" in str(stored.content)


# ---------------------------------------------------------------------------
# Transport / protocol errors → FailedToolCall
# ---------------------------------------------------------------------------


async def test_raised_mcp_error_stores_failed_tool_call() -> None:
    def boom(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        raise McpError(ErrorData(code=-32603, message="internal error", data=None))

    fake = FakeMcpServer(name="gmail", tools=[_td("send")], invoker=boom)
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td("send"))

    state = State()
    _add_pending_tool_call(state, tool_name="send", tool_call_id="tc-fail", args={})
    await bridge.run(_make_ctx(state), "tc-fail")

    stored = state.tool_results.get("tc-fail")
    assert isinstance(stored, FailedToolCall)
    assert stored.exc_type == "McpError(-32603)"
    assert "internal error" in stored.exc_message


async def test_generic_exception_stores_failed_tool_call() -> None:
    def boom(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        raise RuntimeError("unexpected")

    fake = FakeMcpServer(name="gmail", tools=[_td()], invoker=boom)
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td())

    state = State()
    _add_pending_tool_call(state, tool_name="t", tool_call_id="tc-fail", args={})
    await bridge.run(_make_ctx(state), "tc-fail")

    stored = state.tool_results.get("tc-fail")
    assert isinstance(stored, FailedToolCall)
    assert stored.exc_type == "RuntimeError"


# ---------------------------------------------------------------------------
# meta hook raises → FailedToolCall (operator-visible, NOT retryable)
# ---------------------------------------------------------------------------


async def test_meta_hook_raises_stores_failed_tool_call() -> None:
    captured_calls: list[Any] = []

    def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        captured_calls.append((name, args, meta))
        return _ok_result()

    fake = FakeMcpServer(name="gmail", tools=[_td()], invoker=invoker)

    def bad_meta(ctx: Any) -> dict[str, Any]:
        raise ValueError("bad meta hook")

    fake._meta = bad_meta
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td())

    state = State()
    _add_pending_tool_call(state, tool_name="t", tool_call_id="tc", args={})
    await bridge.run(_make_ctx(state), "tc")

    # MCP was NEVER called — the failure happened in the meta hook
    assert captured_calls == []
    stored = state.tool_results.get("tc")
    assert isinstance(stored, FailedToolCall)
    assert stored.exc_type == "ValueError"


# ---------------------------------------------------------------------------
# Missing tool call in state → Silent (no crash)
# ---------------------------------------------------------------------------


async def test_missing_tool_call_returns_silent() -> None:
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td())

    state = State()  # no tool call registered
    result = await bridge.run(_make_ctx(state), "tc-missing")

    assert isinstance(result, Silent)


# ---------------------------------------------------------------------------
# Session-not-open guard
# ---------------------------------------------------------------------------


async def test_session_not_open_returns_failed_tool_call() -> None:
    """If McpServer._open_bridge_session never ran, dispatch surfaces clearly."""
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    # NOT calling _open_fake(fake) — session is None
    bridge = McpBridge(server=fake, tool_def=_td())

    state = State()
    _add_pending_tool_call(state, tool_name="t", tool_call_id="tc-x", args={})
    await bridge.run(_make_ctx(state), "tc-x")

    stored = state.tool_results.get("tc-x")
    assert isinstance(stored, FailedToolCall)
    assert "no open session" in stored.exc_message.lower()


# ---------------------------------------------------------------------------
# Idempotency cache — hit / miss / non-idempotent / idempotent
# ---------------------------------------------------------------------------


async def test_dedup_cache_hit_skips_dispatch() -> None:
    """A redelivered envelope (same tool_call_id + args) is served from cache."""
    call_count = 0

    def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        nonlocal call_count
        call_count += 1
        return _ok_result(text=f"call #{call_count}")

    fake = FakeMcpServer(name="gmail", tools=[_td("send")], invoker=invoker)
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td("send"))

    state = State()
    _add_pending_tool_call(state, tool_name="send", tool_call_id="tc-dup", args={"x": 1})

    # First dispatch — hits MCP, caches result
    await bridge.run(_make_ctx(state), "tc-dup")
    assert call_count == 1
    first = state.tool_results.get("tc-dup")

    # Reset state (simulate redelivery)
    state2 = State()
    _add_pending_tool_call(state2, tool_name="send", tool_call_id="tc-dup", args={"x": 1})

    # Second dispatch — should serve from cache (no MCP call)
    await bridge.run(_make_ctx(state2), "tc-dup")
    assert call_count == 1  # NOT incremented
    second = state2.tool_results.get("tc-dup")
    assert second == first  # same cached value


async def test_dedup_cache_miss_when_args_differ() -> None:
    """Different args for the same tool_call_id → different cache key → re-dispatch."""
    call_count = 0

    def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        nonlocal call_count
        call_count += 1
        return _ok_result()

    fake = FakeMcpServer(name="gmail", tools=[_td("send")], invoker=invoker)
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td("send"))

    state1 = State()
    _add_pending_tool_call(state1, tool_name="send", tool_call_id="tc-x", args={"x": 1})
    await bridge.run(_make_ctx(state1), "tc-x")

    state2 = State()
    _add_pending_tool_call(state2, tool_name="send", tool_call_id="tc-x", args={"x": 2})
    await bridge.run(_make_ctx(state2), "tc-x")

    assert call_count == 2  # different args → re-dispatched


async def test_idempotent_tool_bypasses_cache() -> None:
    """Tools with idempotentHint=True are NOT cached — re-runs are safe by definition."""
    call_count = 0

    def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        nonlocal call_count
        call_count += 1
        return _ok_result()

    fake = FakeMcpServer(name="gmail", tools=[_td("search", idempotent=True)], invoker=invoker)
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td("search", idempotent=True))

    state1 = State()
    _add_pending_tool_call(state1, tool_name="search", tool_call_id="tc-dup", args={"q": "hello"})
    await bridge.run(_make_ctx(state1), "tc-dup")

    state2 = State()
    _add_pending_tool_call(state2, tool_name="search", tool_call_id="tc-dup", args={"q": "hello"})
    await bridge.run(_make_ctx(state2), "tc-dup")

    assert call_count == 2  # idempotent → re-dispatched both times
    # And cache stays empty
    assert make_cache_key("tc-dup", {"q": "hello"}) not in bridge.dedup_cache


async def test_failed_dispatch_not_cached() -> None:
    """A FailedToolCall is NOT cached — let retries actually retry."""
    call_count = 0

    def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        nonlocal call_count
        call_count += 1
        raise RuntimeError(f"call #{call_count} failed")

    fake = FakeMcpServer(name="x", tools=[_td()], invoker=invoker)
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td())

    for i in range(3):
        state = State()
        _add_pending_tool_call(state, tool_name="t", tool_call_id="tc-x", args={})
        await bridge.run(_make_ctx(state), "tc-x")

    assert call_count == 3  # all three failures actually executed


async def test_is_error_result_not_cached() -> None:
    """A RetryPromptPart (is_error=True) is NOT cached.

    If the LLM retries the same call, the server may have transient state
    that changes the outcome. Don't lock in the prior error.
    """
    call_count = 0

    def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        nonlocal call_count
        call_count += 1
        return _err_result()

    fake = FakeMcpServer(name="x", tools=[_td()], invoker=invoker)
    await _open_fake(fake)
    bridge = McpBridge(server=fake, tool_def=_td())

    for i in range(3):
        state = State()
        _add_pending_tool_call(state, tool_name="t", tool_call_id="tc-x", args={})
        await bridge.run(_make_ctx(state), "tc-x")

    assert call_count == 3  # all three tool-errors actually executed


# ---------------------------------------------------------------------------
# Shared cache across multiple bridges (Phase 4 will pass shared)
# ---------------------------------------------------------------------------


async def test_shared_cache_across_bridges() -> None:
    """When two bridges share a cache, a hit on one is visible to the other.

    Not common in practice (each bridge handles one tool, so cache keys
    don't collide across bridges) — but the shared-cache contract should hold.
    """
    shared = IdempotencyCache(max_entries=100)
    call_count = 0

    def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        nonlocal call_count
        call_count += 1
        return _ok_result()

    fake = FakeMcpServer(
        name="gmail",
        tools=[_td("send"), _td("search")],
        invoker=invoker,
    )
    await _open_fake(fake)
    bridge_send = McpBridge(server=fake, tool_def=_td("send"), dedup_cache=shared)
    bridge_search = McpBridge(server=fake, tool_def=_td("search"), dedup_cache=shared)

    # Different bridges — but if they happened to share a cache key,
    # the cache would dedup. Verify the cache state across bridges.
    state1 = State()
    _add_pending_tool_call(state1, tool_name="send", tool_call_id="tc-1", args={"to": "x"})
    await bridge_send.run(_make_ctx(state1), "tc-1")
    assert len(shared) == 1

    state2 = State()
    _add_pending_tool_call(state2, tool_name="search", tool_call_id="tc-2", args={"q": "y"})
    await bridge_search.run(_make_ctx(state2), "tc-2")
    assert len(shared) == 2


# ---------------------------------------------------------------------------
# Subscriber input parameter detection
# ---------------------------------------------------------------------------


def test_bridge_run_accepts_input() -> None:
    """The framework's _run_accepts_input introspection picks up our run(ctx, tool_call_id) signature.

    Without this, the worker handler would never pass tool_call_id, breaking dispatch.
    """
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    bridge = McpBridge(server=fake, tool_def=_td())
    assert bridge._run_accepts_input is True
