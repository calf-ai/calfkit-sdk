"""Integration tests for ``calfkit.worker.Worker`` MCP-server segregation
and lifecycle hooks.

Focus:
- Worker segregates McpServer from regular nodes (constructor + add_nodes)
- _on_startup opens sessions, constructs McpBridges from filtered tools,
  shares a single IdempotencyCache, registers handlers
- _on_startup BaseException cleanup closes opened sessions before re-raising
- _on_shutdown closes all opened sessions, isolates failures
- An end-to-end smoke test of the wired Worker without spinning up
  FastStream — verifies the bridges' subscriber wiring is callable.

For E2E with real Kafka + real MCP servers, see the Phase 8 lane.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from mcp.types import CallToolResult, TextContent

from calfkit.client.client import Client
from calfkit.mcp._dedup import IdempotencyCache
from calfkit.mcp._testing import FakeMcpServer
from calfkit.mcp._tool_def import McpToolDef
from calfkit.nodes import agent_tool
from calfkit.worker.worker import Worker

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client() -> Client:
    """Build a minimal Client backed by an in-memory KafkaBroker.

    Used for tests that don't actually run FastStream — we just need the
    .subscriber() / .publisher() decorators to be callable for the
    handler registration path. ``Client.connect`` is the canonical factory.
    """
    return Client.connect("memory")


def _td(name: str = "t") -> McpToolDef:
    return McpToolDef(name=name, input_schema={"type": "object"})


def _ok_result(text: str = "ok") -> CallToolResult:
    return CallToolResult(content=[TextContent(type="text", text=text)], isError=False)


@agent_tool
def _native_tool() -> str:
    """A regular calfkit @agent_tool for mixed-node tests."""
    return "native"


# ---------------------------------------------------------------------------
# Constructor segregation
# ---------------------------------------------------------------------------


def test_constructor_segregates_mcp_servers() -> None:
    client = _make_client()
    fake = FakeMcpServer(name="gmail", tools=[_td("search")], invoker=lambda n, a, m: _ok_result())
    worker = Worker(client, nodes=[fake, _native_tool])

    assert worker._mcp_servers == [fake]
    assert worker._nodes == [_native_tool]


def test_constructor_with_no_mcp_servers() -> None:
    client = _make_client()
    worker = Worker(client, nodes=[_native_tool])
    assert worker._mcp_servers == []
    assert worker._nodes == [_native_tool]


def test_constructor_with_only_mcp_servers() -> None:
    client = _make_client()
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    worker = Worker(client, nodes=[fake])
    assert worker._mcp_servers == [fake]
    assert worker._nodes == []


def test_constructor_creates_default_idempotency_cache() -> None:
    client = _make_client()
    worker = Worker(client)
    assert isinstance(worker._dedup_cache, IdempotencyCache)


def test_constructor_accepts_shared_idempotency_cache() -> None:
    client = _make_client()
    shared = IdempotencyCache(max_entries=42)
    worker = Worker(client, idempotency_cache=shared)
    assert worker._dedup_cache is shared


# ---------------------------------------------------------------------------
# add_nodes segregation
# ---------------------------------------------------------------------------


def test_add_nodes_segregates_mcp_servers() -> None:
    client = _make_client()
    worker = Worker(client)
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result())

    worker.add_nodes(fake, _native_tool)

    assert worker._mcp_servers == [fake]
    assert worker._nodes == [_native_tool]


def test_add_nodes_mixed_with_constructor() -> None:
    """add_nodes adds to what's already there from the constructor."""
    client = _make_client()
    fake1 = FakeMcpServer(name="a", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    fake2 = FakeMcpServer(name="b", tools=[_td()], invoker=lambda n, a, m: _ok_result())

    worker = Worker(client, nodes=[fake1])
    worker.add_nodes(fake2)

    assert worker._mcp_servers == [fake1, fake2]


# ---------------------------------------------------------------------------
# _on_startup happy path
# ---------------------------------------------------------------------------


async def test_on_startup_constructs_bridges_per_tool() -> None:
    """One McpBridge per filtered tool, all sharing the worker's cache."""
    client = _make_client()
    fake = FakeMcpServer(
        name="gmail",
        tools=[_td("search"), _td("send"), _td("draft")],
        invoker=lambda n, a, m: _ok_result(),
    )
    worker = Worker(client, nodes=[fake])

    await worker._on_startup()

    assert len(worker._mcp_bridges) == 3
    bridge_names = {b.tool_name for b in worker._mcp_bridges}
    assert bridge_names == {"search", "send", "draft"}


async def test_on_startup_opens_each_servers_session() -> None:
    client = _make_client()
    fake1 = FakeMcpServer(name="a", tools=[_td("t1")], invoker=lambda n, a, m: _ok_result())
    fake2 = FakeMcpServer(name="b", tools=[_td("t2")], invoker=lambda n, a, m: _ok_result())
    worker = Worker(client, nodes=[fake1, fake2])

    assert fake1.session is None
    assert fake2.session is None
    await worker._on_startup()
    assert fake1.session is not None
    assert fake2.session is not None


async def test_on_startup_passes_shared_cache_to_bridges() -> None:
    """All bridges share the worker's idempotency cache.

    Critical: a Kafka redelivery to a DIFFERENT bridge of the same tool_call_id
    must dedup, which only works if the cache is shared.
    """
    client = _make_client()
    shared = IdempotencyCache()
    fake = FakeMcpServer(name="x", tools=[_td("a"), _td("b")], invoker=lambda n, a, m: _ok_result())
    worker = Worker(client, nodes=[fake], idempotency_cache=shared)

    await worker._on_startup()

    for bridge in worker._mcp_bridges:
        assert bridge.dedup_cache is shared


async def test_on_startup_appends_bridges_to_nodes() -> None:
    """Bridges flow into the same _nodes list that register_handlers iterates."""
    client = _make_client()
    fake = FakeMcpServer(name="x", tools=[_td("a"), _td("b")], invoker=lambda n, a, m: _ok_result())
    worker = Worker(client, nodes=[fake, _native_tool])

    await worker._on_startup()

    # _nodes should now contain the native tool PLUS the two bridges
    assert _native_tool in worker._nodes
    for bridge in worker._mcp_bridges:
        assert bridge in worker._nodes


async def test_on_startup_calls_register_handlers() -> None:
    client = _make_client()
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    worker = Worker(client, nodes=[fake])

    assert worker._prepared is False
    await worker._on_startup()
    assert worker._prepared is True


async def test_on_startup_with_no_mcp_servers_still_registers() -> None:
    """A worker with no MCP servers still registers regular handlers via _on_startup."""
    client = _make_client()
    worker = Worker(client, nodes=[_native_tool])

    await worker._on_startup()

    assert worker._prepared is True
    assert worker._mcp_bridges == []


async def test_on_startup_only_constructs_bridges_for_declared_tools() -> None:
    """If a fake server declares a subset of tools, only those get bridges."""
    client = _make_client()
    # Declare two tools; only those become bridges.
    fake = FakeMcpServer(name="x", tools=[_td("a"), _td("b")], invoker=lambda n, a, m: _ok_result())
    worker = Worker(client, nodes=[fake])

    await worker._on_startup()

    assert {b.tool_name for b in worker._mcp_bridges} == {"a", "b"}


# ---------------------------------------------------------------------------
# _on_startup cancellation safety (BaseException cleanup)
# ---------------------------------------------------------------------------


async def test_on_startup_closes_opened_sessions_on_failure() -> None:
    """If _open_bridge_session raises on server N, sessions 0..N-1 are closed."""
    client = _make_client()

    fake_ok = FakeMcpServer(name="ok", tools=[_td()], invoker=lambda n, a, m: _ok_result())

    # Construct a fake whose _open_bridge_session raises
    class FailingFake(FakeMcpServer):
        async def _open_bridge_session(self) -> None:
            raise RuntimeError("simulated startup failure")

    failing = FailingFake(name="fail", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    worker = Worker(client, nodes=[fake_ok, failing])

    with pytest.raises(RuntimeError, match="simulated startup failure"):
        await worker._on_startup()

    # The good fake's session was opened, then closed during cleanup
    assert fake_ok.session is None  # closed
    assert failing.session is None  # never opened


async def test_on_startup_cleanup_survives_close_failure() -> None:
    """If a close() call also raises during cleanup, we log + continue."""
    client = _make_client()

    class UnclosableFake(FakeMcpServer):
        async def _close_bridge_session(self) -> None:
            raise RuntimeError("close also fails")

    class FailingFake(FakeMcpServer):
        async def _open_bridge_session(self) -> None:
            raise RuntimeError("startup fails")

    unclosable = UnclosableFake(name="unc", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    failing = FailingFake(name="f", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    worker = Worker(client, nodes=[unclosable, failing])

    # The ORIGINAL startup error should propagate, not the cleanup error
    with pytest.raises(RuntimeError, match="startup fails"):
        await worker._on_startup()


async def test_on_startup_catches_base_exception_not_just_exception() -> None:
    """Cleanup must run on CancelledError too (subclass of BaseException, not Exception)."""
    client = _make_client()

    fake_ok = FakeMcpServer(name="ok", tools=[_td()], invoker=lambda n, a, m: _ok_result())

    class CancelledOpen(FakeMcpServer):
        async def _open_bridge_session(self) -> None:
            import asyncio

            raise asyncio.CancelledError()

    cancelled = CancelledOpen(name="cancel", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    worker = Worker(client, nodes=[fake_ok, cancelled])

    import asyncio

    with pytest.raises(asyncio.CancelledError):
        await worker._on_startup()
    # fake_ok's session was opened then closed during cleanup
    assert fake_ok.session is None


# ---------------------------------------------------------------------------
# _on_shutdown
# ---------------------------------------------------------------------------


async def test_on_shutdown_closes_all_sessions() -> None:
    client = _make_client()
    fake1 = FakeMcpServer(name="a", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    fake2 = FakeMcpServer(name="b", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    worker = Worker(client, nodes=[fake1, fake2])

    await worker._on_startup()
    assert fake1.session is not None
    assert fake2.session is not None

    await worker._on_shutdown()
    assert fake1.session is None
    assert fake2.session is None


async def test_on_shutdown_skips_unopened_sessions() -> None:
    """A server that never had its session opened is silently skipped."""
    client = _make_client()
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    worker = Worker(client, nodes=[fake])
    # Do NOT call _on_startup — sessions remain None
    await worker._on_shutdown()  # should not raise


async def test_on_shutdown_isolates_failures() -> None:
    """A failing close() on one server doesn't block other servers from closing."""
    client = _make_client()

    class FailingClose(FakeMcpServer):
        async def _close_bridge_session(self) -> None:
            raise RuntimeError("close fails")

    fake_ok = FakeMcpServer(name="ok", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    failing = FailingClose(name="bad", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    worker = Worker(client, nodes=[fake_ok, failing])

    await worker._on_startup()
    # Both opened
    assert fake_ok.session is not None
    assert failing.session is not None

    await worker._on_shutdown()
    # fake_ok closed successfully; failing's exception logged + swallowed
    assert fake_ok.session is None


# ---------------------------------------------------------------------------
# Idempotent _on_startup → register_handlers
# ---------------------------------------------------------------------------


async def test_register_handlers_idempotent_on_second_call() -> None:
    """Calling register_handlers twice no longer raises — it's a no-op log."""
    client = _make_client()
    worker = Worker(client, nodes=[_native_tool])
    worker.register_handlers()  # first call
    worker.register_handlers()  # should NOT raise
    assert worker._prepared is True


async def test_on_startup_works_when_register_handlers_already_called() -> None:
    """The provider-style pattern (test calls register_handlers manually) +
    Worker.run lifecycle should coexist. The on_startup path should not
    raise if register_handlers was already called.
    """
    client = _make_client()
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    worker = Worker(client, nodes=[fake, _native_tool])

    # Simulate the test-provider pattern: register handlers BEFORE startup.
    # In real life that's the wrong order — bridges wouldn't be in _nodes
    # yet — but the idempotency guard prevents errors.
    worker.register_handlers()
    # Now run startup (would normally be called by FastStream)
    # This should NOT raise because register_handlers is now a no-op.
    await worker._on_startup()
    # Bridges were constructed even though they didn't get registered this run
    assert len(worker._mcp_bridges) == 1


# ---------------------------------------------------------------------------
# End-to-end smoke: drive a bridge through Worker-wired state
# ---------------------------------------------------------------------------


async def test_e2e_worker_dispatches_via_constructed_bridge() -> None:
    """After Worker._on_startup, the McpBridge can be directly invoked and
    dispatches to the FakeMcpServer's invoker. This is the cross-Phase
    integration sanity check.
    """
    from calfkit._vendor.pydantic_ai.messages import ToolCallPart, ToolReturn
    from calfkit.models import SessionRunContext, State
    from calfkit.models.session_context import Deps

    captured: list[tuple[str, dict[str, Any], dict[str, Any] | None]] = []

    def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        captured.append((name, args, meta))
        return _ok_result(f"called {name}")

    client = _make_client()
    fake = FakeMcpServer(name="gmail", tools=[_td("search")], invoker=invoker)
    worker = Worker(client, nodes=[fake])
    await worker._on_startup()

    # We have one bridge; drive its run() directly
    assert len(worker._mcp_bridges) == 1
    bridge = worker._mcp_bridges[0]

    state = State()
    state.add_tool_call(ToolCallPart(tool_name="search", args={"q": "hi"}, tool_call_id="tc-e2e"))
    ctx = SessionRunContext(state=state, deps=Deps(correlation_id="cid-e2e", provided_deps={}))

    await bridge.run(ctx, "tc-e2e")

    # The invoker was called with the right args + None meta
    assert captured == [("search", {"q": "hi"}, None)]
    # The result is a ToolReturn stored in state
    stored = state.tool_results.get("tc-e2e")
    assert isinstance(stored, ToolReturn)
    assert stored.return_value == "called search"

    # Cleanup
    await worker._on_shutdown()
    assert fake.session is None


# ---------------------------------------------------------------------------
# Worker.run construction (smoke; doesn't actually start)
# ---------------------------------------------------------------------------


def test_worker_run_constructs_faststream_with_hooks() -> None:
    """Verify Worker.run wires the lifecycle hooks into FastStream construction.

    We mock FastStream itself so the test doesn't block on app.run().
    """
    from unittest.mock import AsyncMock, patch

    client = _make_client()
    worker = Worker(client)

    mock_app = MagicMock()
    mock_app.run = AsyncMock()

    with patch("calfkit.worker.worker.FastStream", return_value=mock_app) as mock_faststream_cls:
        # Run it in an event loop briefly
        import asyncio

        asyncio.run(worker.run())

    # FastStream was constructed with on_startup and on_shutdown
    _, kwargs = mock_faststream_cls.call_args
    assert kwargs["on_startup"] == [worker._on_startup]
    assert kwargs["on_shutdown"] == [worker._on_shutdown]
    mock_app.run.assert_awaited_once()
