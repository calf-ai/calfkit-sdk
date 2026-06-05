"""Unit tests for the worker lifecycle engine (Phase 4).

These exercise ``calfkit/worker/worker.py``'s new lifecycle machinery:

- worker identity (``id``/``name``, read-only, uuid7 default),
- single-start guard,
- the engine methods ``_make_ctx`` / ``_owner_cms`` / ``_enter_into`` against
  fake owners (Kafka-free where possible),
- the four FastStream hooks wired around the existing MCP open/close logic.

Where a broker is needed we use the repo's ``TestKafkaBroker`` in-memory
simulation, mirroring the patterns in ``tests/test_gates.py`` etc.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import pytest

from calfkit.client import Client

if TYPE_CHECKING:
    from calfkit.worker import Worker


def _make_worker(**kwargs: Any) -> Worker:
    from calfkit.worker import Worker

    client = Client.connect()
    return Worker(client, **kwargs)


class _FakeOwner:
    """Owner double for the engine methods (mirrors tests/test_lifecycle.py)."""

    def __init__(
        self,
        hooks: dict[str, list[Callable[..., Any]]] | None = None,
        resource_cms: list[tuple[str, Callable[..., Any]]] | None = None,
    ) -> None:
        from calfkit.worker.lifecycle import _ResourceBag

        self._hooks = hooks or {}
        self._cms = resource_cms or []
        self.resources = _ResourceBag()

    def _hooks_for(self, phase: str) -> list[Callable[..., Any]]:
        return self._hooks.get(phase, [])

    def _resource_cms(self) -> list[tuple[str, Callable[..., Any]]]:
        return self._cms


# ---------------------------------------------------------------------------
# Worker identity (§2.5)
# ---------------------------------------------------------------------------


def test_worker_id_defaults_to_uuid7_hex() -> None:
    worker = _make_worker()
    # A uuid7 hex is 32 lowercase hex chars; just assert it is a non-empty str.
    assert isinstance(worker.id, str)
    assert worker.id


def test_worker_id_uses_provided_value() -> None:
    worker = _make_worker(id="fleet-worker-1")
    assert worker.id == "fleet-worker-1"


def test_worker_id_is_read_only_no_setter() -> None:
    worker = _make_worker(id="immutable")
    with pytest.raises(AttributeError):
        worker.id = "reassigned"  # type: ignore[misc]


def test_worker_blank_id_raises_value_error() -> None:
    with pytest.raises(ValueError, match="non-empty string"):
        _make_worker(id="   ")


def test_worker_name_defaults_to_id() -> None:
    worker = _make_worker(id="w1")
    assert worker.name == "w1"


def test_worker_name_uses_provided_value() -> None:
    worker = _make_worker(id="w1", name="Fleet Worker One")
    assert worker.name == "Fleet Worker One"


# ---------------------------------------------------------------------------
# Single-start guard (§3.6)
# ---------------------------------------------------------------------------


async def test_worker_double_start_raises_runtime_error() -> None:
    from faststream.kafka import TestKafkaBroker

    worker = _make_worker()
    async with TestKafkaBroker(worker._client.broker):
        await worker.start()
        with pytest.raises(RuntimeError, match="single-use"):
            await worker.start()
        await worker.stop()


async def test_worker_run_after_start_raises_runtime_error() -> None:
    from faststream.kafka import TestKafkaBroker

    worker = _make_worker()
    async with TestKafkaBroker(worker._client.broker):
        await worker.start()
        with pytest.raises(RuntimeError, match="single-use"):
            await worker.run()
        await worker.stop()


# ---------------------------------------------------------------------------
# Engine: _make_ctx (§3.3)
# ---------------------------------------------------------------------------


def test_make_ctx_serving_returns_serving_context_with_broker() -> None:
    from calfkit.worker.lifecycle import ServingContext

    worker = _make_worker()
    owner = _FakeOwner()

    ctx = worker._make_ctx(owner, "serving")

    assert isinstance(ctx, ServingContext)
    assert ctx.owner is owner
    assert ctx.broker is worker._client.broker
    # Serving resources are read-only.
    with pytest.raises(TypeError):
        ctx.resources["db"] = "x"  # type: ignore[index]


def test_make_ctx_resource_returns_writable_lifecycle_context() -> None:
    from calfkit.worker.lifecycle import LifecycleContext

    worker = _make_worker()
    owner = _FakeOwner()

    ctx = worker._make_ctx(owner, "resource")

    assert isinstance(ctx, LifecycleContext)
    assert ctx.owner is owner
    # Writes funnel through the bag's claim mechanism.
    ctx.resources["db"] = "pool"
    assert owner.resources["db"] == "pool"
    assert owner.resources._claims["db"] == "an on_startup/after_shutdown callback"


# ---------------------------------------------------------------------------
# Engine: _owner_cms (§3.3)
# ---------------------------------------------------------------------------


async def test_owner_cms_resource_includes_resource_bracket_and_span() -> None:
    worker = _make_worker()

    events: list[str] = []

    async def genfn(ctx: Any) -> Any:
        events.append("res-enter")
        try:
            yield "pool"
        finally:
            events.append("res-exit")

    async def on_startup(ctx: Any) -> None:
        events.append("cb-enter")

    async def after_shutdown(ctx: Any) -> None:
        events.append("cb-exit")

    owner = _FakeOwner(
        hooks={"on_startup": [on_startup], "after_shutdown": [after_shutdown]},
        resource_cms=[("db", genfn)],
    )

    cms = worker._owner_cms(owner, "resource")
    assert len(cms) == 2  # one @resource bracket + one callback span

    from contextlib import AsyncExitStack

    async with AsyncExitStack() as stack:
        for cm in cms:
            await stack.enter_async_context(cm)
        assert owner.resources["db"] == "pool"

    assert events == ["res-enter", "cb-enter", "cb-exit", "res-exit"]


def test_owner_cms_resource_omits_span_when_no_hooks() -> None:
    worker = _make_worker()

    async def genfn(ctx: Any) -> Any:
        yield "pool"

    owner = _FakeOwner(resource_cms=[("db", genfn)])

    cms = worker._owner_cms(owner, "resource")
    assert len(cms) == 1  # only the @resource bracket; no callback span


def test_owner_cms_serving_has_no_resource_brackets() -> None:
    worker = _make_worker()

    async def after_startup(ctx: Any) -> None: ...

    owner = _FakeOwner(
        hooks={"after_startup": [after_startup]},
        # A @resource on the owner must NOT be entered in the serving phase.
        resource_cms=[("db", lambda ctx: None)],
    )

    cms = worker._owner_cms(owner, "serving")
    assert len(cms) == 1  # only the serving span


def test_owner_cms_serving_empty_when_no_serving_hooks() -> None:
    worker = _make_worker()
    owner = _FakeOwner(hooks={"on_startup": [lambda ctx: None]})

    assert worker._owner_cms(owner, "serving") == []


# ---------------------------------------------------------------------------
# Engine: _enter_into + _owners (§3.3)
# ---------------------------------------------------------------------------


async def test_enter_into_runs_worker_first_then_nodes() -> None:
    from contextlib import AsyncExitStack

    worker = _make_worker(id="w1")

    order: list[str] = []

    async def worker_hook(ctx: Any) -> None:
        order.append("worker")

    async def node_hook(ctx: Any) -> None:
        order.append("node")

    worker.on_startup(worker_hook)
    node = _FakeOwner(hooks={"on_startup": [node_hook]})
    worker._nodes.append(node)  # type: ignore[arg-type]

    async with AsyncExitStack() as stack:
        await worker._enter_into(stack, "resource")

    assert order == ["worker", "node"]


# ---------------------------------------------------------------------------
# _hook_after_startup failure (§3.4 / §4)
# ---------------------------------------------------------------------------


async def test_after_startup_failure_unwinds_both_stacks_and_stops_broker() -> None:
    from unittest.mock import AsyncMock, patch

    from faststream.kafka import TestKafkaBroker

    worker = _make_worker()

    torn_down: list[str] = []

    # A resource bracket on the worker so we can prove the resource stack is
    # unwound when after_startup fails.
    @worker.resource("db")
    async def db(ctx: Any) -> Any:
        try:
            yield "pool"
        finally:
            torn_down.append("resource")

    # An after_startup hook that fails, triggering the rollback path.
    @worker.after_startup
    async def boom(ctx: Any) -> None:
        raise RuntimeError("serving boom")

    broker = worker._client.broker
    async with TestKafkaBroker(broker):
        # Spy on the calfkit-side broker.stop intent (TestKafkaBroker no-ops
        # the real disconnect, so we assert our call was made).
        with patch.object(broker, "stop", new=AsyncMock(wraps=broker.stop)) as stop_spy:
            with pytest.raises(RuntimeError, match="serving boom"):
                await worker.start()

    # Resource bracket was torn down, both stacks cleared, broker.stop called.
    assert torn_down == ["resource"]
    assert worker._serving_stack is None
    assert worker._resource_stack is None
    stop_spy.assert_awaited()


# ---------------------------------------------------------------------------
# _hook_on_startup partial failure rolls back resource stack + MCP (§3.4 / §4)
# ---------------------------------------------------------------------------


async def test_on_startup_failure_rolls_back_resource_stack_and_mcp() -> None:
    from faststream.kafka import TestKafkaBroker

    worker = _make_worker()

    torn_down: list[str] = []

    # A healthy resource that must be torn down (LIFO) when a later one fails.
    @worker.resource("ok")
    async def ok(ctx: Any) -> Any:
        try:
            yield "ok-value"
        finally:
            torn_down.append("ok")

    # A second resource whose setup raises, failing _enter_into mid-flight.
    @worker.resource("bad")
    async def bad(ctx: Any) -> Any:
        raise RuntimeError("resource setup boom")
        yield  # pragma: no cover

    # Spy that MCP close runs during rollback even though there are no MCP
    # servers — the rollback unconditionally calls _on_shutdown.
    mcp_closed: list[bool] = []
    original_on_shutdown = worker._on_shutdown

    async def tracking_on_shutdown() -> None:
        mcp_closed.append(True)
        await original_on_shutdown()

    worker._on_shutdown = tracking_on_shutdown  # type: ignore[method-assign]

    broker = worker._client.broker
    async with TestKafkaBroker(broker):
        with pytest.raises(RuntimeError, match="resource setup boom"):
            await worker._hook_on_startup()

    # The healthy "ok" resource was torn down; resource stack cleared; MCP
    # cleanup invoked.
    assert torn_down == ["ok"]
    assert worker._resource_stack is None
    assert mcp_closed == [True]


# ---------------------------------------------------------------------------
# Happy-path full lifecycle: all four phases fire in order (start/stop)
# ---------------------------------------------------------------------------


async def test_full_lifecycle_runs_all_phases_in_order() -> None:
    from faststream.kafka import TestKafkaBroker

    worker = _make_worker()
    events: list[str] = []

    @worker.resource("db")
    async def db(ctx: Any) -> Any:
        events.append("resource-enter")
        try:
            yield "pool"
        finally:
            events.append("resource-exit")

    @worker.on_startup
    async def on_startup(ctx: Any) -> None:
        events.append("on_startup")

    @worker.after_startup
    async def after_startup(ctx: Any) -> None:
        events.append("after_startup")

    @worker.on_shutdown
    async def on_shutdown(ctx: Any) -> None:
        events.append("on_shutdown")

    @worker.after_shutdown
    async def after_shutdown(ctx: Any) -> None:
        events.append("after_shutdown")

    async with TestKafkaBroker(worker._client.broker):
        await worker.start()
        # During serving the @resource value is visible to read-only views.
        assert worker.resources["db"] == "pool"
        await worker.stop()

    # Entry order per owner: @resource brackets, then callback span. Teardown
    # is LIFO within the resource stack, so after_shutdown (callback span) runs
    # before resource-exit (the @resource bracket).
    assert events == [
        "resource-enter",
        "on_startup",
        "after_startup",
        "on_shutdown",
        "after_shutdown",
        "resource-exit",
    ]
    # Resource key + provenance popped after stop (start/stop clean).
    assert "db" not in worker.resources
    assert "db" not in worker.resources._claims


async def test_async_context_manager_starts_and_stops() -> None:
    from faststream.kafka import TestKafkaBroker

    worker = _make_worker()
    events: list[str] = []

    @worker.after_startup
    async def up(ctx: Any) -> None:
        events.append("up")

    @worker.on_shutdown
    async def down(ctx: Any) -> None:
        events.append("down")

    async with TestKafkaBroker(worker._client.broker):
        async with worker:
            assert events == ["up"]
        assert events == ["up", "down"]


async def test_after_startup_can_publish_presence_with_worker_id() -> None:
    from faststream.kafka import TestKafkaBroker

    worker = _make_worker(id="fleet-1")
    seen_ids: list[str] = []

    @worker.after_startup
    async def announce(ctx: Any) -> None:
        seen_ids.append(ctx.worker.id)
        await ctx.broker.publish({"id": ctx.worker.id, "status": "up"}, topic="fleet.presence")

    async with TestKafkaBroker(worker._client.broker):
        await worker.start()
        await worker.stop()

    # The presence hook ran with the worker's wire id, against ctx.broker.
    assert seen_ids == ["fleet-1"]
