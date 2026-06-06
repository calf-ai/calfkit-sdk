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
        self._hooks = hooks or {}
        self._cms = resource_cms or []
        self.resources: dict[str, Any] = {}

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


def test_worker_blank_name_raises_value_error() -> None:
    with pytest.raises(ValueError, match="non-empty string"):
        _make_worker(name="   ")


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
    owner.resources["db"] = "pool"

    ctx = worker._make_ctx(owner, "serving")

    assert isinstance(ctx, ServingContext)
    assert ctx.owner is owner
    assert ctx.broker is worker._client.broker
    # Serving resources share the owner's plain-dict bag (read-only by type only).
    assert ctx.resources is owner.resources
    assert ctx.resources["db"] == "pool"


def test_make_ctx_resource_returns_writable_lifecycle_context() -> None:
    from calfkit.worker.lifecycle import LifecycleContext

    worker = _make_worker()
    owner = _FakeOwner()

    ctx = worker._make_ctx(owner, "resource")

    assert isinstance(ctx, LifecycleContext)
    assert ctx.owner is owner
    # The context wraps the owner's plain-dict bag; writes land in it directly.
    assert ctx.resources is owner.resources
    ctx.resources["db"] = "pool"
    assert owner.resources["db"] == "pool"


# ---------------------------------------------------------------------------
# Engine: _owner_cms (§3.3)
# ---------------------------------------------------------------------------


async def test_owner_cms_resource_brackets_win_over_callbacks_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """One pattern per owner: when both ``@resource`` and resource-phase
    callbacks are present, the ``@resource`` brackets win, the callbacks are
    ignored, and a warning is logged (mirrors FastAPI lifespan-vs-on_event)."""
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

    import logging

    with caplog.at_level(logging.WARNING):
        cms = worker._owner_cms(owner, "resource")
    assert len(cms) == 1  # only the @resource bracket; the callback span is dropped
    assert any("callbacks are ignored" in r.message for r in caplog.records)

    from contextlib import AsyncExitStack

    async with AsyncExitStack() as stack:
        for cm in cms:
            await stack.enter_async_context(cm)
        assert owner.resources["db"] == "pool"

    # The callbacks never ran; only the @resource bracket entered/exited.
    assert events == ["res-enter", "res-exit"]


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


async def test_normal_stop_tears_down_resources_when_broker_stop_raises() -> None:
    """On a *clean* shutdown, FastStream skips after_shutdown (where the resource
    brackets are torn down) if broker.stop() raises. stop() must release the
    resource stack itself so a flaky drain never strands pools/clients."""
    from unittest.mock import AsyncMock, patch

    from faststream.kafka import TestKafkaBroker

    worker = _make_worker()
    torn_down: list[str] = []

    @worker.resource("db")
    async def db(ctx: Any) -> Any:
        try:
            yield "pool"
        finally:
            torn_down.append("resource")

    broker = worker._client.broker
    async with TestKafkaBroker(broker):
        await worker.start()
        assert worker.resources["db"] == "pool"  # opened, serving
        with patch.object(broker, "stop", new=AsyncMock(side_effect=RuntimeError("drain failed"))):
            with pytest.raises(RuntimeError, match="drain failed"):
                await worker.stop()

    # The failing drain surfaced, but the resource bracket still closed.
    assert torn_down == ["resource"]
    assert worker._resource_stack is None


async def test_after_startup_failure_tears_down_resources_when_broker_stop_raises() -> None:
    """Rollback must still tear down resources even if the drain (broker.stop)
    itself raises — the guarded broker.stop in _hook_after_startup logs and
    continues to resource teardown (covers worker.py:353-354)."""
    from unittest.mock import AsyncMock, patch

    from faststream.kafka import TestKafkaBroker

    worker = _make_worker()
    torn_down: list[str] = []

    @worker.resource("db")
    async def db(ctx: Any) -> Any:
        try:
            yield "pool"
        finally:
            torn_down.append("resource")

    @worker.after_startup
    async def boom(ctx: Any) -> None:
        raise RuntimeError("serving boom")

    broker = worker._client.broker
    async with TestKafkaBroker(broker):
        with patch.object(broker, "stop", new=AsyncMock(side_effect=RuntimeError("drain failed"))):
            # The original after_startup error surfaces (not the drain error).
            with pytest.raises(RuntimeError, match="serving boom"):
                await worker.start()

    # A failing drain did not strand the resource bracket.
    assert torn_down == ["resource"]
    assert worker._resource_stack is None


# ---------------------------------------------------------------------------
# broker.start() failure tears down resources + MCP across all run surfaces
# ---------------------------------------------------------------------------


async def test_broker_start_failure_tears_down_resources_via_start() -> None:
    """If broker.start() raises (e.g. Kafka unreachable), FastStream runs no
    shutdown hooks — Worker.start() must run its own teardown so the resource
    bracket opened in on_startup never leaks."""
    from unittest.mock import AsyncMock, patch

    from faststream.kafka import TestKafkaBroker

    worker = _make_worker()
    torn_down: list[str] = []

    @worker.resource("db")
    async def db(ctx: Any) -> Any:
        try:
            yield "pool"
        finally:
            torn_down.append("resource")

    broker = worker._client.broker
    async with TestKafkaBroker(broker):
        with patch.object(broker, "start", new=AsyncMock(side_effect=RuntimeError("broker down"))):
            with pytest.raises(RuntimeError, match="broker down"):
                await worker.start()

    assert torn_down == ["resource"]
    assert worker._resource_stack is None


async def test_broker_start_failure_tears_down_resources_via_async_with() -> None:
    """Python skips __aexit__ when __aenter__ raises, so the self-cleaning
    start() is what makes `async with worker:` recover from a boot failure."""
    from unittest.mock import AsyncMock, patch

    from faststream.kafka import TestKafkaBroker

    worker = _make_worker()
    torn_down: list[str] = []

    @worker.resource("db")
    async def db(ctx: Any) -> Any:
        try:
            yield "pool"
        finally:
            torn_down.append("resource")

    broker = worker._client.broker
    async with TestKafkaBroker(broker):
        with patch.object(broker, "start", new=AsyncMock(side_effect=RuntimeError("broker down"))):
            with pytest.raises(RuntimeError, match="broker down"):
                async with worker:
                    pass  # pragma: no cover — __aenter__ raises before the body

    assert torn_down == ["resource"]
    assert worker._resource_stack is None


async def test_run_startup_failure_tears_down_resources() -> None:
    """run() never reaches its own _shutdown() when startup fails inside the
    task group, so Worker.run() must run teardown on the failure path too."""
    from unittest.mock import AsyncMock, patch

    from faststream.kafka import TestKafkaBroker

    worker = _make_worker()
    torn_down: list[str] = []

    @worker.resource("db")
    async def db(ctx: Any) -> Any:
        try:
            yield "pool"
        finally:
            torn_down.append("resource")

    broker = worker._client.broker
    async with TestKafkaBroker(broker):
        with patch.object(broker, "start", new=AsyncMock(side_effect=RuntimeError("broker down"))):
            with pytest.raises(RuntimeError, match="broker down"):
                await worker.run()

    assert torn_down == ["resource"]
    assert worker._resource_stack is None


# ---------------------------------------------------------------------------
# Happy-path full lifecycle: all four phases fire in order (start/stop)
# ---------------------------------------------------------------------------


async def test_full_lifecycle_runs_all_phases_in_order() -> None:
    """Resource-pattern owner: ``@resource`` bracket + serving callbacks.

    Per the "one pattern per owner" rule, resource-phase callbacks
    (``on_startup``/``after_shutdown``) are *not* mixed with ``@resource`` here —
    that combination is covered separately. The serving callbacks
    (``after_startup``/``on_shutdown``) run independently around the broker.
    """
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

    @worker.after_startup
    async def after_startup(ctx: Any) -> None:
        events.append("after_startup")

    @worker.on_shutdown
    async def on_shutdown(ctx: Any) -> None:
        events.append("on_shutdown")

    async with TestKafkaBroker(worker._client.broker):
        await worker.start()
        # During serving the @resource value is visible via the shared bag.
        assert worker.resources["db"] == "pool"
        await worker.stop()

    # Entry: resource bracket, then serving span. Teardown is LIFO: serving span
    # (on_shutdown) unwinds before the resource bracket (resource-exit).
    assert events == [
        "resource-enter",
        "after_startup",
        "on_shutdown",
        "resource-exit",
    ]
    # Resource key popped after stop (start/stop clean).
    assert "db" not in worker.resources


async def test_full_lifecycle_callback_pattern_runs_all_phases_in_order() -> None:
    """Callback-pattern owner: ``on_startup``/``after_shutdown`` callbacks own
    the resource bag (no ``@resource``); all four phases fire in order."""
    from faststream.kafka import TestKafkaBroker

    worker = _make_worker()
    events: list[str] = []

    @worker.on_startup
    async def on_startup(ctx: Any) -> None:
        events.append("on_startup")
        ctx.resources["db"] = "pool"

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
        assert worker.resources["db"] == "pool"
        await worker.stop()

    # Resource span (on_startup/after_shutdown) brackets the serving span
    # (after_startup/on_shutdown), with LIFO teardown.
    assert events == [
        "on_startup",
        "after_startup",
        "on_shutdown",
        "after_shutdown",
    ]


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
        seen_ids.append(ctx.owner.id)
        await ctx.broker.publish({"id": ctx.owner.id, "status": "up"}, topic="fleet.presence")

    async with TestKafkaBroker(worker._client.broker):
        await worker.start()
        await worker.stop()

    # The presence hook ran with the worker's wire id, against ctx.broker.
    assert seen_ids == ["fleet-1"]
