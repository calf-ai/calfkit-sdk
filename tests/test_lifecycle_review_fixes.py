"""Regression tests for bugs found in the PR review of the lifecycle-hooks feature.

Each test pins a defect the multi-agent review surfaced in the *real* (passing)
implementation that the feature's own tests missed. See
``docs/research/pr-review-*.md``.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# B1 — direct ``bag[k] = v`` writes must funnel through claim() so the
# "one owner per key" invariant holds for ALL writers, not just the views.
# ---------------------------------------------------------------------------


def test_direct_bag_write_records_provenance_and_collides() -> None:
    from calfkit.exceptions import LifecycleConfigError
    from calfkit.worker.lifecycle import _ResourceBag

    bag = _ResourceBag()
    bag["db"] = "pool"  # direct write (the path tests/seeds use)

    # A different claimant writing the same key must collide — the direct write
    # established provenance instead of silently leaving the key unclaimed.
    with pytest.raises(LifecycleConfigError):
        bag.claim("db", "other", "@resource('db')")


def test_resource_bag_release_pops_value_and_provenance() -> None:
    from calfkit.worker.lifecycle import _ResourceBag

    bag = _ResourceBag()
    bag.claim("db", "pool", "@resource('db')")
    bag.release("db")

    assert "db" not in bag
    # Provenance was cleared too, so a different owner may now claim the key.
    bag.claim("db", "other", "a callback")
    assert bag["db"] == "other"


def test_resource_bags_are_per_owner_independent() -> None:
    from calfkit.worker.lifecycle import _ResourceBag

    a = _ResourceBag()
    b = _ResourceBag()
    a.claim("db", "a-pool", "@resource('db')")
    # Same key on a *different* owner's bag must not collide.
    b.claim("db", "b-pool", "@resource('db')")
    assert a["db"] == "a-pool"
    assert b["db"] == "b-pool"


# ---------------------------------------------------------------------------
# B2 — _resource_cm must close the just-opened resource if claim() collides,
# instead of leaking it (claim was outside the try).
# ---------------------------------------------------------------------------


async def test_resource_cm_closes_resource_when_claim_collides() -> None:
    from calfkit.exceptions import LifecycleConfigError
    from calfkit.worker.lifecycle import _resource_cm, _ResourceBag

    class _Owner:
        def __init__(self) -> None:
            self.resources = _ResourceBag()

    owner = _Owner()
    # Pre-claim the key under a different owner so the @resource claim collides.
    owner.resources.claim("db", "callback-pool", "a callback")

    closed = []

    async def genfn(_ctx: Any) -> Any:
        try:
            yield "resource-pool"
        finally:
            closed.append("closed")

    cm = _resource_cm(owner, "db", genfn, setup_ctx=None)
    with pytest.raises(LifecycleConfigError):
        await cm.__aenter__()

    # The generator was entered (resource opened); on the collision its finally
    # must have run so the resource is not leaked.
    assert closed == ["closed"]


# ---------------------------------------------------------------------------
# T1 — _safe_teardown lets CancelledError propagate (and aborts the rest),
# per plan §4. (Characterization of the documented cancellation behavior.)
# ---------------------------------------------------------------------------


async def test_safe_teardown_propagates_cancelled_error() -> None:
    from calfkit.worker.lifecycle import _safe_teardown

    ran: list[str] = []

    async def action(item: str) -> None:
        if item == "boom":
            raise asyncio.CancelledError
        ran.append(item)

    # Reversed order: "c", then "boom" (cancels), so "a"/"b" must NOT run.
    with pytest.raises(asyncio.CancelledError):
        await _safe_teardown(["a", "b", "boom", "c"], action, "test")
    assert ran == ["c"]


# ---------------------------------------------------------------------------
# B4 — the read-only resources guarantee must hold on the no-resources
# (default) path for every injected surface, not only SessionRunContext.
# ---------------------------------------------------------------------------


def test_tool_context_default_resources_is_read_only() -> None:
    from calfkit.models.tool_context import ToolContext

    ctx = ToolContext(deps={}, run_id="cid")
    with pytest.raises(TypeError):
        ctx.resources["db"] = object()  # type: ignore[index]


def test_node_result_default_resources_is_read_only() -> None:
    from calfkit.client.node_result import NodeResult
    from calfkit.models.state import State

    result = NodeResult(output=None, state=State(), correlation_id="cid")
    with pytest.raises(TypeError):
        result.resources["db"] = object()  # type: ignore[index]


# ---------------------------------------------------------------------------
# B6 — SessionRunContext.resources is read-only via the property (not only
# when stamped with a proxy), and the private attr stays deep-copy-safe.
# ---------------------------------------------------------------------------


def test_session_context_resources_property_is_read_only_when_set() -> None:
    from calfkit.models.session_context import SessionRunContext
    from calfkit.models.state import State

    ctx = SessionRunContext(state=State(), deps={})
    # Stamp with a plain dict (what the fixed prepare_context stores).
    ctx._resources = {"db": object()}
    with pytest.raises(TypeError):
        ctx.resources["db"] = object()  # type: ignore[index]


def test_stamped_session_context_is_deep_copyable() -> None:
    from calfkit.models.session_context import SessionRunContext
    from calfkit.models.state import State

    ctx = SessionRunContext(state=State(), deps={})
    ctx._resources = {"db": object()}  # a plain mapping, not a read-only proxy
    # Deep-copying a stamped context must not crash (a proxy is not deep-copyable).
    copied = ctx.model_copy(deep=True)
    assert set(copied.resources) == {"db"}


# ---------------------------------------------------------------------------
# B5 — Worker.stop() before start() is a no-op, not a raw AttributeError that
# leaks the internal _app attribute.
# ---------------------------------------------------------------------------


async def test_worker_stop_before_start_is_noop() -> None:
    from calfkit.client import Client
    from calfkit.worker import Worker

    worker = Worker(Client.connect())
    # Must not raise AttributeError('_app'); stopping a never-started worker is a no-op.
    await worker.stop()


# ---------------------------------------------------------------------------
# B3 — after_startup-failure rollback drains the broker BEFORE tearing down
# resource brackets (a draining handler must never read a popped bag).
# ---------------------------------------------------------------------------


async def test_after_startup_failure_stops_broker_before_resource_teardown() -> None:
    from unittest.mock import AsyncMock, patch

    from faststream.kafka import TestKafkaBroker

    from calfkit.client import Client
    from calfkit.worker import Worker

    order: list[str] = []
    worker = Worker(Client.connect())
    broker = worker._client.broker

    @worker.resource("db")
    async def _db(_ctx: Any) -> Any:
        try:
            yield "pool"
        finally:
            order.append("resource_teardown")

    @worker.after_startup
    async def _boom(_ctx: Any) -> None:
        raise RuntimeError("after_startup boom")

    async def _record_stop(*_a: Any, **_k: Any) -> None:
        order.append("broker_stop")

    # Patch the spy INSIDE the TestKafkaBroker context (it patches the broker on
    # entry; an earlier assignment would be overridden).
    async with TestKafkaBroker(broker):
        with patch.object(broker, "stop", new=AsyncMock(side_effect=_record_stop)):
            with pytest.raises(RuntimeError, match="after_startup boom"):
                await worker.start()

    assert "broker_stop" in order and "resource_teardown" in order
    assert order.index("broker_stop") < order.index("resource_teardown")
