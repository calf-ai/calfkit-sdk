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
# Resources are a per-owner plain ``dict`` (no provenance / claim machinery).
# ---------------------------------------------------------------------------


def test_owner_resources_default_is_a_plain_dict() -> None:
    from calfkit.worker.lifecycle import LifecycleHookMixin

    class _Owner(LifecycleHookMixin):
        pass

    owner = _Owner()
    assert isinstance(owner.resources, dict)
    assert owner.resources == {}


def test_resource_dicts_are_per_owner_independent() -> None:
    from calfkit.worker.lifecycle import LifecycleHookMixin

    class _Owner(LifecycleHookMixin):
        pass

    a = _Owner()
    b = _Owner()
    a.resources["db"] = "a-pool"
    # Same key on a *different* owner's bag is independent.
    b.resources["db"] = "b-pool"
    assert a.resources["db"] == "a-pool"
    assert b.resources["db"] == "b-pool"


# ---------------------------------------------------------------------------
# B2 — _resource_cm closes the just-opened resource if setup of a later step
# fails, instead of leaking it.
# ---------------------------------------------------------------------------


async def test_resource_cm_pops_key_and_closes_on_exit() -> None:
    from collections.abc import Callable

    from calfkit.worker.lifecycle import _resource_cm

    class _Owner:
        """Minimal owner double satisfying ``SupportsLifecycleHooks``."""

        def __init__(self) -> None:
            self.resources: dict[str, Any] = {}

        def _hooks_for(self, phase: str) -> list[Callable[..., Any]]:
            return []

        def _resource_cms(self) -> list[tuple[str, Callable[..., Any]]]:
            return []

    owner = _Owner()
    closed = []

    async def genfn(_ctx: Any) -> Any:
        try:
            yield "resource-pool"
        finally:
            closed.append("closed")

    async with _resource_cm(owner, "db", genfn, setup_ctx=None):
        assert owner.resources["db"] == "resource-pool"

    # On exit the key is popped and the user generator's finally ran.
    assert "db" not in owner.resources
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
# B4 — the resources read-side is a Mapping on the no-resources (default) path
# for every injected surface. Read-only is now TYPE-LEVEL only (mypy), not a
# runtime guarantee.
# ---------------------------------------------------------------------------


def test_tool_context_default_resources_is_a_mapping() -> None:
    from collections.abc import Mapping

    from calfkit.models.tool_context import ToolContext

    ctx = ToolContext(deps={}, run_id="cid")
    assert isinstance(ctx.resources, Mapping)
    assert dict(ctx.resources) == {}


def test_node_result_default_resources_is_a_mapping() -> None:
    from collections.abc import Mapping

    from calfkit.client.node_result import NodeResult
    from calfkit.models.state import State

    result = NodeResult(output=None, state=State(), correlation_id="cid")
    assert isinstance(result.resources, Mapping)
    assert dict(result.resources) == {}


# ---------------------------------------------------------------------------
# B6 — SessionRunContext.resources reads through the stamped private attr, and
# the private attr stays deep-copy-safe (a plain dict, not a proxy).
# ---------------------------------------------------------------------------


def test_session_context_resources_property_reads_stamped_value() -> None:
    from collections.abc import Mapping

    from calfkit.models.session_context import SessionRunContext
    from calfkit.models.state import State

    ctx = SessionRunContext(state=State(), deps={})
    sentinel = object()
    # Stamp with a plain dict (what prepare_context stores: dict(self.resources)).
    ctx._resources = {"db": sentinel}
    assert isinstance(ctx.resources, Mapping)
    assert ctx.resources["db"] is sentinel


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
