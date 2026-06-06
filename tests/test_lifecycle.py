"""Unit tests for ``calfkit/worker/lifecycle.py`` (Phase 1 — foundation).

These are pure units: no Kafka. The context-manager builders are exercised
against a tiny fake owner double that exposes only the surface the builders
read: ``_hooks_for``, ``_resource_cms``, and ``resources``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import pytest

from calfkit.exceptions import LifecycleConfigError

if TYPE_CHECKING:
    from calfkit.worker.lifecycle import LifecycleHookMixin


class _FakeOwner:
    """Minimal owner double exposing only what the CM builders read.

    Real nodes/workers grow this surface via ``LifecycleHookMixin``; here we
    inject hooks and ``@resource`` generators directly to keep the CM-builder
    units Kafka-free and decoupled from the mixin.
    """

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


def test_lifecycle_config_error_is_an_exception() -> None:
    err = LifecycleConfigError("boom")
    assert isinstance(err, Exception)
    assert str(err) == "boom"


# ---------------------------------------------------------------------------
# Context dataclasses + owner aliases
#
# Resources are a plain ``dict``; the read-only-ness on ResourceSetupContext /
# ServingContext is TYPE-LEVEL only (mypy ``Mapping``), not enforced at runtime.
# ---------------------------------------------------------------------------


def test_lifecycle_context_exposes_owner() -> None:
    from calfkit.worker.lifecycle import LifecycleContext

    owner = object()
    ctx = LifecycleContext(owner, {})

    assert ctx.owner is owner


def test_lifecycle_context_resources_are_writable() -> None:
    from calfkit.worker.lifecycle import LifecycleContext

    bag: dict[str, Any] = {}
    ctx = LifecycleContext(object(), bag)

    ctx.resources["db"] = "pool"

    assert bag["db"] == "pool"


def test_resource_setup_context_exposes_owner_and_reads_resources() -> None:
    from calfkit.worker.lifecycle import ResourceSetupContext

    owner = object()
    ctx = ResourceSetupContext(owner, {"db": "pool"})

    assert ctx.owner is owner
    # Read-only is type-level only; reads work against the plain dict.
    assert ctx.resources["db"] == "pool"


def test_serving_context_carries_broker_and_resources() -> None:
    from faststream.kafka import KafkaBroker

    from calfkit.worker.lifecycle import ServingContext

    owner = object()
    # Constructing a KafkaBroker opens no connection; we never start it.
    broker = KafkaBroker()
    ctx = ServingContext(owner, {"db": "pool"}, broker)

    assert ctx.owner is owner
    assert ctx.broker is broker
    # Read-only is type-level only; reads work against the plain dict.
    assert ctx.resources["db"] == "pool"


# ---------------------------------------------------------------------------
# _maybe_await
# ---------------------------------------------------------------------------


async def test_maybe_await_returns_none_for_none() -> None:
    from calfkit.worker.lifecycle import _maybe_await

    assert await _maybe_await(None) is None


async def test_maybe_await_awaits_a_coroutine() -> None:
    from calfkit.worker.lifecycle import _maybe_await

    async def coro() -> str:
        return "done"

    assert await _maybe_await(coro()) == "done"


async def test_maybe_await_raises_type_error_for_generator() -> None:
    from calfkit.worker.lifecycle import _maybe_await

    def gen() -> Any:
        yield 1

    with pytest.raises(TypeError):
        await _maybe_await(gen())


# ---------------------------------------------------------------------------
# PHASE_PAIRS
# ---------------------------------------------------------------------------


def test_phase_pairs_maps_phases_and_resource_flags() -> None:
    from calfkit.worker.lifecycle import PHASE_PAIRS

    assert PHASE_PAIRS == {
        "resource": (("on_startup", "after_shutdown"), True),
        "serving": (("after_startup", "on_shutdown"), False),
    }


# ---------------------------------------------------------------------------
# _safe_teardown
# ---------------------------------------------------------------------------


async def test_safe_teardown_runs_items_in_reverse_order() -> None:
    from calfkit.worker.lifecycle import _safe_teardown

    seen: list[int] = []

    async def action(item: int) -> None:
        seen.append(item)

    await _safe_teardown([1, 2, 3], action, "test")

    assert seen == [3, 2, 1]


async def test_safe_teardown_logs_and_continues_on_exception() -> None:
    from calfkit.worker.lifecycle import _safe_teardown

    seen: list[int] = []

    async def action(item: int) -> None:
        if item == 2:
            raise RuntimeError("boom")
        seen.append(item)

    # Must not raise; the failing sibling does not abort the others.
    await _safe_teardown([1, 2, 3], action, "test")

    assert seen == [3, 1]


# ---------------------------------------------------------------------------
# LifecycleHook alias
# ---------------------------------------------------------------------------


def test_lifecycle_hook_alias_accepts_sync_and_async_hooks() -> None:
    from calfkit.worker.lifecycle import LifecycleHook

    def sync_hook(ctx: Any) -> None: ...

    async def async_hook(ctx: Any) -> None: ...

    # Both shapes are valid LifecycleHook values; the alias is a Callable type.
    hooks: list[LifecycleHook] = [sync_hook, async_hook]
    assert all(callable(h) for h in hooks)


# ---------------------------------------------------------------------------
# SupportsLifecycleHooks protocol
# ---------------------------------------------------------------------------


def test_supports_lifecycle_hooks_is_satisfied_by_fake_owner() -> None:
    from calfkit.worker.lifecycle import SupportsLifecycleHooks

    owner = _FakeOwner()

    assert isinstance(owner, SupportsLifecycleHooks)


def test_supports_lifecycle_hooks_rejects_plain_object() -> None:
    from calfkit.worker.lifecycle import SupportsLifecycleHooks

    assert not isinstance(object(), SupportsLifecycleHooks)


# ---------------------------------------------------------------------------
# _span_cm
# ---------------------------------------------------------------------------


async def test_span_cm_runs_enter_then_exit_hooks() -> None:
    from calfkit.worker.lifecycle import _span_cm

    events: list[str] = []

    async def enter_hook(ctx: Any) -> None:
        events.append("enter")

    async def exit_hook(ctx: Any) -> None:
        events.append("exit")

    owner = _FakeOwner(hooks={"on_startup": [enter_hook], "after_shutdown": [exit_hook]})
    ctx = object()

    async with _span_cm(owner, "on_startup", "after_shutdown", ctx):
        events.append("body")

    assert events == ["enter", "body", "exit"]


async def test_span_cm_setup_error_propagates() -> None:
    from calfkit.worker.lifecycle import _span_cm

    async def bad_enter(ctx: Any) -> None:
        raise RuntimeError("setup boom")

    owner = _FakeOwner(hooks={"on_startup": [bad_enter]})

    with pytest.raises(RuntimeError, match="setup boom"):
        async with _span_cm(owner, "on_startup", "after_shutdown", object()):
            pass


async def test_span_cm_exit_hook_error_does_not_raise() -> None:
    from calfkit.worker.lifecycle import _span_cm

    async def bad_exit(ctx: Any) -> None:
        raise RuntimeError("teardown boom")

    owner = _FakeOwner(hooks={"after_shutdown": [bad_exit]})

    # Teardown logs-never-raises, so the context manager exits cleanly.
    async with _span_cm(owner, "on_startup", "after_shutdown", object()):
        pass


async def test_span_cm_passes_ctx_to_hooks() -> None:
    from calfkit.worker.lifecycle import _span_cm

    received: list[Any] = []

    async def enter_hook(ctx: Any) -> None:
        received.append(ctx)

    owner = _FakeOwner(hooks={"on_startup": [enter_hook]})
    sentinel = object()

    async with _span_cm(owner, "on_startup", "after_shutdown", sentinel):
        pass

    assert received == [sentinel]


# ---------------------------------------------------------------------------
# _resource_cm
# ---------------------------------------------------------------------------


async def test_resource_cm_sets_value_on_enter_and_pops_on_exit() -> None:
    from calfkit.worker.lifecycle import ResourceSetupContext, _resource_cm

    closed: list[bool] = []

    async def genfn(ctx: Any) -> Any:
        try:
            yield "pool"
        finally:
            closed.append(True)

    owner = _FakeOwner()
    setup_ctx = ResourceSetupContext(owner, owner.resources)

    async with _resource_cm(owner, "db", genfn, setup_ctx):
        # Inside the bracket the value is stored in the owner's plain dict bag.
        assert owner.resources["db"] == "pool"

    # After exit the key is gone, and the user generator's finally ran.
    assert "db" not in owner.resources
    assert closed == [True]


async def test_resource_cm_setup_error_propagates() -> None:
    from calfkit.worker.lifecycle import ResourceSetupContext, _resource_cm

    async def genfn(ctx: Any) -> Any:
        raise RuntimeError("setup boom")
        yield  # pragma: no cover

    owner = _FakeOwner()
    setup_ctx = ResourceSetupContext(owner, owner.resources)

    with pytest.raises(RuntimeError, match="setup boom"):
        async with _resource_cm(owner, "db", genfn, setup_ctx):
            pass


async def test_resource_cm_teardown_error_does_not_raise() -> None:
    from calfkit.worker.lifecycle import ResourceSetupContext, _resource_cm

    async def genfn(ctx: Any) -> Any:
        try:
            yield "pool"
        finally:
            raise RuntimeError("teardown boom")

    owner = _FakeOwner()
    setup_ctx = ResourceSetupContext(owner, owner.resources)

    # Teardown is guarded log-not-raise, so the bracket exits cleanly and the
    # key is still popped.
    async with _resource_cm(owner, "db", genfn, setup_ctx):
        pass

    assert "db" not in owner.resources


# ---------------------------------------------------------------------------
# LifecycleHookMixin
# ---------------------------------------------------------------------------


def _mixin_owner() -> LifecycleHookMixin:
    from calfkit.worker.lifecycle import LifecycleHookMixin

    class _Owner(LifecycleHookMixin):
        pass

    return _Owner()


def test_mixin_resources_is_a_lazy_dict() -> None:
    owner = _mixin_owner()

    assert isinstance(owner.resources, dict)
    assert owner.resources == {}
    # Same bag returned on repeated access (lazily created once).
    assert owner.resources is owner.resources


def test_mixin_satisfies_supports_lifecycle_hooks_protocol() -> None:
    from calfkit.worker.lifecycle import SupportsLifecycleHooks

    assert isinstance(_mixin_owner(), SupportsLifecycleHooks)


@pytest.mark.parametrize("phase", ["on_startup", "after_startup", "on_shutdown", "after_shutdown"])
def test_mixin_decorator_registers_hook_for_phase(phase: str) -> None:
    owner = _mixin_owner()

    async def hook(ctx: Any) -> None: ...

    decorator = getattr(owner, phase)
    returned = decorator(hook)

    # Decorator returns the original function unchanged (composable / testable).
    assert returned is hook
    assert owner._hooks_for(phase) == [hook]


def test_mixin_hooks_for_unknown_phase_is_empty() -> None:
    owner = _mixin_owner()

    assert owner._hooks_for("on_startup") == []


def test_mixin_resource_decorator_registers_cm() -> None:
    owner = _mixin_owner()

    @owner.resource("db")
    async def db(ctx: Any) -> Any:
        yield "pool"

    cms = owner._resource_cms()
    assert [name for name, _ in cms] == ["db"]
    assert cms[0][1] is db


def test_mixin_duplicate_resource_name_raises() -> None:
    owner = _mixin_owner()

    @owner.resource("db")
    async def db1(ctx: Any) -> Any:
        yield "a"

    with pytest.raises(LifecycleConfigError):

        @owner.resource("db")
        async def db2(ctx: Any) -> Any:
            yield "b"


def test_mixin_storage_works_without_calling_init() -> None:
    """Dataclass node subclasses bypass ``__init__``; storage must be lazy."""
    from calfkit.worker.lifecycle import LifecycleHookMixin

    class _Owner(LifecycleHookMixin):
        def __init__(self) -> None:  # pragma: no cover - intentionally never called
            raise AssertionError("__init__ must not be required for hook storage")

    owner = _Owner.__new__(_Owner)

    async def hook(ctx: Any) -> None: ...

    owner.on_startup(hook)

    assert owner._hooks_for("on_startup") == [hook]
    assert isinstance(owner.resources, object)
