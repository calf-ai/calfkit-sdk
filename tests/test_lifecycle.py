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
        from calfkit.worker.lifecycle import _ResourceBag

        self._hooks = hooks or {}
        self._cms = resource_cms or []
        self.resources = _ResourceBag()

    def _hooks_for(self, phase: str) -> list[Callable[..., Any]]:
        return self._hooks.get(phase, [])

    def _resource_cms(self) -> list[tuple[str, Callable[..., Any]]]:
        return self._cms


def test_lifecycle_config_error_is_an_exception() -> None:
    err = LifecycleConfigError("boom")
    assert isinstance(err, Exception)
    assert str(err) == "boom"


def test_collision_msg_names_prior_and_now_owner() -> None:
    from calfkit.worker.lifecycle import _collision_msg

    msg = _collision_msg("db", "@resource('db')", "an on_startup/after_shutdown callback")
    assert msg == (
        "resource 'db' is owned by @resource('db'); "
        "an on_startup/after_shutdown callback may not also write it. "
        "Use one owner per key."
    )


# ---------------------------------------------------------------------------
# _ResourceBag
# ---------------------------------------------------------------------------


def test_resource_bag_claim_stores_value_and_provenance() -> None:
    from calfkit.worker.lifecycle import _ResourceBag

    bag = _ResourceBag()
    bag.claim("db", "pool", "@resource('db')")

    assert bag["db"] == "pool"
    assert bag._claims["db"] == "@resource('db')"


def test_resource_bag_same_claimant_may_reset_value() -> None:
    from calfkit.worker.lifecycle import _ResourceBag

    bag = _ResourceBag()
    bag.claim("db", "pool-a", "@resource('db')")
    bag.claim("db", "pool-b", "@resource('db')")

    assert bag["db"] == "pool-b"


def test_resource_bag_different_claimant_raises_with_collision_message() -> None:
    from calfkit.worker.lifecycle import _ResourceBag

    bag = _ResourceBag()
    bag.claim("db", "pool", "@resource('db')")

    with pytest.raises(LifecycleConfigError) as excinfo:
        bag.claim("db", "other", "an on_startup/after_shutdown callback")

    assert str(excinfo.value) == (
        "resource 'db' is owned by @resource('db'); "
        "an on_startup/after_shutdown callback may not also write it. "
        "Use one owner per key."
    )


def test_resource_bag_collision_is_order_independent() -> None:
    from calfkit.worker.lifecycle import _ResourceBag

    # Callback first, then @resource — collision must still raise (both directions).
    bag = _ResourceBag()
    bag.claim("db", "pool", "an on_startup/after_shutdown callback")

    with pytest.raises(LifecycleConfigError):
        bag.claim("db", "other", "@resource('db')")


# ---------------------------------------------------------------------------
# _ClaimingView
# ---------------------------------------------------------------------------


def test_claiming_view_setitem_claims_through_bag() -> None:
    from calfkit.worker.lifecycle import _ClaimingView, _ResourceBag

    bag = _ResourceBag()
    view = _ClaimingView(bag, "an on_startup/after_shutdown callback")

    view["db"] = "pool"

    assert bag["db"] == "pool"
    assert bag._claims["db"] == "an on_startup/after_shutdown callback"


def test_claiming_view_reads_delegate_to_bag() -> None:
    from calfkit.worker.lifecycle import _ClaimingView, _ResourceBag

    bag = _ResourceBag()
    bag.claim("db", "pool", "@resource('db')")
    view = _ClaimingView(bag, "an on_startup/after_shutdown callback")

    assert view["db"] == "pool"
    assert "db" in view
    assert len(view) == 1
    assert list(view) == ["db"]


def test_claiming_view_setitem_collides_with_other_claimant() -> None:
    from calfkit.worker.lifecycle import _ClaimingView, _ResourceBag

    bag = _ResourceBag()
    bag.claim("db", "pool", "@resource('db')")
    view = _ClaimingView(bag, "an on_startup/after_shutdown callback")

    with pytest.raises(LifecycleConfigError):
        view["db"] = "other"


# ---------------------------------------------------------------------------
# Read-only resource view
# ---------------------------------------------------------------------------


def test_read_only_resources_reads_through_to_bag() -> None:
    from calfkit.worker.lifecycle import _read_only_resources, _ResourceBag

    bag = _ResourceBag()
    bag.claim("db", "pool", "@resource('db')")
    ro = _read_only_resources(bag)

    assert ro["db"] == "pool"
    assert dict(ro) == {"db": "pool"}


def test_read_only_resources_write_raises_type_error() -> None:
    from calfkit.worker.lifecycle import _read_only_resources, _ResourceBag

    ro = _read_only_resources(_ResourceBag())

    with pytest.raises(TypeError):
        ro["db"] = "pool"  # type: ignore[index]


# ---------------------------------------------------------------------------
# Context dataclasses + owner aliases
# ---------------------------------------------------------------------------


def test_lifecycle_context_aliases_owner_as_node_and_worker() -> None:
    from calfkit.worker.lifecycle import LifecycleContext, _ClaimingView, _ResourceBag

    owner = object()
    bag = _ResourceBag()
    ctx = LifecycleContext(owner, _ClaimingView(bag, "an on_startup/after_shutdown callback"))

    assert ctx.owner is owner
    assert ctx.node is owner
    assert ctx.worker is owner


def test_lifecycle_context_resources_are_writable() -> None:
    from calfkit.worker.lifecycle import LifecycleContext, _ClaimingView, _ResourceBag

    bag = _ResourceBag()
    ctx = LifecycleContext(object(), _ClaimingView(bag, "an on_startup/after_shutdown callback"))

    ctx.resources["db"] = "pool"

    assert bag["db"] == "pool"


def test_resource_setup_context_resources_are_read_only() -> None:
    from calfkit.worker.lifecycle import ResourceSetupContext, _read_only_resources, _ResourceBag

    ctx = ResourceSetupContext(object(), _read_only_resources(_ResourceBag()))

    with pytest.raises(TypeError):
        ctx.resources["db"] = "pool"  # type: ignore[index]


def test_serving_context_carries_broker_and_read_only_resources() -> None:
    from faststream.kafka import KafkaBroker

    from calfkit.worker.lifecycle import ServingContext, _read_only_resources, _ResourceBag

    owner = object()
    # Constructing a KafkaBroker opens no connection; we never start it.
    broker = KafkaBroker()
    ctx = ServingContext(owner, _read_only_resources(_ResourceBag()), broker)

    assert ctx.owner is owner
    assert ctx.node is owner
    assert ctx.worker is owner
    assert ctx.broker is broker
    with pytest.raises(TypeError):
        ctx.resources["db"] = "pool"  # type: ignore[index]


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


async def test_resource_cm_claims_value_on_enter_and_pops_on_exit() -> None:
    from calfkit.worker.lifecycle import ResourceSetupContext, _read_only_resources, _resource_cm

    closed: list[bool] = []

    async def genfn(ctx: Any) -> Any:
        try:
            yield "pool"
        finally:
            closed.append(True)

    owner = _FakeOwner()
    setup_ctx = ResourceSetupContext(owner, _read_only_resources(owner.resources))

    async with _resource_cm(owner, "db", genfn, setup_ctx):
        # Inside the bracket the value is claimed under @resource provenance.
        assert owner.resources["db"] == "pool"
        assert owner.resources._claims["db"] == "@resource('db')"

    # After exit the key and its provenance entry are gone, and the user
    # generator's finally ran.
    assert "db" not in owner.resources
    assert "db" not in owner.resources._claims
    assert closed == [True]


async def test_resource_cm_setup_error_propagates() -> None:
    from calfkit.worker.lifecycle import ResourceSetupContext, _read_only_resources, _resource_cm

    async def genfn(ctx: Any) -> Any:
        raise RuntimeError("setup boom")
        yield  # pragma: no cover

    owner = _FakeOwner()
    setup_ctx = ResourceSetupContext(owner, _read_only_resources(owner.resources))

    with pytest.raises(RuntimeError, match="setup boom"):
        async with _resource_cm(owner, "db", genfn, setup_ctx):
            pass


async def test_resource_cm_teardown_error_does_not_raise() -> None:
    from calfkit.worker.lifecycle import ResourceSetupContext, _read_only_resources, _resource_cm

    async def genfn(ctx: Any) -> Any:
        try:
            yield "pool"
        finally:
            raise RuntimeError("teardown boom")

    owner = _FakeOwner()
    setup_ctx = ResourceSetupContext(owner, _read_only_resources(owner.resources))

    # Teardown is guarded log-not-raise, so the bracket exits cleanly and the
    # key is still popped.
    async with _resource_cm(owner, "db", genfn, setup_ctx):
        pass

    assert "db" not in owner.resources
    assert "db" not in owner.resources._claims


async def test_resource_cm_collision_with_callback_claim_raises() -> None:
    from calfkit.worker.lifecycle import ResourceSetupContext, _read_only_resources, _resource_cm

    async def genfn(ctx: Any) -> Any:
        yield "pool"

    owner = _FakeOwner()
    # A callback already owns "db"; the @resource claim must collide.
    owner.resources.claim("db", "other", "an on_startup/after_shutdown callback")
    setup_ctx = ResourceSetupContext(owner, _read_only_resources(owner.resources))

    with pytest.raises(LifecycleConfigError):
        async with _resource_cm(owner, "db", genfn, setup_ctx):
            pass


# ---------------------------------------------------------------------------
# LifecycleHookMixin
# ---------------------------------------------------------------------------


def _mixin_owner() -> LifecycleHookMixin:
    from calfkit.worker.lifecycle import LifecycleHookMixin

    class _Owner(LifecycleHookMixin):
        pass

    return _Owner()


def test_mixin_resources_is_a_lazy_resource_bag() -> None:
    from calfkit.worker.lifecycle import _ResourceBag

    owner = _mixin_owner()

    assert isinstance(owner.resources, _ResourceBag)
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
