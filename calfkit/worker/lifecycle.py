"""Lifecycle-hook foundation for nodes and workers.

Pure, Kafka-free building blocks for the lifecycle-hooks feature:

- the per-phase context dataclasses (resources writable in the resource phase,
  read-only-typed elsewhere),
- the async context-manager builders that run hooks and ``@resource`` brackets,
- the :class:`LifecycleHookMixin` that gives owners (nodes/workers) their
  decorator surface and a plain-``dict`` resource bag.

Resources are stored in a plain ``dict`` per owner. A node's per-message handler
receives a *shallow copy* of that bag (see ``BaseNodeDef.prepare_context``), so a
handler can never corrupt the shared resources, and the read-side is typed
read-only (``Mapping``) so writes are a type error at dev time.

An owner uses **one** resource pattern: either ``@resource`` brackets *or*
``on_startup``/``after_shutdown`` callbacks. If both are present the worker logs
a warning and the ``@resource`` brackets win (mirrors FastAPI's lifespan-vs-
on_event rule); see :meth:`Worker._owner_cms`.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, MutableMapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any, Generic, Protocol, runtime_checkable

from faststream.kafka import KafkaBroker
from typing_extensions import Self, TypeVar

from calfkit.exceptions import LifecycleConfigError

logger = logging.getLogger(__name__)

OwnerT = TypeVar("OwnerT", default=Any)
ItemT = TypeVar("ItemT")


@dataclass
class LifecycleContext(Generic[OwnerT]):
    """Context for ``on_startup`` / ``after_shutdown`` — resources are WRITABLE.

    This is the callback shape of resource management: a hook writes
    ``ctx.resources["key"] = value`` into the owner's shared bag. ``ctx.owner``
    is the node or worker the hook was registered on; it is typed precisely
    (e.g. ``LifecycleContext[Worker]``) when the hook is registered via the
    owner's decorator, so ``ctx.owner.id`` autocompletes.
    """

    owner: OwnerT
    resources: MutableMapping[str, Any]


@dataclass
class ResourceSetupContext(Generic[OwnerT]):
    """Context for a ``@resource`` body — resources are read-only-typed.

    ``ctx.owner`` is the owning node or worker (see :class:`LifecycleContext`).
    """

    owner: OwnerT
    resources: Mapping[str, Any]


@dataclass
class ServingContext(Generic[OwnerT]):
    """Context for ``after_startup`` / ``on_shutdown`` — read-only resources + broker.

    ``ctx.broker`` is the live :class:`~faststream.kafka.KafkaBroker` (the
    producer is up during these phases), so a presence/announcement hook can
    ``await ctx.broker.publish(...)``. ``ctx.owner`` is the owning node or
    worker (see :class:`LifecycleContext`).
    """

    owner: OwnerT
    resources: Mapping[str, Any]
    broker: KafkaBroker


async def _maybe_await(value: Any) -> Any:
    """Await ``value`` if it is awaitable; pass ``None`` through.

    A hook may be sync (returns ``None``) or async (returns a coroutine). A
    hook mistakenly written as a *generator* (e.g. it ``yield``s) returns a
    non-``None`` non-awaitable here — that is a programming error, so we raise
    ``TypeError`` rather than silently dropping it.
    """
    if value is None:
        return None
    if inspect.isawaitable(value):
        return await value
    raise TypeError(f"lifecycle hook returned a non-awaitable {type(value).__name__!r}; hooks must be sync (return None) or async")


# Maps a logical phase-pair name to ((enter_phase, exit_phase), has_resources).
# ``resource`` brackets own the bag (writable callbacks + ``@resource``);
# ``serving`` brackets run while the broker consumes (read-only + broker).
PHASE_PAIRS: dict[str, tuple[tuple[str, str], bool]] = {
    "resource": (("on_startup", "after_shutdown"), True),
    "serving": (("after_startup", "on_shutdown"), False),
}


async def _safe_teardown(
    items: Sequence[ItemT],
    action: Callable[[ItemT], Awaitable[Any]],
    label: str,
) -> None:
    """Run ``action`` over ``items`` in reverse (LIFO) order, logs-never-raises.

    A failing item is logged and skipped so siblings still tear down. A
    ``CancelledError`` is *not* caught — cancellation must propagate (it derives
    from ``BaseException``, not ``Exception``) and aborts the remaining items.
    """
    for it in reversed(items):
        try:
            await action(it)
        except Exception:
            logger.exception("%s teardown failed: %r", label, it)


# Internal, loose storage type: a lifecycle hook receives a context and returns
# ``None`` (sync) or an awaitable (async). ``Any`` for the context keeps the
# heterogeneous per-phase hooks in one registry list.
LifecycleHook = Callable[[Any], Awaitable[Any] | None]

# Public, phase-specific hook types. The decorators are typed with these (over
# ``Self``) so a hook annotated ``ctx: ServingContext`` type-checks and
# ``ctx.owner`` resolves to the owning node/worker. ``resource`` phases
# (``on_startup``/``after_shutdown``) get a writable :class:`LifecycleContext`;
# ``serving`` phases (``after_startup``/``on_shutdown``) get a
# :class:`ServingContext` with the live broker.
_ResourceHook = Callable[[LifecycleContext[OwnerT]], Awaitable[Any] | None]
_ServingHook = Callable[[ServingContext[OwnerT]], Awaitable[Any] | None]


@runtime_checkable
class SupportsLifecycleHooks(Protocol):
    """Structural type for an owner (node/worker) the CM builders operate on.

    The builders read only this surface; concrete owners gain it via
    :class:`LifecycleHookMixin`. ``resources`` is a read-only property returning
    the owner's mutable bag — the engine writes into it via ``@resource``
    (``_resource_cm``) but never rebinds the attribute.
    """

    @property
    def resources(self) -> MutableMapping[str, Any]: ...

    def _hooks_for(self, phase: str) -> list[LifecycleHook]: ...

    def _resource_cms(self) -> list[tuple[str, ResourceGenFn]]: ...


def _span_cm(
    owner: SupportsLifecycleHooks,
    enter_phase: str,
    exit_phase: str,
    ctx: Any,
) -> AbstractAsyncContextManager[None]:
    """Async CM that runs ``enter_phase`` hooks on entry and ``exit_phase`` on exit.

    Setup (enter) errors propagate so boot fails. Teardown (exit) hooks run via
    :func:`_safe_teardown`, so they log-never-raise and drain the rest.
    """

    @asynccontextmanager
    async def _cm() -> AsyncIterator[None]:
        for cb in owner._hooks_for(enter_phase):
            await _maybe_await(cb(ctx))
        try:
            yield
        finally:
            await _safe_teardown(owner._hooks_for(exit_phase), lambda cb: _maybe_await(cb(ctx)), f"{owner!r} {exit_phase}")

    return _cm()


def _resource_cm(
    owner: SupportsLifecycleHooks,
    name: str,
    genfn: Callable[[Any], AsyncIterator[Any]],
    setup_ctx: Any,
) -> AbstractAsyncContextManager[None]:
    """Async CM that brackets a single ``@resource`` for the owner's lifetime.

    Enters the user generator-CM (setup errors propagate so boot fails), stores
    the yielded value in the owner's bag under ``name``, then on exit removes the
    key and closes the user CM with ``(None, None, None)`` (guarded log-not-raise).

    The user CM is always torn down via ``(None, None, None)`` because
    ``@resource`` is a worker-lifetime bracket, not a per-request transaction —
    don't rely on exception-into-generator forwarding.
    """

    @asynccontextmanager
    async def _cm() -> AsyncIterator[None]:
        cm = asynccontextmanager(genfn)(setup_ctx)
        value = await cm.__aenter__()
        owner.resources[name] = value
        try:
            yield
        finally:
            owner.resources.pop(name, None)
            try:
                await cm.__aexit__(None, None, None)
            except Exception:
                logger.exception("@resource %r on %r teardown failed", name, owner)

    return _cm()


# A ``@resource`` body: a single-yield async generator taking the setup context.
# Generic over the owner type so a decorated body can annotate
# ``ctx: ResourceSetupContext[MyNode]``; bare ``ResourceGenFn`` is ``[Any]``.
ResourceGenFn = Callable[[ResourceSetupContext[OwnerT]], AsyncIterator[Any]]


class LifecycleHookMixin:
    """Mixin giving an owner (node/worker) its lifecycle-hook decorator surface.

    Storage is created lazily on first access so the mixin is ``__init__``-free:
    dataclass node subclasses bypass cooperative ``__init__`` chains, so we must
    not rely on an initializer to set up the registries.

    Resources are a plain ``dict`` (the shared bag); hooks are stored per phase;
    ``@resource`` brackets are stored as ``(name, genfn)`` pairs with duplicate
    names rejected at registration.
    """

    @property
    def resources(self) -> dict[str, Any]:
        """The owner's shared resource bag (a plain ``dict``).

        Written by the engine (``@resource``) or by ``on_startup``/
        ``after_shutdown`` callbacks. Per-message handlers receive a shallow copy
        of this dict (typed read-only), so handler code cannot corrupt it.
        """
        bag = self.__dict__.get("_lifecycle_resources")
        if bag is None:
            bag = {}
            self.__dict__["_lifecycle_resources"] = bag
        return bag

    def _hook_registry(self) -> dict[str, list[LifecycleHook]]:
        reg = self.__dict__.get("_lifecycle_hooks")
        if reg is None:
            reg = {}
            self.__dict__["_lifecycle_hooks"] = reg
        return reg

    def _resource_registry(self) -> list[tuple[str, ResourceGenFn]]:
        cms = self.__dict__.get("_lifecycle_resource_cms")
        if cms is None:
            cms = []
            self.__dict__["_lifecycle_resource_cms"] = cms
        return cms

    def _hooks_for(self, phase: str) -> list[LifecycleHook]:
        return self._hook_registry().get(phase, [])

    def _resource_cms(self) -> list[tuple[str, ResourceGenFn]]:
        return self._resource_registry()

    def _register_hook(self, phase: str, fn: LifecycleHook) -> LifecycleHook:
        reg = self._hook_registry()
        # Assignment (not in-place append) keeps the stored list a fresh object
        # per phase and mirrors the repo's "mark-as-set via assignment" idiom.
        reg[phase] = [*reg.get(phase, []), fn]
        return fn

    def on_startup(self, fn: _ResourceHook[Self]) -> _ResourceHook[Self]:
        """Register an ``on_startup`` hook (pre-broker; writable resources).

        The hook receives a :class:`LifecycleContext` (``ctx.resources`` is
        writable). ``ctx.owner`` is typed as the owning node/worker.
        """
        self._register_hook("on_startup", fn)
        return fn

    def after_startup(self, fn: _ServingHook[Self]) -> _ServingHook[Self]:
        """Register an ``after_startup`` hook (broker up; read-only + broker).

        The hook receives a :class:`ServingContext` (``ctx.broker`` is live).

        Caveat: under at-least-once delivery + rebalances a presence-style
        publish here may fire more than once, and a hard crash publishes
        nothing — pair with a consumer-side TTL/liveness check.
        """
        self._register_hook("after_startup", fn)
        return fn

    def on_shutdown(self, fn: _ServingHook[Self]) -> _ServingHook[Self]:
        """Register an ``on_shutdown`` hook (broker up; read-only + broker).

        The hook receives a :class:`ServingContext` (``ctx.broker`` is live).

        Caveat: see :meth:`after_startup` — departure publishes are not
        guaranteed exactly once and may be skipped on a hard crash.
        """
        self._register_hook("on_shutdown", fn)
        return fn

    def after_shutdown(self, fn: _ResourceHook[Self]) -> _ResourceHook[Self]:
        """Register an ``after_shutdown`` hook (post-drain; writable resources).

        The hook receives a :class:`LifecycleContext` (``ctx.resources`` is
        writable).
        """
        self._register_hook("after_shutdown", fn)
        return fn

    def resource(self, name: str) -> Callable[[ResourceGenFn[Self]], ResourceGenFn[Self]]:
        """Register a ``@resource`` bracket under ``name``.

        The decorated function is a single-yield async generator that receives
        a :class:`ResourceSetupContext`. Its teardown always runs with
        ``(None, None, None)`` — ``@resource`` is a worker-lifetime bracket, not
        a per-request transaction, so don't rely on exception-into-generator
        forwarding.

        Use one resource pattern per owner: combining ``@resource`` with
        ``on_startup``/``after_shutdown`` callbacks makes the callbacks ignored
        (with a warning), mirroring FastAPI's lifespan-vs-on_event rule.

        Raises:
            LifecycleConfigError: if ``name`` is already registered on this owner.
        """

        def _decorator(fn: ResourceGenFn[Self]) -> ResourceGenFn[Self]:
            cms = self._resource_registry()
            if any(existing == name for existing, _ in cms):
                raise LifecycleConfigError(f"@resource({name!r}) is already registered on this owner; resource names must be unique per owner")
            self.__dict__["_lifecycle_resource_cms"] = [*cms, (name, fn)]
            return fn

        return _decorator
