"""Lifecycle-hook foundation for nodes and workers.

Pure, Kafka-free building blocks for the lifecycle-hooks feature:

- the resource bag with provenance (:class:`_ResourceBag`) and the views
  that funnel writes through it,
- the per-phase context dataclasses,
- the async context-manager builders that run hooks and resource brackets,
- the :class:`LifecycleHookMixin` that gives owners (nodes/workers) their
  decorator surface and hook storage.

See ``docs/research/node-worker-lifecycle-hooks-plan-v7.md`` for the design.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping, MutableMapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Generic, Protocol, runtime_checkable

from faststream.kafka import KafkaBroker
from typing_extensions import TypeVar

from calfkit.exceptions import LifecycleConfigError

logger = logging.getLogger(__name__)

OwnerT = TypeVar("OwnerT", default=Any)
ItemT = TypeVar("ItemT")


def _collision_msg(key: str, prior: str, now: str) -> str:
    """Build the message raised when two writers claim the same resource key."""
    return f"resource {key!r} is owned by {prior}; {now} may not also write it. Use one owner per key."


class _ResourceBag(dict):  # type: ignore[type-arg]
    """A ``dict`` of resources plus a provenance map.

    Every writer claims a key: callbacks via :class:`_ClaimingView`, the
    ``@resource`` engine directly, and even a *direct* ``bag[key] = value``
    (which funnels through :meth:`claim` so the invariant cannot be bypassed).
    A *different* claimant writing an already-claimed key raises
    :class:`LifecycleConfigError`; the *same* claimant may re-set the value.
    The rule is order-independent. Provenance is per bag, so the same key on
    two different owners' bags never collides.
    """

    def __init__(self) -> None:
        super().__init__()
        self._claims: dict[str, str] = {}

    def claim(self, key: str, value: Any, claimant: str) -> None:
        prior = self._claims.get(key)
        if prior is not None and prior != claimant:
            raise LifecycleConfigError(_collision_msg(key, prior, claimant))
        self._claims[key] = claimant
        dict.__setitem__(self, key, value)  # bypass our __setitem__ to avoid recursion

    def __setitem__(self, key: str, value: Any) -> None:
        # Direct ``bag[key] = value`` writes funnel through claim() so the
        # "one owner per key" invariant covers them too, not just the views.
        self.claim(key, value, "a direct resources[...] = ... write")

    def release(self, key: str) -> None:
        """Drop a key's value and its provenance together (used on teardown)."""
        dict.pop(self, key, None)
        self._claims.pop(key, None)


class _ClaimingView(MutableMapping[str, Any]):
    """A writable ``Mapping`` view over a :class:`_ResourceBag` for one claimant.

    Writes funnel through ``bag.claim`` so collisions are detected regardless
    of which surface wrote first. Reads and deletes delegate to the bag.
    """

    def __init__(self, bag: _ResourceBag, claimant: str) -> None:
        self._bag = bag
        self._claimant = claimant

    def __getitem__(self, key: str) -> Any:
        return self._bag[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._bag.claim(key, value, self._claimant)

    def __delitem__(self, key: str) -> None:
        self._bag.release(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self._bag)

    def __len__(self) -> int:
        return len(self._bag)


def _read_only_resources(bag: _ResourceBag) -> Mapping[str, Any]:
    """Build the read-only resources view for readers (serving hooks, ``@resource``
    bodies, handlers).

    Uses :class:`types.MappingProxyType` — the repo idiom — so a write raises
    ``TypeError`` at runtime while the static type is a plain ``Mapping``. The
    proxy is a live view: later ``claim``s on the bag are visible through it.
    """
    return MappingProxyType(bag)


class _OwnerAliases(Generic[OwnerT]):
    """Shared ``owner``/``node``/``worker`` accessors for the context types.

    A single owner is exposed under three names so a hook body can read
    whichever reads most naturally (``ctx.node`` on a node, ``ctx.worker`` on
    a worker) without LSP-violating subclassing.
    """

    owner: OwnerT

    @property
    def node(self) -> OwnerT:
        return self.owner

    @property
    def worker(self) -> OwnerT:
        return self.owner


@dataclass
class LifecycleContext(_OwnerAliases[OwnerT]):
    """Context for ``on_startup`` / ``after_shutdown`` — resources are WRITABLE."""

    owner: OwnerT
    resources: MutableMapping[str, Any]


@dataclass
class ResourceSetupContext(_OwnerAliases[OwnerT]):
    """Context for a ``@resource`` body — resources are READ-ONLY."""

    owner: OwnerT
    resources: Mapping[str, Any]


@dataclass
class ServingContext(_OwnerAliases[OwnerT]):
    """Context for ``after_startup`` / ``on_shutdown`` — READ-ONLY resources + broker."""

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
    ``CancelledError`` is *not* caught — cancellation must propagate and aborts
    the remaining items (see plan §4).
    """
    for it in reversed(items):
        try:
            await action(it)
        except Exception:
            logger.exception("%s teardown failed: %r", label, it)


# A lifecycle hook receives a context and returns ``None`` (sync) or an
# awaitable (async). ``Any`` for the context keeps the four phase-specific
# context types assignable without per-phase overloads.
LifecycleHook = Callable[[Any], Awaitable[Any] | None]


@runtime_checkable
class SupportsLifecycleHooks(Protocol):
    """Structural type for an owner (node/worker) the CM builders operate on.

    The builders read only this surface; concrete owners gain it via
    :class:`LifecycleHookMixin`.
    """

    resources: Any

    def _hooks_for(self, phase: str) -> list[LifecycleHook]: ...

    def _resource_cms(self) -> list[tuple[str, Callable[..., Any]]]: ...


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
            await _safe_teardown(owner._hooks_for(exit_phase), lambda cb: _maybe_await(cb(ctx)), exit_phase)

    return _cm()


def _resource_cm(
    owner: SupportsLifecycleHooks,
    name: str,
    genfn: Callable[[Any], AsyncIterator[Any]],
    setup_ctx: Any,
) -> AbstractAsyncContextManager[None]:
    """Async CM that brackets a single ``@resource`` for the owner's lifetime.

    Enters the user generator-CM (setup errors propagate so boot fails), claims
    the yielded value into the bag under ``@resource`` provenance, then on exit
    pops the key + its provenance entry and closes the user CM with
    ``(None, None, None)``. Teardown is guarded log-not-raise.

    The user CM is always torn down via ``aclose``-style ``__aexit__(None, ...)``
    because ``@resource`` is a worker-lifetime bracket, not a per-request
    transaction — don't rely on exception-into-generator forwarding.
    """

    @asynccontextmanager
    async def _cm() -> AsyncIterator[None]:
        cm = asynccontextmanager(genfn)(setup_ctx)
        value = await cm.__aenter__()
        try:
            owner.resources.claim(name, value, f"@resource({name!r})")
        except BaseException:
            # Claim collided (or was cancelled) AFTER the resource opened — close
            # it so a misconfiguration doesn't leak the just-opened resource.
            try:
                await cm.__aexit__(None, None, None)
            except Exception:
                logger.exception("@resource %r close after failed claim", name)
            raise
        try:
            yield
        finally:
            owner.resources.release(name)
            try:
                await cm.__aexit__(None, None, None)
            except Exception:
                logger.exception("@resource %r teardown failed", name)

    return _cm()


# A ``@resource`` body: a single-yield async generator taking the setup context.
ResourceGenFn = Callable[[Any], AsyncIterator[Any]]


class LifecycleHookMixin:
    """Mixin giving an owner (node/worker) its lifecycle-hook decorator surface.

    Storage is created lazily on first access so the mixin is ``__init__``-free:
    dataclass node subclasses bypass cooperative ``__init__`` chains, so we must
    not rely on an initializer to set up the hook registries.

    Hooks are stored per phase; ``@resource`` brackets are stored as
    ``(name, genfn)`` pairs with duplicate names rejected at registration.
    """

    @property
    def resources(self) -> _ResourceBag:
        bag = self.__dict__.get("_lifecycle_resources")
        if bag is None:
            bag = _ResourceBag()
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

    def on_startup(self, fn: LifecycleHook) -> LifecycleHook:
        """Register an ``on_startup`` hook (pre-broker; writable resources)."""
        return self._register_hook("on_startup", fn)

    def after_startup(self, fn: LifecycleHook) -> LifecycleHook:
        """Register an ``after_startup`` hook (broker up; read-only + broker).

        Caveat: under at-least-once delivery + rebalances a presence-style
        publish here may fire more than once, and a hard crash publishes
        nothing — pair with a consumer-side TTL/liveness check.
        """
        return self._register_hook("after_startup", fn)

    def on_shutdown(self, fn: LifecycleHook) -> LifecycleHook:
        """Register an ``on_shutdown`` hook (broker up; read-only + broker).

        Caveat: see :meth:`after_startup` — departure publishes are not
        guaranteed exactly once and may be skipped on a hard crash.
        """
        return self._register_hook("on_shutdown", fn)

    def after_shutdown(self, fn: LifecycleHook) -> LifecycleHook:
        """Register an ``after_shutdown`` hook (post-drain; writable resources)."""
        return self._register_hook("after_shutdown", fn)

    def resource(self, name: str) -> Callable[[ResourceGenFn], ResourceGenFn]:
        """Register a ``@resource`` bracket under ``name``.

        The decorated function is a single-yield async generator that receives
        a read-only :class:`ResourceSetupContext`. Its teardown always runs with
        ``(None, None, None)`` — ``@resource`` is a worker-lifetime bracket, not
        a per-request transaction, so don't rely on exception-into-generator
        forwarding.

        Raises:
            LifecycleConfigError: if ``name`` is already registered on this owner.
        """

        def _decorator(fn: ResourceGenFn) -> ResourceGenFn:
            cms = self._resource_registry()
            if any(existing == name for existing, _ in cms):
                raise LifecycleConfigError(f"@resource({name!r}) is already registered on this owner; resource names must be unique per owner")
            self.__dict__["_lifecycle_resource_cms"] = [*cms, (name, fn)]
            return fn

        return _decorator
