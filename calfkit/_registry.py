"""Per-subclass collection of route handlers marked with :func:`handler`.

Subclasses of :class:`RegistryMixin` mark methods with :func:`handler`, giving
each a **route pattern** (§6). Each subclass exposes its own registry of those
handlers, built once when the subclass is created (never shared with siblings).

A decorator in a class body runs *before* the class object exists, so
:func:`handler` only stamps metadata onto the function;
:meth:`RegistryMixin.__init_subclass__` does the collection after the class is
built. Handlers are stored by route and resolved to bound methods at access
time, so an override — even one that does not re-apply :func:`handler` — is
reflected.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, ClassVar, TypeVar

from pydantic import BaseModel

from calfkit._routing import validate_route_pattern
from calfkit.exceptions import RegistryConfigError

__all__ = ["HandlerInfo", "RegistryMixin", "handler", "handler_info"]

_HANDLER_ATTR = "__calf_handler__"

_MethodT = TypeVar("_MethodT", bound=Callable[..., Any])


def _unwrap(method: Callable[..., Any]) -> Callable[..., Any]:
    """The underlying function of a static/class/bound method (else ``method`` itself).

    The marker lands on — and is read from — this underlying function, so it
    survives static/classmethod wrapping and bound-method access.
    """
    return getattr(method, "__func__", method)


@dataclass(frozen=True, slots=True)
class HandlerInfo:
    """Metadata stamped on a method by :func:`handler`.

    ``route`` is the route pattern the handler matches and its unique key in the
    registry; ``name`` is a human/debug identifier (defaults to the method's
    name); ``schema`` is an optional pydantic model the inbound body is validated
    against before the handler runs.
    """

    route: str
    name: str
    schema: type[BaseModel] | None = None

    def __post_init__(self) -> None:
        # Self-validating: the registry's contract rests on a usable route + name.
        if not self.route:
            raise ValueError("HandlerInfo.route must be non-empty")
        if not self.name:
            raise ValueError("HandlerInfo.name must be non-empty")


def handler(route: str, *, schema: type[BaseModel] | None = None, name: str | None = None) -> Callable[[_MethodT], _MethodT]:
    """Mark a method as a route handler for collection by :class:`RegistryMixin`.

    Returns the method **unchanged** — it stays an ordinary, callable method; the
    only effect is a :class:`HandlerInfo` marker read by
    :meth:`RegistryMixin.__init_subclass__` when the owning class is built. The
    target must be a method (sync/async/static/class — the underlying function is
    unwrapped via ``__func__``); a ``property`` is not a valid target.

    Args:
        route: The route pattern (§6 grammar), validated here. Unique per class
            hierarchy — a duplicate raises
            :class:`~calfkit.exceptions.RegistryConfigError` at class definition.
        schema: Optional pydantic ``BaseModel`` subclass the inbound body is
            validated against before the handler runs.
        name: Optional human/debug identifier; defaults to the method's name.

    Raises:
        ValueError: If ``route`` violates the grammar, or ``schema`` is not a
            ``BaseModel`` subclass.
    """
    validate_route_pattern(route)
    if schema is not None and not (isinstance(schema, type) and issubclass(schema, BaseModel)):
        raise ValueError(f"handler() schema must be a pydantic BaseModel subclass, got {schema!r}")

    def _decorate(method: _MethodT) -> _MethodT:
        target = _unwrap(method)
        resolved_name = name if name is not None else getattr(target, "__name__", route)
        setattr(target, _HANDLER_ATTR, HandlerInfo(route=route, name=resolved_name, schema=schema))
        return method

    return _decorate


def handler_info(method: Callable[..., Any]) -> HandlerInfo | None:
    """Return the :class:`HandlerInfo` for a method, or ``None`` if unmarked.

    Accepts a plain function or a bound/unbound method (it unwraps ``__func__``),
    so it works on the values returned by :meth:`RegistryMixin.handlers`.
    """
    info: HandlerInfo | None = getattr(_unwrap(method), _HANDLER_ATTR, None)
    return info


class RegistryMixin:
    """Base that collects :func:`handler`-marked methods, per subclass, keyed by route.

    Each subclass gets its **own** registry, merged across the MRO (an override
    that re-applies :func:`handler` replaces the inherited entry — most-derived
    wins) and built in :meth:`__init_subclass__`. Route patterns must be unique
    within a class hierarchy, else
    :class:`~calfkit.exceptions.RegistryConfigError` is raised at class definition.
    Handlers resolve to bound methods at access time, so a subclass that overrides
    a handler — even without re-decorating — is reflected.
    """

    # route -> attribute name; assigned fresh per subclass. The empty default on
    # the base is a never-mutated sentinel.
    _handlers: ClassVar[dict[str, str]] = {}
    # route -> HandlerInfo (name + schema metadata), parallel to _handlers.
    _handler_info: ClassVar[dict[str, HandlerInfo]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        # Walk base -> derived so the most-derived definition of an attribute wins,
        # while the dict preserves first-seen order.
        marked: dict[str, HandlerInfo] = {}  # attr -> info
        for klass in reversed(cls.__mro__):
            for attr, member in vars(klass).items():
                info = handler_info(member)
                if info is not None:
                    marked[attr] = info

        route_to_attr: dict[str, str] = {}
        route_to_info: dict[str, HandlerInfo] = {}
        for attr, info in marked.items():
            owner = route_to_attr.get(info.route)
            if owner is not None and owner != attr:
                raise RegistryConfigError(
                    f"{cls.__qualname__}: route {info.route!r} is registered by both {owner!r} and {attr!r}; routes must be unique per class"
                )
            route_to_attr[info.route] = attr
            route_to_info[info.route] = info
        cls._handlers = route_to_attr
        cls._handler_info = route_to_info

    def handlers(self) -> dict[str, Callable[..., Any]]:
        """Return this instance's handlers as ``{route: bound method}``.

        Each value is a method already bound to ``self``. A fresh dict is returned
        each call, so mutating it cannot corrupt the per-class registry.
        """
        return {route: getattr(self, attr) for route, attr in type(self)._handlers.items()}

    def get_handler(self, route: str) -> Callable[..., Any]:
        """Return the bound method registered under ``route``.

        Raises:
            KeyError: If no handler is registered under ``route``.
        """
        method: Callable[..., Any] = getattr(self, type(self)._handlers[route])
        return method

    @classmethod
    def routes(cls) -> tuple[str, ...]:
        """The route patterns registered on this class, in registration order.

        See :meth:`route_table` for the per-route metadata (human name + schema)
        and :meth:`handlers` for the bound methods on an instance.
        """
        return tuple(cls._handlers)

    @classmethod
    def route_table(cls) -> dict[str, HandlerInfo]:
        """Class-level introspection: ``{route: HandlerInfo}`` (human name + schema)."""
        return dict(cls._handler_info)
