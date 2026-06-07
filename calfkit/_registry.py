"""Per-subclass collection of methods marked with a decorator.

A small, dependency-light foundation: subclasses of :class:`RegistryMixin` mark
methods with :func:`handler`, and each subclass exposes its **own** registry of
those methods. The registry is built once — when the subclass is created — and
is unique per subclass (never shared with sibling subclasses).

Why a marker + collector (rather than the decorator registering directly)
-------------------------------------------------------------------------
A decorator in a class body runs *before* the class object exists, so it cannot
append to a per-class list itself. :func:`handler` therefore only stamps
metadata onto the function; :meth:`RegistryMixin.__init_subclass__` does the
collection after the class is built, assigning a fresh registry onto that
specific subclass. That per-subclass assignment is what avoids a single list
being shared across siblings via inheritance.

Methods are stored by attribute name and resolved to bound methods on the
instance via ``getattr`` at access time (see :meth:`RegistryMixin.handlers`), so
an override — even one that does not re-apply :func:`handler` — is reflected.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, ClassVar, TypeVar

from calfkit.exceptions import RegistryConfigError

__all__ = ["HandlerInfo", "RegistryMixin", "handler", "handler_info"]

_HANDLER_ATTR = "__calf_handler__"

_MethodT = TypeVar("_MethodT", bound=Callable[..., Any])


@dataclass(frozen=True, slots=True)
class HandlerInfo:
    """Metadata stamped on a method by :func:`handler`.

    ``name`` is the stable, unique-per-class identifier the method is registered
    under; ``schema`` is an optional, registry-opaque payload (e.g. an input
    model) carried alongside it.
    """

    name: str
    schema: Any | None = None

    def __post_init__(self) -> None:
        # Keep the type self-validating: the registry's whole contract rests on
        # a usable name. ``handler()`` also rejects an empty name early (at the
        # decorator call), but this guards direct construction of the public type.
        if not self.name:
            raise ValueError("HandlerInfo.name must be non-empty")


def handler(name: str, *, schema: Any | None = None) -> Callable[[_MethodT], _MethodT]:
    """Mark a method for collection by :class:`RegistryMixin`.

    Returns the method **unchanged** — it stays an ordinary, callable method;
    the only effect is a :class:`HandlerInfo` marker that
    :meth:`RegistryMixin.__init_subclass__` reads when the owning class is
    created.

    The target must be a method — sync, async, ``staticmethod``, or
    ``classmethod`` (the underlying function is unwrapped via ``__func__``). A
    ``property`` is not a valid target.

    Args:
        name: Stable identifier the method is registered under. Must be
            non-empty and unique within a class hierarchy — a duplicate raises
            :class:`~calfkit.exceptions.RegistryConfigError` at class-definition
            time.
        schema: Optional payload carried with the handler; opaque to the
            registry.

    Raises:
        ValueError: If ``name`` is empty.
    """
    if not name:
        raise ValueError("handler() requires a non-empty name")

    def _decorate(method: _MethodT) -> _MethodT:
        # Unwrap static/classmethod so the marker lands on the underlying
        # function, where handler_info can find it from a bound method.
        target = getattr(method, "__func__", method)
        setattr(target, _HANDLER_ATTR, HandlerInfo(name=name, schema=schema))
        return method

    return _decorate


def handler_info(method: Callable[..., Any]) -> HandlerInfo | None:
    """Return the :class:`HandlerInfo` for a method, or ``None`` if unmarked.

    Accepts a plain function or a bound/unbound method (it unwraps ``__func__``),
    so it works on the values returned by :meth:`RegistryMixin.handlers`.
    """
    func = getattr(method, "__func__", method)
    info: HandlerInfo | None = getattr(func, _HANDLER_ATTR, None)
    return info


class RegistryMixin:
    """Base that collects methods marked with :func:`handler`, per subclass.

    Each subclass gets its **own** registry, built in :meth:`__init_subclass__`
    when the subclass is created — never shared with sibling subclasses. The
    registry merges markers across the MRO, so a subclass inherits its bases'
    handlers; an override that re-applies :func:`handler` replaces the inherited
    entry (most-derived wins). Handler ``name``\\ s must be unique within a
    class hierarchy, else :class:`~calfkit.exceptions.RegistryConfigError` is
    raised at class-definition time.

    Methods are stored by attribute name and resolved to bound methods on the
    instance at access time (see :meth:`handlers`), so a subclass that overrides
    a handler — even without re-decorating — is reflected.
    """

    # Maps each handler ``name`` to the attribute name of the method that
    # implements it. Assigned a fresh dict per subclass in __init_subclass__;
    # the empty default on the base is a never-mutated sentinel.
    _handlers: ClassVar[dict[str, str]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        # Walk base -> derived so the most-derived definition of an attribute
        # wins on collision, while the dict preserves first-seen order.
        marked: dict[str, HandlerInfo] = {}
        for klass in reversed(cls.__mro__):
            for attr, member in vars(klass).items():
                info = handler_info(member)
                if info is not None:
                    marked[attr] = info

        registry: dict[str, str] = {}
        for attr, info in marked.items():
            owner = registry.get(info.name)
            if owner is not None and owner != attr:
                raise RegistryConfigError(
                    f"{cls.__qualname__}: handler name {info.name!r} is registered by both "
                    f"{owner!r} and {attr!r}; handler names must be unique per class"
                )
            registry[info.name] = attr
        cls._handlers = registry

    def handlers(self) -> dict[str, Callable[..., Any]]:
        """Return this instance's handlers as ``{name: bound method}``.

        Each value is a method already bound to ``self`` — call it directly. A
        fresh dict is returned each call, so mutating it cannot corrupt the
        per-class registry.
        """
        return {name: getattr(self, attr) for name, attr in type(self)._handlers.items()}

    def get_handler(self, name: str) -> Callable[..., Any]:
        """Return the bound method registered under ``name``.

        Raises:
            KeyError: If no handler is registered under ``name``.
        """
        method: Callable[..., Any] = getattr(self, type(self)._handlers[name])
        return method

    @classmethod
    def handler_names(cls) -> tuple[str, ...]:
        """The handler names registered on this class, in registration order.

        A read-only view for class-level introspection (no instance needed);
        :meth:`handlers` gives the bound callables for an instance.
        """
        return tuple(cls._handlers)
