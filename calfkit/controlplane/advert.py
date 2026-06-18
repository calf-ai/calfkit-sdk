"""Per-subclass collection of control-plane adverts marked with :func:`advertises`.

Mirrors the :mod:`calfkit._registry` handler pattern: a class-body decorator only
stamps metadata onto the method (the class object does not exist yet);
:meth:`AdvertRegistryMixin.__init_subclass__` does the collection once the class is
built. An advert binds a node *type* to one control-plane topic + record schema;
instances of the type all advertise to that topic, differing only in the content
their factory returns. Factories resolve to bound methods at access time, so an
override — even one that does not re-apply :func:`advertises` — is reflected.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, ClassVar, TypeVar

from calfkit._registry import _unwrap
from calfkit.controlplane.records import ControlPlaneRecord
from calfkit.exceptions import RegistryConfigError

__all__ = ["AdvertInfo", "AdvertRegistryMixin", "advert_info", "advertises"]

_ADVERT_ATTR = "__calf_advert__"

_MethodT = TypeVar("_MethodT", bound=Callable[..., Any])


@dataclass(frozen=True, slots=True)
class AdvertInfo:
    """Metadata stamped on a method by :func:`advertises`.

    ``topic`` is the control-plane topic the record is published to and its unique
    key in the registry; ``record`` is the :class:`ControlPlaneRecord` subclass the
    factory returns; ``name`` is the attribute name the factory is bound under
    (set authoritatively during collection, used to resolve the bound method).
    """

    topic: str
    record: type[ControlPlaneRecord]
    name: str

    def __post_init__(self) -> None:
        if not self.topic:
            raise ValueError("AdvertInfo.topic must be non-empty")
        if not self.name:
            raise ValueError("AdvertInfo.name must be non-empty")
        if not (isinstance(self.record, type) and issubclass(self.record, ControlPlaneRecord)):
            raise ValueError(f"AdvertInfo.record must be a ControlPlaneRecord subclass, got {self.record!r}")


def advertises(topic: str, *, record: type[ControlPlaneRecord]) -> Callable[[_MethodT], _MethodT]:
    """Mark a method as a control-plane advert factory for :class:`AdvertRegistryMixin`.

    Returns the method **unchanged** — it stays an ordinary, callable method; the
    only effect is an :class:`AdvertInfo` marker read by
    :meth:`AdvertRegistryMixin.__init_subclass__` when the owning class is built.

    The decorated method is a *content factory*: it takes a
    :class:`~calfkit.controlplane.records.ControlPlaneIdentity` (stamped by the
    worker-owned publisher) and returns a fully-formed ``record`` instance —
    typically ``record(**identity.model_dump(), <content>)``.

    The publisher *pulls* this factory once per heartbeat tick, so any "content
    currency" timestamp the record carries (e.g. a ``content_updated_at``) must come
    from a value the node tracks and advances only when its content actually changes
    — never ``now()`` computed inside the factory (that would move it every tick and
    mask content staleness).

    The ``(topic, record)`` binding is per node *type*: every instance advertises
    to the same topic with the same schema (a topic's view decodes exactly one
    record type). **One topic ⇒ one record schema** must also hold across *all*
    node types (a deployment-time contract — a foreign payload that fails to decode is
    dropped by ktables below the view, so that node silently vanishes); that cross-type
    case is not enforceable here.

    Args:
        topic: The control-plane topic this type advertises to. Unique per class
            hierarchy — a duplicate raises
            :class:`~calfkit.exceptions.RegistryConfigError` at class definition.
        record: The :class:`ControlPlaneRecord` subclass the factory returns.

    Raises:
        ValueError: If ``topic`` is empty, or ``record`` is not a
            ``ControlPlaneRecord`` subclass.
    """
    if not topic:
        raise ValueError("advertises() topic must be non-empty")
    if not (isinstance(record, type) and issubclass(record, ControlPlaneRecord)):
        raise ValueError(f"advertises() record must be a ControlPlaneRecord subclass, got {record!r}")

    def _decorate(method: _MethodT) -> _MethodT:
        target = _unwrap(method)
        # ``name`` is a placeholder here (the real attribute name is resolved in
        # __init_subclass__, which knows the class-body key); only validity matters.
        setattr(target, _ADVERT_ATTR, AdvertInfo(topic=topic, record=record, name=getattr(target, "__name__", topic)))
        return method

    return _decorate


def advert_info(method: Callable[..., Any]) -> AdvertInfo | None:
    """Return the :class:`AdvertInfo` for a method, or ``None`` if unmarked.

    Accepts a plain function or a bound/unbound method (it unwraps ``__func__``),
    so it works on the values returned by :meth:`AdvertRegistryMixin.control_plane_adverts`.
    """
    info: AdvertInfo | None = getattr(_unwrap(method), _ADVERT_ATTR, None)
    return info


class AdvertRegistryMixin:
    """Collects :func:`advertises`-marked methods, per subclass, keyed by topic.

    Each subclass gets its **own** registry, merged across the MRO (an override
    that re-applies :func:`advertises` replaces the inherited entry — most-derived
    wins) and built in :meth:`__init_subclass__`. Topics must be unique within a
    class hierarchy, else :class:`~calfkit.exceptions.RegistryConfigError` is
    raised at class definition.
    """

    # topic -> AdvertInfo; assigned fresh per subclass. The empty default on the
    # base is a never-mutated sentinel.
    _adverts: ClassVar[dict[str, AdvertInfo]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        # Walk base -> derived so the most-derived definition of an attribute wins.
        # Bind AdvertInfo.name to the real attribute key (authoritative for
        # resolution), independent of the function's __name__.
        marked: dict[str, AdvertInfo] = {}  # attr -> info
        for klass in reversed(cls.__mro__):
            for attr, member in vars(klass).items():
                info = advert_info(member)
                if info is not None:
                    marked[attr] = replace(info, name=attr)

        topic_to_attr: dict[str, str] = {}
        topic_to_info: dict[str, AdvertInfo] = {}
        for attr, info in marked.items():
            owner = topic_to_attr.get(info.topic)
            if owner is not None and owner != attr:
                raise RegistryConfigError(
                    f"{cls.__qualname__}: control-plane topic {info.topic!r} is registered by both "
                    f"{owner!r} and {attr!r}; topics must be unique per class"
                )
            topic_to_attr[info.topic] = attr
            topic_to_info[info.topic] = info
        cls._adverts = topic_to_info

    def control_plane_adverts(self) -> dict[str, Callable[..., ControlPlaneRecord]]:
        """Return this instance's advert factories as ``{topic: bound method}``.

        Each value is a method already bound to ``self``. A fresh dict is returned
        each call, so mutating it cannot corrupt the per-class registry.
        """
        return {topic: getattr(self, info.name) for topic, info in type(self)._adverts.items()}
