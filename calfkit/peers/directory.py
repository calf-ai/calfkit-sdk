"""The live, scoped, self-excluded peer directory render (§5.2).

``resolve_live_peers`` reads a ``ControlPlaneView[AgentCard]`` (via ``view.snapshot()``) and a list of
capability handles (``Messaging`` in PR-B; ``Handoff`` reuses this in PR-C) and returns the in-scope,
live, self-excluded ``(name, description)`` peers, sorted by name. A curated name absent from the live
view is omitted with a per-turn WARNING (mirroring the unresolved-selector warning in selector
resolution); a degraded or missing view warns-and-degrades to an empty directory, never raising on
reader health (§9). ``render_peer_directory`` formats the entries into a directory body (or a "none
reachable" sentinel) — shared by the ``message_agent`` tool description (§5.2) and the ``HandoffRequest``
option doc (§5.3).
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from calfkit.controlplane.view import ControlPlaneView
    from calfkit.models.agents import AgentCard

logger = logging.getLogger(__name__)

_NONE_REACHABLE = "(no peer agents are currently reachable)"


class _PeerScope(Protocol):
    """The structural shape ``resolve_live_peers`` needs from a capability handle: a curated name set XOR
    open ``discover``. Both :class:`~calfkit.peers.messaging.Messaging` and
    :class:`~calfkit.peers.handoff.Handoff` satisfy it structurally, so the render stays capability-agnostic
    and is shared by messaging and handoff. ``@property`` members (not bare annotations) because the handles
    expose read-only frozen-dataclass attributes — strict mypy rejects a *settable* Protocol member against a
    read-only attribute. (Static typing only; the ctor guard matches the concrete ``(Messaging, Handoff)``,
    so ``@runtime_checkable`` is not needed.)"""

    @property
    def names(self) -> tuple[str, ...]: ...

    @property
    def discover(self) -> bool: ...


def _safe_snapshot(view: ControlPlaneView[AgentCard] | None) -> dict[str, Any]:
    """The view's live ``name -> AgentCard`` snapshot, warn-and-degrading to ``{}`` on a missing or
    degraded view — peer discovery never raises on reader health (§9)."""
    if view is None:
        return {}
    try:
        return dict(view.snapshot())
    except Exception:  # noqa: BLE001 — reader health must never raise into the agent's run loop (§9)
        logger.warning("agents view degraded; rendering an empty peer directory this turn", exc_info=True)
        return {}


def resolve_live_peers(view: ControlPlaneView[AgentCard] | None, handles: Sequence[_PeerScope], *, self_name: str) -> list[tuple[str, str | None]]:
    """The live, in-scope, self-excluded ``(name, description)`` peers, sorted by name.

    Scope is the union of the handles' curated names, OR — if any handle is ``discover=True`` — every live
    agent. Self is always excluded (load-bearing in discover mode: every agent advertises, so its own card
    is in the snapshot). A curated name absent from the live view is omitted with a per-turn WARNING (the
    directory re-renders every turn, so it self-heals); a missing/degraded view degrades to ``[]``.
    """
    snapshot = _safe_snapshot(view)
    live = set(snapshot)
    if any(h.discover for h in handles):
        names = live - {self_name}
    else:
        curated = {name for h in handles for name in h.names}
        for absent in sorted((curated - live) - {self_name}):
            # Capability-agnostic: the same render serves messaging and handoff (the prefix would mislabel
            # a handoff omission), so the warning names only the unreachable peer.
            logger.warning("peer %r is not currently reachable; omitting it this turn", absent)
        names = (curated & live) - {self_name}
    return [(name, snapshot[name].description) for name in sorted(names)]


def render_peer_directory(entries: Sequence[tuple[str, str | None]]) -> str:
    """Format the resolved peers into a directory body: ``name — description`` per line (sorted, name-only
    when a peer has no description); an empty set renders the "none reachable" sentinel. Shared by the
    ``message_agent`` tool description (§5.2, still advertised so the model retains the capability) and the
    ``HandoffRequest`` option doc (§5.3)."""
    if not entries:
        return _NONE_REACHABLE
    return "\n".join(f"{name} — {description}" if description else name for name, description in entries)
