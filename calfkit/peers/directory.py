"""The live, scoped, self-excluded peer directory render (§5.2).

``resolve_live_peers`` reads a ``ControlPlaneView[AgentCard]`` (via ``view.snapshot()``) and a list of
capability handles (``Messaging`` in PR-B; ``Handoff`` reuses this in PR-C) and returns the in-scope,
live, self-excluded ``(name, description)`` peers, sorted by name. A curated name absent from the live
view is omitted with a per-turn WARNING (mirroring the unresolved-selector warning in selector
resolution); a degraded or missing view warns-and-degrades to an empty directory, never raising on
reader health (§9). ``render_peer_directory`` formats the entries into the ``message_agent`` tool
description body (or a "none reachable" sentinel — the tool is still advertised so the model retains the
capability).
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from calfkit.peers.messaging import Messaging

if TYPE_CHECKING:
    from calfkit.controlplane.view import ControlPlaneView
    from calfkit.models.agents import AgentCard

logger = logging.getLogger(__name__)

_NONE_REACHABLE = "(no peer agents are currently reachable)"


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


def resolve_live_peers(view: ControlPlaneView[AgentCard] | None, handles: Sequence[Messaging], *, self_name: str) -> list[tuple[str, str | None]]:
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
            logger.warning("message_agent: peer %r is not currently reachable; omitting it this turn", absent)
        names = (curated & live) - {self_name}
    return [(name, snapshot[name].description) for name in sorted(names)]


def render_peer_directory(entries: Sequence[tuple[str, str | None]]) -> str:
    """Format the resolved peers into the ``message_agent`` directory body: ``name — description`` per
    line (sorted, name-only when a peer has no description); an empty set renders the "none reachable"
    sentinel (the tool is still advertised so the model retains the capability, §5.2)."""
    if not entries:
        return _NONE_REACHABLE
    return "\n".join(f"{name} — {description}" if description else name for name, description in entries)
