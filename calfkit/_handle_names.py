"""Shared name-normalization for the capability handles (:class:`~calfkit.nodes.tool.Tools`,
:class:`~calfkit.peers.messaging.Messaging`, :class:`~calfkit.peers.handoff.Handoff`).

All three are frozen, identity-only handles with the *same* curated-XOR-discover varargs constructor; this
is the single home for that fail-loud rail (an accidental empty splat must never silently become an implicit
"everything"). The three classes stay distinct — they discriminate by **type** (``isinstance``) for dispatch
and for ``tools=``/``peers=`` routing — but the construction logic lives here so the rail is defined once.
"""

from __future__ import annotations

from collections.abc import Sequence


def normalize_handle_names(label: str, noun: str, *, positional: tuple[str, ...], names: Sequence[str] | None, discover: bool) -> tuple[str, ...]:
    """The curated name set for a capability handle, under the discover-XOR-names invariant shared by
    ``Tools``/``Messaging``/``Handoff``.

    Returns ``()`` for discover mode (the handle takes every live node of its kind), else an
    order-preserving-deduped tuple of non-empty names. ``label`` is the class name and ``noun`` the per-kind
    word (``"tool"`` / ``"agent"``) used verbatim in the error messages.

    Both, or neither, of {non-empty names, ``discover=True``} raise — never an implicit "everything" (the
    fail-loud rail: an accidental empty splat ``Handle(*[])`` must not silently open the full scope).
    """
    # ``discover`` IS the absence of names, so pairing it with names is contradictory.
    if discover and (positional or names is not None):
        raise ValueError(f"{label}(discover=True) takes no {noun} names")
    if discover:
        return ()
    if positional and names is not None:
        raise ValueError(f"{label}: pass {noun} names positionally or via names=, not both")
    collected = tuple(dict.fromkeys(positional if positional else tuple(names or ())))  # order-preserving dedupe
    if not collected:
        raise ValueError(f"{label} requires at least one {noun} name, or discover=True")
    if any(not n for n in collected):
        raise ValueError(f"{label} names must be non-empty")
    return collected
