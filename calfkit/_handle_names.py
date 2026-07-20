"""Shared construction rails for the capability handles (:class:`~calfkit.nodes.tool.Tools`,
:class:`~calfkit.peers.messaging.Messaging`, :class:`~calfkit.peers.handoff.Handoff`,
:class:`~calfkit.nodes.toolbox.Toolboxes`).

All four are frozen, identity-only handles with the *same* curated-XOR-discover varargs constructor; this
is the single home for that fail-loud rail (an accidental empty splat must never silently become an implicit
"everything"). The classes stay distinct — they discriminate by **type** (``isinstance``) for dispatch
and for ``tools=``/``peers=`` routing — but the construction grammar lives here so the rail is defined once.

The grammar (:func:`_selection_grammar`) is shared; the **collection policy** is per-family:
name handles dedupe by value (a repeated name is repetition), toolbox entries raise on a repeated
name (an entry carries policy — ``include=`` — so a duplicate is a conflict, never collapsed).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Protocol, TypeVar


def _selection_grammar(
    label: str,
    noun: str,
    *,
    positional: tuple[object, ...],
    alt: Sequence[object] | None,
    discover: bool,
    item_singular: str,
    item_plural: str,
    kwarg_label: str,
) -> tuple[object, ...] | None:
    """The discover-XOR-items grammar shared by every family handle.

    Returns ``None`` for discover mode, else the raw ordered item tuple (pre-collection).
    Owns the three fail-loud branches: discover-takes-no-items, varargs-XOR-kwarg, and
    empty-raises. ``kwarg_label`` is threaded so each family's error names its *own* kwarg
    (``names=`` for the name handles, ``entries=`` for ``Toolboxes``).
    """
    # A bare string satisfies Sequence[str] and would silently iterate character-wise —
    # reject it for the whole family (decided 2026-07-19, post-review).
    if isinstance(alt, str):
        raise ValueError(f"{label}: {kwarg_label}= must be a sequence of {item_plural}, not a bare string")
    # ``discover`` IS the absence of items, so pairing it with items is contradictory.
    if discover and (positional or alt is not None):
        raise ValueError(f"{label}(discover=True) takes no {noun} {item_plural}")
    if discover:
        return None
    if positional and alt is not None:
        raise ValueError(f"{label}: pass {noun} {item_plural} positionally or via {kwarg_label}=, not both")
    collected = positional if positional else tuple(alt or ())
    if not collected:
        raise ValueError(f"{label} requires at least one {noun} {item_singular}, or discover=True")
    return collected


def normalize_handle_names(label: str, noun: str, *, positional: tuple[str, ...], names: Sequence[str] | None, discover: bool) -> tuple[str, ...]:
    """The curated name set for a capability handle, under the discover-XOR-names invariant shared by
    ``Tools``/``Messaging``/``Handoff``.

    Returns ``()`` for discover mode (the handle takes every live node of its kind), else an
    order-preserving-deduped tuple of non-empty names. ``label`` is the class name and ``noun`` the per-kind
    word (``"tool"`` / ``"agent"``) used verbatim in the error messages.

    Both, or neither, of {non-empty names, ``discover=True``} raise — never an implicit "everything" (the
    fail-loud rail: an accidental empty splat ``Handle(*[])`` must not silently open the full scope).
    """
    raw = _selection_grammar(
        label,
        noun,
        positional=positional,
        alt=names,
        discover=discover,
        item_singular="name",
        item_plural="names",
        kwarg_label="names",
    )
    if raw is None:
        return ()
    collected = tuple(dict.fromkeys(raw))  # order-preserving dedupe: a repeated name is repetition, not conflict
    if any(not n for n in collected):
        raise ValueError(f"{label} names must be non-empty")
    return collected  # type: ignore[return-value]  # grammar preserves the str elements it was given


class _NamedEntry(Protocol):
    @property
    def name(self) -> str: ...


_EntryT = TypeVar("_EntryT", bound=_NamedEntry)


def normalize_toolbox_entries(
    label: str,
    *,
    positional: tuple[object, ...],
    entries: Sequence[object] | None,
    discover: bool,
    desugar: Callable[[object], _EntryT],
) -> tuple[_EntryT, ...]:
    """The entry tuple for a toolbox-family selector: grammar + desugar + by-name duplicate-raise.

    Returns ``()`` for discover mode, else the desugared, order-preserving tuple of entries.
    ``desugar`` is the family's element constructor (bare name → entry spec; specs pass through) —
    injected so this leaf module never imports from ``calfkit.nodes``. Duplicates raise **by
    name**, byte-identical entries included: an entry carries per-box policy, so a repeat is a
    conflict — never the silent value-dedupe of the name rail.
    """
    raw = _selection_grammar(
        label,
        "toolbox",
        positional=positional,
        alt=entries,
        discover=discover,
        item_singular="entry",
        item_plural="entries",
        kwarg_label="entries",
    )
    if raw is None:
        return ()
    desugared = tuple(desugar(e) for e in raw)
    seen: set[str] = set()
    for entry in desugared:
        if entry.name in seen:
            raise ValueError(f"{label}: duplicate toolbox {entry.name!r} — one policy per toolbox per handle")
        seen.add(entry.name)
    return desugared
