"""The :class:`Handoff` peer-handoff capability handle (ADR-0019)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, Field, create_model

from calfkit.peers.directory import render_peer_directory


@dataclass(frozen=True)
class Handoff:
    """Identity-only handle declaring which peer agents this agent may **hand off** to (a Handoff —
    transfer of conversation control: the handing agent relinquishes and does **not** regain it; the peer
    continues the full conversation and returns to the original caller). Passed in
    ``Agent(peers=[Handoff(...)])``; gates and feeds the runtime-built ``HandoffRequest`` structured-output
    option. Two modes, mutually exclusive on one handle:

    - **curated** — ``Handoff("refunds", "billing")`` / ``Handoff(names=[...])``: the named peers are the
      only handoff targets (the v1 caller-side trust boundary, §7).
    - **open** — ``Handoff(discover=True)``: any live agent is a handoff target.

    Exactly one of {non-empty names, ``discover=True``} — both, or neither, raise. A byte-for-byte mirror of
    the :class:`Messaging` handle: frozen value semantics (names order-preserving-deduped, so equal handles
    compare and hash equal), a custom varargs ``__init__``, and no ``merge`` — multiple same-kind handles
    stay independent and the rendered directory dedupes by name. Deliberately does **not** implement the
    tool protocols (``resolve_tools``/``tool_bindings``), so a handle mistakenly placed in ``tools=`` falls
    through ``split_tool_declarations`` to its ``else -> TypeError`` rather than being silently absorbed
    (M4). The agent's-own-name reject lives in the ``Agent`` ctor (a handle can't see its enclosing agent's
    name, M2).
    """

    names: tuple[str, ...]
    discover: bool = False

    # ``*positional`` varargs (the common case) plus a keyword-only ``names=`` list; no name collision
    # because the varargs param is ``positional`` while the stored field is ``names`` (mirrors ``Tools``).
    def __init__(self, *positional: str, names: Sequence[str] | None = None, discover: bool = False) -> None:
        # ``discover`` IS the absence of names (it opens to every live agent), so pairing it with names
        # is contradictory: exactly one of {non-empty names, discover=True}.
        if discover and (positional or names is not None):
            raise ValueError("Handoff(discover=True) takes no agent names")
        if discover:
            object.__setattr__(self, "names", ())
        else:
            if positional and names is not None:
                raise ValueError("Handoff: pass agent names positionally or via names=, not both")
            source = positional if positional else tuple(names or ())
            collected = tuple(dict.fromkeys(source))  # order-preserving dedupe
            if not collected:
                # Empty STILL raises — never an implicit "everything" (the fail-loud rail: an accidental
                # empty splat ``Handoff(*[])`` must not silently become open mode).
                raise ValueError("Handoff requires at least one agent name, or discover=True")
            if any(not n for n in collected):
                raise ValueError("Handoff names must be non-empty")
            object.__setattr__(self, "names", collected)
        object.__setattr__(self, "discover", discover)


class HandoffRequest(BaseModel):
    """The model produces this as its turn's **output** to transfer control to a peer (§5.3, L4) — a BARE
    structured-output union member, NOT a tool. ``output_type = [final_output_type, HandoffRequest,
    DeferredToolRequests]`` keeps ``allow_text_output=True`` on every provider (a ``ToolOutput`` wrapper
    would force ``tool_choice=required``, which Anthropic rejects under extended thinking); calfkit
    discriminates by ``isinstance(result.output, HandoffRequest)`` for dispatch.

    This is the **stable base**. The per-turn subclass (:func:`_build_handoff_request`) narrows ``name`` to
    a ``Literal`` over the live directory and carries the directory as its ``__doc__``; ``__base__`` keeps
    ``isinstance`` valid across rebuilds.

    - ``name`` — the peer to hand off to (validated against the live ``Literal`` in the per-turn subclass).
    - ``message`` — the handing agent's summary/context for the peer, required non-empty (``min_length=1``),
      so an empty value is auto-retried by pydantic-ai like an out-of-``Literal`` ``name`` (L5/C2).
    """

    name: str
    message: str = Field(min_length=1)


_HANDOFF_PREAMBLE = (
    "Transfer this conversation to another agent. You relinquish control and will NOT regain it — the "
    "chosen agent continues the full conversation and answers the original caller in your place. Choose by "
    "exact name from the agents below, and put any summary or context the agent needs in `message`.\n\n"
    "Agents (name — description):\n"
)

# Injected into the per-run instructions (NOT a persisted message part) when a Handoff handle is present but
# no in-scope peer is live, so the dormant capability stays legible while the member is omitted (§5.3).
# Self-heals: the moment a peer comes online the Literal member returns and this note disappears.
_HANDOFF_NO_PEERS_NOTE = "You cannot currently hand off this conversation or task to another agent, as no other agents are online."


@lru_cache(maxsize=128)
def _build_handoff_request(live: tuple[tuple[str, str | None], ...]) -> type[HandoffRequest]:
    """The per-turn :class:`HandoffRequest` subclass for a given live, sorted directory — built once per
    distinct live set (``@lru_cache``; an explicit bound, not the silent default-128 that reads as
    "unbounded"). Callers pass :func:`~calfkit.peers.directory.resolve_live_peers`' sorted-by-name output,
    so identical directories reuse one model **and** its compiled schema. ``description`` is part of the key
    (the ``__doc__`` depends on it). ``__base__=HandoffRequest`` keeps ``isinstance`` discrimination valid;
    ``name`` is a ``Literal`` over the live names (pydantic-ai natively rejects + auto-retries an
    out-of-directory value, no calfkit code); ``__doc__`` is the handoff directory (the structured-output
    analog of a tool docstring). **Requires a non-empty live set** — an empty ``Literal`` is unbuildable
    (raises at pydantic schema-build), so the dispatch path omits the member instead (§5.3)."""
    if not live:
        raise ValueError("_build_handoff_request requires a non-empty live directory — an empty Literal is unbuildable; omit the member instead")
    names = tuple(name for name, _ in live)
    return create_model(
        "HandoffRequest",
        __base__=HandoffRequest,
        __doc__=_HANDOFF_PREAMBLE + render_peer_directory(live),
        name=(Literal[names], ...),  # names is a runtime tuple -> a dynamic Literal over the live directory
    )
