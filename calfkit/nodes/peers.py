"""The ``peers=`` surface: capability-typed handles declaring an agent's agent-to-agent reach (ADR-0015).

A list of capability handles passed to ``Agent(peers=[...])``. The capability is the **type**: a
:class:`Messaging` handle feeds the runtime-rendered ``message_agent`` tool (a Peer message — consult and
keep control). Each handle owns its scope **curated XOR open**, independently per capability. The handles
mirror the shipped ``Tools`` handle (frozen value objects, custom varargs ``__init__``, order-preserving
dedupe, no merge) and deliberately do **not** implement the tool protocols, so they cannot be absorbed by
``tools=`` (M4); the agent's-own-name reject lives in the ``Agent`` ctor, where ``peers=`` is processed (a
handle can't see its enclosing agent's name, M2).

(PR-B introduces ``Messaging``; the ``Handoff`` handle — transfer control via a structured-output union —
is added by PR-C, ADR-0019.)
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class Messaging:
    """Identity-only handle declaring which peer agents this agent may **message** (a Peer message —
    request/reply consult; the caller keeps control). Passed in ``Agent(peers=[Messaging(...)])``; gates
    and feeds the runtime-rendered ``message_agent`` tool. Two modes, mutually exclusive on one handle:

    - **curated** — ``Messaging("billing", "support")`` / ``Messaging(names=[...])``: the named peers are
      the only messageable agents (the v1 caller-side trust boundary, §7).
    - **open** — ``Messaging(discover=True)``: any live agent is messageable.

    Exactly one of {non-empty names, ``discover=True``} — both, or neither, raise. Mirrors the shipped
    ``Tools`` handle: frozen value semantics (names order-preserving-deduped, so equal handles compare and
    hash equal), a custom varargs ``__init__``, and no ``merge`` — multiple same-kind handles stay
    independent and the rendered directory dedupes by name. Deliberately does **not** implement the tool
    protocols (``resolve_tools``/``tool_bindings``), so a handle mistakenly placed in ``tools=`` falls
    through ``split_tool_declarations`` to its ``else -> TypeError`` rather than being silently absorbed
    (M4). The agent's-own-name reject lives in the ``Agent`` ctor (a handle can't see its enclosing
    agent's name, M2).
    """

    names: tuple[str, ...]
    discover: bool = False

    # ``*positional`` varargs (the common case) plus a keyword-only ``names=`` list; no name collision
    # because the varargs param is ``positional`` while the stored field is ``names`` (mirrors ``Tools``).
    def __init__(self, *positional: str, names: Sequence[str] | None = None, discover: bool = False) -> None:
        # ``discover`` IS the absence of names (it opens to every live agent), so pairing it with names
        # is contradictory: exactly one of {non-empty names, discover=True}.
        if discover and (positional or names is not None):
            raise ValueError("Messaging(discover=True) takes no agent names")
        if discover:
            object.__setattr__(self, "names", ())
        else:
            if positional and names is not None:
                raise ValueError("Messaging: pass agent names positionally or via names=, not both")
            source = positional if positional else tuple(names or ())
            collected = tuple(dict.fromkeys(source))  # order-preserving dedupe
            if not collected:
                # Empty STILL raises — never an implicit "everything" (the fail-loud rail: an accidental
                # empty splat ``Messaging(*[])`` must not silently become open mode).
                raise ValueError("Messaging requires at least one agent name, or discover=True")
            if any(not n for n in collected):
                raise ValueError("Messaging names must be non-empty")
            object.__setattr__(self, "names", collected)
        object.__setattr__(self, "discover", discover)
