"""The ``peers=`` surface: capability-typed handles declaring an agent's agent-to-agent reach.

A list of capability handles passed to ``Agent(peers=[...])``. The capability is the **type**: a
:class:`Messaging` handle feeds the runtime-rendered ``message_agent`` tool (a Peer message — consult and
keep control). Each handle owns its scope **curated XOR open**, independently per capability. The handles
mirror the shipped ``Tools`` handle (frozen value objects, custom varargs ``__init__``, order-preserving
dedupe, no merge) and deliberately do **not** implement the tool protocols, so they cannot be absorbed by
``tools=`` (M4); the agent's-own-name reject lives in the ``Agent`` ctor, where ``peers=`` is processed (a
handle can't see its enclosing agent's name, M2).

(PR-B introduces :class:`Messaging`; the ``Handoff`` handle — transfer control via a structured-output
union — is added by PR-C, ADR-0019.)
"""

from calfkit.peers.messaging import Messaging

__all__ = ["Messaging"]
