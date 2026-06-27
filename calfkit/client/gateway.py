"""The caller-surface agent gateway (spec §2.2).

v1 lands the ``Dispatch`` fire token here; the ``AgentGateway`` + verb triad
(``send`` / ``start`` / ``execute``) arrive in a later commit.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Dispatch:
    """What ``send()`` returns: a fire token carrying only the ``correlation_id``.

    Deliberately **not** an :class:`~calfkit.client.invocation_handle.InvocationHandle` — the
    type itself says the result is not retrievable by id. Observe a ``send()`` result on the
    firehose (``client.events()``), or use ``start()`` / ``execute()`` to get a per-run result.
    """

    correlation_id: str
