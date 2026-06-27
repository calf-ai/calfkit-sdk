"""Caller-surface streaming events + the firehose tuning default (spec §3.3 / §2.1).

The closed v1 ``RunEvent`` terminal union (``RunCompleted | RunFailed``). Intermediate event
types (AgentMessage, ToolCalled, HandoffOccurred) are a future shape — emitted only once
intermediate emission ships (spec §9.1) — so v1 yields exactly the terminal.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from calfkit.models.error_report import ErrorReport

if TYPE_CHECKING:
    from calfkit.models.envelope import Envelope

DEFAULT_FIREHOSE_BUFFER_SIZE = 1024
"""Default per-observer firehose buffer bound, in events (spec §2.1 / §5.4).

A *starting* default — low thousands, seconds of transient-stall tolerance for coarse
whole-turn events at low memory — tunable from the ``EventStream.dropped`` signal.
"""


@dataclass(frozen=True)
class RunCompleted:
    """A run's successful terminal (spec §3.3).

    ``output`` is the **raw, type-agnostic** best-effort value (``extract_lenient``) — the firehose
    surfaces foreign runs whose ``output_type`` is unknown, so the terminal is never pre-projected.
    The carried ``_envelope`` (the decoded reply) is what ``result()`` projects to the developer's
    ``output_type`` → the rich ``InvocationResult`` (``InvocationResult.from_envelope``, spec §5.9).
    """

    output: Any
    correlation_id: str
    agent: str | None
    _envelope: Envelope = field(repr=False, compare=False)


@dataclass(frozen=True)
class RunFailed:
    """A run's fault terminal: the ``ErrorReport`` carried verbatim (mapped to ``NodeFaultError``
    by ``result()``, spec §5.9)."""

    report: ErrorReport
    correlation_id: str


RunEvent = RunCompleted | RunFailed
"""The closed v1 terminal union — a run's stream ends in exactly one of these. Widened when
intermediate emission ships (spec §3.3 / §9.1)."""
