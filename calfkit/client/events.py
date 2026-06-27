"""Caller-surface streaming events + the firehose tuning default (spec §3.3 / §2.1).

The closed v1 ``RunEvent`` terminal union (``RunCompleted | RunFailed``). Intermediate event
types (AgentMessage, ToolCalled, HandoffOccurred) are a future shape — emitted only once
intermediate emission ships (spec §9.1) — so v1 yields exactly the terminal.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from calfkit.models.error_report import ErrorReport

DEFAULT_FIREHOSE_BUFFER_SIZE = 1024
"""Default per-observer firehose buffer bound, in events (spec §2.1 / §5.4).

A *starting* default — low thousands, seconds of transient-stall tolerance for coarse
whole-turn events at low memory — tunable from the ``EventStream.dropped`` signal.
"""


@dataclass(frozen=True)
class RunCompleted:
    """A run's successful terminal: the raw (unprojected) output and who produced it (spec §3.3)."""

    output: Any
    correlation_id: str
    agent: str | None


@dataclass(frozen=True)
class RunFailed:
    """A run's fault terminal: the ``ErrorReport`` carried verbatim (mapped to ``NodeFaultError``
    by ``result()``, spec §5.9)."""

    report: ErrorReport
    correlation_id: str


RunEvent = RunCompleted | RunFailed
"""The closed v1 terminal union — a run's stream ends in exactly one of these. Widened when
intermediate emission ships (spec §3.3 / §9.1)."""
