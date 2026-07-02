"""Agent-daemon supervisor for ``ck dev`` (dev-agent-lifecycle spec §5).

The agent-layer sibling of :mod:`calfkit.cli._dev_broker`, one layer up: preflight ``module:attr``
targets, connect-or-spawn detached agent daemons (identified statelessly by the ``--dev-daemon``
argv marker), gate on presence-plane readiness, and provide the stop/status data the ``ck dev``
management commands render.

Import hygiene (load-bearing, the ``_dev_broker`` rule): ``ck dev`` imports this module on every
invocation, so nothing here may import ``psutil`` at module top — it ships only in the ``[mesh]``
extra and is imported lazily by the process scan.
"""

from __future__ import annotations

DEV_HEARTBEAT_INTERVAL = 5.0
"""The dev heartbeat preset (spec §5.6): every worker ``ck dev`` launches — foreground runs,
``-d`` daemons, and in-process chat session workers alike — heartbeats every 5s instead of the
30s production default, so crash-staleness detection (3 × interval) is ~15s. Writer-side only:
the reader's ``stale_after`` derives from the interval stamped on each record and must never be
shortened below the writer's cadence (live agents would flap offline between heartbeats)."""
