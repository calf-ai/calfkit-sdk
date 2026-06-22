"""Shared non-fixture helpers for the real-broker (``kafka``) integration lane.

The lane's *fixtures* (``kafka_bootstrap``, ``topic_namespace``) live in ``conftest.py``; this
module is the single home for plain helper functions shared across the discovery/MCP suites
(mirroring ``_roundtrip_helpers.py``, which is scoped to the offline model/history helpers).
"""

from __future__ import annotations

from typing import Any

from calfkit.controlplane import ControlPlaneConfig
from calfkit.tuning import KTableReaderTuning


def fast_control_plane(bootstrap: str, **overrides: Any) -> ControlPlaneConfig:
    """The standard control-plane config for the kafka lane, tuned for low convergence latency.

    Lowers the ktables reader cadence (poll/fetch) and the heartbeat so capability records become
    visible in tens of ms instead of being gated by the ~500ms idle-``barrier()`` floor + a slow
    heartbeat — which shortens the discovery/MCP round-trip tests. Behaviour is unchanged, only
    faster. ``overrides`` pass through to :class:`~calfkit.controlplane.ControlPlaneConfig`.
    """
    return ControlPlaneConfig(
        reader_tuning=KTableReaderTuning(poll_timeout_ms=20, fetch_max_wait_ms=10),
        heartbeat_interval=0.1,
        bootstrap_servers=bootstrap,
        **overrides,
    )
