"""Worker-level control-plane configuration (spec §11).

Optional tuning for the control-plane substrate. The knobs split by side:
``heartbeat_interval`` is the *publisher's* (and is stamped on every record, so it
reaches readers); ``stale_after`` and ``catchup_timeout`` are the *view's*;
``bootstrap_servers`` applies to whichever side opens a table. A reader in a
different process than the writer needs no agreement on ``heartbeat_interval`` —
it arrives on the record (spec §5, §9).
"""

from __future__ import annotations

import math
from dataclasses import dataclass

STALE_MULTIPLIER = 3
"""Default staleness threshold = ``STALE_MULTIPLIER × record.heartbeat_interval``
when a reader sets no explicit ``stale_after`` (spec §9)."""


@dataclass(frozen=True)
class ControlPlaneConfig:
    """Tuning for the control-plane substrate; every field is optional.

    Args:
        heartbeat_interval: Seconds between record re-publishes (the publisher's
            loop cadence). Also stamped on every record so a reader can judge
            staleness without knowing this value.
        stale_after: Read-side override/floor for the staleness threshold. ``None``
            (default) derives ``STALE_MULTIPLIER × record.heartbeat_interval``.
        catchup_timeout: Bound on a view's catch-up gate.
        bootstrap_servers: Override for the control-plane Kafka cluster. ``None``
            (default) derives from the connected client; set only for the
            split-cluster case (control plane on different brokers than data).
    """

    heartbeat_interval: float = 30.0
    stale_after: float | None = None
    catchup_timeout: float = 30.0
    bootstrap_servers: str | None = None

    def __post_init__(self) -> None:
        # `not isfinite(...)` guards NaN/inf, which slip past every `<= 0` comparison.
        if not math.isfinite(self.heartbeat_interval) or self.heartbeat_interval <= 0:
            raise ValueError(f"heartbeat_interval must be a finite number > 0, got {self.heartbeat_interval}")
        if not math.isfinite(self.catchup_timeout) or self.catchup_timeout <= 0:
            raise ValueError(f"catchup_timeout must be a finite number > 0, got {self.catchup_timeout}")
        if self.stale_after is not None and (not math.isfinite(self.stale_after) or self.stale_after <= 0):
            raise ValueError(f"stale_after must be a finite number > 0 or None, got {self.stale_after}")
