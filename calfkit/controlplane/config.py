"""Worker-level control-plane configuration (spec §11, ADR-0021).

Optional tuning for the control-plane substrate. The knobs split by side:
``heartbeat_interval`` is the *publisher's* (and is stamped on every record, so it
reaches readers); ``stale_after`` and ``catchup_timeout`` are the *view's*;
``reader_tuning`` tunes the view reader's cadence (idle ``barrier()`` latency);
``bootstrap_servers`` applies to whichever side opens a table. A reader in a
different process than the writer needs no agreement on ``heartbeat_interval`` —
it arrives on the record (spec §5, §9).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from calfkit.tuning import KTableReaderTuning, PositiveFiniteFloat

STALE_MULTIPLIER = 3
"""Default staleness threshold = ``STALE_MULTIPLIER × record.heartbeat_interval``
when a reader sets no explicit ``stale_after`` (spec §9)."""


class ControlPlaneConfig(BaseModel):
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
        reader_tuning: Cadence for the capability-view reader (reader-only). Lower
            both knobs to cut idle ``barrier()`` latency; see
            :class:`~calfkit.tuning.KTableReaderTuning`.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    heartbeat_interval: PositiveFiniteFloat = 30.0
    stale_after: PositiveFiniteFloat | None = None
    catchup_timeout: PositiveFiniteFloat = 30.0
    bootstrap_servers: str | None = None
    reader_tuning: KTableReaderTuning = Field(default_factory=KTableReaderTuning)
