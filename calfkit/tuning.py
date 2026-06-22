"""Worker-level tuning for the ktables-backed substrates (ADR-0021).

`KTableReaderTuning` is the shared reader-cadence value object; `FanoutConfig` is the
worker-level config for fan-out agents' durable batch stores. Both are passed to
``Worker(...)`` (``fanout=``); see :class:`~calfkit.controlplane.ControlPlaneConfig` for the
control-plane equivalent, which composes the same `KTableReaderTuning`.

The two reader-cadence knobs are ktables `KafkaTable`/`GroupedKafkaTable` constructor
parameters: lowering both cuts idle ``barrier()`` latency
(``~ max(fetch_max_wait_ms, poll_timeout_ms)``) at the cost of more fetches/wakeups. ``None``
means "omit it, let ktables apply its own default" — so ktables stays the single source of
truth for the default value.
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field

#: A positive timeout in milliseconds. ``strict=True`` so a config knob is a real ``int`` —
#: not a coerced ``bool`` (``True -> 1``), ``float`` (``5.0 -> 5``), or ``str`` (``"5" -> 5``).
PositiveTimeoutMs = Annotated[int, Field(ge=1, strict=True)]

#: A finite, strictly-positive timeout in seconds (rejects ``0``, negatives, ``inf``, ``nan``).
PositiveFiniteFloat = Annotated[float, Field(gt=0, allow_inf_nan=False)]


class KTableReaderTuning(BaseModel):
    """Reader-cadence knobs for one ktables reader the worker opens (reader-only).

    ``None`` (the default for both) omits the knob so ktables applies its own default
    (``poll_timeout_ms=200``, ``fetch_max_wait_ms=500``).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    poll_timeout_ms: PositiveTimeoutMs | None = None
    """Reader-loop ``getmany`` cadence; gates the barrier resolution half + idle CPU."""

    fetch_max_wait_ms: PositiveTimeoutMs | None = None
    """Consumer fetch long-poll; gates the barrier end-offset snapshot half."""

    def as_kwargs(self) -> dict[str, Any]:
        """The set (non-``None``) knobs, ready to splat into a ktables ``*.json(...)`` factory."""
        return self.model_dump(exclude_none=True)


class FanoutConfig(BaseModel):
    """Worker-level tuning for fan-out agents' durable batch stores."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    reader_tuning: KTableReaderTuning = Field(default_factory=KTableReaderTuning)
    """Cadence applied to the store's two compacted-table readers (``state`` + ``basestate``)."""

    catchup_timeout: PositiveFiniteFloat | None = None
    """Bound on each reader's catch-up gate at ``start()``; ``None`` => ktables' default."""

    barrier_timeout: PositiveFiniteFloat = 30.0
    """Per-read ``barrier()`` timeout for the read-your-own-writes freshness wait."""


__all__ = ["FanoutConfig", "KTableReaderTuning", "PositiveFiniteFloat", "PositiveTimeoutMs"]
