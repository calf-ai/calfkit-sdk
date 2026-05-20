"""In-process state store + in-flight batch representation.

Internal — used directly by :class:`~calfkit.nodes.aggregator.testing.InMemoryAggregator`
for unit-test fixtures, and indirectly by the production
:class:`_KafkaStateStore` which wraps the same in-memory data structures
with Kafka-backed durability.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from calfkit.models.state import State
from calfkit.nodes.aggregator.state import FanOutState


@dataclass(frozen=True)
class _InFlightBatch:
    """Immutable in-memory representation of an in-flight fan-out batch.

    Mirrors :class:`FanOutState` on the wire but lives in process memory.
    The state store converts between the two when reading from / writing
    to the compacted topic.

    Frozen and ``received`` exposed as a :class:`~collections.abc.Mapping`
    (backed by :class:`types.MappingProxyType`) so the aggregator's
    "publish then update cache" invariant cannot be undermined by an
    in-place mutation: callers building an updated batch use
    :meth:`with_received` to produce a new instance, leaving the original
    intact if the durable publish fails.
    """

    correlation_id: str
    fan_out_id: str
    expected_tool_call_ids: frozenset[str]
    base_state: State
    started_at_ms: int
    last_updated_ms: int
    agent_topic: str
    degraded: bool = False
    received: Mapping[str, Any] = field(default_factory=dict)
    traceparent: str | None = None
    tracestate: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.received, MappingProxyType):
            # ``object.__setattr__`` is needed because the dataclass is frozen.
            object.__setattr__(self, "received", MappingProxyType(dict(self.received)))

    def to_fanout_state(self) -> FanOutState:
        return FanOutState(
            correlation_id=self.correlation_id,
            fan_out_id=self.fan_out_id,
            expected_tool_call_ids=self.expected_tool_call_ids,
            received=dict(self.received),
            base_state=self.base_state,
            started_at_ms=self.started_at_ms,
            last_updated_ms=self.last_updated_ms,
            agent_topic=self.agent_topic,
            degraded=self.degraded,
            traceparent=self.traceparent,
            tracestate=self.tracestate,
        )

    @classmethod
    def from_fanout_state(cls, state: FanOutState) -> _InFlightBatch:
        return cls(
            correlation_id=state.correlation_id,
            fan_out_id=state.fan_out_id,
            expected_tool_call_ids=state.expected_tool_call_ids,
            base_state=state.base_state,
            received=dict(state.received),
            started_at_ms=state.started_at_ms,
            last_updated_ms=state.last_updated_ms,
            agent_topic=state.agent_topic,
            degraded=state.degraded,
            traceparent=state.traceparent,
            tracestate=state.tracestate,
        )

    def with_received(
        self,
        received: Mapping[str, Any],
        *,
        last_updated_ms: int,
    ) -> _InFlightBatch:
        """Return a new batch with ``received`` and ``last_updated_ms`` replaced.

        The aggregator handler uses this to build the post-merge batch
        BEFORE the durable publish, so a failed publish leaves the cached
        batch unchanged. Redelivery (driven by the fanout-returns
        subscription's ack_policy=NACK_ON_ERROR) re-merges the return
        cleanly. Mutating ``self.received`` in place would couple the
        cache to the in-flight modification and let a publish failure
        produce a phantom-merged result that no record reflects.
        """
        return _InFlightBatch(
            correlation_id=self.correlation_id,
            fan_out_id=self.fan_out_id,
            expected_tool_call_ids=self.expected_tool_call_ids,
            base_state=self.base_state,
            received=received,
            started_at_ms=self.started_at_ms,
            last_updated_ms=last_updated_ms,
            agent_topic=self.agent_topic,
            degraded=self.degraded,
            traceparent=self.traceparent,
            tracestate=self.tracestate,
        )


class _TtlSet:
    """A set with per-entry TTL expiry, using a configurable monotonic clock.

    Used by the aggregator to remember recently-tombstoned batch keys so
    late returns arriving after completion can be distinguished from orphan
    returns. Tests inject a fake clock via the ``clock`` argument for
    deterministic TTL tests.

    Expiry handling: ``__contains__`` performs an O(1) per-key expiry
    check (dropping the key if its TTL has lapsed). Bulk sweeping happens
    periodically on ``add`` (every ``_SWEEP_EVERY_N_ADDS`` writes),
    bounding memory growth even under add-heavy workloads with no
    intervening reads.
    """

    _SWEEP_EVERY_N_ADDS = 64

    def __init__(self, ttl_seconds: float, clock: Callable[[], float] | None = None) -> None:
        self._ttl_seconds = ttl_seconds
        self._clock: Callable[[], float] = clock if clock is not None else time.monotonic
        self._entries: dict[Any, float] = {}
        self._adds_since_sweep = 0

    def add(self, key: Any) -> None:
        self._entries[key] = self._clock() + self._ttl_seconds
        self._adds_since_sweep += 1
        if self._adds_since_sweep >= self._SWEEP_EVERY_N_ADDS:
            self._sweep()

    def __contains__(self, key: Any) -> bool:
        # O(1) per-key expiry check. The full bulk sweep happens
        # periodically on add() (every _SWEEP_EVERY_N_ADDS); on the
        # hot read path we just check this one key's expiry.
        # Sweeping the entire entries dict per __contains__ — the
        # previous behaviour — would have been O(n) per inbound
        # tool return.
        if key not in self._entries:
            return False
        if self._entries[key] <= self._clock():
            del self._entries[key]
            return False
        return True

    def remove(self, key: Any) -> None:
        self._entries.pop(key, None)

    def discard(self, key: Any) -> None:
        """Set-like discard: drop ``key`` if present, no-op otherwise."""
        self._entries.pop(key, None)

    def clear(self) -> None:
        self._entries.clear()
        self._adds_since_sweep = 0

    def _sweep(self) -> None:
        now = self._clock()
        expired = [k for k, exp in self._entries.items() if exp <= now]
        for k in expired:
            del self._entries[k]
        self._adds_since_sweep = 0


class _InMemoryStateStore:
    """In-process state store with no Kafka backing — for unit tests.

    Implements the same lookup / put / tombstone / mark-completed API the
    production :class:`_KafkaStateStore` will expose, so tests of aggregator
    behaviour (merge, should_complete, on_partial, late-return rejection)
    can exercise the full lifecycle without a broker.
    """

    def __init__(self, completion_ttl_seconds: float = 60.0, clock: Callable[[], float] | None = None) -> None:
        self._cache: dict[tuple[str, str], _InFlightBatch] = {}
        self._recently_completed = _TtlSet(completion_ttl_seconds, clock=clock)

    def get(self, key: tuple[str, str]) -> _InFlightBatch | None:
        return self._cache.get(key)

    def put(self, key: tuple[str, str], batch: _InFlightBatch) -> None:
        self._cache[key] = batch

    def tombstone(self, key: tuple[str, str]) -> None:
        """Remove an active batch; remember the key for ``completion_ttl_seconds``
        so late returns are rejected as 'recently completed' rather than
        'orphan'."""
        self._cache.pop(key, None)
        self._recently_completed.add(key)

    def mark_completed(self, key: tuple[str, str]) -> None:
        """Idempotent alias for :meth:`tombstone` — kept for API parity with the
        production Kafka store which distinguishes the durable tombstone write
        from the local cache update."""
        self.tombstone(key)

    def was_recently_completed(self, key: tuple[str, str]) -> bool:
        return key in self._recently_completed

    def all_keys(self) -> list[tuple[str, str]]:
        """Test helper: enumerate currently-active batch keys."""
        return list(self._cache.keys())
