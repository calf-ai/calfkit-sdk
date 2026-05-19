"""In-process state store + in-flight batch representation.

Internal — used directly by :class:`~calfkit.nodes.aggregator.testing.InMemoryAggregator`
for unit-test fixtures, and indirectly by the production
:class:`_KafkaStateStore` (PR 4) which wraps the same in-memory data
structures with Kafka-backed durability.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from calfkit.models.state import State
from calfkit.nodes.aggregator.state import FanOutState


@dataclass
class _InFlightBatch:
    """Mutable in-memory representation of an in-flight fan-out batch.

    Mirrors :class:`FanOutState` on the wire but lives in process memory for
    hot-path mutation. The state store converts between the two when reading
    from / writing to the compacted topic.
    """

    correlation_id: str
    fan_out_id: str
    expected_tool_call_ids: frozenset[str]
    base_state: State
    received: dict[str, Any] = field(default_factory=dict)
    started_at_ms: int = 0
    last_updated_ms: int = 0
    agent_topic: str = ""
    traceparent: str | None = None
    tracestate: str | None = None

    def to_fanout_state(self) -> FanOutState:
        return FanOutState(
            correlation_id=self.correlation_id,
            fan_out_id=self.fan_out_id,
            expected_tool_call_ids=self.expected_tool_call_ids,
            received=self.received,
            base_state=self.base_state,
            started_at_ms=self.started_at_ms,
            last_updated_ms=self.last_updated_ms,
            agent_topic=self.agent_topic,
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
            traceparent=state.traceparent,
            tracestate=state.tracestate,
        )

    def with_received(
        self,
        received: dict[str, Any],
        *,
        last_updated_ms: int,
    ) -> _InFlightBatch:
        """Return a new batch with ``received`` and ``last_updated_ms`` replaced.

        The aggregator handler uses this to build the post-merge batch
        BEFORE the durable publish, so a failed publish leaves the cached
        batch unchanged (and FastStream's redelivery re-merges the return
        cleanly). Mutating ``self.received`` in place would couple the
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
            traceparent=self.traceparent,
            tracestate=self.tracestate,
        )


class _TtlSet:
    """A set with per-entry TTL expiry, using a configurable monotonic clock.

    Used by the aggregator to remember recently-tombstoned batch keys so
    late returns arriving after completion can be distinguished from orphan
    returns. Tests inject a fake clock via the ``clock`` argument for
    deterministic TTL tests.
    """

    def __init__(self, ttl_seconds: float, clock: Callable[[], float] | None = None) -> None:
        self._ttl_seconds = ttl_seconds
        self._clock: Callable[[], float] = clock if clock is not None else time.monotonic
        self._entries: dict[Any, float] = {}

    def add(self, key: Any) -> None:
        self._entries[key] = self._clock() + self._ttl_seconds

    def __contains__(self, key: Any) -> bool:
        self._sweep()
        return key in self._entries

    def remove(self, key: Any) -> None:
        self._entries.pop(key, None)

    def clear(self) -> None:
        self._entries.clear()

    def _sweep(self) -> None:
        now = self._clock()
        expired = [k for k, exp in self._entries.items() if exp <= now]
        for k in expired:
            del self._entries[k]


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
