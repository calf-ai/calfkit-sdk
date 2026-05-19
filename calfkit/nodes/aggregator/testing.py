"""Test harness for fan-out aggregator behaviour.

:class:`InMemoryAggregator` mirrors :class:`FanOutAggregator`'s behaviour
surface (the three override methods) plus the full aggregation lifecycle
(dispatch → submit returns → complete / tombstone), all in-process with no
Kafka. Tests exercise the lifecycle via :meth:`initialize_batch` and
:meth:`submit_return`; async tests wait on completion via
:meth:`wait_for_completion` or partial state via :meth:`wait_for_partial_state`.

``persist_to_disk`` defaults to ``True`` so the harness mirrors the real
Kafka-backed durability of the production aggregator. Set to ``False`` only
for fault-injection tests that explicitly want to simulate "no durable
store available."
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from calfkit.models.state import State
from calfkit.nodes.aggregator._in_memory_store import _InFlightBatch, _InMemoryStateStore
from calfkit.nodes.aggregator.aggregator import FanOutAggregator, MergeErrorPolicy
from calfkit.nodes.aggregator.errors import AggregatorMergeError
from calfkit.nodes.aggregator.state import (
    AggregatedReturn,
    AggregatorBatch,
    FanOutState,
    ToolCallId,
)


class InMemoryAggregator(FanOutAggregator):
    """In-process aggregator harness for unit tests.

    Replaces the Kafka-backed state store with :class:`_InMemoryStateStore` and
    exposes :meth:`initialize_batch` / :meth:`submit_return` so tests drive the
    lifecycle directly without spinning up a broker.

    Subclass and override :meth:`FanOutAggregator.merge` /
    :meth:`should_complete` / :meth:`on_partial` to test custom aggregator
    behaviour, or use :meth:`wrap` to wrap an existing
    :class:`FanOutAggregator` subclass with the in-memory store.
    """

    def __init__(
        self,
        *,
        merge_error_policy: MergeErrorPolicy = MergeErrorPolicy.ABORT,
        persist_to_disk: bool = True,
        disk_path: str | Path | None = None,
        completion_ttl_seconds: float = 60.0,
    ) -> None:
        """Initialise the harness.

        Args:
            merge_error_policy: How exceptions from :meth:`merge` are handled.
                Forwarded to :class:`FanOutAggregator`.
            persist_to_disk: When ``True`` (default), the harness writes a
                JSONL log of state mutations to ``disk_path`` so a
                :meth:`simulate_restart` followed by re-instantiation
                recovers the state. Models the real Kafka-backed durability.
                Set to ``False`` only for tests that explicitly want
                "no durable store" semantics (fault-injection).
            disk_path: Optional path for the JSONL log. If ``None`` while
                ``persist_to_disk=True``, persistence is a no-op (the
                harness still behaves "as if" persistent but writes nothing
                — useful for tests that just need the default flag).
            completion_ttl_seconds: TTL for the recently-completed batch set;
                late returns within this window are dropped as duplicates,
                later returns are treated as orphans.
        """
        super().__init__(
            merge_error_policy=merge_error_policy,
        )
        self.persist_to_disk = persist_to_disk
        self._disk_path: Path | None = Path(disk_path) if disk_path is not None else None
        self._completion_ttl_seconds = completion_ttl_seconds
        self._clock: Callable[[], float] = time.monotonic
        self._store: _InMemoryStateStore = _InMemoryStateStore(
            completion_ttl_seconds=completion_ttl_seconds,
            clock=self._clock,
        )
        self._completion_events: dict[tuple[str, str], asyncio.Event] = {}
        self._partial_state_events: dict[tuple[str, str], list[tuple[int, asyncio.Event]]] = {}

        if self.persist_to_disk and self._disk_path is not None:
            self.rehydrate_from_disk()

    # ------------------------------------------------------------------
    # Test-only configuration helpers
    # ------------------------------------------------------------------

    def set_clock(self, clock: Callable[[], float]) -> None:
        """Inject a fake clock callable for deterministic TTL / timeout tests.

        The clock is a zero-arg callable returning a float (seconds since
        an arbitrary epoch). Replaces the default :func:`time.monotonic`
        and rebuilds the recently-completed TTL set against the new clock.
        """
        self._clock = clock
        self._store = _InMemoryStateStore(
            completion_ttl_seconds=self._completion_ttl_seconds,
            clock=clock,
        )

    def simulate_restart(self) -> None:
        """Simulate a worker restart: wipe the in-process cache, then rehydrate
        from disk if persistence is enabled.

        Models the real worker-restart flow: process memory is gone, durable
        log survives, restart replays the log to rebuild local state.
        """
        self._store = _InMemoryStateStore(
            completion_ttl_seconds=self._completion_ttl_seconds,
            clock=self._clock,
        )
        self._completion_events.clear()
        self._partial_state_events.clear()
        if self.persist_to_disk and self._disk_path is not None:
            self.rehydrate_from_disk()

    def rehydrate_from_disk(self) -> None:
        """Load batches from the JSONL log back into the in-memory cache.

        No-op if ``disk_path`` is ``None`` or the file doesn't exist.
        """
        if self._disk_path is None or not self._disk_path.exists():
            return
        with self._disk_path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                key = (record["correlation_id"], record["fan_out_id"])
                if record.get("tombstone"):
                    self._store.tombstone(key)
                else:
                    state = FanOutState.model_validate(record["state"])
                    self._store.put(key, _InFlightBatch.from_fanout_state(state))

    def _persist_put(self, key: tuple[str, str], batch: _InFlightBatch) -> None:
        if not self.persist_to_disk or self._disk_path is None:
            return
        self._disk_path.parent.mkdir(parents=True, exist_ok=True)
        with self._disk_path.open("a") as f:
            f.write(
                json.dumps(
                    {
                        "correlation_id": key[0],
                        "fan_out_id": key[1],
                        "tombstone": False,
                        "state": batch.to_fanout_state().model_dump(mode="json"),
                    }
                )
                + "\n"
            )

    def _persist_tombstone(self, key: tuple[str, str]) -> None:
        if not self.persist_to_disk or self._disk_path is None:
            return
        self._disk_path.parent.mkdir(parents=True, exist_ok=True)
        with self._disk_path.open("a") as f:
            f.write(
                json.dumps(
                    {
                        "correlation_id": key[0],
                        "fan_out_id": key[1],
                        "tombstone": True,
                    }
                )
                + "\n"
            )

    # ------------------------------------------------------------------
    # Lifecycle helpers — drive the aggregator from tests
    # ------------------------------------------------------------------

    async def initialize_batch(
        self,
        correlation_id: str,
        fan_out_id: str,
        expected_tool_call_ids: frozenset[str],
        base_state: State,
        agent_topic: str = "agent.in",
        traceparent: str | None = None,
        tracestate: str | None = None,
    ) -> None:
        """Simulate the agent dispatching a fan-out — registers the initial batch.

        In production, ``_publish_action`` writes the initial :class:`FanOutState`
        record to the compacted topic before publishing the tool ``Call``\\ s; the
        test harness skips the publish step and just seeds the in-memory store.
        """
        key = (correlation_id, fan_out_id)
        now_ms = int(time.time() * 1000)
        batch = _InFlightBatch(
            correlation_id=correlation_id,
            fan_out_id=fan_out_id,
            expected_tool_call_ids=expected_tool_call_ids,
            base_state=base_state,
            received={},
            started_at_ms=now_ms,
            last_updated_ms=now_ms,
            agent_topic=agent_topic,
            traceparent=traceparent,
            tracestate=tracestate,
        )
        self._store.put(key, batch)
        self._persist_put(key, batch)

    async def submit_return(
        self,
        correlation_id: str,
        fan_out_id: str,
        tool_call_id: str,
        result: Any,
    ) -> AggregatedReturn | None:
        """Simulate a tool return arriving — drives the same handler path as
        the production returns subscriber.

        Returns the merged :class:`AggregatedReturn` if the batch completed on
        this return, ``None`` otherwise.

        Idempotent: re-submitting the same
        ``(correlation_id, fan_out_id, tool_call_id)`` is a no-op (mirrors
        the production dedup on ``tool_call_id in state.received``).

        Returns from a recently-tombstoned batch are dropped (mirrors the
        late-return rejection path).
        """
        key = (correlation_id, fan_out_id)

        if self._store.was_recently_completed(key):
            return None

        batch = self._store.get(key)
        if batch is None:
            return None  # orphan return — no active batch

        if tool_call_id not in batch.expected_tool_call_ids:
            return None  # unexpected tool_call_id
        if tool_call_id in batch.received:
            return None  # duplicate — idempotent

        batch.received[tool_call_id] = result
        batch.last_updated_ms = int(time.time() * 1000)
        self._store.put(key, batch)
        self._persist_put(key, batch)

        self._fire_partial_state_waiters(key, batch)

        view = self._batch_view(batch)
        await self.on_partial(view, frozenset([ToolCallId(tool_call_id)]))

        if await self.should_complete(view):
            merged = await self._run_merge(view, key)
            self._store.tombstone(key)
            self._persist_tombstone(key)
            if key in self._completion_events:
                self._completion_events[key].set()
            return merged

        return None

    async def _run_merge(
        self,
        view: AggregatorBatch,
        key: tuple[str, str],
    ) -> AggregatedReturn:
        """Invoke :meth:`merge` honouring the configured :class:`MergeErrorPolicy`."""
        try:
            return await self.merge(view)
        except Exception as exc:
            if self.merge_error_policy == MergeErrorPolicy.ABORT:
                raise AggregatorMergeError(f"merge() raised for key={key}") from exc
            if self.merge_error_policy == MergeErrorPolicy.RETRY:
                try:
                    return await self.merge(view)
                except Exception as exc2:
                    raise AggregatorMergeError(f"merge() raised on retry for key={key}") from exc2
            if self.merge_error_policy == MergeErrorPolicy.DROP:
                # Fall back to default merge so the batch still completes.
                return await FanOutAggregator.merge(self, view)
            raise

    def _batch_view(self, batch: _InFlightBatch) -> AggregatorBatch:
        return AggregatorBatch(
            correlation_id=batch.correlation_id,
            fan_out_id=batch.fan_out_id,
            expected_tool_call_ids=batch.expected_tool_call_ids,
            received=dict(batch.received),
            base_state=batch.base_state,
            started_at_ms=batch.started_at_ms,
            last_updated_ms=batch.last_updated_ms,
        )

    def _fire_partial_state_waiters(self, key: tuple[str, str], batch: _InFlightBatch) -> None:
        if key not in self._partial_state_events:
            return
        remaining: list[tuple[int, asyncio.Event]] = []
        for needed_n, event in self._partial_state_events[key]:
            if len(batch.received) >= needed_n:
                event.set()
            else:
                remaining.append((needed_n, event))
        self._partial_state_events[key] = remaining

    # ------------------------------------------------------------------
    # Async waiters
    # ------------------------------------------------------------------

    async def wait_for_completion(self, key: tuple[str, str], timeout: float = 5.0) -> None:
        """Async wait until the batch identified by ``key`` tombstones.

        Useful for tests that submit returns from a background task and want
        to assert completion ordering without polling.
        """
        if self._store.was_recently_completed(key):
            return
        event = self._completion_events.setdefault(key, asyncio.Event())
        await asyncio.wait_for(event.wait(), timeout=timeout)

    async def wait_for_partial_state(
        self,
        key: tuple[str, str],
        n: int,
        timeout: float = 5.0,
    ) -> None:
        """Async wait until at least ``n`` tool results have been received for
        the batch identified by ``key``."""
        existing = self._store.get(key)
        if existing is not None and len(existing.received) >= n:
            return
        event = asyncio.Event()
        self._partial_state_events.setdefault(key, []).append((n, event))
        await asyncio.wait_for(event.wait(), timeout=timeout)

    # ------------------------------------------------------------------
    # Subclass wrapping
    # ------------------------------------------------------------------

    @classmethod
    def wrap(cls, base: FanOutAggregator, **kwargs: Any) -> InMemoryAggregator:
        """Wrap a user's :class:`FanOutAggregator` subclass with the in-memory
        store.

        Reuses only the behaviour overrides (:meth:`merge`,
        :meth:`should_complete`, :meth:`on_partial`). Configuration tied to a
        real Kafka backend is ignored; :class:`InMemoryAggregator` manages its
        own storage.

        Args:
            base: A :class:`FanOutAggregator` instance whose behaviour
                overrides should be reused.
            **kwargs: Forwarded to :class:`InMemoryAggregator` (e.g.,
                ``persist_to_disk``, ``disk_path``).

        Returns:
            A dynamically-created :class:`InMemoryAggregator` subclass with
            ``base``'s overrides.
        """
        user_cls = type(base)
        wrapped_cls = type(
            f"InMemory{user_cls.__name__}",
            (cls,),
            {
                "merge": user_cls.merge,
                "should_complete": user_cls.should_complete,
                "on_partial": user_cls.on_partial,
            },
        )
        # Defaults inherited from base; kwargs take precedence on conflict.
        init_kwargs: dict[str, Any] = {
            "merge_error_policy": base.merge_error_policy,
            **kwargs,
        }
        instance = wrapped_cls(**init_kwargs)
        # Copy any user-set instance attributes from ``base`` that aren't
        # already on the wrapper. This preserves state the override methods
        # may depend on (e.g., a retry counter set in the user's __init__).
        # Wrapper-managed attributes (merge_error_policy, etc.) are skipped
        # via the ``not in vars(instance)`` check.
        instance_attrs = vars(instance)
        for attr, value in vars(base).items():
            if attr not in instance_attrs:
                setattr(instance, attr, value)
        return cast(InMemoryAggregator, instance)
