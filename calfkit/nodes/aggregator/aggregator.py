"""User-extensible aggregator for parallel tool fan-out batches.

The :class:`FanOutAggregator` class exposes three behavioural override methods
(:meth:`~FanOutAggregator.merge`, :meth:`~FanOutAggregator.should_complete`,
:meth:`~FanOutAggregator.on_partial`) and is composed onto an agent via
``agent.aggregator = FanOutAggregator()``. When an aggregator is not provided,
the framework auto-attaches the default — so the hello-world case requires
zero user code beyond constructing the agent.

The Kafka mechanics (per-agent state and returns topics, compacted-topic
durability, partition-scoped in-memory cache, rebalance-driven rehydration)
are entirely framework-managed by the production state store wired up in
PR 4 / PR 5. This module exposes only the behaviour surface.

The aggregator is **process-local**. State held in ``__init__`` (e.g., an LLM
client used by :meth:`merge`) is per-process; cross-process coordination
happens via the ``{node_id}.fanout-state`` compacted topic, not via
attributes on this object.
"""

from __future__ import annotations

import enum
import logging
from typing import TYPE_CHECKING, cast

from calfkit.nodes.aggregator.state import (
    AggregatedReturn,
    AggregatorBatch,
    ToolCallId,
)

if TYPE_CHECKING:
    from faststream.kafka import KafkaBroker

    from calfkit.nodes.aggregator._in_memory_store import _InFlightBatch
    from calfkit.nodes.aggregator._kafka_state_store import _KafkaStateStore
    from calfkit.nodes.aggregator._rebalance import _StateStoreRebalanceListener

logger = logging.getLogger(__name__)


def _extract_bootstrap_servers(broker: KafkaBroker) -> str | list[str]:
    """Pull ``bootstrap_servers`` from a FastStream broker.

    Used by :meth:`FanOutAggregator.setup` to construct the transient
    ``AIOKafkaConsumer`` for state-store rehydration without re-specifying
    connection details. FastStream stores connection params on the broker
    instance itself (``self._connection_kwargs``) — not on
    ``broker.config`` directly.

    Falls back to ``"localhost"`` when no servers are configured (e.g. in
    ``TestKafkaBroker`` flows that never connect to a real broker); the
    transient consumer the value feeds into is never invoked under
    ``TestKafkaBroker``, so the fallback is purely a "don't crash on
    setup" affordance.
    """
    for attr in ("_connection_kwargs",):
        kwargs = getattr(broker, attr, None)
        if isinstance(kwargs, dict) and "bootstrap_servers" in kwargs:
            return cast("str | list[str]", kwargs["bootstrap_servers"])
    config = getattr(broker, "config", None)
    if config is not None:
        client_kwargs = getattr(config, "client_kwargs", None) or {}
        if isinstance(client_kwargs, dict) and "bootstrap_servers" in client_kwargs:
            return cast("str | list[str]", client_kwargs["bootstrap_servers"])
        bootstrap = getattr(config, "bootstrap_servers", None)
        if bootstrap is not None:
            return cast("str | list[str]", bootstrap)
    return "localhost"


class MergeErrorPolicy(str, enum.Enum):
    """How the aggregator handles exceptions raised from :meth:`FanOutAggregator.merge`."""

    ABORT = "abort"
    """Raise :class:`AggregatorMergeError` and fail the batch loudly. Default."""

    RETRY = "retry"
    """Retry the merge once after a small delay. Useful for transient failures
    (e.g., a downstream LLM call that times out). If the retry also raises,
    behaves as :data:`ABORT`."""

    DROP = "drop"
    """Log the error and complete the batch via the default merge (apply all
    received results to ``base_state``). Useful for best-effort aggregation
    where falling back to the un-customised state is acceptable."""


class FanOutAggregator:
    """User-extensible aggregator for parallel tool fan-out batches.

    Compose onto an agent::

        agent = Agent(
            "planner",
            tools=[...],
            aggregator=MyAggregator(),  # optional; default is FanOutAggregator()
        )

    Subclasses override at most three methods:

    - :meth:`merge` — how the per-tool results combine into a single state.
    - :meth:`should_complete` — when to emit the aggregated return.
    - :meth:`on_partial` — observe each partial-result moment (for progress
      events, dashboards, etc.).

    Each override is async and receives an immutable :class:`AggregatorBatch`
    view. The defaults below cover the typical "wait for all tools, then
    apply every result to state" behaviour, so most users instantiate
    :class:`FanOutAggregator` directly without subclassing.
    """

    def __init__(
        self,
        *,
        merge_error_policy: MergeErrorPolicy = MergeErrorPolicy.ABORT,
    ) -> None:
        """Initialise the aggregator.

        Args:
            merge_error_policy: How to handle exceptions raised by
                :meth:`merge`. Default :data:`MergeErrorPolicy.ABORT` raises
                :class:`AggregatorMergeError`; see :class:`MergeErrorPolicy`
                for the alternatives.
        """
        self.merge_error_policy = merge_error_policy
        # Populated by :meth:`setup` at worker startup. Internal attributes;
        # the agent's `_aggregator_handler` reads them.
        self._state_topic: str | None = None
        self._returns_topic: str | None = None
        self._partition_count: int | None = None
        self._state_store: _KafkaStateStore | None = None
        self._rebalance_listener: _StateStoreRebalanceListener | None = None

    # ------------------------------------------------------------------
    # Framework lifecycle — called by Worker before subscribers register
    # ------------------------------------------------------------------

    async def setup(
        self,
        broker: KafkaBroker,
        node_id: str,
        main_topic: str,
    ) -> None:
        """Provision topics and wire up the state store + rebalance listener.

        Called by :class:`~calfkit.worker.worker.Worker` exactly once per
        agent, after ``broker.connect()`` and before
        ``Worker.register_handlers()``. Subsequent calls are a no-op.

        Args:
            broker: A connected FastStream :class:`KafkaBroker`. The broker
                must already be connected so ``broker.config.admin_client``
                is available.
            node_id: The agent's node_id; used as the prefix for the two
                aggregator topics.
            main_topic: The agent's main input topic; used to read the
                target partition count (for co-partitioning).
        """
        if self._state_store is not None:
            return  # idempotent

        # Local imports avoid a circular dependency: the state store and
        # listener pull in aiokafka, which we don't want at module import
        # time for the public API surface.
        from calfkit.nodes.aggregator._kafka_state_store import _KafkaStateStore
        from calfkit.nodes.aggregator._rebalance import _StateStoreRebalanceListener
        from calfkit.nodes.aggregator._topic_admin import ensure_aggregator_topics

        state_topic, returns_topic, partition_count = await ensure_aggregator_topics(
            broker,
            node_id=node_id,
            main_topic=main_topic,
        )

        self._state_topic = state_topic
        self._returns_topic = returns_topic
        self._partition_count = partition_count

        bootstrap_servers = _extract_bootstrap_servers(broker)
        self._state_store = _KafkaStateStore(
            broker=broker,
            state_topic=state_topic,
            bootstrap_servers=bootstrap_servers,
            partition_count=partition_count,
        )
        self._rebalance_listener = _StateStoreRebalanceListener(
            state_store=self._state_store,
            returns_topic=returns_topic,
        )

        logger.info(
            "aggregator setup complete: node=%s state_topic=%s returns_topic=%s partitions=%d",
            node_id,
            state_topic,
            returns_topic,
            partition_count,
        )

    # ------------------------------------------------------------------
    # Helpers used by the agent handler — internal API
    # ------------------------------------------------------------------

    def _batch_view(self, batch: _InFlightBatch) -> AggregatorBatch:
        """Build the immutable :class:`AggregatorBatch` view passed to overrides."""
        return AggregatorBatch(
            correlation_id=batch.correlation_id,
            fan_out_id=batch.fan_out_id,
            expected_tool_call_ids=batch.expected_tool_call_ids,
            received=dict(batch.received),
            base_state=batch.base_state,
            started_at_ms=batch.started_at_ms,
            last_updated_ms=batch.last_updated_ms,
        )

    # ------------------------------------------------------------------
    # User-overridable behaviour
    # ------------------------------------------------------------------

    async def merge(self, batch: AggregatorBatch) -> AggregatedReturn:
        """Merge the received tool results into the agent's state.

        Default: copies :attr:`AggregatorBatch.base_state` and writes each
        received tool result into ``state.tool_results``. Override to
        summarise, deduplicate, or transform the results before they're
        returned to the agent.

        Called once per batch, when :meth:`should_complete` returns ``True``.

        Args:
            batch: Immutable view of the batch at completion time.

        Returns:
            An :class:`AggregatedReturn` containing the merged state. The
            framework publishes ``state`` back to the agent's main topic.
        """
        state = batch.base_state.model_copy(deep=True)
        for tool_call_id, result in batch.received.items():
            state.add_tool_result(tool_call_id, result)
        return AggregatedReturn(state=state)

    async def should_complete(self, batch: AggregatorBatch) -> bool:
        """Decide whether the batch is ready to be merged and returned.

        Default: all expected ``tool_call_ids`` have results in
        :attr:`AggregatorBatch.received`. Override to short-circuit on the
        first success (FirstSuccessAggregator pattern), complete on a
        partial threshold, time-box the batch, etc.

        Called after every tool return is merged.

        Args:
            batch: Immutable view of the batch after the latest return was
                applied.

        Returns:
            ``True`` to trigger :meth:`merge` and complete the batch;
            ``False`` to keep waiting for more returns.
        """
        return batch.is_complete_by_count

    async def on_partial(
        self,
        batch: AggregatorBatch,
        newly_received: frozenset[ToolCallId],
    ) -> None:
        """Observe a partial-result moment.

        Called after each tool return is merged into the batch state but
        before :meth:`should_complete` is evaluated. Default: no-op.
        Override to emit progress events, update dashboards, etc.

        Must not block the aggregation loop; expensive work should be
        dispatched to a separate task.

        Args:
            batch: Immutable view of the batch after the latest return.
            newly_received: Tool_call_ids whose results were just added.
                Typically a single-element frozenset (each tool returns
                independently), but the framework accepts multi-element
                returns for forward compatibility.
        """
        pass
