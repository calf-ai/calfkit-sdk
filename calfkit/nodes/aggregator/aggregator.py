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

from calfkit.nodes.aggregator.state import (
    AggregatedReturn,
    AggregatorBatch,
    ToolCallId,
)

logger = logging.getLogger(__name__)


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
        idle_timeout_seconds: float | None = None,
    ) -> None:
        """Initialise the aggregator.

        Args:
            merge_error_policy: How to handle exceptions raised by
                :meth:`merge`. Default :data:`MergeErrorPolicy.ABORT` raises
                :class:`AggregatorMergeError`; see :class:`MergeErrorPolicy`
                for the alternatives.
            idle_timeout_seconds: If set, batches whose ``last_updated_ms`` is
                older than this many seconds are reaped as failed (a
                :class:`FanOutTimeoutError` is logged and the batch is
                tombstoned). ``None`` (default) disables idle-timeout
                reaping; batches live until they complete or are evicted by
                a partition rebalance.
        """
        self.merge_error_policy = merge_error_policy
        self.idle_timeout_seconds = idle_timeout_seconds

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
