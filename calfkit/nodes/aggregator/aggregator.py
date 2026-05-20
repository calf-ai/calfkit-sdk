"""User-extensible aggregator for parallel tool fan-out batches.

The :class:`FanOutAggregator` class exposes three behavioural override methods
(:meth:`~FanOutAggregator.merge`, :meth:`~FanOutAggregator.should_complete`,
:meth:`~FanOutAggregator.on_partial`) and is composed onto an agent via
``agent.aggregator = FanOutAggregator()``. When an aggregator is not provided,
the framework auto-attaches the default — so the hello-world case requires
zero user code beyond constructing the agent.

The Kafka mechanics (per-agent state and returns topics, compacted-topic
durability, partition-scoped in-memory cache, rebalance-driven rehydration)
are entirely framework-managed. This module exposes only the behaviour
surface; the framework runtime lives on :attr:`FanOutAggregator._runtime`.

The aggregator is **process-local**. State held in ``__init__`` (e.g., an LLM
client used by :meth:`merge`) is per-process; cross-process coordination
happens via the ``{node_id}.fanout-state`` compacted topic, not via
attributes on this object.

.. note::

   The fanout-returns subscription is configured with
   ``AckPolicy.NACK_ON_ERROR`` so handler raises rewind the offset for
   redelivery. The default :data:`MergeErrorPolicy.FALLBACK_TO_DEFAULT`
   logs the user-merge failure and completes the batch via the framework's
   default merge so non-transient bugs don't loop indefinitely.

   Users who opt into :data:`MergeErrorPolicy.ABORT` should be aware that
   ABORT raises :class:`AggregatorMergeError`, which under
   ``AckPolicy.NACK_ON_ERROR`` triggers indefinite redelivery for
   non-transient failures. The aggregator does not yet ship a DLQ on
   ``{node_id}.fanout-returns``; deployments running ABORT in production
   should provision their own DLQ or accept the redelivery-loop risk
   (tracked in ``ROADMAP.md``).
"""

from __future__ import annotations

import enum
import logging
from types import MappingProxyType
from typing import TYPE_CHECKING

from calfkit.client.kafka_config import KafkaConfig
from calfkit.nodes.aggregator._runtime import _FanOutRuntime
from calfkit.nodes.aggregator.errors import AggregatorStateStoreError
from calfkit.nodes.aggregator.state import (
    AggregatedReturn,
    AggregatorBatch,
)

if TYPE_CHECKING:
    from faststream.kafka import KafkaBroker

    from calfkit.models.state import State
    from calfkit.nodes.aggregator._in_memory_store import _InFlightBatch

logger = logging.getLogger(__name__)


class MergeErrorPolicy(str, enum.Enum):
    """How the aggregator handles exceptions raised from :meth:`FanOutAggregator.merge`.

    Process-local enum — values are never persisted to the durable state
    topic, so the member names can evolve pre-1.0 without a wire-format
    migration.

    The default is :data:`FALLBACK_TO_DEFAULT`: a failed user merge logs
    and completes via the framework default so a buggy custom merge can't
    silently stall the partition with an unbounded NACK redelivery loop.
    Users who prefer fail-loud semantics (and have a DLQ wired up) can opt
    into :data:`ABORT` explicitly.
    """

    ABORT = "abort"
    """Raise :class:`AggregatorMergeError` and fail the batch loudly.

    .. warning::

       Under ``AckPolicy.NACK_ON_ERROR`` (the fanout-returns subscription
       default) the raise causes Kafka to redeliver the inbound. If the
       merge failure is non-transient (a bug in the user's :meth:`merge`),
       the message redelivers indefinitely until an operator intervenes.
       The aggregator does not yet ship a DLQ on
       ``{node_id}.fanout-returns`` (see ``ROADMAP.md``); deployments
       opting into ABORT in production should provision their own DLQ or
       accept the redelivery-loop risk.
    """

    RETRY = "retry"
    """Retry the merge once. Useful for transient failures (e.g., a
    downstream LLM call that times out). If the retry also raises,
    behaves as :data:`ABORT`.

    .. warning::

       The user-provided :meth:`FanOutAggregator.merge` is awaited without
       a framework-level timeout. A hanging merge (e.g., a downstream
       LLM/DB call that never returns) blocks the aggregator handler for
       the affected partition for the entire consumer session, stalling
       all in-flight returns on that partition. Wrap your merge logic in
       :func:`asyncio.wait_for` (or equivalent) if downstream calls can
       hang. A framework-level merge-timeout option is tracked as a
       future enhancement in ``ROADMAP.md``.
    """

    FALLBACK_TO_DEFAULT = "fallback_to_default"
    """Log the error and complete the batch via the framework's default merge
    (apply all received results to ``base_state``). Useful for best-effort
    aggregation where falling back to the un-customised state is acceptable.
    The framework stamps :data:`HDR_DEGRADED_MERGE` on the published envelope
    so operators can detect silently-degraded batches. If the default merge
    itself raises, the framework treats it as :data:`ABORT`."""


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
        merge_error_policy: MergeErrorPolicy = MergeErrorPolicy.FALLBACK_TO_DEFAULT,
    ) -> None:
        """Initialise the aggregator.

        Args:
            merge_error_policy: How to handle exceptions raised by
                :meth:`merge`. Default :data:`MergeErrorPolicy.FALLBACK_TO_DEFAULT`
                logs the user-merge failure and completes the batch via the
                framework's default merge (stamping
                :data:`HDR_DEGRADED_MERGE` for observability) so a buggy
                custom merge can't stall the partition in an unbounded NACK
                redelivery loop. See :class:`MergeErrorPolicy` for the
                fail-loud :data:`ABORT` and one-shot :data:`RETRY`
                alternatives.
        """
        self.merge_error_policy = merge_error_policy
        # Populated by :meth:`setup` at worker startup. Framework-internal
        # state stays behind a single underscore-prefixed attribute rather
        # than leaking five separate _state_topic / _state_store / etc.
        # attributes onto the public class. Access via :attr:`runtime`
        # (raises if unset) or directly via ``_runtime`` (may be None
        # pre-setup, used by ``kafka_subscriptions`` which can run before
        # ``setup()``).
        self._runtime: _FanOutRuntime | None = None

    @property
    def runtime(self) -> _FanOutRuntime:
        """Return the framework-managed runtime state, or raise.

        Strict validation: raises :class:`AggregatorStateStoreError` if
        :meth:`setup` has not run. :meth:`~calfkit.worker.worker.Worker.run`
        handles setup automatically in production; tests bypassing
        ``Worker.run()`` must call
        :func:`calfkit.nodes.aggregator.testing.setup_for_tests` explicitly
        before invoking handler paths.

        Pre-setup access from non-handler paths (e.g., ``kafka_subscriptions``
        building a subscription list) should check ``self._runtime is not None``
        instead — the property intentionally raises so handler code stays terse.
        """
        if self._runtime is None:
            raise AggregatorStateStoreError(
                "FanOutAggregator.setup() must run before runtime access. "
                "Worker.run() runs setup at startup; tests bypassing Worker.run() "
                "must call calfkit.nodes.aggregator.testing.setup_for_tests("
                "agent, broker) before invoking handler paths."
            )
        return self._runtime

    # ------------------------------------------------------------------
    # Framework lifecycle — called by Worker before subscribers register
    # ------------------------------------------------------------------

    async def setup(
        self,
        broker: KafkaBroker,
        node_id: str,
        main_topic: str,
        *,
        kafka_config: KafkaConfig,
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
            kafka_config: Connection config (bootstrap servers + SASL/SSL
                kwargs) captured by ``Client.connect`` and threaded
                forward by the worker. Forwarded to the underlying
                :class:`_KafkaStateStore` so its transient
                :class:`AIOKafkaConsumer` for rehydration inherits the
                same security/transport settings as the broker.
        """
        if self._runtime is not None:
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

        # ``to_consumer_kwargs`` merges the typed fields (security_protocol,
        # sasl_*, client_id, ...) with the ``client_kwargs`` escape hatch
        # so the rehydration consumer inherits the broker's full auth
        # config. Pop bootstrap_servers because the state store accepts
        # it as a separate constructor parameter.
        consumer_kwargs = kafka_config.to_consumer_kwargs()
        consumer_bootstrap = consumer_kwargs.pop("bootstrap_servers")
        state_store = _KafkaStateStore(
            broker=broker,
            state_topic=state_topic,
            bootstrap_servers=consumer_bootstrap,
            partition_count=partition_count,
            client_kwargs=consumer_kwargs,
        )
        rebalance_listener = _StateStoreRebalanceListener(
            state_store=state_store,
            returns_topic=returns_topic,
        )

        self._runtime = _FanOutRuntime(
            state_topic=state_topic,
            returns_topic=returns_topic,
            partition_count=partition_count,
            state_store=state_store,
            rebalance_listener=rebalance_listener,
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

    def _batch_view(
        self,
        batch: _InFlightBatch,
        *,
        base_state_copy: State | None = None,
    ) -> AggregatorBatch:
        """Build the immutable :class:`AggregatorBatch` view passed to overrides.

        ``base_state`` is deep-copied so a user :meth:`merge` that mutates
        ``batch.base_state`` (instead of operating on a copy) cannot corrupt
        the framework's cached :class:`_InFlightBatch.base_state`. The view's
        ``received`` mapping is wrapped in ``MappingProxyType`` for the same
        defensive reason.

        ``base_state_copy`` lets the caller pass a pre-computed deep copy
        of ``batch.base_state`` so a single handler invocation can amortise
        one deep copy across the up-to-three override calls (``on_partial``,
        ``should_complete``, ``merge``). The contract documented on
        :meth:`merge` — observers don't mutate, ``merge`` may mutate last —
        is what makes a shared copy safe. When ``None`` the legacy
        per-call deep-copy behaviour is preserved (used by tests that
        invoke ``_batch_view`` directly).
        """
        if base_state_copy is None:
            base_state_copy = batch.base_state.model_copy(deep=True)
        return AggregatorBatch(
            correlation_id=batch.correlation_id,
            fan_out_id=batch.fan_out_id,
            expected_tool_call_ids=batch.expected_tool_call_ids,
            received=MappingProxyType(dict(batch.received)),
            base_state=base_state_copy,
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

        Mutation contract: ``merge`` MAY mutate ``batch.base_state``;
        :meth:`should_complete` and :meth:`on_partial` MUST NOT. The
        framework relies on this ordering to amortise a single deep copy
        of ``base_state`` across all three override calls per handler
        invocation (observers run first, ``merge`` runs last). Mutating
        ``base_state`` from an observer would silently corrupt the view
        that ``merge`` later sees.

        The default ``merge`` does its own ``model_copy(deep=True)``
        defensively so the FALLBACK_TO_DEFAULT recovery path (which reuses
        the view after a user override raised) still sees a clean state
        even if the failed user override partially mutated ``base_state``
        before raising.

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
        newly_received: frozenset[str],
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
