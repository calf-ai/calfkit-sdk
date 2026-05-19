"""Framework-managed runtime state for a :class:`FanOutAggregator`.

The user-facing :class:`FanOutAggregator` only holds behaviour overrides
(``merge_error_policy`` plus the three async hooks). The Kafka mechanics
— per-agent state and returns topics, partition count, the durable state
store, the rebalance listener — are populated by
:meth:`FanOutAggregator.setup` at worker startup and live on this frozen
dataclass under a single ``_runtime`` attribute, instead of five
separate underscore-prefixed attributes on the public class.

The agent's internal handlers reach into the runtime via
:attr:`FanOutAggregator.runtime` (raises :class:`AggregatorStateStoreError`
if ``setup()`` hasn't run) or :attr:`FanOutAggregator._runtime` (returns
``None`` pre-setup, used by ``kafka_subscriptions`` to register a
subscriber whose ``listener=`` is filled in after setup).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from calfkit.nodes.aggregator._kafka_state_store import _KafkaStateStore
    from calfkit.nodes.aggregator._rebalance import _StateStoreRebalanceListener


@dataclass(frozen=True)
class _FanOutRuntime:
    """Framework-managed runtime state for a :class:`FanOutAggregator`.

    Constructed once inside :meth:`FanOutAggregator.setup`; subsequent
    setup calls are no-ops (idempotent). Frozen so framework-internal
    code can rely on the values staying stable for the aggregator's
    lifetime.
    """

    state_topic: str
    """``{node_id}.fanout-state`` — compacted topic name."""

    returns_topic: str
    """``{node_id}.fanout-returns`` — regular topic the aggregator's
    handler subscribes to."""

    partition_count: int
    """Total partition count for the state + returns topics. Must match
    the agent's main topic (co-partitioning invariant)."""

    state_store: _KafkaStateStore
    """The durable state store backing the compacted state topic."""

    rebalance_listener: _StateStoreRebalanceListener
    """The :class:`ConsumerRebalanceListener` driving partition-scoped
    rehydration and eviction on rebalance events."""
