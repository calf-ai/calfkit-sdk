"""Durable fan-out aggregator for parallel tool calls in agent nodes.

Public API:

- :class:`FanOutAggregator` — compose onto an agent (``agent.aggregator = ...``)
  to control how parallel tool-call results are merged and when the batch is
  considered complete. The framework auto-attaches a default instance when one
  isn't provided, so the hello-world case requires no extra user code.
- :class:`MergeErrorPolicy` — how :meth:`FanOutAggregator.merge` exceptions
  are handled.
- :class:`AggregatorBatch` — immutable batch view passed to override methods.
- :class:`AggregatedReturn` — value returned from :meth:`FanOutAggregator.merge`.
- :class:`FanOutState` — durable wire-format record (advanced; primarily for
  test inspection).

Exception hierarchy under :class:`~calfkit.exceptions.CalfkitError`:

- :class:`AggregatorError` — base.
- :class:`AggregatorMergeError` — :meth:`merge` raised with ABORT policy.
- :class:`AggregatorStateStoreError` — durable store init / config failure.

Most users instantiate :class:`FanOutAggregator()` directly without subclassing.
The Kafka mechanics (per-agent topics, compacted-state durability, partition-
scoped cache, rebalance-driven rehydration) are entirely framework-managed.
"""

from calfkit.nodes.aggregator.aggregator import FanOutAggregator, MergeErrorPolicy
from calfkit.nodes.aggregator.errors import (
    AggregatorError,
    AggregatorMergeError,
    AggregatorStateStoreError,
)
from calfkit.nodes.aggregator.state import (
    AggregatedReturn,
    AggregatorBatch,
    FanOutState,
)

__all__ = [
    "AggregatedReturn",
    "AggregatorBatch",
    "AggregatorError",
    "AggregatorMergeError",
    "AggregatorStateStoreError",
    "FanOutAggregator",
    "FanOutState",
    "MergeErrorPolicy",
]
