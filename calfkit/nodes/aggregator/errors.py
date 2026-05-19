"""Exception hierarchy for the durable fan-out aggregator.

All aggregator errors inherit from :class:`~calfkit.exceptions.CalfkitError`
so they can be caught alongside other SDK exceptions with a single ``except``
clause.
"""

from calfkit.exceptions import CalfkitError


class AggregatorError(CalfkitError):
    """Base class for fan-out aggregator-related errors."""


class AggregatorMergeError(AggregatorError):
    """Raised when :meth:`FanOutAggregator.merge` raises and the configured
    ``merge_error_policy`` is :data:`MergeErrorPolicy.ABORT` (or RETRY exhausted)."""


class AggregatorStateStoreError(AggregatorError):
    """Raised when the aggregator's state store cannot be initialised or maintained.

    Typical causes:

    - Topic configuration mismatch (e.g., ``cleanup.policy != compact``).
    - Partition-count mismatch between the agent's main topic and the
      aggregator's state/returns topics (co-partitioning violation).
    - Broker unreachable during startup rehydration.
    """
