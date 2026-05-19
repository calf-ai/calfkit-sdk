"""Exception hierarchy for the durable fan-out aggregator.

All aggregator errors inherit from :class:`~calfkit.exceptions.CalfkitError`
so they can be caught alongside other SDK exceptions with a single ``except``
clause. Concrete subclasses carry structured context (correlation_id,
fan_out_id, state_topic) as attributes so callers can act programmatically
on the exception without re-parsing the message string.
"""

from calfkit.exceptions import CalfkitError


class AggregatorError(CalfkitError):
    """Base class for fan-out aggregator-related errors."""


class AggregatorMergeError(AggregatorError):
    """Raised when :meth:`FanOutAggregator.merge` raises and the configured
    ``merge_error_policy`` is :data:`MergeErrorPolicy.ABORT` (or RETRY exhausted).

    Attributes ``correlation_id`` and ``fan_out_id`` identify the affected
    batch so a Sentry / Statsig handler can group failures by run or batch.
    """

    def __init__(
        self,
        message: str = "",
        *,
        correlation_id: str | None = None,
        fan_out_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.correlation_id = correlation_id
        self.fan_out_id = fan_out_id


class AggregatorStateStoreError(AggregatorError):
    """Raised when the aggregator's state store cannot be initialised or maintained.

    Typical causes:

    - Topic configuration mismatch (e.g., ``cleanup.policy != compact``).
    - Partition-count mismatch between the agent's main topic and the
      aggregator's state/returns topics (co-partitioning violation).
    - Broker unreachable during startup rehydration.
    - Stalled or corrupt durable log during partition rehydration.

    Attribute ``state_topic`` identifies which agent's state store is
    affected when the framework operates on multiple agents in the same
    worker.
    """

    def __init__(
        self,
        message: str = "",
        *,
        state_topic: str | None = None,
    ) -> None:
        super().__init__(message)
        self.state_topic = state_topic
