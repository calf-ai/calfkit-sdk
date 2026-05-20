"""Exception hierarchy for the calfkit SDK.

All SDK-raised exceptions inherit from :class:`CalfkitError` so users can catch
the whole family with a single ``except`` clause. Concrete subtypes carry the
specific semantics (deserialization failure, aggregator state-store failure,
etc.).
"""


class CalfkitError(Exception):
    """Base class for all exceptions raised by the calfkit SDK."""


class DeserializationError(CalfkitError):
    """Raised when client-side output deserialization fails."""


class DurabilityConfigError(CalfkitError):
    """Raised when the Kafka producer configuration would compromise the
    durability invariants the fan-out aggregator relies on.

    The aggregator writes its state to a compacted Kafka topic via the
    same FastStream producer that publishes inter-node messages. If the
    producer is configured with ``acks!="all"`` or
    ``enable_idempotence=False`` a single-broker outage between the
    leader's ack and follower catch-up can silently drop a state-topic
    write — leaving the aggregator's in-memory cache out of sync with
    the durable log and surfacing as inconsistent recovery on the next
    rebalance.

    :meth:`calfkit.client.base.BaseClient.connect` raises this when the
    user-supplied ``broker_kwargs`` would weaken the contract.
    """
