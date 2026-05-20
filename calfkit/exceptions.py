"""Exception hierarchy for the calfkit SDK.

All SDK-raised exceptions inherit from :class:`CalfkitError` so users can catch
the whole family with a single ``except`` clause. Concrete subtypes carry the
specific semantics (deserialization failure, aggregator state-store failure,
etc.).
"""

from typing import Any


class CalfkitError(Exception):
    """Base class for all exceptions raised by the calfkit SDK."""


class DeserializationError(CalfkitError):
    """Raised when client-side output deserialization fails."""


class DurabilityConfigError(CalfkitError):
    """Raised when the Kafka producer/consumer configuration would compromise
    the durability invariants the fan-out aggregator relies on.

    The aggregator writes its state to a compacted Kafka topic via the
    same FastStream producer that publishes inter-node messages. If the
    producer is configured with ``acks!="all"`` or
    ``enable_idempotence=False`` a single-broker outage between the
    leader's ack and follower catch-up can silently drop a state-topic
    write — leaving the aggregator's in-memory cache out of sync with
    the durable log and surfacing as inconsistent recovery on the next
    rebalance.

    The exception also covers consumer-side misconfiguration that would
    threaten rehydration correctness (e.g. ``rebalance_timeout_ms``
    below the recommended floor — see
    :meth:`calfkit.client.kafka_config.KafkaConfig.assert_rehydration_timeout_ok`)
    and ``KafkaConfig`` typed-field / ``client_kwargs`` collisions.

    :meth:`calfkit.client.base.BaseClient.connect` raises this when the
    user-supplied ``broker_kwargs`` would weaken the contract, as does
    the :class:`~calfkit.client.kafka_config.KafkaConfig` constructor on
    collision.

    Structured attributes
    ---------------------
    Operators can branch on the structured context programmatically
    instead of regex-matching the message string:

    * ``kwarg_name`` — the offending Kafka kwarg name (e.g. ``"acks"``,
      ``"enable_idempotence"``, ``"rebalance_timeout_ms"``,
      ``"security_protocol"``).
    * ``offending_value`` — the value the caller supplied that triggered
      the raise.
    * ``expected_value`` — the value (or constraint, formatted as a
      string) the SDK expected; ``None`` when no single expected value
      applies.
    """

    def __init__(
        self,
        message: str,
        *,
        kwarg_name: str | None = None,
        offending_value: Any = None,
        expected_value: Any = None,
    ) -> None:
        super().__init__(message)
        self.kwarg_name = kwarg_name
        self.offending_value = offending_value
        self.expected_value = expected_value
