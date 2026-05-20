"""Kafka connection configuration captured at Client.connect time.

FastStream exposes no public API to retrieve a KafkaBroker's bootstrap
servers or security/SASL/SSL settings after construction. The aggregator
subsystem needs them for the transient AIOKafkaConsumer used during
state-topic rehydration. Rather than reaching into FastStream private
internals, Client.connect captures the kwargs it passed and threads
them forward through Worker._prepare_aggregators -> FanOutAggregator.
setup -> _KafkaStateStore.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# Minimum recommended ``rebalance_timeout_ms`` for groups that own
# aggregator state-topic partitions. Rehydration reads the entire
# compacted partition contents on every reassignment; if the worker
# can't finish before the broker considers it dead, the broker fires
# another rebalance and the system enters a rebalance storm.
#
# aiokafka's default is 30s, which is fine for stateless consumers but
# too tight for state-store rehydration on production-sized topics.
# Five minutes gives realistic headroom for partition counts in the
# low hundreds and segments in the low GB range, without becoming so
# permissive that genuine worker hangs go undetected.
_REHYDRATE_REBALANCE_TIMEOUT_FLOOR_MS: int = 300_000


@dataclass(frozen=True)
class KafkaConfig:
    """Snapshot of the Kafka client kwargs Client.connect used to
    construct the FastStream KafkaBroker.

    The common Kafka client options are surfaced as typed fields for
    discoverability and static-type checking. ``client_kwargs`` remains
    the escape hatch for any additional aiokafka kwargs (e.g.
    ``rebalance_timeout_ms``, ``session_timeout_ms``) that aren't
    promoted to a typed field.

    Used by :class:`~calfkit.worker.worker.Worker` to construct the
    transient :class:`AIOKafkaConsumer` the aggregator's state store
    spins up during rehydration. Without this snapshot, rehydration
    cannot inherit the broker's security / SASL / SSL settings.
    """

    bootstrap_servers: str | list[str]
    security_protocol: str | None = None
    sasl_mechanism: str | None = None
    sasl_plain_username: str | None = None
    sasl_plain_password: str | None = None
    # Typed as Any to avoid importing ssl in this hot module; callers
    # pass an ``ssl.SSLContext`` here.
    ssl_context: Any | None = None
    client_id: str | None = None
    client_kwargs: dict[str, Any] = field(default_factory=dict)

    def to_consumer_kwargs(self) -> dict[str, Any]:
        """Return the merged kwargs dict for ``AIOKafkaConsumer(**kwargs)``.

        Typed fields take precedence over ``client_kwargs`` entries on
        key collision — typed fields are explicit user intent, while
        ``client_kwargs`` is the escape hatch. ``None`` values are
        skipped so they don't override aiokafka defaults.
        """
        # ``client_kwargs`` first, typed overrides on top.
        merged: dict[str, Any] = {k: v for k, v in self.client_kwargs.items() if v is not None}
        merged["bootstrap_servers"] = self.bootstrap_servers
        typed_fields: dict[str, Any] = {
            "security_protocol": self.security_protocol,
            "sasl_mechanism": self.sasl_mechanism,
            "sasl_plain_username": self.sasl_plain_username,
            "sasl_plain_password": self.sasl_plain_password,
            "ssl_context": self.ssl_context,
            "client_id": self.client_id,
        }
        for key, value in typed_fields.items():
            if value is not None:
                merged[key] = value
        return merged

    def check_rehydration_timeout_floor(self) -> None:
        """Log a WARN if ``rebalance_timeout_ms`` is below the recommended floor.

        Rehydration reads the entire compacted state-topic partition on
        every reassignment. The worker must finish before
        ``rebalance_timeout_ms`` expires, otherwise the broker considers
        it dead and triggers another rebalance — leading to a rebalance
        storm on production-sized topics.

        Aiokafka's default ``rebalance_timeout_ms`` is 30s; the
        recommended floor for aggregator workers is documented on
        :data:`_REHYDRATE_REBALANCE_TIMEOUT_FLOOR_MS` (5 minutes).
        """
        merged = self.to_consumer_kwargs()
        # aiokafka's default when unset; mirrors aiokafka.AIOKafkaConsumer
        # session/rebalance timeouts at the time of writing (30s).
        aiokafka_default_ms = 30_000
        configured = merged.get("rebalance_timeout_ms", aiokafka_default_ms)
        if configured < _REHYDRATE_REBALANCE_TIMEOUT_FLOOR_MS:
            logger.warning(
                "rebalance_timeout_ms=%d is below the recommended floor of %d for "
                "workers that own fan-out aggregator partitions. Rehydration may "
                "not complete before the broker considers the worker dead and "
                "triggers another rebalance (rebalance-storm risk). Raise "
                "rebalance_timeout_ms via Client.connect(..., rebalance_timeout_ms=%d).",
                configured,
                _REHYDRATE_REBALANCE_TIMEOUT_FLOOR_MS,
                _REHYDRATE_REBALANCE_TIMEOUT_FLOOR_MS,
            )
