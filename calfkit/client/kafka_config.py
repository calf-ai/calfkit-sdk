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

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class KafkaConfig:
    """Snapshot of the Kafka client kwargs Client.connect used to
    construct the FastStream KafkaBroker.

    ``client_kwargs`` is the rest of the construction kwargs (excluding
    framework-specific args like middlewares / partitioner) -- typically
    security_protocol, sasl_mechanism, sasl_plain_username,
    sasl_plain_password, ssl_context, client_id. They are forwarded to
    the transient AIOKafkaConsumer the state store uses for rehydration.
    """

    bootstrap_servers: str | list[str]
    client_kwargs: dict[str, Any] = field(default_factory=dict)
