"""Tests for KafkaConfig threading from Client.connect to the aggregator.

Client.connect must snapshot the bootstrap servers + client kwargs it
passed to KafkaBroker so the Worker can forward them to the fan-out
aggregator's state store. The state store's transient
AIOKafkaConsumer (used only during state-topic rehydration) does not
share the FastStream broker's connection -- without this snapshot it
fails on any production cluster with SASL/SSL auth.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from calfkit.client import Client
from calfkit.client.kafka_config import (
    REHYDRATE_REBALANCE_TIMEOUT_FLOOR_MS,
    KafkaConfig,
)
from calfkit.exceptions import DurabilityConfigError


def test_client_connect_captures_kafka_config() -> None:
    """Client.connect must record the bootstrap_servers it used so
    the worker can thread them forward to the aggregator's rehydration
    consumer (which doesn't share the FastStream broker's connection)."""
    with patch("calfkit.client.base.KafkaBroker"):
        client = Client.connect("kafka.example:9092", client_id="my-client")
    assert client.kafka_config is not None
    assert client.kafka_config.bootstrap_servers == "kafka.example:9092"
    # client_id is a typed field; it must NOT also appear in client_kwargs
    # (the typed-field extraction in Client.connect moves it out).
    assert client.kafka_config.client_id == "my-client"
    assert "client_id" not in client.kafka_config.client_kwargs


def test_client_connect_captures_security_kwargs() -> None:
    """SASL/SSL kwargs the user passes must survive into kafka_config so
    rehydration uses the same auth as the broker."""
    with patch("calfkit.client.base.KafkaBroker"):
        client = Client.connect(
            "kafka.example:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username="alice",
            sasl_plain_password="hunter2",
        )
    assert client.kafka_config is not None
    assert client.kafka_config.security_protocol == "SASL_SSL"
    assert client.kafka_config.sasl_mechanism == "SCRAM-SHA-256"
    assert client.kafka_config.sasl_plain_username == "alice"
    assert client.kafka_config.sasl_plain_password == "hunter2"


def test_client_connect_kafka_config_is_independent_of_broker_kwargs() -> None:
    """The captured client_kwargs dict must be a copy -- not a reference
    to the dict KafkaBroker received -- so neither side can mutate the
    other's view. KafkaConfig also wraps the snapshot in MappingProxyType,
    so any mutation attempt raises TypeError (verified separately)."""
    with patch("calfkit.client.base.KafkaBroker") as mock_broker_cls:
        # Pass a non-typed kwarg so client_kwargs is non-empty; the
        # snapshot's contents must not leak back into KafkaBroker's
        # kwargs even if MappingProxyType were swapped out.
        client = Client.connect("kafka.example:9092", request_timeout_ms=5000)
    assert client.kafka_config is not None
    # The snapshot is now immutable (MappingProxyType); attempting to
    # mutate it raises TypeError. Verify that AND that the captured
    # kwarg made it into KafkaBroker.
    with pytest.raises(TypeError):
        client.kafka_config.client_kwargs["mutated"] = True  # type: ignore[index]
    broker_kwargs = mock_broker_cls.call_args.kwargs
    assert broker_kwargs.get("request_timeout_ms") == 5000


async def test_security_kwargs_reach_rehydration_consumer_end_to_end() -> None:
    """End-to-end thread-through: user-supplied security_protocol /
    sasl_* kwargs from Client.connect must reach the
    AIOKafkaConsumer the state store constructs during rehydration.
    Pure unit coverage of the dataclass capture (test_*_captures_*)
    wouldn't catch a regression that broke the path between
    ``KafkaConfig`` and ``_KafkaStateStore.__init__``."""
    from unittest.mock import AsyncMock, MagicMock

    from aiokafka import TopicPartition

    from calfkit.nodes.aggregator._kafka_state_store import _KafkaStateStore

    with patch("calfkit.client.base.KafkaBroker"):
        client = Client.connect(
            "kafka.example:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username="alice",
            sasl_plain_password="hunter2",
        )

    assert client.kafka_config is not None

    # Build the state store the way FanOutAggregator.setup would: use
    # ``to_consumer_kwargs`` to merge typed fields + client_kwargs into
    # the dict the rehydration consumer needs.
    broker = MagicMock()
    broker.publish = AsyncMock()
    consumer_kwargs = client.kafka_config.to_consumer_kwargs()
    bootstrap = consumer_kwargs.pop("bootstrap_servers")
    store = _KafkaStateStore(
        broker=broker,
        state_topic="agent.fanout-state",
        bootstrap_servers=bootstrap,
        partition_count=4,
        client_kwargs=consumer_kwargs,
    )

    # Trigger rehydration with a no-op partition so AIOKafkaConsumer is
    # constructed but doesn't actually poll.
    mock_consumer = AsyncMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.assign = MagicMock()
    mock_consumer.seek_to_beginning = AsyncMock()
    mock_consumer.end_offsets = AsyncMock(return_value={TopicPartition("agent.fanout-state", 0): 0})
    mock_consumer.getmany = AsyncMock(return_value={})

    with patch(
        "calfkit.nodes.aggregator._kafka_state_store.AIOKafkaConsumer",
        return_value=mock_consumer,
    ) as mock_cls:
        await store.rehydrate_partitions({0})

    construction_kwargs = mock_cls.call_args.kwargs
    assert construction_kwargs["bootstrap_servers"] == "kafka.example:9092"
    assert construction_kwargs["security_protocol"] == "SASL_SSL"
    assert construction_kwargs["sasl_mechanism"] == "SCRAM-SHA-256"
    assert construction_kwargs["sasl_plain_username"] == "alice"
    assert construction_kwargs["sasl_plain_password"] == "hunter2"


# ----------------------------------------------------------------------
# to_consumer_kwargs() coverage
# ----------------------------------------------------------------------


def test_to_consumer_kwargs_with_only_bootstrap_servers() -> None:
    """A minimal KafkaConfig must serialise to just bootstrap_servers; no
    ``None`` placeholders for unset typed fields, which would otherwise
    override aiokafka defaults."""
    config = KafkaConfig(bootstrap_servers="broker:9092")
    assert config.to_consumer_kwargs() == {"bootstrap_servers": "broker:9092"}


def test_to_consumer_kwargs_with_typed_fields() -> None:
    """All typed fields must surface in to_consumer_kwargs so the
    rehydration consumer inherits the broker's full auth config."""
    config = KafkaConfig(
        bootstrap_servers="broker:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username="alice",
        sasl_plain_password="hunter2",
        client_id="aggregator-rehydrate",
    )
    result = config.to_consumer_kwargs()
    assert result["bootstrap_servers"] == "broker:9092"
    assert result["security_protocol"] == "SASL_SSL"
    assert result["sasl_mechanism"] == "SCRAM-SHA-256"
    assert result["sasl_plain_username"] == "alice"
    assert result["sasl_plain_password"] == "hunter2"
    assert result["client_id"] == "aggregator-rehydrate"


def test_kafka_config_typed_field_collision_raises() -> None:
    """Typed fields and client_kwargs cannot overlap: such a collision
    means the operator set the same kwarg twice, which is ambiguous
    enough to be a misconfiguration rather than a precedence question.
    Raising at construction surfaces the bug at Client.connect (the
    user's call site) rather than at rehydration."""
    with pytest.raises(DurabilityConfigError) as exc_info:
        KafkaConfig(
            bootstrap_servers="broker:9092",
            security_protocol="SASL_SSL",
            client_kwargs={"security_protocol": "PLAINTEXT"},
        )
    assert exc_info.value.kwarg_name == "security_protocol"
    assert exc_info.value.offending_value == "PLAINTEXT"
    assert exc_info.value.expected_value == "SASL_SSL"


def test_to_consumer_kwargs_strips_producer_only_kwargs() -> None:
    """Defense in depth: even if a caller constructs KafkaConfig
    directly with producer-only kwargs in client_kwargs (bypassing the
    Client.connect partitioning), to_consumer_kwargs() must strip them.
    AIOKafkaConsumer.__init__ rejects producer kwargs with TypeError;
    surfacing that on a deployed worker would be catastrophic."""
    config = KafkaConfig(
        bootstrap_servers="broker:9092",
        client_kwargs={
            "acks": "all",
            "enable_idempotence": True,
            "linger_ms": 5,
            "request_timeout_ms": 5000,
        },
    )
    result = config.to_consumer_kwargs()
    assert "acks" not in result
    assert "enable_idempotence" not in result
    assert "linger_ms" not in result
    # Non-producer kwargs and bootstrap_servers survive intact.
    assert result["request_timeout_ms"] == 5000
    assert result["bootstrap_servers"] == "broker:9092"


def test_client_kwargs_is_immutable_after_construction() -> None:
    """KafkaConfig wraps client_kwargs in MappingProxyType so the
    snapshot cannot be mutated post-construction. A plain dict would
    still expose ``cfg.client_kwargs["k"] = v`` even though the frozen
    dataclass blocks attribute reassignment."""
    cfg = KafkaConfig(bootstrap_servers="broker:9092", client_kwargs={"a": 1})
    with pytest.raises(TypeError):
        cfg.client_kwargs["b"] = 2  # type: ignore[index]


def test_to_consumer_kwargs_excludes_none() -> None:
    """Unset typed fields default to None; they must not leak into the
    output, otherwise aiokafka would receive ``security_protocol=None``
    and reject it (or worse, accept and disable a default)."""
    config = KafkaConfig(
        bootstrap_servers="broker:9092",
        security_protocol=None,
        sasl_mechanism=None,
    )
    result = config.to_consumer_kwargs()
    for value in result.values():
        assert value is not None


def test_to_consumer_kwargs_preserves_extra_client_kwargs() -> None:
    """The escape hatch must survive intact: non-typed kwargs the user
    passes through ``client_kwargs`` (e.g., aiokafka tunables that
    aren't promoted to typed fields) must appear in the output."""
    config = KafkaConfig(
        bootstrap_servers="broker:9092",
        client_kwargs={"request_timeout_ms": 5000, "rebalance_timeout_ms": 600_000},
    )
    result = config.to_consumer_kwargs()
    assert result["request_timeout_ms"] == 5000
    assert result["rebalance_timeout_ms"] == 600_000


# ----------------------------------------------------------------------
# assert_rehydration_timeout_ok() coverage
# ----------------------------------------------------------------------


def test_assert_rehydration_timeout_ok_raises_when_below_floor() -> None:
    """If the worker can't finish rehydration before rebalance_timeout_ms
    expires, the broker triggers another rebalance — the rebalance-storm
    risk is severe enough to fail fast at startup. The exception carries
    structured ``kwarg_name``/``offending_value`` so operators can branch
    programmatically."""
    config = KafkaConfig(
        bootstrap_servers="broker:9092",
        client_kwargs={"rebalance_timeout_ms": 30_000},
    )
    with pytest.raises(DurabilityConfigError) as exc_info:
        config.assert_rehydration_timeout_ok()
    assert exc_info.value.kwarg_name == "rebalance_timeout_ms"
    assert exc_info.value.offending_value == 30_000
    assert isinstance(exc_info.value.expected_value, str)
    assert str(REHYDRATE_REBALANCE_TIMEOUT_FLOOR_MS) in exc_info.value.expected_value


def test_assert_rehydration_timeout_ok_silent_at_floor() -> None:
    """At exactly the floor, no error should fire — the floor is
    inclusive on the safe side."""
    config = KafkaConfig(
        bootstrap_servers="broker:9092",
        client_kwargs={"rebalance_timeout_ms": REHYDRATE_REBALANCE_TIMEOUT_FLOOR_MS},
    )
    # No exception expected.
    config.assert_rehydration_timeout_ok()


def test_assert_rehydration_timeout_ok_silent_above_floor() -> None:
    """Above the floor is the recommended config; must not raise."""
    config = KafkaConfig(
        bootstrap_servers="broker:9092",
        client_kwargs={"rebalance_timeout_ms": REHYDRATE_REBALANCE_TIMEOUT_FLOOR_MS + 60_000},
    )
    # No exception expected.
    config.assert_rehydration_timeout_ok()
