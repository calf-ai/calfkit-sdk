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

from calfkit.client import Client


def test_client_connect_captures_kafka_config() -> None:
    """Client.connect must record the bootstrap_servers it used so
    the worker can thread them forward to the aggregator's rehydration
    consumer (which doesn't share the FastStream broker's connection)."""
    with patch("calfkit.client.base.KafkaBroker"):
        client = Client.connect("kafka.example:9092", client_id="my-client")
    assert client.kafka_config is not None
    assert client.kafka_config.bootstrap_servers == "kafka.example:9092"
    assert client.kafka_config.client_kwargs.get("client_id") == "my-client"


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
    assert client.kafka_config.client_kwargs.get("security_protocol") == "SASL_SSL"
    assert client.kafka_config.client_kwargs.get("sasl_mechanism") == "SCRAM-SHA-256"
    assert client.kafka_config.client_kwargs.get("sasl_plain_username") == "alice"
    assert client.kafka_config.client_kwargs.get("sasl_plain_password") == "hunter2"


def test_client_connect_kafka_config_is_independent_of_broker_kwargs() -> None:
    """The captured client_kwargs dict must be a copy -- not a reference
    to the dict KafkaBroker received -- so neither side can mutate the
    other's view."""
    with patch("calfkit.client.base.KafkaBroker") as mock_broker_cls:
        client = Client.connect("kafka.example:9092", client_id="x")
    assert client.kafka_config is not None
    # Mutating the captured dict must not affect what was passed to KafkaBroker.
    client.kafka_config.client_kwargs["mutated"] = True
    broker_kwargs = mock_broker_cls.call_args.kwargs
    assert "mutated" not in broker_kwargs


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

    # Build the state store the way FanOutAggregator.setup would.
    broker = MagicMock()
    broker.publish = AsyncMock()
    store = _KafkaStateStore(
        broker=broker,
        state_topic="agent.fanout-state",
        bootstrap_servers=client.kafka_config.bootstrap_servers,
        partition_count=4,
        client_kwargs=client.kafka_config.client_kwargs,
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
