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
