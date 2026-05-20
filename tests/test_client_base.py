"""Tests for BaseClient input validation."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from calfkit.client import Client
from calfkit.exceptions import DurabilityConfigError
from calfkit.models.state import State


async def test_invoke_rejects_correlation_id_with_pipe_delimiter() -> None:
    """correlation_id containing '|' would corrupt the fan-out aggregator's
    composite-key partitioning. Validate at the SDK boundary."""
    client = Client.connect("localhost:9092")
    try:
        with pytest.raises(ValueError, match=r"correlation_id"):
            await client._invoke(
                topic="some.topic",
                reply_topic=client.reply_topic,
                correlation_id="bad|id",
                state=State(),
            )
    finally:
        # close() awaits dispatcher cleanup; broker was never started so
        # broker.stop() is effectively a no-op.
        await client.close()


# ----------------------------------------------------------------------
# Durability enforcement at Client.connect (Issue 7)
#
# State-topic writes share the FastStream producer; a leader outage
# between ack and replica catch-up would silently drop a state-topic
# write without acks="all". Validate at Client.connect — the single
# choke point — rather than relying on every aggregator setup site to
# re-check.
# ----------------------------------------------------------------------


def test_connect_rejects_acks_one() -> None:
    """acks=1 acknowledges as soon as the leader writes locally, before
    replication. A leader crash between ack and replica catch-up would
    silently drop a state-topic write."""
    with patch("calfkit.client.base.KafkaBroker"):
        with pytest.raises(DurabilityConfigError, match="acks"):
            Client.connect("kafka.example:9092", acks=1)


def test_connect_rejects_acks_zero() -> None:
    """acks=0 is fire-and-forget; no durability whatsoever."""
    with patch("calfkit.client.base.KafkaBroker"):
        with pytest.raises(DurabilityConfigError, match="acks"):
            Client.connect("kafka.example:9092", acks=0)


def test_connect_rejects_enable_idempotence_false() -> None:
    """Without idempotence, producer retries can produce duplicate state
    records that compaction cannot reliably deduplicate."""
    with patch("calfkit.client.base.KafkaBroker"):
        with pytest.raises(DurabilityConfigError, match="enable_idempotence"):
            Client.connect("kafka.example:9092", enable_idempotence=False)


def test_connect_injects_acks_all_when_missing() -> None:
    """The default Client.connect must produce a durability-safe broker
    even when the user provides no producer kwargs."""
    with patch("calfkit.client.base.KafkaBroker") as mock_broker_cls:
        Client.connect("kafka.example:9092")
    kwargs = mock_broker_cls.call_args.kwargs
    assert kwargs["acks"] == "all"


def test_connect_injects_enable_idempotence_when_missing() -> None:
    """Idempotence must be on by default; a missed validation is silent
    data loss, so default to safe."""
    with patch("calfkit.client.base.KafkaBroker") as mock_broker_cls:
        Client.connect("kafka.example:9092")
    kwargs = mock_broker_cls.call_args.kwargs
    assert kwargs["enable_idempotence"] is True


def test_connect_accepts_explicit_acks_all() -> None:
    """Explicit acks='all' + enable_idempotence=True must succeed without
    spurious error."""
    with patch("calfkit.client.base.KafkaBroker") as mock_broker_cls:
        Client.connect("kafka.example:9092", acks="all", enable_idempotence=True)
    kwargs = mock_broker_cls.call_args.kwargs
    assert kwargs["acks"] == "all"
    assert kwargs["enable_idempotence"] is True


def test_connect_accepts_acks_minus_one() -> None:
    """Kafka accepts -1 as a synonym for 'all'; the validator must not
    reject it."""
    with patch("calfkit.client.base.KafkaBroker") as mock_broker_cls:
        Client.connect("kafka.example:9092", acks=-1)
    kwargs = mock_broker_cls.call_args.kwargs
    # Validator leaves the original value intact (no normalisation) — the
    # caller's -1 means the same as "all" to Kafka.
    assert kwargs["acks"] == -1


def test_connect_kafka_config_reflects_injected_durability() -> None:
    """The KafkaConfig snapshot must reflect what KafkaBroker actually
    received, including auto-injected acks="all". Without this, the
    rehydration consumer could see a different view than the producer
    and the durability contract would be split across two configs."""
    with patch("calfkit.client.base.KafkaBroker"):
        client = Client.connect("kafka.example:9092")
    assert client.kafka_config is not None
    # acks lives in client_kwargs (not a typed field on KafkaConfig).
    assert client.kafka_config.client_kwargs.get("acks") == "all"
    assert client.kafka_config.client_kwargs.get("enable_idempotence") is True
