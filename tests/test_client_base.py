"""Tests for BaseClient input validation."""

from __future__ import annotations

import logging
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
    silently drop a state-topic write. Verify via the structured
    ``kwarg_name`` attribute instead of regex match — operators branch
    on the attribute programmatically."""
    with patch("calfkit.client.base.KafkaBroker"):
        with pytest.raises(DurabilityConfigError) as exc_info:
            Client.connect("kafka.example:9092", acks=1)
    assert exc_info.value.kwarg_name == "acks"
    assert exc_info.value.offending_value == 1


def test_connect_rejects_acks_zero() -> None:
    """acks=0 is fire-and-forget; no durability whatsoever."""
    with patch("calfkit.client.base.KafkaBroker"):
        with pytest.raises(DurabilityConfigError) as exc_info:
            Client.connect("kafka.example:9092", acks=0)
    assert exc_info.value.kwarg_name == "acks"
    assert exc_info.value.offending_value == 0


def test_connect_rejects_enable_idempotence_false() -> None:
    """Without idempotence, producer retries can produce duplicate state
    records that compaction cannot reliably deduplicate."""
    with patch("calfkit.client.base.KafkaBroker"):
        with pytest.raises(DurabilityConfigError) as exc_info:
            Client.connect("kafka.example:9092", enable_idempotence=False)
    assert exc_info.value.kwarg_name == "enable_idempotence"
    assert exc_info.value.offending_value is False


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


def test_connect_kafka_config_excludes_producer_kwargs_but_broker_gets_them() -> None:
    """``KafkaConfig`` is the consumer-side snapshot used to construct
    the rehydration ``AIOKafkaConsumer``; producer-only kwargs (acks,
    enable_idempotence) MUST be stripped from ``client_kwargs`` because
    ``AIOKafkaConsumer.__init__`` raises ``TypeError`` on them. They
    still reach the producer via ``KafkaBroker(**broker_kwargs)``."""
    with patch("calfkit.client.base.KafkaBroker") as mock_broker_cls:
        client = Client.connect("kafka.example:9092")
    assert client.kafka_config is not None
    # Producer-only kwargs are stripped from the consumer-side snapshot.
    assert "acks" not in client.kafka_config.client_kwargs
    assert "enable_idempotence" not in client.kafka_config.client_kwargs
    # But they DO reach KafkaBroker (the producer-side path).
    broker_kwargs = mock_broker_cls.call_args.kwargs
    assert broker_kwargs["acks"] == "all"
    assert broker_kwargs["enable_idempotence"] is True


def test_build_kafka_config_excludes_producer_kwargs_from_client_kwargs() -> None:
    """Producer-only kwargs the user explicitly passes (e.g.
    ``compression_type``) belong on the producer; ``KafkaConfig`` is
    the consumer-side snapshot. They must reach ``KafkaBroker`` intact
    but stay out of ``client_kwargs``."""
    with patch("calfkit.client.base.KafkaBroker") as mock_broker_cls:
        client = Client.connect(
            "kafka.example:9092",
            acks="all",
            compression_type="lz4",
        )
    assert client.kafka_config is not None
    assert "acks" not in client.kafka_config.client_kwargs
    assert "compression_type" not in client.kafka_config.client_kwargs
    broker_kwargs = mock_broker_cls.call_args.kwargs
    assert broker_kwargs["acks"] == "all"
    assert broker_kwargs["compression_type"] == "lz4"


def test_durability_config_error_carries_kwarg_attributes() -> None:
    """The structured ``kwarg_name`` / ``offending_value`` /
    ``expected_value`` attributes let operators branch on the failure
    programmatically instead of regex-matching error messages."""
    with patch("calfkit.client.base.KafkaBroker"):
        with pytest.raises(DurabilityConfigError) as exc_info:
            Client.connect("kafka.example:9092", acks=1)
    assert exc_info.value.kwarg_name == "acks"
    assert exc_info.value.offending_value == 1
    # Expected value is a docs string mentioning "all" or -1.
    assert exc_info.value.expected_value is not None
    expected_str = str(exc_info.value.expected_value)
    assert "all" in expected_str or "-1" in expected_str


def test_connect_logs_injected_acks_all(caplog: pytest.LogCaptureFixture) -> None:
    """When acks isn't supplied, Client.connect injects acks='all' for
    durability and surfaces an INFO log so the operator can audit the
    auto-injection (and suppress it by passing the kwarg explicitly)."""
    with patch("calfkit.client.base.KafkaBroker"):
        with caplog.at_level(logging.INFO, logger="calfkit.client.base"):
            Client.connect("kafka.example:9092")
    info_records = [r for r in caplog.records if r.levelno == logging.INFO and "injected broker_kwarg acks" in r.getMessage()]
    assert info_records, "expected an INFO log for the injected acks default"


def test_connect_logs_injected_enable_idempotence(caplog: pytest.LogCaptureFixture) -> None:
    """Same audit-trail INFO for the enable_idempotence default injection."""
    with patch("calfkit.client.base.KafkaBroker"):
        with caplog.at_level(logging.INFO, logger="calfkit.client.base"):
            Client.connect("kafka.example:9092")
    info_records = [r for r in caplog.records if r.levelno == logging.INFO and "injected broker_kwarg enable_idempotence" in r.getMessage()]
    assert info_records, "expected an INFO log for the injected enable_idempotence default"
