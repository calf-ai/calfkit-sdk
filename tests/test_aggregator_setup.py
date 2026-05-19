"""Tests for FanOutAggregator.setup() lifecycle and idempotency."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from calfkit.client.kafka_config import KafkaConfig
from calfkit.nodes.aggregator.aggregator import FanOutAggregator
from calfkit.nodes.aggregator.errors import AggregatorStateStoreError


async def test_setup_constructs_runtime_once() -> None:
    """FanOutAggregator.setup is idempotent — calling it twice does NOT
    re-allocate the state store or rebalance listener."""
    aggregator = FanOutAggregator()
    assert aggregator._runtime is None

    # Patch the heavy aiokafka-touching imports inside setup.
    with patch(
        "calfkit.nodes.aggregator._topic_admin.ensure_aggregator_topics",
        AsyncMock(return_value=("agent.fanout-state", "agent.fanout-returns", 4)),
    ):
        broker = MagicMock()
        config = KafkaConfig(bootstrap_servers="localhost:9092", client_kwargs={})

        await aggregator.setup(broker, node_id="agent", main_topic="agent.in", kafka_config=config)
        first_runtime = aggregator._runtime
        assert first_runtime is not None
        assert first_runtime.state_topic == "agent.fanout-state"
        assert first_runtime.partition_count == 4

        # Second call: idempotent, runtime unchanged.
        await aggregator.setup(broker, node_id="agent", main_topic="agent.in", kafka_config=config)
        assert aggregator._runtime is first_runtime  # SAME object, not re-allocated


def test_runtime_property_raises_before_setup() -> None:
    """Accessing aggregator.runtime before setup() raises a typed
    AggregatorStateStoreError so callers can distinguish "uninitialised"
    from "broker unhealthy" / "config mismatch"."""
    aggregator = FanOutAggregator()
    with pytest.raises(AggregatorStateStoreError, match="setup()"):
        _ = aggregator.runtime
