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


def test_agent_ensure_aggregator_ready_raises_when_runtime_missing() -> None:
    """``_ensure_aggregator_ready`` is the production hot-path guard
    fired from both ``_publish_action`` (parallel-fan-out branch) and
    ``_aggregator_handler``. It must NOT synthesise any test-fixture
    KafkaConfig — that lives in the ``aggregator.testing`` helper.
    Production code reaching here with ``_runtime is None`` is a bug
    (Worker.run() should have set up); the raise surfaces it."""
    from calfkit._vendor.pydantic_ai.models.function import FunctionModel
    from calfkit.nodes.agent import BaseAgentNodeDef

    agent = BaseAgentNodeDef(
        node_id="test_strict",
        subscribe_topics="test_strict.input",
        model_client=FunctionModel(lambda messages, info: None),  # type: ignore[arg-type]
    )
    assert agent.aggregator._runtime is None

    with pytest.raises(AggregatorStateStoreError, match="setup_for_tests"):
        agent._ensure_aggregator_ready()


async def test_setup_for_tests_helper_initialises_aggregator_runtime() -> None:
    """The ``aggregator/testing.py`` helper is the canonical way to set up
    an agent's aggregator runtime in tests that bypass Worker.run().
    Must be idempotent (matching ``FanOutAggregator.setup``) so calling
    it twice in a fixture is safe, and must unblock the strict
    ``_ensure_aggregator_ready`` guard once it runs."""
    from calfkit._vendor.pydantic_ai.models.function import FunctionModel
    from calfkit.nodes.agent import BaseAgentNodeDef
    from calfkit.nodes.aggregator.testing import setup_for_tests

    agent = BaseAgentNodeDef(
        node_id="test_helper",
        subscribe_topics="test_helper.input",
        model_client=FunctionModel(lambda messages, info: None),  # type: ignore[arg-type]
    )
    broker = MagicMock()
    broker.connect = AsyncMock()

    # Patch the broker-touching part of setup so this test runs without
    # a live Kafka.
    with patch(
        "calfkit.nodes.aggregator._topic_admin.ensure_aggregator_topics",
        AsyncMock(return_value=("test_helper.fanout-state", "test_helper.fanout-returns", 4)),
    ):
        await setup_for_tests(agent, broker)
        assert agent.aggregator._runtime is not None
        first_runtime = agent.aggregator._runtime

        # Idempotent: second call is a no-op, same runtime.
        await setup_for_tests(agent, broker)
        assert agent.aggregator._runtime is first_runtime

    # The handler-guard no longer raises now that setup ran.
    agent._ensure_aggregator_ready()  # should not raise
