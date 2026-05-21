"""Tests for FanOutAggregator.setup() lifecycle and idempotency."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from calfkit.client.kafka_config import KafkaConfig
from calfkit.nodes.aggregator._topic_admin import ensure_aggregator_topics
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


# ---------------------------------------------------------------------------
# ensure_aggregator_topics must also provision the agent's _return_topic
# (``{node_id}.private.return``) so the aggregator's merged-completion
# re-entry publish has a co-partitioned target under
# auto.create.topics.enable=false. See PR #142 / issue #141 for the
# co-tenant leak rationale that made _return_topic load-bearing.
# ---------------------------------------------------------------------------


def _topic_metadata(name: str, partition_count: int) -> dict[str, Any]:
    """Mimic aiokafka.admin.describe_topics result shape."""
    return {
        "topic": name,
        "partitions": [{"partition": i} for i in range(partition_count)],
    }


def _make_admin_with_only_main(main_topic: str, partition_count: int) -> AsyncMock:
    """Build an admin mock where only ``main_topic`` exists, so the aggregator
    provisions every per-agent topic from scratch."""
    admin = AsyncMock()

    async def describe_topics(topic_names: list[str]) -> list[dict[str, Any]]:
        return [_topic_metadata(name, partition_count) for name in topic_names if name == main_topic]

    admin.describe_topics = AsyncMock(side_effect=describe_topics)
    admin.create_topics = AsyncMock()
    return admin


def _broker_with_admin(admin: AsyncMock) -> MagicMock:
    broker = MagicMock()
    broker.config = MagicMock()
    broker.config.admin_client = admin
    return broker


async def test_ensure_aggregator_topics_also_provisions_return_topic() -> None:
    """The aggregator's merged-completion publish targets ``_return_topic``
    (``{node_id}.private.return``) after PR #142 / issue #141 closed the
    co-tenant tool-return leak. Under ``auto.create.topics.enable=false``,
    failing to provision this topic at worker startup would cause the
    first fan-out completion to fail with ``UnknownTopicOrPartitionError``.
    Worse, auto-creation could silently land it with the broker-default
    partition count, breaking co-partitioning with the agent's keyed
    traffic.
    """
    admin = _make_admin_with_only_main("agent.in", partition_count=8)
    broker = _broker_with_admin(admin)

    await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")

    created_names = {call.args[0][0].name for call in admin.create_topics.await_args_list}
    assert "agent.private.return" in created_names, "_return_topic must be provisioned alongside the aggregator's state/returns topics"


async def test_return_topic_is_co_partitioned_with_main() -> None:
    """``_return_topic`` must inherit the main topic's partition count so the
    agent's keyed traffic remains co-partitioned end-to-end. A mismatch
    here would silently break ordering guarantees for tool returns +
    aggregator completions that target the same agent instance."""
    admin = _make_admin_with_only_main("agent.in", partition_count=12)
    broker = _broker_with_admin(admin)

    await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")

    return_topic_call = next(call for call in admin.create_topics.await_args_list if call.args[0][0].name == "agent.private.return")
    return_new_topic = return_topic_call.args[0][0]
    assert return_new_topic.num_partitions == 12, "_return_topic must be co-partitioned with the main topic"


async def test_return_topic_is_not_compacted() -> None:
    """``_return_topic`` carries one-shot tool-return and aggregator completion
    envelopes (not last-write-wins state), so it must be created as a
    regular topic without ``cleanup.policy=compact``. Compaction here would
    silently drop tool returns whose key (correlation_id) matches a
    previously seen one."""
    admin = _make_admin_with_only_main("agent.in", partition_count=6)
    broker = _broker_with_admin(admin)

    await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")

    return_topic_call = next(call for call in admin.create_topics.await_args_list if call.args[0][0].name == "agent.private.return")
    return_new_topic = return_topic_call.args[0][0]
    # aiokafka normalizes None to {}, so check the meaningful property.
    assert "cleanup.policy" not in (return_new_topic.topic_configs or {})


async def test_return_topic_provisioned_for_arbitrary_node_id() -> None:
    """Topic name derives from ``node_id`` with the documented
    ``.private.return`` suffix; PR #142 made this name framework-private,
    so a regression that hard-coded ``agent.private.return`` would silently
    miss every non-``agent`` node."""
    admin = _make_admin_with_only_main("my-special_agent.in", partition_count=4)
    broker = _broker_with_admin(admin)

    await ensure_aggregator_topics(broker, node_id="my-special_agent", main_topic="my-special_agent.in")

    created_names = {call.args[0][0].name for call in admin.create_topics.await_args_list}
    assert "my-special_agent.private.return" in created_names
