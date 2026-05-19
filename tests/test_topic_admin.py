"""Unit tests for ensure_aggregator_topics.

Mocks the FastStream broker's admin_client to verify topic creation /
validation logic without spinning up a real Kafka instance. Rebalance
+ partition tests for the state store land in the testcontainers
integration milestone.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from calfkit.nodes.aggregator._topic_admin import (
    STATE_TOPIC_CONFIG,
    ensure_aggregator_topics,
)
from calfkit.nodes.aggregator.errors import AggregatorStateStoreError


def _topic_metadata(name: str, partition_count: int) -> dict[str, Any]:
    """Mimic aiokafka.admin.describe_topics result shape."""
    return {
        "topic": name,
        "partitions": [{"partition": i} for i in range(partition_count)],
    }


def _make_admin(known_topics: dict[str, int]) -> AsyncMock:
    """Build a mock admin client.

    ``known_topics`` maps topic name → partition count for topics that
    already exist. Topics not in the map return as not-found.
    """
    admin = AsyncMock()

    async def describe_topics(topic_names: list[str]) -> list[dict[str, Any]]:
        return [
            _topic_metadata(name, known_topics[name])
            for name in topic_names
            if name in known_topics
        ]

    admin.describe_topics = AsyncMock(side_effect=describe_topics)
    admin.create_topics = AsyncMock()
    return admin


def _broker_with_admin(admin: AsyncMock) -> MagicMock:
    broker = MagicMock()
    broker.config = MagicMock()
    broker.config.admin_client = admin
    return broker


async def test_creates_both_topics_when_main_exists() -> None:
    admin = _make_admin({"agent.in": 8})
    broker = _broker_with_admin(admin)

    state_topic, returns_topic, partitions = await ensure_aggregator_topics(
        broker, node_id="agent", main_topic="agent.in"
    )

    assert state_topic == "agent.fanout-state"
    assert returns_topic == "agent.fanout-returns"
    assert partitions == 8
    # Two create_topics calls — one per topic
    assert admin.create_topics.await_count == 2


async def test_uses_default_partitions_when_main_missing() -> None:
    admin = _make_admin({})
    broker = _broker_with_admin(admin)

    _state, _returns, partitions = await ensure_aggregator_topics(
        broker,
        node_id="agent",
        main_topic="agent.in",
        default_partitions=4,
    )

    assert partitions == 4
    assert admin.create_topics.await_count == 2


async def test_state_topic_creation_uses_compact_config() -> None:
    """The state topic must be created with cleanup.policy=compact."""
    admin = _make_admin({"agent.in": 6})
    broker = _broker_with_admin(admin)

    await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")

    # Inspect the create_topics calls — first is the state topic.
    calls = admin.create_topics.await_args_list
    state_call_topics = calls[0].args[0]
    assert len(state_call_topics) == 1
    state_new_topic = state_call_topics[0]
    assert state_new_topic.name == "agent.fanout-state"
    assert state_new_topic.topic_configs == STATE_TOPIC_CONFIG

    returns_call_topics = calls[1].args[0]
    returns_new_topic = returns_call_topics[0]
    assert returns_new_topic.name == "agent.fanout-returns"
    # The returns topic must NOT be compacted (it's an event stream).
    # aiokafka normalizes None to {}, so check the meaningful property.
    assert "cleanup.policy" not in (returns_new_topic.topic_configs or {})


async def test_skips_existing_with_matching_partitions() -> None:
    admin = _make_admin({
        "agent.in": 6,
        "agent.fanout-state": 6,
        "agent.fanout-returns": 6,
    })
    broker = _broker_with_admin(admin)

    await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")

    # No create_topics calls — both already exist with matching partition count
    assert admin.create_topics.await_count == 0


async def test_raises_on_partition_count_mismatch() -> None:
    admin = _make_admin({
        "agent.in": 6,
        "agent.fanout-state": 3,  # wrong!
    })
    broker = _broker_with_admin(admin)

    with pytest.raises(AggregatorStateStoreError, match="partitions"):
        await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")


async def test_returns_correct_topic_names() -> None:
    """Topic names are derived from node_id with documented suffixes."""
    admin = _make_admin({})
    broker = _broker_with_admin(admin)

    state_topic, returns_topic, _ = await ensure_aggregator_topics(
        broker, node_id="my-special_agent", main_topic="my-special_agent.in"
    )

    assert state_topic == "my-special_agent.fanout-state"
    assert returns_topic == "my-special_agent.fanout-returns"


async def test_describe_topics_failure_falls_back_to_default() -> None:
    """If describe_topics raises (e.g., broker unreachable), default partitions
    are used and creation proceeds."""
    admin = AsyncMock()
    admin.describe_topics = AsyncMock(side_effect=Exception("broker unreachable"))
    admin.create_topics = AsyncMock()
    broker = _broker_with_admin(admin)

    _state, _returns, partitions = await ensure_aggregator_topics(
        broker,
        node_id="agent",
        main_topic="agent.in",
        default_partitions=6,
    )

    assert partitions == 6
    assert admin.create_topics.await_count == 2
