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
from aiokafka.errors import (  # type: ignore[import-untyped]
    TopicAlreadyExistsError,
    UnknownTopicOrPartitionError,
)

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


def _describe_configs_response(topic: str, entries: dict[str, str]) -> Any:
    """Build a mock DescribeConfigsResponse with the given topic config entries.

    Mirrors the aiokafka response shape: each response has a ``resources`` list
    of tuples ``(error_code, error_message, resource_type, resource_name,
    config_entries)``; each ``config_entries`` row is itself a tuple whose
    first element is the config key name and second is the value.
    """
    config_entries = [(name, value, False, False, False) for name, value in entries.items()]
    resource = (0, "", 2, topic, config_entries)
    return MagicMock(resources=[resource])


def _make_admin(
    known_topics: dict[str, int],
    *,
    topic_configs: dict[str, dict[str, str]] | None = None,
) -> AsyncMock:
    """Build a mock admin client.

    ``known_topics`` maps topic name → partition count for topics that
    already exist. Topics not in the map return as not-found.

    ``topic_configs`` optionally maps topic name → config dict for the
    ``describe_configs`` response. When omitted, existing state topics
    are reported with the default ``STATE_TOPIC_CONFIG`` so existing
    tests don't need explicit configs.
    """
    admin = AsyncMock()
    effective_configs: dict[str, dict[str, str]] = {}
    if topic_configs is not None:
        effective_configs.update(topic_configs)

    async def describe_topics(topic_names: list[str]) -> list[dict[str, Any]]:
        return [_topic_metadata(name, known_topics[name]) for name in topic_names if name in known_topics]

    async def describe_configs(config_resources: list[Any]) -> list[Any]:
        responses: list[Any] = []
        for resource in config_resources:
            topic = resource.name
            cfg = effective_configs.get(topic, dict(STATE_TOPIC_CONFIG))
            responses.append(_describe_configs_response(topic, cfg))
        return responses

    admin.describe_topics = AsyncMock(side_effect=describe_topics)
    admin.describe_configs = AsyncMock(side_effect=describe_configs)
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

    state_topic, returns_topic, partitions = await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")

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


def test_state_topic_config_includes_segment_ms_and_dirty_ratio() -> None:
    """The state topic's default config must include ``segment.ms=3600000``
    (1 hour) and ``min.cleanable.dirty.ratio=0.1``. The Kafka compactor
    only operates on closed segments, so leaving segment.ms at the default
    7 days would let tombstones for completed batches linger on disk for
    up to a week before becoming eligible for compaction. Lowering the
    dirty-ratio threshold keeps the compactor eager about reclaiming
    space on this write-heavy, tombstone-heavy state topic."""
    assert STATE_TOPIC_CONFIG["segment.ms"] == "3600000"
    assert STATE_TOPIC_CONFIG["min.cleanable.dirty.ratio"] == "0.1"


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
    admin = _make_admin(
        {
            "agent.in": 6,
            "agent.fanout-state": 6,
            "agent.fanout-returns": 6,
        }
    )
    broker = _broker_with_admin(admin)

    await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")

    # No create_topics calls — both already exist with matching partition count
    assert admin.create_topics.await_count == 0


async def test_raises_on_partition_count_mismatch() -> None:
    admin = _make_admin(
        {
            "agent.in": 6,
            "agent.fanout-state": 3,  # wrong!
        }
    )
    broker = _broker_with_admin(admin)

    with pytest.raises(AggregatorStateStoreError, match="partitions"):
        await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")


async def test_returns_correct_topic_names() -> None:
    """Topic names are derived from node_id with documented suffixes."""
    admin = _make_admin({})
    broker = _broker_with_admin(admin)

    state_topic, returns_topic, _ = await ensure_aggregator_topics(broker, node_id="my-special_agent", main_topic="my-special_agent.in")

    assert state_topic == "my-special_agent.fanout-state"
    assert returns_topic == "my-special_agent.fanout-returns"


async def test_resolve_partition_count_raises_on_broker_down() -> None:
    """A non-UnknownTopic exception (e.g., broker unreachable) must surface as
    AggregatorStateStoreError rather than silently degrading to defaults."""
    admin = AsyncMock()
    admin.describe_topics = AsyncMock(side_effect=Exception("connection refused"))
    admin.create_topics = AsyncMock()
    broker = _broker_with_admin(admin)

    with pytest.raises(AggregatorStateStoreError, match="failed to describe main topic"):
        await ensure_aggregator_topics(
            broker,
            node_id="agent",
            main_topic="agent.in",
            default_partitions=6,
        )

    # No topics should have been created — startup must fail fast.
    assert admin.create_topics.await_count == 0


async def test_resolve_partition_count_falls_back_on_unknown_topic(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When the main topic legitimately doesn't exist (UnknownTopicOrPartitionError),
    fall back to default_partitions and emit a WARN."""
    admin = AsyncMock()
    admin.describe_topics = AsyncMock(side_effect=UnknownTopicOrPartitionError())
    admin.create_topics = AsyncMock()
    broker = _broker_with_admin(admin)

    with caplog.at_level("WARNING", logger="calfkit.nodes.aggregator._topic_admin"):
        _state, _returns, partitions = await ensure_aggregator_topics(
            broker,
            node_id="agent",
            main_topic="agent.in",
            default_partitions=4,
        )

    assert partitions == 4
    assert admin.create_topics.await_count == 2
    # Confirm the WARN about defaulting to the fallback partition count fired.
    assert any("agent.in" in record.message and "defaulting" in record.message for record in caplog.records if record.levelname == "WARNING")


async def test_create_topic_swallows_topic_already_exists() -> None:
    """If a peer worker creates the topic concurrently (TopicAlreadyExistsError),
    re-describe the topic and accept when the partition count matches.
    """
    admin = AsyncMock()

    # describe_topics is called five times when both topics race-create:
    #   1. resolve main topic partition count -> 8
    #   2. _ensure_topic(state) pre-create describe -> not found
    #   3. _ensure_topic(state) post-race re-describe -> found w/ 8 partitions
    #   4. _ensure_topic(returns) pre-create describe -> not found
    #   5. _ensure_topic(returns) post-race re-describe -> found w/ 8 partitions
    describe_results: list[list[dict[str, Any]]] = [
        [_topic_metadata("agent.in", 8)],
        [],
        [_topic_metadata("agent.fanout-state", 8)],
        [],
        [_topic_metadata("agent.fanout-returns", 8)],
    ]
    describe_call_count = 0

    async def describe_topics(_topic_names: list[str]) -> list[dict[str, Any]]:
        nonlocal describe_call_count
        result = describe_results[describe_call_count]
        describe_call_count += 1
        return result

    admin.describe_topics = AsyncMock(side_effect=describe_topics)
    # Both create_topics calls raise — simulating the race on both topics.
    admin.create_topics = AsyncMock(side_effect=TopicAlreadyExistsError())

    broker = _broker_with_admin(admin)

    state_topic, returns_topic, partitions = await ensure_aggregator_topics(
        broker,
        node_id="agent",
        main_topic="agent.in",
    )

    assert state_topic == "agent.fanout-state"
    assert returns_topic == "agent.fanout-returns"
    assert partitions == 8
    # Both create attempts ran (race on each topic) and both were swallowed.
    assert admin.create_topics.await_count == 2


async def test_resolve_partition_count_raises_on_authorization_failure() -> None:
    """When describe_topics returns ``error_code=29`` (TOPIC_AUTHORIZATION_FAILED)
    for the main topic, ``_resolve_partition_count`` must raise rather than
    silently fall back to default partitions. Falling back would silently
    break co-partitioning when the real cause is missing broker ACLs — the
    aggregator's state and returns topics would be created with the wrong
    partition count and the agent would be undeliverable.
    """
    admin = AsyncMock()

    async def describe_topics(_topic_names: list[str]) -> list[dict[str, Any]]:
        meta = _topic_metadata("agent.in", 12)
        meta["error_code"] = 29  # TOPIC_AUTHORIZATION_FAILED
        return [meta]

    admin.describe_topics = AsyncMock(side_effect=describe_topics)
    admin.create_topics = AsyncMock()
    broker = _broker_with_admin(admin)

    with pytest.raises(AggregatorStateStoreError) as exc_info:
        await ensure_aggregator_topics(
            broker,
            node_id="agent",
            main_topic="agent.in",
            default_partitions=4,
        )

    message = str(exc_info.value)
    assert "TOPIC_AUTHORIZATION_FAILED" in message
    assert "agent.in" in message
    # No topics should have been created — startup must fail fast.
    assert admin.create_topics.await_count == 0


async def test_resolve_partition_count_falls_back_only_on_unknown_topic() -> None:
    """When the broker reports ``error_code=3`` (UNKNOWN_TOPIC_OR_PARTITION)
    on the per-topic description, the function MUST fall back to default
    partitions — that's the legitimate "topic not provisioned yet" path
    where defaulting is the right behaviour. This is the regression-safe
    counterpart to the auth-failure test above.
    """
    from aiokafka.errors import UnknownTopicOrPartitionError  # type: ignore[import-untyped]

    admin = AsyncMock()

    async def describe_topics(_topic_names: list[str]) -> list[dict[str, Any]]:
        meta = _topic_metadata("agent.in", 0)
        meta["error_code"] = UnknownTopicOrPartitionError.errno
        return [meta]

    admin.describe_topics = AsyncMock(side_effect=describe_topics)
    admin.create_topics = AsyncMock()
    broker = _broker_with_admin(admin)

    _state, _returns, partitions = await ensure_aggregator_topics(
        broker,
        node_id="agent",
        main_topic="agent.in",
        default_partitions=4,
    )

    assert partitions == 4
    # Both aggregator topics are created at the default partition count.
    assert admin.create_topics.await_count == 2


async def test_resolve_partition_count_accepts_topic_with_error_code_zero() -> None:
    """A topic explicitly reporting error_code=0 must be accepted as valid."""
    admin = AsyncMock()

    async def describe_topics(_topic_names: list[str]) -> list[dict[str, Any]]:
        meta = _topic_metadata("agent.in", 10)
        meta["error_code"] = 0
        return [meta]

    admin.describe_topics = AsyncMock(side_effect=describe_topics)
    admin.create_topics = AsyncMock()
    broker = _broker_with_admin(admin)

    _state, _returns, partitions = await ensure_aggregator_topics(
        broker,
        node_id="agent",
        main_topic="agent.in",
        default_partitions=6,
    )

    assert partitions == 10


async def test_try_describe_raises_on_authorization_failure() -> None:
    """``_try_describe`` (used during the existence check) must raise when
    the broker returns a non-not-found error code such as
    TOPIC_AUTHORIZATION_FAILED. Falling through to the create-topic path
    would just trigger a misleading create-failure stack trace — operators
    should see the actual auth error instead, with the resolved label so
    they can grep Kafka docs for the remediation."""
    admin = AsyncMock()

    call_count = 0

    async def describe_topics(topic_names: list[str]) -> list[dict[str, Any]]:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # main topic resolution succeeds.
            return [_topic_metadata(topic_names[0], 6)]
        # _ensure_topic existence check — broker returns an auth failure.
        meta = _topic_metadata(topic_names[0], 6)
        meta["error_code"] = 29  # TOPIC_AUTHORIZATION_FAILED
        return [meta]

    admin.describe_topics = AsyncMock(side_effect=describe_topics)
    admin.create_topics = AsyncMock()
    broker = _broker_with_admin(admin)

    with pytest.raises(AggregatorStateStoreError) as exc_info:
        await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")

    message = str(exc_info.value)
    assert "TOPIC_AUTHORIZATION_FAILED" in message
    # No create_topics call — the existence check must surface the auth error
    # before any provisioning is attempted.
    assert admin.create_topics.await_count == 0


async def test_try_describe_falls_back_only_on_unknown_topic() -> None:
    """``_try_describe`` must continue treating ``UnknownTopicOrPartitionError``
    (errno=3) as "topic doesn't exist yet" so the create-topic path runs as
    designed — the auth-failure raise must not regress the legitimate
    not-yet-provisioned path."""
    from aiokafka.errors import UnknownTopicOrPartitionError  # type: ignore[import-untyped]

    admin = AsyncMock()

    call_count = 0

    async def describe_topics(topic_names: list[str]) -> list[dict[str, Any]]:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # main topic resolution succeeds.
            return [_topic_metadata(topic_names[0], 6)]
        meta = _topic_metadata(topic_names[0], 6)
        meta["error_code"] = UnknownTopicOrPartitionError.errno
        return [meta]

    admin.describe_topics = AsyncMock(side_effect=describe_topics)
    admin.create_topics = AsyncMock()
    broker = _broker_with_admin(admin)

    await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")

    # Both topics must be created when the existence-check describe reports
    # UNKNOWN_TOPIC_OR_PARTITION.
    assert admin.create_topics.await_count == 2


async def test_create_topic_race_validates_partition_count_matches() -> None:
    """When TopicAlreadyExistsError fires during create, re-describe must run
    and accept the topic when the partition count matches.
    """
    admin = AsyncMock()

    # 1: resolve main topic -> 6
    # 2: state pre-create describe -> not found
    # 3: state post-race re-describe -> found w/ 6 partitions (matches)
    # 4: returns pre-create describe -> found w/ 6 partitions (no race)
    describe_results: list[list[dict[str, Any]]] = [
        [_topic_metadata("agent.in", 6)],
        [],
        [_topic_metadata("agent.fanout-state", 6)],
        [_topic_metadata("agent.fanout-returns", 6)],
    ]
    describe_call_count = 0

    async def describe_topics(_topic_names: list[str]) -> list[dict[str, Any]]:
        nonlocal describe_call_count
        result = describe_results[describe_call_count]
        describe_call_count += 1
        return result

    admin.describe_topics = AsyncMock(side_effect=describe_topics)
    # Only the state create races; returns is unaffected (already exists in step 4).
    admin.create_topics = AsyncMock(side_effect=TopicAlreadyExistsError())
    broker = _broker_with_admin(admin)

    _state, _returns, partitions = await ensure_aggregator_topics(
        broker,
        node_id="agent",
        main_topic="agent.in",
    )

    assert partitions == 6
    # state attempted create + raced; returns was pre-existing so no create.
    assert admin.create_topics.await_count == 1


async def test_create_topic_race_raises_on_partition_count_mismatch() -> None:
    """If the peer worker race-created the topic with a different partition
    count, the re-describe must surface that drift as AggregatorStateStoreError.
    """
    admin = AsyncMock()

    # 1: resolve main topic -> 6
    # 2: state pre-create describe -> not found
    # 3: state post-race re-describe -> found w/ 3 partitions (mismatch!)
    describe_results: list[list[dict[str, Any]]] = [
        [_topic_metadata("agent.in", 6)],
        [],
        [_topic_metadata("agent.fanout-state", 3)],
    ]
    describe_call_count = 0

    async def describe_topics(_topic_names: list[str]) -> list[dict[str, Any]]:
        nonlocal describe_call_count
        result = describe_results[describe_call_count]
        describe_call_count += 1
        return result

    admin.describe_topics = AsyncMock(side_effect=describe_topics)
    admin.create_topics = AsyncMock(side_effect=TopicAlreadyExistsError())
    broker = _broker_with_admin(admin)

    with pytest.raises(AggregatorStateStoreError, match="race-created"):
        await ensure_aggregator_topics(
            broker,
            node_id="agent",
            main_topic="agent.in",
        )


async def test_create_topic_race_raises_if_redescribe_returns_none() -> None:
    """If create raises TopicAlreadyExistsError but the post-race describe
    returns nothing, the worker can't validate co-partitioning and must fail
    loudly rather than continue.
    """
    admin = AsyncMock()

    # 1: resolve main topic -> 6
    # 2: state pre-create describe -> not found
    # 3: state post-race re-describe -> still not found
    describe_results: list[list[dict[str, Any]]] = [
        [_topic_metadata("agent.in", 6)],
        [],
        [],
    ]
    describe_call_count = 0

    async def describe_topics(_topic_names: list[str]) -> list[dict[str, Any]]:
        nonlocal describe_call_count
        result = describe_results[describe_call_count]
        describe_call_count += 1
        return result

    admin.describe_topics = AsyncMock(side_effect=describe_topics)
    admin.create_topics = AsyncMock(side_effect=TopicAlreadyExistsError())
    broker = _broker_with_admin(admin)

    with pytest.raises(AggregatorStateStoreError, match="cannot be re-described"):
        await ensure_aggregator_topics(
            broker,
            node_id="agent",
            main_topic="agent.in",
        )


# ---------------------------------------------------------------------------
# Existing-topic config validation (cleanup.policy must match for state topic)
# ---------------------------------------------------------------------------


async def test_existing_state_topic_with_correct_cleanup_policy_passes() -> None:
    """A pre-existing state topic whose cleanup.policy matches the required
    ``compact`` setting must be accepted without re-creation."""
    admin = _make_admin(
        {
            "agent.in": 6,
            "agent.fanout-state": 6,
            "agent.fanout-returns": 6,
        },
        topic_configs={
            "agent.fanout-state": dict(STATE_TOPIC_CONFIG),
        },
    )
    broker = _broker_with_admin(admin)

    await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")

    assert admin.create_topics.await_count == 0
    # The state topic's config must have been queried for validation.
    admin.describe_configs.assert_awaited()


async def test_existing_state_topic_with_wrong_cleanup_policy_raises() -> None:
    """If the pre-existing state topic was created with ``cleanup.policy=delete``,
    durability would silently break (tombstones get retention-deleted). The
    aggregator must refuse to start rather than corrupt durable state."""
    admin = _make_admin(
        {
            "agent.in": 6,
            "agent.fanout-state": 6,
            "agent.fanout-returns": 6,
        },
        topic_configs={
            "agent.fanout-state": {
                **STATE_TOPIC_CONFIG,
                "cleanup.policy": "delete",
            },
        },
    )
    broker = _broker_with_admin(admin)

    with pytest.raises(AggregatorStateStoreError, match="cleanup.policy"):
        await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")


async def test_returns_topic_does_not_validate_configs() -> None:
    """The returns topic is a regular event stream (no required configs);
    only partition count matters. Even if its cleanup.policy is unusual,
    ``_ensure_topic`` must skip the config-validation path entirely."""
    admin = _make_admin(
        {
            "agent.in": 6,
            "agent.fanout-state": 6,
            "agent.fanout-returns": 6,
        },
        topic_configs={
            "agent.fanout-state": dict(STATE_TOPIC_CONFIG),
            # Intentionally surprising config on the returns topic; must be ignored.
            "agent.fanout-returns": {"cleanup.policy": "delete", "retention.ms": "1000"},
        },
    )
    broker = _broker_with_admin(admin)

    await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")

    # describe_configs may be called for the state topic, but never for returns.
    described_topics = [resource.name for call in admin.describe_configs.await_args_list for resource in call.args[0]]
    assert "agent.fanout-returns" not in described_topics


# ---------------------------------------------------------------------------
# describe_configs per-resource error_code handling
# ---------------------------------------------------------------------------


def _describe_configs_response_with_error(topic: str, error_code: int) -> Any:
    """Build a mock DescribeConfigsResponse where the resource carries a
    non-zero ``error_code`` (broker auth failure, invalid topic, etc.).

    Mirrors the aiokafka response shape; the broker returns the topic name
    and an empty config_entries list alongside the error code.
    """
    resource = (error_code, "", 2, topic, [])
    return MagicMock(resources=[resource])


async def test_describe_topic_configs_raises_on_authorization_failure() -> None:
    """When ``describe_configs`` returns a per-resource error_code (e.g.,
    TOPIC_AUTHORIZATION_FAILED = 29), the aggregator must surface the failure
    as an actionable :class:`AggregatorStateStoreError` rather than silently
    returning an empty config dict and reporting a misleading
    "cleanup.policy=None" mismatch downstream."""
    admin = AsyncMock()

    async def describe_topics(topic_names: list[str]) -> list[dict[str, Any]]:
        # main topic + existing state/returns topics so we reach the
        # config-validation branch in _ensure_topic.
        return [_topic_metadata(name, 6) for name in topic_names]

    async def describe_configs(config_resources: list[Any]) -> list[Any]:
        return [_describe_configs_response_with_error(resource.name, 29) for resource in config_resources]

    admin.describe_topics = AsyncMock(side_effect=describe_topics)
    admin.describe_configs = AsyncMock(side_effect=describe_configs)
    admin.create_topics = AsyncMock()
    broker = _broker_with_admin(admin)

    with pytest.raises(AggregatorStateStoreError) as exc_info:
        await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")

    message = str(exc_info.value)
    # The canonical Kafka protocol error name must surface so an operator can
    # search broker docs / logs for it directly.
    assert "TOPIC_AUTHORIZATION_FAILED" in message
    assert "agent.fanout-state" in message
    # Clear "describe_configs failed" wording disambiguates this failure
    # from the cleanup.policy-mismatch error path.
    assert "describe_configs" in message


async def test_describe_topic_configs_raises_with_state_topic_context() -> None:
    """The raised error must carry ``state_topic`` so structured handlers
    (Sentry / metric tags) can attribute the failure to the right agent."""
    admin = AsyncMock()

    async def describe_topics(topic_names: list[str]) -> list[dict[str, Any]]:
        return [_topic_metadata(name, 6) for name in topic_names]

    async def describe_configs(config_resources: list[Any]) -> list[Any]:
        return [_describe_configs_response_with_error(resource.name, 29) for resource in config_resources]

    admin.describe_topics = AsyncMock(side_effect=describe_topics)
    admin.describe_configs = AsyncMock(side_effect=describe_configs)
    admin.create_topics = AsyncMock()
    broker = _broker_with_admin(admin)

    with pytest.raises(AggregatorStateStoreError) as exc_info:
        await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")

    assert exc_info.value.state_topic == "agent.fanout-state"


async def test_describe_topic_configs_succeeds_on_zero_error_code() -> None:
    """Sanity check: when ``error_code=0``, the happy path returns configs
    correctly and aggregator topic provisioning succeeds without raising."""
    admin = _make_admin(
        {
            "agent.in": 6,
            "agent.fanout-state": 6,
            "agent.fanout-returns": 6,
        },
        topic_configs={
            "agent.fanout-state": dict(STATE_TOPIC_CONFIG),
        },
    )
    broker = _broker_with_admin(admin)

    await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")

    assert admin.create_topics.await_count == 0
    admin.describe_configs.assert_awaited()


async def test_describe_topic_configs_raises_on_unresolvable_error_code() -> None:
    """If the broker returns an error code that aiokafka can't map to a known
    class, the message must still cite the numeric code so operators have a
    grep target — ``for_code`` is documented to return ``UnknownError`` for
    unmapped codes, so the implementation must always include the raw int."""
    admin = AsyncMock()

    async def describe_topics(topic_names: list[str]) -> list[dict[str, Any]]:
        return [_topic_metadata(name, 6) for name in topic_names]

    async def describe_configs(config_resources: list[Any]) -> list[Any]:
        return [_describe_configs_response_with_error(resource.name, 12345) for resource in config_resources]

    admin.describe_topics = AsyncMock(side_effect=describe_topics)
    admin.describe_configs = AsyncMock(side_effect=describe_configs)
    admin.create_topics = AsyncMock()
    broker = _broker_with_admin(admin)

    with pytest.raises(AggregatorStateStoreError) as exc_info:
        await ensure_aggregator_topics(broker, node_id="agent", main_topic="agent.in")

    assert "12345" in str(exc_info.value)
