"""Topic provisioning for the fan-out aggregator's per-agent topics.

Uses FastStream-exposed ``broker.config.admin_client`` (an
``aiokafka.AIOKafkaAdminClient`` under the hood) to ensure
``{node_id}.fanout-state`` (compacted) and ``{node_id}.fanout-returns``
(regular) exist with the correct configuration before the worker starts
processing. When topics already exist, validates the partition count
matches the agent's main topic; raises :class:`AggregatorStateStoreError`
on mismatch.
"""

from __future__ import annotations

import logging
from typing import Any, cast

from aiokafka.admin import NewTopic  # type: ignore[import-untyped]
from aiokafka.errors import (  # type: ignore[import-untyped]
    TopicAlreadyExistsError,
    UnknownTopicOrPartitionError,
)
from faststream.kafka import KafkaBroker

from calfkit.nodes.aggregator.errors import AggregatorStateStoreError

logger = logging.getLogger(__name__)


STATE_TOPIC_CONFIG: dict[str, str] = {
    "cleanup.policy": "compact",
    "min.compaction.lag.ms": "60000",
    "delete.retention.ms": "60000",
    "segment.ms": "604800000",
}
"""Default Kafka topic configuration for the compacted state topic.

``min.compaction.lag.ms`` and ``delete.retention.ms`` are set to 60 seconds
to match the ``_recently_completed`` TTL in the in-memory cache. This gives
the aggregator a 60-second window to distinguish a recently-tombstoned key
from one that has been compacted away during a rebalance.
"""


async def ensure_aggregator_topics(
    broker: KafkaBroker,
    node_id: str,
    main_topic: str,
    *,
    default_partitions: int = 6,
    replication_factor: int = 3,
) -> tuple[str, str, int]:
    """Provision the aggregator's state + returns topics, co-partitioned with
    the agent's main topic.

    Algorithm:

    1. Look up ``main_topic``'s partition count via
       ``admin.describe_topics``. If the main topic doesn't exist yet,
       fall back to ``default_partitions`` and emit a WARN — the user is
       presumably running an end-to-end demo and not managing topics
       externally.
    2. For each of state and returns:

       - If the topic doesn't exist, create it with the right partition
         count and (for state) ``cleanup.policy=compact``.
       - If it exists, validate that the partition count matches.

    3. Return ``(state_topic, returns_topic, partition_count)`` for the
       state store and rebalance listener to use downstream.

    Raises:
        AggregatorStateStoreError: configuration mismatch (e.g., existing
            state topic has a different partition count from the main topic).
    """
    state_topic = f"{node_id}.fanout-state"
    returns_topic = f"{node_id}.fanout-returns"

    admin: Any = broker.config.admin_client

    partition_count = await _resolve_partition_count(
        admin,
        main_topic,
        default_partitions=default_partitions,
    )

    await _ensure_topic(
        admin,
        state_topic,
        partitions=partition_count,
        replication_factor=replication_factor,
        configs=STATE_TOPIC_CONFIG,
    )

    await _ensure_topic(
        admin,
        returns_topic,
        partitions=partition_count,
        replication_factor=replication_factor,
        configs=None,
    )

    logger.info(
        "aggregator topics ensured: state=%s returns=%s partitions=%d",
        state_topic,
        returns_topic,
        partition_count,
    )

    return state_topic, returns_topic, partition_count


async def _resolve_partition_count(
    admin: Any,
    main_topic: str,
    *,
    default_partitions: int,
) -> int:
    """Read the main topic's partition count, or fall back to default.

    aiokafka's ``describe_topics`` returns a list of dicts each with at
    least ``"topic"`` and ``"partitions"`` keys (the latter being a list
    of per-partition metadata dicts).
    """
    try:
        descriptions = await admin.describe_topics([main_topic])
    except UnknownTopicOrPartitionError:
        logger.debug(
            "main topic %s not found via describe_topics; using default",
            main_topic,
        )
        descriptions = []
    except Exception as exc:
        raise AggregatorStateStoreError(
            f"failed to describe main topic {main_topic!r}: {exc}",
            state_topic=main_topic,
        ) from exc

    for desc in descriptions:
        if desc.get("topic") == main_topic and not desc.get("error"):
            partitions = desc.get("partitions", [])
            if partitions:
                return len(partitions)

    logger.warning(
        "main topic %r not found or has no partitions; defaulting to %d for "
        "aggregator topics. Provision the main topic before worker startup "
        "for production deployments.",
        main_topic,
        default_partitions,
    )
    return default_partitions


async def _ensure_topic(
    admin: Any,
    topic: str,
    *,
    partitions: int,
    replication_factor: int,
    configs: dict[str, str] | None,
) -> None:
    """Create the topic if missing; validate partition count if present.

    Catches the multi-worker startup race where two workers concurrently
    call ``create_topics`` — falls through to the validate path on the
    second worker.
    """
    existing = await _try_describe(admin, topic)
    if existing is not None:
        existing_partitions = len(existing.get("partitions", []))
        if existing_partitions != partitions:
            raise AggregatorStateStoreError(
                f"topic {topic!r} has {existing_partitions} partitions but "
                f"the aggregator requires {partitions} (must match main topic). "
                f"Repartition the topic or align partition counts manually before "
                f"restarting.",
                state_topic=topic,
            )
        logger.debug("topic %s already exists with %d partitions", topic, partitions)
        return

    new_topic = NewTopic(
        name=topic,
        num_partitions=partitions,
        replication_factor=replication_factor,
        topic_configs=configs,
    )
    try:
        await admin.create_topics([new_topic], validate_only=False)
    except TopicAlreadyExistsError:
        # Multi-worker startup race: another worker created the topic
        # between our describe + create. Fall through to validate.
        logger.debug("topic %s race-created by another worker; continuing", topic)
        return
    except Exception as exc:
        raise AggregatorStateStoreError(
            f"failed to create topic {topic!r}: {exc}",
            state_topic=topic,
        ) from exc

    logger.info(
        "created topic %s partitions=%d configs=%s",
        topic,
        partitions,
        configs,
    )


async def _try_describe(admin: Any, topic: str) -> dict[str, Any] | None:
    """Return the topic's metadata dict, or ``None`` if the topic doesn't exist."""
    try:
        descriptions = await admin.describe_topics([topic])
    except UnknownTopicOrPartitionError:
        return None
    except Exception as exc:
        raise AggregatorStateStoreError(
            f"failed to describe topic {topic!r}: {exc}",
            state_topic=topic,
        ) from exc
    for desc in descriptions:
        if desc.get("topic") == topic and not desc.get("error"):
            return cast("dict[str, Any]", desc)
    return None
