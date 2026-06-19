"""Shared real-broker helpers for the fault / seam / fan-out integration suites.

The fault suites are a cohesive family, so they share one Worker builder and one
topic pre-creation helper here rather than each re-declaring them (the older roundtrip
suites predate this module and keep their own ``_worker``/``_topics`` copies).
"""

from __future__ import annotations

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from calfkit.client import Client
from calfkit.worker import Worker

EARLIEST = {"auto_offset_reset": "earliest"}
"""Mandatory for every node + tap consumer: ``earliest`` keeps a consumer-group join from
racing (and dropping) the publish addressing it on a freshly auto-created partition."""


def fault_worker(bootstrap: str, *, nodes: list) -> Worker:
    """A Worker on its own broker connection, reading earliest."""
    return Worker(Client.connect(bootstrap), nodes=nodes, extra_subscribe_kwargs=EARLIEST)


async def ensure_topic(bootstrap: str, topic: str, *, config: dict[str, str] | None = None) -> None:
    """Pre-create *topic* (1 partition) so a consumer has a partition to read from the
    moment it starts — removing the auto-create metadata race for tapped / injected topics.

    ``config`` sets topic-level overrides (e.g. ``{"max.message.bytes": "4096"}`` to make a
    callback topic reject an oversized fault, exercising the strip-and-retry floor)."""
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1, topic_configs=config)])
    except TopicAlreadyExistsError:
        pass
    finally:
        await admin.close()
