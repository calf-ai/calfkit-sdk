"""Broker reachability probe for ``ck dev`` (spec §5.1).

A topic-less Kafka metadata handshake: :meth:`AIOKafkaClient.bootstrap` negotiates ``ApiVersions`` on
connect and issues a ``MetadataRequest([])`` with an **empty topic list** — so it names zero topics and
cannot auto-create any (unlike a metadata request that names a topic, or ``describe_topics``, which
auto-creates on some brokers). Success means a healthy Kafka-speaking broker is present. The probe is
**plaintext** (a SASL/TLS-secured broker reads as absent — spec §5.7), and the *same* probe gates
spawn-readiness (§5.5).

``aiokafka`` is imported lazily inside the functions so importing this module — and ``calfkit`` — stays
cheap and pulls no aiokafka at load time.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence


async def broker_reachable(bootstrap: str | Sequence[str], *, timeout: float) -> bool:
    """Return ``True`` iff a Kafka-speaking broker answers at *bootstrap* within *timeout* seconds.

    Has no topic side effects (the bootstrap metadata request names zero topics). The client is
    **always** closed, which cancels aiokafka's metadata-sync task and closes any retained connection.
    """
    from aiokafka import AIOKafkaClient  # type: ignore[import-untyped]
    from aiokafka.errors import KafkaError  # type: ignore[import-untyped]

    servers = bootstrap if isinstance(bootstrap, str) else list(bootstrap)
    client = AIOKafkaClient(bootstrap_servers=servers)
    # aiokafka logs 'Unable connect …' at ERROR on every failed bootstrap attempt; while a broker is
    # still coming up those are expected noise, so raise its threshold for the probe and restore it.
    aiokafka_log = logging.getLogger("aiokafka.client")
    previous_level = aiokafka_log.level
    aiokafka_log.setLevel(logging.CRITICAL)
    try:
        await asyncio.wait_for(client.bootstrap(), timeout)
        return True
    except (KafkaError, asyncio.TimeoutError, OSError):
        return False
    finally:
        aiokafka_log.setLevel(previous_level)
        await client.close()


def is_reachable(bootstrap: str | Sequence[str], *, timeout: float) -> bool:
    """Synchronous wrapper over :func:`broker_reachable` for CLI code paths."""
    return asyncio.run(broker_reachable(bootstrap, timeout=timeout))
