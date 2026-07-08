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
import contextlib
import logging
from collections.abc import Sequence


class _DropConnectRetryNoise(logging.Filter):
    """Drops aiokafka's expected per-attempt bootstrap retry log while a broker is still coming up.

    aiokafka has TWO ``Unable connect …`` ERROR sites on the ``aiokafka`` logger: the bootstrap-attempt
    retry ``Unable connect to "<host>:<port>": …`` (client.py, the noise we want gone) and a genuine
    ``Unable connect to node with id <n>: …`` diagnostic for a discovered node dropping. We anchor on
    the ``to "`` quote so ONLY the bootstrap noise is dropped and the node-failure diagnostic — like
    every other aiokafka error (auth/TLS/protocol) — stays visible. (String-coupled to a pinned
    aiokafka message; it fails *open* — noise reappears — if that wording ever changes.)"""

    def filter(self, record: logging.LogRecord) -> bool:
        return not record.getMessage().startswith('Unable connect to "')


async def broker_reachable(bootstrap: str | Sequence[str], *, timeout: float) -> bool:
    """Return ``True`` iff a Kafka-speaking broker answers at *bootstrap* within *timeout* seconds.

    Has no topic side effects (the bootstrap metadata request names zero topics). The client is
    **always** closed, which cancels aiokafka's metadata-sync task and closes any retained connection.
    """
    from aiokafka import AIOKafkaClient  # type: ignore[import-untyped]
    from aiokafka.errors import KafkaError  # type: ignore[import-untyped]

    servers = bootstrap if isinstance(bootstrap, str) else list(bootstrap)
    client = AIOKafkaClient(bootstrap_servers=servers)
    # aiokafka logs 'Unable connect …' at ERROR on the "aiokafka" logger (client.py binds
    # logging.getLogger("aiokafka")) on every failed bootstrap attempt; while a broker is still
    # coming up those are expected retry noise. Drop ONLY those records for the probe with a
    # message-scoped filter, removed afterward — so a genuinely different aiokafka error still
    # surfaces (visibility into a real failure) and logging during real operation is untouched.
    aiokafka_log = logging.getLogger("aiokafka")
    noise_filter = _DropConnectRetryNoise()
    aiokafka_log.addFilter(noise_filter)
    try:
        await asyncio.wait_for(client.bootstrap(), timeout)
        return True
    except (KafkaError, asyncio.TimeoutError, OSError):
        return False
    finally:
        aiokafka_log.removeFilter(noise_filter)
        # Best-effort cleanup — the probe result is the signal, a close failure must not surface as a throw.
        with contextlib.suppress(Exception):
            await client.close()


def is_reachable(bootstrap: str | Sequence[str], *, timeout: float) -> bool:
    """Synchronous wrapper over :func:`broker_reachable` for CLI code paths."""
    return asyncio.run(broker_reachable(bootstrap, timeout=timeout))
