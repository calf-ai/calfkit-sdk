"""Channel A — the broadcast-mirror fault tap for the real-broker fault tests.

The client edge now raises a typed ``NodeFaultError`` from ``result()`` (#250's client-side
reception shipped). This tap is the complementary *typed* channel for observing a node's
escalated (or floored) fault on its ``publish_topic`` broadcast mirror, independent of a
waiting client: every caller-capable node that escalates (or floors) a fault returns the
fault-bearing envelope as its handler ``Response``, and the worker's ``@publisher``
broadcasts it on ``publish_topic`` with headers ``x-calf-kind=fault`` +
``x-calf-error-type``. This module taps that topic with a raw ``AIOKafkaConsumer`` and
decodes the calfkit ``Envelope`` so a test can read ``envelope.reply.error`` — a typed
:class:`~calfkit.models.error_report.ErrorReport`.

Why a raw consumer and not a ``@consumer`` node: ``ConsumerContext`` has no ``.fault``
field yet (the consumer-side reception of #250 is still deferred), so a sink node projects
a ``FaultMessage`` to ``output=None`` and never sees the report. The raw tap reads the
reply slot directly.

Read ``earliest`` with a fresh group so the tap sees the mirror even if it joins after
the publish. Pre-create the tapped topic (see ``_ensure_topic`` in the test module) so
the consumer has a partition to read from the moment it starts.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaConsumer

from calfkit._protocol import decode_header_str
from calfkit.models.envelope import Envelope
from calfkit.models.reply import FaultMessage


class FaultTap:
    """A live tap over one topic: decode calfkit ``Envelope``s and surface their replies."""

    def __init__(self, consumer: AIOKafkaConsumer) -> None:
        self._consumer = consumer

    async def next_envelope(self, *, timeout: float = 30.0) -> tuple[Envelope, dict[str, str | None]]:
        """The next decoded envelope on the topic, with its headers as ``str | None``."""
        record = await asyncio.wait_for(self._consumer.getone(), timeout=timeout)
        envelope = Envelope.model_validate_json(record.value)
        headers = {key: decode_header_str(value) for key, value in record.headers}
        return envelope, headers

    async def next_fault(self, *, timeout: float = 30.0) -> tuple[FaultMessage, dict[str, str | None]]:
        """Drain until a ``FaultMessage`` envelope appears.

        A faulting node's ``publish_topic`` also carries its non-fault mirrors (the
        ``kind=call`` dispatch mirror with ``reply=None``), so skip past anything whose
        reply is not a :class:`FaultMessage`. ``timeout`` bounds the whole wait.
        """
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise AssertionError("no FaultMessage observed on the tapped topic within the timeout")
            try:
                envelope, headers = await self.next_envelope(timeout=remaining)
            except asyncio.TimeoutError:
                # No message before the deadline — surface the single, predictable failure
                # mode (AssertionError) on the next loop, so callers asserting "no fault
                # arrived" can ``pytest.raises(AssertionError)`` rather than catch a raw
                # timeout. Catch ``asyncio.TimeoutError`` (what ``wait_for`` raises), NOT the
                # builtin ``TimeoutError``: on Python <= 3.10 they are distinct classes, so
                # ``except TimeoutError`` would miss it (it is only an alias on 3.11+).
                continue
            if isinstance(envelope.reply, FaultMessage):
                return envelope.reply, headers


@asynccontextmanager
async def fault_tap(bootstrap: str, topic: str) -> AsyncIterator[FaultTap]:
    """Open a :class:`FaultTap` over *topic* for the duration of the ``with`` block.

    A fresh ``group_id`` + ``auto_offset_reset="earliest"`` so the tap reads the topic
    from the start regardless of when it joins relative to the publish under test.
    """
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        group_id=f"fault-tap-{uuid.uuid4().hex[:8]}",
    )
    await consumer.start()
    try:
        yield FaultTap(consumer)
    finally:
        await consumer.stop()
