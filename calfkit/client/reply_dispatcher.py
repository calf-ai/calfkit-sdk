from __future__ import annotations

import asyncio
import logging
from typing import Annotated

from faststream import Context
from faststream.kafka import KafkaBroker

from calfkit.models.envelope import Envelope

logger = logging.getLogger(__name__)


class _ReplyDispatcher:
    """Shared reply consumer that dispatches incoming envelopes to futures keyed by correlation_id."""

    def __init__(self) -> None:
        self._pending: dict[str, asyncio.Future[Envelope]] = {}

    def register(self, broker: KafkaBroker, reply_topic: str, group_id: str) -> None:
        """Register a FastStream subscriber on *reply_topic*. Must be called before ``broker.start()``."""

        @broker.subscriber(reply_topic, group_id=group_id, auto_offset_reset="latest")
        async def _handle_reply(
            envelope: Envelope,
            correlation_id: Annotated[str, Context()],
        ) -> None:
            future = self._pending.pop(correlation_id, None)
            if future is None:
                logger.warning("[%s] reply received but no pending future", correlation_id[:8])
                return
            if future.cancelled():
                logger.debug("[%s] reply received but future already cancelled", correlation_id[:8])
                return
            future.set_result(envelope)

    def expect(self, correlation_id: str) -> asyncio.Future[Envelope]:
        """Create and return a future for *correlation_id*. Raises if already pending."""
        if correlation_id in self._pending:
            raise RuntimeError(f"Duplicate correlation_id: {correlation_id}")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Envelope] = loop.create_future()
        self._pending[correlation_id] = future
        future.add_done_callback(lambda _: self._pending.pop(correlation_id, None))
        return future

    def close(self) -> None:
        """Cancel all pending futures and clear the registry."""
        for future in self._pending.values():
            if not future.done():
                future.cancel()
        self._pending.clear()
