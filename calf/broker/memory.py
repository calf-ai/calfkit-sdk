import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator

from calf.broker.base import Broker
from calf.message import Message

class LocalInMemoryBroker(Broker):
    """In-memory broker for local development and testing."""

    def __init__(self) -> None:
        self._queues: dict[str, list[asyncio.Queue[Message]]] = defaultdict(list)
        self._connected = False

    async def connect(self) -> None:
        """Establish connection (no-op for in-memory broker)."""
        self._connected = True

    async def disconnect(self) -> None:
        """Close connection (no-op for in-memory broker)."""
        self._connected = False

    async def send(self, channel: str, message: Message) -> None:
        """Send a message to all subscribers of a channel."""
        if not self._connected:
            raise RuntimeError("Broker is not connected")

        for queue in self._queues[channel]:
            await queue.put(message)

    async def subscribe(self, channel: str) -> AsyncIterator[Message]:
        """Subscribe to a channel and yield messages."""
        if not self._connected:
            raise RuntimeError("Broker is not connected")

        queue: asyncio.Queue[Message] = asyncio.Queue()
        self._queues[channel].append(queue)

        try:
            while True:
                message = await queue.get()
                yield message
        finally:
            self._queues[channel].remove(queue)
