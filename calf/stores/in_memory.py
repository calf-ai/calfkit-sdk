"""In-memory MessageHistoryStore implementation for testing and development."""

from collections import defaultdict
from typing import cast

from pydantic_ai.messages import ModelMessage

from calf.stores.base import MessageHistoryStore


class InMemoryMessageHistoryStore(MessageHistoryStore):
    """In-memory message history store.

    Useful for testing and development. Not suitable for production
    as data is lost when the process exits.
    """

    def __init__(self) -> None:
        """Initialize an empty in-memory store."""
        self._messages: dict[str, list[ModelMessage]] = defaultdict(list)

    async def get(self, thread_id: str) -> list[ModelMessage]:
        """Load message history for a thread."""
        return list(cast(list[ModelMessage], self._messages.get(thread_id, [])))

    async def append(self, thread_id: str, message: ModelMessage) -> None:
        """Append a single message to history."""
        self._messages[thread_id].append(message)

    async def delete(self, thread_id: str) -> None:
        """Delete all messages for a thread."""
        self._messages.pop(thread_id, None)
