from collections import defaultdict
from collections.abc import Sequence

from calfkit._vendor.pydantic_ai.messages import ModelMessage
from calfkit.stores.base import MessageHistoryStore


class InMemoryMessageHistoryStore(MessageHistoryStore):
    """In-memory message history store.

    Useful for testing and development. Not suitable for production
    as data is lost when the process exits.
    """

    def __init__(self) -> None:
        """Initialize an empty in-memory store."""
        self._messages: dict[str, list[tuple[str | None, ModelMessage]]] = defaultdict(list)

    async def get(self, thread_id: str, scope: str | None = None) -> list[ModelMessage]:
        """Load message history for a thread, optionally filtered by scope."""
        entries = self._messages.get(thread_id, [])
        if scope is None:
            return [msg for _, msg in entries]
        return [msg for s, msg in entries if s == scope]

    async def append(self, thread_id: str, message: ModelMessage, scope: str | None = None) -> None:
        """Append a single message to history."""
        self._messages[thread_id].append((scope, message))

    async def append_many(
        self, thread_id: str, messages: Sequence[ModelMessage], scope: str | None = None
    ) -> None:
        self._messages[thread_id].extend((scope, msg) for msg in messages)

    async def delete(self, thread_id: str, scope: str | None = None) -> None:
        """Delete messages for a thread, optionally filtered by scope."""
        if scope is None:
            self._messages.pop(thread_id, None)
        else:
            self._messages[thread_id] = [
                (s, msg) for s, msg in self._messages[thread_id] if s != scope
            ]
