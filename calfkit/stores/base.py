from abc import ABC, abstractmethod
from collections.abc import Sequence

from calfkit._vendor.pydantic_ai.messages import ModelMessage


class MessageHistoryStore(ABC):
    """Abstract store for conversation message history.

    Designed for strong consistency in event-driven agent systems.
    Messages are persisted atomically as they flow through the agent graph.
    """

    @abstractmethod
    async def get(self, thread_id: str) -> list[ModelMessage]:
        """Load message history for a thread.

        Args:
            thread_id: Unique identifier for the conversation thread.

        Returns:
            List of messages in the thread. Returns empty list if thread
            doesn't exist (auto-create behavior).
        """
        ...

    @abstractmethod
    async def append(self, thread_id: str, message: ModelMessage) -> None:
        """Append a single message to history.

        This is the primary method for strong consistency - each message
        is persisted immediately before the next step in the agentic loop.

        Args:
            thread_id: Unique identifier for the conversation thread.
            message: The message to append.
        """
        ...

    async def append_many(self, thread_id: str, messages: Sequence[ModelMessage]) -> None:
        """Append multiple messages to history.

        Default implementation calls append() for each message.
        Override for batch optimization if needed.

        Args:
            thread_id: Unique identifier for the conversation thread.
            messages: List of messages to append.
        """
        for message in messages:
            await self.append(thread_id, message)

    @abstractmethod
    async def delete(self, thread_id: str) -> None:
        """Delete all messages for a thread.

        Args:
            thread_id: Unique identifier for the conversation thread.
        """
        ...
