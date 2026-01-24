"""Abstract ModelClient protocol for LLM providers."""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, Generic, TypeVar

MessageT = TypeVar("MessageT")
ToolT = TypeVar("ToolT")
ResponseT = TypeVar("ResponseT", bound="GenerateResponse")


class ToolCall:
    """Base class for tool calls from the LLM."""

    def __init__(
        self,
        id: str,
        name: str,
        args: Sequence[Any],
        kwargs: dict[str, Any],
    ):
        self.id = id
        self.name = name
        self.args = args
        self.kwargs = kwargs


class GenerateResponse:
    """Base class for normalized LLM response."""

    def __init__(
        self,
        text: str | None,
        tool_calls: Sequence[ToolCall] | None,
    ):
        self.text = text
        self.tool_calls = tool_calls


class ModelClient(ABC, Generic[MessageT, ToolT, ResponseT]):
    """Abstract base class for LLM model clients.

    Implementations must support OpenAI-compatible chat completion APIs.
    """

    @abstractmethod
    async def generate(
        self,
        messages: list[MessageT],
        *,
        tools: list[ToolT] | None = None,
    ) -> ResponseT:
        """Execute a chat completion request against the LLM.

        Args:
            messages: Ordered sequence of messages.
            tools: Optional sequence of tool definitions.

        Returns:
            GenerateResponse containing a normalized response containing generated content,
            tool calls, and usage stats.
        """
        ...
