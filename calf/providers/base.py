"""Abstract ProviderClient protocol for LLM providers."""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, Callable, Generic, Self, TypedDict, TypeVar

from pydantic import BaseModel

MessageT = TypeVar("MessageT")
ToolT = TypeVar("ToolT")
ResponseT = TypeVar("ResponseT", bound="GenerateResult")


class ToolCall(ABC):
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


class GenerateResult(ABC):
    """Base class for normalized LLM response."""

    def __init__(
        self,
        text: str | None,
        tool_calls: Sequence[ToolCall] | None,
    ):
        self.text = text
        self.tool_calls = tool_calls


class MessageAdaptor(ABC, Generic[MessageT]):
    """Base message adaptor class to translate defined input to generic provider's message object."""

    @classmethod
    @abstractmethod
    def create_user_message(cls, message: str, **kwargs) -> MessageT: ...

    @classmethod
    @abstractmethod
    def create_assistant_message(cls, message: str, tool_calls, **kwargs) -> MessageT: ...

    @classmethod
    @abstractmethod
    def create_tool_message(cls, tool_call_id: str, message: str, **kwargs) -> MessageT: ...

    @classmethod
    @abstractmethod
    def create_system_message(cls, message: str) -> MessageT: ...


class BasicToolSchema(TypedDict):
    name: str
    parameters: dict[str, Any]
    description: str


class ToolAdaptor(ABC, Generic[ToolT]):
    """Base tool adaptor class to translate defined input to provider's tool object"""

    @classmethod
    @abstractmethod
    def create_tool_schema(cls, tool: Callable | BasicToolSchema) -> ToolT: ...


class ProviderClient(MessageAdaptor[MessageT], ToolAdaptor[ToolT], ABC, Generic[MessageT, ToolT, ResponseT]):
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
            GenerateResult containing a normalized response containing generated content,
            tool calls, and usage stats.
        """
        ...
