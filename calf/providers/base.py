"""Abstract ModelClient protocol for LLM providers."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Sequence, Union

from pydantic import BaseModel, ConfigDict


class ToolCall(BaseModel):
    """A function call from the LLM."""

    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    arguments: str  # JSON string

class Message(BaseModel):
    """Base message schema for provider inputs."""

    model_config = ConfigDict(extra="allow", frozen=True)
    
    data: Dict[str, Any]
    
class Tool(BaseModel):
    """Base tool schema for provider inputs."""

    model_config = ConfigDict(extra="allow", frozen=True)
    
    data: Dict[str, Any]


class GenerateResponse(BaseModel):
    """Normalized response schema from LLM generation for simpler downstream parsing."""

    model_config = ConfigDict(frozen=True)

    message: str | None
    
    tool_calls: List[ToolCall] | None
    
    usage: Optional[Dict[str, Any]] = {}

class ModelClient(ABC):
    """Abstract base class for LLM model clients.

    Implementations must support OpenAI-compatible chat completion APIs.
    """

    @abstractmethod
    async def generate(
        self,
        messages: Sequence[Message],
        *,
        tools: Optional[Sequence[Tool]] = None,
    ) -> GenerateResponse:
        """Execute a chat completion request against the LLM.

        Args:
            messages: Ordered sequence of messages.
            tools: Optional sequence of tool definitions.

        Returns:
            GenerateResponse containing a normalized response containing generated content, tool calls,
            and usage stats.
        """
        ...
