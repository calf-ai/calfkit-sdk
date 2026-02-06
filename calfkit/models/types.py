from typing import Any, TypeAlias

from pydantic import BaseModel
from typing_extensions import TypedDict

from calfkit._vendor.pydantic_ai import ToolCallPart

ToolCallRequest: TypeAlias = ToolCallPart


class SerializableModelSettings(TypedDict, total=False):
    """Serializable version of pydantic_ai.ModelSettings.

    This is a copy of ModelSettings with `timeout` narrowed
    to `float` only to ensure JSON serializability.
    """

    max_tokens: int
    """The maximum number of tokens to generate before stopping."""

    temperature: float
    """Amount of randomness injected into the response."""

    top_p: float
    """Nucleus sampling parameter."""

    timeout: float
    """Request timeout in seconds (float only, not httpx.Timeout)."""

    parallel_tool_calls: bool
    """Whether to allow parallel tool calls."""

    seed: int
    """Random seed for deterministic results."""

    presence_penalty: float
    """Penalize tokens based on whether they have appeared in the text so far."""

    frequency_penalty: float
    """Penalize tokens based on their existing frequency in the text so far."""

    logit_bias: dict[str, int]
    """Modify the likelihood of specified tokens appearing in the completion."""

    stop_sequences: list[str]
    """Sequences that will cause the model to stop generating."""

    extra_headers: dict[str, str]
    """Extra headers to send to the model."""

    extra_body: dict[str, Any]
    """Extra body to send to the model."""


class CompactBaseModel(BaseModel):
    """Base model that excludes unset and None values during serialization."""

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        kwargs.setdefault("exclude_unset", True)
        kwargs.setdefault("exclude_none", True)
        kwargs.setdefault("mode", "json")  # Converts datetime, etc. to JSON-serializable types
        return super().model_dump(**kwargs)

    def model_dump_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("exclude_unset", True)
        kwargs.setdefault("exclude_none", True)
        return super().model_dump_json(**kwargs)
