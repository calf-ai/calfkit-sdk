from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic

from calfkit._vendor.pydantic_ai.messages import ModelMessage
from calfkit.experimental._types import OutputT
from calfkit.experimental.data_model.payload import ContentPart


@dataclass(frozen=True)
class NodeResult(Generic[OutputT]):
    """Client-facing projection of an agent node's reply.

    Strips framework internals from the wire-format ``Envelope`` and
    deserializes ``final_output_parts`` into a typed ``output``.
    """

    output: OutputT
    """Deserialized final output (typed via ``output_type``)."""

    output_parts: list[ContentPart]
    """Raw content parts for advanced introspection."""

    message_history: list[ModelMessage]
    """Full conversation history from the agent session."""

    metadata: Any
    """Application-level metadata attached to the workflow state."""

    correlation_id: str
    """The correlation ID that ties this result to its invocation."""
