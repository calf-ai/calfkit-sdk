from collections.abc import Sequence
from typing import Literal

from pydantic_ai import ModelMessage, ModelRequest
from pydantic_ai.models import ModelRequestParameters

from calf.models.types import CompactBaseModel, SerializableModelSettings, ToolCallRequest


class EventEnvelope(CompactBaseModel):
    kind: Literal["tool_call_request", "user_prompt", "ai_response", "tool_result"]
    text: str | None = None
    trace_id: str | None = None

    # Used to surface the tool call from latest message so tool call workers do not have to dig
    tool_call_request: ToolCallRequest | None = None

    # Optional inference-time patch in settings and parameters
    patch_model_request_params: ModelRequestParameters | None = None
    patch_model_settings: SerializableModelSettings | None = None

    # running message history
    message_history: list[ModelMessage] = []

    @property
    def latest_message_in_history(self):
        return self.message_history[-1] if self.message_history else None

    # The result message from a node
    incoming_node_messages: Sequence[ModelMessage] = []

    # thread id / conversation identifier
    thread_id: str | None = None

    system_message: ModelRequest | None = None
