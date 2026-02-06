from collections.abc import Sequence

from calfkit._vendor.pydantic_ai import ModelMessage, ModelRequest
from calfkit._vendor.pydantic_ai.models import ModelRequestParameters
from calfkit.models.types import CompactBaseModel, SerializableModelSettings, ToolCallRequest


class EventEnvelope(CompactBaseModel):
    trace_id: str | None = None

    # Used to surface the tool call from latest message so tool call workers do not have to dig
    # For tool node eyes only
    tool_call_request: ToolCallRequest | None = None

    # Pending tool calls to enforce sequential tool calling when thread_id
    # is not provided or when there is no memory history store configured
    pending_tool_calls: list[ToolCallRequest] = []

    # Optional inference-time patch in settings and parameters
    patch_model_request_params: ModelRequestParameters | None = None
    patch_model_settings: SerializableModelSettings | None = None

    # Running message history
    message_history: list[ModelMessage] = []

    @property
    def latest_message_in_history(self) -> ModelMessage | None:
        return self.message_history[-1] if self.message_history else None

    # Uncommitted messages, often coming out from a node and not yet persisted in message history
    uncommitted_messages: Sequence[ModelMessage] = []

    # thread id / conversation identifier
    thread_id: str | None = None

    # Allow client to dynamically patch system message at runtime
    # Intentionally kept separate from message_history in order to simplify patch logic
    system_message: ModelRequest | None = None

    # Where the final response from AI should be published to
    final_response_topic: str | None = None

    # Whether the current state of messages is the final response from the AI to the user
    final_response: bool = False
