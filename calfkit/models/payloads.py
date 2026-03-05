from calfkit._vendor.pydantic_ai.models import ModelRequestParameters
from calfkit.models.types import CompactBaseModel, SerializableModelSettings, ToolCallRequest


class ChatPayload(CompactBaseModel):
    """What ChatNode needs to call the LLM."""

    user_prompt: str | None = None
    instructions: str | None = None
    name: str | None = None
    patch_model_request_params: ModelRequestParameters | None = None
    patch_model_settings: SerializableModelSettings | None = None


class ToolPayload(CompactBaseModel):
    """What a ToolNode needs to execute a tool call."""

    tool_call_request: ToolCallRequest
    agent_name: str | None = None
