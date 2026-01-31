from abc import ABC
from typing import Any, cast

from pydantic_ai import ModelResponse, ModelSettings
from pydantic_ai.direct import model_request
from pydantic_ai.models import Model, ModelRequestParameters

from calf.models.event_envelope import EventEnvelope
from calf.nodes.base_node import BaseNode, publish_to, subscribe_to


class ChatNode(BaseNode, ABC):
    """Entity for defining llm chat node internal wiring.
    Separate from any logic for LLM persona or behaviour."""

    # TODO: a separate layer of abstraction for LLM behaviour
    # and persona. i.e. memory, prompting, etc.

    _on_enter_topic_name = "ai_prompted"
    _post_to_topic_name = "ai_generated"

    def __init__(
        self,
        model_client: Model | None = None,
        *,
        request_parameters: ModelRequestParameters | None = None,
        **kwargs: Any,
    ):
        self.model_client = model_client
        self.request_parameters = request_parameters
        super().__init__(**kwargs)

    @subscribe_to(_on_enter_topic_name)
    @publish_to(_post_to_topic_name)
    async def _call_llm(self, event_envelope: EventEnvelope) -> EventEnvelope:
        if self.model_client is None:
            raise RuntimeError("Unable to handle incoming request because Model client is None.")
        if event_envelope.latest_message_in_history is None:
            raise RuntimeError("latest message must not be None")
        request_parameters = event_envelope.patch_model_request_params or self.request_parameters
        patch_model_settings = event_envelope.patch_model_settings
        model_response: ModelResponse = await model_request(
            model=self.model_client,
            messages=event_envelope.message_history,
            model_settings=cast(ModelSettings | None, patch_model_settings),
            model_request_parameters=request_parameters,
        )
        return_envelope = event_envelope.model_copy(
            update={"kind": "ai_response", "incoming_node_messages": [model_response]}
        )
        return return_envelope
