from abc import ABC
from typing import Any, cast

from calfkit._vendor.pydantic_ai import ModelResponse, ModelSettings
from calfkit._vendor.pydantic_ai.direct import model_request
from calfkit._vendor.pydantic_ai.models import Model, ModelRequestParameters
from calfkit.models.event_envelope import EventEnvelope
from calfkit.nodes.base_node import BaseNode, entrypoint, publish_to, subscribe_to


class ChatNode(BaseNode, ABC):
    """Node defining the llm chat node internal wiring.
    Separate from any logic for LLM persona or behaviour."""

    _on_enter_topic_name = "ai_prompted"
    _post_to_topic_name = "ai_generated"

    def __init__(
        self,
        model_client: Model | None = None,
        *,
        name: str | None = None,
        request_parameters: ModelRequestParameters | None = None,
        **kwargs: Any,
    ):
        self.model_client = model_client
        self.request_parameters = request_parameters
        super().__init__(name=name, **kwargs)
        if self.name is not None:
            self._remove_shared_subscribe_topic()

    def _remove_shared_subscribe_topic(self) -> None:
        """Remove the shared subscribe topic so a named ChatNode only
        listens on its private topic."""
        for topics in self.bound_registry.values():
            shared = topics.get("shared_subscribe_topic")
            if shared and "subscribe_topics" in topics:
                topics["subscribe_topics"] = [t for t in topics["subscribe_topics"] if t != shared]

    @subscribe_to(_on_enter_topic_name)
    @entrypoint("ai_prompted.{name}")
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
        if event_envelope.name is not None:
            model_response.name = event_envelope.name
        event_envelope.add_to_uncommitted_messages(model_response)
        return event_envelope
