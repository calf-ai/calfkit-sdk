from abc import ABC
from typing import Any, cast

from calfkit._vendor.pydantic_ai import Agent, DeferredToolRequests, ExternalToolset, ModelSettings
from calfkit._vendor.pydantic_ai.models import Model
from calfkit.models.event_envelope import EventEnvelope
from calfkit.nodes.base_node import BaseNode, publish_to, subscribe_to


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
        input_topic: str | list[str] | None = None,
        output_topic: str | None = None,
        output_type: type | None = None,
        **kwargs: Any,
    ):
        self.model_client = model_client
        self._structured_output_type = output_type
        if model_client is not None:
            agent_output_types: list[type] = (
                [output_type, DeferredToolRequests] if output_type else [str, DeferredToolRequests]
            )
            self.agent: Agent[Any, Any] = Agent(
                model_client,
                output_type=agent_output_types,
                defer_model_check=True,
            )
        if name is not None:
            if input_topic is None:
                input_topic = f"ai_prompted.{name}"
            if output_topic is None:
                output_topic = f"ai_generated.{name}"
        super().__init__(name=name, input_topic=input_topic, output_topic=output_topic, **kwargs)

    @subscribe_to(_on_enter_topic_name)
    @publish_to(_post_to_topic_name)
    async def _call_llm(self, event_envelope: EventEnvelope) -> EventEnvelope:
        if self.model_client is None:
            raise RuntimeError("Unable to handle incoming request because Model client is None.")

        # Build ExternalToolset from envelope's tool definitions
        toolsets: list[ExternalToolset[Any]] = []
        request_params = event_envelope.patch_model_request_params
        if request_params and request_params.function_tools:
            toolsets.append(ExternalToolset(request_params.function_tools))

        # Call the Agent — pass deps so pydantic-ai dynamic instructions can access structured input
        result = await self.agent.run(
            user_prompt=event_envelope.user_prompt,
            message_history=list(event_envelope.message_history),
            instructions=event_envelope.instructions,
            model_settings=cast(ModelSettings | None, event_envelope.patch_model_settings),
            toolsets=toolsets or None,
            deps=event_envelope.deps,
        )

        # Clear user_prompt to prevent re-use on subsequent passes
        event_envelope.user_prompt = None

        # Stamp agent name on the model response
        if event_envelope.name is not None:
            result.response.name = event_envelope.name

        # Detect structured output — if the result is not str or DeferredToolRequests,
        # it's a structured output type → serialize to payload
        if not isinstance(result.output, (str, DeferredToolRequests)):
            event_envelope.payload = (
                result.output.model_dump(mode="json")
                if hasattr(result.output, "model_dump")
                else result.output
            )

        # Add new messages to uncommitted
        for msg in result.new_messages():
            event_envelope.add_to_uncommitted_messages(msg)

        return event_envelope
