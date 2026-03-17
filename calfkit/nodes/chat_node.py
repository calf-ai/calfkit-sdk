from abc import ABC
from typing import Any, cast

from calfkit._vendor.pydantic_ai import Agent, DeferredToolRequests, ExternalToolset, ModelSettings
from calfkit._vendor.pydantic_ai.models import Model
from calfkit.models.event_envelope import EventEnvelope
from calfkit.models.payloads import ChatPayload
from calfkit.nodes.base_node import BaseNode, publish_to, subscribe_to
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient


class ChatNode(BaseNode, ABC):
    """Node defining the llm chat node internal wiring.
    Separate from any logic for LLM persona or behaviour."""

    _on_enter_topic_name = "ai_prompted"
    _post_to_topic_name = "ai_generated"

    def __init__(
        self,
        model_client: PydanticModelClient | None = None,
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

        # Read from ChatPayload
        payload = (
            ChatPayload.model_validate(event_envelope.payload)
            if event_envelope.payload is not None
            else ChatPayload()
        )

        # Build ExternalToolset from payload's tool definitions
        toolsets: list[ExternalToolset[Any]] = []
        if payload.patch_model_request_params and payload.patch_model_request_params.function_tools:
            toolsets.append(ExternalToolset(payload.patch_model_request_params.function_tools))

        # Call the Agent — pass deps so pydantic-ai dynamic instructions can access structured input
        result = await self.agent.run(
            user_prompt=payload.user_prompt,
            message_history=list(event_envelope.state.message_history),
            instructions=payload.instructions,
            model_settings=cast(ModelSettings | None, payload.patch_model_settings),
            toolsets=toolsets or None,
            deps=event_envelope.deps,
        )

        # Clear user_prompt to prevent re-use on subsequent passes
        payload.user_prompt = None

        # Stamp agent name on the model response
        if payload.name is not None:
            result.response.name = payload.name

        # Set output on payload — every output type (str, BaseModel, etc.) is a payload.
        # Only DeferredToolRequests (tool calls pending) clears payload.
        if isinstance(result.output, DeferredToolRequests):
            event_envelope.payload = None
        else:
            event_envelope.payload = (
                result.output.model_dump(mode="json")
                if hasattr(result.output, "model_dump")
                else result.output
            )

        # Add new messages to state's uncommitted
        for msg in result.new_messages():
            event_envelope.state.add_to_uncommitted_messages(msg)

        return event_envelope
