from typing import Annotated, Any

import uuid_utils
from faststream import Context
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)
from pydantic_ai import ModelRequest, ModelResponse, SystemPromptPart
from pydantic_ai.models import ModelRequestParameters

from calf.broker.broker import Broker
from calf.messages import patch_system_prompts
from calf.models.event_envelope import EventEnvelope, ToolCallRequest
from calf.nodes.base_node import BaseNode, publish_to, subscribe_to
from calf.nodes.base_tool_node import BaseToolNode
from calf.stores.base import MessageHistoryStore


class AgentRouterNode(BaseNode):
    """Deployable unit orchestrating the internal routing to operate agents"""

    _router_sub_topic_name = "agent_router.input"
    _router_pub_topic_name = "agent_router.output"

    def __init__(
        self,
        chat_node: BaseNode,
        tool_nodes: list[BaseToolNode],
        system_prompt: str | None = None,
        handoff_nodes: list[type[BaseNode]] = [],
        message_history_store: MessageHistoryStore | None = None,
        *args,
        **kwargs,
    ):
        self.chat = chat_node
        self.tools = tool_nodes
        self.handoffs = handoff_nodes
        self.system_prompt = system_prompt
        self.system_message = (
            ModelRequest(parts=[SystemPromptPart(self.system_prompt)])
            if self.system_prompt
            else None
        )
        self.message_history_store = message_history_store

        self.tools_topic_registry: dict[str, str] = {
            tool.tool_schema().name: tool.subscribed_topic
            for tool in tool_nodes
            if tool.subscribed_topic is not None
        }

        self.tool_response_topics = [tool.publish_to_topic for tool in self.tools]

        super().__init__(*args, **kwargs)

    @subscribe_to(_router_sub_topic_name)
    @publish_to(_router_pub_topic_name)
    async def _router(
        self,
        ctx: EventEnvelope,
        correlation_id: Annotated[str, Context()],
        broker: BrokerAnnotation,
    ):
        if not ctx.incoming_node_messages:
            raise RuntimeError("There is no response message to process")

        # One central place where message history is updated
        if self.message_history_store is not None and ctx.thread_id is not None:
            await self.message_history_store.append_many(
                thread_id=ctx.thread_id, messages=ctx.incoming_node_messages
            )
            ctx.message_history = await self.message_history_store.get(thread_id=ctx.thread_id)
        else:
            ctx.message_history.extend(ctx.incoming_node_messages)

        # Apply system prompts with priority: incoming > self.system_message > existing history
        # First, apply self.system_message as fallback (replaces existing history)
        if ctx.system_message is not None:
            ctx.message_history = patch_system_prompts(ctx.message_history, [ctx.system_message])
        elif self.system_message is not None:
            ctx.message_history = patch_system_prompts(
                ctx.message_history,
                [self.system_message],
            )

        if isinstance(ctx.latest_message_in_history, ModelResponse):
            if (
                ctx.latest_message_in_history.finish_reason == "tool_call"
                or ctx.latest_message_in_history.tool_calls
            ):
                for tool_call in ctx.latest_message_in_history.tool_calls:
                    await self._route_tool(ctx, tool_call, correlation_id, broker)
            else:
                # reply to sender here
                await self._reply_to_sender(ctx, correlation_id, broker)
        else:
            # TODO: implement user chat request forwarding to the chat node
            # so message history can be managed in a central location: the router node
            await self._call_model(ctx, correlation_id, broker)
        return ctx

    async def _route_tool(
        self,
        event_envelope: EventEnvelope,
        generated_tool_call: ToolCallRequest,
        correlation_id: str,
        broker: Any,
    ) -> None:
        tool_topic = self.tools_topic_registry.get(generated_tool_call.tool_name)
        if tool_topic is None:
            # TODO: implement a short circuit to respond with an
            # error message for when provided tool does not exist.
            return
        event_envelope = event_envelope.model_copy(
            update={"kind": "tool_call_request", "tool_call_request": generated_tool_call}
        )
        await broker.publish(
            event_envelope,
            topic=tool_topic,
            correlation_id=correlation_id,
            reply_to=self.subscribed_topic,
        )

    async def _reply_to_sender(
        self, event_envelope: EventEnvelope, correlation_id: str, broker: Any
    ) -> None:
        event_envelope = event_envelope.model_copy(update={"kind": "ai_response"})
        await broker.publish(
            event_envelope,
            topic=self.publish_to_topic,
            correlation_id=correlation_id,
        )

    async def _call_model(
        self,
        event_envelope: EventEnvelope,
        correlation_id: str,
        broker: Any,
    ) -> None:
        patch_model_request_params = ModelRequestParameters(
            function_tools=[tool.tool_schema() for tool in self.tools]
        )
        event_envelope = event_envelope.model_copy(
            update={"patch_model_request_params": patch_model_request_params}
        )
        await broker.publish(
            event_envelope,
            topic=self.chat.subscribed_topic,
            correlation_id=correlation_id,
            reply_to=self.subscribed_topic,
        )

    async def invoke(
        self,
        user_prompt: str,
        broker: Broker,
        thread_id: str | None = None,
        correlation_id: str | None = None,
    ) -> str:
        """Invoke the agent

        Args:
            user_prompt (str): User prompt to request the model
            broker (Broker): The broker to connect to
            correlation_id (str | None, optional): Optionally provide a correlation ID
            for this request. Defaults to None.

        Returns:
            str: The correlation ID for this request
        """
        patch_model_request_params = ModelRequestParameters(
            function_tools=[tool.tool_schema() for tool in self.tools]
        )
        if correlation_id is None:
            correlation_id = uuid_utils.uuid7().hex
        new_node_messages = [ModelRequest.user_text_prompt(user_prompt)]
        await broker.publish(
            EventEnvelope(
                kind="user_prompt",
                trace_id=correlation_id,
                patch_model_request_params=patch_model_request_params,
                thread_id=thread_id,
                incoming_node_messages=new_node_messages,
                system_message=self.system_message,
            ),
            topic=self.subscribed_topic or "",
            correlation_id=correlation_id,
        )
        return correlation_id
