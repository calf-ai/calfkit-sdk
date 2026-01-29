from typing import Annotated, Awaitable, Callable

from faststream import Context
from pydantic_ai import ModelRequest, ModelResponse
from pydantic_ai.models import ModelRequestParameters

from calf.broker.broker import Broker
from calf.models.event_envelope import EventEnvelope, ToolCallRequest
from calf.nodes.base_node import BaseNode
from calf.nodes.base_tool_node import BaseToolNode
from calf.nodes.registrator import Registrator


class AgentRouterRunner(Registrator):
    """Deployable unit orchestrating the internal routing to operate agents"""

    def __init__(
        self,
        chat_node: BaseNode,
        tool_nodes: list[BaseToolNode],
        reply_to_topic: str,
        handoff_node_classes: list[type[BaseNode]] = [],
    ):
        self.chat = chat_node
        self.tools = tool_nodes
        self.handoffs = handoff_node_classes
        self.reply_to_topic = reply_to_topic

        self.tools_topic_registry: dict[str, str] = {
            tool_class.tool_schema().name: tool_class.get_on_enter_topic()
            for tool_class in tool_nodes
        }

        self.tool_response_topics = [tool.get_post_to_topic() for tool in self.tools]

        super().__init__()

    def register_on(
        self,
        broker: Broker,
        *,
        gather_func: Callable | Callable[..., Awaitable] | None = None,
    ):
        async def gather_response(
            ctx: EventEnvelope,
            correlation_id: Annotated[str, Context()],
        ):
            if ctx.node_result_message is None:
                raise RuntimeError("There is no response message to process")

            # One place where message history is modified
            ctx.message_history.append(ctx.node_result_message)

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
                # tool call result block
                await self._call_model(ctx, correlation_id, broker)

        if gather_func is None:
            gather_func = gather_response

        for topic in self.tool_response_topics:
            gather_func = broker.subscriber(topic)(gather_func)
        gather_func = broker.subscriber(self.chat.get_post_to_topic())(gather_func)

    async def _route_tool(
        self,
        event_envelope: EventEnvelope,
        generated_tool_call: ToolCallRequest,
        correlation_id: str,
        broker: Broker,
    ) -> None:
        tool_topic = self.tools_topic_registry.get(generated_tool_call.tool_name)
        if tool_topic is None:
            # TODO: implement a short circuit to respond with an error message for when provided tool does not exist.
            return
        event_envelope = event_envelope.model_copy(
            update={"kind": "tool_call_request", "tool_call_request": generated_tool_call}
        )
        await broker.publish(
            event_envelope,
            topic=tool_topic,
            correlation_id=correlation_id,
        )

    async def _reply_to_sender(
        self, event_envelope: EventEnvelope, correlation_id: str, broker: Broker
    ) -> None:
        event_envelope = event_envelope.model_copy(update={"kind": "ai_response"})
        await broker.publish(
            event_envelope,
            topic=self.reply_to_topic,
            correlation_id=correlation_id,
        )

    async def _call_model(
        self,
        event_envelope: EventEnvelope,
        correlation_id: str,
        broker: Broker,
    ) -> None:
        patch_model_request_params = ModelRequestParameters(
            function_tools=[tool.tool_schema() for tool in self.tools]
        )
        event_envelope = event_envelope.model_copy(
            update={"kind": "tool_result", "patch_model_request_params": patch_model_request_params}
        )
        await broker.publish(
            event_envelope,
            topic=self.chat.get_on_enter_topic(),
            correlation_id=correlation_id,
        )

    async def invoke(self, request: ModelRequest, correlation_id: str, broker: Broker) -> None:
        patch_model_request_params = ModelRequestParameters(
            function_tools=[tool.tool_schema() for tool in self.tools]
        )
        await broker.publish(
            EventEnvelope(
                kind="user_prompt",
                trace_id=correlation_id,
                patch_model_request_params=patch_model_request_params,
                message_history=[request],
            ),
            topic=self.chat.get_on_enter_topic(),
            correlation_id=correlation_id,
        )
