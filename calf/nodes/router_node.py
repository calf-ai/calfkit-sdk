from typing import Annotated, Awaitable, Callable

from faststream import Context
from pydantic_ai import ModelRequest, ModelResponse

from calf.broker.broker import Broker
from calf.models.event_envelope import EventEnvelope
from calf.models.types import ToolCallRequest
from calf.nodes.base_node import BaseNode
from calf.nodes.base_tool_node import BaseToolNode


class AgentRouterNode(BaseNode):
    def __init__(
        self,
        chat_node: BaseNode,
        tool_nodes: list[BaseToolNode],
        reply_to_topic: str,
        *args,
        **kwargs,
    ):
        self.chat_node = chat_node
        self.tool_nodes = tool_nodes
        self.tool_response_topics = [tool.get_post_to_topic() for tool in self.tool_nodes]
        self.reply_to_topic = (
            reply_to_topic  # TODO: allow for dynamic reply_to. Provided at request time.
        )
        self.topic_to_tool_registry = {
            tool.tool_schema().name: tool.get_on_enter_topic() for tool in self.tool_nodes
        }
        super().__init__(*args, **kwargs)

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
            if ctx.latest_message_in_history is None:
                raise RuntimeError("The latest message is None")
            if isinstance(ctx.latest_message_in_history, ModelResponse):
                if (
                    ctx.latest_message_in_history.finish_reason == "tool_call"
                    or ctx.latest_message_in_history.tool_calls
                ):
                    for tool_call in ctx.latest_message_in_history.tool_calls:
                        await self._route_tool(tool_call, correlation_id, broker)
                else:
                    # reply to sender here
                    await self._reply_to_sender(
                        ctx.latest_message_in_history, correlation_id, broker
                    )
            else:
                # tool call result block
                await self._call_model(ctx.latest_message_in_history, correlation_id, broker)

        if gather_func is None:
            gather_func = gather_response

        for topic in self.tool_response_topics:
            gather_func = broker.subscriber(topic)(gather_func)
        gather_func = broker.subscriber(self.chat_node.get_post_to_topic())(gather_func)

    async def _route_tool(
        self, generated_tool_call: ToolCallRequest, correlation_id: str, broker: Broker
    ) -> None:
        tool_topic = self.topic_to_tool_registry.get(generated_tool_call.tool_name)
        if tool_topic is None:
            # TODO: implement a short circuit to respond with an error message for when provided tool does not exist.
            return

        await broker.publish(
            EventEnvelope(
                kind="tool_call_request",
                trace_id=correlation_id,
                tool_call_request=generated_tool_call,
            ),
            topic=tool_topic,
            correlation_id=correlation_id,
        )

    async def _reply_to_sender(
        self, ai_response: ModelResponse, correlation_id: str, broker: Broker
    ) -> None:
        await broker.publish(
            EventEnvelope(
                kind="ai_response",
                trace_id=correlation_id,
                latest_message=ai_response,
            ),
            topic=self.reply_to_topic,
            correlation_id=correlation_id,
        )

    async def _call_model(
        self, tool_result: ModelRequest, correlation_id: str, broker: Broker
    ) -> None:
        await broker.publish(
            EventEnvelope(
                kind="tool_result",
                trace_id=correlation_id,
                latest_message=tool_result,
            ),
            topic=self.chat_node.get_on_enter_topic(),
            correlation_id=correlation_id,
        )
