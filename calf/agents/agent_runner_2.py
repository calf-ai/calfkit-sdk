from typing import Annotated

from faststream import Context
from faststream.kafka import KafkaBroker, KafkaPublisher, KafkaRoute, KafkaRouter
from pydantic_ai import ModelResponse

from calf.context.schema import EventContext
from calf.contracts.topics import Topics
from calf.nodes.base_node import BaseNode
from calf.runtime import CalfRuntime


class AgentRunner2:
    def __init__(
        self,
        chat: BaseNode,
        *,
        tools: list[BaseNode] = [],
        handoffs: list[BaseNode] = [],
        route_prefix: str,
        sep: str = ".",
    ):
        self.chat = chat
        self.tools = tools
        self.handoffs = handoffs
        self.route_prefix = route_prefix
        self.sep = sep

    def register(self):
        chat_handler = KafkaRoute(
            self.chat.on_enter,
            Topics.INVOKE,
            publishers=(KafkaPublisher(Topics.AI_GENERATON_RESPONSE),),
        )
        tool_handlers = [
            KafkaRoute(
                tool.on_enter,
                Topics.AI_TOOL_CALL,
                publishers=(KafkaPublisher(Topics.TOOL_RESPONSE),),
            )
            for tool in self.tools
        ]

        handlers = [chat_handler] + tool_handlers

        br = KafkaRouter(
            prefix=f"{self.route_prefix}{self.sep}",
            handlers=handlers,
        )
        self.br = br

        async def gather_response(
            ctx: EventContext,
            correlation_id: Annotated[str, Context()],
        ):
            if not isinstance(ctx.latest_message, ModelResponse):
                raise RuntimeError(
                    f"latext_message in event context object on event '{Topics.AI_GENERATON_RESPONSE}' was not ModelResponse type."
                )
            if ctx.latest_message.finish_reason == "tool_call" or ctx.latest_message.tool_calls:
                print(f"Tool call here: {ctx.latest_message.tool_calls}")
                print(f"Tool call thinking here: {ctx.latest_message.thinking}")
                print(f"Tool call text here: {ctx.latest_message.text}")
                await CalfRuntime.calf.publish(
                    EventContext(trace_id=correlation_id, latest_message=ctx.latest_message),
                    topic=Topics.AI_TOOL_CALL,
                    correlation_id=correlation_id,
                )
            else:
                print(f"Response text here: {ctx.latest_message.text}")

        response_handler = KafkaRoute(
            gather_response,
            self.chat.get_post_to_topic(),
            publishers=(KafkaPublisher(Topics.AI_GENERATON_RESPONSE),),
        )
