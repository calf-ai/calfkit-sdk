from typing import Annotated

from faststream import Context
from pydantic_ai import ModelRequest, ModelResponse

from calf.context.schema import EventContext
from calf.contracts.topics import Topics
from calf.nodes.atomic_node import BaseAtomicNode, on, on_default, post_to
from calf.runtime import CalfRuntime


class AgentRunner(BaseAtomicNode):
    def __init__(
        self,
        chat: type[BaseAtomicNode],
        tools: list[type[BaseAtomicNode]],
        handoffs: list[type[BaseAtomicNode]],
    ):
        self.chat = chat
        self.tools = tools
        self.handoffs = handoffs
        super().__init__()

    @classmethod
    async def run(cls, user_prompt: str, trace_id: str | None = None):
        await CalfRuntime.calf.publish(
            EventContext(
                trace_id=trace_id,
                latest_message=ModelRequest.user_text_prompt(user_prompt),
            ),
            topic=Topics.INVOKE,
            correlation_id=trace_id,
        )

    @on(Topics.AI_GENERATON_RESPONSE)
    async def gather_response(
        self,
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
            await self.calf.publish(
                EventContext(trace_id=correlation_id, latest_message=ctx.latest_message),
                topic=Topics.AI_TOOL_CALL,
                correlation_id=correlation_id,
            )
        else:
            print(f"Response text here: {ctx.latest_message.text}")
