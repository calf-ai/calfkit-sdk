from typing import TypeVar

from calf.atomic_node import BaseAtomicNode, on, post_to
from calf.context.schema import EventContext
from calf.providers.base import MessageT, ProviderClient, ResponseT, ToolT
from calf.providers.openai.adaptor import OpenAIClientMessage
from calf.types import UNSET, UnsetType


class Chat(BaseAtomicNode):
    DEFAULT_INPUT_TOPIC = "calf_chat_input_topic"
    DEFAULT_OUTPUT_TOPIC = "calf_chat_output_topic"

    def __init__(self, model_client: ProviderClient, name: str = "default_chat"):
        self.model_client = model_client
        self.name = name
        super().__init__()

    @on(DEFAULT_INPUT_TOPIC)
    @post_to(DEFAULT_OUTPUT_TOPIC)
    async def _handler(
        self, ctx: EventContext
    ):  # TODO: implement the handler to accept a schema contract, maybe use pydantic ai's contract
        # private method. Not meant for users/clients to call.

        msgs = [self.model_client.create_user_message(ctx.text)]

        response = await self.model_client.generate(messages=msgs)
        return EventContext(text=response.text)

    @classmethod
    async def invoke(cls, prompt: str, trace_id: str | None = None):
        # public method meant for client to publish to topic and start the node
        await cls.calf.publish(
            EventContext(text=prompt, trace_id=trace_id),
            topic=cls.DEFAULT_INPUT_TOPIC,
            correlation_id=trace_id,
        )

    @classmethod
    def invoke_topic(cls) -> str:
        return cls.DEFAULT_INPUT_TOPIC

    # TODO: alternatively, implement a 'runner' object that takes a class of BaseAtomicNode, and when it runs, it pubs to the default topic of the node.
