import asyncio
import itertools
from typing import Annotated

import pytest
from dotenv import load_dotenv
from faststream import Context
from faststream.kafka import TestKafkaBroker
from pydantic_ai import ModelResponse
from pydantic_ai.messages import ModelRequest

from calf.agents.chat_runner import ChatRunner
from calf.broker.broker import Broker
from calf.models.event_envelope import EventEnvelope
from calf.nodes.chat_node import ChatNode
from calf.providers.pydantic_ai.openai import OpenAIModelClient

load_dotenv()

counter = itertools.count()
store: dict[str, EventEnvelope] = {}
condition = asyncio.Condition()


@pytest.fixture(scope="session")
def deploy_broker() -> Broker:
    # simulate the deployment pre-testing
    model_client = OpenAIModelClient("gpt-5-nano")
    chat_runner = ChatRunner(model_client)
    broker = Broker()
    chat_runner.register_on(broker)

    @broker.subscriber(ChatNode.get_post_to_topic())
    def gather(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        store[correlation_id] = event_envelope

    # await broker.run_app()
    return broker


@pytest.mark.asyncio
async def test_simple_chat(deploy_broker: Broker):
    async with TestKafkaBroker(deploy_broker) as br:
        print(f"\n\n{'=' * 10}Start{'=' * 10}")

        trace_id = str(next(counter))

        await br.publish(
            EventEnvelope(
                kind="user_prompt",
                trace_id=trace_id,
                message_history=[ModelRequest.user_text_prompt("Hi, what's your name?")],
            ),
            topic=ChatNode.get_on_enter_topic(),
            correlation_id=trace_id,
        )

        await asyncio.wait_for(condition.wait_for(lambda: trace_id in store), timeout=10.0)
        result_envelope = store[trace_id]
        print("Result received")
        assert isinstance(result_envelope.latest_message_in_history, ModelResponse)
        print(f"Response: {result_envelope.latest_message_in_history.text}")
        print(f"{'=' * 10}End{'=' * 10}")
