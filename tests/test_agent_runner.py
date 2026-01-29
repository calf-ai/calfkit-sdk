import asyncio
import itertools
from typing import Annotated

import pytest
from dotenv import load_dotenv
from faststream import Context
from faststream.kafka import TestKafkaBroker
from pydantic_ai import ModelResponse
from pydantic_ai.messages import ModelRequest

from calf.agents.agent_router_runner import AgentRouterRunner
from calf.agents.base_node_runner import BaseNodeRunner
from calf.agents.chat_runner import ChatRunner
from calf.agents.tool_runner import ToolRunner
from calf.broker.broker import Broker
from calf.models.event_envelope import EventEnvelope
from calf.nodes.base_tool_node import function_tool
from calf.nodes.chat_node import ChatNode
from calf.providers.pydantic_ai.openai import OpenAIModelClient

load_dotenv()

counter = itertools.count()
store: dict[str, EventEnvelope] = {}
condition = asyncio.Condition()

collect_topic = "collect"


@function_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location

    Args:
        location (str): The name of the location (e.g. Irvine, CA)

    Returns:
        str: The current weather at the location (e.g. It's currently dry and sunny in Irvine, CA)
    """
    return f"It's snowing heavily in {location}"


@pytest.fixture(scope="session")
def deploy_broker() -> tuple[Broker, AgentRouterRunner]:
    # simulate the deployment pre-testing
    broker = Broker()

    # 1. Deploy llm model node worker
    model_client = OpenAIModelClient("gpt-5-nano", reasoning_effort="medium")
    chat_node = ChatNode(model_client)
    chat_runner = ChatRunner(chat_node=chat_node)
    chat_runner.register_on(broker)
    # if we're just deploying chat as an isolated deployment:
    #   await broker.run_app()

    # 2. Deploy tool node worker
    tool_runner = ToolRunner(get_weather)
    tool_runner.register_on(broker)
    # if we're just deploying this tool as an isolated deployment:
    #   await broker.run_app()

    # 3. Deploy router node worker
    router = AgentRouterRunner(
        chat_node=chat_node, tool_nodes=[get_weather], reply_to_topic=collect_topic
    )
    router.register_on(broker)
    # if we're just deploying this router as an isolated deployment:
    #   await broker.run_app()

    @broker.subscriber(collect_topic)
    def gather(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        store[correlation_id] = event_envelope

    # await broker.run_app()
    return broker, router


@pytest.mark.asyncio
async def test_simple_chat(deploy_broker):
    broker, router = deploy_broker
    async with TestKafkaBroker(broker) as br:
        print(f"\n\n{'=' * 10}Start{'=' * 10}")

        trace_id = str(next(counter))

        msg = ModelRequest.user_text_prompt("Hi, what's the weather like at Tokyo?")
        await br.publish(
            EventEnvelope(
                kind="user_prompt",
                trace_id=trace_id,
                message_history=[msg],
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


@pytest.mark.asyncio
async def test_agent(deploy_broker):
    broker, router = deploy_broker
    async with TestKafkaBroker(broker) as br:
        print(f"\n\n{'=' * 10}Start{'=' * 10}")

        trace_id = str(next(counter))

        await router.invoke(
            ModelRequest.user_text_prompt("Hey, what's the weather in Tokyo?"),
            correlation_id=trace_id,
            broker=broker,
        )

        await asyncio.wait_for(condition.wait_for(lambda: trace_id in store), timeout=10.0)
        result_envelope = store[trace_id]
        print("Result received")
        assert isinstance(result_envelope.latest_message_in_history, ModelResponse)
        print(f"Response: {result_envelope.latest_message_in_history.text}")
        print(f"{'=' * 10}End{'=' * 10}")
