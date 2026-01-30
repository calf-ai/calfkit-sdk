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
from calf.agents.chat_runner import ChatRunner
from calf.agents.tool_runner import ToolRunner
from calf.broker.broker import Broker
from calf.models.event_envelope import EventEnvelope
from calf.nodes.agent_router_node import AgentRouterNode
from calf.nodes.base_tool_node import agent_tool
from calf.nodes.chat_node import ChatNode
from calf.providers.pydantic_ai.openai import OpenAIModelClient

load_dotenv()

counter = itertools.count()
store: dict[str, asyncio.Queue[EventEnvelope]] = {}
condition = asyncio.Condition()

collect_topic = "collect"


@agent_tool
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
    model_client = OpenAIModelClient("gpt-5-nano", reasoning_effort="low")
    chat_node = ChatNode(model_client)
    chat_runner = ChatRunner(node=chat_node)
    chat_runner.register_on(broker)
    # if we're just deploying chat as an isolated deployment:
    #   await broker.run_app()

    # 2. Deploy tool node worker
    tool_runner = ToolRunner(get_weather)
    tool_runner.register_on(broker)
    # if we're just deploying this tool as an isolated deployment:
    #   await broker.run_app()

    # 3. Deploy router node worker
    router_node = AgentRouterNode(chat_node=ChatNode(), tool_nodes=[get_weather])
    router = AgentRouterRunner(node=router_node)
    router.register_on(broker)
    # if we're just deploying this router as an isolated deployment:
    #   await broker.run_app()

    @broker.subscriber(router_node.publish_to_topic or "default_collect")
    def gather(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in store:
            store[correlation_id] = asyncio.Queue()
        store[correlation_id].put_nowait(event_envelope)

    # await broker.run_app()
    return broker, router


@pytest.mark.asyncio
async def test_agent(deploy_broker):
    broker, _ = deploy_broker
    router_node = AgentRouterNode(
        chat_node=ChatNode(),
        tool_nodes=[get_weather],
        system_prompt="Please always greet the user as Conan before every message",
    )
    async with TestKafkaBroker(broker) as _:
        print(f"\n\n{'=' * 10}Start{'=' * 10}")

        trace_id = str(next(counter))

        await router_node.invoke(
            "Hey, what's the weather in Tokyo?", broker=broker, correlation_id=trace_id
        )

        await asyncio.wait_for(condition.wait_for(lambda: trace_id in store), timeout=20.0)
        queue = store[trace_id]
        while True:
            result_envelope = await queue.get()
            if isinstance(result_envelope.latest_message_in_history, ModelResponse):
                print(f"{result_envelope.latest_message_in_history.text}")
            else:
                print(f"{result_envelope}")
            print("|")
            if result_envelope.kind == "ai_response":
                break
        print(f"{'=' * 10}End{'=' * 10}")
