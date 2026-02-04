import asyncio
import itertools
import os
import time
from typing import Annotated

import pytest
from dotenv import load_dotenv
from faststream import Context
from faststream.kafka import TestKafkaBroker
from pydantic_ai import ModelResponse

from calfkit.broker.broker import Broker
from calfkit.models.event_envelope import EventEnvelope
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.base_tool_node import agent_tool
from calfkit.nodes.chat_node import ChatNode
from calfkit.providers.pydantic_ai.openai import OpenAIModelClient
from calfkit.runners.node_runner import AgentRouterRunner, ChatRunner, ToolRunner
from calfkit.stores.in_memory import InMemoryMessageHistoryStore
from tests.utils import wait_for_condition

load_dotenv()

# Skip integration tests if OpenAI API key is not available
skip_if_no_openai_key = pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"),
    reason="Skipping integration test: OPENAI_API_KEY not set in environment",
)

counter = itertools.count()
store: dict[str, asyncio.Queue[EventEnvelope]] = {}
final_resp_store: dict[str, asyncio.Queue[EventEnvelope]] = {}


@agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location

    Args:
        location (str): The name of the location (e.g. Irvine, CA)

    Returns:
        str: The current weather at the location (e.g. It's currently dry and sunny in Irvine, CA)
    """
    return f"It's raining heavily in {location}"


@agent_tool
async def get_temperature(location: str) -> str:
    """Get the current temperature at a location

    Args:
        location (str): The name of the location (e.g. Brookline, MA)

    Returns:
        str: The current temperature at the location (e.g. It's currently 95°F in Brookline, MA)
    """
    print(f"Call for {location} at {time.perf_counter()}")
    await asyncio.sleep(10.0)
    print(f"Result for {location} at {time.perf_counter()}")
    return f"It's -4.5°F in {location}"


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

    # 2a. Deploy tool node worker for get_weather tool
    tool_runner_weather = ToolRunner(get_weather)
    tool_runner_weather.register_on(broker)
    # if we're just deploying this tool as an isolated deployment:
    #   await broker.run_app()

    # 2b. Deploy 2 tool node worker for get_temperature tool
    tool_runner_temp = ToolRunner(get_temperature)
    tool_runner_temp.register_on(broker, max_workers=2)
    # if we're just deploying this tool as an isolated deployment:
    #   await broker.run_app()

    # 3. Deploy router node worker
    router_node = AgentRouterNode(
        chat_node=ChatNode(),
        tool_nodes=[get_weather, get_temperature],
        message_history_store=InMemoryMessageHistoryStore(),
        system_prompt="Please always greet the user as Conan before every message",
    )
    router = AgentRouterRunner(node=router_node)
    router.register_on(broker)
    # if we're just deploying this router as an isolated deployment:
    #   await broker.run_app()

    @broker.subscriber(router_node.publish_to_topic or "default_collect")
    def trace(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in store:
            store[correlation_id] = asyncio.Queue()
        store[correlation_id].put_nowait(event_envelope)

    @broker.subscriber("final_response")
    def gather(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in final_resp_store:
            final_resp_store[correlation_id] = asyncio.Queue()
        final_resp_store[correlation_id].put_nowait(event_envelope)

    # await broker.run_app()
    return broker, router


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_agent(deploy_broker):
    broker, _ = deploy_broker
    router_node = AgentRouterNode(
        chat_node=ChatNode(),
        tool_nodes=[get_weather],
    )
    async with TestKafkaBroker(broker) as _:
        print(f"\n\n{'=' * 10}Start{'=' * 10}")

        trace_id = await router_node.invoke(
            "Hey, what's the weather in Tokyo?",
            broker=broker,
            final_response_topic="final_response",
        )

        queue = store[trace_id]
        while not queue.empty():
            result_envelope = queue.get_nowait()
            if isinstance(result_envelope.latest_message_in_history, ModelResponse):
                print(f"Text: {result_envelope.latest_message_in_history.text}")
                print(f"Tool calls: {result_envelope.latest_message_in_history.tool_calls}")
            else:
                print(f"{result_envelope}")
            print("|")

        print(f"{'=' * 10}End{'=' * 10}")


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_multi_turn_agent(deploy_broker):
    broker, _ = deploy_broker
    router_node = AgentRouterNode(
        chat_node=ChatNode(),
        tool_nodes=[get_weather],
        system_prompt="Please speak like an insufferable gen z teenager in 2026",
        # override the deployment system prompt
    )
    async with TestKafkaBroker(broker) as _:
        print(f"\n\n{'=' * 10}Start{'=' * 10}")
        thread_id = str(next(counter))

        trace_id = await router_node.invoke(
            "Hey, what's your name? My name is LeBron by the way.",
            broker=broker,
            thread_id=thread_id,
            final_response_topic="final_response",
        )

        await wait_for_condition(lambda: trace_id in final_resp_store, timeout=20.0)
        queue = final_resp_store[trace_id]
        while True:
            result_envelope = await queue.get()
            if isinstance(result_envelope.latest_message_in_history, ModelResponse):
                print(f"{result_envelope.latest_message_in_history.text}")
            else:
                print(f"{result_envelope}")
            print("|")
            if result_envelope.final_response:
                assert isinstance(result_envelope.latest_message_in_history, ModelResponse)
                assert result_envelope.latest_message_in_history.text is not None
                assert "gpt" in result_envelope.latest_message_in_history.text.lower()
                break

        trace_id = await router_node.invoke(
            "Do you know the weather in Tokyo right now?",
            broker=broker,
            thread_id=thread_id,
            final_response_topic="final_response",
        )

        await wait_for_condition(lambda: trace_id in final_resp_store, timeout=20.0)
        queue = final_resp_store[trace_id]
        while True:
            result_envelope = await queue.get()
            if isinstance(result_envelope.latest_message_in_history, ModelResponse):
                print(f"{result_envelope.latest_message_in_history.text}")
            else:
                print(f"{result_envelope}")
            print("|")
            if result_envelope.final_response:
                assert isinstance(result_envelope.latest_message_in_history, ModelResponse)
                assert result_envelope.latest_message_in_history.text is not None
                assert "snow" in result_envelope.latest_message_in_history.text.lower()
                break

        trace_id = await router_node.invoke(
            "Do you remember my name?",
            broker=broker,
            thread_id=thread_id,
            final_response_topic="final_response",
        )
        await wait_for_condition(lambda: trace_id in final_resp_store, timeout=20.0)
        queue = final_resp_store[trace_id]
        while True:
            result_envelope = await queue.get()
            if isinstance(result_envelope.latest_message_in_history, ModelResponse):
                print(f"{result_envelope.latest_message_in_history.text}")
            else:
                print(f"{result_envelope}")
            print("|")
            if result_envelope.final_response:
                assert isinstance(result_envelope.latest_message_in_history, ModelResponse)
                assert result_envelope.latest_message_in_history.text is not None
                assert "lebron" in result_envelope.latest_message_in_history.text.lower()
                break
        print(f"{'=' * 10}End{'=' * 10}")


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_parallel_tool_calls(deploy_broker):
    broker, _ = deploy_broker
    router_node = AgentRouterNode(
        chat_node=ChatNode(),
        tool_nodes=[get_weather, get_temperature],
    )
    async with TestKafkaBroker(broker) as _:
        print(f"\n\n{'=' * 10}Start{'=' * 10}")
        thread_id = str(next(counter))

        trace_id = await asyncio.wait_for(
            router_node.invoke(
                "Hey, what's the temperature in Detroit and San Diego right now?",
                broker=broker,
                thread_id=thread_id,
                final_response_topic="final_response",
            ),
            timeout=15.0,
        )

        queue = store[trace_id]
        while True:
            result_envelope = await queue.get()
            if isinstance(result_envelope.latest_message_in_history, ModelResponse):
                print(f"{result_envelope.latest_message_in_history.text}")
            else:
                print(f"{result_envelope}")
            print("|")
            if result_envelope.final_response:
                assert isinstance(result_envelope.latest_message_in_history, ModelResponse)
                assert result_envelope.latest_message_in_history.text is not None
                assert "detroit" in result_envelope.latest_message_in_history.text.lower()
                break

        print(f"{'=' * 10}End{'=' * 10}")
