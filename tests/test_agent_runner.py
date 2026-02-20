import asyncio
import itertools
import os
import time

import pytest
from dotenv import load_dotenv
from faststream.kafka import TestKafkaBroker

from calfkit._vendor.pydantic_ai import ModelResponse, models
from calfkit.broker.broker import BrokerClient
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.base_tool_node import agent_tool
from calfkit.nodes.chat_node import ChatNode
from calfkit.providers.pydantic_ai.openai import OpenAIModelClient
from calfkit.runners.service import NodesService
from calfkit.runners.service_client import RouterServiceClient
from calfkit.stores.in_memory import InMemoryMessageHistoryStore

load_dotenv()

# Ensure model requests are allowed for integration tests
models.ALLOW_MODEL_REQUESTS = True

# Skip integration tests if OpenAI API key is not available
skip_if_no_openai_key = pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"),
    reason="Skipping integration test: OPENAI_API_KEY not set in environment",
)

counter = itertools.count()


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
def deploy_broker() -> BrokerClient:
    # simulate the deployment pre-testing
    broker = BrokerClient()
    service = NodesService(broker)

    # 1. Deploy llm model node worker
    model_client = OpenAIModelClient("gpt-5-nano", reasoning_effort="low")
    chat_node = ChatNode(model_client)
    service.register_node(chat_node)

    # 2a. Deploy tool node worker for get_weather tool
    service.register_node(get_weather)

    # 2b. Deploy 2 tool node worker for get_temperature tool
    service.register_node(get_temperature, max_workers=2)

    # 3. Deploy router node worker
    router_node = AgentRouterNode(
        chat_node=ChatNode(),
        tool_nodes=[get_weather, get_temperature],
        message_history_store=InMemoryMessageHistoryStore(),
        system_prompt="Please always greet the user as Conan before every message",
    )
    service.register_node(router_node)

    return broker


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_agent(deploy_broker):
    broker = deploy_broker
    router_node = AgentRouterNode(
        chat_node=ChatNode(),
        tool_nodes=[get_weather],
    )
    async with TestKafkaBroker(broker) as _:
        print(f"\n\n{'=' * 10}Start{'=' * 10}")

        client = RouterServiceClient(broker, router_node)
        response = await client.request(user_prompt="Hey, what's the weather in Tokyo?")
        print(f"  Sent with correlation_id: {response.correlation_id[:8]}...")

        final_msg = await asyncio.wait_for(response.get_final_response(), timeout=30.0)
        assert isinstance(final_msg, ModelResponse)
        print(f"Text: {final_msg.text}")
        print(f"Tool calls: {final_msg.tool_calls}")

        print(f"{'=' * 10}End{'=' * 10}")


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_multi_turn_agent(deploy_broker):
    broker = deploy_broker
    router_node = AgentRouterNode(
        chat_node=ChatNode(),
        tool_nodes=[get_weather],
        system_prompt="Please speak like an insufferable gen z teenager in 2026. Your name is Jeff",
        # override the deployment system prompt
    )
    async with TestKafkaBroker(broker) as _:
        print(f"\n\n{'=' * 10}Start{'=' * 10}")
        thread_id = str(next(counter))

        # First turn
        client = RouterServiceClient(broker, router_node)
        response = await client.request(
            user_prompt="Hey, what's your name? My name is LeBron by the way.",
            thread_id=thread_id,
        )

        final_msg = await asyncio.wait_for(response.get_final_response(), timeout=20.0)
        assert isinstance(final_msg, ModelResponse)
        assert final_msg.text is not None
        print(f"{final_msg.text}")
        assert "jeff" in final_msg.text.lower()

        # Second turn
        response = await client.request(
            user_prompt="Do you know the weather in Tokyo right now?",
            thread_id=thread_id,
        )

        final_msg = await asyncio.wait_for(response.get_final_response(), timeout=20.0)
        assert isinstance(final_msg, ModelResponse)
        assert final_msg.text is not None
        print(f"{final_msg.text}")
        assert "rain" in final_msg.text.lower()

        # Third turn
        response = await client.request(
            user_prompt="Do you remember my name?",
            thread_id=thread_id,
        )

        final_msg = await asyncio.wait_for(response.get_final_response(), timeout=20.0)
        assert isinstance(final_msg, ModelResponse)
        assert final_msg.text is not None
        print(f"{final_msg.text}")
        assert "lebron" in final_msg.text.lower()

        print(f"{'=' * 10}End{'=' * 10}")


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_parallel_tool_calls(deploy_broker):
    broker = deploy_broker
    router_node = AgentRouterNode(
        chat_node=ChatNode(),
        tool_nodes=[get_weather, get_temperature],
    )
    async with TestKafkaBroker(broker) as _:
        print(f"\n\n{'=' * 10}Start{'=' * 10}")
        thread_id = str(next(counter))

        client = RouterServiceClient(broker, router_node)
        response = await client.request(
            user_prompt="Hey, what's the temperature in Detroit and San Diego right now?",
            thread_id=thread_id,
        )

        final_msg = await asyncio.wait_for(response.get_final_response(), timeout=30.0)
        assert isinstance(final_msg, ModelResponse)
        assert final_msg.text is not None
        print(f"{final_msg.text}")
        assert "detroit" in final_msg.text.lower()

        print(f"{'=' * 10}End{'=' * 10}")
