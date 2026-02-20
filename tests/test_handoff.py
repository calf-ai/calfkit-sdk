import asyncio
import itertools
import os

import pytest
from dotenv import load_dotenv
from faststream.kafka import TestKafkaBroker

from calfkit._vendor.pydantic_ai import ModelResponse, models
from calfkit.broker.broker import BrokerClient
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.base_tool_node import agent_tool
from calfkit.nodes.chat_node import ChatNode
from calfkit.prebuilt_agent_tools.handoff_tool import HandoffTool
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
        str: The current weather at the location
    """
    return f"It's raining heavily in {location}"


@pytest.fixture(scope="session")
def deploy_handoff_broker() -> tuple[BrokerClient, AgentRouterNode, AgentRouterNode]:
    broker = BrokerClient()
    service = NodesService(broker)

    # 1. Deploy LLM model node worker
    model_client = OpenAIModelClient("gpt-5-nano", reasoning_effort="low")
    chat_node = ChatNode(model_client)
    service.register_node(chat_node)

    # 2. Deploy tool node worker
    service.register_node(get_weather)

    # 3. Deploy Agent B (the specialist that gets delegated to)
    agent_b_router = AgentRouterNode(
        chat_node=ChatNode(),
        name="agent_b",
        tool_nodes=[get_weather],
        message_history_store=InMemoryMessageHistoryStore(),
        system_prompt=(
            "You are a weather specialist. When asked about weather, "
            "you can use your tool and report the results. Be concise."
        ),
    )
    service.register_node(agent_b_router, group_id="agent_b")

    # 4. Deploy HandoffTool scoped to Agent A
    handoff = HandoffTool(nodes=[agent_b_router])
    service.register_node(handoff)

    # 5. Deploy Agent A (the dispatcher that delegates)
    agent_a_router = AgentRouterNode(
        chat_node=ChatNode(),
        name="agent_a",
        tool_nodes=[handoff],
        message_history_store=InMemoryMessageHistoryStore(),
        system_prompt=(
            "You are a dispatcher agent. You do NOT answer questions yourself. "
            "You can use the handoff_tool to delegate questions or tasks to another AI agent. "
            "After receiving the delegation result, you should summarize the response for the user."
        ),
    )
    service.register_node(agent_a_router, group_id="agent_a")

    return broker, agent_a_router, agent_b_router


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_handoff_delegation(deploy_handoff_broker):
    """Test that Agent A delegates to Agent B via HandoffTool and receives a response."""
    broker, agent_a_router, _ = deploy_handoff_broker
    thread_id = str(next(counter))

    async with TestKafkaBroker(broker) as _:
        print(f"\n\n{'=' * 10}Start Handoff Test{'=' * 10}")

        client = RouterServiceClient(broker, agent_a_router)
        response = await client.request(
            user_prompt="What's the weather in Tokyo?",
            thread_id=thread_id,
        )
        print(f"  Sent with correlation_id: {response.correlation_id[:8]}...")

        final_msg = await asyncio.wait_for(response.get_final_response(), timeout=60.0)
        assert isinstance(final_msg, ModelResponse)
        assert final_msg.text is not None
        print(f"Text: {final_msg.text}")
        # Agent B should have used get_weather which returns "raining heavily"
        assert "rain" in final_msg.text.lower()

        print(f"{'=' * 10}End{'=' * 10}")
