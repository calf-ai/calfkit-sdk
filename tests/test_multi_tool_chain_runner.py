"""Integration tests for multi-tool chaining with sequential dependencies.

Tests verify that an agent can use the result of one tool call as input to
a subsequent tool call, confirming memory/context retention between tool
invocations across the full pipeline.
"""

import asyncio
import os

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

models.ALLOW_MODEL_REQUESTS = True

skip_if_no_openai_key = pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"),
    reason="Skipping integration test: OPENAI_API_KEY not set in environment",
)


# --- Tools ---


@agent_tool
def lookup_employee_id(employee_name: str) -> str:
    """Look up an employee's unique ID by their full name.

    Args:
        employee_name (str): The full name of the employee (e.g. John Smith)

    Returns:
        str: The employee's unique identifier (e.g. EMP-48291)
    """
    return f"The employee ID for {employee_name} is EMP-48291"


@agent_tool
def get_employee_salary(employee_id: str) -> str:
    """Get the annual salary for an employee given their unique employee ID.

    Args:
        employee_id (str): The unique employee identifier (e.g. EMP-48291)

    Returns:
        str: The employee's annual salary (e.g. $95,000/year)
    """
    if "48291" in employee_id:
        return f"The annual salary for employee {employee_id} is $95,000/year"
    return f"No salary record found for employee {employee_id}"


# --- Fixtures ---


@pytest.fixture(scope="session")
def deploy_chain_broker() -> BrokerClient:
    """Deploy all nodes needed for multi-tool chain integration tests."""
    broker = BrokerClient()
    service = NodesService(broker)

    model_client = OpenAIModelClient(
        os.environ["TEST_LLM_MODEL_NAME"], reasoning_effort=os.getenv("TEST_REASONING_EFFORT")
    )
    service.register_node(ChatNode(model_client))

    service.register_node(lookup_employee_id)
    service.register_node(get_employee_salary)

    service.register_node(
        AgentRouterNode(
            chat_node=ChatNode(),
            tool_nodes=[lookup_employee_id, get_employee_salary],
            name="chain_agent",
            input_topic="chain.agent.input",
            system_prompt=(
                "You are an HR assistant. When asked about an employee's salary, "
                "you MUST first look up their employee ID using their name, then "
                "use that ID to retrieve their salary. Always report the exact "
                "salary figure from the tool."
            ),
            message_history_store=InMemoryMessageHistoryStore(),
        )
    )

    return broker


# --- Tests ---


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_sequential_tool_chain(deploy_chain_broker):
    """Agent must chain two tool calls: lookup ID by name, then get salary by ID.

    Verifies that the agent retains the intermediate result (employee ID) from
    the first tool call and correctly passes it to the second tool call.
    """
    broker = deploy_chain_broker

    router_node = AgentRouterNode(
        chat_node=ChatNode(),
        tool_nodes=[lookup_employee_id, get_employee_salary],
        name="chain_agent",
        input_topic="chain.agent.input",
    )

    async with TestKafkaBroker(broker) as _:
        client = RouterServiceClient(broker, router_node)
        response = await client.request(
            user_prompt="What is John Smith's annual salary?",
        )

        final_msg = await asyncio.wait_for(response.get_final_response(), timeout=30.0)
        assert isinstance(final_msg, ModelResponse)
        assert final_msg.text is not None
        assert "95,000" in final_msg.text or "95000" in final_msg.text, (
            f"Expected salary figure in response, got: {final_msg.text}"
        )

        print(f"Response: {final_msg.text}")
