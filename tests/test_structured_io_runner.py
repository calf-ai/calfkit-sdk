"""Integration tests for structured input/output support using real OpenAI API calls.

Tests verify the full pipeline: client → router → chat node → router → client
with typed Pydantic models as input payloads and structured output results.

Each router uses a unique input_topic to avoid TestKafkaBroker consumer group
isolation limitations (all subscribers on the same topic receive every message).
"""

import asyncio
import os
import sys

import pytest
from dotenv import load_dotenv
from faststream.kafka import TestKafkaBroker
from pydantic import BaseModel

from calfkit._vendor.pydantic_ai import ModelResponse, models
from calfkit.broker.broker import BrokerClient
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.base_tool_node import agent_tool
from calfkit.nodes.chat_node import ChatNode
from calfkit.providers.pydantic_ai.openai import OpenAIModelClient
from calfkit.runners.service import NodesService
from calfkit.runners.service_client import RouterServiceClient
from calfkit.stores.in_memory import InMemoryMessageHistoryStore

# TestKafkaBroker dispatches synchronously (recursive call stack), so the
# tool-routing round-trips in structured-output-with-tools can exceed the
# default limit. Same workaround as test_groupchat_runner.py.
sys.setrecursionlimit(10000)

load_dotenv()

models.ALLOW_MODEL_REQUESTS = True

skip_if_no_openai_key = pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"),
    reason="Skipping integration test: OPENAI_API_KEY not set in environment",
)


# --- Structured models ---


class CityInfo(BaseModel):
    """Extracted city information."""

    city_name: str
    country: str
    known_for: str


class MathResult(BaseModel):
    """Result of a math evaluation."""

    expression: str
    result: float


class ReviewRequest(BaseModel):
    """Structured input for a review task."""

    product_name: str
    category: str


class ReviewResult(BaseModel):
    """Structured output from a review task."""

    product_name: str
    rating: int
    summary: str


# --- Tools ---

# Pre-defined safe operations for the calculate tool
_SAFE_OPS: dict[str, float] = {
    "17 * 23 + 5": 396.0,
    "17*23+5": 396.0,
}


@agent_tool
def calculate(expression: str) -> str:
    """Evaluate a simple math expression and return the result.

    Args:
        expression: A math expression to evaluate (e.g. "2 + 3 * 4")

    Returns:
        str: The result of the expression
    """
    # Normalize whitespace for lookup
    normalized = " ".join(expression.split())
    for key, value in _SAFE_OPS.items():
        if normalized == key or expression.strip() == key:
            return f"The result of {expression} is {value}"
    return f"Cannot evaluate expression: {expression}"


# --- Fixtures ---


@pytest.fixture(scope="session")
def deploy_structured_broker() -> BrokerClient:
    """Deploy all nodes needed for structured I/O integration tests.

    Each router gets a unique input_topic to avoid TestKafkaBroker delivering
    the same message to multiple routers (no consumer group isolation in test mode).
    """
    broker = BrokerClient()
    service = NodesService(broker)

    model_client = OpenAIModelClient(os.environ["TEST_LLM_MODEL_NAME"], reasoning_effort=os.getenv("TEST_REASONING_EFFORT"))

    # Deploy shared chat nodes (one plain, one with each output_type)
    service.register_node(ChatNode(model_client))
    service.register_node(ChatNode(model_client, name="city_chat", output_type=CityInfo))
    service.register_node(ChatNode(model_client, name="math_chat", output_type=MathResult))
    service.register_node(ChatNode(model_client, name="review_chat", output_type=ReviewResult))

    # Deploy tool nodes
    service.register_node(calculate)

    # Deploy router: structured output only (CityInfo)
    service.register_node(
        AgentRouterNode(
            chat_node=ChatNode(name="city_chat", output_type=CityInfo),
            name="city_agent",
            input_topic="structured.city.input",
            system_prompt=(
                "You are a geography expert. Extract city information from the user's message. "
                "Always provide the city name, country, and what it is known for."
            ),
            message_history_store=InMemoryMessageHistoryStore(),
        )
    )

    # Deploy router: tools + structured output (MathResult)
    service.register_node(
        AgentRouterNode(
            chat_node=ChatNode(name="math_chat", output_type=MathResult),
            tool_nodes=[calculate],
            name="math_agent",
            input_topic="structured.math.input",
            system_prompt=("You are a math assistant. Use the calculate tool to evaluate expressions, then return the structured result."),
            message_history_store=InMemoryMessageHistoryStore(),
        )
    )

    # Deploy router: structured input + structured output (ReviewRequest → ReviewResult)
    service.register_node(
        AgentRouterNode(
            chat_node=ChatNode(name="review_chat", output_type=ReviewResult),
            name="review_agent",
            input_topic="structured.review.input",
            deps_type=ReviewRequest,
            system_prompt=(
                "You are a product reviewer. You receive a product name and category as structured "
                "input via the deps/context. Write a brief, single-sentence review and give a "
                "rating from 1 to 5. The product_name in the output must exactly match the input."
            ),
            message_history_store=InMemoryMessageHistoryStore(),
        )
    )

    # Deploy router: structured input, plain text output
    service.register_node(
        AgentRouterNode(
            chat_node=ChatNode(),
            name="input_only_agent",
            input_topic="structured.inputonly.input",
            deps_type=ReviewRequest,
            system_prompt=("You receive structured product data as context. Respond with a plain text summary mentioning the product name."),
        )
    )

    # Deploy router: backward compat (no structured I/O)
    service.register_node(
        AgentRouterNode(
            chat_node=ChatNode(),
            name="compat_agent",
            input_topic="structured.compat.input",
            system_prompt="You are a helpful assistant. Keep responses brief.",
        )
    )

    return broker


# --- Tests ---


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_structured_output(deploy_structured_broker):
    """Agent with output_type=CityInfo returns structured payload via get_output()."""
    broker = deploy_structured_broker

    router_node = AgentRouterNode(
        chat_node=ChatNode(name="city_chat", output_type=CityInfo),
        name="city_agent",
        input_topic="structured.city.input",
    )

    async with TestKafkaBroker(broker) as _:
        client = RouterServiceClient(broker, router_node)
        response = await client.request(
            user_prompt="Tell me about Paris, the capital of France.",
        )

        output = await asyncio.wait_for(response.get_output(), timeout=30.0)
        assert output is not None, "Expected structured output but got None"
        assert "paris" in output["city_name"].lower()
        assert "france" in output["country"].lower()
        assert isinstance(output["known_for"], str)
        assert len(output["known_for"]) > 0

        print(f"CityInfo output: {output}")


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_structured_output_with_tools(deploy_structured_broker):
    """Agent uses calculate tool, then returns MathResult structured output."""
    broker = deploy_structured_broker

    router_node = AgentRouterNode(
        chat_node=ChatNode(name="math_chat", output_type=MathResult),
        tool_nodes=[calculate],
        name="math_agent",
        input_topic="structured.math.input",
    )

    async with TestKafkaBroker(broker) as _:
        client = RouterServiceClient(broker, router_node)
        response = await client.request(
            user_prompt="What is 17 * 23 + 5?",
        )

        output = await asyncio.wait_for(response.get_output(), timeout=30.0)
        assert output is not None, "Expected structured output but got None"
        assert output["result"] == 396.0
        assert isinstance(output["expression"], str)

        print(f"MathResult output: {output}")


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_structured_input_and_output(deploy_structured_broker):
    """Client sends ReviewRequest payload, agent returns ReviewResult."""
    broker = deploy_structured_broker

    router_node = AgentRouterNode(
        chat_node=ChatNode(name="review_chat", output_type=ReviewResult),
        name="review_agent",
        input_topic="structured.review.input",
        deps_type=ReviewRequest,
    )

    async with TestKafkaBroker(broker) as _:
        client = RouterServiceClient(broker, router_node)
        response = await client.request(
            user_prompt=("Review the following product. Product name: Thunderbolt Cable. Category: Electronics."),
            deps={"product_name": "Thunderbolt Cable", "category": "Electronics"},
        )

        output = await asyncio.wait_for(response.get_output(), timeout=30.0)
        assert output is not None, "Expected structured output but got None"
        assert "thunderbolt" in output["product_name"].lower()
        assert 1 <= output["rating"] <= 5
        assert isinstance(output["summary"], str)
        assert len(output["summary"]) > 0

        print(f"ReviewResult output: {output}")


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_structured_input_text_output(deploy_structured_broker):
    """Client sends structured input payload, agent returns plain text (no output_type)."""
    broker = deploy_structured_broker

    router_node = AgentRouterNode(
        chat_node=ChatNode(),
        name="input_only_agent",
        input_topic="structured.inputonly.input",
        deps_type=ReviewRequest,
    )

    async with TestKafkaBroker(broker) as _:
        client = RouterServiceClient(broker, router_node)
        response = await client.request(
            user_prompt="Summarize this product: Wireless Mouse, category Peripherals.",
            deps={"product_name": "Wireless Mouse", "category": "Peripherals"},
        )

        final_msg = await asyncio.wait_for(response.get_final_response(), timeout=30.0)
        assert isinstance(final_msg, ModelResponse)
        assert final_msg.text is not None

        # Text output goes on payload — str is an output type
        output = await response.get_output()
        assert isinstance(output, str)
        assert len(output) > 0

        print(f"Text response: {output}")


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_backward_compat_no_structured_io(deploy_structured_broker):
    """Agent without output_type or input_type works exactly as before."""
    broker = deploy_structured_broker

    router_node = AgentRouterNode(
        chat_node=ChatNode(),
        name="compat_agent",
        input_topic="structured.compat.input",
    )

    async with TestKafkaBroker(broker) as _:
        client = RouterServiceClient(broker, router_node)
        response = await client.request(
            user_prompt="Say hello in exactly three words.",
        )

        final_msg = await asyncio.wait_for(response.get_final_response(), timeout=30.0)
        assert isinstance(final_msg, ModelResponse)
        assert final_msg.text is not None
        assert len(final_msg.text) > 0

        # Text output goes on payload — str is an output type
        output = await response.get_output()
        assert isinstance(output, str)
        assert len(output) > 0

        print(f"Plain text: {output}")
