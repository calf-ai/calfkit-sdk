import asyncio
from typing import Annotated

import pytest
from faststream import Context
from faststream.kafka import TestKafkaBroker

from calfkit._vendor.pydantic_ai import ModelMessage, ModelResponse, TextPart, models
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.broker.broker import BrokerClient
from calfkit.models.event_envelope import EventEnvelope
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.chat_node import ChatNode
from calfkit.runners.service import NodesService
from tests.utils import wait_for_condition


@pytest.fixture(autouse=True)
def block_model_requests():
    """Block actual model requests during unit tests."""
    original_value = models.ALLOW_MODEL_REQUESTS
    models.ALLOW_MODEL_REQUESTS = False
    yield
    models.ALLOW_MODEL_REQUESTS = original_value


# --- Unit tests: topic wiring ---


def test_default_topics_unchanged():
    """Default AgentRouterNode (no overrides) keeps hardcoded topics."""
    node = AgentRouterNode()
    assert node.subscribed_topic == "agent_router.input"
    assert node.publish_to_topic == "agent_router.output"

    for topics in node.bound_registry.values():
        if "shared_subscribe_topic" in topics:
            assert topics["shared_subscribe_topic"] == "agent_router.input"
            assert "agent_router.input" in topics["subscribe_topics"]
        if "publish_topic" in topics:
            assert topics["publish_topic"] == "agent_router.output"


def test_default_topics_unchanged_named():
    """Named node without overrides keeps defaults + entrypoint."""
    node = AgentRouterNode(name="my_router")
    assert node.subscribed_topic == "agent_router.input"
    assert node.publish_to_topic == "agent_router.output"
    assert node.entrypoint_topic == "agent_router.private.my_router"

    # Entrypoint should be in subscribe_topics alongside the shared topic
    for topics in node.bound_registry.values():
        if "shared_subscribe_topic" in topics:
            assert "agent_router.input" in topics["subscribe_topics"]
            assert "agent_router.private.my_router" in topics["subscribe_topics"]


def test_single_input_topic_override():
    """Single string input_topic overrides subscribed_topic and bound_registry."""
    node = AgentRouterNode(input_topic="custom.input")
    assert node.subscribed_topic == "custom.input"
    assert node.publish_to_topic == "agent_router.output"  # unchanged

    for topics in node.bound_registry.values():
        if "shared_subscribe_topic" in topics:
            assert topics["shared_subscribe_topic"] == "custom.input"
            assert "custom.input" in topics["subscribe_topics"]
            assert "agent_router.input" not in topics["subscribe_topics"]


def test_multiple_input_topics_override():
    """List input_topic: subscribed_topic returns first; subscribe_topics has all."""
    node = AgentRouterNode(input_topic=["upstream.a", "upstream.b", "upstream.c"])
    assert node.subscribed_topic == "upstream.a"

    for topics in node.bound_registry.values():
        if "shared_subscribe_topic" in topics:
            assert topics["shared_subscribe_topic"] == "upstream.a"
            assert "upstream.a" in topics["subscribe_topics"]
            assert "upstream.b" in topics["subscribe_topics"]
            assert "upstream.c" in topics["subscribe_topics"]
            assert "agent_router.input" not in topics["subscribe_topics"]


def test_output_topic_override():
    """output_topic overrides publish_to_topic and bound_registry."""
    node = AgentRouterNode(output_topic="custom.output")
    assert node.publish_to_topic == "custom.output"
    assert node.subscribed_topic == "agent_router.input"  # unchanged

    for topics in node.bound_registry.values():
        if "publish_topic" in topics:
            assert topics["publish_topic"] == "custom.output"


def test_named_with_input_topic_preserves_entrypoint():
    """Named node with input_topic: entrypoint preserved, old shared removed."""
    node = AgentRouterNode(name="my_router", input_topic="custom.input")
    assert node.subscribed_topic == "custom.input"
    assert node.entrypoint_topic == "agent_router.private.my_router"

    for topics in node.bound_registry.values():
        if "shared_subscribe_topic" in topics:
            assert "custom.input" in topics["subscribe_topics"]
            assert "agent_router.private.my_router" in topics["subscribe_topics"]
            assert "agent_router.input" not in topics["subscribe_topics"]


def test_named_with_multiple_input_topics_preserves_entrypoint():
    """Named node with multiple input_topics: entrypoint alongside all inputs."""
    node = AgentRouterNode(name="my_router", input_topic=["up.a", "up.b"])
    assert node.subscribed_topic == "up.a"

    for topics in node.bound_registry.values():
        if "shared_subscribe_topic" in topics:
            assert "up.a" in topics["subscribe_topics"]
            assert "up.b" in topics["subscribe_topics"]
            assert "agent_router.private.my_router" in topics["subscribe_topics"]
            assert "agent_router.input" not in topics["subscribe_topics"]


def test_both_overrides():
    """Both input_topic and output_topic work simultaneously."""
    node = AgentRouterNode(input_topic="in.custom", output_topic="out.custom")
    assert node.subscribed_topic == "in.custom"
    assert node.publish_to_topic == "out.custom"

    for topics in node.bound_registry.values():
        if "shared_subscribe_topic" in topics:
            assert topics["shared_subscribe_topic"] == "in.custom"
            assert "in.custom" in topics["subscribe_topics"]
            assert "agent_router.input" not in topics["subscribe_topics"]
        if "publish_topic" in topics:
            assert topics["publish_topic"] == "out.custom"


def test_class_registry_isolation():
    """Overriding one instance doesn't affect another or the class-level registry."""
    custom = AgentRouterNode(input_topic="custom.in", output_topic="custom.out")
    default = AgentRouterNode()

    # Custom instance has overrides
    assert custom.subscribed_topic == "custom.in"
    assert custom.publish_to_topic == "custom.out"

    # Default instance is untouched
    assert default.subscribed_topic == "agent_router.input"
    assert default.publish_to_topic == "agent_router.output"

    # Class-level _handler_registry is untouched
    for topics in AgentRouterNode._handler_registry.values():
        if "shared_subscribe_topic" in topics:
            assert topics["shared_subscribe_topic"] == "agent_router.input"
        if "publish_topic" in topics:
            assert topics["publish_topic"] == "agent_router.output"


# --- ChatNode topic tests ---


def test_chat_node_named_derives_topics():
    """Named ChatNode derives input/output topics from name."""
    chat = ChatNode(name="gpt4o")
    assert chat.subscribed_topic == "ai_prompted.gpt4o"
    assert chat.publish_to_topic == "ai_generated.gpt4o"
    assert chat.entrypoint_topic is None


def test_chat_node_unnamed_keeps_defaults():
    """Unnamed ChatNode keeps default shared topics."""
    chat = ChatNode()
    assert chat.subscribed_topic == "ai_prompted"
    assert chat.publish_to_topic == "ai_generated"
    assert chat.entrypoint_topic is None


def test_chat_node_explicit_input_topic_overrides_name():
    """Explicit input_topic on ChatNode overrides the name-derived default."""
    chat = ChatNode(name="gpt4o", input_topic="custom.chat.in")
    assert chat.subscribed_topic == "custom.chat.in"
    # output_topic still derived from name
    assert chat.publish_to_topic == "ai_generated.gpt4o"


def test_chat_node_explicit_output_topic_overrides_name():
    """Explicit output_topic on ChatNode overrides the name-derived default."""
    chat = ChatNode(name="gpt4o", output_topic="custom.chat.out")
    assert chat.subscribed_topic == "ai_prompted.gpt4o"
    assert chat.publish_to_topic == "custom.chat.out"


def test_chat_node_class_registry_isolation():
    """ChatNode override doesn't affect other instances."""
    custom = ChatNode(name="custom_chat", input_topic="custom.in")
    default = ChatNode()

    assert custom.subscribed_topic == "custom.in"
    assert default.subscribed_topic == "ai_prompted"

    # Class-level registry untouched
    for topics in ChatNode._handler_registry.values():
        if "shared_subscribe_topic" in topics:
            assert topics["shared_subscribe_topic"] == "ai_prompted"
        if "publish_topic" in topics:
            assert topics["publish_topic"] == "ai_generated"


# --- Integration test: full roundtrip with TestKafkaBroker ---


@pytest.mark.asyncio
async def test_integration_custom_topics_with_test_broker():
    """Full roundtrip using custom topics with FunctionModel and TestKafkaBroker."""

    def echo_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last_user = str(messages[-1].parts[0])
        return ModelResponse(parts=[TextPart(f"echo: {last_user}")])

    custom_input = "my_agent.input"
    custom_output = "my_agent.output"

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = FunctionModel(echo_model)
    chat_node = ChatNode(model_client, name="echo_chat")
    service.register_node(chat_node)

    router_node = AgentRouterNode(
        chat_node=chat_node,
        name="custom_router",
        input_topic=custom_input,
        output_topic=custom_output,
    )
    service.register_node(router_node)

    # Verify topics are set correctly before running
    assert router_node.subscribed_topic == custom_input
    assert router_node.publish_to_topic == custom_output

    response_store: dict[str, asyncio.Queue[EventEnvelope]] = {}

    @broker.subscriber(custom_output)
    def collect_response(
        event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]
    ):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(event_envelope)

    async with TestKafkaBroker(broker) as _:
        trace_id = await router_node.invoke(
            user_prompt="Hello from custom topic",
            broker=broker,
            final_response_topic=custom_output,
            correlation_id="custom-topic-test-1",
        )

        await wait_for_condition(lambda: trace_id in response_store, timeout=5.0)
        queue = response_store[trace_id]
        result = await queue.get()
        assert isinstance(result.latest_message_in_history, ModelResponse)
        content = str(result.latest_message_in_history.parts[0])
        assert "echo:" in content
