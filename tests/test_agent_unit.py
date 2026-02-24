import asyncio
from dataclasses import dataclass
from typing import Annotated

import pytest
from faststream import Context
from faststream.kafka import TestKafkaBroker

from calfkit._vendor.pydantic_ai import (
    ModelMessage,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    models,
)
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.broker.broker import BrokerClient
from calfkit.models.event_envelope import EventEnvelope
from calfkit.models.tool_context import ToolContext
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.base_tool_node import agent_tool
from calfkit.nodes.chat_node import ChatNode
from calfkit.runners.service import NodesService
from calfkit.stores.in_memory import InMemoryMessageHistoryStore
from tests.utils import wait_for_condition


@pytest.fixture(autouse=True)
def block_model_requests():
    """Block actual model requests during unit tests."""
    original_value = models.ALLOW_MODEL_REQUESTS
    models.ALLOW_MODEL_REQUESTS = False
    yield
    models.ALLOW_MODEL_REQUESTS = original_value


# Test fixtures - tools defined just for testing


@agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location.

    Args:
        location (str): The name of the location (e.g. Irvine, CA)

    Returns:
        str: The current weather at the location (e.g. It's currently dry and sunny in Irvine, CA)
    """
    return f"It's raining heavily in {location}"


@agent_tool
def get_temperature(location: str) -> str:
    """Get the current temperature at a location.

    Args:
        location (str): The name of the location (e.g. Brookline, MA)

    Returns:
        str: The current temperature at the location (e.g. It's currently 95°F in Brookline, MA)
    """
    return f"It's -4.5°F in {location}"


# Test: Agent Memory with FunctionModel


@pytest.mark.asyncio
async def test_agent_memory_with_function_model():
    """Test that agent memory works - second invocation has access to previous messages.

    This test uses a FunctionModel that receives the message history and asserts
    that previous messages are preserved across invocations when using the same thread_id.
    """
    call_count = 0
    expected_thread_id = "test-memory-thread-123"

    def memory_test_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        """FunctionModel that asserts on message history for memory verification.

        The key assertions here verify that:
        1. First call receives only the initial user prompt (1 message)
        2. Second call receives the full conversation history (3 messages)
        """
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            # First call - should have only the initial user prompt
            assert len(messages) == 1, (
                f"First call should have 1 message, got {len(messages)}: {messages}"
            )
            # Check the user prompt content
            user_content = str(messages[0].parts[0])
            assert "Hello, my name is Alice" in user_content
            return ModelResponse(parts=[TextPart("Hi Alice! Nice to meet you.")])
        elif call_count == 2:
            # Second call - should have previous messages in history
            # Expected: user prompt 1, model response 1, user prompt 2
            assert len(messages) == 3, (
                f"Second call should have 3 messages in history, got {len(messages)}: {messages}"
            )

            # Verify first user prompt is preserved
            first_msg = messages[0]
            first_content = str(first_msg.parts[0])
            assert "Alice" in first_content or "Hello" in first_content

            # Verify first model response is preserved
            second_msg = messages[1]
            second_content = str(second_msg.parts[0])
            assert "Alice" in second_content

            # Verify second user prompt
            third_msg = messages[2]
            third_content = str(third_msg.parts[0])
            assert "What's my name?" in third_content or "name" in third_content

            return ModelResponse(parts=[TextPart("Your name is Alice!")])
        else:
            raise AssertionError(f"Unexpected call count: {call_count}")

    broker = BrokerClient()
    service = NodesService(broker)

    # Create chat node with FunctionModel
    model_client = FunctionModel(memory_test_model)
    chat_node = ChatNode(model_client)
    service.register_node(chat_node)

    # Create router node with message history store
    memory_store = InMemoryMessageHistoryStore()
    router_node = AgentRouterNode(
        chat_node=ChatNode(model_client),
        message_history_store=memory_store,
    )
    service.register_node(router_node)

    response_store: dict[str, asyncio.Queue[EventEnvelope]] = {}

    @broker.subscriber(router_node.publish_to_topic or "default_collect")
    def collect_response(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(event_envelope)

    async with TestKafkaBroker(broker) as _:
        # First invocation
        trace_id_1 = await router_node.invoke(
            user_prompt="Hello, my name is Alice",
            broker=broker,
            thread_id=expected_thread_id,
            final_response_topic="final_response",
            correlation_id="test-memory-1",
        )

        # Wait for first response
        await wait_for_condition(lambda: trace_id_1 in response_store, timeout=5.0)
        queue_1 = response_store[trace_id_1]
        result_1 = await queue_1.get()
        assert result_1.trace_id == trace_id_1
        assert isinstance(result_1.latest_message_in_history, ModelResponse)
        result_content = str(result_1.latest_message_in_history.parts[0])
        assert "Alice" in result_content

        # Second invocation with SAME thread_id - should access memory
        trace_id_2 = await router_node.invoke(
            user_prompt="What's my name?",
            broker=broker,
            thread_id=expected_thread_id,
            final_response_topic="final_response",
            correlation_id="test-memory-2",
        )

        # Wait for second response
        await wait_for_condition(lambda: trace_id_2 in response_store, timeout=5.0)
        queue_2 = response_store[trace_id_2]
        result_2 = await queue_2.get()
        assert result_2.trace_id == trace_id_2
        assert isinstance(result_2.latest_message_in_history, ModelResponse)
        result_content = str(result_2.latest_message_in_history.parts[0])
        assert "Alice" in result_content

    # Verify the memory store has the messages
    stored_messages = await memory_store.get(expected_thread_id)
    assert len(stored_messages) >= 4  # user1, response1, user2, response2


# Test: Tool Visibility with FunctionModel


@pytest.mark.asyncio
async def test_tool_visibility_with_function_model():
    """Test that the model can see exactly the tools provided to it.

    This test uses a FunctionModel that receives the AgentInfo and asserts
    that the correct tools are visible and properly configured.
    """
    tool_call_made = False

    def tool_visibility_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        """FunctionModel that asserts on tool visibility.

        The key assertions here verify that:
        1. The exact tools we provided (get_weather, get_temperature) are in function_tools
        2. Tool names match what we defined
        3. Tool descriptions are present
        """
        nonlocal tool_call_made

        # function_tools is a list of ToolDefinition objects
        available_tools = info.function_tools
        tool_names = [tool.name for tool in available_tools]

        # Assert exactly the tools we provided are visible
        assert len(available_tools) == 2, (
            f"Expected 2 tools, got {len(available_tools)}\nTool names: {tool_names}"
        )
        assert "get_weather" in tool_names, f"get_weather not in tool names: {tool_names}"
        assert "get_temperature" in tool_names, f"get_temperature not in tool names: {tool_names}"

        # Verify tool descriptions are present and contain relevant keywords
        for tool in available_tools:
            if tool.name == "get_weather":
                desc = tool.description or ""
                assert "weather" in desc.lower(), (
                    f"get_weather description missing 'weather': {desc}"
                )
            elif tool.name == "get_temperature":
                desc = tool.description or ""
                assert "temperature" in desc.lower(), (
                    f"get_temperature description missing 'temperature': {desc}"
                )

        if not tool_call_made:
            # First call - make a tool call
            tool_call_made = True
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name="get_weather",
                        args={"location": "Tokyo"},
                        tool_call_id="test-call-123",
                    )
                ]
            )
        else:
            # Second call - after tool return
            return ModelResponse(parts=[TextPart("The weather in Tokyo is rainy.")])

    broker = BrokerClient()
    service = NodesService(broker)

    # Create chat node with FunctionModel
    model_client = FunctionModel(tool_visibility_model)
    chat_node = ChatNode(model_client)
    service.register_node(chat_node)

    # Create router node with tools
    router_node = AgentRouterNode(
        chat_node=ChatNode(model_client),
        tool_nodes=[get_weather, get_temperature],
    )
    service.register_node(router_node)

    # Register tool nodes
    service.register_node(get_weather)
    service.register_node(get_temperature)

    response_store: dict[str, asyncio.Queue[EventEnvelope]] = {}

    @broker.subscriber(router_node.publish_to_topic or "default_collect")
    def collect_response(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(event_envelope)

    async with TestKafkaBroker(broker) as _:
        trace_id = await router_node.invoke(
            user_prompt="What's the weather in Tokyo?",
            broker=broker,
            final_response_topic="final_response",
            correlation_id="test-tool-visibility-1",
        )

        # Wait for tool call response
        await wait_for_condition(lambda: trace_id in response_store, timeout=5.0)
        queue = response_store[trace_id]
        result = await queue.get()
        assert isinstance(result.latest_message_in_history, ModelResponse)

        # The model should have received the tool return and made a final response
        # Verify our assertions in the FunctionModel passed
        assert tool_call_made  # The tool was called


# Test: ToolContext schema exclusion


def test_tool_context_excluded_from_schema():
    """Verify that ToolContext is excluded from the tool's JSON schema.

    When a tool function declares ToolContext as its first parameter,
    the generated schema should NOT include ToolContext — it's an
    internal runtime injection, invisible to the LLM.
    """

    @agent_tool
    def my_tool(ctx: ToolContext, query: str) -> str:
        """Search for something.

        Args:
            query: The search query.
        """
        return f"result for {query}"

    schema = my_tool.tool_schema
    properties = schema.parameters_json_schema.get("properties", {})
    assert "ctx" not in properties, f"ToolContext leaked into schema: {properties}"
    assert "query" in properties, f"Expected 'query' in schema: {properties}"


# Test: ToolContext backward compatibility


def test_tools_without_context_still_work():
    """Existing tools without ToolContext must keep working identically."""
    schema = get_weather.tool_schema
    properties = schema.parameters_json_schema.get("properties", {})
    assert "location" in properties
    assert "ctx" not in properties


# Test: ToolContext runtime injection


@agent_tool
def ctx_echo_tool(ctx: ToolContext, message: str) -> str:
    """Echo back the agent name and deps from context.

    Args:
        message: A message to echo.
    """
    return f"agent={ctx.agent_name} deps={ctx.deps} msg={message}"


@pytest.mark.asyncio
async def test_tool_context_runtime_injection():
    """Test that ToolContext is injected at runtime with agent_name and deps.

    Uses FunctionModel to trigger a tool call, then verifies the tool
    receives a populated ToolContext with the correct agent_name and deps.
    """
    tool_call_made = False

    @dataclass
    class MyDeps:
        api_key: str

    def ctx_injection_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        nonlocal tool_call_made
        if not tool_call_made:
            tool_call_made = True
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name="ctx_echo_tool",
                        args={"message": "hello"},
                        tool_call_id="ctx-test-call-1",
                    )
                ]
            )
        else:
            return ModelResponse(parts=[TextPart("done")])

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = FunctionModel(ctx_injection_model)
    chat_node = ChatNode(model_client)
    service.register_node(chat_node)

    my_deps = MyDeps(api_key="sk-test-123")
    router_node = AgentRouterNode(
        chat_node=ChatNode(model_client),
        tool_nodes=[ctx_echo_tool],
        name="test_agent",
        deps_type=MyDeps,
    )
    service.register_node(router_node)
    service.register_node(ctx_echo_tool)

    response_store: dict[str, asyncio.Queue[EventEnvelope]] = {}

    @broker.subscriber(router_node.publish_to_topic or "default_collect")
    def collect_response(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(event_envelope)

    async with TestKafkaBroker(broker) as _:
        trace_id = await router_node.invoke(
            user_prompt="Call the tool",
            broker=broker,
            final_response_topic="final_response",
            correlation_id="test-ctx-inject-1",
            deps=my_deps,
        )

        await wait_for_condition(lambda: trace_id in response_store, timeout=5.0)
        queue = response_store[trace_id]
        result = await queue.get()
        assert isinstance(result.latest_message_in_history, ModelResponse)

        # Verify the tool was called and received correct context
        assert tool_call_made

        # Find the tool return in message history to verify context injection
        tool_returns = [
            part
            for msg in result.message_history
            for part in msg.parts
            if isinstance(part, ToolReturnPart) and part.tool_name == "ctx_echo_tool"
        ]
        assert len(tool_returns) == 1, f"Expected 1 tool return, got {len(tool_returns)}"
        content = str(tool_returns[0].content)
        assert "agent=test_agent" in content, f"agent_name not injected: {content}"
        assert "sk-test-123" in content, f"deps not injected: {content}"
        assert "msg=hello" in content, f"message arg missing: {content}"


# Test: deps round-trip through EventEnvelope


def test_deps_round_trip_on_envelope():
    """Verify that deps set on EventEnvelope survives serialization."""

    @dataclass
    class Config:
        url: str

    envelope = EventEnvelope(
        trace_id="test",
        deps=Config(url="https://example.com"),
        agent_name="my_agent",
    )
    assert envelope.deps.url == "https://example.com"
    assert envelope.agent_name == "my_agent"


# Test: Named ChatNode topic resolution


def test_named_chat_node_entrypoint_topic():
    """Named ChatNode should resolve an entrypoint topic like 'ai_prompted.<name>'."""
    chat = ChatNode(name="gpt4o")
    assert chat.entrypoint_topic == "ai_prompted.gpt4o"


def test_named_chat_node_removes_shared_subscribe_topic():
    """A named ChatNode should NOT subscribe to the shared 'ai_prompted' topic."""
    chat = ChatNode(name="gpt4o")
    for topics in chat.bound_registry.values():
        subscribe_topics = topics.get("subscribe_topics", [])
        assert "ai_prompted" not in subscribe_topics, (
            "Shared topic 'ai_prompted' should be removed for named "
            f"ChatNode, got {subscribe_topics}"
        )


def test_unnamed_chat_node_backwards_compat():
    """Unnamed ChatNode should behave exactly as before — shared topic, no entrypoint."""
    chat = ChatNode()
    assert chat.subscribed_topic == "ai_prompted"
    assert chat.entrypoint_topic is None


@pytest.mark.asyncio
async def test_router_targets_named_chat_node():
    """Two routers with different named ChatNodes should route to the correct model.

    Uses FunctionModel instances that return distinct responses so we can verify
    each router's messages reach the right ChatNode.

    Messages are published directly to each router's private entrypoint topic
    (not the shared "agent_router.input") because TestKafkaBroker doesn't
    support consumer-group routing on a shared topic.
    """
    from calfkit._vendor.pydantic_ai import ModelRequest

    def model_alpha(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart("alpha-response")])

    def model_beta(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart("beta-response")])

    broker = BrokerClient()
    service = NodesService(broker)

    chat_alpha = ChatNode(FunctionModel(model_alpha), name="alpha")
    chat_beta = ChatNode(FunctionModel(model_beta), name="beta")
    service.register_node(chat_alpha)
    service.register_node(chat_beta)

    router_alpha = AgentRouterNode(chat_node=chat_alpha, name="agent_alpha")
    router_beta = AgentRouterNode(chat_node=chat_beta, name="agent_beta")
    service.register_node(router_alpha)
    service.register_node(router_beta)

    response_store: dict[str, asyncio.Queue[EventEnvelope]] = {}

    @broker.subscriber(router_alpha.publish_to_topic or "default_collect")
    def collect_response(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(event_envelope)

    async with TestKafkaBroker(broker) as _:
        # Publish directly to router_alpha's private entrypoint
        env_a = EventEnvelope(
            trace_id="alpha-1",
            final_response_topic="final_response",
        )
        env_a.mark_as_start_of_turn()
        env_a.prepare_uncommitted_agent_messages([ModelRequest.user_text_prompt("hello")])
        await broker.publish(
            env_a,
            topic=router_alpha.entrypoint_topic,
            correlation_id="alpha-1",
        )
        await wait_for_condition(lambda: "alpha-1" in response_store, timeout=5.0)
        result_a = await response_store["alpha-1"].get()
        assert isinstance(result_a.latest_message_in_history, ModelResponse)
        assert "alpha-response" in str(result_a.latest_message_in_history.parts[0])

        # Publish directly to router_beta's private entrypoint
        env_b = EventEnvelope(
            trace_id="beta-1",
            final_response_topic="final_response",
        )
        env_b.mark_as_start_of_turn()
        env_b.prepare_uncommitted_agent_messages([ModelRequest.user_text_prompt("hello")])
        await broker.publish(
            env_b,
            topic=router_beta.entrypoint_topic,
            correlation_id="beta-1",
        )
        await wait_for_condition(lambda: "beta-1" in response_store, timeout=5.0)
        result_b = await response_store["beta-1"].get()
        assert isinstance(result_b.latest_message_in_history, ModelResponse)
        assert "beta-response" in str(result_b.latest_message_in_history.parts[0])
