import asyncio
from typing import Annotated

import pytest
from faststream import Context
from faststream.kafka import TestKafkaBroker

from calfkit._vendor.pydantic_ai import ModelMessage, ModelResponse, TextPart, ToolCallPart, models
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.broker.broker import BrokerClient
from calfkit.models.event_envelope import EventEnvelope
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
