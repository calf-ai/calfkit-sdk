"""Integration tests for experimental choreography-based agent and tool nodes.

Uses real OpenAI API calls with TestKafkaBroker for synchronous Kafka simulation.

These tests define expected behavior for a complete implementation of the
experimental choreography-based node architecture. Some tests may fail with
the current WIP implementation -- that's intentional (TDD approach).

Known issues that tests will surface:
- agent_def.py:128 hardcodes topic="test" instead of the tool's subscribe topic
- agent_def.py:106 creates new State() for tool delegations, losing message_history
- agent_def.py:129 only assigns state to the last delegate (multi-tool broken)
- agent_def.py:77 tries to assign to frozen Deps model (deps validation path)
- agent_def.py:52 calls super().__init__(agent_id) without subscribe_topics
"""

import asyncio
import os
from typing import Annotated, Any

import pytest
from dotenv import load_dotenv
from faststream import Context
from faststream.kafka import TestKafkaBroker
from faststream.kafka.annotations import KafkaBroker as BrokerAnnotation

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ModelRequest, UserPromptPart
from calfkit._vendor.pydantic_ai.messages import ToolCallPart as VendorToolCallPart
from calfkit.broker.broker import BrokerClient
from calfkit.experimental.base_models.actions import Call, ReturnCall
from calfkit.experimental.base_models.session_context import BaseSessionRunContext
from calfkit.experimental.data_model.state_deps import Deps, State
from calfkit.experimental.nodes.agent_def import BaseAgentNodeDef
from calfkit.experimental.nodes.node_def import Envelope
from calfkit.experimental.nodes.tool_def import agent_tool as experimental_agent_tool
from calfkit.models.tool_context import ToolContext
from calfkit.providers.pydantic_ai.openai import OpenAIModelClient
from tests.utils import wait_for_condition

load_dotenv()

# Ensure model requests are allowed for integration tests
models.ALLOW_MODEL_REQUESTS = True

skip_if_no_openai_key = pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"),
    reason="Skipping integration test: OPENAI_API_KEY not set in environment",
)


# ---------------------------------------------------------------------------
# Tool definitions (experimental @agent_tool)
# ---------------------------------------------------------------------------


@experimental_agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location.

    Args:
        location: The name of the location (e.g. Tokyo, Japan)

    Returns:
        The current weather at the location.
    """
    return f"It's raining heavily in {location}"


@experimental_agent_tool
def get_temperature(location: str) -> str:
    """Get the current temperature at a location.

    Args:
        location: The name of the location (e.g. Brookline, MA)

    Returns:
        The current temperature at the location.
    """
    return f"It's -4.5F in {location}"


@experimental_agent_tool
def ctx_echo_tool(ctx: ToolContext, message: str) -> str:
    """Echo back the agent name and deps from context.

    Args:
        message: A message to echo.
    """
    return f"agent={ctx.agent_name} deps={ctx.deps} msg={message}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_model_client() -> OpenAIModelClient:
    """Create an OpenAI model client from environment variables."""
    return OpenAIModelClient(
        os.environ["TEST_LLM_MODEL_NAME"],
        reasoning_effort=os.getenv("TEST_REASONING_EFFORT"),
    )


# ---------------------------------------------------------------------------
# Tests: BaseAgentNodeDef
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_basic_agent_text_reply():
    """Agent with no tools receives a prompt and returns a text ReturnCall.

    Verifies:
    - Agent processes staged prompt from uncommitted_message
    - LLM generates a text response (no tool calls)
    - run() returns ReturnCall[State] with updated message_history
    """
    agent_node = BaseAgentNodeDef(
        agent_id="basic_agent",
        model_client=make_model_client(),
        system_prompt="You are a helpful assistant. Always respond briefly in one sentence.",
    )

    state = State(
        uncommitted_message=ModelRequest(parts=[UserPromptPart(content="What is 2 + 2?")])
    )
    ctx = BaseSessionRunContext(
        state=state,
        deps=Deps(correlation_id="test-1", agent_deps=None),
    )

    result = await agent_node.run(ctx)

    assert isinstance(result, ReturnCall), f"Expected ReturnCall, got {type(result).__name__}"
    assert isinstance(result.state, State)
    assert len(result.state.message_history) > 0, "Should have message_history from LLM call"

    # The response should mention "4"
    last_msg = result.state.message_history[-1]
    text = " ".join(str(p) for p in last_msg.parts)
    assert "4" in text, f"Expected '4' in response: {text}"


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_basic_agent_text_reply_via_broker():
    """Full broker flow: client publishes to agent topic, gets Reply on output topic.

    Verifies the Reply choreography:
    - reply_stack is popped correctly
    - Response is published to the correct output topic
    - State with message_history arrives at the collector
    """
    agent_node = BaseAgentNodeDef(
        agent_id="broker_agent",
        model_client=make_model_client(),
        system_prompt="You are a helpful assistant. Respond briefly.",
    )

    broker = BrokerClient()
    output_topic = "test.broker_agent.output"
    agent_input_topic = "broker_agent.input"

    # Register agent handler for the input topic
    @broker.subscriber(agent_input_topic)
    async def handle_agent(
        envelope: Envelope[State, Deps],
        correlation_id: Annotated[str, Context()],
        broker_: BrokerAnnotation,
    ):
        await agent_node.handler(envelope, correlation_id, broker_)

    # Collect responses on the output topic
    response_store: dict[str, asyncio.Queue[Envelope]] = {}

    @broker.subscriber(output_topic)
    async def collect_response(
        envelope: Envelope[State, Deps],
        correlation_id: Annotated[str, Context()],
    ):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(envelope)

    async with TestKafkaBroker(broker) as _:
        corr_id = "test-broker-1"
        envelope = make_envelope(
            "What is the capital of France?",
            correlation_id=corr_id,
            reply_stack=[output_topic],
        )

        await broker.publish(envelope, topic=agent_input_topic, correlation_id=corr_id)

        await wait_for_condition(lambda: corr_id in response_store, timeout=30.0)
        result = await response_store[corr_id].get()

        # Verify state arrived with message history
        assert result.context.state is not None
        assert len(result.context.state.message_history) > 0

        # reply_stack should be empty (popped the output_topic)
        assert result.reply_stack == []

        # Response should mention Paris
        text = " ".join(str(p) for p in result.context.state.message_history[-1].parts)
        assert "paris" in text.lower(), f"Expected 'Paris' in response: {text}"


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_agent_multi_turn_memory():
    """Agent preserves conversation history across multiple turns via State.

    Verifies:
    - message_history from first turn is carried into second turn
    - Agent can recall information introduced in the first turn
    """
    agent_node = BaseAgentNodeDef(
        agent_id="memory_agent",
        model_client=make_model_client(),
        system_prompt="You are a helpful assistant. Respond briefly.",
    )

    # First turn: introduce information
    state1 = State(
        uncommitted_message=ModelRequest(parts=[UserPromptPart(content="My name is Alice.")])
    )
    ctx1 = BaseSessionRunContext(
        state=state1,
        deps=Deps(correlation_id="turn-1", agent_deps=None),
    )

    result1 = await agent_node.run(ctx1)
    assert isinstance(result1, ReturnCall), (
        f"First turn: expected ReturnCall, got {type(result1).__name__}"
    )
    assert len(result1.state.message_history) > 0

    # Second turn: carry forward message_history, stage new prompt
    state2 = result1.state
    state2.stage_message(ModelRequest(parts=[UserPromptPart(content="What is my name?")]))
    ctx2 = BaseSessionRunContext(
        state=state2,
        deps=Deps(correlation_id="turn-2", agent_deps=None),
    )

    result2 = await agent_node.run(ctx2)
    assert isinstance(result2, ReturnCall), (
        f"Second turn: expected ReturnCall, got {type(result2).__name__}"
    )

    # Agent should remember "Alice" from the first turn
    text = " ".join(str(p) for p in result2.state.message_history[-1].parts)
    assert "alice" in text.lower(), f"Expected 'Alice' in response: {text}"


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_agent_tool_visibility():
    """Agent passes tool schemas to the LLM via ExternalToolset.

    Verifies:
    - When given a tool-appropriate prompt, the LLM chooses to call the tool
    - run() returns a Call (not a ReturnCall) when tool calls are made
    """
    agent_node = BaseAgentNodeDef(
        agent_id="visibility_agent",
        model_client=make_model_client(),
        system_prompt="You are a weather assistant. Always use the get_weather tool.",
        tools=[get_weather],
    )

    state = State(
        uncommitted_message=ModelRequest(
            parts=[UserPromptPart(content="What's the weather in Tokyo?")]
        )
    )
    ctx = BaseSessionRunContext(
        state=state,
        deps=Deps(correlation_id="vis-1", agent_deps=None),
    )

    result = await agent_node.run(ctx)

    # The LLM should call the get_weather tool, resulting in a Call
    assert isinstance(result, Call), (
        f"Expected Call (tool call), got {type(result).__name__}: {result}"
    )


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_agent_tool_delegation_uses_correct_topic():
    """When the LLM calls a tool, the Call should route to the tool's actual
    subscribe topic (not a hardcoded placeholder).

    Verifies:
    - Call.target_topic matches the corresponding tool's subscribe topic
    """
    agent_node = BaseAgentNodeDef(
        agent_id="topic_agent",
        model_client=make_model_client(),
        system_prompt="You are a weather assistant. Always use the get_weather tool.",
        tools=[get_weather],
    )

    state = State(
        uncommitted_message=ModelRequest(
            parts=[UserPromptPart(content="What's the weather in Tokyo?")]
        )
    )
    ctx = BaseSessionRunContext(
        state=state,
        deps=Deps(correlation_id="topic-1", agent_deps=None),
    )

    result = await agent_node.run(ctx)
    assert isinstance(result, Call)

    # The call target should be the tool's actual subscribe topic
    expected_topic = get_weather.subscribe_topics[0]  # "tool.get_weather.input"
    assert result.target_topic == expected_topic, (
        f"Call target should be '{expected_topic}', got '{result.target_topic}'"
    )


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_agent_tool_delegation_preserves_message_history():
    """When delegating to a tool, the State should carry the conversation's
    message_history so that the agent can continue the conversation after
    the tool returns.

    Verifies:
    - The Call's State includes message_history from the LLM interaction
    - After tool return, the agent has context to produce a final response
    """
    agent_node = BaseAgentNodeDef(
        agent_id="history_agent",
        model_client=make_model_client(),
        system_prompt="Use get_weather for weather questions.",
        tools=[get_weather],
    )

    state = State(
        uncommitted_message=ModelRequest(
            parts=[UserPromptPart(content="What's the weather in Tokyo?")]
        )
    )
    ctx = BaseSessionRunContext(
        state=state,
        deps=Deps(correlation_id="hist-1", agent_deps=None),
    )

    result = await agent_node.run(ctx)
    assert isinstance(result, Call), f"Expected Call, got {type(result).__name__}"

    assert isinstance(result.state, State)

    # The state should preserve message_history from the LLM call
    # (user prompt + model tool call response)
    assert len(result.state.message_history) > 0, (
        "Call state should carry message_history from the LLM interaction"
    )


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_agent_with_single_tool_call_full_flow():
    """Full agent-tool round-trip via broker: agent calls tool, gets result, responds.

    End-to-end flow:
    1. Client publishes to agent input topic
    2. Agent -> LLM returns tool call -> Delegate to tool topic
    3. Tool executes -> Reply back to agent return topic
    4. Agent -> LLM returns text with tool result -> Reply to output topic
    5. Collector receives final response incorporating tool result

    This test requires:
    - Agent delegates to the correct tool topic
    - Message history is preserved through the tool call round-trip
    - Tool result is incorporated into the final response
    """
    agent_node = BaseAgentNodeDef(
        agent_id="full_flow_agent",
        model_client=make_model_client(),
        system_prompt="You are a weather assistant. Always use the get_weather tool.",
        tools=[get_weather],
    )

    broker = BrokerClient()
    output_topic = "test.full_flow.output"
    agent_input_topic = "full_flow_agent.input"

    # Register agent handler for input topic AND return topic
    @broker.subscriber(agent_input_topic)
    @broker.subscriber(agent_node._return_topic)
    async def handle_agent(
        envelope: Envelope[State, Deps],
        correlation_id: Annotated[str, Context()],
        broker_: BrokerAnnotation,
    ):
        await agent_node.handler(envelope, correlation_id, broker_)

    # Register tool handler on the tool's subscribe topic
    tool_topic = get_weather.subscribe_topics[0]

    @broker.subscriber(tool_topic)
    async def handle_tool(
        envelope: Envelope[State, Deps],
        correlation_id: Annotated[str, Context()],
        broker_: BrokerAnnotation,
    ):
        await get_weather.handler(envelope, correlation_id, broker_)

    # Collect responses on the output topic
    response_store: dict[str, asyncio.Queue[Envelope]] = {}

    @broker.subscriber(output_topic)
    async def collect_response(
        envelope: Envelope[State, Deps],
        correlation_id: Annotated[str, Context()],
    ):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(envelope)

    async with TestKafkaBroker(broker) as _:
        corr_id = "test-full-flow-1"
        envelope = make_envelope(
            "What's the weather in Tokyo?",
            correlation_id=corr_id,
            reply_stack=[output_topic],
        )

        await broker.publish(envelope, topic=agent_input_topic, correlation_id=corr_id)

        await wait_for_condition(lambda: corr_id in response_store, timeout=30.0)
        result = await response_store[corr_id].get()

        # The final response should incorporate the tool result
        assert result.context.state is not None
        assert len(result.context.state.message_history) > 0

        # The tool returns "It's raining heavily in Tokyo"
        # so the final response should mention rain
        text = " ".join(str(p) for p in result.context.state.message_history[-1].parts)
        assert "rain" in text.lower(), f"Expected 'rain' in response: {text}"


# ---------------------------------------------------------------------------
# Tests: ToolNodeDef
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_node_direct_execution():
    """ToolNodeDef.run() processes a ToolCallPart and returns Reply with tool result.

    Verifies:
    - Tool extracts ToolCallPart from state via get_tool_call()
    - Tool function is executed with correct kwargs
    - Result is stored in state.tool_results keyed by tool_call_id
    """
    state = State(uncommitted_message=None)
    state.add_tool_call(
        VendorToolCallPart(
            tool_name="get_weather",
            args={"location": "Paris"},
            tool_call_id="call-1",
        )
    )
    deps = Deps(correlation_id="tool-exec-1", agent_deps=None)
    ctx = BaseSessionRunContext(state=state, deps=deps)

    result = await get_weather.run(ctx, "call-1", "caller_agent")

    assert isinstance(result, ReturnCall), f"Expected ReturnCall, got {type(result).__name__}"
    assert len(result.state.tool_results) > 0
    assert "call-1" in result.state.tool_results

    tool_return = result.state.tool_results["call-1"]
    assert "raining heavily" in str(tool_return.return_value)
    assert "Paris" in str(tool_return.return_value)


@pytest.mark.asyncio
async def test_tool_node_context_injection():
    """ToolNodeDef injects correct ToolContext values when executing a tool.

    Verifies:
    - ToolContext.agent_name is set from payload.source_node_id
    - ToolContext.deps is set from context.deps.agent_deps
    - Tool function receives these values correctly
    """
    state = State(uncommitted_message=None)
    state.add_tool_call(
        VendorToolCallPart(
            tool_name="ctx_echo_tool",
            args={"message": "hello"},
            tool_call_id="call-ctx-1",
        )
    )
    deps = Deps(correlation_id="ctx-inject-1", agent_deps={"api_key": "sk-test-123"})
    ctx = BaseSessionRunContext(state=state, deps=deps)

    result = await ctx_echo_tool.run(ctx, "call-ctx-1", "test_source_agent")

    assert isinstance(result, ReturnCall)
    assert len(result.state.tool_results) > 0
    assert "call-ctx-1" in result.state.tool_results

    tool_return = result.state.tool_results["call-ctx-1"]
    content = str(tool_return.return_value)
    assert "agent=test_source_agent" in content, f"agent_name not injected: {content}"
    assert "msg=hello" in content, f"message arg missing: {content}"


@pytest.mark.asyncio
async def test_tool_node_returns_silent_when_no_tool_call():
    """ToolNodeDef returns Silent when no ToolCallPart is found in the payload.

    Verifies graceful handling of payloads without tool calls.
    """
    from calfkit.experimental.nodes.node_def import Silent

    # State with no tool calls registered
    state = State(uncommitted_message=None)
    deps = Deps(correlation_id="no-tool-1", agent_deps=None)
    ctx = BaseSessionRunContext(state=state, deps=deps)

    # Pass a non-existent tool_call_id — should return Silent
    result = await get_weather.run(ctx, "nonexistent-call-id", "some_agent")

    assert isinstance(result, Silent), f"Expected Silent, got {type(result).__name__}"


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_agent_with_tool_context_injection_full_flow():
    """Full flow: agent calls a tool that uses ToolContext, verifies context is correct.

    Verifies:
    - ToolContext.agent_name reflects the calling agent's id
    - ToolContext.deps carries the agent_deps from the original request
    - The tool result incorporating context values flows back to the agent
    """
    agent_node = BaseAgentNodeDef(
        agent_id="ctx_agent",
        model_client=make_model_client(),
        system_prompt="You must use the ctx_echo_tool tool for every request. Pass the user's message.",  # noqa: E501
        tools=[ctx_echo_tool],
    )

    broker = BrokerClient()
    output_topic = "test.ctx_flow.output"
    agent_input_topic = "ctx_agent.input"

    @broker.subscriber(agent_input_topic)
    @broker.subscriber(agent_node._return_topic)
    async def handle_agent(
        envelope: Envelope[State, Deps],
        correlation_id: Annotated[str, Context()],
        broker_: BrokerAnnotation,
    ):
        await agent_node.handler(envelope, correlation_id, broker_)

    tool_topic = ctx_echo_tool.subscribe_topics[0]

    @broker.subscriber(tool_topic)
    async def handle_tool(
        envelope: Envelope[State, Deps],
        correlation_id: Annotated[str, Context()],
        broker_: BrokerAnnotation,
    ):
        await ctx_echo_tool.handler(envelope, correlation_id, broker_)

    response_store: dict[str, asyncio.Queue[Envelope]] = {}

    @broker.subscriber(output_topic)
    async def collect_response(
        envelope: Envelope[State, Deps],
        correlation_id: Annotated[str, Context()],
    ):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(envelope)

    async with TestKafkaBroker(broker) as _:
        corr_id = "test-ctx-flow-1"
        envelope = make_envelope(
            "test message",
            correlation_id=corr_id,
            reply_stack=[output_topic],
            agent_deps={"api_key": "sk-ctx-test"},
        )

        await broker.publish(envelope, topic=agent_input_topic, correlation_id=corr_id)

        await wait_for_condition(lambda: corr_id in response_store, timeout=30.0)
        result = await response_store[corr_id].get()

        # The tool should have received the correct context
        assert result.context.state is not None
        assert len(result.context.state.message_history) > 0
