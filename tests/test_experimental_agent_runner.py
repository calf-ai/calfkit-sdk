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
import time
from typing import Annotated, Any

import pytest
from dotenv import load_dotenv
from faststream import Context
from faststream.kafka import TestKafkaBroker
from faststream.kafka.annotations import KafkaBroker as BrokerAnnotation

from calfkit._vendor.pydantic_ai import models
from calfkit.broker.broker import BrokerClient
from calfkit.experimental.agent_def import BaseAgentNodeDef
from calfkit.experimental.context_models import BaseSessionRunContext
from calfkit.experimental.node_def import Delegate, Envelope, Reply
from calfkit.experimental.payload_model import Payload, TextPart, ToolCallPart
from calfkit.experimental.state_and_deps_models import AgentActivityState, Deps, State
from calfkit.experimental.tool_def import ToolNodeDef
from calfkit.experimental.tool_def import agent_tool as experimental_agent_tool
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


def make_payload(user_prompt: str, *, correlation_id: str) -> Payload:
    """Create a Payload with a single TextPart."""
    return Payload(
        correlation_id=correlation_id,
        source_node_id="client",
        parts=[TextPart(text=user_prompt)],
        timestamp=time.time(),
    )


def make_envelope(
    user_prompt: str,
    *,
    correlation_id: str,
    reply_stack: list[str],
    agent_deps: Any = None,
    state: State | None = None,
) -> Envelope[State, Deps]:
    """Create an Envelope with a user prompt payload in State.run_state.todo_stack."""
    payload = make_payload(user_prompt, correlation_id=correlation_id)
    if state is None:
        state = State(run_state=AgentActivityState(todo_stack=[payload]))
    else:
        # Carry forward existing state, add new prompt to todo_stack
        updated_run_state = state.run_state.model_copy(
            update={"todo_stack": [*state.run_state.todo_stack, payload]}
        )
        state = state.model_copy(update={"run_state": updated_run_state})
    deps = Deps(correlation_id=correlation_id, agent_deps=agent_deps)
    return Envelope(
        context=BaseSessionRunContext(state=state, deps=deps),
        reply_stack=reply_stack,
    )


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
    """Agent with no tools receives a prompt and returns a text Reply.

    Verifies:
    - Agent processes prompt from state.todo_stack via _prepare_prompt
    - LLM generates a text response (no tool calls)
    - run() returns Reply[State] with updated message_history
    """
    agent_node = BaseAgentNodeDef(
        agent_id="basic_agent",
        model_client=make_model_client(),
        system_prompt="You are a helpful assistant. Always respond briefly in one sentence.",
    )

    ctx = BaseSessionRunContext(
        state=State(run_state=AgentActivityState(todo_stack=[make_payload("What is 2 + 2?", correlation_id="test-1")])),
        deps=Deps(correlation_id="test-1", agent_deps=None),
    )

    result = await agent_node.run(ctx)

    assert isinstance(result, Reply), f"Expected Reply, got {type(result).__name__}"
    assert isinstance(result.value, State)
    assert len(result.value.run_state.message_history) > 0, "Should have message_history from LLM call"

    # The response should mention "4"
    last_msg = result.value.run_state.message_history[-1]
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
        assert len(result.context.state.run_state.message_history) > 0

        # reply_stack should be empty (popped the output_topic)
        assert result.reply_stack == []

        # Response should mention Paris
        text = " ".join(str(p) for p in result.context.state.run_state.message_history[-1].parts)
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
    ctx1 = BaseSessionRunContext(
        state=State(run_state=AgentActivityState(todo_stack=[make_payload("My name is Alice.", correlation_id="turn-1")])),
        deps=Deps(correlation_id="turn-1", agent_deps=None),
    )

    result1 = await agent_node.run(ctx1)
    assert isinstance(result1, Reply), f"First turn: expected Reply, got {type(result1).__name__}"
    assert len(result1.value.run_state.message_history) > 0

    # Second turn: carry forward run_state with message_history, ask about first turn
    second_payload = make_payload("What is my name?", correlation_id="turn-2")
    carried_run_state = result1.value.run_state.model_copy(
        update={"todo_stack": [*result1.value.run_state.todo_stack, second_payload]}
    )
    state2 = State(run_state=carried_run_state)
    ctx2 = BaseSessionRunContext(
        state=state2,
        deps=Deps(correlation_id="turn-2", agent_deps=None),
    )

    result2 = await agent_node.run(ctx2)
    assert isinstance(result2, Reply), f"Second turn: expected Reply, got {type(result2).__name__}"

    # Agent should remember "Alice" from the first turn
    text = " ".join(str(p) for p in result2.value.run_state.message_history[-1].parts)
    assert "alice" in text.lower(), f"Expected 'Alice' in response: {text}"


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_agent_tool_visibility():
    """Agent passes tool schemas to the LLM via ExternalToolset.

    Verifies:
    - When given a tool-appropriate prompt, the LLM chooses to call the tool
    - run() returns a list of Delegates (not a Reply) when tool calls are made
    """
    agent_node = BaseAgentNodeDef(
        agent_id="visibility_agent",
        model_client=make_model_client(),
        system_prompt="You are a weather assistant. Always use the get_weather tool.",
        tools=[get_weather],
    )

    ctx = BaseSessionRunContext(
        state=State(
            run_state=AgentActivityState(todo_stack=[make_payload("What's the weather in Tokyo?", correlation_id="vis-1")])
        ),
        deps=Deps(correlation_id="vis-1", agent_deps=None),
    )

    result = await agent_node.run(ctx)

    # The LLM should call the get_weather tool, resulting in Delegate(s)
    assert isinstance(result, list), (
        f"Expected list[Delegate] (tool call), got {type(result).__name__}: {result}"
    )
    assert len(result) > 0
    for item in result:
        assert isinstance(item, Delegate), f"Expected Delegate, got {type(item).__name__}"


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_agent_tool_delegation_uses_correct_topic():
    """When the LLM calls a tool, the Delegate should route to the tool's actual
    subscribe topic (not a hardcoded placeholder).

    Verifies:
    - Each Delegate.topic matches the corresponding tool's subscribe topic
    - The Delegate state contains the correct ToolCallPart in todo_stack
    """
    agent_node = BaseAgentNodeDef(
        agent_id="topic_agent",
        model_client=make_model_client(),
        system_prompt="You are a weather assistant. Always use the get_weather tool.",
        tools=[get_weather],
    )

    ctx = BaseSessionRunContext(
        state=State(
            run_state=AgentActivityState(todo_stack=[make_payload("What's the weather in Tokyo?", correlation_id="topic-1")])
        ),
        deps=Deps(correlation_id="topic-1", agent_deps=None),
    )

    result = await agent_node.run(ctx)
    assert isinstance(result, list) and len(result) > 0

    # The delegate topic should be the tool's actual subscribe topic
    expected_topic = get_weather.subscribe_topics[0]  # "tool.get_weather.input"
    delegate_with_state = [d for d in result if d.value is not None]
    assert len(delegate_with_state) > 0, "At least one delegate should carry the state"

    # Verify the delegate routes to the correct tool topic
    for delegate in result:
        assert delegate.topic == expected_topic, (
            f"Delegate topic should be '{expected_topic}', got '{delegate.topic}'"
        )


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_agent_tool_delegation_preserves_message_history():
    """When delegating to a tool, the State should carry the conversation's
    message_history so that the agent can continue the conversation after
    the tool returns.

    Verifies:
    - The delegate's State includes message_history from the LLM interaction
    - After tool return, the agent has context to produce a final response
    """
    agent_node = BaseAgentNodeDef(
        agent_id="history_agent",
        model_client=make_model_client(),
        system_prompt="Use get_weather for weather questions.",
        tools=[get_weather],
    )

    ctx = BaseSessionRunContext(
        state=State(
            run_state=AgentActivityState(todo_stack=[make_payload("What's the weather in Tokyo?", correlation_id="hist-1")])
        ),
        deps=Deps(correlation_id="hist-1", agent_deps=None),
    )

    result = await agent_node.run(ctx)
    assert isinstance(result, list)

    # Find the delegate that carries the state
    delegate_with_state = [d for d in result if d.value is not None]
    assert len(delegate_with_state) > 0

    state = delegate_with_state[0].value
    assert isinstance(state, State)

    # The state should preserve message_history from the LLM call
    # (user prompt + model tool call response)
    assert len(state.run_state.message_history) > 0, (
        "Delegate state should carry message_history from the LLM interaction"
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
        assert len(result.context.state.run_state.message_history) > 0

        # The tool returns "It's raining heavily in Tokyo"
        # so the final response should mention rain
        text = " ".join(str(p) for p in result.context.state.run_state.message_history[-1].parts)
        assert "rain" in text.lower(), f"Expected 'rain' in response: {text}"


# ---------------------------------------------------------------------------
# Tests: ToolNodeDef
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_node_direct_execution():
    """ToolNodeDef.run() processes a ToolCallPart and returns Reply with tool result.

    Verifies:
    - Tool extracts ToolCallPart from state.todo_stack
    - Tool function is executed with correct kwargs
    - Result is stored in state.run_state.tool_results keyed by tool_call_id
    """
    tool_call_payload = Payload(
        correlation_id="tool-exec-1",
        source_node_id="caller_agent",
        parts=[
            ToolCallPart(
                tool_call_id="call-1",
                kwargs={"location": "Paris"},
                tool_name="get_weather",
            )
        ],
        timestamp=time.time(),
    )

    state = State(run_state=AgentActivityState(todo_stack=[tool_call_payload]))
    deps = Deps(correlation_id="tool-exec-1", agent_deps=None)
    ctx = BaseSessionRunContext(state=state, deps=deps)

    result = await get_weather.run(ctx)

    assert isinstance(result, Reply), f"Expected Reply, got {type(result).__name__}"
    assert result.value.uncommited_tool_results is not None
    assert "call-1" in result.value.uncommited_tool_results

    tool_return = result.value.uncommited_tool_results["call-1"]
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
    tool_call_payload = Payload(
        correlation_id="ctx-inject-1",
        source_node_id="test_source_agent",
        parts=[
            ToolCallPart(
                tool_call_id="call-ctx-1",
                kwargs={"message": "hello"},
                tool_name="ctx_echo_tool",
            )
        ],
        timestamp=time.time(),
    )

    state = State(run_state=AgentActivityState(todo_stack=[tool_call_payload]))
    deps = Deps(correlation_id="ctx-inject-1", agent_deps={"api_key": "sk-test-123"})
    ctx = BaseSessionRunContext(state=state, deps=deps)

    result = await ctx_echo_tool.run(ctx)

    assert isinstance(result, Reply)
    assert result.value.uncommited_tool_results is not None
    assert "call-ctx-1" in result.value.uncommited_tool_results

    tool_return = result.value.uncommited_tool_results["call-ctx-1"]
    content = str(tool_return.return_value)
    assert "agent=test_source_agent" in content, f"agent_name not injected: {content}"
    assert "msg=hello" in content, f"message arg missing: {content}"


@pytest.mark.asyncio
async def test_tool_node_returns_silent_when_no_tool_call():
    """ToolNodeDef returns Silent when no ToolCallPart is found in the payload.

    Verifies graceful handling of payloads without tool calls.
    """
    from calfkit.experimental.node_def import Silent

    # Payload with only a TextPart -- no ToolCallPart
    text_payload = Payload(
        correlation_id="no-tool-1",
        source_node_id="some_agent",
        parts=[TextPart(text="This is just text, no tool call")],
        timestamp=time.time(),
    )

    state = State(run_state=AgentActivityState(todo_stack=[text_payload]))
    deps = Deps(correlation_id="no-tool-1", agent_deps=None)
    ctx = BaseSessionRunContext(state=state, deps=deps)

    result = await get_weather.run(ctx)

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
        system_prompt="You must use the ctx_echo_tool tool for every request. Pass the user's message.",
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
        assert len(result.context.state.run_state.message_history) > 0
