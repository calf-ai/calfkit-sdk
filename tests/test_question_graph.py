"""Tests for choreography-based multi-agent workflows.

Tests the state split (AgentState / RunState / AgentSnapshot), input_fn,
filter-based conditional routing, and the full question graph flow.

Uses FunctionModel + TestKafkaBroker for synchronous, deterministic testing.
"""

import asyncio
import json
import os
from typing import Annotated

import pytest
from dotenv import load_dotenv
from faststream import Context
from faststream.kafka import TestKafkaBroker
from pydantic import BaseModel

from calfkit._vendor.pydantic_ai import (
    ModelMessage,
    ModelResponse,
    TextPart,
    ToolCallPart,
    models,
)
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.broker.broker import BrokerClient
from calfkit.models.event_envelope import AgentSnapshot, AgentState, EventEnvelope, RunState
from calfkit.models.payloads import RouterPayload
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.base_node import BaseNode, publish_to, subscribe_to
from calfkit.nodes.chat_node import ChatNode
from calfkit.runners.service import NodesService
from calfkit.runners.service_client import RouterServiceClient
from tests.utils import wait_for_condition

load_dotenv()


def _get_payload(envelope: EventEnvelope) -> dict | None:
    """Extract payload dict from an EventEnvelope."""
    if envelope.payload is None:
        return None
    return envelope.payload if isinstance(envelope.payload, dict) else None


@pytest.fixture(autouse=True)
def block_model_requests():
    """Block actual model requests during unit tests."""
    original_value = models.ALLOW_MODEL_REQUESTS
    models.ALLOW_MODEL_REQUESTS = False
    yield
    models.ALLOW_MODEL_REQUESTS = original_value


# --- Shared data model ---


class EvaluationOutput(BaseModel):
    correct: bool
    comment: str


# --- Unit tests for state split ---


def test_agent_state_basic():
    """AgentState has all the expected fields and methods."""
    state = AgentState()
    assert state.message_history == []
    assert state.uncommitted_messages == []
    assert state.pending_tool_calls == []
    assert state.final_response is False
    assert state.instructions is None
    assert state.agent_name is None
    assert state.model_request_params is None
    assert state.latest_message_in_history is None
    assert state.is_end_of_turn is False
    assert state.has_uncommitted_messages is False


def test_run_state_basic():
    """RunState has delegation_stack and agent_history."""
    run_state = RunState()
    assert run_state.delegation_stack == []
    assert run_state.agent_history == []


def test_agent_snapshot_basic():
    """AgentSnapshot stores agent name and state."""
    state = AgentState()
    state.mark_as_end_of_turn()
    snapshot = AgentSnapshot(agent_name="test_agent", agent_state=state)
    assert snapshot.agent_name == "test_agent"
    assert snapshot.agent_state.final_response is True


def test_envelope_has_run_state():
    """EventEnvelope has both state (AgentState) and run_state (RunState)."""
    envelope = EventEnvelope(trace_id="test-1")
    assert isinstance(envelope.state, AgentState)
    assert isinstance(envelope.run_state, RunState)


def test_envelope_run_state_serialization():
    """run_state is always included in serialization."""
    envelope = EventEnvelope(trace_id="test-2")
    dumped = envelope.model_dump()
    assert "state" in dumped
    assert "run_state" in dumped


def test_envelope_run_state_with_snapshot():
    """AgentSnapshot roundtrips through EventEnvelope serialization."""
    envelope = EventEnvelope(trace_id="test-3")
    snapshot = AgentSnapshot(agent_name="agent_a", agent_state=AgentState())
    envelope.run_state.agent_history = [snapshot]

    dumped = envelope.model_dump()
    assert len(dumped["run_state"]["agent_history"]) == 1
    assert dumped["run_state"]["agent_history"][0]["agent_name"] == "agent_a"

    restored = EventEnvelope.model_validate(dumped)
    assert len(restored.run_state.agent_history) == 1
    assert restored.run_state.agent_history[0].agent_name == "agent_a"


# --- Unit test: snapshot is pushed in _reply_to_sender ---


@pytest.mark.asyncio
async def test_snapshot_pushed_on_reply():
    """When an agent finishes, a snapshot is pushed to run_state.agent_history."""

    def echo_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart("response")])

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = FunctionModel(echo_model)
    chat_node = ChatNode(model_client, name="snap_chat")
    service.register_node(chat_node)

    router_node = AgentRouterNode(
        chat_node=chat_node,
        name="snap_agent",
    )
    service.register_node(router_node)

    response_store: dict[str, asyncio.Queue[EventEnvelope]] = {}

    @broker.subscriber(router_node.publish_to_topic or "default_collect")
    def collect_response(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(event_envelope)

    async with TestKafkaBroker(broker) as _:
        trace_id = await router_node.invoke(
            user_prompt="Hello",
            broker=broker,
            final_response_topic="final_response",
            correlation_id="test-snap-1",
        )

        await wait_for_condition(lambda: trace_id in response_store, timeout=5.0)
        result = await response_store[trace_id].get()

        # Verify snapshot was pushed
        assert len(result.run_state.agent_history) == 1
        snapshot = result.run_state.agent_history[0]
        assert snapshot.agent_name == "snap_agent"
        assert result.state.is_end_of_turn


# --- Unit test: state reset on choreography ---


@pytest.mark.asyncio
async def test_state_reset_on_choreography():
    """When a downstream agent receives an envelope with final_response=True,
    agent state is reset to fresh AgentState."""

    call_count = 0

    def downstream_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        nonlocal call_count
        call_count += 1
        # Downstream agent should get clean message history
        return ModelResponse(parts=[TextPart("downstream-response")])

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = FunctionModel(downstream_model)
    chat_node = ChatNode(model_client, name="downstream_chat")
    service.register_node(chat_node)

    downstream_agent = AgentRouterNode(
        chat_node=chat_node,
        name="downstream_agent",
        input_fn=lambda payload: f"Process: {payload}",
    )
    service.register_node(downstream_agent)

    response_store: dict[str, asyncio.Queue[EventEnvelope]] = {}

    @broker.subscriber(downstream_agent.publish_to_topic or "default_collect")
    def collect_response(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(event_envelope)

    async with TestKafkaBroker(broker) as _:
        # Simulate an upstream agent's completed envelope
        upstream_envelope = EventEnvelope(
            trace_id="choreography-1",
            payload={"answer": "Paris"},
            final_response_topic="final",
        )
        upstream_envelope.state.final_response = True
        # Add some "dirty" state from the upstream agent
        upstream_envelope.state.agent_name = "upstream_agent"
        upstream_envelope.state.instructions = "upstream instructions"

        await broker.publish(
            upstream_envelope,
            topic=downstream_agent.entrypoint_topic,
            correlation_id="choreography-1",
        )

        await wait_for_condition(lambda: "choreography-1" in response_store, timeout=5.0)
        result = await response_store["choreography-1"].get()

        # Downstream agent processed with its own clean state
        assert call_count == 1
        assert isinstance(result.state.latest_message_in_history, ModelResponse)
        assert "downstream-response" in str(result.state.latest_message_in_history.parts[0])


# --- Unit test: input_fn transforms payload ---


@pytest.mark.asyncio
async def test_input_fn_transforms_payload():
    """input_fn converts upstream structured output into a user_prompt string."""
    captured_messages: list[list[ModelMessage]] = []

    def capturing_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        captured_messages.append(messages)
        return ModelResponse(parts=[TextPart("processed")])

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = FunctionModel(capturing_model)
    chat_node = ChatNode(model_client, name="inputfn_chat")
    service.register_node(chat_node)

    def my_input_fn(payload: dict) -> str:
        return f"Answer to evaluate: {payload.get('answer', 'unknown')}"

    agent = AgentRouterNode(
        chat_node=chat_node,
        name="inputfn_agent",
        input_fn=my_input_fn,
    )
    service.register_node(agent)

    response_store: dict[str, asyncio.Queue[EventEnvelope]] = {}

    @broker.subscriber(agent.publish_to_topic or "default_collect")
    def collect_response(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(event_envelope)

    async with TestKafkaBroker(broker) as _:
        # Simulate upstream completed envelope
        upstream = EventEnvelope(
            trace_id="inputfn-1",
            payload={"answer": "42"},
            final_response_topic="final",
        )
        upstream.state.final_response = True

        await broker.publish(
            upstream,
            topic=agent.entrypoint_topic,
            correlation_id="inputfn-1",
        )

        await wait_for_condition(lambda: "inputfn-1" in response_store, timeout=5.0)
        result = await response_store["inputfn-1"].get()

        # Verify the model received the transformed prompt
        assert len(captured_messages) == 1
        first_call_msgs = captured_messages[0]
        # Should have one user message with the transformed prompt
        assert len(first_call_msgs) == 1
        user_msg_content = str(first_call_msgs[0].parts[0])
        assert "Answer to evaluate: 42" in user_msg_content


# --- Unit test: filter-based conditional routing ---


def _filter_correct(envelope: EventEnvelope) -> bool:
    payload = _get_payload(envelope)
    if payload is None:
        return False
    return payload.get("correct", False) is True


def _filter_wrong(envelope: EventEnvelope) -> bool:
    payload = _get_payload(envelope)
    if payload is None:
        return False
    return payload.get("correct", False) is False


class CorrectEndNode(BaseNode):
    @subscribe_to("eval.output", filter=_filter_correct)
    @publish_to("workflow.correct")
    async def on_correct(
        self, event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]
    ) -> EventEnvelope:
        event_envelope.payload = {"routed_to": "correct"}
        return event_envelope


class WrongEndNode(BaseNode):
    @subscribe_to("eval.output", filter=_filter_wrong)
    @publish_to("workflow.wrong")
    async def on_wrong(
        self, event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]
    ) -> EventEnvelope:
        event_envelope.payload = {"routed_to": "wrong"}
        return event_envelope


@pytest.mark.asyncio
async def test_filter_based_routing():
    """Filter functions on @subscribe_to correctly route messages conditionally."""

    broker = BrokerClient()
    service = NodesService(broker)

    correct_node = CorrectEndNode(name="correct_node")
    wrong_node = WrongEndNode(name="wrong_node")
    service.register_node(correct_node)
    service.register_node(wrong_node)

    correct_store: list[EventEnvelope] = []
    wrong_store: list[EventEnvelope] = []

    @broker.subscriber("workflow.correct")
    def collect_correct(event_envelope: EventEnvelope):
        correct_store.append(event_envelope)

    @broker.subscriber("workflow.wrong")
    def collect_wrong(event_envelope: EventEnvelope):
        wrong_store.append(event_envelope)

    async with TestKafkaBroker(broker) as _:
        # Publish a "correct" evaluation
        correct_env = EventEnvelope(
            trace_id="filter-1",
            payload={"correct": True, "comment": "Well done"},
        )
        await broker.publish(correct_env, topic="eval.output", correlation_id="filter-1")

        await wait_for_condition(lambda: len(correct_store) == 1, timeout=5.0)
        assert correct_store[0].payload["routed_to"] == "correct"
        assert len(wrong_store) == 0

        # Publish a "wrong" evaluation
        wrong_env = EventEnvelope(
            trace_id="filter-2",
            payload={"correct": False, "comment": "Incorrect"},
        )
        await broker.publish(wrong_env, topic="eval.output", correlation_id="filter-2")

        await wait_for_condition(lambda: len(wrong_store) == 1, timeout=5.0)
        assert wrong_store[0].payload["routed_to"] == "wrong"
        assert len(correct_store) == 1  # No new correct messages


# --- Full graph flow test ---


@pytest.mark.asyncio
async def test_full_graph_flow_correct_path():
    """Full question graph: ask → answer → evaluate(correct) → end."""

    def ask_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart("What is the capital of France?")])

    def evaluate_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name="final_result",
                    args=json.dumps({"correct": True, "comment": "Paris is correct!"}),
                    tool_call_id="eval-1",
                )
            ]
        )

    broker = BrokerClient()
    service = NodesService(broker)

    # Ask agent
    ask_model_client = FunctionModel(ask_model)
    ask_chat = ChatNode(ask_model_client, name="ask_chat")
    service.register_node(ask_chat)

    ask_agent = AgentRouterNode(
        chat_node=ask_chat,
        name="ask_agent",
        input_topic="ask.agent.input",
        output_topic="ask.agent.output",
        system_prompt="Generate a trivia question.",
    )
    service.register_node(ask_agent)

    # Answer node
    class AnswerNode(BaseNode):
        @subscribe_to("ask.agent.output")
        @publish_to("evaluate.agent.input")
        async def on_answer(
            self, event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]
        ) -> EventEnvelope:
            event_envelope.payload = {"answer": "Paris"}
            return event_envelope

    answer_node = AnswerNode(name="answer_node")
    service.register_node(answer_node)

    # Evaluate agent
    eval_model_client = FunctionModel(evaluate_model)
    eval_chat = ChatNode(eval_model_client, name="eval_chat", output_type=EvaluationOutput)
    service.register_node(eval_chat)

    evaluate_agent = AgentRouterNode(
        chat_node=eval_chat,
        name="evaluate_agent",
        input_topic="evaluate.agent.input",
        output_topic="evaluate.agent.output",
        system_prompt="Evaluate the trivia answer.",
        input_fn=lambda payload: (
            f"The answer is: {payload.get('answer', 'unknown') if isinstance(payload, dict) else payload}. "
            "Is this correct?"
        ),
    )
    service.register_node(evaluate_agent)

    # End node (correct path)
    def end_filter(envelope: EventEnvelope) -> bool:
        payload = _get_payload(envelope)
        if payload is None:
            return False
        return payload.get("correct", False) is True

    class EndNode(BaseNode):
        @subscribe_to("evaluate.agent.output", filter=end_filter)
        @publish_to("workflow.end")
        async def on_end(
            self, event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]
        ) -> EventEnvelope:
            return event_envelope

    end_node = EndNode(name="end_node")
    service.register_node(end_node)

    end_store: list[EventEnvelope] = []

    @broker.subscriber("workflow.end")
    def collect_end(event_envelope: EventEnvelope):
        end_store.append(event_envelope)

    async with TestKafkaBroker(broker) as _:
        trace_id = await ask_agent.invoke(
            user_prompt="Ask a trivia question about capitals.",
            broker=broker,
            final_response_topic="client.response.graph1",
            correlation_id="graph-flow-1",
        )

        await wait_for_condition(lambda: len(end_store) >= 1, timeout=10.0)

        final = end_store[0]
        assert final.payload is not None
        payload = final.payload if isinstance(final.payload, dict) else {}
        assert payload.get("correct") is True
        assert "Paris" in payload.get("comment", "")


# --- Full graph flow test: wrong path with loop ---


@pytest.mark.asyncio
async def test_full_graph_flow_wrong_then_correct():
    """Full question graph: ask → answer → evaluate(wrong) → reprimand → ask → answer → evaluate(correct) → end."""

    ask_call_count = 0
    eval_call_count = 0

    def ask_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        nonlocal ask_call_count
        ask_call_count += 1
        if ask_call_count == 1:
            return ModelResponse(parts=[TextPart("What is 2+2?")])
        else:
            return ModelResponse(parts=[TextPart("What is the capital of France?")])

    def evaluate_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        nonlocal eval_call_count
        eval_call_count += 1
        if eval_call_count == 1:
            # First evaluation: wrong
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name="final_result",
                        args=json.dumps({"correct": False, "comment": "Paris is not 4"}),
                        tool_call_id="eval-1",
                    )
                ]
            )
        else:
            # Second evaluation: correct
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name="final_result",
                        args=json.dumps({"correct": True, "comment": "Paris is correct!"}),
                        tool_call_id="eval-2",
                    )
                ]
            )

    broker = BrokerClient()
    service = NodesService(broker)

    # Ask agent
    ask_model_client = FunctionModel(ask_model)
    ask_chat = ChatNode(ask_model_client, name="loop_ask_chat")
    service.register_node(ask_chat)

    ask_agent = AgentRouterNode(
        chat_node=ask_chat,
        name="loop_ask_agent",
        input_topic="loop.ask.agent.input",
        output_topic="loop.ask.agent.output",
        system_prompt="Generate a trivia question.",
    )
    service.register_node(ask_agent)

    # Answer node
    class AnswerNode(BaseNode):
        @subscribe_to("loop.ask.agent.output")
        @publish_to("loop.evaluate.agent.input")
        async def on_answer(
            self, event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]
        ) -> EventEnvelope:
            event_envelope.payload = {"answer": "Paris"}
            return event_envelope

    answer_node = AnswerNode(name="loop_answer_node")
    service.register_node(answer_node)

    # Evaluate agent
    eval_model_client = FunctionModel(evaluate_model)
    eval_chat = ChatNode(eval_model_client, name="loop_eval_chat", output_type=EvaluationOutput)
    service.register_node(eval_chat)

    evaluate_agent = AgentRouterNode(
        chat_node=eval_chat,
        name="loop_evaluate_agent",
        input_topic="loop.evaluate.agent.input",
        output_topic="loop.evaluate.agent.output",
        system_prompt="Evaluate the trivia answer.",
        input_fn=lambda payload: (
            f"The answer is: {payload.get('answer', 'unknown') if isinstance(payload, dict) else payload}. "
            "Is this correct?"
        ),
    )
    service.register_node(evaluate_agent)

    # End node (correct path)
    def end_filter(envelope: EventEnvelope) -> bool:
        payload = _get_payload(envelope)
        if payload is None:
            return False
        return payload.get("correct", False) is True

    class EndNode(BaseNode):
        @subscribe_to("loop.evaluate.agent.output", filter=end_filter)
        @publish_to("loop.workflow.end")
        async def on_end(
            self, event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]
        ) -> EventEnvelope:
            return event_envelope

    end_node = EndNode(name="loop_end_node")
    service.register_node(end_node)

    # Reprimand node (wrong path → loop back)
    def wrong_filter(envelope: EventEnvelope) -> bool:
        payload = _get_payload(envelope)
        if payload is None:
            return False
        return payload.get("correct", False) is False

    class ReprimandNode(BaseNode):
        @subscribe_to("loop.evaluate.agent.output", filter=wrong_filter)
        @publish_to("loop.ask.agent.input")
        async def on_wrong(
            self, event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]
        ) -> EventEnvelope:
            payload = event_envelope.payload if isinstance(event_envelope.payload, dict) else {}
            comment = payload.get("comment", "Wrong")

            event_envelope.payload = RouterPayload(
                user_prompt=f"Previous answer was wrong: '{comment}'. Ask a different question.",
            )
            event_envelope.state = AgentState()
            return event_envelope

    reprimand_node = ReprimandNode(name="loop_reprimand_node")
    service.register_node(reprimand_node)

    end_store: list[EventEnvelope] = []

    @broker.subscriber("loop.workflow.end")
    def collect_end(event_envelope: EventEnvelope):
        end_store.append(event_envelope)

    async with TestKafkaBroker(broker) as _:
        trace_id = await ask_agent.invoke(
            user_prompt="Ask a trivia question.",
            broker=broker,
            final_response_topic="client.response.loop1",
            correlation_id="loop-flow-1",
        )

        await wait_for_condition(lambda: len(end_store) >= 1, timeout=10.0)

        # Both ask and evaluate should have been called twice (one loop)
        assert ask_call_count == 2
        assert eval_call_count == 2

        final = end_store[0]
        assert final.payload is not None
        payload = final.payload if isinstance(final.payload, dict) else {}
        assert payload.get("correct") is True


# --- Integration test (requires OPENAI_API_KEY) ---


skip_if_no_openai_key = pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"),
    reason="Skipping integration test: OPENAI_API_KEY not set in environment",
)


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_question_graph_integration():
    """End-to-end question graph with real LLM."""
    import logging

    logger = logging.getLogger("integ_graph_debug")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)

    loop_count = 0

    models.ALLOW_MODEL_REQUESTS = True

    from calfkit.providers.pydantic_ai.openai import OpenAIModelClient

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = OpenAIModelClient(
        os.environ["TEST_LLM_MODEL_NAME"],
    )

    # Ask agent
    ask_chat = ChatNode(model_client, name="integ_ask_chat")
    service.register_node(ask_chat)

    ask_agent = AgentRouterNode(
        chat_node=ask_chat,
        name="integ_ask_agent",
        input_topic="integ.ask.input",
        output_topic="integ.ask.output",
        system_prompt=(
            "You are a trivia question generator. Generate exactly one trivia question "
            "about world geography. Just ask the question, nothing else."
        ),
    )
    service.register_node(ask_agent)

    # Answer node — always answers "Paris"
    class IntegAnswerNode(BaseNode):
        @subscribe_to("integ.ask.output")
        @publish_to("integ.eval.input")
        async def on_answer(
            self, event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]
        ) -> EventEnvelope:
            logger.debug(
                f"[ANSWER_NODE] Received on integ.ask.output | "
                f"final_response_topic={event_envelope.final_response_topic} | "
                f"state.final_response={event_envelope.state.final_response} | "
                f"payload_type={type(event_envelope.payload).__name__}"
            )
            event_envelope.payload = {"answer": "Paris"}
            logger.debug("[ANSWER_NODE] Publishing to integ.eval.input with answer='Paris'")
            return event_envelope

    answer_node = IntegAnswerNode(name="integ_answer_node")
    service.register_node(answer_node)

    # Evaluate agent
    eval_chat = ChatNode(model_client, name="integ_eval_chat", output_type=EvaluationOutput)
    service.register_node(eval_chat)

    evaluate_agent = AgentRouterNode(
        chat_node=eval_chat,
        name="integ_eval_agent",
        input_topic="integ.eval.input",
        output_topic="integ.eval.output",
        system_prompt=(
            "You are a trivia answer evaluator. Evaluate whether the given answer is correct "
            "for the trivia question that was asked. Respond with your evaluation."
        ),
        input_fn=lambda payload: (
            f"The proposed answer is: {payload.get('answer', 'unknown') if isinstance(payload, dict) else payload}. "
            "Is this correct?"
        ),
    )
    service.register_node(evaluate_agent)

    # End node
    def end_filter(envelope: EventEnvelope) -> bool:
        payload = _get_payload(envelope)
        result = payload.get("correct", False) is True if payload else False
        logger.debug(f"[END_FILTER] payload={payload} → result={result}")
        return result

    def wrong_filter(envelope: EventEnvelope) -> bool:
        payload = _get_payload(envelope)
        result = payload.get("correct", False) is False if payload else False
        logger.debug(f"[WRONG_FILTER] payload={payload} → result={result}")
        return result

    class IntegEndNode(BaseNode):
        @subscribe_to("integ.eval.output", filter=end_filter)
        @publish_to("integ.workflow.end")
        async def on_end(
            self, event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]
        ) -> EventEnvelope:
            logger.debug(
                f"[END_NODE] Correct path reached | payload={event_envelope.payload} | "
                f"final_response_topic={event_envelope.final_response_topic}"
            )
            return event_envelope

    class IntegReprimandNode(BaseNode):
        @subscribe_to("integ.eval.output", filter=wrong_filter)
        @publish_to("integ.ask.input")
        async def on_wrong(
            self, event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]
        ) -> EventEnvelope:
            nonlocal loop_count
            loop_count += 1
            payload = event_envelope.payload if isinstance(event_envelope.payload, dict) else {}
            comment = payload.get("comment", "Wrong")
            logger.debug(
                f"[REPRIMAND_NODE] Wrong path (loop #{loop_count}) | "
                f"payload={payload} | comment={comment} | "
                f"final_response_topic={event_envelope.final_response_topic}"
            )
            event_envelope.payload = RouterPayload(
                user_prompt=f"Wrong: '{comment}'. Ask a trivia question about the capital of France.",
            )
            event_envelope.state = AgentState()
            logger.debug(f"[REPRIMAND_NODE] Publishing RouterPayload to integ.ask.input")
            return event_envelope

    end_node = IntegEndNode(name="integ_end_node")
    reprimand_node = IntegReprimandNode(name="integ_reprimand_node")
    service.register_node(end_node)
    service.register_node(reprimand_node)

    end_store: list[EventEnvelope] = []

    @broker.subscriber("integ.workflow.end")
    def collect_end(event_envelope: EventEnvelope):
        logger.debug(f"[COLLECT_END] Final result received | payload={event_envelope.payload}")
        end_store.append(event_envelope)

    logger.debug("[TEST] Starting integration test")

    async with TestKafkaBroker(broker) as _:
        logger.debug("[TEST] Invoking ask_agent")
        await ask_agent.invoke(
            user_prompt="Ask a trivia question about the capital of France.",
            broker=broker,
            final_response_topic="client.response.integ1",
            correlation_id="integ-graph-1",
        )

        logger.debug(f"[TEST] invoke returned, loop_count={loop_count}")
        await wait_for_condition(lambda: len(end_store) >= 1, timeout=60.0)

        logger.debug(f"[TEST] Done. Total loops={loop_count}, end_store count={len(end_store)}")
        final = end_store[0]
        assert final.payload is not None
        payload = final.payload if isinstance(final.payload, dict) else {}
        assert payload.get("correct") is True
