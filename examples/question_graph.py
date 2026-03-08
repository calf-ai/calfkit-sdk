"""Choreography-based multi-agent workflow: Question Graph.

Ported from pydantic_ai's ``pydantic_graph`` example. Three agents (ask, answer,
evaluate) are wired via topic topology — no orchestrator, pure choreography.

Topology::

    Client → ask.agent.input
    ask.agent.output → AnswerNode → evaluate.agent.input
    evaluate.agent.output (filter: correct) → EndNode → workflow.end
    evaluate.agent.output (filter: wrong)  → ReprimandNode → ask.agent.input (loop)
    workflow.end → Client

Usage::

    uv run python examples/question_graph.py
"""

from __future__ import annotations

import asyncio
import os
from typing import Annotated

from dotenv import load_dotenv
from faststream import Context
from pydantic import BaseModel

from calfkit.broker.broker import BrokerClient
from calfkit.models.event_envelope import EventEnvelope
from calfkit.models.payloads import RouterPayload
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.base_node import BaseNode, publish_to, subscribe_to
from calfkit.nodes.chat_node import ChatNode
from calfkit.providers.pydantic_ai.openai import OpenAIModelClient
from calfkit.runners.service import NodesService
from calfkit.runners.service_client import RouterServiceClient

load_dotenv()


# --- Data model ---


class EvaluationOutput(BaseModel):
    correct: bool
    comment: str


# --- Filter functions ---
# Filters receive an EventEnvelope — the framework adapts them for FastStream's
# raw message format automatically.


def filter_correct(envelope: EventEnvelope) -> bool:
    """Pass only envelopes where the evaluation payload has correct=True."""
    if envelope.payload is None:
        return False
    payload = envelope.payload if isinstance(envelope.payload, dict) else {}
    return payload.get("correct", False) is True


def filter_wrong(envelope: EventEnvelope) -> bool:
    """Pass only envelopes where the evaluation payload has correct=False."""
    if envelope.payload is None:
        return False
    payload = envelope.payload if isinstance(envelope.payload, dict) else {}
    return payload.get("correct", False) is False


# --- Custom nodes ---


class AnswerNode(BaseNode):
    """Subscribes to ask_agent output, hardcodes an answer, publishes to evaluate_agent input."""

    @subscribe_to("ask.agent.output")
    @publish_to("evaluate.agent.input")
    async def on_answer(
        self,
        event_envelope: EventEnvelope,
        correlation_id: Annotated[str, Context()],
    ) -> EventEnvelope:
        # The ask_agent's text response is the trivia question.
        # Hardcode an answer for demonstration.
        event_envelope.payload = {"answer": "Paris"}
        return event_envelope


class EndNode(BaseNode):
    """Terminal node — receives correct evaluations and publishes to workflow.end."""

    @subscribe_to("evaluate.agent.output", filter=filter_correct)
    @publish_to("workflow.end")
    async def on_end(
        self,
        event_envelope: EventEnvelope,
        correlation_id: Annotated[str, Context()],
    ) -> EventEnvelope:
        return event_envelope


class ReprimandNode(BaseNode):
    """Loop node — receives wrong evaluations and routes back to ask_agent."""

    @subscribe_to("evaluate.agent.output", filter=filter_wrong)
    @publish_to("ask.agent.input")
    async def on_wrong(
        self,
        event_envelope: EventEnvelope,
        correlation_id: Annotated[str, Context()],
        broker: Annotated[object, Context()],
    ) -> EventEnvelope:
        # Extract the evaluation comment
        payload = event_envelope.payload if isinstance(event_envelope.payload, dict) else {}
        comment = payload.get("comment", "Wrong answer")

        # Construct a fresh RouterPayload to re-enter ask_agent properly
        event_envelope.payload = RouterPayload(
            user_prompt=(
                f"The previous answer was wrong. Evaluator said: '{comment}'. "
                "Please ask a different trivia question."
            ),
        )
        # Reset state so ask_agent treats this as a new request
        from calfkit.models.event_envelope import AgentState

        event_envelope.state = AgentState()
        return event_envelope


# --- Build and run ---


async def main() -> None:
    broker = BrokerClient()
    service = NodesService(broker)

    model_client = OpenAIModelClient(
        os.environ.get("TEST_LLM_MODEL_NAME", "gpt-4o-mini"),
    )

    # 1. Ask agent — generates trivia questions
    ask_chat = ChatNode(model_client, name="ask_chat")
    service.register_node(ask_chat)

    ask_agent = AgentRouterNode(
        chat_node=ask_chat,
        name="ask_agent",
        input_topic="ask.agent.input",
        output_topic="ask.agent.output",
        system_prompt="You are a trivia question generator. Generate one trivia question.",
    )
    service.register_node(ask_agent)

    # 2. Answer node (custom, no LLM)
    answer_node = AnswerNode(name="answer_node")
    service.register_node(answer_node)

    # 3. Evaluate agent — evaluates the answer
    eval_chat = ChatNode(model_client, name="eval_chat", output_type=EvaluationOutput)
    service.register_node(eval_chat)

    evaluate_agent = AgentRouterNode(
        chat_node=eval_chat,
        name="evaluate_agent",
        input_topic="evaluate.agent.input",
        output_topic="evaluate.agent.output",
        system_prompt=(
            "You are a trivia answer evaluator. "
            "Given a question and answer, determine if the answer is correct. "
            "Respond with your evaluation."
        ),
        input_fn=lambda payload: (
            f"Question context from previous agent. "
            f"The proposed answer is: {payload.get('answer', 'unknown') if isinstance(payload, dict) else payload}. "
            f"Is this correct?"
        ),
    )
    service.register_node(evaluate_agent)

    # 4. End node
    end_node = EndNode(name="end_node")
    service.register_node(end_node)

    # 5. Reprimand node (loop back)
    reprimand_node = ReprimandNode(name="reprimand_node")
    service.register_node(reprimand_node)

    # Run
    from faststream.kafka import TestKafkaBroker

    async with TestKafkaBroker(broker) as _:
        client = RouterServiceClient(broker, ask_agent)
        response = await client.request(
            user_prompt="Ask me a trivia question about world capitals.",
        )

        final_msg = await asyncio.wait_for(response.get_final_response(), timeout=60.0)
        print(f"Final response: {final_msg}")


if __name__ == "__main__":
    asyncio.run(main())
