import asyncio
import os
import sys

import pytest
from dotenv import load_dotenv
from faststream.kafka import TestKafkaBroker

from calfkit._vendor.pydantic_ai import ModelRequest, ModelResponse, models
from calfkit.broker.broker import BrokerClient
from calfkit.models.event_envelope import EventEnvelope
from calfkit.models.groupchat import GroupchatDataModel
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.chat_node import ChatNode
from calfkit.nodes.groupchat_router_node import GroupchatNode
from calfkit.providers.pydantic_ai.openai import OpenAIModelClient
from calfkit.runners.service import NodesService

# Temporarily raise limit so the debug logging can show the full conversation
# before TestKafkaBroker's synchronous dispatch exhausts the stack.
sys.setrecursionlimit(10000)

load_dotenv()

# Ensure model requests are allowed for integration tests
models.ALLOW_MODEL_REQUESTS = True

skip_if_no_openai_key = pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"),
    reason="Skipping integration test: OPENAI_API_KEY not set in environment",
)


@pytest.fixture(scope="session")
def deploy_groupchat_broker():
    broker = BrokerClient()
    service = NodesService(broker)

    # 1. Deploy LLM chat node
    model_client = OpenAIModelClient("gpt-5-nano", reasoning_effort="low")
    chat_node = ChatNode(model_client)
    service.register_node(chat_node)

    # 2. Deploy shared agent router (handles all groupchat agent turns)
    router = AgentRouterNode(
        chat_node=ChatNode(),
        system_prompt="You are a helpful, concise assistant. Keep responses to 1-2 sentences.",
    )
    service.register_node(router)

    # 3. Create named agent references for groupchat topology.
    #    Both point to the same deployed router topic — this tests
    #    the groupchat orchestration (round-robin, gating, termination)
    #    without requiring per-agent topic routing.
    alice = AgentRouterNode(name="Alice")
    bob = AgentRouterNode(name="Bob")

    # 4. Deploy groupchat orchestrator
    groupchat_node = GroupchatNode(agent_nodes=[alice, bob])
    service.register_node(groupchat_node)

    return broker, groupchat_node, [alice, bob]


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_groupchat(deploy_groupchat_broker):
    """Test that a groupchat completes a full cycle: agents respond then skip to terminate."""
    broker, groupchat_node, agent_refs = deploy_groupchat_broker

    responses: list[EventEnvelope] = []
    done = asyncio.Event()

    @broker.subscriber(groupchat_node.publish_to_topic or "groupchat_generated")
    def collect(event_envelope: EventEnvelope):
        responses.append(event_envelope)
        if (
            event_envelope.groupchat_data is not None
            and event_envelope.groupchat_data.is_all_skipped()
        ):
            done.set()

    async with TestKafkaBroker(broker) as _:
        print(f"\n\n{'=' * 10}Start Groupchat{'=' * 10}")

        correlation_id = "test-groupchat-1"
        groupchat_data = GroupchatDataModel.create_new_groupchat(agent_refs)

        envelope = EventEnvelope(
            trace_id=correlation_id,
            groupchat_data=groupchat_data,
        )
        envelope.add_to_uncommitted_messages(
            ModelRequest.user_text_prompt("What is the capital of France?")
        )

        await broker.publish(
            envelope,
            topic=groupchat_node.subscribed_topic,
            correlation_id=correlation_id,
        )

        await asyncio.wait_for(done.wait(), timeout=60.0)

        # Groupchat should produce multiple observer snapshots
        assert len(responses) >= 3, f"Expected at least 3 turns, got {len(responses)}"

        # Verify that exactly one response is the termination envelope.
        # NOTE: TestKafkaBroker delivers @publish_to return values in LIFO
        # order (synchronous dispatch), so the termination envelope is NOT
        # necessarily the last item — don't rely on ordering.
        terminated = [
            r
            for r in responses
            if r.groupchat_data is not None and r.groupchat_data.is_all_skipped()
        ]
        assert len(terminated) == 1, (
            f"Expected exactly 1 termination envelope, got {len(terminated)}"
        )

        for i, r in enumerate(responses):
            msg = r.latest_message_in_history
            if isinstance(msg, ModelResponse) and msg.text:
                print(f"  Turn {i + 1}: {msg.text[:150]}")
            else:
                print(f"  Turn {i + 1}: (no text response)")

        print(f"\nTotal turns: {len(responses)}")
        print(f"{'=' * 10}End Groupchat{'=' * 10}")
