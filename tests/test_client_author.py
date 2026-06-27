"""Tests for the optional human ``author`` kwarg on the client gateway (design §8).

``author=`` is mapped onto the staged ``UserPromptPart.name`` by the gateway's verbs (it must not
be silently dropped), and ``execute`` forwards it. These drive the real public flow over an offline
``TestKafkaBroker`` — a capture subscriber on the agent topic records the published session ``State``.
"""

from __future__ import annotations

import asyncio
from typing import Annotated, Any

from faststream import Context
from faststream.kafka import TestKafkaBroker

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, HDR_KIND
from calfkit._vendor.pydantic_ai.messages import ModelRequest, UserPromptPart
from calfkit.client import Client
from calfkit.models import CallFrameStack, Envelope, SessionRunContext, WorkflowState
from calfkit.models.payload import TextPart
from calfkit.models.reply import ReturnMessage
from calfkit.models.state import State

_AGENT_TOPIC = "agent.x.private.input"


def _staged_user_part(state: State) -> UserPromptPart:
    msg = state.uncommitted_message
    assert isinstance(msg, ModelRequest)
    part = msg.parts[0]
    assert isinstance(part, UserPromptPart)
    return part


def _register_capture_agent(client: Client, *, reply: bool = False) -> list[State]:
    """Capture the published session State on the agent topic; optionally echo a return to the inbox
    (so an ``execute`` round-trip can complete)."""
    captured: list[State] = []

    @client._broker.subscriber(_AGENT_TOPIC)
    async def agent_node(env: Envelope, correlation_id: Annotated[str, Context()]) -> None:
        captured.append(env.context.state)
        if reply:
            r = Envelope(
                context=SessionRunContext(state=State(), deps={}),
                internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
                reply=ReturnMessage(in_reply_to=None, tag=None, parts=[TextPart(text="ok")]),
            )
            headers: dict[str, Any] = {HDR_KIND: "return", HDR_EMITTER: "agent.x", HDR_EMITTER_KIND: "agent"}
            await client._broker.publish(r, client.inbox_topic, correlation_id=correlation_id, headers=headers)

    return captured


async def test_start_author_stamps_user_prompt_name() -> None:
    client = Client.connect("localhost:9092", inbox_topic="inbox")
    captured = _register_capture_agent(client)
    async with TestKafkaBroker(client._broker):
        await client.agent(topic=_AGENT_TOPIC).start("hello", author="Alice")
    assert _staged_user_part(captured[0]).name == "Alice"


async def test_start_no_author_leaves_name_none() -> None:
    client = Client.connect("localhost:9092", inbox_topic="inbox")
    captured = _register_capture_agent(client)
    async with TestKafkaBroker(client._broker):
        await client.agent(topic=_AGENT_TOPIC).start("hello")
    assert _staged_user_part(captured[0]).name is None


async def test_execute_forwards_author_to_start() -> None:
    client = Client.connect("localhost:9092", inbox_topic="inbox")
    captured = _register_capture_agent(client, reply=True)
    async with TestKafkaBroker(client._broker):
        await asyncio.wait_for(client.agent(topic=_AGENT_TOPIC).execute("hello", author="Bob"), timeout=1.0)
    assert _staged_user_part(captured[0]).name == "Bob"  # execute did not drop author= before publishing
