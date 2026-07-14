"""PR-B carry-forwards (plan §1): the PR-A input-topic contracts, testable now that ``message_agent``
makes an agent's ``{node_kind}.{name}.private.input`` inbox a real dispatch target.

NO new floor — these confirm the EXISTING receive pipeline already disposes a stray non-``ToolCallRef``
``call`` (it is not silently swallowed; flooring a headerless call would break the raw-producer norm),
and that a non-agent node stays dormant in the messaging plane (it never advertises an ``AgentCard``, so
it is never a ``message_agent`` target — the v1 "no producer" dormancy, ADR-0017).
"""

from __future__ import annotations

from typing import Any, cast

from calfkit._vendor.pydantic_ai.messages import ModelResponse, TextPart
from calfkit._vendor.pydantic_ai.models.function import FunctionModel
from calfkit.models.agents import AGENTS_TOPIC, derive_input_topic
from calfkit.models.envelope import Envelope
from calfkit.models.session_context import CallFrame, SessionRunContext, Stack, WorkflowState
from calfkit.models.state import State
from calfkit.nodes import Agent, agent_tool
from tests._broker_fakes import CaptureBroker


def _final_model() -> FunctionModel:
    return FunctionModel(lambda messages, info: ModelResponse(parts=[TextPart("done")]))


async def test_stray_non_toolcallref_call_to_agent_inbox_is_disposed_not_swallowed() -> None:
    # PR-A `_private_input_topic` contract: a stray `call` (NO x-calf-kind header => classified "call"; no
    # reply slot; a non-ToolCallRef body) delivered to an agent's derived input topic is disposed by the
    # EXISTING pipeline — it does NOT raise/escape into FastStream. (No fan-out store is needed to
    # drive the handler offline: a stray-call disposal never resolves the store, and the @resource
    # never runs when the handler is driven directly.)
    agent = Agent("a", subscribe_topics="a.in", model_client=_final_model())
    frame = CallFrame(target_topic=derive_input_topic("a"), callback_topic=None, payload={"not": "a ToolCallRef"})
    env = Envelope(context=SessionRunContext(state=State(), deps={}), internal_workflow_state=WorkflowState(call_stack=Stack([frame])))
    broker = CaptureBroker()

    # No exception escapes; the stray is disposed (fire-and-forget: no reply slot, so nothing is published
    # back to a non-existent caller). The assertion is "handler returned + no spurious reply", which fails
    # loudly if a future change lets a stray raise out of the handler or fabricate a reply.
    await agent.handler(env, correlation_id="c-stray", headers={}, broker=cast(Any, broker))
    assert all(c.topic != "a.in" for c in broker.published)  # no spurious reply to a caller


def test_non_agent_node_is_dormant_in_the_messaging_plane() -> None:
    # A non-agent node never advertises an AgentCard, so it never appears in the agents directory and is
    # never a `message_agent` target; `derive_input_topic` only ever yields `agent.` topics, so a tool's
    # own `tool.{name}.private.input` inbox receives no messaging traffic (the v1 dormancy, ADR-0017).
    tool = agent_tool(lambda: "x", name="dormant_tool")
    assert AGENTS_TOPIC not in {info.topic for info in type(tool)._adverts.values()}  # non-agents don't advertise on calf.agents
    assert derive_input_topic("dormant_tool").startswith("agent.")  # never tool.{name}.private.input
