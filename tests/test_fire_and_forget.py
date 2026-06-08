"""Unit-scope contracts for the fire-and-forget (null-callback) terminal, plus
end-to-end tests for the true one-way ``Client.emit_to_node`` method.

The unit tests drive ``BaseNodeDef._publish_action`` directly with a spy broker
(mirroring ``tests/test_co_tenant_tool_isolation.py``) so the worker's terminal
callback-guard is isolated from the messaging layer.

A bottom call frame with ``callback_topic=None`` represents a true
fire-and-forget invocation: there is no requester to return a point-to-point
reply to. The worker must therefore NOT publish to a callback topic on the
terminal ``ReturnCall`` — yet it must still build and return the terminal
envelope so the result is broadcast to the node's ``publish_topic`` (the
traceability channel via the worker's ``@publisher`` wiring).

The end-to-end tests mirror ``tests/test_headers.py`` (TestKafkaBroker +
FunctionModel + ``prepare_worker``): they prove ``emit_to_node`` allocates zero
per-call client state and that the broadcast ``publish_topic`` channel still
records the terminal result even though the point-to-point callback is
suppressed (no reply future, no "no pending future" warning).
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, ToolCallPart, ToolReturnPart
from calfkit._vendor.pydantic_ai.messages import TextPart as ModelTextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client, NodeResult
from calfkit.models.actions import ReturnCall
from calfkit.models.envelope import Envelope
from calfkit.models.payload import TextPart
from calfkit.models.session_context import (
    CallFrame,
    CallFrameStack,
    SessionRunContext,
    WorkflowState,
)
from calfkit.models.state import State
from calfkit.nodes import Agent, agent_tool, consumer
from calfkit.nodes.base import BaseNodeDef
from calfkit.worker import Worker
from tests.providers import prepare_worker

REPLY_DISPATCHER_LOGGER = "calfkit.client.reply_dispatcher"


class _TerminalNode(BaseNodeDef):
    """Minimal concrete node so ``_publish_action`` can be exercised in isolation."""

    async def run(self, ctx: SessionRunContext) -> Any:
        return ReturnCall(state=ctx.state)


def _make_node() -> _TerminalNode:
    return _TerminalNode(
        node_id="terminal_node",
        subscribe_topics=["terminal_node.input"],
        publish_topic="terminal_node.output",
    )


def _make_envelope(callback_topic: str | None) -> Envelope:
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic="terminal_node.input", callback_topic=callback_topic))
    return Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=stack),
    )


async def test_return_call_with_null_callback_publishes_no_callback():
    """A ``ReturnCall`` on a ``callback_topic=None`` frame must NOT publish to any
    callback topic, yet must still return the terminal envelope carrying the
    final state (so the broadcast/traceability channel still fires)."""
    node = _make_node()
    envelope = _make_envelope(callback_topic=None)

    broker = MagicMock()
    broker.publish = AsyncMock()

    final_state = State()
    result = await node._publish_action(ReturnCall(state=final_state), envelope, "cid-ff", broker)

    assert broker.publish.call_count == 0, f"expected no callback publish for null-callback terminal; got {broker.publish.call_count}"
    # The terminal envelope must still be returned so the worker's @publisher
    # broadcasts it to publish_topic (the traceability channel).
    assert isinstance(result, Envelope)
    assert result.context.state is final_state


async def test_return_call_with_non_null_callback_publishes_to_callback():
    """Contrast: a ``ReturnCall`` on a frame with a real ``callback_topic`` must
    publish the point-to-point reply to that topic exactly once."""
    node = _make_node()
    envelope = _make_envelope(callback_topic="client.reply.inbox")

    broker = MagicMock()
    broker.publish = AsyncMock()

    result = await node._publish_action(ReturnCall(state=State()), envelope, "cid-cb", broker)

    assert broker.publish.call_count == 1, f"expected exactly one callback publish for non-null callback; got {broker.publish.call_count}"
    assert broker.publish.call_args.kwargs["topic"] == "client.reply.inbox"
    assert isinstance(result, Envelope)


# ---------------------------------------------------------------------------
# End-to-end: Client.emit_to_node — true one-way fire-and-forget.
# ---------------------------------------------------------------------------


def _text_then_done() -> FunctionModel:
    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ModelTextPart("done")])

    return FunctionModel(_fn)


async def test_emit_to_node_allocates_no_client_state(container):
    """Calling ``emit_to_node`` in a loop leaves ``_dispatcher._pending`` empty
    (no reply future is ever registered) and returns a non-empty uuid-hex
    correlation_id per call."""
    worker = container.get(Worker)
    agent = Agent(
        "emit_no_state_agent",
        system_prompt="x",
        subscribe_topics="emit_no_state_agent.input",
        publish_topic="emit_no_state_agent.output",
        model_client=_text_then_done(),
    )
    worker.add_nodes(agent)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    correlation_ids: list[str] = []
    async with TestKafkaBroker(broker):
        for _ in range(5):
            cid = await client.emit_to_node("hi", agent.subscribe_topics[0])
            correlation_ids.append(cid)

    # No future was ever registered: the dispatcher's pending map stays empty.
    assert client._dispatcher._pending == {}
    # Each call returns a non-empty uuid hex correlation_id.
    assert len(correlation_ids) == 5
    for cid in correlation_ids:
        assert isinstance(cid, str)
        assert cid
        # uuid7().hex is 32 lowercase hex chars.
        assert len(cid) == 32
        int(cid, 16)
    # Correlation ids are unique per call.
    assert len(set(correlation_ids)) == 5


async def test_emit_to_node_traceable_via_publish_topic_but_no_reply(container, caplog):
    """The terminal result still rides the broadcast ``publish_topic`` channel (a
    ``@consumer`` observes it with populated output), while the client reply inbox
    receives nothing — no reply future, and no "no pending future" warning."""
    received: list[NodeResult] = []

    @consumer(subscribe_topics="emit_trace_agent.output")
    def sink(result: NodeResult) -> None:
        received.append(result)

    worker = container.get(Worker)
    agent = Agent(
        "emit_trace_agent",
        system_prompt="x",
        subscribe_topics="emit_trace_agent.input",
        publish_topic="emit_trace_agent.output",
        model_client=_text_then_done(),
    )
    worker.add_nodes(agent, sink)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    with caplog.at_level(logging.WARNING, logger=REPLY_DISPATCHER_LOGGER):
        async with TestKafkaBroker(broker):
            cid = await client.emit_to_node("hi", agent.subscribe_topics[0])

    # The terminal result reached the broadcast publish_topic channel.
    final = [r for r in received if any(isinstance(p, TextPart) for p in r.output_parts)]
    assert final, f"consumer never saw a terminal envelope; saw={received}"
    assert final[-1].output == "done"
    assert isinstance(final[-1], NodeResult)
    assert final[-1].correlation_id == cid

    # The client reply inbox received nothing: no pending future was registered,
    # so the dispatcher never logged "no pending future".
    assert client._dispatcher._pending == {}
    no_pending_warnings = [r for r in caplog.records if "no pending future" in r.getMessage()]
    assert not no_pending_warnings, f"client reply inbox should receive nothing; got {no_pending_warnings}"


# ---------------------------------------------------------------------------
# emit_to_node input-shaping: it carries its OWN copy of invoke_node's
# model_settings guard / correlation_id default / OverridesState build / deps
# passthrough (it does not delegate to invoke_node), so those branches are
# covered here directly by spying on ``_emit``.
# ---------------------------------------------------------------------------


async def test_emit_to_node_rejects_non_json_serializable_model_settings(container):
    """The model_settings JSON-serializability guard rejects a non-serializable
    payload BEFORE publishing — ``_emit`` is never reached."""
    client = container.get(Client)
    client._emit = AsyncMock()  # the only publish path; must not be called

    with pytest.raises(ValueError, match="not JSON-serializable"):
        await client.emit_to_node("hi", "agent.input", model_settings={"bad": object()})

    client._emit.assert_not_called()


async def test_emit_to_node_uses_caller_supplied_correlation_id(container):
    """A caller-supplied correlation_id is returned verbatim and forwarded to ``_emit``."""
    client = container.get(Client)
    client._emit = AsyncMock(return_value="given-cid")

    returned = await client.emit_to_node("hi", "agent.input", correlation_id="given-cid")

    assert returned == "given-cid"
    assert client._emit.await_args.kwargs["correlation_id"] == "given-cid"


async def test_emit_to_node_autogenerates_correlation_id_when_none(container):
    """With no correlation_id, a fresh uuid7 hex is generated, forwarded to ``_emit``,
    and returned."""
    client = container.get(Client)
    client._emit = AsyncMock(side_effect=lambda **kw: kw["correlation_id"])

    returned = await client.emit_to_node("hi", "agent.input")

    assert isinstance(returned, str) and len(returned) == 32
    int(returned, 16)  # valid hex
    assert client._emit.await_args.kwargs["correlation_id"] == returned


async def test_emit_to_node_builds_overrides_and_forwards_deps_and_run_args(container):
    """``model_settings`` builds an ``OverridesState`` carried to ``_emit``; ``deps``
    and ``run_args`` pass through; a ``State`` is constructed for the prompt."""
    client = container.get(Client)
    client._emit = AsyncMock(return_value="cid")

    deps = {"tenant": "acme"}
    await client.emit_to_node(
        "summarize",
        "agent.input",
        deps=deps,
        model_settings={"temperature": 0.0},
        run_args=("a", 1),
    )

    kwargs = client._emit.await_args.kwargs
    assert kwargs["deps"] == deps
    assert kwargs["run_args"] == ("a", 1)
    assert kwargs["overrides"] is not None
    assert kwargs["overrides"].model_settings == {"temperature": 0.0}
    assert isinstance(kwargs["state"], State)


async def test_emit_to_node_no_overrides_when_unset(container):
    """With neither tool_overrides nor model_settings, no ``OverridesState`` is built."""
    client = container.get(Client)
    client._emit = AsyncMock(return_value="cid")

    await client.emit_to_node("hi", "agent.input")

    assert client._emit.await_args.kwargs["overrides"] is None


async def test_emit_to_node_tool_overrides_only_builds_overrides(container):
    """The tool_overrides-set / model_settings-None arm of the OverridesState OR:
    overrides carry the tool list and a None model_settings."""
    tool = agent_tool(lambda: "pong")  # a BaseToolNodeSchema (ToolNodeDef)
    client = container.get(Client)
    client._emit = AsyncMock(return_value="cid")

    await client.emit_to_node("hi", "agent.input", tool_overrides=[tool])

    overrides = client._emit.await_args.kwargs["overrides"]
    assert overrides is not None
    assert overrides.override_agent_tools == [tool]
    assert overrides.model_settings is None


async def test_emit_to_node_passes_temp_instructions_and_history_into_state(container):
    """temp_instructions and message_history are carried onto the State handed to ``_emit``."""
    client = container.get(Client)
    client._emit = AsyncMock(return_value="cid")

    history: list[ModelMessage] = [ModelRequest.user_text_prompt("earlier turn")]
    await client.emit_to_node("now", "agent.input", temp_instructions="be brief", message_history=history)

    state = client._emit.await_args.kwargs["state"]
    assert state.temp_instructions == "be brief"
    assert history[0] in state.message_history


# ---------------------------------------------------------------------------
# Multi-hop: a tool Call beneath an emitted invocation must still round-trip.
# Only the bottom (client) frame is None; intermediate tool frames carry the
# agent's real _return_topic, so the tool's ReturnCall routes back.
# ---------------------------------------------------------------------------


@agent_tool
def ping() -> str:
    return "pong"


def _calls_ping_then_done() -> FunctionModel:
    """A model that calls ``ping`` on the first turn, then summarizes once the
    tool result is in history."""

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        tool_already_ran = isinstance(last, ModelRequest) and any(isinstance(p, ToolReturnPart) for p in last.parts)
        if tool_already_ran:
            return ModelResponse(parts=[ModelTextPart("done")])
        return ModelResponse(parts=[ToolCallPart("ping")])

    return FunctionModel(_fn)


async def test_emit_to_node_multi_hop_tool_call_still_terminates_traceably(container, caplog):
    """emit → agent issues a tool Call → tool ReturnCall routes back via the
    agent's _return_topic (a NON-None intermediate callback) → agent terminal
    ReturnCall hits the None bottom frame and suppresses the callback, while the
    terminal still reaches publish_topic. If None ever leaked into the tool
    frame's callback the tool round-trip would break and no terminal would
    arrive — so a passing terminal proves the invariant."""
    received: list[NodeResult] = []

    @consumer(subscribe_topics="emit_tool_agent.output")
    def sink(result: NodeResult) -> None:
        received.append(result)

    worker = container.get(Worker)
    agent = Agent(
        "emit_tool_agent",
        system_prompt="x",
        subscribe_topics="emit_tool_agent.input",
        publish_topic="emit_tool_agent.output",
        model_client=_calls_ping_then_done(),
        tools=[ping],
    )
    worker.add_nodes(agent, ping, sink)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    with caplog.at_level(logging.WARNING, logger=REPLY_DISPATCHER_LOGGER):
        async with TestKafkaBroker(broker):
            cid = await client.emit_to_node("hi", agent.subscribe_topics[0])

    # A terminal with real output reached publish_topic → the tool frame's
    # callback was a real topic, the tool ran, and its ReturnCall routed back.
    terminals = [r for r in received if any(isinstance(p, TextPart) for p in r.output_parts)]
    assert terminals, f"no terminal reached publish_topic; the tool round-trip likely broke under emit: {received}"
    assert terminals[-1].output == "done"
    assert terminals[-1].correlation_id == cid

    # The agent actually issued the ping tool call.
    tool_called = any(
        any(getattr(tc, "tool_name", None) == "ping" for tc in getattr(m, "tool_calls", [])) for r in received for m in r.message_history
    )
    assert tool_called, "agent never issued the ping tool call in the captured history"

    # The bottom (client) frame callback was suppressed: no reply future, no warning.
    assert client._dispatcher._pending == {}
    assert not [r for r in caplog.records if "no pending future" in r.getMessage()]
