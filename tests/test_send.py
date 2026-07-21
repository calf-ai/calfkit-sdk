"""Contracts for the caller-surface ``client.agent(...).send`` verb (post-ADR-0022 cutover), plus
the node-level null-callback ("fire-and-forget") terminal that wire-level dispatch rests on.

The unit tests drive ``BaseNodeDef._publish_action`` directly with a spy broker (mirroring
``tests/test_co_tenant_tool_isolation.py``) so the worker's terminal callback-guard is isolated
from the messaging layer. A bottom call frame with ``callback_topic=None`` represents a true
wire-level fire-and-forget invocation: there is no requester to return a point-to-point reply to,
so the worker must NOT publish a callback on the terminal ``ReturnCall`` — yet it must still build
and return the terminal envelope so the result is broadcast to the node's ``publish_topic`` (the
traceability channel). This null-callback terminal stays a *node* concept (ADR-0022) and is
unaffected by the client redesign.

The end-to-end tests exercise the redesigned client (``calfkit.client.caller.Client``): the ``send``
verb now hangs off a per-destination gateway (``client.agent(topic=...).send(...)``), returns a
:class:`~calfkit.client.gateway.Dispatch` token (``.correlation_id``), and — per ADR-0022 — always
routes its terminal to the client's OWN inbox (observed on the firehose), registering no per-run
handle (``client._hub._runs`` stays empty). There is no ``reply_to`` Return Address and no
client-level true fire-and-forget anymore. Input-shaping (correlation_id default, deps/state
passthrough) is covered by spying on the single publish path, ``client._publish_call``; a
non-serializable ``deps`` bubbles from publish (no call-site pre-flight, spec §2.5).
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiokafka.errors import UnknownTopicOrPartitionError
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._protocol import HDR_KIND
from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, ToolCallPart, ToolReturnPart
from calfkit._vendor.pydantic_ai.messages import TextPart as ModelTextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client
from calfkit.models import ConsumerContext
from calfkit.models.actions import ReturnCall, TailCall
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

HUB_LOGGER = "calfkit.client.hub"


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
    result, _kind = await node._publish_action(ReturnCall(state=final_state), envelope, "cid-ff", "task-under-test", broker)

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

    result, _kind = await node._publish_action(ReturnCall(state=State()), envelope, "cid-cb", "task-under-test", broker)

    assert broker.publish.call_count == 1, f"expected exactly one callback publish for non-null callback; got {broker.publish.call_count}"
    assert broker.publish.call_args.kwargs["topic"] == "client.reply.inbox"
    assert isinstance(result, Envelope)


# ---------------------------------------------------------------------------
# End-to-end: client.agent(...).send — dispatch without awaiting (ADR-0022).
# ---------------------------------------------------------------------------


def _text_then_done() -> FunctionModel:
    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ModelTextPart("done")])

    return FunctionModel(_fn)


async def test_send_allocates_no_client_state(container):
    """Calling ``send`` in a loop registers no per-run handle (``_hub._runs`` stays
    empty) and each call returns a ``Dispatch`` carrying a fresh uuid-hex
    correlation_id."""
    worker = container.get(Worker)
    agent = Agent(
        "send_no_state_agent",
        system_prompt="x",
        subscribe_topics="send_no_state_agent.input",
        publish_topic="send_no_state_agent.output",
        model_client=_text_then_done(),
    )
    worker.add_nodes(agent)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    correlation_ids: list[str] = []
    async with TestKafkaBroker(broker):
        for _ in range(5):
            dispatch = await client.agent(topic=agent.subscribe_topics[0]).send("hi")
            correlation_ids.append(dispatch.correlation_id)

    # send() registers no per-run handle: the hub's routing map stays empty.
    assert client._hub._runs == {}
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


async def test_send_traceable_via_publish_topic_but_no_reply(container, caplog):
    """The terminal result still rides the broadcast ``publish_topic`` channel (a
    ``@consumer`` observes it with populated output). Per ADR-0022 the terminal
    ALSO lands on the client inbox, but ``send`` registers no per-run handle
    (``_hub._runs`` stays empty), so the hub logs the inbox reply as ``no pending
    handle`` (firehose-recoverable) rather than resolving a run."""
    received: list[ConsumerContext] = []

    @consumer(subscribe_topics="send_trace_agent.output")
    def sink(ctx: ConsumerContext) -> None:
        received.append(ctx)

    worker = container.get(Worker)
    agent = Agent(
        "send_trace_agent",
        system_prompt="x",
        subscribe_topics="send_trace_agent.input",
        publish_topic="send_trace_agent.output",
        model_client=_text_then_done(),
    )
    worker.add_nodes(agent, sink)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    with caplog.at_level(logging.WARNING, logger=HUB_LOGGER):
        async with TestKafkaBroker(broker):
            dispatch = await client.agent(topic=agent.subscribe_topics[0]).send("hi")
    cid = dispatch.correlation_id

    # The terminal result reached the broadcast publish_topic channel.
    final = [c for c in received if any(isinstance(p, TextPart) for p in c.output_parts)]
    assert final, f"consumer never saw a terminal envelope; saw={received}"
    assert final[-1].output == "done"
    assert isinstance(final[-1], ConsumerContext)
    assert final[-1].correlation_id == cid

    # send registers no per-run handle; the inbox terminal finds none → logged
    # firehose-recoverable (ADR-0022), never resolving a run.
    assert client._hub._runs == {}
    no_handle_warnings = [r for r in caplog.records if "no pending handle" in r.getMessage()]
    assert no_handle_warnings, "send's inbox terminal should be logged 'no pending handle' (ADR-0022)"


# ---------------------------------------------------------------------------
# send input-shaping: the shared ``_build_state`` path forwards to the single
# publish path ``_publish_call`` — the correlation_id default / deps + state
# passthrough branches are covered here by spying on ``_publish_call``.
# (No pre-flight, spec §2.5.)
# ---------------------------------------------------------------------------


def _publish_spy(client: Client) -> AsyncMock:
    """Spy on the single publish path (``_publish_call``) and stub the lazy broker-start
    guard to a no-op, so ``send``'s input-shaping is inspectable without Kafka I/O."""
    client._ensure_started = AsyncMock()
    client._publish_call = AsyncMock()
    return client._publish_call


async def test_send_non_serializable_deps_bubbles_at_publish(container):
    """No call-site pre-flight (spec §2.5): non-serializable input is not rejected by a
    localized ValueError — it bubbles from the publish path (``deps`` is the vehicle)."""
    client = container.get(Client)
    broker = container.get(KafkaBroker)

    @broker.subscriber("agent.input")
    async def _sink(env: Envelope) -> None: ...

    bad: Any = {"bad": object()}
    async with TestKafkaBroker(broker):
        with pytest.raises(Exception):  # noqa: B017 — serialization bubbles from publish, not a call-site guard
            await client.agent(topic="agent.input").send("hi", deps=bad)


async def test_send_uses_caller_supplied_correlation_id(container):
    """A caller-supplied correlation_id rides the returned ``Dispatch`` and is forwarded to ``_publish_call``."""
    client = container.get(Client)
    publish = _publish_spy(client)

    dispatch = await client.agent(topic="agent.input").send("hi", correlation_id="given-cid")

    assert dispatch.correlation_id == "given-cid"
    assert publish.await_args.kwargs["correlation_id"] == "given-cid"


async def test_send_autogenerates_correlation_id_when_none(container):
    """With no correlation_id, a fresh uuid7 hex is generated, forwarded to ``_publish_call``,
    and carried on the returned ``Dispatch``."""
    client = container.get(Client)
    publish = _publish_spy(client)

    dispatch = await client.agent(topic="agent.input").send("hi")

    cid = dispatch.correlation_id
    assert isinstance(cid, str) and len(cid) == 32
    int(cid, 16)  # valid hex
    assert publish.await_args.kwargs["correlation_id"] == cid


async def test_send_forwards_deps_and_builds_state(container):
    """``deps`` pass through to ``_publish_call``; a ``State`` is constructed for the prompt."""
    client = container.get(Client)
    publish = _publish_spy(client)

    deps = {"tenant": "acme"}
    await client.agent(topic="agent.input").send("summarize", deps=deps)

    kwargs = publish.await_args.kwargs
    assert kwargs["deps"] == deps
    assert isinstance(kwargs["state"], State)


async def test_send_passes_temp_instructions_and_history_into_state(container):
    """temp_instructions and message_history are carried onto the State handed to ``_publish_call``."""
    client = container.get(Client)
    publish = _publish_spy(client)

    history: list[ModelMessage] = [ModelRequest.user_text_prompt("earlier turn")]
    await client.agent(topic="agent.input").send("now", temp_instructions="be brief", message_history=history)

    state = publish.await_args.kwargs["state"]
    assert state.temp_instructions == "be brief"
    assert history[0] in state.message_history


# ---------------------------------------------------------------------------
# Multi-hop: a tool Call beneath a send invocation must still round-trip.
# Intermediate tool frames carry the agent's real _return_topic, so the tool's
# ReturnCall routes back; the bottom (client) frame is the inbox (ADR-0022).
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


async def test_send_multi_hop_tool_call_still_terminates_traceably(container, caplog):
    """send → agent issues a tool Call → tool ReturnCall routes back via the
    agent's _return_topic (a NON-None intermediate callback) → the agent's
    terminal ReturnCall reaches publish_topic. Per ADR-0022 the bottom (client)
    frame's callback is the client inbox, so the terminal also lands there with no
    per-run handle (logged firehose-recoverable). A terminal with real output
    proves the tool round-trip survived emit — if None ever leaked into the tool
    frame's callback the tool round-trip would break and no terminal would
    arrive."""
    received: list[ConsumerContext] = []

    @consumer(subscribe_topics="send_tool_agent.output")
    def sink(ctx: ConsumerContext) -> None:
        received.append(ctx)

    worker = container.get(Worker)
    agent = Agent(
        "send_tool_agent",
        system_prompt="x",
        subscribe_topics="send_tool_agent.input",
        publish_topic="send_tool_agent.output",
        model_client=_calls_ping_then_done(),
        tools=[ping],
    )
    worker.add_nodes(agent, ping, sink)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    with caplog.at_level(logging.WARNING, logger=HUB_LOGGER):
        async with TestKafkaBroker(broker):
            dispatch = await client.agent(topic=agent.subscribe_topics[0]).send("hi")
    cid = dispatch.correlation_id

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

    # send registered no per-run handle; the agent's inbox terminal finds none →
    # logged firehose-recoverable (ADR-0022).
    assert client._hub._runs == {}
    assert [r for r in caplog.records if "no pending handle" in r.getMessage()]


# ---------------------------------------------------------------------------
# Wire shape (caller-surface spec): the bottom CallFrame's callback_topic + the
# x-calf-kind stamp. Per ADR-0022 send routes to the client inbox (no reply_to).
# ---------------------------------------------------------------------------


def _wire_spy(client: Client) -> AsyncMock:
    """Replace the client's broker with a spy so the published envelope can be
    inspected without Kafka. The lazy start-guard is skipped because every
    ``MagicMock`` attribute — including ``_connection`` — is truthy."""
    broker = MagicMock()
    broker.publish = AsyncMock()
    client._broker = broker
    return broker.publish


# NOTE: test_send_reply_to_sets_callback_topic_on_wire removed at the caller-surface cutover —
# send() has no reply_to / always replies to the inbox (ADR-0022).


async def test_send_stamps_x_calf_kind_call_on_wire(container):
    """Every client publish is a call-kind ingress: it carries ``x-calf-kind: call``
    so a node/consumer classifies it correctly (the wire stamp PR-C's fault arm relies on)."""
    client = container.get(Client)
    publish = _wire_spy(client)

    await client.agent(topic="agent.input").send("hi")

    assert publish.await_args.kwargs["headers"][HDR_KIND] == "call"


async def test_send_routes_callback_to_the_client_inbox(container):
    """Per ADR-0022 ``send`` routes its terminal to the client's OWN inbox — the
    published bottom CallFrame's ``callback_topic`` is ``client.inbox_topic`` (no
    third-party Return Address, no client-level null-callback fire-and-forget)."""
    client = container.get(Client)
    publish = _wire_spy(client)

    await client.agent(topic="agent.input").send("hi")

    envelope = publish.await_args.args[0]
    assert envelope.internal_workflow_state.current_frame.callback_topic == client.inbox_topic


# NOTE: removed at the caller-surface cutover — send() has no reply_to (always replies to the
# inbox, ADR-0022): test_send_reply_to_registers_no_future, test_send_reply_to_rejects_blank_topic,
# test_send_reply_to_rejects_own_inbox.


async def test_start_callback_is_always_client_inbox(container):
    """``start`` always writes the client's own inbox as the wire callback — the
    only address whose per-run handle can ever resolve."""
    client = container.get(Client)
    publish = _wire_spy(client)

    await client.agent(topic="agent.input").start("hi")

    envelope = publish.await_args.args[0]
    assert envelope.internal_workflow_state.current_frame.callback_topic == client.inbox_topic


# NOTE: removed at the caller-surface cutover (ADR-0022): test_start_and_execute_reject_removed_
# reply_topic_param (the gateway verbs agent().start/execute have no reply_topic param);
# test_send_reply_to_delivers_terminal_to_topic + test_send_reply_to_rejects_illegal_topic_names
# (send() has no reply_to / always replies to the inbox).


async def test_return_call_callback_publish_failure_propagates_to_the_publish_guard():
    """The terminal point-to-point publish failure is no longer swallowed inside
    ``_publish_action`` (the old broadcast-the-return traceability fallback, PR-6 scenario 42):
    it propagates so the handler's publish guard faults the caller on the pre-mutation snapshot.
    Traceability survives — as the fault's broadcast mirror — and the handler-level behavior is
    covered in tests/test_fault_pipeline.py::TestPublishGuard."""
    node = _make_node()
    envelope = _make_envelope(callback_topic="missing.sink")

    broker = MagicMock()
    broker.publish = AsyncMock(side_effect=UnknownTopicOrPartitionError())

    with pytest.raises(UnknownTopicOrPartitionError):
        await node._publish_action(ReturnCall(state=State()), envelope, "cid-fail", "task-under-test", broker)


async def test_tail_call_preserves_reply_to_callback():
    """Regression pin for the spec claim (nodes/base.py TailCall branch): a
    tail chain re-pushes the popped frame's callback, so a ``reply_to``
    address on the bottom frame survives to the eventual terminal exactly
    like a client inbox does. (Pinned existing behavior, not a new feature.)"""
    node = _make_node()
    envelope = _make_envelope(callback_topic="sink.topic")

    broker = MagicMock()
    broker.publish = AsyncMock()

    await node._publish_action(TailCall(target_topic="terminal_node.input", state=State()), envelope, "cid-tail", "task-under-test", broker)

    frame = envelope.internal_workflow_state.current_frame
    assert frame.callback_topic == "sink.topic", f"TailCall dropped the reply_to callback; got {frame.callback_topic!r}"


# NOTE: removed at the caller-surface cutover: test_send_reply_to_survives_tool_hops (send() has no
# reply_to / always replies to the inbox, ADR-0022); test_no_pending_future_warning_names_emitter
# (_ReplyDispatcher is gone — the hub's "no pending handle" warning is covered in
# tests/test_caller_surface_hub.py).
