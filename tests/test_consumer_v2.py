"""Tests for the ConsumerContext-based consumer (``consumer_v2``).

Drives ``ConsumerNode.handler`` directly (the shared ``BaseNodeDef.handler``) with
frameless envelopes — the shape a sink sees tapping a producer's publish_topic.
"""

import asyncio
import logging
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND
from calfkit.models import ConsumerContext, SessionRunContext
from calfkit.models.envelope import Envelope
from calfkit.models.payload import DataPart, TextPart
from calfkit.models.session_context import CallFrameStack, WorkflowState
from calfkit.models.state import State
from calfkit.nodes.consumer import ConsumerNode, consumer_v2

V2_LOGGER = "calfkit.nodes.consumer_v2"
_HEADERS = {HDR_EMITTER: b"upstream", HDR_EMITTER_KIND: b"agent"}


# --- helpers --------------------------------------------------------------- #


def _envelope(state: State, *, deps: dict | None = None) -> Envelope:
    return Envelope(
        context=SessionRunContext(state=state, deps=deps or {}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
    )


def _text_state(text: str) -> State:
    s = State()
    s.final_output_parts = [TextPart(text=text)]
    return s


async def _handle(node: ConsumerNode, envelope: Envelope, correlation_id: str = "cid-0000") -> object:
    return await node.handler(envelope, correlation_id=correlation_id, headers=_HEADERS, broker=MagicMock())


# --- construction ---------------------------------------------------------- #


def test_node_kind_is_consumer():
    assert ConsumerNode._node_kind == "consumer"


def test_decorator_defaults_node_id_and_normalizes_topics():
    @consumer_v2(subscribe_topics="t")
    def my_sink(ctx: ConsumerContext) -> None:
        return None

    assert isinstance(my_sink, ConsumerNode)
    assert my_sink.node_id == "consumer_my_sink"
    assert my_sink.subscribe_topics == ["t"]
    assert my_sink.publish_topic is None


def test_publish_topic_setter_rejects_non_none():
    node = ConsumerNode(node_id="x", subscribe_topics="t", consume_fn=lambda ctx: None)
    with pytest.raises(AttributeError, match="terminal sinks"):
        node.publish_topic = "sneaky.topic"
    assert node.publish_topic is None


def test_generator_function_rejected_at_construction():
    def gen(ctx):
        yield ctx

    with pytest.raises(TypeError, match="generator function"):
        ConsumerNode(node_id="x", subscribe_topics="t", consume_fn=gen)


def test_async_generator_function_rejected_at_construction():
    async def agen(ctx):
        yield ctx

    with pytest.raises(TypeError, match="async generator function"):
        ConsumerNode(node_id="x", subscribe_topics="t", consume_fn=agen)


# --- ConsumerContext.from_run_context -------------------------------------- #


def test_from_run_context_projects_fields_and_resources():
    ctx = SessionRunContext(state=_text_state("done"), deps={"k": "v"})
    ctx._stamp_transport(correlation_id="cid-fc", emitter_node_id="up", emitter_node_kind="agent")
    sentinel = object()
    ctx._resources = {"db": sentinel}

    cctx = ConsumerContext.from_run_context(ctx, str)

    assert cctx.output == "done"
    assert cctx.state is ctx.state
    assert cctx.correlation_id == "cid-fc"
    assert cctx.emitter_node_id == "up"
    assert cctx.emitter_node_kind == "agent"
    assert cctx.deps is ctx.deps
    assert cctx.resources["db"] is sentinel


def test_from_run_context_intermediate_hop_output_none():
    ctx = SessionRunContext(state=State(), deps={})  # no final_output_parts
    ctx._stamp_transport(correlation_id="cid-int", emitter_node_id="up", emitter_node_kind="agent")
    cctx = ConsumerContext.from_run_context(ctx)
    assert cctx.output is None


# --- handler / run end-to-end ---------------------------------------------- #


async def test_sync_sink_receives_consumer_context():
    received: list[ConsumerContext] = []
    node = ConsumerNode(node_id="s", subscribe_topics="t", consume_fn=received.append)
    env = _envelope(_text_state("hello"))

    await _handle(node, env)

    assert len(received) == 1
    assert isinstance(received[0], ConsumerContext)
    assert received[0].output == "hello"
    assert received[0].emitter_node_id == "upstream"
    assert received[0].emitter_node_kind == "agent"


async def test_async_sink_is_awaited():
    ran: list[str] = []

    async def sink(ctx: ConsumerContext) -> None:
        await asyncio.sleep(0)
        ran.append(ctx.output)

    node = ConsumerNode(node_id="s", subscribe_topics="t", consume_fn=sink)
    await _handle(node, _envelope(_text_state("async-out")))
    assert ran == ["async-out"]


async def test_intermediate_hop_output_none_no_raise():
    received: list[ConsumerContext] = []
    node = ConsumerNode(node_id="s", subscribe_topics="t", consume_fn=received.append)

    await _handle(node, _envelope(State()))  # empty final_output_parts

    assert len(received) == 1
    assert received[0].output is None


async def test_state_and_deps_are_not_copied():
    """No-copy contract: prepare_context stamps in place, so ctx.state/deps are the
    same instances carried on the envelope."""
    received: list[ConsumerContext] = []
    node = ConsumerNode(node_id="s", subscribe_topics="t", consume_fn=received.append)
    env = _envelope(_text_state("x"), deps={"a": 1})

    await _handle(node, env)

    assert received[0].state is env.context.state
    assert received[0].deps is env.context.deps


async def test_resources_flow_from_node_bag():
    received: list[ConsumerContext] = []
    node = ConsumerNode(node_id="s", subscribe_topics="t", consume_fn=received.append)
    sentinel = object()
    node.resources["db"] = sentinel

    await _handle(node, _envelope(_text_state("x")))

    assert received[0].resources["db"] is sentinel


async def test_output_type_deserialization():
    @dataclass
    class Report:
        location: str
        summary: str

    received: list[ConsumerContext] = []
    node = ConsumerNode(node_id="s", subscribe_topics="t", consume_fn=received.append, output_type=Report)
    state = State()
    state.final_output_parts = [DataPart(data={"location": "Tokyo", "summary": "sunny"})]

    await _handle(node, _envelope(state))

    assert isinstance(received[0].output, Report)
    assert received[0].output.location == "Tokyo"


async def test_consume_fn_exception_swallowed_and_logged(caplog):
    def boom(ctx: ConsumerContext) -> None:
        raise RuntimeError("intentional")

    node = ConsumerNode(node_id="boom", subscribe_topics="t", consume_fn=boom)

    with caplog.at_level(logging.ERROR, logger=V2_LOGGER):
        resp = await _handle(node, _envelope(_text_state("x")))  # must not raise

    assert resp is not None
    assert any("consume_fn raised" in r.getMessage() for r in caplog.records)


async def test_cancelled_error_propagates():
    async def cancels(ctx: ConsumerContext) -> None:
        raise asyncio.CancelledError()

    node = ConsumerNode(node_id="c", subscribe_topics="t", consume_fn=cancels)
    with pytest.raises(asyncio.CancelledError):
        await _handle(node, _envelope(_text_state("x")))


async def test_function_returning_generator_object_detected(caplog):
    def _wrapper_gen():
        yield 1

    def sink(ctx):
        return _wrapper_gen()  # slips past _validate_consume_fn

    node = ConsumerNode(node_id="g", subscribe_topics="t", consume_fn=sink)
    with caplog.at_level(logging.ERROR, logger=V2_LOGGER):
        await _handle(node, _envelope(_text_state("x")))  # swallowed, not raised

    rec = [r for r in caplog.records if "consume_fn raised" in r.getMessage()]
    assert rec and isinstance(rec[0].exc_info[1], TypeError)


async def test_gate_rejection_skips_consume_fn():
    calls: list[str] = []

    def reject(ctx: SessionRunContext) -> bool:
        return False

    node = ConsumerNode(node_id="g", subscribe_topics="t", consume_fn=lambda ctx: calls.append("ran"), gates=[reject])
    await _handle(node, _envelope(_text_state("x")))
    assert calls == []
