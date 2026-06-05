"""Phase 5 — end-to-end integration tests for lifecycle hooks.

These drive the *whole* stack: a real :class:`Worker` boots through its four
FastStream hooks under the repo's in-memory ``TestKafkaBroker`` simulation,
opens its resources, routes a real message to a node handler, and tears the
resources down on stop. They verify the convergence properties the plan §5
calls out:

* a ``@resource`` (and a callback-shape resource) opened at boot reaches a live
  handler as ``ctx.resources[...]`` and is closed (key + provenance popped) on
  stop;
* ``resources`` reach all four read surfaces (Agent ``run`` / custom
  ``BaseNodeDef`` ``ctx.resources``, ``agent_tool`` ``ctx.resources``, and the
  consumer ``result.resources``);
* same-key collisions in both directions raise ``LifecycleConfigError`` at boot
  while different keys coexist;
* the presence idiom publishes "up"/"down" keyed on ``worker.id``;
* an ``after_startup`` failure stops the broker, and a double ``start()`` raises.

We avoid the LLM by driving deterministic node handlers directly over the
broker (mirroring ``tests/test_consumer.py``'s hand-built envelope pattern) and
a ``FunctionModel``-backed agent for the tool surface.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from faststream.kafka import TestKafkaBroker

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND
from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client, NodeResult
from calfkit.exceptions import LifecycleConfigError
from calfkit.models import Silent
from calfkit.models.envelope import Envelope
from calfkit.models.payload import TextPart as PayloadTextPart
from calfkit.models.session_context import (
    CallFrame,
    CallFrameStack,
    SessionRunContext,
    WorkflowState,
)
from calfkit.models.state import State
from calfkit.models.tool_context import ToolContext
from calfkit.nodes import Agent, agent_tool, consumer
from calfkit.nodes.base import BaseNodeDef
from calfkit.worker import Worker

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_worker(**kwargs: Any) -> Worker:
    return Worker(Client.connect(), **kwargs)


def _frame_envelope(target_topic: str) -> Envelope:
    """A minimal envelope with a single call frame, as a producer would emit.

    ``BaseNodeDef.prepare_context`` peeks ``current_frame`` (for the frame_id),
    so the stack must carry at least one frame for a server-side handler.
    """
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic=target_topic, callback_topic="cb"))
    return Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=stack),
    )


def _text_envelope(text: str) -> Envelope:
    """A terminal envelope carrying a final TextPart (for consumer output)."""
    state = State(final_output_parts=[PayloadTextPart(text=text)])
    return Envelope(
        context=SessionRunContext(state=state, deps={}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
    )


async def _publish(broker: Any, envelope: Envelope, topic: str, correlation_id: str) -> None:
    await broker.publish(
        envelope,
        topic=topic,
        correlation_id=correlation_id,
        headers={HDR_EMITTER: "client", HDR_EMITTER_KIND: "client"},
    )


class _ProbeNode(BaseNodeDef):
    """Custom ``BaseNodeDef`` subclass that records ``ctx.resources`` on run."""

    def __init__(self, captured: dict[str, Any], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._captured = captured

    async def run(self, ctx: SessionRunContext) -> Any:
        self._captured["resources"] = dict(ctx.resources)
        return Silent()


# ---------------------------------------------------------------------------
# @resource opens -> reaches a handler -> closed on stop (key + _claims gone)
# ---------------------------------------------------------------------------


async def test_resource_opens_reaches_handler_then_closes_on_stop() -> None:
    captured: dict[str, Any] = {}
    closed: list[str] = []

    worker = _make_worker()
    node = _ProbeNode(captured, node_id="res_probe", subscribe_topics=["res_probe.in"])

    @node.resource("db")
    async def db(ctx: Any) -> Any:
        try:
            yield "POOL"
        finally:
            closed.append("db")

    worker.add_nodes(node)
    broker = worker._client.broker

    async with TestKafkaBroker(broker):
        await worker.start()
        # While serving, the @resource value is in the node's live bag.
        assert node.resources["db"] == "POOL"
        await _publish(broker, _frame_envelope("res_probe.in"), "res_probe.in", "cid-res")
        await worker.stop()

    # The handler saw the resource via ctx.resources["db"].
    assert captured["resources"] == {"db": "POOL"}
    # Closed on stop, and both the key and its provenance entry are gone.
    assert closed == ["db"]
    assert "db" not in node.resources
    assert "db" not in node.resources._claims


# ---------------------------------------------------------------------------
# Callback shape (on_startup writes, after_shutdown closes) reaches a handler
# ---------------------------------------------------------------------------


async def test_callback_resource_reaches_handler_then_closes_on_stop() -> None:
    captured: dict[str, Any] = {}
    closed: list[str] = []

    worker = _make_worker()
    node = _ProbeNode(captured, node_id="cb_probe", subscribe_topics=["cb_probe.in"])

    @node.on_startup
    async def open_db(ctx: Any) -> None:
        ctx.resources["db"] = "CB-POOL"

    @node.after_shutdown
    async def close_db(ctx: Any) -> None:
        closed.append(ctx.resources["db"])

    worker.add_nodes(node)
    broker = worker._client.broker

    async with TestKafkaBroker(broker):
        await worker.start()
        assert node.resources["db"] == "CB-POOL"
        await _publish(broker, _frame_envelope("cb_probe.in"), "cb_probe.in", "cid-cb")
        await worker.stop()

    # Same handler-reaching shape as @resource.
    assert captured["resources"] == {"db": "CB-POOL"}
    # after_shutdown saw the resource before teardown; the key persists across
    # the callback (a callback-shape resource owns its own cleanup) but the
    # provenance claim was made by the callback.
    assert closed == ["CB-POOL"]
    assert node.resources._claims.get("db") == "an on_startup/after_shutdown callback"


# ---------------------------------------------------------------------------
# resources reach ALL FOUR surfaces
# ---------------------------------------------------------------------------


async def test_resources_reach_custom_basenodedef_subclass() -> None:
    captured: dict[str, Any] = {}
    worker = _make_worker()
    node = _ProbeNode(captured, node_id="surface_node", subscribe_topics=["surface_node.in"])
    sentinel = object()
    node.resources["db"] = sentinel  # callback/direct claim

    worker.add_nodes(node)
    broker = worker._client.broker

    async with TestKafkaBroker(broker):
        await worker.start()
        await _publish(broker, _frame_envelope("surface_node.in"), "surface_node.in", "cid-surf")
        await worker.stop()

    assert captured["resources"]["db"] is sentinel


async def test_resources_reach_consumer_result() -> None:
    received: list[NodeResult] = []
    worker = _make_worker()

    @consumer(subscribe_topics="surface_consumer.in")
    def sink(result: NodeResult) -> None:
        received.append(result)

    sentinel = object()
    sink.resources["db"] = sentinel

    worker.add_nodes(sink)
    broker = worker._client.broker

    async with TestKafkaBroker(broker):
        await worker.start()
        await _publish(broker, _text_envelope("hi"), "surface_consumer.in", "cid-cons")
        await worker.stop()

    assert received, "consumer never ran"
    assert received[-1].resources["db"] is sentinel


def _tool_then_text(captured: dict[str, Any]) -> FunctionModel:
    """A FunctionModel that calls ``read_resource`` once, then emits text."""

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        if any(isinstance(p, ToolReturnPart) for p in getattr(last, "parts", [])):
            return ModelResponse(parts=[TextPart("done")])
        return ModelResponse(parts=[ToolCallPart(tool_name="read_resource", args={})])

    return FunctionModel(_fn)


async def test_resources_reach_agent_run_and_agent_tool() -> None:
    """The Agent's ``run`` reads ``ctx.resources`` (via prepare_context) and an
    ``agent_tool`` reads ``ctx.resources`` (via the ToolContext) — two of the
    four surfaces, exercised through a real agent->tool round trip over Kafka."""
    tool_saw: dict[str, Any] = {}

    def read_resource(ctx: ToolContext) -> str:
        tool_saw["db"] = ctx.resources["db"]
        return "ok"

    tool = agent_tool(read_resource)
    tool_sentinel = object()
    tool.resources["db"] = tool_sentinel

    # A custom agent subclass so we can also assert the *agent's* ctx.resources
    # surface (prepare_context stamping) on the same run.
    agent_saw: dict[str, Any] = {}

    class _ResAgent(Agent[str]):
        async def run(self, ctx: SessionRunContext) -> Any:
            agent_saw["db"] = ctx.resources.get("db")
            return await super().run(ctx)

    agent: _ResAgent = _ResAgent(
        "res_agent",
        system_prompt="x",
        subscribe_topics="res_agent.in",
        publish_topic="res_agent.out",
        model_client=_tool_then_text(tool_saw),  # type: ignore[arg-type]
        tools=[tool],
    )
    agent_sentinel = object()
    agent.resources["db"] = agent_sentinel

    worker = _make_worker()
    worker.add_nodes(agent, tool)
    broker = worker._client.broker
    client = worker._client

    async with TestKafkaBroker(broker):
        await worker.start()
        result = await client.execute_node("hi", "res_agent.in", timeout=5)
        await worker.stop()

    assert result.output == "done"
    # agent_tool surface: the tool read its node's resources.
    assert tool_saw["db"] is tool_sentinel
    # Agent run surface: the agent read its own node's resources.
    assert agent_saw["db"] is agent_sentinel


# ---------------------------------------------------------------------------
# Same-key collision BOTH directions raises; different keys coexist
# ---------------------------------------------------------------------------


async def test_collision_resource_then_callback_raises_at_boot() -> None:
    worker = _make_worker()
    node = _ProbeNode({}, node_id="collide_a", subscribe_topics=["collide_a.in"])

    @node.resource("db")
    async def db(ctx: Any) -> Any:
        yield "from-resource"

    @node.on_startup
    async def also_db(ctx: Any) -> None:
        ctx.resources["db"] = "from-callback"

    worker.add_nodes(node)
    broker = worker._client.broker

    async with TestKafkaBroker(broker):
        with pytest.raises(LifecycleConfigError, match="one owner per key"):
            await worker.start()


async def test_collision_callback_then_resource_raises_at_boot() -> None:
    """Opposite registration order — collision must still raise (the bag's
    ``claim`` is order-independent)."""
    worker = _make_worker()
    node = _ProbeNode({}, node_id="collide_b", subscribe_topics=["collide_b.in"])

    @node.on_startup
    async def first_db(ctx: Any) -> None:
        ctx.resources["db"] = "from-callback"

    @node.resource("db")
    async def db(ctx: Any) -> Any:
        yield "from-resource"

    worker.add_nodes(node)
    broker = worker._client.broker

    async with TestKafkaBroker(broker):
        with pytest.raises(LifecycleConfigError, match="one owner per key"):
            await worker.start()


async def test_different_keys_coexist_resource_and_callback() -> None:
    captured: dict[str, Any] = {}
    worker = _make_worker()
    node = _ProbeNode(captured, node_id="coexist", subscribe_topics=["coexist.in"])

    @node.resource("res_key")
    async def res_key(ctx: Any) -> Any:
        yield "res-value"

    @node.on_startup
    async def cb_key(ctx: Any) -> None:
        ctx.resources["cb_key"] = "cb-value"

    worker.add_nodes(node)
    broker = worker._client.broker

    async with TestKafkaBroker(broker):
        await worker.start()
        await _publish(broker, _frame_envelope("coexist.in"), "coexist.in", "cid-coexist")
        await worker.stop()

    assert captured["resources"] == {"res_key": "res-value", "cb_key": "cb-value"}


# ---------------------------------------------------------------------------
# Presence: after_startup -> "up", on_shutdown -> "down", keyed on worker.id
# ---------------------------------------------------------------------------


async def test_presence_publishes_up_and_down_keyed_on_worker_id() -> None:
    worker = _make_worker(id="fleet-presence-1")
    captured: list[dict[str, Any]] = []
    broker = worker._client._connection

    @broker.subscriber("fleet.presence")
    async def record(msg: dict[str, Any]) -> None:
        captured.append(msg)

    @worker.after_startup
    async def up(ctx: Any) -> None:
        await ctx.broker.publish({"id": ctx.worker.id, "status": "up"}, topic="fleet.presence")

    @worker.on_shutdown
    async def down(ctx: Any) -> None:
        await ctx.broker.publish({"id": ctx.worker.id, "status": "down"}, topic="fleet.presence")

    async with TestKafkaBroker(broker):
        await worker.start()
        await worker.stop()

    assert captured == [
        {"id": "fleet-presence-1", "status": "up"},
        {"id": "fleet-presence-1", "status": "down"},
    ]


# ---------------------------------------------------------------------------
# after_startup failure stops the broker; double start() raises
# ---------------------------------------------------------------------------


async def test_after_startup_failure_stops_broker() -> None:
    worker = _make_worker()

    @worker.after_startup
    async def boom(ctx: Any) -> None:
        raise RuntimeError("serving boom")

    broker = worker._client.broker
    async with TestKafkaBroker(broker):
        with patch.object(broker, "stop", new=AsyncMock(wraps=broker.stop)) as stop_spy:
            with pytest.raises(RuntimeError, match="serving boom"):
                await worker.start()

    stop_spy.assert_awaited()
    assert worker._serving_stack is None
    assert worker._resource_stack is None


async def test_double_start_raises_runtime_error() -> None:
    worker = _make_worker()
    broker = worker._client.broker
    async with TestKafkaBroker(broker):
        await worker.start()
        with pytest.raises(RuntimeError, match="single-use"):
            await worker.start()
        await worker.stop()
