"""Phase 5 — end-to-end integration tests for lifecycle hooks.

These drive the *whole* stack: a real :class:`Worker` boots through its four
FastStream hooks under the repo's in-memory ``TestKafkaBroker`` simulation,
opens its resources, routes a real message to a node handler, and tears the
resources down on stop. They verify the convergence properties the plan §5
calls out:

* a ``@resource`` (and a callback-shape resource) opened at boot reaches a live
  handler as ``ctx.resources[...]`` and is closed (key popped from the bag) on
  stop;
* ``resources`` reach all four read surfaces (Agent ``run`` / custom
  ``BaseNodeDef`` ``ctx.resources``, ``agent_tool`` ``ctx.resources``, and the
  consumer ``result.resources``);
* when an owner mixes ``@resource`` and resource-phase callbacks on the same
  key, the ``@resource`` brackets win (callbacks ignored, warning logged), and a
  duplicate ``@resource`` name on one owner raises ``LifecycleConfigError`` at
  registration, while different keys coexist;
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
from calfkit.models.reply import ReturnMessage
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
from calfkit.worker import ServingContext, Worker

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
    """A terminal envelope carrying a final TextPart in its reply slot (for consumer output)."""
    return Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
        reply=ReturnMessage(in_reply_to=None, tag=None, parts=[PayloadTextPart(text=text)]),
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
# @resource opens -> reaches a handler -> closed on stop (key popped from bag)
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
    # Closed on stop, and the key is gone from the node's bag.
    assert closed == ["db"]
    assert "db" not in node.resources


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
    # after_shutdown saw the resource before teardown; a callback-shape resource
    # owns its own cleanup, so the key persists in the node's bag (the callback
    # span never pops it).
    assert closed == ["CB-POOL"]
    assert node.resources["db"] == "CB-POOL"


# ---------------------------------------------------------------------------
# resources reach ALL FOUR surfaces
# ---------------------------------------------------------------------------


async def test_resources_reach_custom_basenodedef_subclass() -> None:
    captured: dict[str, Any] = {}
    worker = _make_worker()
    node = _ProbeNode(captured, node_id="surface_node", subscribe_topics=["surface_node.in"])
    sentinel = object()
    node.resources["db"] = sentinel  # direct write into the node's bag (no provenance machinery)

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


async def test_consumer_gate_reads_resources() -> None:
    """A consumer's *gate* can read ``ctx.resources`` (parity with regular-node
    gates), so a gate can branch on a lifecycle resource — not just the consumer
    function via ``result.resources``."""
    received: list[NodeResult] = []
    gate_saw: list[Any] = []

    def gate(ctx: SessionRunContext) -> bool:
        flag = ctx.resources.get("flag")
        gate_saw.append(flag)
        return flag == "ON"

    worker = _make_worker()

    @consumer(subscribe_topics="gate_res.in", gates=[gate])
    def sink(result: NodeResult) -> None:
        received.append(result)

    sink.resources["flag"] = "ON"

    worker.add_nodes(sink)
    broker = worker._client.broker

    async with TestKafkaBroker(broker):
        await worker.start()
        await _publish(broker, _text_envelope("hi"), "gate_res.in", "cid-gate")
        await worker.stop()

    # The gate saw the resource and admitted the message.
    assert gate_saw == ["ON"]
    assert received, "consumer never ran (gate should have admitted on flag=ON)"


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
        result = await client.execute("hi", "res_agent.in", timeout=5)
        await worker.stop()

    assert result.output == "done"
    # agent_tool surface: the tool read its node's resources.
    assert tool_saw["db"] is tool_sentinel
    # Agent run surface: the agent read its own node's resources.
    assert agent_saw["db"] is agent_sentinel


# ---------------------------------------------------------------------------
# One pattern per owner: @resource wins over resource-phase callbacks (with a
# warning); duplicate @resource name raises; different keys coexist
# ---------------------------------------------------------------------------


async def test_resource_wins_over_callback_same_key_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When an owner declares both ``@resource('db')`` and an ``on_startup``
    callback writing ``'db'``, the ``@resource`` bracket wins: the worker boots,
    the handler sees the ``@resource`` value (the callback is ignored), and a
    warning is logged (mirrors FastAPI lifespan-vs-on_event)."""
    import logging

    captured: dict[str, Any] = {}
    worker = _make_worker()
    node = _ProbeNode(captured, node_id="collide_a", subscribe_topics=["collide_a.in"])

    @node.resource("db")
    async def db(ctx: Any) -> Any:
        yield "from-resource"

    @node.on_startup
    async def also_db(ctx: Any) -> None:
        ctx.resources["db"] = "from-callback"  # ignored: @resource wins

    worker.add_nodes(node)
    broker = worker._client.broker

    with caplog.at_level(logging.WARNING):
        async with TestKafkaBroker(broker):
            await worker.start()
            assert node.resources["db"] == "from-resource"
            await _publish(broker, _frame_envelope("collide_a.in"), "collide_a.in", "cid-collide-a")
            await worker.stop()

    # Handler saw the @resource value; the callback never wrote into the bag.
    assert captured["resources"] == {"db": "from-resource"}
    assert any("callbacks are ignored" in r.message for r in caplog.records)


async def test_resource_wins_regardless_of_registration_order_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Opposite registration order (callback first, then ``@resource``) — the
    ``@resource`` bracket still wins and a warning is still logged."""
    import logging

    captured: dict[str, Any] = {}
    worker = _make_worker()
    node = _ProbeNode(captured, node_id="collide_b", subscribe_topics=["collide_b.in"])

    @node.on_startup
    async def first_db(ctx: Any) -> None:
        ctx.resources["db"] = "from-callback"  # ignored: @resource wins

    @node.resource("db")
    async def db(ctx: Any) -> Any:
        yield "from-resource"

    worker.add_nodes(node)
    broker = worker._client.broker

    with caplog.at_level(logging.WARNING):
        async with TestKafkaBroker(broker):
            await worker.start()
            assert node.resources["db"] == "from-resource"
            await _publish(broker, _frame_envelope("collide_b.in"), "collide_b.in", "cid-collide-b")
            await worker.stop()

    assert captured["resources"] == {"db": "from-resource"}
    assert any("callbacks are ignored" in r.message for r in caplog.records)


def test_duplicate_resource_name_on_one_owner_raises() -> None:
    """A duplicate ``@resource`` NAME on a single owner still raises
    ``LifecycleConfigError`` at registration time."""
    node = _ProbeNode({}, node_id="dup", subscribe_topics=["dup.in"])

    @node.resource("db")
    async def db1(ctx: Any) -> Any:
        yield "a"

    with pytest.raises(LifecycleConfigError):

        @node.resource("db")
        async def db2(ctx: Any) -> Any:
            yield "b"


async def test_different_keys_coexist_resource_and_callback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The ``@resource`` and callback patterns coexist when each owns its own
    bag (one pattern *per owner*): a ``@resource`` node and a callback-shape
    worker both run, with no "callbacks ignored" warning.

    The node's handler sees the *merged* view — the worker's callback-managed
    key plus the node's own ``@resource`` value — because worker-scoped
    resources merge into every node handler (node wins on key collision).
    """
    import logging

    captured: dict[str, Any] = {}
    worker = _make_worker()
    node = _ProbeNode(captured, node_id="coexist", subscribe_topics=["coexist.in"])

    # @resource pattern on the node.
    @node.resource("res_key")
    async def res_key(ctx: Any) -> Any:
        yield "res-value"

    # Callback pattern on the worker (a different owner — no mixing, no warning).
    @worker.on_startup
    async def cb_key(ctx: Any) -> None:
        ctx.resources["cb_key"] = "cb-value"

    worker.add_nodes(node)
    broker = worker._client.broker

    with caplog.at_level(logging.WARNING):
        async with TestKafkaBroker(broker):
            await worker.start()
            assert node.resources["res_key"] == "res-value"
            assert worker.resources["cb_key"] == "cb-value"
            await _publish(broker, _frame_envelope("coexist.in"), "coexist.in", "cid-coexist")
            await worker.stop()

    # Both patterns ran; the handler saw the merged view (worker key + node key).
    assert captured["resources"] == {"cb_key": "cb-value", "res_key": "res-value"}
    # No mixing on a single owner, so no "callbacks ignored" warning fired.
    assert not any("callbacks are ignored" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Worker-scoped resources merge into node handlers (node wins on key collision)
# ---------------------------------------------------------------------------


async def test_worker_resource_reaches_node_handler() -> None:
    """A ``@worker.resource`` (worker-scoped) is visible to every node's handler
    as ``ctx.resources[...]`` — the canonical "one shared pool for all nodes"
    pattern."""
    captured: dict[str, Any] = {}
    closed: list[str] = []

    worker = _make_worker()
    node = _ProbeNode(captured, node_id="wres_probe", subscribe_topics=["wres_probe.in"])

    @worker.resource("shared_pool")
    async def shared_pool(ctx: Any) -> Any:
        try:
            yield "WORKER-POOL"
        finally:
            closed.append("shared_pool")

    worker.add_nodes(node)
    broker = worker._client.broker

    async with TestKafkaBroker(broker):
        await worker.start()
        await _publish(broker, _frame_envelope("wres_probe.in"), "wres_probe.in", "cid-wres")
        await worker.stop()

    # The node handler saw the worker-scoped resource even though the node
    # declared none of its own.
    assert captured["resources"] == {"shared_pool": "WORKER-POOL"}
    assert closed == ["shared_pool"]


async def test_node_resource_wins_over_worker_resource_on_same_key() -> None:
    """When a worker and a node both own a resource under the same key, the
    node's value wins in that node's handler (merge is ``{**worker, **node}``)."""
    captured: dict[str, Any] = {}

    worker = _make_worker()
    node = _ProbeNode(captured, node_id="merge_probe", subscribe_topics=["merge_probe.in"])

    @worker.resource("db")
    async def worker_db(ctx: Any) -> Any:
        yield "WORKER-DB"

    # A worker-only key so the assertion can distinguish "merge happened, node
    # wins" from "no merge" and from "wrong precedence".
    @worker.resource("shared")
    async def worker_shared(ctx: Any) -> Any:
        yield "SHARED"

    @node.resource("db")
    async def node_db(ctx: Any) -> Any:
        yield "NODE-DB"

    worker.add_nodes(node)
    broker = worker._client.broker

    async with TestKafkaBroker(broker):
        await worker.start()
        # While serving, the two bags are independent (the merge copies, never
        # mutates): worker holds its value, node holds its own.
        assert worker.resources["db"] == "WORKER-DB"
        assert node.resources["db"] == "NODE-DB"
        await _publish(broker, _frame_envelope("merge_probe.in"), "merge_probe.in", "cid-merge")
        await worker.stop()

    # Merged view: worker-only key present, and on the shared key the node wins.
    # (Fails if the merge is dropped -> missing "shared"; or precedence reversed
    # -> db == "WORKER-DB".)
    assert captured["resources"] == {"db": "NODE-DB", "shared": "SHARED"}


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
    async def up(ctx: ServingContext[Worker]) -> None:
        await ctx.broker.publish({"id": ctx.owner.id, "status": "up"}, topic="fleet.presence")

    @worker.on_shutdown
    async def down(ctx: ServingContext[Worker]) -> None:
        await ctx.broker.publish({"id": ctx.owner.id, "status": "down"}, topic="fleet.presence")

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
