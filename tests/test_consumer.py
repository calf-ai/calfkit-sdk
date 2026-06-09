"""End-to-end and unit tests for ``ConsumerNode`` and the ``@consumer`` decorator.

The consumer rides the shared ``BaseNodeDef.handler``: ``run`` is the inherited
``@handler('*')`` catch-all, and the user function receives a
:class:`~calfkit.models.consumer_context.ConsumerContext` (not the client-facing
``NodeResult``). Direct ``handler()`` calls pin contracts that TestKafkaBroker's
propagation makes ambiguous (error swallowing, projection skips).
"""

import asyncio
import logging
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker
from pydantic import TypeAdapter

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND
from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    ToolCallPart,
)
from calfkit._vendor.pydantic_ai.messages import (
    TextPart as ModelTextPart,
)
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client, NodeResult
from calfkit.exceptions import DeserializationError
from calfkit.models import ConsumerContext, SessionRunContext
from calfkit.models.envelope import Envelope
from calfkit.models.payload import DataPart, TextPart
from calfkit.models.session_context import CallFrameStack, WorkflowState
from calfkit.models.state import State
from calfkit.nodes import Agent, ConsumerNode, consumer
from calfkit.worker import Worker
from tests.providers import prepare_worker

CONSUMER_LOGGER = "calfkit.nodes.consumer"
BASE_LOGGER = "calfkit.nodes.base"
_HEADERS = {HDR_EMITTER: b"upstream", HDR_EMITTER_KIND: b"agent"}


@dataclass
class Report:
    location: str
    summary: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _text_then_done() -> FunctionModel:
    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ModelTextPart("done")])

    return FunctionModel(_fn)


def _structured_report_model() -> FunctionModel:
    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ModelTextPart('{"location":"Tokyo","summary":"sunny"}')])

    return FunctionModel(_fn)


def _wire_agent_with_consumer(container, consumer_node: ConsumerNode) -> Agent:
    worker = container.get(Worker)
    agent = Agent(
        "consumer_test_agent",
        system_prompt="x",
        subscribe_topics="consumer_test_agent.input",
        publish_topic="consumer_test_agent.output",
        model_client=_text_then_done(),
    )
    worker.add_nodes(agent, consumer_node)
    prepare_worker(container)
    return agent


def _text_state(text: str) -> State:
    s = State()
    s.final_output_parts = [TextPart(text=text)]
    return s


def _data_state(data: dict) -> State:
    s = State()
    s.final_output_parts = [DataPart(data=data)]
    return s


def _envelope(state: State, *, deps: dict | None = None) -> Envelope:
    """A frameless envelope — the shape a sink sees tapping a publish_topic."""
    return Envelope(
        context=SessionRunContext(state=state, deps=deps or {}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
    )


async def _handle(node: ConsumerNode, envelope: Envelope, *, correlation_id: str = "cid-00000000", headers: dict | None = None) -> object:
    return await node.handler(
        envelope,
        correlation_id=correlation_id,
        headers=_HEADERS if headers is None else headers,
        broker=MagicMock(),
    )


# ---------------------------------------------------------------------------
# Construction / static invariants
# ---------------------------------------------------------------------------


def test_consumer_node_kind_is_consumer():
    assert ConsumerNode._node_kind == "consumer"


def test_decorator_sets_default_node_id_from_fn_name():
    @consumer(subscribe_topics="t")
    def my_sink(ctx: ConsumerContext) -> None:
        return None

    assert isinstance(my_sink, ConsumerNode)
    assert my_sink.node_id == "consumer_my_sink"
    assert my_sink.subscribe_topics == ["t"]
    assert my_sink.publish_topic is None


def test_decorator_accepts_explicit_node_id_and_list_topics():
    @consumer(subscribe_topics=["a", "b"], node_id="custom_id")
    def my_sink(ctx: ConsumerContext) -> None:
        return None

    assert my_sink.node_id == "custom_id"
    assert my_sink.subscribe_topics == ["a", "b"]


def test_class_form_normalizes_str_topic_to_list():
    node = ConsumerNode(node_id="x", subscribe_topics="single_topic", consume_fn=lambda ctx: None)
    assert node.subscribe_topics == ["single_topic"]
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


# ---------------------------------------------------------------------------
# ConsumerContext.from_run_context (the projection)
# ---------------------------------------------------------------------------


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
    assert ConsumerContext.from_run_context(ctx).output is None


# ---------------------------------------------------------------------------
# End-to-end: consumer receives a ConsumerContext from agent output
# ---------------------------------------------------------------------------


async def test_sync_consumer_receives_context_from_agent_output(container):
    received: list[ConsumerContext] = []

    @consumer(subscribe_topics="consumer_test_agent.output")
    def sink(ctx: ConsumerContext) -> None:
        received.append(ctx)

    agent = _wire_agent_with_consumer(container, sink)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        await client.execute_node("hi", agent.subscribe_topics[0], timeout=5)

    final = [c for c in received if any(isinstance(p, TextPart) for p in c.output_parts)]
    assert final, f"consumer never saw a terminal envelope; saw={received}"
    assert final[-1].output == "done"
    assert isinstance(final[-1], ConsumerContext)


async def test_async_consumer_receives_context_from_agent_output(container):
    received: list[ConsumerContext] = []

    @consumer(subscribe_topics="consumer_test_agent.output")
    async def sink(ctx: ConsumerContext) -> None:
        await asyncio.sleep(0)
        received.append(ctx)

    agent = _wire_agent_with_consumer(container, sink)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        await client.execute_node("hi", agent.subscribe_topics[0], timeout=5)

    final = [c for c in received if any(isinstance(p, TextPart) for p in c.output_parts)]
    assert final
    assert final[-1].output == "done"


async def test_consumer_sees_upstream_emitter_identity(container):
    received: list[ConsumerContext] = []

    @consumer(subscribe_topics="consumer_test_agent.output")
    def sink(ctx: ConsumerContext) -> None:
        received.append(ctx)

    agent = _wire_agent_with_consumer(container, sink)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        await client.execute_node("hi", agent.subscribe_topics[0], timeout=5)

    final = [c for c in received if any(isinstance(p, TextPart) for p in c.output_parts)]
    assert final
    assert final[-1].emitter_node_id == agent.node_id
    assert final[-1].emitter_node_kind == "agent"


async def test_consumer_deserializes_output_type_dataclass(container):
    received: list[ConsumerContext[Report]] = []

    @consumer(subscribe_topics="structured_agent.output", output_type=Report)
    def sink(ctx: ConsumerContext[Report]) -> None:
        received.append(ctx)

    worker = container.get(Worker)
    structured_agent = Agent[Report](
        "structured_agent",
        system_prompt="x",
        subscribe_topics="structured_agent.input",
        publish_topic="structured_agent.output",
        model_client=_structured_report_model(),
        final_output_type=Report,
    )
    worker.add_nodes(structured_agent, sink)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        await client.execute_node("hi", structured_agent.subscribe_topics[0], timeout=5)

    final = [c for c in received if any(isinstance(p, DataPart) for p in c.output_parts)]
    assert final, f"no final DataPart envelope observed; received={received}"
    assert isinstance(final[-1].output, Report)
    assert final[-1].output.location == "Tokyo"
    assert final[-1].output.summary == "sunny"


async def test_consumer_fans_in_across_multiple_topics(container):
    """Regression guard: ``broker.subscriber(*topics)`` must wire EVERY topic."""
    received: list[ConsumerContext] = []

    @consumer(subscribe_topics=["fanin.a", "fanin.b"])
    def sink(ctx: ConsumerContext) -> None:
        received.append(ctx)

    worker = container.get(Worker)
    worker.add_nodes(sink)
    prepare_worker(container)

    broker = container.get(KafkaBroker)

    headers_a = {HDR_EMITTER: "src_a", HDR_EMITTER_KIND: "client"}
    headers_b = {HDR_EMITTER: "src_b", HDR_EMITTER_KIND: "client"}
    async with TestKafkaBroker(broker):
        await broker.publish(_envelope(_text_state("from_a")), topic="fanin.a", correlation_id="cid-a", headers=headers_a)
        await broker.publish(_envelope(_text_state("from_b")), topic="fanin.b", correlation_id="cid-b", headers=headers_b)

    assert {c.output for c in received} == {"from_a", "from_b"}


# ---------------------------------------------------------------------------
# Intermediate-hop / projection-skip semantics (direct handler)
# ---------------------------------------------------------------------------


async def test_intermediate_hop_passes_to_fn_with_output_none(caplog):
    """Empty final_output_parts (intermediate hop) → ctx.output is None, NO error log."""
    received: list[ConsumerContext] = []
    node = ConsumerNode(node_id="int_sink", subscribe_topics="t", consume_fn=received.append)

    with caplog.at_level(logging.ERROR, logger=CONSUMER_LOGGER):
        await _handle(node, _envelope(State()))

    assert len(received) == 1
    assert received[0].output is None
    assert received[0].output_parts == []
    assert not [r for r in caplog.records if "projection failed" in r.getMessage()]


async def test_validation_error_on_present_parts_logs_and_skips(caplog):
    """Parts present but don't match output_type → skip + ERROR with context."""
    invocations: list[str] = []
    node = ConsumerNode(node_id="val_sink", subscribe_topics="t", consume_fn=lambda ctx: invocations.append("ran"), output_type=Report)

    with caplog.at_level(logging.ERROR, logger=CONSUMER_LOGGER):
        await _handle(node, _envelope(_data_state({"unexpected": "shape"})))

    assert invocations == []
    rec = [r for r in caplog.records if "projection failed" in r.getMessage()]
    assert rec, "validation error must be logged at ERROR"
    assert "emitter=upstream" in rec[0].getMessage()
    assert "output_type=Report" in rec[0].getMessage()


async def test_missing_emitter_header_warns_and_still_invokes_fn(caplog):
    """No x-calf-emitter header → BaseNodeDef.handler warns; fn still runs with
    emitter_node_id=None (a legitimate non-calfkit producer)."""
    received: list[ConsumerContext] = []
    node = ConsumerNode(node_id="noemit_sink", subscribe_topics="t", consume_fn=received.append)

    with caplog.at_level(logging.WARNING, logger=BASE_LOGGER):
        await _handle(node, _envelope(_text_state("hello")), headers={})

    assert len(received) == 1
    assert received[0].output == "hello"
    assert received[0].emitter_node_id is None
    assert any("emitter unknown" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# Error handling (direct handler)
# ---------------------------------------------------------------------------


async def test_consume_fn_exception_swallowed_and_logged(caplog):
    """consume_fn raises → logged at ERROR and swallowed (handler returns normally)."""

    def boom(ctx: ConsumerContext) -> None:
        raise RuntimeError("intentional consumer failure")

    node = ConsumerNode(node_id="boom_sink", subscribe_topics="t", consume_fn=boom)

    with caplog.at_level(logging.ERROR, logger=CONSUMER_LOGGER):
        resp = await _handle(node, _envelope(_text_state("hi")))

    assert resp is not None
    err = [r for r in caplog.records if "consume_fn raised" in r.getMessage()]
    assert err
    assert "emitter=upstream" in err[0].getMessage()
    assert "kind=agent" in err[0].getMessage()


async def test_cancelled_error_always_propagates():
    """Even though consume_fn errors are swallowed, CancelledError must propagate."""

    async def cancels(ctx: ConsumerContext) -> None:
        raise asyncio.CancelledError()

    node = ConsumerNode(node_id="cancel_sink", subscribe_topics="t", consume_fn=cancels)
    with pytest.raises(asyncio.CancelledError):
        await _handle(node, _envelope(_text_state("hi")))


async def test_callable_class_generator_detected_at_call_site(caplog):
    """A callable-class whose __call__ is a generator passes _validate_consume_fn
    but yields a generator object — detected at the call site, logged, swallowed."""

    class GenSink:
        def __call__(self, ctx):
            yield ctx

    node = ConsumerNode(node_id="gen_sink", subscribe_topics="t", consume_fn=GenSink())
    with caplog.at_level(logging.ERROR, logger=CONSUMER_LOGGER):
        await _handle(node, _envelope(_text_state("hi")))

    rec = [r for r in caplog.records if "consume_fn raised" in r.getMessage()]
    assert rec and isinstance(rec[0].exc_info[1], TypeError)


async def test_function_returning_generator_object_detected(caplog):
    def _wrapper_gen():
        yield 1

    def sink(ctx):
        return _wrapper_gen()

    node = ConsumerNode(node_id="gen_ret", subscribe_topics="t", consume_fn=sink)
    with caplog.at_level(logging.ERROR, logger=CONSUMER_LOGGER):
        await _handle(node, _envelope(_text_state("hi")))

    rec = [r for r in caplog.records if "consume_fn raised" in r.getMessage()]
    assert rec and isinstance(rec[0].exc_info[1], TypeError)
    assert "generator" in str(rec[0].exc_info[1])


async def test_function_returning_async_generator_object_detected(caplog):
    async def _wrapper_agen():
        yield 1

    def sink(ctx):
        return _wrapper_agen()

    node = ConsumerNode(node_id="agen_ret", subscribe_topics="t", consume_fn=sink)
    with caplog.at_level(logging.ERROR, logger=CONSUMER_LOGGER):
        await _handle(node, _envelope(_text_state("hi")))

    rec = [r for r in caplog.records if "consume_fn raised" in r.getMessage()]
    assert rec and isinstance(rec[0].exc_info[1], TypeError)
    assert "async_generator" in str(rec[0].exc_info[1])


# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------


async def test_gate_rejection_skips_consume_fn(container):
    invocations: list[str] = []

    @consumer(subscribe_topics="consumer_test_agent.output", gates=[lambda ctx: False])
    def sink(ctx: ConsumerContext) -> None:
        invocations.append("ran")

    agent = _wire_agent_with_consumer(container, sink)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.execute_node("hi", agent.subscribe_topics[0], timeout=5)
        assert result.output == "done"

    assert invocations == []


async def test_gate_acceptance_runs_consume_fn(container):
    gate_calls: list[str] = []
    invocations: list[str] = []

    def accept(ctx: SessionRunContext) -> bool:
        gate_calls.append("g")
        return True

    @consumer(subscribe_topics="consumer_test_agent.output", gates=[accept])
    def sink(ctx: ConsumerContext) -> None:
        invocations.append("ran")

    agent = _wire_agent_with_consumer(container, sink)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        await client.execute_node("hi", agent.subscribe_topics[0], timeout=5)

    assert gate_calls
    assert invocations


async def test_final_only_gate_idiom_filters_intermediate():
    """Documented idiom: a gate keyed off final_output_parts drops intermediate hops."""
    received: list[ConsumerContext] = []

    def has_final_output(ctx: SessionRunContext) -> bool:
        return bool(ctx.state.final_output_parts)

    node = ConsumerNode(node_id="filtered", subscribe_topics="t", consume_fn=received.append, gates=[has_final_output])

    await _handle(node, _envelope(State()), correlation_id="cid-int")  # intermediate → rejected
    await _handle(node, _envelope(_text_state("final!")), correlation_id="cid-final")  # terminal → accepted

    assert len(received) == 1
    assert received[0].output == "final!"


# ---------------------------------------------------------------------------
# Full session-state visibility via ctx.state
# ---------------------------------------------------------------------------


async def test_consumer_sees_tool_results_via_state():
    """A consumer wired to a tool's output reads the tool's return value through
    ``ctx.state.tool_results`` (full session-state visibility)."""
    captured: list[ConsumerContext] = []
    node = ConsumerNode(node_id="tool_obs", subscribe_topics="t", consume_fn=captured.append)

    state = State()
    tool_call = ToolCallPart(tool_name="get_weather", args={"location": "Tokyo"})
    state.add_tool_call(tool_call)
    state.add_tool_result(tool_call.tool_call_id, "sunny in Tokyo")

    await _handle(node, _envelope(state), headers={HDR_EMITTER: "tool_get_weather", HDR_EMITTER_KIND: "tool"})

    assert len(captured) == 1
    c = captured[0]
    assert c.output is None  # tool hop — no final_output_parts
    assert c.emitter_node_kind == "tool"
    assert c.state.tool_results[tool_call.tool_call_id] == "sunny in Tokyo"
    assert c.state.tool_calls[tool_call.tool_call_id].tool_name == "get_weather"


async def test_consumer_reads_inbound_deps():
    captured: list[ConsumerContext] = []
    node = ConsumerNode(node_id="deps_sink", subscribe_topics="t", consume_fn=captured.append)

    await _handle(node, _envelope(_text_state("hi"), deps={"discord": {"channel_id": 42}}), correlation_id="cid-deps")

    assert captured[0].deps == {"discord": {"channel_id": 42}}
    assert captured[0].correlation_id == "cid-deps"


async def test_consumer_deps_defaults_to_empty_dict():
    captured: list[ConsumerContext] = []
    node = ConsumerNode(node_id="deps_empty", subscribe_topics="t", consume_fn=captured.append)

    await _handle(node, _envelope(_text_state("hi")))

    assert captured[0].deps == {}


async def test_consumer_convenience_properties_read_through_state():
    captured: list[ConsumerContext] = []
    node = ConsumerNode(node_id="props", subscribe_topics="t", consume_fn=captured.append)

    state = State()
    state.final_output_parts = [TextPart(text="hello")]
    state.message_history = [ModelRequest.user_text_prompt("hi")]
    state.metadata = {"app": "demo"}

    await _handle(node, _envelope(state))

    c = captured[0]
    assert c.output_parts is c.state.final_output_parts
    assert c.message_history is c.state.message_history
    assert c.metadata is c.state.metadata


async def test_resources_flow_from_node_bag():
    captured: list[ConsumerContext] = []
    node = ConsumerNode(node_id="res_sink", subscribe_topics="t", consume_fn=captured.append)
    sentinel = object()
    node.resources["db"] = sentinel

    await _handle(node, _envelope(_text_state("hi")))

    assert captured[0].resources["db"] is sentinel


async def test_deps_round_trip_through_agent_to_consumer(container):
    """E2E: deps passed to execute_node surface on the consumer's ctx.deps."""
    received: list[ConsumerContext] = []

    @consumer(subscribe_topics="consumer_test_agent.output")
    def sink(ctx: ConsumerContext) -> None:
        received.append(ctx)

    agent = _wire_agent_with_consumer(container, sink)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.execute_node("hi", agent.subscribe_topics[0], deps={"discord": {"channel_id": 7}}, timeout=5)

    assert result.deps == {"discord": {"channel_id": 7}}
    assert received, "consumer saw no envelopes"
    assert all(c.deps == {"discord": {"channel_id": 7}} for c in received)


# ---------------------------------------------------------------------------
# ConsumerContext value semantics
# ---------------------------------------------------------------------------


def test_consumer_context_is_not_hashable():
    state = State()
    state.final_output_parts = [TextPart(text="x")]
    cctx = ConsumerContext(output="x", state=state, correlation_id="cid")

    assert ConsumerContext.__hash__ is None
    with pytest.raises(TypeError, match="unhashable"):
        hash(cctx)


def test_consumer_context_is_frozen():
    cctx = ConsumerContext(output="x", state=State(), correlation_id="cid")
    with pytest.raises(Exception):  # FrozenInstanceError
        cctx.output = "different"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Pre-built TypeAdapter — built once at construction, reused per envelope
# ---------------------------------------------------------------------------


def test_consumer_reuses_prebuilt_type_adapter(monkeypatch):
    import sys

    import calfkit.models.node_result as node_result_mod

    consumer_mod = sys.modules["calfkit.nodes.consumer"]
    real_adapter = TypeAdapter
    construct_count = {"n": 0}

    class _CountingAdapter:
        def __init__(self, *args, **kwargs):
            construct_count["n"] += 1
            self._adapter = real_adapter(*args, **kwargs)

        def validate_python(self, *args, **kwargs):
            return self._adapter.validate_python(*args, **kwargs)

    monkeypatch.setattr(node_result_mod, "TypeAdapter", _CountingAdapter)
    monkeypatch.setattr(consumer_mod, "TypeAdapter", _CountingAdapter)

    @consumer(subscribe_topics="prebuilt.topic", output_type=Report)
    def sink(ctx: ConsumerContext[Report]) -> None:
        return None

    constructions_after_decoration = construct_count["n"]
    assert constructions_after_decoration == 1  # built once at __init__

    env = _envelope(_data_state({"location": "Tokyo", "summary": "sunny"}))
    for _ in range(3):
        asyncio.run(sink.handler(env, correlation_id="cid-reuse", headers=_HEADERS, broker=MagicMock()))

    assert construct_count["n"] == constructions_after_decoration  # no new adapters


def test_unschematizable_output_type_raises_at_construction():
    class BadType:
        __slots__ = ("_blocker",)

    try:
        TypeAdapter(BadType)
    except Exception:
        with pytest.raises(Exception):
            ConsumerNode(node_id="bad", subscribe_topics="t", consume_fn=lambda ctx: None, output_type=BadType)
    else:
        pytest.skip("TypeAdapter(BadType) succeeded on this pydantic version")


# ---------------------------------------------------------------------------
# Safety-net Exception catch in the projection block
# ---------------------------------------------------------------------------


async def test_unexpected_projection_exception_is_logged_and_skipped(monkeypatch, caplog):
    """Anything past (DeserializationError, ValidationError) is logged and skipped,
    so one bad envelope can't poison-pill the offset."""
    import sys

    invocations: list[str] = []
    node = ConsumerNode(node_id="safety", subscribe_topics="t", consume_fn=lambda ctx: invocations.append("ran"))

    consumer_mod = sys.modules["calfkit.nodes.consumer"]

    class _BoomContext:
        @staticmethod
        def from_run_context(*args, **kwargs):
            raise RuntimeError("simulated unforeseen projection bug")

    monkeypatch.setattr(consumer_mod, "ConsumerContext", _BoomContext)

    with caplog.at_level(logging.ERROR, logger=CONSUMER_LOGGER):
        resp = await _handle(node, _envelope(_text_state("hi")), correlation_id="cid-safety")

    assert invocations == []
    assert resp is not None
    rec = [r for r in caplog.records if "unexpected projection error" in r.getMessage()]
    assert rec
    assert isinstance(rec[0].exc_info[1], RuntimeError)


# ---------------------------------------------------------------------------
# Client-side NodeResult contract — the consumer no longer uses NodeResult, but
# Client.execute_node / InvocationHandle.result still return it. Kept here to
# preserve the original coverage.
# ---------------------------------------------------------------------------


def test_node_result_is_not_hashable():
    state = State()
    state.final_output_parts = [TextPart(text="x")]
    state.message_history = [ModelRequest.user_text_prompt("hi")]
    result = NodeResult(output="x", state=state, correlation_id="cid-hash")

    assert NodeResult.__hash__ is None
    with pytest.raises(TypeError, match="unhashable"):
        hash(result)


def test_strict_mode_raises_on_empty_final_output_parts():
    envelope = _envelope(State())
    with pytest.raises(DeserializationError, match="No DataPart or TextPart"):
        NodeResult.from_envelope(envelope, correlation_id="cid-strict-empty")  # strict=True default


def test_lenient_mode_returns_none_on_empty_final_output_parts():
    envelope = _envelope(State())
    result = NodeResult.from_envelope(envelope, correlation_id="cid-lenient", strict=False)
    assert result.output is None
    assert result.state is envelope.context.state  # client path: no copy in from_envelope


async def test_client_execute_node_result_carries_state(container):
    worker = container.get(Worker)
    agent = Agent(
        "state_vis_agent",
        system_prompt="x",
        subscribe_topics="state_vis_agent.input",
        publish_topic="state_vis_agent.output",
        model_client=_text_then_done(),
    )
    worker.add_nodes(agent)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.execute_node("hi there", agent.subscribe_topics[0], timeout=5)

    assert result.output == "done"
    assert isinstance(result.state, State)
    assert len(result.state.message_history) >= 1


def test_correlation_id_raises_when_unstamped():
    from calfkit.models import ToolContext

    ctx = SessionRunContext(state=State(), deps={})
    with pytest.raises(RuntimeError, match="correlation_id is unset"):
        _ = ctx.correlation_id

    tool_ctx = ToolContext(deps={})  # run_id defaults to None
    with pytest.raises(RuntimeError, match="without a run_id"):
        _ = tool_ctx.correlation_id
