"""End-to-end and unit tests for ConsumerNodeDef and the @consumer decorator.

Mirrors the project pattern: TestKafkaBroker for in-memory routing,
FunctionModel for deterministic agent runs, no broker mocking for E2E flows.

Direct handler-level tests bypass the broker to pin contracts that
TestKafkaBroker's propagation semantics make ambiguous (e.g. ``re_raise``).
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
    ToolReturnPart,
)
from calfkit._vendor.pydantic_ai.messages import (
    TextPart as ModelTextPart,
)
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client, NodeResult
from calfkit.models import SessionRunContext
from calfkit.models.envelope import Envelope
from calfkit.models.payload import DataPart, TextPart
from calfkit.models.session_context import CallFrameStack, WorkflowState
from calfkit.models.state import State
from calfkit.nodes import Agent, ConsumerNodeDef, consumer
from calfkit.worker import Worker
from tests.providers import prepare_worker

CONSUMER_LOGGER = "calfkit.nodes.consumer"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _text_then_done() -> FunctionModel:
    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ModelTextPart("done")])

    return FunctionModel(_fn)


def _wire_agent_with_consumer(container, consumer_node: ConsumerNodeDef) -> Agent:
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


def _make_ctx(state: State, correlation_id: str, deps: dict | None = None) -> SessionRunContext:
    """Build a context with deps and a pre-stamped correlation_id (as a handler would)."""
    ctx = SessionRunContext(state=state, deps=deps or {})
    ctx._correlation_id = correlation_id
    return ctx


def _envelope_with_text(text: str, correlation_id: str = "test-cid-00000000", deps: dict | None = None) -> Envelope:
    state = State()
    state.final_output_parts = [TextPart(text=text)]
    return Envelope(
        context=_make_ctx(state, correlation_id, deps),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
    )


def _envelope_without_final_parts(correlation_id: str = "test-cid-00000000", deps: dict | None = None) -> Envelope:
    """Intermediate-hop shape: state populated but no final_output_parts."""
    return Envelope(
        context=_make_ctx(State(), correlation_id, deps),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
    )


def _envelope_with_data(data: dict, correlation_id: str = "test-cid-00000000", deps: dict | None = None) -> Envelope:
    state = State()
    state.final_output_parts = [DataPart(data=data)]
    return Envelope(
        context=_make_ctx(state, correlation_id, deps),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
    )


# ---------------------------------------------------------------------------
# Static / construction-level invariants
# ---------------------------------------------------------------------------


def test_consumer_node_kind_is_consumer():
    assert ConsumerNodeDef._node_kind == "consumer"


def test_decorator_sets_default_node_id_from_fn_name():
    @consumer(subscribe_topics="t")
    def my_sink(result: NodeResult) -> None:
        return None

    assert my_sink.node_id == "consumer_my_sink"
    assert my_sink.subscribe_topics == ["t"]
    assert my_sink.publish_topic is None


def test_decorator_accepts_explicit_node_id_and_list_topics():
    @consumer(subscribe_topics=["a", "b"], node_id="custom_id")
    def my_sink(result: NodeResult) -> None:
        return None

    assert my_sink.node_id == "custom_id"
    assert my_sink.subscribe_topics == ["a", "b"]


def test_class_form_normalizes_str_topic_to_list():
    node = ConsumerNodeDef(
        node_id="x",
        subscribe_topics="single_topic",
        consume_fn=lambda r: None,
    )
    assert node.subscribe_topics == ["single_topic"]
    assert node.publish_topic is None


def test_publish_topic_setter_rejects_non_none():
    """Consumers are terminal sinks; the no-publish invariant must hold for
    the life of the instance to prevent Worker from wiring up a publisher."""
    node = ConsumerNodeDef(node_id="x", subscribe_topics="t", consume_fn=lambda r: None)
    with pytest.raises(AttributeError, match="terminal sinks"):
        node.publish_topic = "sneaky.topic"
    assert node.publish_topic is None


def test_publish_topic_setter_accepts_none():
    """None must be accepted (BaseNodeSchema's dataclass __init__ sets it)."""
    node = ConsumerNodeDef(node_id="x", subscribe_topics="t", consume_fn=lambda r: None)
    node.publish_topic = None  # must not raise
    assert node.publish_topic is None


def test_generator_function_rejected_at_construction():
    """A generator function would return an iterator from the handler call,
    which is neither None nor awaitable — the user body would never run."""

    def gen(result):
        yield result

    with pytest.raises(TypeError, match="generator function"):
        ConsumerNodeDef(node_id="x", subscribe_topics="t", consume_fn=gen)


def test_async_generator_function_rejected_at_construction():
    async def agen(result):
        yield result

    with pytest.raises(TypeError, match="async generator function"):
        ConsumerNodeDef(node_id="x", subscribe_topics="t", consume_fn=agen)


async def test_run_raises_assertion_error_if_called():
    """Defensive guard: handler() is overridden and run() should never be
    reached. If a future refactor calls it, fail loud."""
    node = ConsumerNodeDef(node_id="x", subscribe_topics="t", consume_fn=lambda r: None)
    ctx = SessionRunContext(state=State(), deps={})

    with pytest.raises(AssertionError, match="never be invoked"):
        await node.run(ctx)


# ---------------------------------------------------------------------------
# End-to-end: consumer receives client-facing NodeResult
# ---------------------------------------------------------------------------


async def test_sync_consumer_receives_node_result_from_agent_output(container):
    received: list[NodeResult] = []

    @consumer(subscribe_topics="consumer_test_agent.output")
    def sink(result: NodeResult) -> None:
        received.append(result)

    agent = _wire_agent_with_consumer(container, sink)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        await client.execute_node("hi", agent.subscribe_topics[0], timeout=5)

    final = [r for r in received if any(isinstance(p, TextPart) for p in r.output_parts)]
    assert final, f"consumer never saw an envelope with final output_parts; saw={received}"
    assert final[-1].output == "done"
    assert isinstance(final[-1], NodeResult)


async def test_async_consumer_receives_node_result_from_agent_output(container):
    received: list[NodeResult] = []

    @consumer(subscribe_topics="consumer_test_agent.output")
    async def sink(result: NodeResult) -> None:
        await asyncio.sleep(0)
        received.append(result)

    agent = _wire_agent_with_consumer(container, sink)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        await client.execute_node("hi", agent.subscribe_topics[0], timeout=5)

    final = [r for r in received if any(isinstance(p, TextPart) for p in r.output_parts)]
    assert final
    assert final[-1].output == "done"


async def test_consumer_sees_upstream_emitter_id_on_result(container):
    """The NodeResult passed to the consumer carries the upstream node's
    x-calf-emitter identity (proves we wire emitter headers, not just body)."""
    received: list[NodeResult] = []

    @consumer(subscribe_topics="consumer_test_agent.output")
    def sink(result: NodeResult) -> None:
        received.append(result)

    agent = _wire_agent_with_consumer(container, sink)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        await client.execute_node("hi", agent.subscribe_topics[0], timeout=5)

    final = [r for r in received if any(isinstance(p, TextPart) for p in r.output_parts)]
    assert final
    assert final[-1].emitter_node_id == agent.node_id
    assert final[-1].emitter_node_kind == "agent"


# ---------------------------------------------------------------------------
# output_type deserialization
# ---------------------------------------------------------------------------


@dataclass
class Report:
    location: str
    summary: str


def _structured_report_model() -> FunctionModel:
    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        if isinstance(last, ModelRequest) and any(isinstance(p, ToolReturnPart) for p in last.parts):
            return ModelResponse(parts=[ModelTextPart('{"location":"Tokyo","summary":"sunny"}')])
        return ModelResponse(parts=[ModelTextPart('{"location":"Tokyo","summary":"sunny"}')])

    return FunctionModel(_fn)


async def test_consumer_deserializes_output_type_dataclass(container):
    received: list[NodeResult[Report]] = []

    @consumer(subscribe_topics="structured_agent.output", output_type=Report)
    def sink(result: NodeResult[Report]) -> None:
        received.append(result)

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

    final = [r for r in received if any(isinstance(p, DataPart) for p in r.output_parts)]
    assert final, f"no final DataPart envelope observed; received={received}"
    assert isinstance(final[-1].output, Report)
    assert final[-1].output.location == "Tokyo"
    assert final[-1].output.summary == "sunny"


# ---------------------------------------------------------------------------
# Intermediate-hop / "consume all outputs" semantics
# ---------------------------------------------------------------------------


async def test_intermediate_hop_passes_to_fn_with_output_none(container, caplog):
    """An envelope with empty final_output_parts (intermediate agent hop, tool
    completion) is passed to the user fn with ``result.output is None``. NO
    error log — intermediate hops are expected, not malformed."""
    received: list[NodeResult] = []

    @consumer(subscribe_topics="intermediate.topic")
    def sink(result: NodeResult) -> None:
        received.append(result)

    worker = container.get(Worker)
    worker.add_nodes(sink)
    prepare_worker(container)

    broker = container.get(KafkaBroker)

    envelope = _envelope_without_final_parts("cid-int-0")
    with caplog.at_level(logging.ERROR, logger=CONSUMER_LOGGER):
        async with TestKafkaBroker(broker):
            await broker.publish(
                envelope,
                topic="intermediate.topic",
                correlation_id="cid-int-0",
                headers={HDR_EMITTER: "upstream_node", HDR_EMITTER_KIND: "agent"},
            )

    assert len(received) == 1
    assert received[0].output is None
    assert received[0].emitter_node_id == "upstream_node"
    assert received[0].emitter_node_kind == "agent"
    assert received[0].output_parts == []
    deserialize_errors = [r for r in caplog.records if "deserialize failed" in r.getMessage()]
    assert not deserialize_errors, f"intermediate hop should not log a deserialize error: {deserialize_errors}"


async def test_validation_error_on_present_parts_logs_and_skips(container, caplog):
    """When parts ARE present but don't match the requested ``output_type``,
    that's a real malformed envelope (or schema drift). User fn is skipped,
    ERROR is logged with operational context."""
    invocations: list[str] = []

    @consumer(subscribe_topics="validation.topic", output_type=Report)
    def sink(result: NodeResult[Report]) -> None:
        invocations.append("ran")

    worker = container.get(Worker)
    worker.add_nodes(sink)
    prepare_worker(container)

    broker = container.get(KafkaBroker)

    # DataPart present but doesn't match Report dataclass shape → ValidationError.
    bad_envelope = _envelope_with_data({"unexpected": "shape"}, correlation_id="cid-bad-0")
    with caplog.at_level(logging.ERROR, logger=CONSUMER_LOGGER):
        async with TestKafkaBroker(broker):
            await broker.publish(
                bad_envelope,
                topic="validation.topic",
                correlation_id="cid-bad-0",
                headers={HDR_EMITTER: "upstream", HDR_EMITTER_KIND: "agent"},
            )

    assert invocations == []
    err_records = [r for r in caplog.records if "deserialize failed" in r.getMessage()]
    assert err_records, "validation error must be logged at ERROR"
    msg = err_records[0].getMessage()
    assert "emitter=upstream" in msg
    assert "output_type=Report" in msg


async def test_missing_emitter_header_warns_and_still_invokes_fn(container, caplog):
    """A consumer wired to a non-calfkit producer (no x-calf-emitter header)
    should log a warning, invoke the user fn with ``emitter_node_id=None``,
    and continue. Don't fail-loud on this — it's a legitimate integration."""
    received: list[NodeResult] = []

    @consumer(subscribe_topics="no_emitter.topic")
    def sink(result: NodeResult) -> None:
        received.append(result)

    worker = container.get(Worker)
    worker.add_nodes(sink)
    prepare_worker(container)

    broker = container.get(KafkaBroker)

    envelope = _envelope_with_text("hello", correlation_id="cid-noemit-0")
    with caplog.at_level(logging.WARNING, logger=CONSUMER_LOGGER):
        async with TestKafkaBroker(broker):
            await broker.publish(envelope, topic="no_emitter.topic", correlation_id="cid-noemit-0")

    assert len(received) == 1
    assert received[0].output == "hello"
    assert received[0].emitter_node_id is None
    assert any("emitter unknown" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# Multi-topic subscription — runtime E2E
# ---------------------------------------------------------------------------


async def test_consumer_fans_in_across_multiple_topics(container):
    """Regression guard for ``Worker.register_handlers`` unpacking the topic
    list: ``broker.subscriber(*topics)`` must wire the consumer to EVERY
    topic, not just the first."""
    received: list[NodeResult] = []

    @consumer(subscribe_topics=["fanin.a", "fanin.b"])
    def sink(result: NodeResult) -> None:
        received.append(result)

    worker = container.get(Worker)
    worker.add_nodes(sink)
    prepare_worker(container)

    broker = container.get(KafkaBroker)

    env_a = _envelope_with_text("from_a", correlation_id="cid-a")
    env_b = _envelope_with_text("from_b", correlation_id="cid-b")

    async with TestKafkaBroker(broker):
        await broker.publish(env_a, topic="fanin.a", correlation_id="cid-a", headers={HDR_EMITTER: "src_a", HDR_EMITTER_KIND: "client"})
        await broker.publish(env_b, topic="fanin.b", correlation_id="cid-b", headers={HDR_EMITTER: "src_b", HDR_EMITTER_KIND: "client"})

    outputs = {r.output for r in received}
    assert outputs == {"from_a", "from_b"}, f"expected both topics to fire; got {outputs}"


# ---------------------------------------------------------------------------
# Error handling — direct handler invocation to pin contracts
# ---------------------------------------------------------------------------


async def test_consume_fn_exception_swallowed_by_default_direct():
    """Direct-handler test: ``re_raise=False`` (default) — exception is
    logged and the handler returns normally (offset would commit)."""
    invocations: list[str] = []

    def boom(result: NodeResult) -> None:
        invocations.append("ran")
        raise RuntimeError("intentional consumer failure")

    node = ConsumerNodeDef(node_id="boom_sink", subscribe_topics="t", consume_fn=boom)

    envelope = _envelope_with_text("hi", correlation_id="cid-swallow-0")
    response = await node.handler(
        envelope,
        correlation_id="cid-swallow-0",
        headers={HDR_EMITTER: b"probe", HDR_EMITTER_KIND: b"client"},
        broker=MagicMock(),
    )

    assert invocations == ["ran"]
    # handler returns a Response; the test contract is "did not raise"
    assert response is not None


async def test_consume_fn_exception_re_raises_when_configured_direct():
    """Direct-handler test: ``re_raise=True`` — exception propagates out of
    the handler. This is the contract the previous broker-coupled test could
    not pin (TestKafkaBroker's propagation path is undefined)."""
    invocations: list[str] = []

    def boom(result: NodeResult) -> None:
        invocations.append("ran")
        raise RuntimeError("propagate me")

    node = ConsumerNodeDef(node_id="boom_sink", subscribe_topics="t", consume_fn=boom, re_raise=True)

    envelope = _envelope_with_text("hi", correlation_id="cid-raise-0")
    with pytest.raises(RuntimeError, match="propagate me"):
        await node.handler(
            envelope,
            correlation_id="cid-raise-0",
            headers={HDR_EMITTER: b"probe", HDR_EMITTER_KIND: b"client"},
            broker=MagicMock(),
        )

    assert invocations == ["ran"]


async def test_consume_fn_exception_swallow_emits_error_log(caplog):
    """Companion to the direct test above: verify the ERROR log carries the
    operational context (emitter, kind) we promised in the log message."""

    def boom(result: NodeResult) -> None:
        raise RuntimeError("absorb me")

    node = ConsumerNodeDef(node_id="boom_sink", subscribe_topics="t", consume_fn=boom)
    envelope = _envelope_with_text("hi", correlation_id="cid-swallow-log-0")

    with caplog.at_level(logging.ERROR, logger=CONSUMER_LOGGER):
        await node.handler(
            envelope,
            correlation_id="cid-swallow-log-0",
            headers={HDR_EMITTER: b"upstream_x", HDR_EMITTER_KIND: b"agent"},
            broker=MagicMock(),
        )

    err = [r for r in caplog.records if "consume_fn raised" in r.getMessage()]
    assert err, "swallowed exception must emit an ERROR log line"
    assert "emitter=upstream_x" in err[0].getMessage()
    assert "kind=agent" in err[0].getMessage()


async def test_cancelled_error_always_propagates():
    """Even with ``re_raise=False``, asyncio.CancelledError must propagate —
    swallowing cancellation breaks cooperative shutdown."""

    async def cancels(result: NodeResult) -> None:
        raise asyncio.CancelledError()

    node = ConsumerNodeDef(node_id="cancel_sink", subscribe_topics="t", consume_fn=cancels, re_raise=False)
    envelope = _envelope_with_text("hi", correlation_id="cid-cancel-0")

    with pytest.raises(asyncio.CancelledError):
        await node.handler(
            envelope,
            correlation_id="cid-cancel-0",
            headers={HDR_EMITTER: b"probe", HDR_EMITTER_KIND: b"client"},
            broker=MagicMock(),
        )


# ---------------------------------------------------------------------------
# Gates on the consumer
# ---------------------------------------------------------------------------


async def test_gate_rejection_skips_consume_fn(container):
    invocations: list[str] = []

    def reject(ctx: SessionRunContext) -> bool:
        return False

    @consumer(subscribe_topics="consumer_test_agent.output", gates=[reject])
    def sink(result: NodeResult) -> None:
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
    def sink(result: NodeResult) -> None:
        invocations.append("ran")

    agent = _wire_agent_with_consumer(container, sink)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        await client.execute_node("hi", agent.subscribe_topics[0], timeout=5)

    assert gate_calls, "gate must fire when consumer receives a message"
    assert invocations, "gate accepted → consume_fn must run"


async def test_final_only_gate_idiom_filters_intermediate(container):
    """Documented idiom: a gate keyed off ``final_output_parts`` filters out
    intermediate hops so the user fn only sees agent terminals."""
    received: list[NodeResult] = []

    def has_final_output(ctx: SessionRunContext) -> bool:
        return bool(ctx.state.final_output_parts)

    @consumer(subscribe_topics="filtered.topic", gates=[has_final_output])
    def sink(result: NodeResult) -> None:
        received.append(result)

    worker = container.get(Worker)
    worker.add_nodes(sink)
    prepare_worker(container)

    broker = container.get(KafkaBroker)

    async with TestKafkaBroker(broker):
        # Intermediate envelope (no final_output_parts) — gate rejects.
        await broker.publish(
            _envelope_without_final_parts("cid-int"),
            topic="filtered.topic",
            correlation_id="cid-int",
            headers={HDR_EMITTER: "upstream", HDR_EMITTER_KIND: "agent"},
        )
        # Terminal envelope — gate accepts.
        await broker.publish(
            _envelope_with_text("final!", "cid-final"),
            topic="filtered.topic",
            correlation_id="cid-final",
            headers={HDR_EMITTER: "upstream", HDR_EMITTER_KIND: "agent"},
        )

    assert len(received) == 1
    assert received[0].output == "final!"


# ---------------------------------------------------------------------------
# NodeResult.state — full session state visibility (the core "state on
# NodeResult" feature). These tests pin the contract: consumers and clients
# both see the same projection, with convenience properties reading through.
# ---------------------------------------------------------------------------


async def test_node_result_state_is_envelope_state_no_copy():
    """``result.state`` is the same instance as ``envelope.context.state``.
    This pins the "no deep-copy" decision: lifetime is the user fn's scope,
    not a defensive snapshot."""
    captured: list[NodeResult] = []

    def sink(result: NodeResult) -> None:
        captured.append(result)

    node = ConsumerNodeDef(node_id="state_id_check", subscribe_topics="t", consume_fn=sink)
    envelope = _envelope_with_text("hi", correlation_id="cid-state-id")

    await node.handler(
        envelope,
        correlation_id="cid-state-id",
        headers={HDR_EMITTER: b"upstream", HDR_EMITTER_KIND: b"agent"},
        broker=MagicMock(),
    )

    assert len(captured) == 1
    assert captured[0].state is envelope.context.state


async def test_node_result_convenience_properties_read_through_state():
    """``message_history``, ``output_parts``, and ``metadata`` are properties
    backed by ``state.*``. They must return the SAME objects, not copies."""
    captured: list[NodeResult] = []

    def sink(result: NodeResult) -> None:
        captured.append(result)

    node = ConsumerNodeDef(node_id="props_check", subscribe_topics="t", consume_fn=sink)

    state = State()
    state.final_output_parts = [TextPart(text="hello")]
    state.message_history = [ModelRequest.user_text_prompt("hi")]
    state.metadata = {"app": "demo"}
    envelope = Envelope(
        context=SessionRunContext(state=state, deps={}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
    )

    await node.handler(
        envelope,
        correlation_id="cid-prop",
        headers={HDR_EMITTER: b"upstream", HDR_EMITTER_KIND: b"agent"},
        broker=MagicMock(),
    )

    r = captured[0]
    assert r.output_parts is r.state.final_output_parts
    assert r.message_history is r.state.message_history
    assert r.metadata is r.state.metadata


async def test_node_result_is_frozen():
    """``NodeResult`` is a frozen dataclass; field reassignment must raise."""
    captured: list[NodeResult] = []

    def sink(result: NodeResult) -> None:
        captured.append(result)

    node = ConsumerNodeDef(node_id="frozen_check", subscribe_topics="t", consume_fn=sink)
    envelope = _envelope_with_text("hi", correlation_id="cid-frozen")
    await node.handler(envelope, correlation_id="cid-frozen", headers={HDR_EMITTER: b"x", HDR_EMITTER_KIND: b"agent"}, broker=MagicMock())

    r = captured[0]
    with pytest.raises(Exception):  # FrozenInstanceError
        r.state = State()  # type: ignore[misc]
    with pytest.raises(Exception):
        r.output = "different"  # type: ignore[misc]


async def test_consumer_sees_tool_results_via_state(container):
    """The motivating use case: a consumer wired to a tool's output topic
    (or any topic carrying a post-tool envelope) reads the tool's return
    value through ``result.state.tool_results`` — the gap NodeResult had
    before this change."""
    captured: list[NodeResult] = []

    @consumer(subscribe_topics="tool_results_obs.topic")
    def sink(result: NodeResult) -> None:
        captured.append(result)

    worker = container.get(Worker)
    worker.add_nodes(sink)
    prepare_worker(container)

    broker = container.get(KafkaBroker)

    # Hand-build a post-tool envelope: final_output_parts empty (tool hop),
    # but tool_calls + tool_results populated as the agent would have set them.
    state = State()
    tool_call = ToolCallPart(tool_name="get_weather", args={"location": "Tokyo"})
    state.add_tool_call(tool_call)
    state.add_tool_result(tool_call.tool_call_id, "sunny in Tokyo")
    envelope = Envelope(
        context=SessionRunContext(state=state, deps={}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
    )

    async with TestKafkaBroker(broker):
        await broker.publish(
            envelope,
            topic="tool_results_obs.topic",
            correlation_id="cid-tool",
            headers={HDR_EMITTER: "tool_get_weather", HDR_EMITTER_KIND: "tool"},
        )

    assert len(captured) == 1
    r = captured[0]
    assert r.output is None  # tool hop — no final_output_parts
    assert r.emitter_node_kind == "tool"
    assert tool_call.tool_call_id in r.state.tool_results
    assert r.state.tool_results[tool_call.tool_call_id] == "sunny in Tokyo"
    assert tool_call.tool_call_id in r.state.tool_calls
    assert r.state.tool_calls[tool_call.tool_call_id].tool_name == "get_weather"


async def test_client_execute_node_result_carries_state(container):
    """The client side gets the same state visibility. After
    ``client.execute_node()`` returns, the caller can inspect the agent's
    full session state — tool calls, tool results, overrides, etc."""
    worker = container.get(Worker)
    agent = Agent(
        "state_visibility_agent",
        system_prompt="x",
        subscribe_topics="state_visibility_agent.input",
        publish_topic="state_visibility_agent.output",
        model_client=_text_then_done(),
    )
    worker.add_nodes(agent)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.execute_node("hi there", agent.subscribe_topics[0], timeout=5)

    assert result.output == "done"
    # The State projection includes the full conversation, not just the deserialized output.
    assert isinstance(result.state, State)
    assert result.state is not None
    assert len(result.state.message_history) >= 1
    # final_output_parts should match the convenience property
    assert result.state.final_output_parts is result.output_parts
    # Tool dicts exist (empty, since this agent didn't call tools).
    assert isinstance(result.state.tool_calls, dict)
    assert isinstance(result.state.tool_results, dict)


# ---------------------------------------------------------------------------
# Hashability — NodeResult must declare itself non-hashable (Critical #1 from
# iteration 2 review). The dataclass-synthesized __hash__ would recursively
# hash unhashable fields (Pydantic State, lists) and raise at call time.
# ---------------------------------------------------------------------------


def test_node_result_is_not_hashable():
    """NodeResult must be explicitly non-hashable (__hash__ = None). Both
    static type checkers and runtime introspection should see it as
    unhashable so users get a clean error at type-check time / decoration
    time rather than a surprise TypeError on first hash() call."""
    state_ = State()
    state_.final_output_parts = [TextPart(text="x")]
    state_.message_history = [ModelRequest.user_text_prompt("hi")]
    result = NodeResult(output="x", state=state_, correlation_id="cid-hash")

    assert NodeResult.__hash__ is None
    with pytest.raises(TypeError, match="unhashable"):
        hash(result)
    with pytest.raises(TypeError, match="unhashable"):
        {result}  # set construction
    with pytest.raises(TypeError, match="unhashable"):
        {result: 1}  # dict key


# ---------------------------------------------------------------------------
# Strict-mode contract — `strict=True` (client default) must raise
# DeserializationError on empty `final_output_parts`. The only test coverage
# of the four strict-flag branches that wasn't pinned in iteration 2.
# ---------------------------------------------------------------------------


def test_strict_mode_raises_on_empty_final_output_parts():
    from calfkit.client.deserialize import deserialize_to_node_result
    from calfkit.exceptions import DeserializationError

    envelope = _envelope_without_final_parts("cid-strict-empty")
    with pytest.raises(DeserializationError, match="No DataPart or TextPart"):
        deserialize_to_node_result(envelope, correlation_id="cid-strict-empty")  # strict=True default


def test_lenient_mode_returns_none_on_empty_final_output_parts():
    """Companion to the strict test — pins the consumer-mode contract."""
    from calfkit.client.deserialize import deserialize_to_node_result

    envelope = _envelope_without_final_parts("cid-lenient-empty")
    result = deserialize_to_node_result(envelope, correlation_id="cid-lenient-empty", strict=False)
    assert result.output is None
    assert result.state is envelope.context.state


# ---------------------------------------------------------------------------
# deps on NodeResult (issue #144) — inbound producer deps are surfaced to
# consumers/clients as ``result.deps``, symmetric with ``ctx.deps`` in tools.
# ---------------------------------------------------------------------------


async def test_consumer_reads_inbound_deps():
    """A @consumer reads the deps the producer set via ``result.deps["key"]`` —
    the same data tools see via ``ctx.deps["key"]``."""
    captured: list[NodeResult] = []

    def sink(result: NodeResult) -> None:
        captured.append(result)

    node = ConsumerNodeDef(node_id="deps_sink", subscribe_topics="t", consume_fn=sink)
    envelope = _envelope_with_text("hi", correlation_id="cid-deps", deps={"discord": {"channel_id": 42}})

    await node.handler(
        envelope,
        correlation_id="cid-deps",
        headers={HDR_EMITTER: b"upstream", HDR_EMITTER_KIND: b"agent"},
        broker=MagicMock(),
    )

    assert len(captured) == 1
    assert captured[0].deps == {"discord": {"channel_id": 42}}
    assert captured[0].correlation_id == "cid-deps"


async def test_node_result_deps_is_envelope_deps_no_copy():
    """``result.deps`` is the same dict instance carried on the envelope (no
    defensive copy), mirroring the documented no-copy decision for ``state``."""
    captured: list[NodeResult] = []

    def sink(result: NodeResult) -> None:
        captured.append(result)

    node = ConsumerNodeDef(node_id="deps_nocopy", subscribe_topics="t", consume_fn=sink)
    envelope = _envelope_with_text("hi", correlation_id="cid-deps-id", deps={"k": "v"})

    await node.handler(
        envelope,
        correlation_id="cid-deps-id",
        headers={HDR_EMITTER: b"upstream", HDR_EMITTER_KIND: b"agent"},
        broker=MagicMock(),
    )

    assert captured[0].deps is envelope.context.deps


async def test_node_result_deps_defaults_to_empty_dict():
    """When no deps were provided, ``result.deps`` is an empty dict (never None)."""
    captured: list[NodeResult] = []

    def sink(result: NodeResult) -> None:
        captured.append(result)

    node = ConsumerNodeDef(node_id="deps_empty", subscribe_topics="t", consume_fn=sink)
    envelope = _envelope_with_text("hi", correlation_id="cid-deps-empty")  # no deps set

    await node.handler(
        envelope,
        correlation_id="cid-deps-empty",
        headers={HDR_EMITTER: b"upstream", HDR_EMITTER_KIND: b"agent"},
        broker=MagicMock(),
    )

    assert captured[0].deps == {}


async def test_deps_round_trip_through_agent_to_client_and_consumer(container):
    """E2E: deps passed to ``execute_node`` flow through the agent and surface on
    BOTH the client-returned ``NodeResult`` and the consumer's ``NodeResult`` —
    proving the producer-side propagation (``_publish_action``) reaches the
    consumer API boundary, which is the gap issue #144 closes."""
    received: list[NodeResult] = []

    @consumer(subscribe_topics="consumer_test_agent.output")
    def sink(result: NodeResult) -> None:
        received.append(result)

    agent = _wire_agent_with_consumer(container, sink)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.execute_node("hi", agent.subscribe_topics[0], deps={"discord": {"channel_id": 7}}, timeout=5)

    # Client side carries deps.
    assert result.deps == {"discord": {"channel_id": 7}}
    # Consumer side carries the same deps on every observed hop.
    assert received, "consumer saw no envelopes"
    assert all(r.deps == {"discord": {"channel_id": 7}} for r in received)


def test_correlation_id_raises_when_unstamped():
    """Reading ``correlation_id`` on a context no handler has stamped raises a
    clear ``RuntimeError`` — not an assert (which ``python -O`` would strip,
    leaking ``None`` into ``correlation_id[:8]`` as an opaque ``TypeError``)."""
    from calfkit.models import ToolContext

    ctx = SessionRunContext(state=State(), deps={})
    with pytest.raises(RuntimeError, match="correlation_id is unset"):
        _ = ctx.correlation_id

    tool_ctx = ToolContext(deps={})  # run_id defaults to None
    with pytest.raises(RuntimeError, match="without a run_id"):
        _ = tool_ctx.correlation_id


# ---------------------------------------------------------------------------
# Pre-built TypeAdapter — schema-generation errors surface at construction
# time, not per-envelope (Critical #3 from iteration 2 review).
# ---------------------------------------------------------------------------


def test_unschematizable_output_type_raises_at_construction():
    """An output_type that pydantic can't schematize must fail at decoration
    / instantiation, not silently per envelope at runtime."""

    class BadType:
        # No __init__/annotations and no pydantic config — TypeAdapter
        # rejects it. This is the kind of programmer bug iteration-2 review
        # flagged: previously, the consumer would log a stack trace on every
        # envelope forever.
        __slots__ = ("_blocker",)

    # Some pydantic versions accept arbitrary classes; this test asserts only
    # that IF schema generation fails, it fails loudly at construction.
    try:
        TypeAdapter(BadType)
    except Exception:
        # Verified the type IS unschematizable — assert the consumer surfaces
        # the same error at __init__ time.
        with pytest.raises(Exception):
            ConsumerNodeDef(
                node_id="bad_type_sink",
                subscribe_topics="t",
                consume_fn=lambda r: None,
                output_type=BadType,
            )
    else:
        pytest.skip("TypeAdapter(BadType) succeeded — can't exercise the failure path on this pydantic version")


def test_consumer_reuses_prebuilt_type_adapter(monkeypatch):
    """When a consumer is constructed with a concrete output_type, the
    TypeAdapter is built once at __init__ — not per envelope. Counts adapter
    constructions across multiple deserialize calls."""
    import sys

    import calfkit.client.deserialize as deserialize_mod

    # `calfkit.nodes.consumer` as a dotted path resolves to the decorator
    # function (re-exported via __init__) — go through sys.modules to hit the
    # module itself.
    consumer_mod = sys.modules["calfkit.nodes.consumer"]

    real_adapter = TypeAdapter
    construct_count = {"n": 0}

    class _CountingAdapter:
        def __init__(self, *args, **kwargs):
            construct_count["n"] += 1
            self._adapter = real_adapter(*args, **kwargs)

        def validate_python(self, *args, **kwargs):
            return self._adapter.validate_python(*args, **kwargs)

    monkeypatch.setattr(deserialize_mod, "TypeAdapter", _CountingAdapter)
    monkeypatch.setattr(consumer_mod, "TypeAdapter", _CountingAdapter)

    @consumer(subscribe_topics="prebuilt.topic", output_type=Report)
    def sink(result: NodeResult[Report]) -> None:
        return None

    # The decoration above constructed exactly one adapter.
    constructions_after_decoration = construct_count["n"]
    assert constructions_after_decoration == 1

    # Deserialize via the consumer's pre-built adapter several times.
    env = _envelope_with_data({"location": "Tokyo", "summary": "sunny"}, correlation_id="cid-reuse-1")
    for _ in range(3):
        asyncio.run(
            sink.handler(
                env,
                correlation_id="cid-reuse-1",
                headers={HDR_EMITTER: b"upstream", HDR_EMITTER_KIND: b"agent"},
                broker=MagicMock(),
            )
        )

    # No additional adapters constructed.
    assert construct_count["n"] == constructions_after_decoration


# ---------------------------------------------------------------------------
# Safety-net Exception catch in the deserialize block — anything that slips
# past the narrow (DeserializationError, ValidationError) tuple is logged
# and the envelope is skipped (no offset poison-pill).
# ---------------------------------------------------------------------------


async def test_unexpected_deserialize_exception_is_logged_and_skipped(monkeypatch, caplog):
    """Simulate an unexpected error inside `deserialize_to_node_result` (e.g.
    a pydantic edge case or third-party model adapter failure). The safety-
    net `except Exception` block must catch it, log, and skip — not propagate
    to FastStream and crash the consumer task."""
    invocations: list[str] = []

    @consumer(subscribe_topics="safety.topic")
    def sink(result: NodeResult) -> None:
        invocations.append("ran")

    # Patch the deserializer used by the consumer module to raise an unrelated
    # exception type that doesn't match the narrow catch tuple. Go via
    # sys.modules — the dotted-string path collides with the re-exported
    # `consumer` decorator function in calfkit.nodes.__init__.
    import sys

    consumer_mod = sys.modules["calfkit.nodes.consumer"]

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated unforeseen deserialize bug")

    monkeypatch.setattr(consumer_mod, "deserialize_to_node_result", _boom)

    envelope = _envelope_with_text("hi", correlation_id="cid-safety-0")
    with caplog.at_level(logging.ERROR, logger=CONSUMER_LOGGER):
        response = await sink.handler(
            envelope,
            correlation_id="cid-safety-0",
            headers={HDR_EMITTER: b"upstream", HDR_EMITTER_KIND: b"agent"},
            broker=MagicMock(),
        )

    assert invocations == []
    assert response is not None
    safety_records = [r for r in caplog.records if "unexpected deserialize error" in r.getMessage()]
    assert safety_records, "safety-net Exception catch must log at ERROR"
    assert safety_records[0].exc_info is not None
    assert isinstance(safety_records[0].exc_info[1], RuntimeError)


# ---------------------------------------------------------------------------
# Runtime generator detection — `_validate_consume_fn` rejects direct
# generator/asyncgen functions, but cannot detect callable-class generators
# or functions that return generator objects. The handler must catch these
# at the call site and fail loud rather than silently no-op.
# ---------------------------------------------------------------------------


async def test_callable_class_generator_detected_at_call_site():
    """A class whose ``__call__`` is a generator passes ``_validate_consume_fn``
    (instance is not a generator function), but produces a generator object
    when called — body never runs. Detect at the handler's call site."""

    class GenSink:
        def __call__(self, result):
            yield result

    invocations: list[str] = []

    def make_gen_sink():
        gs = GenSink()
        invocations.append("would-have-run")
        return gs

    node = ConsumerNodeDef(node_id="gen_sink", subscribe_topics="t", consume_fn=GenSink())
    envelope = _envelope_with_text("hi", correlation_id="cid-gen-0")

    # With re_raise=False (default), the handler logs and continues.
    response = await node.handler(
        envelope,
        correlation_id="cid-gen-0",
        headers={HDR_EMITTER: b"upstream", HDR_EMITTER_KIND: b"agent"},
        broker=MagicMock(),
    )
    assert response is not None

    # With re_raise=True, the TypeError propagates.
    node_raising = ConsumerNodeDef(node_id="gen_sink_raising", subscribe_topics="t", consume_fn=GenSink(), re_raise=True)
    with pytest.raises(TypeError, match="generator"):
        await node_raising.handler(
            envelope,
            correlation_id="cid-gen-1",
            headers={HDR_EMITTER: b"upstream", HDR_EMITTER_KIND: b"agent"},
            broker=MagicMock(),
        )


async def test_function_returning_generator_object_detected_at_call_site():
    """A plain function that explicitly returns a generator object also slips
    past `_validate_consume_fn` — detect at the call site."""

    def _wrapper_gen():
        yield 1

    def sink(result):
        return _wrapper_gen()

    node = ConsumerNodeDef(node_id="gen_ret", subscribe_topics="t", consume_fn=sink, re_raise=True)
    envelope = _envelope_with_text("hi", correlation_id="cid-gen-ret")

    with pytest.raises(TypeError, match="generator"):
        await node.handler(
            envelope,
            correlation_id="cid-gen-ret",
            headers={HDR_EMITTER: b"upstream", HDR_EMITTER_KIND: b"agent"},
            broker=MagicMock(),
        )


async def test_function_returning_async_generator_object_detected_at_call_site():
    """Symmetric to the sync generator test — async-generator objects also
    fail `inspect.isawaitable` and must be caught at the call site."""

    async def _wrapper_agen():
        yield 1

    def sink(result):
        return _wrapper_agen()

    node = ConsumerNodeDef(node_id="agen_ret", subscribe_topics="t", consume_fn=sink, re_raise=True)
    envelope = _envelope_with_text("hi", correlation_id="cid-agen-ret")

    with pytest.raises(TypeError, match="async_generator"):
        await node.handler(
            envelope,
            correlation_id="cid-agen-ret",
            headers={HDR_EMITTER: b"upstream", HDR_EMITTER_KIND: b"agent"},
            broker=MagicMock(),
        )
