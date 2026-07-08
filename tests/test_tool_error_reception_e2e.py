"""End-to-end tests for agent tool-error reception through ``TestKafkaBroker`` (Phase 5, offline lane).

Drive a REAL round trip (agent → faulting tool → fault rail → the agent's ``on_tool_error`` seam) and
assert what the MODEL sees on its next turn: a converted fault materializes as a ``RetryPromptPart``
(``is_error=True``) carrying the level-A text, a fallback value as a ``ToolReturnPart`` (``is_error=
False``), and a declined / unhandled fault escalates (the run faults; the model sees nothing). The
``message_agent`` and real-ktables-store cases live on the kafka lane
(``tests/integration/test_tool_error_reception_e2e_kafka.py``)."""

from __future__ import annotations

import contextlib
from typing import Any

import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, RetryPromptPart, TextPart, ToolCallPart, ToolReturnPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client
from calfkit.exceptions import NodeFaultError
from calfkit.models.payload import retry_text_part
from calfkit.nodes import Agent, agent_tool
from calfkit.nodes._tool_error import surface_to_model
from calfkit.worker import Worker
from tests.providers import prepare_worker


@agent_tool
def kaboom(x: int) -> int:
    """Raises → the chokepoint synthesizes ``calf.exception`` (``exception.type='ValueError'``)."""
    raise ValueError(f"kaboom({x})")


@agent_tool
def ok_tool(x: int) -> str:
    """A fault-free sibling for mixed fan-out batches."""
    return f"ok({x})"


def _react_model(tool_calls: list[ToolCallPart], capture: dict[str, list[str]]) -> FunctionModel:
    """Emit ``tool_calls`` on turn 1; on the next turn, record what materialized into the conversation
    (``RetryPromptPart`` content under ``retries``, ``ToolReturnPart`` content under ``returns``) and
    finalize echoing it — so a test observes exactly what the model was shown."""

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        if isinstance(last, ModelRequest):
            retries = [p for p in last.parts if isinstance(p, RetryPromptPart)]
            returns = [p for p in last.parts if isinstance(p, ToolReturnPart)]
            if retries or returns:
                capture["retries"] = [str(p.content) for p in retries]
                capture["returns"] = [str(p.content) for p in returns]
                return ModelResponse(parts=[TextPart(f"RESULT[{' | '.join(capture['retries'] + capture['returns'])}]")])
        return ModelResponse(parts=list(tool_calls))

    return FunctionModel(_fn)


async def _run(container: Any, agent: Agent[str], *tools: Any, message: str = "go") -> Any:
    """Deploy ``agent`` + ``tools`` and drive one execute() round trip, tolerating an escalated fault."""
    worker = container.get(Worker)
    worker.add_nodes(agent, *tools)
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)
    async with TestKafkaBroker(broker):
        return await client.agent(topic=agent.subscribe_topics[0]).execute(message, timeout=10)


def _agent(name: str, model: FunctionModel, *, tools: list[Any], sequential: bool = True, **seams: Any) -> Agent[str]:
    return Agent(name, subscribe_topics=f"{name}.in", model_client=model, tools=tools, sequential_only_mode=sequential, **seams)


# ── conversion: the model sees the level-A is_error result ─────────────────────


async def test_surface_to_model_converts_a_single_tool_fault(container) -> None:
    capture: dict[str, list[str]] = {}
    agent = _agent("s1", _react_model([ToolCallPart("kaboom", {"x": 7})], capture), tools=[kaboom], on_tool_error=[surface_to_model()])
    result = await _run(container, agent, kaboom)
    assert result.output is not None and "ValueError: kaboom(7)" in result.output
    assert capture["retries"] == ["ValueError: kaboom(7)"]  # materialized as a RetryPromptPart (is_error=True)
    assert capture["returns"] == []


async def test_custom_handler_branches_on_tool_name_and_converts(container) -> None:
    capture: dict[str, list[str]] = {}

    def on_error(tool_call: Any, ctx: Any, report: Any) -> Any:
        if tool_call.tool_name == "kaboom":
            return retry_text_part(f"custom: {report.message}")
        return None

    agent = _agent("s2", _react_model([ToolCallPart("kaboom", {"x": 3})], capture), tools=[kaboom], on_tool_error=on_error)
    await _run(container, agent, kaboom)
    assert capture["retries"] == ["custom: kaboom(3)"]  # the custom handler's marked text, is_error=True


async def test_fallback_success_returns_is_error_false(container) -> None:
    # A plain value is a genuine fallback SUCCESS — materializes as a ToolReturn (is_error=False), NOT a
    # retry. Auto-marking every return would destroy this case (D7).
    capture: dict[str, list[str]] = {}

    def on_error(tool_call: Any, ctx: Any, report: Any) -> Any:
        return "cached-value"

    agent = _agent("s3", _react_model([ToolCallPart("kaboom", {"x": 1})], capture), tools=[kaboom], on_tool_error=on_error)
    await _run(container, agent, kaboom)
    assert capture["returns"] == ["cached-value"]  # ToolReturn (is_error=False)
    assert capture["retries"] == []


async def test_empty_list_substitute_becomes_tool_return_none(container) -> None:
    # `[]` coerces to empty parts → a ToolReturn(return_value=None) (is_error=False), not a retry.
    capture: dict[str, list[str]] = {}

    def on_error(tool_call: Any, ctx: Any, report: Any) -> Any:
        return []

    agent = _agent("s4", _react_model([ToolCallPart("kaboom", {"x": 1})], capture), tools=[kaboom], on_tool_error=on_error)
    await _run(container, agent, kaboom)
    assert capture["returns"] == ["None"]  # ToolReturn(return_value=None)
    assert capture["retries"] == []


async def test_chain_runtime_order_first_non_none_wins(container) -> None:
    capture: dict[str, list[str]] = {}

    def first(tool_call: Any, ctx: Any, report: Any) -> Any:
        return retry_text_part("FIRST")

    def second(tool_call: Any, ctx: Any, report: Any) -> Any:
        return retry_text_part("SECOND")

    agent = _agent("s5", _react_model([ToolCallPart("kaboom", {"x": 1})], capture), tools=[kaboom], on_tool_error=[first, second])
    await _run(container, agent, kaboom)
    assert capture["retries"] == ["FIRST"]  # first-non-None short-circuits the chain


async def test_same_tool_twice_distinct_tags_both_convert(container) -> None:
    # A parallel batch of the SAME tool (distinct tags) → surface_to_model converts each independently.
    capture: dict[str, list[str]] = {}
    calls = [ToolCallPart("kaboom", {"x": 1}, tool_call_id="t1"), ToolCallPart("kaboom", {"x": 2}, tool_call_id="t2")]
    agent = _agent("s6", _react_model(calls, capture), tools=[kaboom], sequential=False, on_tool_error=[surface_to_model()])
    await _run(container, agent, kaboom)
    assert sorted(capture["retries"]) == ["ValueError: kaboom(1)", "ValueError: kaboom(2)"]


async def test_fanout_sibling_fault_converts(container) -> None:
    # A faulting tool in a mixed parallel batch: surface_to_model converts the faulting sibling; the
    # successful sibling folds normally. Both results reach the model, the run finalizes.
    capture: dict[str, list[str]] = {}
    calls = [ToolCallPart("kaboom", {"x": 9}, tool_call_id="t1"), ToolCallPart("ok_tool", {"x": 5}, tool_call_id="t2")]
    agent = _agent("s7", _react_model(calls, capture), tools=[kaboom, ok_tool], sequential=False, on_tool_error=[surface_to_model()])
    await _run(container, agent, kaboom, ok_tool)
    assert capture["retries"] == ["ValueError: kaboom(9)"]  # the faulting sibling converted
    assert capture["returns"] == ["ok(5)"]  # the successful sibling folded normally


# ── escalation: a declined / unhandled fault still faults the run ──────────────


async def test_no_handler_escalates_default_regression(container) -> None:
    # The default is escalate: with no on_tool_error, a tool fault faults the whole run (the model sees
    # nothing). Guards that the feature is strictly opt-in.
    capture: dict[str, list[str]] = {}
    agent = _agent("e1", _react_model([ToolCallPart("kaboom", {"x": 7})], capture), tools=[kaboom])
    with pytest.raises(NodeFaultError):
        await _run(container, agent, kaboom)
    assert capture == {}  # the model was never re-invoked


async def test_declining_handler_escalates(container) -> None:
    capture: dict[str, list[str]] = {}

    def declines(tool_call: Any, ctx: Any, report: Any) -> Any:
        return None  # decline → the fault continues escalating

    agent = _agent("e2", _react_model([ToolCallPart("kaboom", {"x": 7})], capture), tools=[kaboom], on_tool_error=declines)
    with pytest.raises(NodeFaultError):
        await _run(container, agent, kaboom)
    assert capture == {}


async def test_handler_accidental_raise_escalates_not_converts(container) -> None:
    # An accidental raise inside an on_tool_error handler is SLOT-scoped (§6.5): the slot fails
    # (calf.exception, the callee fault chained), the run escalates — the wrapper does NOT swallow it.
    capture: dict[str, list[str]] = {}

    def boom_handler(tool_call: Any, ctx: Any, report: Any) -> Any:
        raise RuntimeError("handler boom")

    agent = _agent("e3", _react_model([ToolCallPart("kaboom", {"x": 7})], capture), tools=[kaboom], on_tool_error=boom_handler)
    with contextlib.suppress(NodeFaultError):
        await _run(container, agent, kaboom)
    assert capture == {}  # never converted


async def test_handler_node_fault_error_mints_verbatim(container) -> None:
    # A handler raising NodeFaultError is the per-slot mint gesture (§6.5): the minted fault converts
    # VERBATIM (its error_type), not downgraded to calf.exception — inherited through the on_tool_error surface.
    def mint(tool_call: Any, ctx: Any, report: Any) -> Any:
        raise NodeFaultError("billing.quota_exceeded", message="minted in the handler")

    agent = _agent("e5", _react_model([ToolCallPart("kaboom", {"x": 7})], {}), tools=[kaboom], on_tool_error=mint)
    with pytest.raises(NodeFaultError) as exc_info:
        await _run(container, agent, kaboom)
    assert exc_info.value.report.find("billing.quota_exceeded") is not None  # minted verbatim


async def test_d12_one_unhandled_sibling_escalates_the_whole_batch(container) -> None:
    # D12 (all-or-nothing): a parallel batch where one faulting sibling is CONVERTED and another is
    # DECLINED. Any unhandled sibling at closure escalates the whole batch as a fault group — the handled
    # conversion is DISCARDED. surface_to_model rescues a batch only by handling EVERY faulting sibling.
    capture: dict[str, list[str]] = {}
    calls = [ToolCallPart("kaboom", {"x": 1}, tool_call_id="t1"), ToolCallPart("kaboom", {"x": 2}, tool_call_id="t2")]

    def selective(tool_call: Any, ctx: Any, report: Any) -> Any:
        if (tool_call.args or {}).get("x") == 1:
            return retry_text_part("converted-1")  # convert the x=1 sibling
        return None  # decline the x=2 sibling → unhandled → the batch escalates

    agent = _agent("e6", _react_model(calls, capture), tools=[kaboom], sequential=False, on_tool_error=selective)
    with pytest.raises(NodeFaultError):
        await _run(container, agent, kaboom)
    assert capture == {}  # the converted sibling was discarded — the batch escalated as a fault group


async def test_wire_unsafe_substitute_becomes_materialization_failed(container) -> None:
    # A handler returning a non-wire-serializable value fails the slot deterministically as
    # calf.slot.materialization_failed (§6.9) — the run escalates, it never hangs mid-publish.
    class _NotSerializable:
        pass

    def on_error(tool_call: Any, ctx: Any, report: Any) -> Any:
        return _NotSerializable()  # wire-unsafe substitute

    agent = _agent("e7", _react_model([ToolCallPart("kaboom", {"x": 7})], {}), tools=[kaboom], on_tool_error=on_error)
    with pytest.raises(NodeFaultError) as exc_info:
        await _run(container, agent, kaboom)
    assert exc_info.value.report.find("calf.slot.materialization_failed") is not None


# ── M2: fold-time ctx.state mutation is discarded for a fan-out sibling (rail §7.6) ──


def _marker_scan_model(tool_calls: list[ToolCallPart], capture: dict[str, bool]) -> FunctionModel:
    """Emit ``tool_calls`` on turn 1; once a result materializes, record whether the ``M2_MARKER`` a
    handler staged onto ``ctx.state`` survived into the model's history, then finalize."""

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        if isinstance(last, ModelRequest) and any(isinstance(p, (RetryPromptPart, ToolReturnPart)) for p in last.parts):
            capture["saw_marker"] = any("M2_MARKER" in str(m) for m in messages)
            return ModelResponse(parts=[TextPart("done")])
        return ModelResponse(parts=list(tool_calls))

    return FunctionModel(_fn)


def _mutate_and_convert(tool_call: Any, ctx: Any, report: Any) -> Any:
    ctx.state.stage_message(ModelRequest.user_text_prompt("M2_MARKER"))  # a fold-time state mutation
    return retry_text_part("converted")


async def test_m2_fanout_sibling_state_mutation_is_discarded(container) -> None:
    # M2 (rail §7.6): a handler mutating ctx.state on a FAN-OUT sibling seam lands on a throwaway copy —
    # close restores the OPEN snapshot, so the mutation is DISCARDED. A developer must not rely on a
    # fold-time state mutation surviving a fan-out.
    capture: dict[str, bool] = {}
    calls = [ToolCallPart("kaboom", {"x": 1}, tool_call_id="t1"), ToolCallPart("kaboom", {"x": 2}, tool_call_id="t2")]
    agent = _agent("m2f", _marker_scan_model(calls, capture), tools=[kaboom], sequential=False, on_tool_error=_mutate_and_convert)
    await _run(container, agent, kaboom)
    assert capture["saw_marker"] is False  # discarded at close


async def test_m2_single_call_state_mutation_persists(container) -> None:
    # The contrast: a single-call fault has no snapshot/restore, so the same fold-time mutation PERSISTS.
    capture: dict[str, bool] = {}
    agent = _agent("m2s", _marker_scan_model([ToolCallPart("kaboom", {"x": 1})], capture), tools=[kaboom], on_tool_error=_mutate_and_convert)
    await _run(container, agent, kaboom)
    assert capture["saw_marker"] is True  # persists (no snapshot/restore for a single call)
