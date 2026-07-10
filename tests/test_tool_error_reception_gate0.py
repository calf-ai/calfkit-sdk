"""Phase 0 de-risk gates for the agent tool-error reception feature (offline lane).

These are **characterization probes** of *existing* framework behaviour, run before any
feature code is written, to prove the provenance assumptions the design (spec D3,
state-first resolution) depends on:

* **Gate A** — a single tool fault folds back to the agent's ``on_callee_error``
  seam with ``ctx.state.tool_calls[tag]`` populated as the *full* ``ToolCallPart`` (with ``.args``).
* **Gate B1** — a normal fan-out sibling fault likewise resolves via ``state.tool_calls[tag]``
  **with ``.args`` intact** (the deep-copied sibling state is mirrored back unchanged).
**Gate B2** — a ``message_agent`` peer fault (empty state + populated ``target_topic``) is the single
empty-state arm and runs on the **kafka lane** (``tests/integration/test_tool_error_reception_gate0_kafka.py``):
``message_agent`` gates dispatch on the target being in the live ``calf.agents`` view, which the real
control plane populates naturally but the offline ``TestKafkaBroker`` does not — so B2 is authoritative
on the real broker (the handoff also flags the kafka lane as load-bearing for message_agent fold-path
state provenance).

The probe (:class:`_ToolCallProbe`) is a raw ``on_callee_error`` recorder that captures the live
seam view, then **declines** (returns ``None``) so the fault still escalates exactly as if the seam
were absent — capture-in-callback, assert-in-test-body (a raise inside the seam would itself become
a node-own fault). Gate C (the coercion mixed-list trap) lives with its sibling coercion tests in
``tests/test_reply_slot.py::TestCoerceToParts``.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from typing import Any

from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.exceptions import NodeFaultError
from calfkit.models.error_report import ErrorReport
from calfkit.models.seam_context import SeamContext
from calfkit.nodes import Agent, agent_tool
from calfkit.worker import Worker
from tests.providers import prepare_worker


@agent_tool
def boom_tool(x: int) -> int:
    """A tool whose body raises → synthesized as ``calf.exception`` at the chokepoint."""
    raise ValueError(f"kaboom({x})")


@agent_tool
def ok_tool() -> str:
    """A fault-free sibling, for the mixed fan-out batch (gate B1)."""
    return "ok_result"


@dataclass
class _ToolCallProbe:
    """A raw ``on_callee_error`` recorder capturing the seam's view of the failing call.

    Records, per firing: the delivery kind, the failing call's ``tag`` / ``target_topic``, the
    tags present in ``state.tool_calls`` at fold time, and whether the state lookup resolves the
    failing tag to a full ``ToolCallPart`` (with ``.args``). Then declines so the fault escalates.
    """

    calls: list[dict[str, Any]] = field(default_factory=list)

    def __call__(self, ctx: SeamContext[Any], fault: ErrorReport) -> None:
        failing = ctx.failing_call
        tag = failing.tag if failing is not None else None
        tool_calls = ctx.state.tool_calls
        resolved = tool_calls.get(tag) if tag is not None else None
        self.calls.append(
            {
                "delivery_kind": ctx.delivery_kind,
                "error_type": fault.error_type,
                "tag": tag,
                "target_topic": failing.target_topic if failing is not None else None,
                "state_tool_call_tags": sorted(tool_calls.keys()),
                "resolved_present": resolved is not None,
                "resolved_has_args": resolved is not None and getattr(resolved, "args", None) is not None,
                "resolved_tool_name": getattr(resolved, "tool_name", None) if resolved is not None else None,
            }
        )
        return None


async def test_gate_a_single_tool_fault_resolves_via_state_with_args(container) -> None:
    # Gate A: a single tool fault folds to on_callee_error with the caller's State
    # carried back on the reply, so state.tool_calls[tag] resolves the FULL ToolCallPart (with
    # .args). This is the state-first happy path the resolver will reuse (spec D3).
    recorder = _ToolCallProbe()
    worker = container.get(Worker)
    agent = Agent(
        "gatea_agent",
        system_prompt="call the boom_tool tool",
        subscribe_topics="gatea_agent.input",
        model_client=_scripted([ToolCallPart("boom_tool", {"x": 7}, tool_call_id="c1")]),
        tools=[boom_tool],
        on_callee_error=recorder,
    )
    worker.add_nodes(agent, boom_tool)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        with contextlib.suppress(NodeFaultError):
            await client.agent(topic="gatea_agent.input").execute("go", timeout=10)

    assert len(recorder.calls) == 1
    (call,) = recorder.calls
    assert call["delivery_kind"] == "fault"
    assert call["tag"] == "c1"
    assert call["resolved_present"] is True  # state carried back → state-first lookup hits
    assert call["resolved_has_args"] is True  # the FULL ToolCallPart, with .args (grounds D3 state-first)
    assert call["resolved_tool_name"] == "boom_tool"


async def test_gate_b1_fanout_sibling_fault_resolves_via_state_with_args(container) -> None:
    # Gate B1: a normal (non-message_agent) fan-out sibling fault also folds with the caller's
    # State — the deep-copied sibling state is mirrored back unchanged — so state.tool_calls[tag]
    # resolves the FULL ToolCallPart WITH .args. This grounds state-first for parallel tools (the
    # slot-first plan bug caught in review round 2 would strip .args here).
    recorder = _ToolCallProbe()
    worker = container.get(Worker)
    agent = Agent(
        "gateb1_agent",
        system_prompt="call the tools",
        subscribe_topics="gateb1_agent.input",
        model_client=_scripted(
            [
                ToolCallPart("boom_tool", {"x": 7}, tool_call_id="c1"),
                ToolCallPart("ok_tool", {}, tool_call_id="c2"),
            ]
        ),
        tools=[boom_tool, ok_tool],
        on_callee_error=recorder,
    )
    worker.add_nodes(agent, boom_tool, ok_tool)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        with contextlib.suppress(NodeFaultError):
            await client.agent(topic="gateb1_agent.input").execute("go", timeout=10)

    # The boom sibling faulted; its fold fired on_callee_error with the caller's State carried back.
    boom_firings = [c for c in recorder.calls if c["tag"] == "c1"]
    assert len(boom_firings) == 1, f"expected one fault firing for the boom sibling, got {recorder.calls}"
    (call,) = boom_firings
    assert call["delivery_kind"] == "fault"
    assert call["resolved_present"] is True  # fan-out sibling state mirrored back → state-first hits
    assert call["resolved_has_args"] is True  # .args intact for the parallel tool (grounds D3 state-first)
    assert call["resolved_tool_name"] == "boom_tool"


# ── the offline model driver (mirrors tests/integration/_roundtrip_helpers.scripted_model) ──


def _scripted(tool_calls: list[ToolCallPart]):
    from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, RetryPromptPart, TextPart, ToolReturnPart
    from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        if isinstance(last, ModelRequest) and any(isinstance(p, (ToolReturnPart, RetryPromptPart)) for p in last.parts):
            return ModelResponse(parts=[TextPart("done")])
        return ModelResponse(parts=list(tool_calls))

    return FunctionModel(_fn)
