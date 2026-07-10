"""Kernel-minted pair law + hop-exit flush invariants (caller-side step-emission spec §3.2/§3.4, I4/I6/I2/I12).

Offline E2E through the real kernel: container + ``prepare_worker`` + ``TestKafkaBroker`` +
``FunctionModel`` + ``handle.stream()`` (the ``test_step_emission_integration`` harness pattern).
These pin the KERNEL's emission — fold mints from the echoed marker at the two stage-1 sites,
``note_dispatch`` at the chokepoint, and the uniform hop-exit flush (every exit, fault exits
included) — independent of the body-fact rewrite (F2):

- **Pair law (I4)**, both directions, single + fan-out: every marked dispatch has its call step in
  that hop's flush; every marked reply fold has its result step in its hop's flush.
- **Topology symmetry (L18b/L18i)**: the same unhandled tool fault as a single call and as a
  fan-out sibling produces the same one-event ``failed`` closure (``parts=[]``), then ``RunFailed``.
- **Hop-fate independence (L18i)**: a resolved fold whose hop's body then raises keeps its
  ``success`` result step — published by the fault-exit flush before the fault.
- **I6 at-most-one** on the flush-then-failed-action-publish hop (drain, not luck).
- **I2 no-residue**, single-run oracle: flush replaced by a recording no-op spy — the expected
  mints were captured, the run completes, and the terminal envelope carries no step-derived
  residue, field-level.
- **Identity (I12/§5.2)**: emitter = the folding caller; depth = the fold hop's inbound snapshot;
  parked-fold ``frame_id`` = the batch frame id, shared across the batch's folds.
"""

from __future__ import annotations

import contextlib
from typing import Any

from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._protocol import HDR_KIND, HDR_WIRE
from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client
from calfkit.exceptions import NodeFaultError
from calfkit.models.step import StepMessage
from calfkit.models.tool_context import ToolContext
from calfkit.nodes import Agent, agent_tool
from calfkit.nodes._steps import HopStepLedger
from calfkit.worker import Worker
from tests.providers import prepare_worker


@agent_tool
def pair_echo(ctx: ToolContext) -> str:
    return "pair-echo-ok"


@agent_tool
def pair_ok(ctx: ToolContext) -> str:
    return "pair-ok"


@agent_tool
def pair_boom(ctx: ToolContext) -> str:
    raise ValueError("pair kaboom")


def _react(tool_calls: list[ToolCallPart], *, raise_on_reaction: bool = False) -> FunctionModel:
    """Emit ``tool_calls`` on turn 1; on the reaction turn either finalize or raise (the
    hop-fate-independence driver: the fold resolved, then the body dies)."""

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        if isinstance(last, ModelRequest):
            returns = [p for p in last.parts if isinstance(p, ToolReturnPart)]
            if returns or any(type(p).__name__ == "RetryPromptPart" for p in last.parts):
                if raise_on_reaction:
                    raise RuntimeError("body dies after the fold")
                return ModelResponse(parts=[TextPart(f"final: {' | '.join(str(r.content) for r in returns)}")])
        return ModelResponse(parts=[TextPart("let me check"), *tool_calls])

    return FunctionModel(_fn)


async def _run_streaming(container: Any, agent: Agent, *tools: Any) -> list[Any]:
    """Deploy + drive one run, returning the full event stream (terminal last); tolerates a fault."""
    worker = container.get(Worker)
    worker.add_nodes(agent, *tools)
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)
    async with TestKafkaBroker(broker):
        handle = await client.agent(topic=agent.subscribe_topics[0]).start("go")
        return [e async for e in handle.stream()]


def _agent(name: str, model: FunctionModel, *, tools: list[Any]) -> Agent:
    return Agent(name, system_prompt="x", subscribe_topics=f"{name}.in", model_client=model, tools=tools)


def _calls(events: list[Any]) -> list[Any]:
    return [e for e in events if type(e).__name__ == "ToolCallEvent"]


def _results(events: list[Any]) -> list[Any]:
    return [e for e in events if type(e).__name__ == "ToolResultEvent"]


# --------------------------------------------------------------------------- #
# Pair law (I4) — both directions, single + fan-out                            #
# --------------------------------------------------------------------------- #


async def test_pair_law_single_call(container) -> None:
    agent = _agent("pl_single", _react([ToolCallPart("pair_echo", {}, tool_call_id="c1")]), tools=[pair_echo])
    events = await _run_streaming(container, agent, pair_echo)
    assert type(events[-1]).__name__ == "RunCompleted"
    (call,) = _calls(events)
    (result,) = _results(events)
    # Every marked dispatch has its call step; every marked fold has its result step — paired by id.
    assert call.tool_call_id == result.tool_call_id == "c1"
    assert call.name == result.name == "pair_echo"
    assert call.args == {}  # marker-sourced: the parsed dict (spec §5.2)
    assert result.outcome == "success"
    assert "pair-echo-ok" in result.parts[0].text  # the resolved slot parts, verbatim (I10)
    # Identity (I12): the result is minted by the FOLDING CALLER at its fold hop's inbound depth.
    assert result.emitter == "pl_single"
    assert result.depth == 1
    # The fold hop's result precedes the terminal on the wire at depth 1 (I11).
    assert events.index(result) < len(events) - 1


async def test_pair_law_fanout_both_directions_at_the_flush(container, monkeypatch) -> None:
    # Flush-level oracle (spec §12's phrasing: a step "in that hop's FLUSH"): the offline
    # TestKafkaBroker executes the re-entry subtree INLINE inside _publish_reentry, so the
    # COMPLETING fold's trickle publishes after the terminal and is dropped client-side (an
    # accepted reorder; the client-observable trickle is the kafka lane's pin). The spy records
    # every flush's events + identity kwargs and delegates to the real flush.
    flushes: list[dict[str, Any]] = []
    real_flush = HopStepLedger.flush

    async def spy(self: HopStepLedger, broker: Any, **kwargs: Any) -> None:
        if self._events:
            flushes.append({"events": list(self._events), **kwargs})
        await real_flush(self, broker, **kwargs)

    monkeypatch.setattr(HopStepLedger, "flush", spy)
    calls = [ToolCallPart("pair_echo", {}, tool_call_id="c1"), ToolCallPart("pair_ok", {}, tool_call_id="c2")]
    agent = _agent("pl_fan", _react(calls), tools=[pair_echo, pair_ok])
    events = await _run_streaming(container, agent, pair_echo, pair_ok)
    assert type(events[-1]).__name__ == "RunCompleted"

    # Direction 1: every marked dispatch has its call step in the DISPATCH hop's flush.
    (dispatch,) = [f for f in flushes if any(type(e).__name__ == "ToolCallStep" for e in f["events"])]
    assert [e.tool_call_id for e in dispatch["events"] if type(e).__name__ == "ToolCallStep"] == ["c1", "c2"]
    # Direction 2: every marked reply fold has its result step in ITS OWN (parked) hop's flush —
    # one one-event flush per sibling fold (the trickle, I5), outcome derived per fold.
    folds = [f for f in flushes if any(type(e).__name__ == "ToolResultStep" for e in f["events"])]
    assert {f["events"][0].tool_call_id for f in folds} == {"c1", "c2"}
    assert all(len(f["events"]) == 1 and f["events"][0].outcome == "success" for f in folds)
    # Identity anchors (I12/§5.2): emitter = the folding caller at its inbound snapshot depth; the
    # parked-fold frame_id is the BATCH frame id — identical across the batch's folds.
    assert all(f["emitter"] == "pl_fan" for f in folds)
    assert all(f["depth"] == 1 for f in folds)  # the fold hop's inbound depth (a depth-1 caller)
    assert len({f["frame_id"] for f in folds}) == 1


# --------------------------------------------------------------------------- #
# Topology symmetry + hop-fate independence (L18b / L18i)                      #
# --------------------------------------------------------------------------- #


def _assert_boom_closure(closure: Any, emitter: str) -> None:
    """The ONE canonical failed closure both topologies must produce (I10: parts=[], no report;
    emitter = the folding caller). Shared by the two topology tests below — the symmetry pin."""
    assert closure.outcome == "failed"
    assert closure.parts == []
    assert closure.name == "pair_boom"
    assert closure.tool_call_id == "b1"
    assert closure.emitter == emitter


async def test_topology_symmetry_single_call_failed_closure(container) -> None:
    single = _agent("ts_single", _react([ToolCallPart("pair_boom", {}, tool_call_id="b1")]), tools=[pair_boom])
    events = await _run_streaming(container, single, pair_boom)
    assert type(events[-1]).__name__ == "RunFailed"
    (closure,) = _results(events)
    _assert_boom_closure(closure, "ts_single")
    assert events.index(closure) < len(events) - 1  # the failed closure precedes the escalation's terminal


async def test_topology_symmetry_fanout_sibling_failed_closure(container) -> None:
    # The same fault as a fan-out sibling (boom dispatched FIRST, so its fold is the parked —
    # offline-observable — trickle; the completing ok-fold's trickle lands post-terminal in the
    # inline offline broker, an accepted reorder). Identical one-event closure to the single arm.
    calls = [ToolCallPart("pair_boom", {}, tool_call_id="b1"), ToolCallPart("pair_ok", {}, tool_call_id="c2")]
    fan = _agent("ts_fan", _react(calls), tools=[pair_boom, pair_ok])
    events = await _run_streaming(container, fan, pair_boom, pair_ok)
    assert type(events[-1]).__name__ == "RunFailed"
    (closure,) = [r for r in _results(events) if r.tool_call_id == "b1"]
    _assert_boom_closure(closure, "ts_fan")


async def test_hop_fate_independence_resolved_fold_survives_body_raise(container) -> None:
    # The fold resolved (success), then the SAME hop's body raises: the hop-exit flush publishes the
    # result step before the fault — the call's completed lifecycle is never censored (L18i).
    agent = _agent("hf_agent", _react([ToolCallPart("pair_echo", {}, tool_call_id="c1")], raise_on_reaction=True), tools=[pair_echo])
    events = await _run_streaming(container, agent, pair_echo)
    assert type(events[-1]).__name__ == "RunFailed"
    (result,) = _results(events)
    assert (result.tool_call_id, result.outcome) == ("c1", "success")
    assert "pair-echo-ok" in result.parts[0].text
    # Minted by the FOLDING CALLER on its own (later-faulting) hop — not by the callee (v1).
    assert result.emitter == "hf_agent"
    assert result.depth == 1
    assert events.index(result) < len(events) - 1  # the result step precedes the terminal


# --------------------------------------------------------------------------- #
# I6 — at most one StepMessage per hop, held by drain                          #
# --------------------------------------------------------------------------- #


async def test_i6_flush_then_failed_action_publish_publishes_exactly_one_step_message(container, monkeypatch) -> None:
    # The accepted edge (spec §3.4): flush precedes the action publish; when the action publish then
    # fails, the hop has already emitted — and its fault exit re-invokes flush on a DRAINED ledger,
    # so exactly one StepMessage still publishes (at-most-one by drain, not luck). Failure injection:
    # the broker raises EXACTLY ONCE, on the first envelope-wire publish following a step publish
    # (the action); the subsequent fault publish passes.
    agent = _agent("i6_agent", _react([ToolCallPart("pair_echo", {}, tool_call_id="c1")]), tools=[pair_echo])
    worker = container.get(Worker)
    worker.add_nodes(agent, pair_echo)
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    orig_publish = broker.publish
    state = {"step_count": 0, "step_seen": False, "raised": False}

    async def spy(message: Any, *args: Any, **kwargs: Any) -> Any:
        wire = (kwargs.get("headers") or {}).get(HDR_WIRE)
        if wire == StepMessage.WIRE:
            state["step_count"] += 1
            state["step_seen"] = True
        elif state["step_seen"] and not state["raised"]:
            state["raised"] = True
            raise RuntimeError("action publish boom")
        return await orig_publish(message, *args, **kwargs)

    monkeypatch.setattr(broker, "publish", spy)
    async with TestKafkaBroker(broker):
        with contextlib.suppress(NodeFaultError):
            await client.agent(topic="i6_agent.in").execute("go", timeout=10)

    assert state["raised"] is True  # the injection actually fired on the action publish
    assert state["step_count"] == 1  # ONE StepMessage for the hop; the fault-exit flush was drained


async def test_open_guard_fault_exit_flushes_drained_and_faults(container, monkeypatch) -> None:
    # The fan-out OPEN guard (spec §3.4 / plan F1): a raise out of _handle_fanout_open faults the
    # caller on the pre-mutation snapshot, with the exit flush invoked first — a drained no-op
    # (the chokepoint flush already published the dispatch steps), so exactly ONE StepMessage
    # still publishes for the hop and the run fails cleanly instead of hanging.
    from calfkit.nodes.agent import BaseAgentNodeDef

    async def _boom_open(self: Any, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("open boom")

    monkeypatch.setattr(BaseAgentNodeDef, "_handle_fanout_open", _boom_open)
    calls = [ToolCallPart("pair_echo", {}, tool_call_id="c1"), ToolCallPart("pair_ok", {}, tool_call_id="c2")]
    agent = _agent("og_agent", _react(calls), tools=[pair_echo, pair_ok])
    worker = container.get(Worker)
    worker.add_nodes(agent, pair_echo, pair_ok)
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    orig_publish = broker.publish
    step_count = {"n": 0}

    async def spy(message: Any, *args: Any, **kwargs: Any) -> Any:
        if (kwargs.get("headers") or {}).get(HDR_WIRE) == StepMessage.WIRE:
            step_count["n"] += 1
        return await orig_publish(message, *args, **kwargs)

    monkeypatch.setattr(broker, "publish", spy)
    async with TestKafkaBroker(broker):
        handle = await client.agent(topic="og_agent.in").start("go")
        events = [e async for e in handle.stream()]
    assert type(events[-1]).__name__ == "RunFailed"  # the guard faulted the caller; no hang
    assert step_count["n"] == 1  # the chokepoint flush; the guard's exit flush was a drained no-op


# --------------------------------------------------------------------------- #
# I2 — no residue (single-run oracle)                                          #
# --------------------------------------------------------------------------- #


async def test_i2_no_residue_single_run_oracle(container, monkeypatch) -> None:
    # ONE run with flush replaced by a recording NO-OP spy: the kernel minted exactly the expected
    # events (so emission was live), nothing published — and the terminal envelope carries no
    # step-derived residue, field-level. No A/B run comparison (timestamps/tool_call_ids differ).
    recorded: list[list[Any]] = []

    async def spy(self: HopStepLedger, broker: Any, **kwargs: Any) -> None:
        if self._events:
            recorded.append(list(self._events))
        self._events.clear()

    monkeypatch.setattr(HopStepLedger, "flush", spy)

    agent = _agent("i2_agent", _react([ToolCallPart("pair_echo", {}, tool_call_id="c1")]), tools=[pair_echo])
    worker = container.get(Worker)
    worker.add_nodes(agent, pair_echo)
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    orig_publish = broker.publish
    terminals: list[Any] = []

    async def capture(message: Any, *args: Any, **kwargs: Any) -> Any:
        headers = kwargs.get("headers") or {}
        if headers.get(HDR_KIND) == "return":
            terminals.append(message)
        return await orig_publish(message, *args, **kwargs)

    monkeypatch.setattr(broker, "publish", capture)
    async with TestKafkaBroker(broker):
        result = await client.agent(topic="i2_agent.in").execute("go", timeout=10)

    # Run correctness unaffected by total step loss (I1) — the spy published nothing.
    assert result.output is not None and "pair-echo-ok" in result.output
    # The expected mints were captured, in production order (§3.4): the dispatch hop's preamble
    # fact + call step, then the fold hop's result step.
    flat = [(type(e).__name__, getattr(e, "tool_call_id", None)) for hop in recorded for e in hop]
    assert flat == [("AgentMessageStep", None), ("ToolCallStep", "c1"), ("ToolResultStep", "c1")]
    # Field-level residue scan of the terminal envelope: no step event shapes.
    assert terminals, "expected the terminal return envelope to be captured"
    dump = terminals[-1].model_dump_json()
    step_shapes = ('"kind":"tool_call"', '"kind":"tool_result"', '"kind":"agent_message"', '"kind":"handoff"', '"kind":"agent_thinking"')
    for residue in step_shapes:
        assert residue not in dump, f"step residue {residue!r} leaked into the terminal envelope"
