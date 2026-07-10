"""Outcome mapping + terminal gates, end to end (caller-side step-emission spec §5.1/§3.4/§12).

Offline E2E through real runs (container + ``prepare_worker`` + ``TestKafkaBroker`` +
``FunctionModel`` + ``handle.stream()``). These pin the BODY side of the design (F2) composed with
the kernel (F1):

- **Outcome mapping** (§5.1): ``ModelRetry`` → ``failed`` (with the retry text); a plain-value seam
  substitute → ``success`` (deliberate — the stream mirrors the model's view); a schema rejection →
  a born-closed ``denied`` pair; handoff stubs AND rejections → ``denied`` pairs; the winner an
  unpaired ``HandoffStep``.
- **The v1 bug's regression** (§1): a hard fault handled by ``surface_to_model()`` streams
  ``ToolResultEvent(outcome=failed)`` with the substitute text — no more dangling ``ToolCallEvent``.
- **Terminal gates** (§3.4/I11): depth 1 — the final-output hop streams no ``agent_message`` (its
  answer rides the terminal) while earlier hops' preambles DO stream, and the final fold's result
  precedes the terminal on the wire; depth 2 — a consulted peer's answer becomes the CALLER's fold
  result (``name="message_agent"``, payload verbatim), and the returning peer hop streams no
  ``agent_message``.

Depth-2 seeding: the fake agents view rides the caller NODE's resource bag (the ``prepare_worker``
node-resource mechanism — the ``FANOUT_STORE_KEY`` precedent), NOT a hand-built ctx ``_resources``
(``prepare_context`` would rebuild over it); the peer subscribes at ``derive_input_topic(name)``.
"""

from __future__ import annotations

from typing import Any

from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai.exceptions import ModelRetry
from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    RetryPromptPart,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client
from calfkit.models.agents import AGENTS_VIEW_RESOURCE_KEY, derive_input_topic
from calfkit.models.tool_context import ToolContext
from calfkit.nodes import Agent, agent_tool
from calfkit.nodes._tool_error import surface_to_model
from calfkit.peers import Handoff, Messaging
from calfkit.peers.handoff import HANDOFF_TOOL
from calfkit.worker import Worker
from tests._peer_fakes import agents_view
from tests.providers import prepare_worker


@agent_tool
def oc_retry(ctx: ToolContext) -> str:
    raise ModelRetry("narrow the query")


@agent_tool
def oc_boom(ctx: ToolContext) -> str:
    raise ValueError("oc kaboom")


@agent_tool
def oc_typed(ctx: ToolContext, x: int) -> str:
    return f"typed({x})"


@agent_tool
def oc_plain(ctx: ToolContext) -> str:
    return "oc-plain-ok"


def _react(tool_calls: list[ToolCallPart], *, final: str = "done") -> FunctionModel:
    """Turn 1: a preamble + ``tool_calls``; the reaction turn (any ToolReturn/RetryPrompt seen)
    finalizes with ``final`` + whatever materialized."""

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        if isinstance(last, ModelRequest):
            resolved = [p for p in last.parts if isinstance(p, (ToolReturnPart, RetryPromptPart))]
            if resolved:
                return ModelResponse(parts=[TextPart(f"{final}: {' | '.join(str(getattr(p, 'content', '')) for p in resolved)}")])
        return ModelResponse(parts=[TextPart("thinking about it"), *tool_calls])

    return FunctionModel(_fn)


def _final_text(text: str) -> FunctionModel:
    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart(text)])

    return FunctionModel(_fn)


async def _run_streaming(container: Any, agent: Agent, *nodes: Any) -> list[Any]:
    worker = container.get(Worker)
    worker.add_nodes(agent, *nodes)
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)
    async with TestKafkaBroker(broker):
        handle = await client.agent(topic=agent.subscribe_topics[0]).start("go")
        return [e async for e in handle.stream()]


def _events_of(events: list[Any], kind: str) -> list[Any]:
    return [e for e in events if type(e).__name__ == kind]


# --------------------------------------------------------------------------- #
# Outcome mapping (§5.1)                                                       #
# --------------------------------------------------------------------------- #


async def test_model_retry_streams_failed_with_the_retry_text(container) -> None:
    agent = Agent("oc_a1", system_prompt="x", subscribe_topics="oc_a1.in", model_client=_react([ToolCallPart("oc_retry", {})]), tools=[oc_retry])
    events = await _run_streaming(container, agent, oc_retry)
    assert type(events[-1]).__name__ == "RunCompleted"
    (result,) = _events_of(events, "ToolResultEvent")
    assert result.outcome == "failed"
    assert "narrow the query" in result.parts[0].text  # the model-visible retry text rides the step


async def test_plain_value_substitute_streams_success(container) -> None:
    # A seam substituting a plain SUCCESS value over a hard fault reads success — deliberate
    # (§5.1: the stream mirrors the model's view); payload = the resolved (substituted) parts.
    def on_error(tool_call: Any, ctx: Any, report: Any) -> Any:
        return "cached-value"

    agent = Agent(
        "oc_a2",
        system_prompt="x",
        subscribe_topics="oc_a2.in",
        model_client=_react([ToolCallPart("oc_boom", {})]),
        tools=[oc_boom],
        on_tool_error=on_error,
    )
    events = await _run_streaming(container, agent, oc_boom)
    assert type(events[-1]).__name__ == "RunCompleted"
    (result,) = _events_of(events, "ToolResultEvent")
    assert result.outcome == "success"
    assert result.parts[0].text == "cached-value"


async def test_schema_rejection_streams_a_born_closed_denied_pair(container) -> None:
    # The caller refused to dispatch: ONE fact expands into BOTH halves — the call step (raw args)
    # and the denied result with the rejection text the model sees (§3.1a/§5.1).
    bad = ToolCallPart("oc_typed", {"x": "not-a-number"}, tool_call_id="d1")
    agent = Agent("oc_a3", system_prompt="x", subscribe_topics="oc_a3.in", model_client=_react([bad]), tools=[oc_typed])
    events = await _run_streaming(container, agent, oc_typed)
    assert type(events[-1]).__name__ == "RunCompleted"
    (call,) = [c for c in _events_of(events, "ToolCallEvent") if c.tool_call_id == "d1"]
    (result,) = [r for r in _events_of(events, "ToolResultEvent") if r.tool_call_id == "d1"]
    assert call.name == result.name == "oc_typed"
    assert result.outcome == "denied"
    assert result.parts and result.parts[0].text  # the rendered rejection content


async def test_surface_to_model_regression_streams_failed_with_the_substitute(container) -> None:
    # THE v1 bug (§1): a surface_to_model()-handled hard fault left a dangling ToolCallEvent —
    # no paired result, ever — while the run continued. Now the caller's fold mints the closure.
    agent = Agent(
        "oc_a4",
        system_prompt="x",
        subscribe_topics="oc_a4.in",
        model_client=_react([ToolCallPart("oc_boom", {}, tool_call_id="s1")]),
        tools=[oc_boom],
        on_tool_error=[surface_to_model()],
    )
    events = await _run_streaming(container, agent, oc_boom)
    assert type(events[-1]).__name__ == "RunCompleted"
    (result,) = _events_of(events, "ToolResultEvent")
    assert result.tool_call_id == "s1"
    assert result.outcome == "failed"
    assert "ValueError" in result.parts[0].text and "oc kaboom" in result.parts[0].text  # the substitute the model sees


async def test_handoff_winner_stubs_and_rejections_stream_denied(container) -> None:
    # A winning-handoff turn (§7): the rejected handoff AND the stubbed sibling each get a denied
    # pair (precise reason / stub text); the winner gets an unpaired HandoffStep only.
    calls = [
        ToolCallPart(HANDOFF_TOOL, {"name": "ghost", "message": "hi"}, tool_call_id="h1"),  # rejected: not live
        ToolCallPart(HANDOFF_TOOL, {"name": "oc_billing", "message": "take over"}, tool_call_id="h2"),  # the winner
        ToolCallPart("oc_plain", {}, tool_call_id="t1"),  # stubbed sibling
    ]
    caller = Agent(
        "oc_a5", system_prompt="x", subscribe_topics="oc_a5.in", model_client=_react(calls), tools=[oc_plain], peers=[Handoff("oc_billing", "ghost")]
    )
    caller.resources[AGENTS_VIEW_RESOURCE_KEY] = agents_view({"oc_billing": None})
    billing = Agent("oc_billing", subscribe_topics=derive_input_topic("oc_billing"), model_client=_final_text("billing answer"))
    events = await _run_streaming(container, caller, oc_plain, billing)
    assert type(events[-1]).__name__ == "RunCompleted"

    results = {r.tool_call_id: r for r in _events_of(events, "ToolResultEvent")}
    assert results["h1"].outcome == "denied"  # rejected-before-winner: the precise §9 reason
    assert "ghost" in results["h1"].parts[0].text
    assert results["t1"].outcome == "denied"  # stubbed sibling: a never-executed call is not a success
    assert "h2" not in results  # the winner has NO pair (frame-mirror rule, §6)
    (handoff,) = _events_of(events, "HandoffEvent")
    assert (handoff.target, handoff.reason) == ("oc_billing", "take over")
    # Both halves of each denied pair are present (born-closed).
    call_ids = {c.tool_call_id for c in _events_of(events, "ToolCallEvent")}
    assert {"h1", "t1"} <= call_ids


# --------------------------------------------------------------------------- #
# Terminal gates (§3.4 / I11) — depth 1 and depth 2                            #
# --------------------------------------------------------------------------- #


async def test_terminal_gate_depth1_final_hop_streams_no_agent_message(container) -> None:
    model = _react([ToolCallPart("oc_plain", {})], final="all done")
    agent = Agent("oc_a6", system_prompt="x", subscribe_topics="oc_a6.in", model_client=model, tools=[oc_plain])
    events = await _run_streaming(container, agent, oc_plain)
    assert type(events[-1]).__name__ == "RunCompleted"
    messages = _events_of(events, "AgentMessageEvent")
    # The DISPATCH hop's preamble streams; the FINAL hop's answer rides the terminal only — no
    # agent_message for it (belt: the final-output branch declares no Said; suspenders: the gate).
    assert [m.parts[0].text for m in messages] == ["thinking about it"]
    (result,) = _events_of(events, "ToolResultEvent")
    assert events.index(result) < len(events) - 1  # the final fold's result precedes the terminal


async def test_terminal_gate_depth2_peer_answer_is_the_callers_fold_result(container, monkeypatch) -> None:
    # Flush-level oracle for the fold result: a lone message_agent call opens a DEGENERATE durable
    # batch, so its (completing) fold trickles on the park arm — which the offline broker's inline
    # _publish_reentry orders AFTER the terminal (an accepted reorder; the client-observable form is
    # the kafka lane's pin). The spy records each flush's events + identity and delegates.
    from calfkit.nodes._steps import HopStepLedger

    flushes: list[dict[str, Any]] = []
    real_flush = HopStepLedger.flush

    async def spy(self: HopStepLedger, broker: Any, **kwargs: Any) -> None:
        if self._events:
            flushes.append({"events": list(self._events), **kwargs})
        await real_flush(self, broker, **kwargs)

    monkeypatch.setattr(HopStepLedger, "flush", spy)
    caller = Agent(
        "oc_a7",
        system_prompt="x",
        subscribe_topics="oc_a7.in",
        model_client=_react([ToolCallPart("message_agent", {"name": "oc_peer", "message": "help me"}, tool_call_id="m1")]),
        peers=[Messaging("oc_peer")],
    )
    caller.resources[AGENTS_VIEW_RESOURCE_KEY] = agents_view({"oc_peer": None})
    peer = Agent("oc_peer", subscribe_topics=derive_input_topic("oc_peer"), model_client=_final_text("peer answer"))
    events = await _run_streaming(container, caller, peer)
    assert type(events[-1]).__name__ == "RunCompleted"
    # The peer's answer is the CALLER's fold result: name = marker.tool_name (= "message_agent" —
    # §5.2's hard break from v1's answering-peer name), payload = the resolved parts verbatim.
    (fold,) = [f for f in flushes if any(type(e).__name__ == "ToolResultStep" for e in f["events"])]
    (result,) = fold["events"]
    assert result.tool_call_id == "m1"
    assert result.name == "message_agent"
    assert result.outcome == "success"
    assert result.parts[0].text == "peer answer"
    assert fold["emitter"] == "oc_a7"  # the folding caller, not the answering peer
    assert fold["depth"] == 1  # the CALLER's inbound snapshot depth — not the peer's depth-2 hop
    # No agent_message from the RETURNING peer hop (any-depth gate) — across ALL flushes, the only
    # agent_message is the caller's dispatch-hop preamble.
    preambles = [e for f in flushes for e in f["events"] if type(e).__name__ == "AgentMessageStep"]
    assert [p.parts[0].text for p in preambles] == ["thinking about it"]
    assert [m.parts[0].text for m in _events_of(events, "AgentMessageEvent")] == ["thinking about it"]
