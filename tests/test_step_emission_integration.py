"""Increment D — the chokepoint guard, exercised through a real FunctionModel agent run (offline).

Observing the emitted steps is increment E (the client step handler); here we prove the load-bearing
§2.5/§2.9 guarantee: a failure in step projection/publish at the disposition chokepoint logs-and-drops
AND falls through to the real action, so the run still completes. An unguarded raise would escape to
FastStream and — under ACK_FIRST (the inbound already acked) — the tool Call would never publish and
the run would hang.
"""

from __future__ import annotations

from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client
from calfkit.models.tool_context import ToolContext
from calfkit.nodes import Agent, agent_tool
from calfkit.nodes._steps import HopStepLedger
from calfkit.worker import Worker
from tests.providers import prepare_worker


@agent_tool
def echo_for_guard(ctx: ToolContext) -> str:
    return "guarded-1967"


def _call_then_final(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    last = messages[-1]
    if isinstance(last, ModelRequest):
        returns = [p for p in last.parts if isinstance(p, ToolReturnPart)]
        if returns:
            return ModelResponse(parts=[TextPart(f"final: {returns[0].content}")])
    # hop 1: a preamble + a tool call — exactly a step-emitting hop.
    return ModelResponse(parts=[TextPart("let me check that"), ToolCallPart("echo_for_guard")])


async def test_step_emission_failure_does_not_break_the_run(container, monkeypatch) -> None:  # noqa: ANN001
    # Force the ledger's flush to raise on every hop exit; the kernel's exit-flush helper owns the
    # best-effort log-and-drop guard (I1), so the run still COMPLETES (an unguarded raise would
    # escape to FastStream and — under ACK_FIRST — hang the run: the tool Call would never publish).
    # Replacing HopStepLedger.flush WHOLESALE still lands inside the helper's guard.
    async def _boom(self, broker, **kwargs):  # noqa: ANN001, ANN003, ANN202
        raise RuntimeError("step flush boom")

    monkeypatch.setattr(HopStepLedger, "flush", _boom)

    worker = container.get(Worker)
    agent = Agent(
        "guard_agent",
        system_prompt="x",
        subscribe_topics="guard_agent.input",
        model_client=FunctionModel(_call_then_final),
        tools=[echo_for_guard],
    )
    worker.add_nodes(agent, echo_for_guard)
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)
    async with TestKafkaBroker(broker):
        result = await client.agent(topic="guard_agent.input").execute("what year", timeout=10)
    # The run completed end to end DESPITE the projection raising on every hop.
    assert result.output is not None and "guarded-1967" in result.output


async def test_stream_yields_intermediate_steps_then_terminal(container) -> None:  # noqa: ANN001
    # The full D+E offline e2e: a real FunctionModel agent run (preamble + tool call hop, then final
    # answer) — the held handle's stream() yields the intermediate step events (AgentMessageEvent +
    # ToolCallEvent from the agent hop, ToolResultEvent from the tool) then the RunCompleted terminal.
    worker = container.get(Worker)
    agent = Agent(
        "stream_agent",
        system_prompt="x",
        subscribe_topics="stream_agent.input",
        model_client=FunctionModel(_call_then_final),
        tools=[echo_for_guard],
    )
    worker.add_nodes(agent, echo_for_guard)
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)
    async with TestKafkaBroker(broker):
        handle = await client.agent(topic="stream_agent.input").start("go", correlation_id="cid-stream")
        events = [e async for e in handle.stream()]
    kinds = [type(e).__name__ for e in events]
    assert kinds[-1] == "RunCompleted"  # terminal last (terminal-bearing stream)
    assert "AgentMessageEvent" in kinds  # the hop's preamble (a Said fact, absorbed at the unwrap)
    assert "ToolCallEvent" in kinds  # the requested tool call (note_dispatch from the marker)
    assert "ToolResultEvent" in kinds  # the CALLER's fold-minted result (spec §3.2 — not the tool's)
    # the tool result carries the resolved slot parts + resolved as a success
    tr = next(e for e in events if type(e).__name__ == "ToolResultEvent")
    assert tr.outcome == "success"
    assert "guarded-1967" in tr.parts[0].text
    # depth orients the caller: both the dispatch hop and the FOLD hop run at the agent's inbound
    # depth 1 — the result is minted by the folding caller, no longer by the tool at depth 2.
    am = next(e for e in events if type(e).__name__ == "AgentMessageEvent")
    assert am.depth == 1
    assert tr.depth == 1
    assert tr.emitter == "stream_agent"  # the folding caller
