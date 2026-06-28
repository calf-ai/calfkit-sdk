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
from calfkit.nodes.agent import BaseAgentNodeDef
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
    # Force the chokepoint's project_steps to raise on every agent hop; the §2.5/§2.9 guard must
    # swallow it and still publish the action, so execute() COMPLETES (an unguarded raise would hang
    # the run — the tool Call would never publish).
    def _boom(self, output, ctx, frame):  # noqa: ANN001, ANN202
        raise RuntimeError("step projection boom")

    monkeypatch.setattr(BaseAgentNodeDef, "project_steps", _boom)

    worker = container.get(Worker)
    agent = Agent(
        "guard_agent",
        system_prompt="x",
        subscribe_topics="guard_agent.input",
        model_client=FunctionModel(_call_then_final),
        tools=[echo_for_guard],
        sequential_only_mode=True,
    )
    worker.add_nodes(agent, echo_for_guard)
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)
    async with TestKafkaBroker(broker):
        result = await client.agent(topic="guard_agent.input").execute("what year", timeout=10)
    # The run completed end to end DESPITE the projection raising on every hop.
    assert result.output is not None and "guarded-1967" in result.output
