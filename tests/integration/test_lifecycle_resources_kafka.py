"""Real-broker (``kafka`` lane) lifecycle test: resources reach the Agent ``run``
and an ``agent_tool``.

This moved out of the offline ``tests/test_lifecycle_e2e.py`` because hosting a
function tool node now stands up a REAL ``calf.capabilities`` control-plane writer
at worker boot (always-on advertising — the tool node's ``@advertises``). That
writer opens its own aiokafka producer, which the in-memory ``TestKafkaBroker``
does NOT intercept, so the worker can only boot against a reachable broker.

The agent->tool round trip itself still runs over ``TestKafkaBroker``; the real
broker is needed solely so the control-plane writer/publisher can start (and
auto-create ``calf.capabilities``, same as the other capability-lane suites).

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker. Run with
``uv run --group integration pytest tests/integration/test_lifecycle_resources_kafka.py -m kafka``.
"""

from __future__ import annotations

from typing import Any

import pytest
from faststream.kafka import TestKafkaBroker

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client
from calfkit.models.session_context import SessionRunContext
from calfkit.models.tool_context import ToolContext
from calfkit.nodes import Agent, agent_tool
from calfkit.worker import Worker

# Every test here needs a real broker. FunctionModel is offline, but pydantic-ai still
# gates "model requests" behind this flag (matches the other kafka-lane agent suites).
pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True


def _tool_then_text(captured: dict[str, Any]) -> FunctionModel:
    """A FunctionModel that calls ``read_resource`` once, then emits text."""

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        if any(isinstance(p, ToolReturnPart) for p in getattr(last, "parts", [])):
            return ModelResponse(parts=[TextPart("done")])
        return ModelResponse(parts=[ToolCallPart(tool_name="read_resource", args={})])

    return FunctionModel(_fn)


async def test_resources_reach_agent_run_and_agent_tool(kafka_bootstrap: str) -> None:
    """The Agent's ``run`` reads ``ctx.resources`` (via prepare_context) and an
    ``agent_tool`` reads ``ctx.resources`` (via the ToolContext) — two of the
    four surfaces, exercised through a real agent->tool round trip.

    In the ``kafka`` lane because hosting the tool node stands up a real
    control-plane writer at boot (always-on advertising), so the worker needs a
    reachable broker even though the round trip itself runs over ``TestKafkaBroker``.
    """
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

    # A real bootstrap so the always-on control-plane writer can start; the round
    # trip itself is still simulated in-memory by TestKafkaBroker below.
    worker = Worker(Client.connect(kafka_bootstrap))
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
