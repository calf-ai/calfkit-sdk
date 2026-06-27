"""Real-broker (``kafka`` lane) end-to-end DISCOVER-mode roundtrip (cross-process).

The open-ended counterpart to ``test_tool_discovery_kafka.py`` (by name): an agent holds
only ``Tools(discover=True)`` — no names — and discovers EVERY live tool node from the
shared capability view. Two tool nodes are DEPLOYED in two SEPARATE workers (the
cross-process property under test: the agent reads tool processes it does not host); the
agent in a third worker resolves both at runtime, the model's POV includes exactly the two
it advertised, and a call drawn from that POV round-trips over Kafka:

    two tool nodes advertise (separate workers) -> the agent's view catch-up includes both
      -> the model's POV (``AgentInfo.function_tools``) is exactly the two discovered tools
      -> the model calls one -> a ToolCallRef is dispatched over Kafka to that node's topic
      -> the return value comes back and the agent finalizes.

The discover surface is asserted over this test's unique ``topic_namespace`` only: the kafka
lane shares one session broker, so other suites' (still-live) tool nodes may also be on
``calf.capabilities`` — discover correctly pulls them too, but they are not this test's
subject. Filtering to the namespace proves discover found *all of this test's* separately
deployed tool processes.

Opt-in (``-m kafka``); skips cleanly without Docker. Run with
``uv run --group integration pytest tests/integration/test_discover_mode_kafka.py -m kafka``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.controlplane import ControlPlaneConfig, ControlPlaneView
from calfkit.models.capability import CAPABILITY_TOPIC, CapabilityRecord
from calfkit.nodes import Agent, ToolNodeDef, Tools, agent_tool
from calfkit.worker import Worker
from tests.integration._kafka_helpers import fast_control_plane
from tests.integration._roundtrip_helpers import FINAL_OUTPUT, capturing_model, tool_returns

# Every test here needs a real broker. FunctionModel is offline, but pydantic-ai still
# gates "model requests" behind this flag (matches the other kafka-lane agent suites).
pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True

_EARLIEST = {"auto_offset_reset": "earliest"}


def _worker(bootstrap: str, *, nodes: list, control_plane: ControlPlaneConfig) -> Worker:
    """A Worker on its own broker connection, control plane on, reading earliest.

    Each tool node needs the control plane to advertise; the agent worker needs it to
    materialize the capability view ``Tools(discover=True)`` enumerates.
    """
    return Worker(Client.connect(bootstrap), nodes=nodes, control_plane=control_plane, extra_subscribe_kwargs=_EARLIEST)


def _add_tool(name: str) -> ToolNodeDef:
    """A per-test, uniquely-named ``add`` tool node (the node_id IS the capability key on the
    shared ``calf.capabilities`` topic, so it must be unique across tests)."""

    def add(a: int, b: int) -> int:
        return a + b

    return agent_tool(add, name=name)


def _mul_tool(name: str) -> ToolNodeDef:
    """A second, distinct uniquely-named tool node, deployed in its own worker."""

    def mul(a: int, b: int) -> int:
        return a * b

    return agent_tool(mul, name=name)


async def _wait(predicate: Callable[[], bool], *, timeout: float, what: str) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"timed out after {timeout}s waiting for: {what}")


async def _await_view(bootstrap: str, predicate: Callable[[ControlPlaneView[CapabilityRecord]], bool], *, timeout: float, what: str) -> None:
    """Open a transient capability view, wait for ``predicate`` over it, then close it — so the
    agent's view catch-up will include whatever the predicate confirmed is live."""
    view: ControlPlaneView[CapabilityRecord] = ControlPlaneView.open(
        bootstrap_servers=bootstrap, topic=CAPABILITY_TOPIC, record_type=CapabilityRecord, ensure_topic=False
    )
    try:
        await view.start()
        await _wait(lambda: predicate(view), timeout=timeout, what=what)
    finally:
        await view.stop()


async def test_discover_finds_all_separately_deployed_tool_nodes(kafka_bootstrap: str, topic_namespace: str) -> None:
    """``Tools(discover=True)`` discovers EVERY separately-deployed tool node. Two tool nodes
    live in two separate workers; the agent in a third worker holds only ``Tools(discover=True)``
    — within its namespace its model-POV is exactly the two advertised tools, and a call drawn
    from that POV round-trips."""
    add_name = f"{topic_namespace}-add"
    mul_name = f"{topic_namespace}-mul"
    agent_id = f"{topic_namespace}-disc-all"
    agent_in = f"{topic_namespace}.disc-all.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    pov: dict[str, Any] = {}  # name -> ToolDefinition the agent resolved from the view and presented to the model
    agent = Agent(
        agent_id,
        system_prompt="use the available tools",
        subscribe_topics=agent_in,
        model_client=capturing_model(pov, [ToolCallPart(add_name, {"a": 2, "b": 3}, tool_call_id="c1")]),
        tools=[Tools(discover=True)],  # no names — discover every live tool node
    )

    advertised: dict[str, Any] = {}  # bare tool name -> parameters_json_schema, read straight from the view records

    def _both_advertised(view: ControlPlaneView[CapabilityRecord]) -> bool:
        records = [view.get(add_name), view.get(mul_name)]
        if any(record is None for record in records):
            return False
        advertised.clear()
        for record in records:
            advertised.update({t.name: t.parameters_json_schema for t in record.tools or []})
        return True

    driver = Client.connect(kafka_bootstrap)
    # SEPARATE workers per tool node — the cross-process property under test.
    add_worker = _worker(kafka_bootstrap, nodes=[_add_tool(add_name)], control_plane=control_plane)
    mul_worker = _worker(kafka_bootstrap, nodes=[_mul_tool(mul_name)], control_plane=control_plane)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], control_plane=control_plane)

    async with add_worker, mul_worker:
        # the agent's view must materialize BOTH separately-published records before it runs;
        # capture what each node advertised (the source of truth the agent reads) in the same pass
        await _await_view(kafka_bootstrap, _both_advertised, timeout=60, what=f"advertised tools for {add_name!r} and {mul_name!r}")
        async with agent_worker:
            result = await driver.agent(topic=agent_in).execute("add 2 and 3", timeout=120)

    # 1) the call drawn from a discovered tool round-tripped to a finalized turn
    assert result.output is not None and FINAL_OUTPUT in result.output
    assert tool_returns(result.message_history)[add_name] == 5

    # 2) discover found EXACTLY this test's two separately-deployed tool nodes (namespace-scoped:
    #    the shared lane broker may also carry other suites' live tools, which discover correctly
    #    pulls too — but only this test's two are the subject)
    assert pov, "the model never captured the agent's tool POV"
    mine = {name for name in pov if name.startswith(topic_namespace)}
    assert mine == {add_name, mul_name}

    # 3) the model's POV SCHEMAS == what each tool node advertised on the view, discovered at
    #    runtime (nothing baked in). For a tool node the LLM-facing name is the bare node_id, so
    #    pov[name] and advertised[name] key alike.
    assert {n: pov[n].parameters_json_schema for n in mine} == {n: advertised[n] for n in mine}

    await driver.aclose()
    await add_worker._client.aclose()
    await mul_worker._client.aclose()
    await agent_worker._client.aclose()
