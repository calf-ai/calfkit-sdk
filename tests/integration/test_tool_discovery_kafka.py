"""Real-broker (``kafka`` lane) end-to-end runtime tool DISCOVERY roundtrip.

The discovered-path counterpart to ``test_tool_node_roundtrip_kafka.py`` (eager) and the
tool-node analogue of ``test_mcp_roundtrip_kafka.py``: a tool node is DEPLOYED in one
worker (where it advertises its ``CapabilityRecord`` on ``calf.capabilities``), and an
agent in another worker references it by name via a ``Tools(...)`` handle — discovering
its schema at runtime from the capability view, then dispatching the call over Kafka:

    the tool node advertises -> the agent's view catch-up includes it
      -> the model emits a tool call
      -> the agent resolves ``Tools(name)`` from the view (validator=None: schema-only)
      -> dispatches a ToolCallRef over Kafka to the tool node's topic
      -> the tool node runs the Python function
      -> the return value comes back over Kafka
      -> the agent re-enters the model loop and finalizes.

The bad-args case is the discovered-binding contrast with the eager path
(``test_invalid_tool_node_args_rejected_before_dispatch``): a discovered binding has NO
validator, so the agent does NOT reject schema-invalid args locally — it dispatches them,
the tool node raises on receipt, and the chokepoint escalates a typed ``calf.unhandled``
``FaultMessage`` on the agent's ``publish_topic`` mirror (observed via the fault tap; typed
client reception is deferred to #250).

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker. Run with
``uv run --group integration pytest tests/integration/test_tool_discovery_kafka.py -m kafka``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from calfkit._protocol import HDR_ERROR_TYPE, HDR_KIND
from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.controlplane import ControlPlaneConfig, ControlPlaneView
from calfkit.models.capability import CAPABILITY_TOPIC, CapabilityRecord
from calfkit.models.error_report import FaultTypes
from calfkit.nodes import Agent, ToolNodeDef, Tools, agent_tool
from calfkit.worker import Worker
from tests.integration._fault_kafka import ensure_topic
from tests.integration._fault_tap import fault_tap
from tests.integration._kafka_helpers import fast_control_plane
from tests.integration._roundtrip_helpers import FINAL_OUTPUT, capturing_model, scripted_model, tool_returns

# Every test here needs a real broker. FunctionModel is offline, but pydantic-ai still
# gates "model requests" behind this flag (matches the other kafka-lane agent suites).
pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True

_EARLIEST = {"auto_offset_reset": "earliest"}


def _worker(bootstrap: str, *, nodes: list, control_plane: ControlPlaneConfig) -> Worker:
    """A Worker on its own broker connection, control plane on, reading earliest.

    The tool worker needs the control plane to advertise; the agent worker needs it to
    materialize the capability view it resolves ``Tools`` against.
    """
    return Worker(Client.connect(bootstrap), nodes=nodes, control_plane=control_plane, extra_subscribe_kwargs=_EARLIEST)


def _add_tool(name: str) -> ToolNodeDef:
    """A per-test, uniquely-named ``add`` tool node (the node_id IS the capability key on
    the shared ``calf.capabilities`` topic, so it must be unique across tests)."""

    def add(a: int, b: int) -> int:
        return a + b

    return agent_tool(add, name=name)


async def _wait(predicate: Callable[[], bool], *, timeout: float, what: str) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"timed out after {timeout}s waiting for: {what}")


async def _await_view(bootstrap: str, predicate: Callable[[ControlPlaneView[CapabilityRecord]], bool], *, timeout: float, what: str) -> None:
    """Open a transient capability view, wait for ``predicate`` over it, then close it."""
    view: ControlPlaneView[CapabilityRecord] = ControlPlaneView.open(
        bootstrap_servers=bootstrap, topic=CAPABILITY_TOPIC, record_type=CapabilityRecord, ensure_topic=False
    )
    try:
        await view.start()
        await _wait(lambda: predicate(view), timeout=timeout, what=what)
    finally:
        await view.stop()


async def _await_capability(bootstrap: str, node_id: str, *, timeout: float) -> None:
    """Block until the tool node's CapabilityRecord is live on ``calf.capabilities``, so the
    agent's view catch-up includes it and ``Tools(name)`` resolves on the turn."""
    await _await_view(bootstrap, lambda v: v.get(node_id) is not None, timeout=timeout, what=f"capability record {node_id!r}")


async def test_discovered_tool_node_roundtrips_over_the_wire(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A tool node deployed in one worker is discovered by name from another: the agent
    holds only ``Tools(name)`` (no schema baked in), resolves it from the view, dispatches
    ``add(2, 3)``, and ``5`` travels all the way back into the agent's history."""
    tool_name = f"{topic_namespace}-add"
    agent_id = f"{topic_namespace}-disc-agent"
    agent_in = f"{topic_namespace}.disc-agent.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    tool = _add_tool(tool_name)
    agent = Agent(
        agent_id,
        system_prompt="add two numbers",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart(tool_name, {"a": 2, "b": 3}, tool_call_id="c1")]),
        tools=[Tools(tool_name)],  # by name only — schema discovered at runtime
    )

    driver = Client.connect(kafka_bootstrap)
    tool_worker = _worker(kafka_bootstrap, nodes=[tool], control_plane=control_plane)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], control_plane=control_plane)

    async with tool_worker:
        await _await_capability(kafka_bootstrap, tool_name, timeout=60)
        async with agent_worker:
            result = await driver.execute("add 2 and 3", agent_in, timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    assert tool_returns(result.message_history)[tool_name] == 5

    await driver.close()
    await tool_worker._client.close()
    await agent_worker._client.close()


async def test_model_pov_matches_the_advertised_tool(kafka_bootstrap: str, topic_namespace: str) -> None:
    """The model's POV of its tools IS what the tool node advertised on the view.

    The agent holds only ``Tools(name)`` — no schema baked in. Rather than assuming the
    surface, the model captures what the agent actually resolved from the capability view
    and handed it (``AgentInfo.function_tools``); the test asserts that POV matches the live
    ``CapabilityRecord`` on ``calf.capabilities`` by NAME and by SCHEMA, and that the tool
    drawn from that POV round-trips — proving the model sees the *advertised* tool at runtime,
    not a schema the test hardcoded.
    """
    tool_name = f"{topic_namespace}-add"
    agent_id = f"{topic_namespace}-pov-agent"
    agent_in = f"{topic_namespace}.pov-agent.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    tool = _add_tool(tool_name)
    pov: dict[str, Any] = {}  # name -> ToolDefinition the agent resolved from the view and presented to the model
    agent = Agent(
        agent_id,
        system_prompt="add two numbers",
        subscribe_topics=agent_in,
        model_client=capturing_model(pov, [ToolCallPart(tool_name, {"a": 2, "b": 3}, tool_call_id="c1")]),
        tools=[Tools(tool_name)],  # by name only — the schema must come from the view
    )

    advertised: dict[str, Any] = {}  # name -> parameters_json_schema, read straight from the view record

    def _capture_advertised(view: ControlPlaneView[CapabilityRecord]) -> bool:
        record = view.get(tool_name)
        if record is None:
            return False
        advertised.clear()
        advertised.update({t.name: t.parameters_json_schema for t in record.tools})
        return True

    driver = Client.connect(kafka_bootstrap)
    tool_worker = _worker(kafka_bootstrap, nodes=[tool], control_plane=control_plane)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], control_plane=control_plane)

    async with tool_worker:
        await _await_capability(kafka_bootstrap, tool_name, timeout=60)
        # The source of truth the agent reads: what the tool node advertised on the view.
        await _await_view(kafka_bootstrap, _capture_advertised, timeout=60, what=f"advertised tools for {tool_name!r}")
        async with agent_worker:
            result = await driver.execute("add 2 and 3", agent_in, timeout=120)

    # 1) the call drawn from the advertised tool round-tripped to a finalized turn
    assert result.output is not None and FINAL_OUTPUT in result.output
    assert tool_returns(result.message_history)[tool_name] == 5

    # 2) the tool node advertised exactly its one tool
    assert set(advertised) == {tool_name}

    # 3) the model's POV == the advertised view record, by NAME and SCHEMA — the agent presented
    #    to the model exactly what the tool node advertised, discovered at runtime (nothing baked in)
    assert pov, "the model never captured the agent's tool POV"
    assert set(pov) == set(advertised)
    assert {name: td.parameters_json_schema for name, td in pov.items()} == advertised

    # 4) the discovered tool reached the model with its real args
    assert {"a", "b"} <= set(pov[tool_name].parameters_json_schema.get("properties", {}))

    await driver.close()
    await tool_worker._client.close()
    await agent_worker._client.close()


async def test_discovered_bad_args_escalate_unhandled_fault(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A discovered binding carries NO validator (schema-only), so the agent dispatches
    schema-invalid args instead of rejecting them locally (the eager path's contrast): the
    tool node raises on receipt and the chokepoint escalates a typed ``calf.unhandled``
    ``FaultMessage`` on the agent's ``publish_topic`` mirror."""
    tool_name = f"{topic_namespace}-add"
    agent_id = f"{topic_namespace}-disc-fault"
    agent_in = f"{topic_namespace}.disc-fault.input"
    agent_pub = f"{topic_namespace}.disc-fault.mirror"
    control_plane = fast_control_plane(kafka_bootstrap)

    tool = _add_tool(tool_name)
    agent = Agent(
        agent_id,
        system_prompt="add with bad args",
        subscribe_topics=agent_in,
        publish_topic=agent_pub,
        model_client=scripted_model([ToolCallPart(tool_name, {"a": "not-an-int", "b": 3}, tool_call_id="c1")]),
        tools=[Tools(tool_name)],
        sequential_only_mode=True,  # single dispatch; keep the durable fan-out store out of the fault path
    )
    await ensure_topic(kafka_bootstrap, agent_pub)

    driver = Client.connect(kafka_bootstrap)
    tool_worker = _worker(kafka_bootstrap, nodes=[tool], control_plane=control_plane)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], control_plane=control_plane)

    async with tool_worker:
        await _await_capability(kafka_bootstrap, tool_name, timeout=60)
        async with agent_worker, fault_tap(kafka_bootstrap, agent_pub) as tap:
            await driver.start("add with bad args", agent_in)  # reply owed; the discovered binding dispatches the bad args

            fault, headers = await tap.next_fault(timeout=60)
            # The bad args reached the tool node (the discovered binding did NOT validate
            # locally), and its raise escalated as a typed unhandled fault.
            assert headers[HDR_KIND] == "fault"
            assert headers[HDR_ERROR_TYPE] == FaultTypes.UNHANDLED
            assert fault.error.error_type == FaultTypes.UNHANDLED
            assert fault.error.details.get(FaultTypes.EXCEPTION_TYPE) is not None  # the tool's exception class

    await driver.close()
    await tool_worker._client.close()
    await agent_worker._client.close()
