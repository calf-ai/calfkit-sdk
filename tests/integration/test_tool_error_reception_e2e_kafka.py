"""End-to-end tool-error reception on the real broker (Phase 5, ``kafka`` lane).

The over-the-wire counterpart to ``tests/test_tool_error_reception_e2e.py``, proving the two things the
offline lane cannot:

* **fan-out through the real ktables store** — ``surface_to_model`` converts a faulting fan-out sibling
  after a genuine durable fold + snapshot/restore (the offline fake store can mask store round-trips);
* **the ``message_agent`` arm** — the interim ``SlotRef`` carriage lets ``surface_to_model`` convert a
  PEER fault (whose reply state is foreign, so ``state.tool_calls`` can't resolve the caller's tool),
  including the **positive mixed-batch amplification** the carriage exists for: a tool AND a peer both
  faulting in one batch, BOTH converted, so the batch closes instead of escalating and discarding the
  tool conversion.

Modeled on ``test_message_agent_kafka.py`` (the messaging spine) and ``test_fault_escalation_kafka.py``
(the fault harness). Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, RetryPromptPart, TextPart, ToolCallPart, ToolReturnPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client
from calfkit.controlplane import ControlPlaneConfig, ControlPlaneView
from calfkit.models.agents import AGENTS_TOPIC, AgentCard
from calfkit.nodes import Agent
from calfkit.nodes._tool_error import surface_to_model
from calfkit.peers import Messaging
from calfkit.worker import Worker
from tests.integration._fault_kafka import fault_worker
from tests.integration._fault_tools import boom, ok_a, quota
from tests.integration._kafka_helpers import fast_control_plane

pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True

_EARLIEST = {"auto_offset_reset": "earliest"}


def _react_model(tool_calls: list[ToolCallPart], capture: dict[str, list[str]]) -> FunctionModel:
    """Emit ``tool_calls`` on turn 1; on the next turn record what materialized (``RetryPromptPart`` →
    ``retries``, ``ToolReturnPart`` → ``returns``) and finalize echoing it."""

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


def _worker(bootstrap: str, *, nodes: list, control_plane: ControlPlaneConfig) -> Worker:
    return Worker(Client.connect(bootstrap), nodes=nodes, control_plane=control_plane, extra_subscribe_kwargs=_EARLIEST)


async def _await_agents_view(bootstrap: str, predicate: Callable[[ControlPlaneView[AgentCard]], bool], *, timeout: float, what: str) -> None:
    view: ControlPlaneView[AgentCard] = ControlPlaneView.open(
        bootstrap_servers=bootstrap, topic=AGENTS_TOPIC, record_type=AgentCard, ensure_topic=False
    )
    try:
        await view.start()
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            if predicate(view):
                return
            await asyncio.sleep(0.1)
        raise AssertionError(f"timed out after {timeout}s waiting for: {what}")
    finally:
        await view.stop()


async def test_surface_to_model_converts_a_fanout_sibling_via_the_real_store(kafka_bootstrap: str, topic_namespace: str) -> None:
    # A faulting fan-out sibling (boom) + a successful one (ok_a) through the REAL ktables durable fold:
    # surface_to_model converts boom's fault → the model sees the level-A is_error result and finalizes.
    capture: dict[str, list[str]] = {}
    a_in = f"{topic_namespace}.f.input"
    agent = Agent(
        f"{topic_namespace}-f",
        subscribe_topics=a_in,
        model_client=_react_model([ToolCallPart("boom", {"x": 9}, tool_call_id="t1"), ToolCallPart("ok_a", {}, tool_call_id="t2")], capture),
        tools=[boom, ok_a],
        on_tool_error=[surface_to_model()],
    )
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, boom, ok_a])
    try:
        async with worker:
            result = await driver.agent(topic=a_in).execute("go", timeout=60)
    finally:
        await driver.aclose()
        await worker._client.aclose()

    assert result.output is not None and "kaboom(9)" in result.output
    assert capture["retries"] == ["ValueError: kaboom(9)"]  # the faulting sibling converted through the real store
    assert capture["returns"] == ["a_result"]  # the successful sibling folded normally


async def test_surface_to_model_converts_a_message_agent_fault(kafka_bootstrap: str, topic_namespace: str) -> None:
    # THE reason the carriage exists: A messages B; B's tool raises; B's fault escalates to A's
    # message_agent slot; A's surface_to_model converts it (resolving the tool identity from the carriage,
    # not the peer's foreign state) → A's model sees the level-A is_error result and finalizes.
    capture: dict[str, list[str]] = {}
    a_name = f"{topic_namespace}-A"
    b_name = f"{topic_namespace}-B"
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    agent_a = Agent(
        a_name,
        subscribe_topics=a_in,
        model_client=_react_model([ToolCallPart("message_agent", {"name": b_name, "message": "do it"}, tool_call_id="m1")], capture),
        peers=[Messaging(b_name)],
        on_tool_error=[surface_to_model()],
    )
    agent_b = Agent(
        b_name,
        subscribe_topics=f"{topic_namespace}.B.input",
        model_client=_react_model([ToolCallPart("boom", {"x": 1}, tool_call_id="bx")], {}),
        tools=[boom],
    )

    driver = Client.connect(kafka_bootstrap)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b, boom], control_plane=control_plane)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a], control_plane=control_plane)
    try:
        async with b_worker:
            await _await_agents_view(kafka_bootstrap, lambda v: v.get(b_name) is not None, timeout=60, what=f"B's AgentCard {b_name!r}")
            async with a_worker:
                result = await driver.agent(topic=a_in).execute("ask B", timeout=120)
    finally:
        await driver.aclose()
        await b_worker._client.aclose()
        await a_worker._client.aclose()

    assert result.output is not None and "kaboom(1)" in result.output
    assert capture["retries"] == ["ValueError: kaboom(1)"]  # the PEER fault converted via the carriage


async def test_positive_mixed_batch_converts_both_tool_and_peer(kafka_bootstrap: str, topic_namespace: str) -> None:
    # The B1-amplification the carriage exists for: A emits a message_agent AND a tool in ONE batch, BOTH
    # fault. surface_to_model converts BOTH (the peer via the carriage, the tool via state) → the batch
    # CLOSES instead of escalating, and A's model sees both is_error results. Without the carriage the
    # unhandled peer fault would escalate the whole batch, discarding the tool conversion (D12).
    capture: dict[str, list[str]] = {}
    a_name = f"{topic_namespace}-A"
    b_name = f"{topic_namespace}-B"
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    agent_a = Agent(
        a_name,
        subscribe_topics=a_in,
        model_client=_react_model(
            [
                ToolCallPart("message_agent", {"name": b_name, "message": "do it"}, tool_call_id="m1"),
                ToolCallPart("boom", {"x": 7}, tool_call_id="t1"),
            ],
            capture,
        ),
        peers=[Messaging(b_name)],
        tools=[boom],
        on_tool_error=[surface_to_model()],
    )
    # B mints a verbatim NodeFaultError (billing.quota_exceeded) — exception=None, so its level-A render
    # is the message alone ("over quota"), distinguishing it from A's own boom (ValueError: kaboom(7)).
    agent_b = Agent(
        b_name,
        subscribe_topics=f"{topic_namespace}.B.input",
        model_client=_react_model([ToolCallPart("quota", {"x": 3}, tool_call_id="bx")], {}),
        tools=[quota],
    )

    driver = Client.connect(kafka_bootstrap)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b, quota], control_plane=control_plane)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a, boom], control_plane=control_plane)
    try:
        async with b_worker:
            await _await_agents_view(kafka_bootstrap, lambda v: v.get(b_name) is not None, timeout=60, what=f"B's AgentCard {b_name!r}")
            async with a_worker:
                result = await driver.agent(topic=a_in).execute("message B and call boom", timeout=120)
    finally:
        await driver.aclose()
        await b_worker._client.aclose()
        await a_worker._client.aclose()

    assert result.output is not None  # the batch CLOSED (finalized) — it did not escalate
    retries = capture["retries"]
    assert "over quota" in retries  # the PEER fault, converted via the carriage (framework fault → message alone)
    assert "ValueError: kaboom(7)" in retries  # A's own tool fault, converted via state
