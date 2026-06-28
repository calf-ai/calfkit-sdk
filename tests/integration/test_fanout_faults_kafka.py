"""Suite X — fan-out fault paths over the real durable store.

A fan-out-capable agent (no ``sequential_only_mode``) opens a real
``KtablesFanoutBatchStore`` and folds N sibling replies; these tests drive faulting
siblings through that durable fold over the wire (offline-only today):

* **X-1** — one unhandled sibling fault → at closure the batch fails with a *flattened*
  fault (the bare child ``calf.exception``), carrying the partial-success topology under
  ``details["calf.fanout_topology"]``; the body never resumes.
* **X-2** — two unhandled sibling faults → a ``calf.fault_group`` with both in ``causes``.
* **X-3** — an ``on_callee_error`` substitute resolves the faulting slot during the fold,
  so the batch *completes* and the model sees the substitute among the results.

X-1/X-2 read the escalated group on the agent's ``publish_topic`` mirror (Channel A);
X-3 reads the finalized output at the client edge (Channel C).

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker.
"""

from __future__ import annotations

from typing import Any

import pytest

from calfkit._protocol import HDR_ERROR_TYPE
from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.exceptions import NodeFaultError
from calfkit.models.error_report import ErrorReport, FaultTypes
from calfkit.models.seam_context import SeamContext
from calfkit.nodes import Agent
from tests.integration._fault_kafka import ensure_topic, fault_worker
from tests.integration._fault_tap import fault_tap
from tests.integration._fault_tools import boom, ok_a, ok_b
from tests.integration._roundtrip_helpers import FINAL_OUTPUT, scripted_model, tool_returns

pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True


def substitute_failed_callee(ctx: SeamContext[Any], fault: ErrorReport) -> str:
    """An ``on_callee_error`` that resolves any failed slot with a model-visible
    substitute (the slot-position substitute is exempt from output-type validation)."""
    return "boom recovered"


def reject_failed_callee(ctx: SeamContext[Any], fault: ErrorReport) -> str:
    """An ``on_callee_error`` that mints its own fault for the failing slot — a
    slot-scoped raise: the slot resolves failed carrying this error (the inbound fault
    chained as a cause), siblings continue, and the batch escalates at closure."""
    raise NodeFaultError("seam.reject", message="rejecting this slot")


def _fanout_agent(node_id: str, *, agent_in: str, agent_pub: str, calls: list[ToolCallPart], tools: list, **seams) -> Agent:
    """A fan-out-capable agent (durable store opens at worker start) that emits *calls* as
    one fan-out turn, then finalizes once it has acted."""
    return Agent(
        node_id,
        system_prompt="fan out the tools",
        subscribe_topics=agent_in,
        publish_topic=agent_pub,
        model_client=scripted_model(calls),
        tools=tools,
        **seams,
    )


async def test_one_unhandled_sibling_flattens_to_bare_fault(kafka_bootstrap: str, topic_namespace: str) -> None:
    """X-1: a fan-out of 3 with one faulting sibling and no ``on_callee_error`` → the
    batch closes failed with a *flattened* ``calf.exception`` fault carrying the
    partial-success topology (``ok``/``failed`` counts); the body never resumes."""
    agent_in = f"{topic_namespace}.x1.input"
    agent_pub = f"{topic_namespace}.x1.mirror"
    agent = _fanout_agent(
        f"{topic_namespace}-x1",
        agent_in=agent_in,
        agent_pub=agent_pub,
        calls=[
            ToolCallPart("boom", {"x": 1}, tool_call_id="c1"),
            ToolCallPart("ok_a", {}, tool_call_id="c2"),
            ToolCallPart("ok_b", {}, tool_call_id="c3"),
        ],
        tools=[boom, ok_a, ok_b],
    )
    await ensure_topic(kafka_bootstrap, agent_pub)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, boom, ok_a, ok_b])

    try:
        async with worker, fault_tap(kafka_bootstrap, agent_pub) as tap:
            await driver.agent(topic=agent_in).start("go")
            fault, headers = await tap.next_fault(timeout=90)

            # singleton flatten: the bare child, not a group
            assert fault.error.error_type == FaultTypes.EXCEPTION
            assert headers[HDR_ERROR_TYPE] == FaultTypes.EXCEPTION
            topology = fault.error.details[FaultTypes.FANOUT_TOPOLOGY]
            assert topology["ok"] == 2
            assert topology["failed"] == 1
    finally:
        await driver.aclose()
        await worker._client.aclose()


async def test_two_unhandled_siblings_compose_a_fault_group(kafka_bootstrap: str, topic_namespace: str) -> None:
    """X-2: a fan-out with two faulting siblings → a ``calf.fault_group`` carrying both
    in ``causes`` (``find`` matches the nested type at depth)."""
    agent_in = f"{topic_namespace}.x2.input"
    agent_pub = f"{topic_namespace}.x2.mirror"
    agent = _fanout_agent(
        f"{topic_namespace}-x2",
        agent_in=agent_in,
        agent_pub=agent_pub,
        calls=[
            ToolCallPart("boom", {"x": 1}, tool_call_id="c1"),
            ToolCallPart("boom", {"x": 2}, tool_call_id="c2"),
            ToolCallPart("ok_a", {}, tool_call_id="c3"),
        ],
        tools=[boom, ok_a],
    )
    await ensure_topic(kafka_bootstrap, agent_pub)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, boom, ok_a])

    try:
        async with worker, fault_tap(kafka_bootstrap, agent_pub) as tap:
            await driver.agent(topic=agent_in).start("go")
            fault, headers = await tap.next_fault(timeout=90)

            assert fault.error.error_type == FaultTypes.FAULT_GROUP
            assert headers[HDR_ERROR_TYPE] == FaultTypes.FAULT_GROUP
            assert len(fault.error.causes) == 2
            assert fault.error.find(FaultTypes.EXCEPTION) is not None  # matches at depth
            topology = fault.error.details[FaultTypes.FANOUT_TOPOLOGY]
            assert topology["failed"] == 2
            assert topology["ok"] == 1
    finally:
        await driver.aclose()
        await worker._client.aclose()


async def test_on_callee_error_substitute_completes_the_batch(kafka_bootstrap: str, topic_namespace: str) -> None:
    """X-3: an ``on_callee_error`` substitute resolves the faulting slot during the fold,
    so the batch completes, the model sees the substitute, and the agent finalizes."""
    agent_in = f"{topic_namespace}.x3.input"
    agent_pub = f"{topic_namespace}.x3.mirror"
    agent = _fanout_agent(
        f"{topic_namespace}-x3",
        agent_in=agent_in,
        agent_pub=agent_pub,
        calls=[
            ToolCallPart("boom", {"x": 1}, tool_call_id="c1"),
            ToolCallPart("ok_a", {}, tool_call_id="c2"),
            ToolCallPart("ok_b", {}, tool_call_id="c3"),
        ],
        tools=[boom, ok_a, ok_b],
        on_callee_error=substitute_failed_callee,
    )
    await ensure_topic(kafka_bootstrap, agent_pub)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, boom, ok_a, ok_b])

    try:
        async with worker:
            result = await driver.agent(topic=agent_in).execute("go", timeout=90)
            assert result.output is not None and FINAL_OUTPUT in result.output

            returns = tool_returns(result.message_history)
            assert returns["boom"] == "boom recovered"  # the substitute reached the model
            assert returns["ok_a"] == "a_result"
            assert returns["ok_b"] == "b_result"
    finally:
        await driver.aclose()
        await worker._client.aclose()


async def test_slot_scoped_seam_raise_fails_the_slot_and_escalates_at_closure(kafka_bootstrap: str, topic_namespace: str) -> None:
    """S-7: an ``on_callee_error`` that *raises* is slot-scoped — the slot resolves failed
    carrying the seam's mint (with the original fault chained), siblings continue, and the
    batch escalates that at closure (no node-own failure, no double reply)."""
    agent_in = f"{topic_namespace}.s7.input"
    agent_pub = f"{topic_namespace}.s7.mirror"
    agent = _fanout_agent(
        f"{topic_namespace}-s7",
        agent_in=agent_in,
        agent_pub=agent_pub,
        calls=[
            ToolCallPart("boom", {"x": 1}, tool_call_id="c1"),
            ToolCallPart("ok_a", {}, tool_call_id="c2"),
        ],
        tools=[boom, ok_a],
        on_callee_error=reject_failed_callee,
    )
    await ensure_topic(kafka_bootstrap, agent_pub)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, boom, ok_a])

    try:
        async with worker, fault_tap(kafka_bootstrap, agent_pub) as tap:
            await driver.agent(topic=agent_in).start("go")
            fault, _ = await tap.next_fault(timeout=90)

            assert fault.error.error_type == "seam.reject"  # the seam's slot-scoped mint
            assert fault.error.find(FaultTypes.EXCEPTION) is not None  # original boom fault, chained
            topology = fault.error.details[FaultTypes.FANOUT_TOPOLOGY]
            assert topology["failed"] == 1  # the boom slot
            assert topology["ok"] == 1  # ok_a still resolved
    finally:
        await driver.aclose()
        await worker._client.aclose()
