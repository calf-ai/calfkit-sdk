"""Suite Z — fan-out stress / soak over the real durable store.

Pushes the in-node durable fold past the offline lane's 2-3 siblings:

* **Z-1** — a large-N fan-out (8 siblings) folds and completes; the model sees all N
  results.
* **Z-3** — M concurrent invocations of one fan-out agent each open their own durable
  batch (keyed by their own ``fanout_id``); under ``max_workers=1`` they fold serially
  and all complete with their own result set (no cross-batch bleed, no deadlock).
* **Z-4** — a healthy and a faulting batch run concurrently in one worker: the healthy
  one completes, the faulting one escalates — no strand, no cross-contamination.

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker.
"""

from __future__ import annotations

import asyncio

import pytest

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.exceptions import DeserializationError
from calfkit.nodes import Agent
from tests.integration._fault_kafka import fault_worker
from tests.integration._fault_tools import boom, ok_a, ok_b
from tests.integration._roundtrip_helpers import FINAL_OUTPUT, returns_by_call_id, scripted_model

pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True


def _fanout_agent(node_id: str, *, agent_in: str, calls: list[ToolCallPart], tools: list, **seams) -> Agent:
    return Agent(
        node_id,
        system_prompt="fan out the tools",
        subscribe_topics=agent_in,
        model_client=scripted_model(calls),
        tools=tools,
        **seams,
    )


async def test_large_fanout_folds_and_completes(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Z-1: an 8-sibling fan-out folds in the durable store and completes; the model's
    resumed turn sees all 8 results (one per slot, keyed by call id)."""
    n_each = 4
    calls = [ToolCallPart("ok_a", {}, tool_call_id=f"a{i}") for i in range(n_each)]
    calls += [ToolCallPart("ok_b", {}, tool_call_id=f"b{i}") for i in range(n_each)]
    agent_in = f"{topic_namespace}.z1.input"
    agent = _fanout_agent(f"{topic_namespace}-z1", agent_in=agent_in, calls=calls, tools=[ok_a, ok_b])
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, ok_a, ok_b])

    try:
        async with worker:
            result = await driver.agent(topic=agent_in).execute("go", timeout=120)
            assert result.output is not None and FINAL_OUTPUT in result.output
            by_id = returns_by_call_id(result.message_history)
            assert len(by_id) == 2 * n_each  # every slot resolved into its own result
    finally:
        await driver.aclose()
        await worker._client.aclose()


async def test_concurrent_batches_all_complete(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Z-3: M concurrent invocations of one fan-out agent each fold their own durable
    batch serially (``max_workers=1``) and all complete with their own 2 results — no
    cross-batch bleed, no deadlock under load."""
    m = 4
    calls = [ToolCallPart("ok_a", {}, tool_call_id="c1"), ToolCallPart("ok_b", {}, tool_call_id="c2")]
    agent_in = f"{topic_namespace}.z3.input"
    agent = _fanout_agent(f"{topic_namespace}-z3", agent_in=agent_in, calls=calls, tools=[ok_a, ok_b])
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, ok_a, ok_b])

    try:
        async with worker:
            handles = [await driver.agent(topic=agent_in).start("go") for _ in range(m)]
            results = await asyncio.gather(*(handle.result(timeout=120) for handle in handles))
        for result in results:
            assert result.output is not None and FINAL_OUTPUT in result.output
            assert len(returns_by_call_id(result.message_history)) == 2
    finally:
        await driver.aclose()
        await worker._client.aclose()


async def test_mixed_success_and_fault_batches_are_isolated(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Z-4: a healthy fan-out and a faulting fan-out run concurrently in one worker — the
    healthy batch completes, the faulting batch escalates; neither contaminates the other."""
    healthy_in = f"{topic_namespace}.z4ok.input"
    faulty_in = f"{topic_namespace}.z4bad.input"
    healthy = _fanout_agent(
        f"{topic_namespace}-z4ok",
        agent_in=healthy_in,
        calls=[ToolCallPart("ok_a", {}, tool_call_id="c1"), ToolCallPart("ok_b", {}, tool_call_id="c2")],
        tools=[ok_a, ok_b],
    )
    faulting = _fanout_agent(
        f"{topic_namespace}-z4bad",
        agent_in=faulty_in,
        calls=[ToolCallPart("boom", {"x": 1}, tool_call_id="c1"), ToolCallPart("ok_a", {}, tool_call_id="c2")],
        tools=[boom, ok_a],
    )
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[healthy, faulting, ok_a, ok_b, boom])

    try:
        async with worker:
            ok_handle = await driver.agent(topic=healthy_in).start("go")
            bad_handle = await driver.agent(topic=faulty_in).start("go")

            ok_result = await ok_handle.result(timeout=120)
            assert ok_result.output is not None and FINAL_OUTPUT in ok_result.output

            # The faulting batch escalated (a routed fault surfaces today as a
            # DeserializationError at the client edge — reception deferred, #250).
            with pytest.raises(DeserializationError):
                await bad_handle.result(timeout=120)
    finally:
        await driver.aclose()
        await worker._client.aclose()
