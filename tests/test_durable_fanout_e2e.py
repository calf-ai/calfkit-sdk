"""End-to-end durable fan-out (PR-4 6b-B): a real 2-tool fan-out driven through
``TestKafkaBroker`` with a fan-out store injected into the agent's resource bag.

This is the north-star for the staged-handler cutover. It proves the full durable arc:
OPEN (write the batch + publish marked siblings) → fold each sibling reply into the store →
self-publish the closure re-entry on completion → close (restore context from the snapshot,
materialize the outcomes) → resume the agent body to a final result.

Before the cutover the agent aggregates in-process (``_pending_batches``) and the injected
store is never touched — so the store-usage assertions are RED. After the cutover the fold is
durable and they pass.
"""

from __future__ import annotations

import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client
from calfkit.exceptions import DeserializationError
from calfkit.models.fanout import EnvelopeSnapshot, FanoutOpen, FanoutOutcome
from calfkit.models.tool_context import ToolContext
from calfkit.nodes import Agent, agent_tool
from calfkit.nodes._fanout_store import FANOUT_STORE_KEY
from calfkit.worker import Worker
from tests._fanout_fakes import FakeFanoutBatchStore
from tests.providers import prepare_worker


@agent_tool
def e2e_tool_a(ctx: ToolContext) -> str:
    return "a_result"


@agent_tool
def e2e_tool_b(ctx: ToolContext) -> str:
    return "b_result"


@agent_tool
def e2e_tool_boom(ctx: ToolContext) -> str:
    raise ValueError("e2e boom")


def _calls_two_then_done(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    """Fan out both tools on the first turn; once their returns are in history, finish."""
    last = messages[-1]
    if isinstance(last, ModelRequest) and any(isinstance(p, ToolReturnPart) for p in last.parts):
        return ModelResponse(parts=[TextPart("done")])
    return ModelResponse(parts=[ToolCallPart("e2e_tool_a"), ToolCallPart("e2e_tool_b")])


class _SpyStore(FakeFanoutBatchStore):
    """A FakeFanoutBatchStore that counts the durable operations, to prove the fold went
    through the store (not the in-process ``_pending_batches``)."""

    def __init__(self) -> None:
        super().__init__()
        self.opens = 0
        self.folds = 0
        self.tombstones = 0

    async def open(self, fanout_id: str, reg: FanoutOpen, snapshot: EnvelopeSnapshot) -> None:
        self.opens += 1
        await super().open(fanout_id, reg, snapshot)

    async def fold(self, fanout_id: str, outcome: FanoutOutcome):  # type: ignore[no-untyped-def]
        self.folds += 1
        return await super().fold(fanout_id, outcome)

    async def tombstone(self, fanout_id: str) -> None:
        self.tombstones += 1
        await super().tombstone(fanout_id)


async def test_durable_fanout_folds_in_store_and_closes_via_reentry(container) -> None:
    worker = container.get(Worker)
    agent = Agent(
        "durable_fanout_agent",
        system_prompt="x",
        subscribe_topics="durable_fanout_agent.input",
        model_client=FunctionModel(_calls_two_then_done),
        tools=[e2e_tool_a, e2e_tool_b],
    )
    spy = _SpyStore()
    # Offline injection: the node-owned @resource never runs under TestKafkaBroker, so the
    # fan-out agent just gets its store stuffed into the bag (the established pattern).
    agent.resources[FANOUT_STORE_KEY] = spy
    worker.add_nodes(agent, e2e_tool_a, e2e_tool_b)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.execute("call both tools", "durable_fanout_agent.input", timeout=10)

    # The batch was folded DURABLY in the store (not the in-process _pending_batches)...
    assert spy.opens == 1  # one OPEN for the 2-call batch
    assert spy.folds == 2  # both sibling replies folded through the store
    assert spy.tombstones >= 1  # closed (tombstone-first at the re-entry)
    # ...and the agent resumed past the durable close to its final result.
    assert result.output is not None and "done" in result.output


def _calls_ok_and_boom(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    """Fan out a succeeding + a failing tool on the first turn."""
    last = messages[-1]
    if isinstance(last, ModelRequest) and any(isinstance(p, ToolReturnPart) for p in last.parts):
        return ModelResponse(parts=[TextPart("done")])
    return ModelResponse(parts=[ToolCallPart("e2e_tool_a"), ToolCallPart("e2e_tool_boom")])


async def test_durable_fanout_with_failing_tool_escalates_a_fault_not_a_strand(container) -> None:
    # The carriage switch (4.4), end-to-end: a 2-tool fan-out where ONE tool raises NO LONGER strands.
    # The tool's exception escalates via the rail (it is no longer captured into a FailedToolCall); the
    # agent folds it as a FAILED slot, and at close the batch escalates a fault group to the caller
    # (production). Client RECEPTION as a typed NodeFaultError is the deferred reception PR (plan §0):
    # in PR-6 the fault REACHES the client and surfaces when the reply is projected — a
    # DeserializationError on the fault's empty parts — proving the caller is answered, not stranded.
    worker = container.get(Worker)
    agent = Agent(
        "durable_fanout_fail_agent",
        system_prompt="x",
        subscribe_topics="durable_fanout_fail_agent.input",
        model_client=FunctionModel(_calls_ok_and_boom),
        tools=[e2e_tool_a, e2e_tool_boom],
    )
    spy = _SpyStore()
    agent.resources[FANOUT_STORE_KEY] = spy
    worker.add_nodes(agent, e2e_tool_a, e2e_tool_boom)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    with pytest.raises(DeserializationError):  # PR-6: the fault reaches the client; clean typed reception is deferred
        async with TestKafkaBroker(broker):
            await client.execute("call both tools", "durable_fanout_fail_agent.input", timeout=10)

    # The failure travelled the DURABLE path: both siblings folded through the store before close.
    assert spy.opens == 1
    assert spy.folds == 2
    assert spy.tombstones >= 1
