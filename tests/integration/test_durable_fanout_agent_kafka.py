"""Real-broker (``kafka`` lane) agent-level test for PR-4's durable in-node fan-out.

This is the over-the-wire counterpart to the offline arc test
(``tests/test_durable_fanout_e2e.py``). The offline test drives the full
OPEN -> fold -> re-entry -> close -> resume arc through the synchronous
``TestKafkaBroker`` with a **fake** store stuffed into the agent's resource bag,
because the node-owned ``@resource`` never runs under ``TestKafkaBroker``.

What that offline test CANNOT prove — and what this one does — is the one piece
that only exists under a real lifecycle against a live broker:

* the fan-out agent's node-owned ``@resource`` actually OPENS the real
  :class:`~calfkit.nodes._fanout_store.KtablesFanoutBatchStore` (its four
  compacted ktables) when a real :class:`~calfkit.worker.Worker` boots, and
* the durable fold + the self-published re-entry close the batch over the wire,
  so the agent resumes to a final result.

Crucially this test injects **no** store: the agent is a plain non-sequential
:class:`~calfkit.nodes.Agent`, which auto-registers its ``@resource`` in
``__init__``; the worker's resource phase opens the real ktables-backed store
before serving. An offline ``FunctionModel`` removes any LLM dependency — only
the broker is real.

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker. Run
with ``uv run --group integration pytest ... -m kafka``.
"""

from __future__ import annotations

import asyncio

import pytest
from aiokafka.admin import AIOKafkaAdminClient

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client
from calfkit.nodes import Agent, agent_tool
from calfkit.nodes._fanout_store import FANOUT_STORE_KEY
from calfkit.worker import Worker

# Every test here needs a real broker. FunctionModel is an offline model, but
# pydantic-ai still gates "model requests" behind this flag in some paths; set
# it so the agent loop runs without an allow-list error (matches
# tests/integration/test_agent_workers.py).
pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True


# ── the fan-out agent's two tools (mirror tests/test_durable_fanout_e2e.py) ──────


@agent_tool
def fanout_tool_a() -> str:
    return "a_result"


@agent_tool
def fanout_tool_b() -> str:
    return "b_result"


def _calls_two_then_done(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    """Fan out both tools on the first turn; once their returns are in history, finish.

    The marker that the durable arc actually ran is the second turn: it only
    happens after BOTH sibling tool returns have been folded into the durable
    batch, the re-entry has closed it, and the snapshot's materialized outcomes
    have been replayed into ``message_history`` — i.e. the model sees the tool
    returns it never would on a single-call (non-fan-out) path.
    """
    last = messages[-1]
    if isinstance(last, ModelRequest) and any(isinstance(p, ToolReturnPart) for p in last.parts):
        return ModelResponse(parts=[TextPart("done")])
    return ModelResponse(parts=[ToolCallPart("fanout_tool_a"), ToolCallPart("fanout_tool_b")])


def _tool_returns(history: list[ModelMessage]) -> dict[str, object]:
    """Map ``tool_name -> return_value`` over every ToolReturnPart in the history."""
    returns: dict[str, object] = {}
    for msg in history:
        if isinstance(msg, ModelRequest):
            for part in msg.parts:
                if isinstance(part, ToolReturnPart):
                    returns[part.tool_name] = part.content
    return returns


async def _topics(bootstrap: str) -> set[str]:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        return set(await admin.list_topics())
    finally:
        await admin.close()


async def test_fanout_agent_opens_real_store_and_resumes_over_the_wire(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A real Worker hosts a fan-out agent against live Redpanda; the node-owned
    ``@resource`` opens the REAL ktables store; the agent fans out two tools and
    the durable fold + self-published re-entry close the batch so the agent
    resumes to a final result — no fake store injected anywhere.
    """
    agent_id = f"{topic_namespace}-fanout-agent"
    agent_in = f"{topic_namespace}.fanout-agent.input"

    agent = Agent(
        agent_id,
        system_prompt="x",
        subscribe_topics=agent_in,
        model_client=FunctionModel(_calls_two_then_done),
        tools=[fanout_tool_a, fanout_tool_b],
    )

    # A non-sequential agent auto-registers its durable-store @resource in
    # __init__; the worker lifecycle opens the REAL KtablesFanoutBatchStore.
    # Guard the premise of this test: nothing pre-seeds the bag (the offline
    # fake-injection pattern must NOT be in play here).
    assert agent._is_fanout_capable
    assert FANOUT_STORE_KEY not in agent.resources

    client = Client.connect(kafka_bootstrap)
    worker = Worker(
        client,
        nodes=[agent, fanout_tool_a, fanout_tool_b],
        # Read from earliest so a node's consumer-group join never races (and
        # drops) the publish that addresses it — the agent's input call, the
        # fanned-out sibling calls, the tool returns, and the self-published
        # re-entry all flow before their consumers may have positioned
        # themselves on a freshly-auto-created partition.
        extra_subscribe_kwargs={"auto_offset_reset": "earliest"},
    )

    # ``async with worker`` runs the full lifecycle on the shared broker: the
    # resource phase opens the agent's ktables store (the thing under test),
    # then the broker serves both the hosted nodes and the client's reply
    # consumer (registered on the same connection by Client.connect).
    async with worker:
        # The agent's @resource opening the store created its compacted tables;
        # this is the over-the-wire proof the real KtablesFanoutBatchStore
        # opened (the offline fake-store test cannot make this assertion).
        topics = await _topics(kafka_bootstrap)
        assert f"calf.fanout.{agent_id}.state" in topics
        assert f"calf.fanout.{agent_id}.basestate" in topics

        result = await client.agent(topic=agent_in).execute("call both tools", timeout=120)

    # The agent resumed past the durable close to its final result.
    assert result.output is not None and "done" in result.output

    # Both sibling returns were folded durably and materialized back into the
    # resumed body's history — the marker that the OPEN -> fold -> re-entry ->
    # close -> resume arc ran over the wire (not a single-call shortcut).
    returns = _tool_returns(result.message_history)
    assert returns.get("fanout_tool_a") == "a_result"
    assert returns.get("fanout_tool_b") == "b_result"

    # The batch closed exactly once: the model was asked exactly twice (turn 1
    # fans out; turn 2 sees both returns and finishes). A double-close or a
    # re-fanned batch would show extra turns.
    model_turns = [m for m in result.message_history if isinstance(m, ModelResponse)]
    assert len(model_turns) == 2
    fanned = [p for p in model_turns[0].parts if isinstance(p, ToolCallPart)]
    assert {p.tool_name for p in fanned} == {"fanout_tool_a", "fanout_tool_b"}

    await client.aclose()


# ─────────────────────────────────────────────────────────────────────────────
# Secondary goal: graceful-rebalance continuation across two workers.
#
# ADR-0008 claims the durable design's core availability property: "a new owner
# reads the durable state and continues", and the self-published re-entry "keeps
# the resume on the normal delivery path ... so it survives a rebalance between
# completion and resume". This test proves it directly: the original owner opens
# the durable batch and then leaves the group BEFORE any sibling reply is folded;
# a fresh owner (same consumer group, same node id) takes over, folds both
# replies from the durable tables, and closes the batch exactly once.
#
# Determinism (no timing races): the two tools BLOCK on a shared gate, so their
# replies cannot land until the test releases them — strictly after the first
# owner has stopped. The first owner is fully torn down before the gate opens, so
# only the second owner can possibly fold/close. A separate, always-up driver
# client owns the reply future; each worker has its own broker connection, so
# stopping the first owner never disturbs the driver or the second owner. The
# first owner's graceful stop commits its input offset (verified behavior), so
# the second owner — reading ``earliest`` to catch the replies produced while it
# was down — never re-reads the input and never re-OPENs the batch.
# ─────────────────────────────────────────────────────────────────────────────


_GATE = asyncio.Event()
_TOOL_ENTRIES = 0


@agent_tool
async def gated_tool_a() -> str:
    """A fan-out sibling that records its entry, then blocks until the test
    releases the gate — keeping the batch OPEN across the rebalance."""
    global _TOOL_ENTRIES
    _TOOL_ENTRIES += 1
    await _GATE.wait()
    return "a_result"


@agent_tool
async def gated_tool_b() -> str:
    global _TOOL_ENTRIES
    _TOOL_ENTRIES += 1
    await _GATE.wait()
    return "b_result"


def _calls_two_gated_then_done(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    last = messages[-1]
    if isinstance(last, ModelRequest) and any(isinstance(p, ToolReturnPart) for p in last.parts):
        return ModelResponse(parts=[TextPart("done")])
    return ModelResponse(parts=[ToolCallPart("gated_tool_a"), ToolCallPart("gated_tool_b")])


def _make_fanout_agent(node_id: str, agent_in: str) -> Agent:
    """A fresh fan-out agent instance bound to a shared ``node_id`` (so two
    instances in two workers share one consumer group + one durable store)."""
    return Agent(
        node_id,
        system_prompt="x",
        subscribe_topics=agent_in,
        model_client=FunctionModel(_calls_two_gated_then_done),
        tools=[gated_tool_a, gated_tool_b],
    )


async def _wait(predicate, *, timeout: float, what: str) -> None:
    """Poll ``predicate`` until true or fail loudly — no bare sleeps."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"timed out after {timeout}s waiting for: {what}")


async def test_graceful_rebalance_continues_batch_on_new_owner(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Owner-1 OPENs a durable fan-out batch then leaves the group with the batch
    still open; owner-2 (same group + node id) folds both replies from the
    durable tables and closes the batch exactly once, resuming to a final result.
    """
    global _TOOL_ENTRIES
    _GATE.clear()
    _TOOL_ENTRIES = 0

    agent_id = f"{topic_namespace}-rebal-agent"
    agent_in = f"{topic_namespace}.rebal-agent.input"
    earliest = {"auto_offset_reset": "earliest"}

    # Independent broker connections (all -> same Redpanda):
    #  - driver: owns the reply future; stays up the whole test.
    #  - tools worker: hosts the two gated tools; stays up.
    #  - owner-1 / owner-2: each hosts a fan-out agent on the SHARED node id.
    driver = Client.connect(kafka_bootstrap)
    tools_worker = Worker(
        Client.connect(kafka_bootstrap),
        nodes=[gated_tool_a, gated_tool_b],
        extra_subscribe_kwargs=earliest,
    )
    owner1 = Worker(Client.connect(kafka_bootstrap), nodes=[_make_fanout_agent(agent_id, agent_in)], extra_subscribe_kwargs=earliest)
    owner2 = Worker(Client.connect(kafka_bootstrap), nodes=[_make_fanout_agent(agent_id, agent_in)], extra_subscribe_kwargs=earliest)

    # The tools worker stays up across BOTH owners — the blocked siblings must
    # survive owner-1's departure so their replies can be released to owner-2.
    async with tools_worker:
        async with owner1:
            # Kick off the invocation; owner-1 OPENs the durable batch and fans
            # out the two (now-blocked) siblings. Don't await the reply yet.
            handle = await driver.agent(topic=agent_in).start("call both tools")

            # Both siblings reached the tools => owner-1's OPEN ran, the durable
            # batch is written, and the siblings are published and parked on the
            # gate. The batch CANNOT close on owner-1: no reply has folded.
            await _wait(lambda: _TOOL_ENTRIES == 2, timeout=60, what="both fan-out siblings to reach the tools")

            topics = await _topics(kafka_bootstrap)
            assert f"calf.fanout.{agent_id}.state" in topics and f"calf.fanout.{agent_id}.basestate" in topics

        # Owner-1 has gracefully left the group (committing its input offset).
        # The batch is durably OPEN with zero folds; only a new owner finishes it.
        async with owner2:
            # Release the siblings AFTER owner-1 is gone: their replies land on
            # the agent's return inbox while owner-2 owns it. Owner-2 reads them
            # from earliest, folds both from the durable state, completes,
            # self-publishes the re-entry, and closes from the durable
            # basestate — the rebalance continuation.
            _GATE.set()
            result = await handle.result(timeout=120)

    assert result.output is not None and "done" in result.output
    returns = _tool_returns(result.message_history)
    assert returns.get("gated_tool_a") == "a_result"
    assert returns.get("gated_tool_b") == "b_result"
    # Exactly-once close: the batch fanned out once and finished once.
    model_turns = [m for m in result.message_history if isinstance(m, ModelResponse)]
    assert len(model_turns) == 2

    await driver.aclose()
    await tools_worker._client.aclose()
    await owner1._client.aclose()
    await owner2._client.aclose()
