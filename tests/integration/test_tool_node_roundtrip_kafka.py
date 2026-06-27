"""Real-broker (``kafka`` lane) end-to-end tool-node tool-call roundtrips.

The tool-node counterpart to ``test_mcp_roundtrip_kafka.py``: a real broker + an
offline ``FunctionModel`` + plain ``@agent_tool`` tool nodes, proving tool calls
round-trip the whole distributed loop —

    the model emits a tool call
      -> the agent dispatches a ToolCallRef over Kafka to the tool node's topic
      -> the tool node runs the Python function
      -> the return value comes back over Kafka
      -> the agent re-enters the model loop and finalizes.

Unlike the MCP suite there is no discovery, capability view, or server
subprocess: a ``ToolNodeDef`` is registered statically (`tools=[...]`), so the
agent already knows each tool's dispatch topic. The tool node wraps its return
in a ``ToolReturn(return_value=...)``, so the value lands in history as a
``ToolReturnPart`` whose ``content`` is the raw Python value — assertions are
exact equality (not substring on a CallToolResult like MCP).

Existing real-broker tool-node coverage was only the durable fan-out test
(`test_durable_fanout_agent_kafka.py`) — concurrent, no-arg, constant-returning
tools, as a vehicle for the fan-out store. This module covers the gaps: single
dispatch, argument and structured-return wire fidelity, sequential mode,
multi-turn data-flow, cross-worker and multi-agent routing, and the agent's
pre-dispatch arg validation.

Fault paths (a tool raising -> FailedToolCall -> escalation) are intentionally
left to the fault-rail work (#247), where faults travel back as a typed result
rather than stranding until timeout.

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker.
"""

from __future__ import annotations

import pytest
from aiokafka.admin import AIOKafkaAdminClient

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.nodes import Agent, agent_tool
from calfkit.worker import Worker
from tests.integration._roundtrip_helpers import (
    FINAL_OUTPUT,
    reactive_model,
    retry_prompt_texts,
    returns_by_call_id,
    scripted_model,
    tool_returns,
)

# Every test here needs a real broker. FunctionModel is offline, but pydantic-ai
# still gates "model requests" behind this flag, so set it (matches
# tests/integration/test_durable_fanout_agent_kafka.py).
pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True

_EARLIEST = {"auto_offset_reset": "earliest"}


# ── tool nodes (deterministic; output is a pure function of input) ────────────


@agent_tool
def add(a: int, b: int) -> int:
    return a + b


@agent_tool
def mul(a: int, b: int) -> int:
    return a * b


@agent_tool
def shout(text: str) -> str:
    return text.upper()


@agent_tool
def scale(value: float, factor: float) -> float:
    return value * factor


@agent_tool
def stats(numbers: list[int]) -> dict:
    return {"sum": sum(numbers), "count": len(numbers), "max": max(numbers)}


@agent_tool
def motd() -> str:
    return "calfkit-rocks"


# ── builders / utilities ─────────────────────────────────────────────────────


def _worker(bootstrap: str, *, nodes: list) -> Worker:
    """A Worker on its own broker connection, reading earliest.

    Tool nodes need no MCP discovery. ``earliest`` is mandatory so a node's
    consumer-group join never races (and drops) the publish addressing it on a
    freshly-auto-created partition.
    """
    return Worker(Client.connect(bootstrap), nodes=nodes, extra_subscribe_kwargs=_EARLIEST)


async def _topics(bootstrap: str) -> set[str]:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        return set(await admin.list_topics())
    finally:
        await admin.close()


# ── Group A: single dispatch + arg/return fidelity ───────────────────────────


async def test_single_tool_node_roundtrips_with_args(kafka_bootstrap: str, topic_namespace: str) -> None:
    """One tool call (the single-dispatch path, not fan-out): args reach the
    tool node and the computed result returns over the wire."""
    agent_id = f"{topic_namespace}-tn-add"
    agent_in = f"{topic_namespace}.tn-add.input"
    agent = Agent(
        agent_id,
        system_prompt="add two numbers",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart("add", {"a": 2, "b": 3}, tool_call_id="c1")]),
        tools=[add],
    )
    driver = Client.connect(kafka_bootstrap)
    worker = _worker(kafka_bootstrap, nodes=[agent, add])

    async with worker:
        result = await driver.agent(topic=agent_in).execute("add 2 and 3", timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    assert tool_returns(result.message_history)["add"] == 5

    await driver.aclose()
    await worker._client.aclose()


@pytest.mark.parametrize(
    ("tool_name", "args", "expected"),
    [
        ("shout", {"text": "hi"}, "HI"),  # str arg + str return
        ("scale", {"value": 2.5, "factor": 4.0}, 10.0),  # float arg + float return
    ],
)
async def test_scalar_arg_and_return_types_round_trip(
    kafka_bootstrap: str, topic_namespace: str, tool_name: str, args: dict, expected: object
) -> None:
    """str and float args/returns survive the broker round-trip with exact value."""
    agent_id = f"{topic_namespace}-tn-{tool_name}"
    agent_in = f"{topic_namespace}.tn-{tool_name}.input"
    agent = Agent(
        agent_id,
        system_prompt="call a tool",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart(tool_name, args, tool_call_id="c1")]),
        tools=[shout, scale],
    )
    driver = Client.connect(kafka_bootstrap)
    worker = _worker(kafka_bootstrap, nodes=[agent, shout, scale])

    async with worker:
        result = await driver.agent(topic=agent_in).execute(f"call {tool_name}", timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    assert tool_returns(result.message_history)[tool_name] == expected

    await driver.aclose()
    await worker._client.aclose()


async def test_structured_list_arg_and_dict_return_round_trip(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A list argument in and a dict return out both survive serialization."""
    agent_id = f"{topic_namespace}-tn-stats"
    agent_in = f"{topic_namespace}.tn-stats.input"
    agent = Agent(
        agent_id,
        system_prompt="summarize numbers",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart("stats", {"numbers": [1, 2, 3, 4]}, tool_call_id="c1")]),
        tools=[stats],
    )
    driver = Client.connect(kafka_bootstrap)
    worker = _worker(kafka_bootstrap, nodes=[agent, stats])

    async with worker:
        result = await driver.agent(topic=agent_in).execute("summarize 1..4", timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    assert tool_returns(result.message_history)["stats"] == {"sum": 10, "count": 4, "max": 4}

    await driver.aclose()
    await worker._client.aclose()


async def test_no_arg_tool_node_roundtrips(kafka_bootstrap: str, topic_namespace: str) -> None:
    """The no-argument dispatch path: a constant-returning tool round-trips."""
    agent_id = f"{topic_namespace}-tn-motd"
    agent_in = f"{topic_namespace}.tn-motd.input"
    agent = Agent(
        agent_id,
        system_prompt="get the motd",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart("motd", {}, tool_call_id="c1")]),
        tools=[motd],
    )
    driver = Client.connect(kafka_bootstrap)
    worker = _worker(kafka_bootstrap, nodes=[agent, motd])

    async with worker:
        result = await driver.agent(topic=agent_in).execute("motd", timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    assert tool_returns(result.message_history)["motd"] == "calfkit-rocks"

    await driver.aclose()
    await worker._client.aclose()


# ── Group B: concurrency / topology ──────────────────────────────────────────


async def test_concurrent_tool_nodes_roundtrip_via_fanout_with_args(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Two distinct tools with args in one turn drive the durable fan-out path;
    both results round-trip (the existing fan-out test uses only no-arg tools)."""
    agent_id = f"{topic_namespace}-tn-fanout"
    agent_in = f"{topic_namespace}.tn-fanout.input"
    agent = Agent(
        agent_id,
        system_prompt="add and multiply",
        subscribe_topics=agent_in,
        model_client=scripted_model(
            [
                ToolCallPart("add", {"a": 2, "b": 3}, tool_call_id="c-add"),
                ToolCallPart("mul", {"a": 4, "b": 5}, tool_call_id="c-mul"),
            ]
        ),
        tools=[add, mul],
    )
    assert agent._is_fanout_capable

    driver = Client.connect(kafka_bootstrap)
    worker = _worker(kafka_bootstrap, nodes=[agent, add, mul])

    async with worker:
        result = await driver.agent(topic=agent_in).execute("add 2+3 and mul 4*5", timeout=120)
        topics = await _topics(kafka_bootstrap)

    assert result.output is not None and FINAL_OUTPUT in result.output
    returns = tool_returns(result.message_history)
    assert returns["add"] == 5 and returns["mul"] == 20
    # The durable fan-out store was opened over the real broker.
    assert f"calf.fanout.{agent_id}.state" in topics
    assert f"calf.fanout.{agent_id}.basestate" in topics

    await driver.aclose()
    await worker._client.aclose()


async def test_duplicate_tool_node_concurrent_slots_route_by_call_id(kafka_bootstrap: str, topic_namespace: str) -> None:
    """The same tool called twice in one turn: slots are keyed by tool_call_id,
    so both results return to their own slot."""
    agent_id = f"{topic_namespace}-tn-dup"
    agent_in = f"{topic_namespace}.tn-dup.input"
    agent = Agent(
        agent_id,
        system_prompt="add two pairs",
        subscribe_topics=agent_in,
        model_client=scripted_model(
            [
                ToolCallPart("add", {"a": 2, "b": 3}, tool_call_id="call-a"),
                ToolCallPart("add", {"a": 10, "b": 20}, tool_call_id="call-b"),
            ]
        ),
        tools=[add],
    )
    driver = Client.connect(kafka_bootstrap)
    worker = _worker(kafka_bootstrap, nodes=[agent, add])

    async with worker:
        result = await driver.agent(topic=agent_in).execute("add 2+3 and 10+20", timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    by_id = returns_by_call_id(result.message_history)
    assert by_id["call-a"] == 5
    assert by_id["call-b"] == 30

    await driver.aclose()
    await worker._client.aclose()


async def test_tool_nodes_in_separate_workers_route_correctly(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Two tool nodes hosted in two different workers; a fan-out turn routes each
    call to the correct node's topic/worker."""
    agent_id = f"{topic_namespace}-tn-multiworker"
    agent_in = f"{topic_namespace}.tn-multiworker.input"
    agent = Agent(
        agent_id,
        system_prompt="add and multiply",
        subscribe_topics=agent_in,
        model_client=scripted_model(
            [
                ToolCallPart("add", {"a": 2, "b": 3}, tool_call_id="c-add"),
                ToolCallPart("mul", {"a": 4, "b": 5}, tool_call_id="c-mul"),
            ]
        ),
        tools=[add, mul],
    )
    driver = Client.connect(kafka_bootstrap)
    add_worker = _worker(kafka_bootstrap, nodes=[add])
    mul_worker = _worker(kafka_bootstrap, nodes=[mul])
    agent_worker = _worker(kafka_bootstrap, nodes=[agent])

    async with add_worker, mul_worker, agent_worker:
        result = await driver.agent(topic=agent_in).execute("add and mul", timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    returns = tool_returns(result.message_history)
    assert returns["add"] == 5  # serviced by add_worker
    assert returns["mul"] == 20  # serviced by mul_worker

    await driver.aclose()
    await add_worker._client.aclose()
    await mul_worker._client.aclose()
    await agent_worker._client.aclose()


async def test_two_agents_share_one_tool_node_replies_route_per_caller(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Two agents call the same tool node concurrently; replies route per call
    frame to each agent's own return topic — no cross-delivery."""
    a1_id = f"{topic_namespace}-tn-agent-1"
    a1_in = f"{topic_namespace}.tn-agent-1.input"
    a2_id = f"{topic_namespace}-tn-agent-2"
    a2_in = f"{topic_namespace}.tn-agent-2.input"
    agent1 = Agent(
        a1_id,
        system_prompt="add",
        subscribe_topics=a1_in,
        model_client=scripted_model([ToolCallPart("add", {"a": 2, "b": 3}, tool_call_id="c1")]),
        tools=[add],
    )
    agent2 = Agent(
        a2_id,
        system_prompt="add",
        subscribe_topics=a2_in,
        model_client=scripted_model([ToolCallPart("add", {"a": 10, "b": 20}, tool_call_id="c2")]),
        tools=[add],
    )
    driver = Client.connect(kafka_bootstrap)
    tool_worker = _worker(kafka_bootstrap, nodes=[add])
    agent_worker = _worker(kafka_bootstrap, nodes=[agent1, agent2])

    async with tool_worker, agent_worker:
        h1 = await driver.agent(topic=a1_in).start("agent 1 add 2+3")
        h2 = await driver.agent(topic=a2_in).start("agent 2 add 10+20")
        r1 = await h1.result(timeout=120)
        r2 = await h2.result(timeout=120)

    assert r1.output is not None and FINAL_OUTPUT in r1.output
    assert r2.output is not None and FINAL_OUTPUT in r2.output
    # Each caller got ITS OWN result back, not the other's.
    assert tool_returns(r1.message_history)["add"] == 5
    assert tool_returns(r2.message_history)["add"] == 30

    await driver.aclose()
    await tool_worker._client.aclose()
    await agent_worker._client.aclose()


# ── Group C: modes / multi-turn ──────────────────────────────────────────────


async def test_sequential_mode_dispatches_tool_nodes_without_fanout(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A sequential-only agent emitting two calls dispatches them one per turn
    and never opens the durable fan-out store, yet both results round-trip."""
    agent_id = f"{topic_namespace}-tn-seq"
    agent_in = f"{topic_namespace}.tn-seq.input"
    agent = Agent(
        agent_id,
        system_prompt="add then shout",
        subscribe_topics=agent_in,
        model_client=scripted_model(
            [
                ToolCallPart("add", {"a": 2, "b": 3}, tool_call_id="c-add"),
                ToolCallPart("shout", {"text": "hi"}, tool_call_id="c-shout"),
            ]
        ),
        tools=[add, shout],
        sequential_only_mode=True,
    )
    assert not agent._is_fanout_capable

    driver = Client.connect(kafka_bootstrap)
    worker = _worker(kafka_bootstrap, nodes=[agent, add, shout])

    async with worker:
        result = await driver.agent(topic=agent_in).execute("add then shout", timeout=120)
        topics = await _topics(kafka_bootstrap)

    assert result.output is not None and FINAL_OUTPUT in result.output
    returns = tool_returns(result.message_history)
    assert returns["add"] == 5
    assert returns["shout"] == "HI"
    # No durable store was opened — the sequential path never fans out.
    assert f"calf.fanout.{agent_id}.state" not in topics
    assert f"calf.fanout.{agent_id}.basestate" not in topics

    await driver.aclose()
    await worker._client.aclose()


async def test_multi_turn_tool_use_feeds_result_into_next_call(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Multi-turn over real Kafka re-entries: turn 1 calls add(2,3); turn 2 uses
    that result (5) as input to mul(5,4); turn 3 finalizes. Proves a
    round-tripped value drives a subsequent dispatch."""
    agent_id = f"{topic_namespace}-tn-multiturn"
    agent_in = f"{topic_namespace}.tn-multiturn.input"

    def _decide(returns: dict[str, object]) -> list[ToolCallPart] | None:
        if "mul" in returns:
            return None  # both done -> finalize
        if "add" in returns:
            return [ToolCallPart("mul", {"a": returns["add"], "b": 4}, tool_call_id="c-mul")]
        return [ToolCallPart("add", {"a": 2, "b": 3}, tool_call_id="c-add")]

    agent = Agent(
        agent_id,
        system_prompt="add then multiply the result",
        subscribe_topics=agent_in,
        model_client=reactive_model(_decide),
        tools=[add, mul],
    )
    driver = Client.connect(kafka_bootstrap)
    worker = _worker(kafka_bootstrap, nodes=[agent, add, mul])

    async with worker:
        result = await driver.agent(topic=agent_in).execute("add then multiply", timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    returns = tool_returns(result.message_history)
    assert returns["add"] == 5
    assert returns["mul"] == 20  # mul(5, 4): the add result fed the second call

    await driver.aclose()
    await worker._client.aclose()


# ── Group D: agent-side guard ─────────────────────────────────────────────────


async def test_invalid_tool_node_args_rejected_before_dispatch(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Tool-node bindings carry a validator, so the agent rejects schema-invalid
    args BEFORE dispatch: the tool function never runs, the model is handed the
    validation error (not a result), and the turn finalizes."""
    agent_id = f"{topic_namespace}-tn-badargs"
    agent_in = f"{topic_namespace}.tn-badargs.input"
    agent = Agent(
        agent_id,
        system_prompt="add with bad args",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart("add", {"a": "not-an-int", "b": 3}, tool_call_id="c1")]),
        tools=[add],
    )
    driver = Client.connect(kafka_bootstrap)
    worker = _worker(kafka_bootstrap, nodes=[agent, add])

    async with worker:
        result = await driver.agent(topic=agent_in).execute("add with bad args", timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    history = result.message_history
    # The tool function never ran: its real result (5) never came back.
    assert tool_returns(history).get("add") != 5
    # Instead the agent surfaced a schema-validation error for the bad call
    # (it may land as a tool-return-style entry or a retry prompt depending on
    # the path; assert against both).
    surfaced = f"{tool_returns(history).get('add')} {' '.join(retry_prompt_texts(history))}"
    assert "int_parsing" in surfaced or "valid integer" in surfaced.lower()

    await driver.aclose()
    await worker._client.aclose()
