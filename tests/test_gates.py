"""End-to-end tests for gate stack on BaseNodeDef.

Tests follow the project pattern: TestKafkaBroker for in-memory broker,
FunctionModel for offline agent runs, client.execute_node / invoke_node for
real message dispatch. No broker mocking.
"""

import asyncio
import logging

import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit.client import Client
from calfkit.models import SessionRunContext
from calfkit.nodes import ToolNodeDef
from calfkit.worker import Worker
from tests.providers import NO_TOOLS_RESPONSE_TEXT, prepare_worker

GATED_AGENT_TOPIC = "test_gated_agent.input"


# ---------------------------------------------------------------------------
# Acceptance cases (run() executes, agent replies)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("gates_arg", [None, []], ids=["no-gates", "empty-list"])
async def test_no_gates_passes_through(container, deploy_gated_function_agent, gates_arg):
    deploy_gated_function_agent(gates=gates_arg)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.execute_node("hi", GATED_AGENT_TOPIC, timeout=5)

        assert result.output == NO_TOOLS_RESPONSE_TEXT


async def test_single_sync_gate_true_allows_run(container, deploy_gated_function_agent):
    calls: list[str] = []

    def g1(ctx: SessionRunContext) -> bool:
        calls.append("g1")
        return True

    deploy_gated_function_agent(gates=[g1])
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.execute_node("hi", GATED_AGENT_TOPIC, timeout=5)

        assert result.output == NO_TOOLS_RESPONSE_TEXT
        assert calls == ["g1"]


async def test_single_async_gate_true_allows_run(container, deploy_gated_function_agent):
    calls: list[str] = []

    async def g1(ctx: SessionRunContext) -> bool:
        calls.append("g1")
        return True

    deploy_gated_function_agent(gates=[g1])
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.execute_node("hi", GATED_AGENT_TOPIC, timeout=5)

        assert result.output == NO_TOOLS_RESPONSE_TEXT
        assert calls == ["g1"]


async def test_multiple_gates_all_true_run_in_order(container, deploy_gated_function_agent):
    calls: list[str] = []

    def g1(ctx: SessionRunContext) -> bool:
        calls.append("g1")
        return True

    def g2(ctx: SessionRunContext) -> bool:
        calls.append("g2")
        return True

    def g3(ctx: SessionRunContext) -> bool:
        calls.append("g3")
        return True

    deploy_gated_function_agent(gates=[g1, g2, g3])
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.execute_node("hi", GATED_AGENT_TOPIC, timeout=5)

        assert result.output == NO_TOOLS_RESPONSE_TEXT
        assert calls == ["g1", "g2", "g3"]


async def test_mixed_sync_and_async_gates(container, deploy_gated_function_agent):
    calls: list[str] = []

    def sync_gate(ctx: SessionRunContext) -> bool:
        calls.append("sync")
        return True

    async def async_gate(ctx: SessionRunContext) -> bool:
        calls.append("async")
        return True

    deploy_gated_function_agent(gates=[sync_gate, async_gate])
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.execute_node("hi", GATED_AGENT_TOPIC, timeout=5)

        assert result.output == NO_TOOLS_RESPONSE_TEXT
        assert calls == ["sync", "async"]


async def test_decorator_form_registers_gate(container, deploy_gated_function_agent):
    calls: list[str] = []

    agent = deploy_gated_function_agent(gates=None)

    @agent.gate
    def added_via_decorator(ctx: SessionRunContext) -> bool:
        calls.append("decorated")
        return True

    assert agent.gates == [added_via_decorator]

    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.execute_node("hi", GATED_AGENT_TOPIC, timeout=5)

        assert result.output == NO_TOOLS_RESPONSE_TEXT
        assert calls == ["decorated"]


async def test_constructor_then_decorator_order(container, deploy_gated_function_agent):
    calls: list[str] = []

    def from_ctor(ctx: SessionRunContext) -> bool:
        calls.append("ctor")
        return True

    agent = deploy_gated_function_agent(gates=[from_ctor])

    @agent.gate
    def from_decorator(ctx: SessionRunContext) -> bool:
        calls.append("decorator")
        return True

    assert agent.gates == [from_ctor, from_decorator]

    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.execute_node("hi", GATED_AGENT_TOPIC, timeout=5)

        assert result.output == NO_TOOLS_RESPONSE_TEXT
        assert calls == ["ctor", "decorator"]


async def test_gate_sees_overrides_applied_by_prepare_context(
    container,
    deploy_gated_function_agent,
    deploy_multiple_contextual_tools,
):
    """Gates must see post-prepare_context state — including overrides from current_frame."""

    seen_overrides: list[object] = []

    def capturing_gate(ctx: SessionRunContext) -> bool:
        seen_overrides.append(ctx.state.overrides)
        return True

    deploy_gated_function_agent(gates=[capturing_gate])
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        await client.execute_node(
            "hi",
            GATED_AGENT_TOPIC,
            tool_overrides=[],
            timeout=5,
        )

    assert seen_overrides
    assert seen_overrides[0] is not None
    assert seen_overrides[0].override_agent_tools == []


# ---------------------------------------------------------------------------
# Rejection cases (run() skipped, no reply, client times out)
# ---------------------------------------------------------------------------


async def test_single_sync_gate_false_blocks_run(container, deploy_gated_function_agent):
    calls: list[str] = []

    def g1(ctx: SessionRunContext) -> bool:
        calls.append("g1")
        return False

    deploy_gated_function_agent(gates=[g1])
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        handle = await client.invoke_node("hi", GATED_AGENT_TOPIC)
        with pytest.raises(asyncio.TimeoutError):
            await handle.result(timeout=2)

    assert calls == ["g1"]


async def test_single_async_gate_false_blocks_run(container, deploy_gated_function_agent):
    calls: list[str] = []

    async def g1(ctx: SessionRunContext) -> bool:
        calls.append("g1")
        return False

    deploy_gated_function_agent(gates=[g1])
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        handle = await client.invoke_node("hi", GATED_AGENT_TOPIC)
        with pytest.raises(asyncio.TimeoutError):
            await handle.result(timeout=2)

    assert calls == ["g1"]


async def test_short_circuit_stops_after_first_false(container, deploy_gated_function_agent):
    calls: list[str] = []

    def g1(ctx: SessionRunContext) -> bool:
        calls.append("g1")
        return True

    def g2(ctx: SessionRunContext) -> bool:
        calls.append("g2")
        return False

    def g3(ctx: SessionRunContext) -> bool:
        calls.append("g3")
        return True

    deploy_gated_function_agent(gates=[g1, g2, g3])
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        handle = await client.invoke_node("hi", GATED_AGENT_TOPIC)
        with pytest.raises(asyncio.TimeoutError):
            await handle.result(timeout=2)

    assert calls == ["g1", "g2"]


async def test_gate_raises_treated_as_reject_and_logged(
    container,
    deploy_gated_function_agent,
    caplog,
):
    calls: list[str] = []

    def boom(ctx: SessionRunContext) -> bool:
        calls.append("boom")
        raise RuntimeError("intentional")

    def never_called(ctx: SessionRunContext) -> bool:
        calls.append("never")
        return True

    deploy_gated_function_agent(gates=[boom, never_called])
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    with caplog.at_level(logging.ERROR, logger="calfkit.nodes.base"):
        async with TestKafkaBroker(broker):
            handle = await client.invoke_node("hi", GATED_AGENT_TOPIC)
            with pytest.raises(asyncio.TimeoutError):
                await handle.result(timeout=2)

    assert calls == ["boom"]
    assert any("gate[0]=boom raised" in rec.getMessage() for rec in caplog.records)


@pytest.mark.parametrize("non_bool_value", [1, "yes", None], ids=["int", "str", "none"])
async def test_non_bool_return_rejects_and_logs(
    container,
    deploy_gated_function_agent,
    caplog,
    non_bool_value,
):
    def bad_return(ctx: SessionRunContext):
        return non_bool_value

    deploy_gated_function_agent(gates=[bad_return])
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    with caplog.at_level(logging.ERROR, logger="calfkit.nodes.base"):
        async with TestKafkaBroker(broker):
            handle = await client.invoke_node("hi", GATED_AGENT_TOPIC)
            with pytest.raises(asyncio.TimeoutError):
                await handle.result(timeout=2)

    assert any(
        rec.exc_info is not None and isinstance(rec.exc_info[1], TypeError) and "expected bool" in str(rec.exc_info[1]) for rec in caplog.records
    )


# ---------------------------------------------------------------------------
# Tool-node gating
# ---------------------------------------------------------------------------


async def test_tool_node_gating_blocks_tool_function(container, deploy_function_agent):
    """A gate on a ToolNodeDef rejects before the tool function is invoked."""

    tool_invocations: list[str] = []
    gate_invocations: list[str] = []

    def my_tool() -> str:
        """Use this tool."""
        tool_invocations.append("ran")
        return "tool ran"

    def reject_tool(ctx: SessionRunContext) -> bool:
        gate_invocations.append("gate")
        return False

    gated_tool = ToolNodeDef.create_tool_node(
        func=my_tool,
        subscribe_topics="tool.my_tool.input",
        publish_topic="tool.my_tool.output",
        gates=[reject_tool],
    )
    deploy_function_agent.add_tools(gated_tool)
    container.get(Worker).add_nodes(gated_tool)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        handle = await client.invoke_node("hi", deploy_function_agent.subscribe_topics[0])
        with pytest.raises(asyncio.TimeoutError):
            await handle.result(timeout=2)

    assert gate_invocations == ["gate"]
    assert tool_invocations == []
