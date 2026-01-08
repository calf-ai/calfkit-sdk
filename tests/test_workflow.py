"""Tests for Calf workflow functionality."""

import asyncio

import pytest

from calf import Calf, Message
from calf.broker.memory import MemoryBroker


@pytest.fixture
def broker() -> MemoryBroker:
    """Create a memory broker for testing."""
    return MemoryBroker()


@pytest.fixture
def calf_client(broker: MemoryBroker) -> Calf:
    """Create a Calf client with memory broker."""
    client = Calf()
    client._set_broker(broker)
    return client


async def test_emit_and_receive(broker: MemoryBroker) -> None:
    """Test that messages can be emitted and received."""
    await broker.connect()

    received: list[Message] = []

    async def collect_messages() -> None:
        async for msg in broker.subscribe("test.channel"):
            received.append(msg)
            if len(received) >= 1:
                break

    collector_task = asyncio.create_task(collect_messages())
    await asyncio.sleep(0.05)

    test_message = Message(data={"key": "value"}, channel="test.channel")
    await broker.send("test.channel", test_message)

    await asyncio.wait_for(collector_task, timeout=1.0)
    await broker.disconnect()

    assert len(received) == 1
    assert received[0].data == {"key": "value"}


async def test_handler_registration(calf_client: Calf, broker: MemoryBroker) -> None:
    """Test that handlers are registered correctly."""
    results: list[dict] = []

    @calf_client.on("events.test")
    async def handler(msg: Message) -> None:
        results.append(msg.data)

    assert "events.test" in calf_client._handlers

    await broker.connect()

    async def run_handler() -> None:
        async for msg in broker.subscribe("events.test"):
            await calf_client._handlers["events.test"](msg)
            break

    handler_task = asyncio.create_task(run_handler())
    await asyncio.sleep(0.05)

    test_message = Message(data={"test": "data"}, channel="events.test")
    await broker.send("events.test", test_message)

    await asyncio.wait_for(handler_task, timeout=1.0)
    await broker.disconnect()

    assert len(results) == 1
    assert results[0] == {"test": "data"}


async def test_message_flow_between_handlers(calf_client: Calf, broker: MemoryBroker) -> None:
    """Test that messages flow correctly between handlers."""
    final_results: list[dict] = []

    @calf_client.on("step.one")
    async def step_one(msg: Message) -> None:
        await calf_client.emit(
            "step.two",
            {"processed": True, "original_id": msg.data.get("id")},
        )

    @calf_client.on("step.two")
    async def step_two(msg: Message) -> None:
        final_results.append(msg.data)

    await broker.connect()

    async def run_handler(channel: str) -> None:
        async for msg in broker.subscribe(channel):
            await calf_client._handlers[channel](msg)
            break

    step_one_task = asyncio.create_task(run_handler("step.one"))
    step_two_task = asyncio.create_task(run_handler("step.two"))
    await asyncio.sleep(0.05)

    test_message = Message(data={"id": "123"}, channel="step.one")
    await broker.send("step.one", test_message)

    await asyncio.wait_for(step_one_task, timeout=1.0)
    await asyncio.wait_for(step_two_task, timeout=1.0)
    await broker.disconnect()

    assert len(final_results) == 1
    assert final_results[0]["processed"] is True
    assert final_results[0]["original_id"] == "123"
