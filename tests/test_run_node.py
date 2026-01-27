import itertools

import pytest
from faststream.kafka import TestKafkaBroker

from calf.nodes.atomic_node import BaseAtomicNode, on, post_to
from calf.runtime import CalfRuntime

CalfRuntime.initialize()


class TestCalfNode(BaseAtomicNode):
    counter = itertools.count()
    received = []

    @on("test_listen")
    async def test_listen(self, msg: str):
        print(f"Reacting to test_listen, msg recieved: {msg}")
        self.received.append(msg)

    @on("startflow")
    @post_to("test_topic_2")
    async def start_test_flow(self, msg: str) -> str:
        print(f"Writing to test_topic_2 with msg={msg}")
        return f"{msg}.foo"

    @on("test_topic_2")
    @post_to("test_topic_3")
    async def test_flow(self, msg: str) -> str:
        print("Reacting to test_topic_2")
        result = f"{msg}.bar"
        print(f"Publishing '{result}' to test_topic_3")
        return result

    @on("test_topic_3")
    async def test_flow_end(self, msg: str):
        print(f"Reacting to test_topic_3, msg recieved: {msg}")
        self.received.append(f"{msg}.pipe")


@pytest.mark.asyncio
async def test_simple_listen():
    print("\n\n===Start test===")
    node = TestCalfNode()
    async with TestKafkaBroker(node.calf) as br:
        test_msg = f"LeTestSimple{next(node.counter)}"
        print(f"Writing to test_listen with msg={test_msg}")
        await br.publish(test_msg, topic="test_listen")
        assert test_msg in node.received


@pytest.mark.asyncio
async def test_flow():
    print("\n\n===Start test===")
    node = TestCalfNode()
    async with TestKafkaBroker(node.calf) as br:
        test_msg = f"LeTestFlow{next(node.counter)}"
        print(f"Writing to startflow with msg={test_msg}")
        await br.publish(message=test_msg, topic="startflow")
        assert f"{test_msg}.foo.bar.pipe" in node.received
