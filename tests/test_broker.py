from faststream import FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)


@broker.subscriber("test-topic")
async def handle(
    name: str,
    user_id: int,
):
    print("Start handling")
    assert name == "John"
    assert user_id == 1
    print("Finished handling")
    
import pytest
from pydantic import ValidationError

from faststream.kafka import TestKafkaBroker

@pytest.mark.asyncio
async def test_handle():
    print("\n\n===Start test===")
    async with TestKafkaBroker(broker) as br:
        await br.publish({"name": "John", "user_id": 1}, topic="test-topic")