"""Test whether FastStream routes messages to handlers based on Pydantic type signature."""

import asyncio

from pydantic import BaseModel
from faststream.kafka import KafkaBroker, TestKafkaBroker

TOPIC = "test-routing"

broker = KafkaBroker("localhost:9092")
subscriber = broker.subscriber(TOPIC)

results: dict[str, list] = {"user": [], "order": []}


class UserEvent(BaseModel):
    username: str
    email: str


class OrderEvent(BaseModel):
    order_id: int
    total: float


@subscriber
async def handle_user(msg: UserEvent):
    print(f"  -> handle_user received: {msg}")
    results["user"].append(msg)


@subscriber
async def handle_order(msg: OrderEvent):
    print(f"  -> handle_order received: {msg}")
    results["order"].append(msg)


async def main():
    async with TestKafkaBroker(broker) as br:
        # Test 1: Publish a UserEvent-shaped message
        print("\n[Test 1] Publishing UserEvent-shaped message...")
        await br.publish(
            {"username": "alice", "email": "alice@example.com"},
            topic=TOPIC,
        )

        # Test 2: Publish an OrderEvent-shaped message
        print("\n[Test 2] Publishing OrderEvent-shaped message...")
        await br.publish(
            {"order_id": 42, "total": 99.99},
            topic=TOPIC,
        )

        # Test 3: Publish a message that matches neither schema
        print("\n[Test 3] Publishing message matching neither schema...")
        try:
            await br.publish(
                {"unknown_field": "value"},
                topic=TOPIC,
            )
        except Exception as e:
            print(f"  -> Exception: {type(e).__name__}: {e}")

        # Results
        print("\n--- Results ---")
        print(f"  handle_user  called {len(results['user'])} time(s): {results['user']}")
        print(f"  handle_order called {len(results['order'])} time(s): {results['order']}")

        if len(results["user"]) == 1 and len(results["order"]) == 1:
            print("\n  VERDICT: FastStream DOES route by handler type signature!")
        else:
            print("\n  VERDICT: FastStream does NOT route by handler type signature.")


if __name__ == "__main__":
    asyncio.run(main())
