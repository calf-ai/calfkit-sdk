"""Experiment: Can we define a consumer after broker.start()?

This tests whether FastStream/Kafka allows adding subscribers
after the broker has already been started.
"""

import asyncio
from typing import Annotated

from faststream import Context

from calfkit.broker.broker import Broker


async def main():
    print("=" * 60)
    print("Experiment: Consumer After Broker Start")
    print("=" * 60)

    # Create and start the broker
    broker = Broker(bootstrap_servers="localhost:9092")

    print("\n1. Starting broker...")
    await broker.start()
    print("   ✓ Broker started")

    # Define a subscriber AFTER broker.start()
    print("\n2. Defining consumer AFTER broker.start()...")

    received_messages = []

    @broker.subscriber("test-topic-after-start")
    async def test_consumer(
        message: str,
        correlation_id: Annotated[str, Context()],
    ):
        print(f"   [Consumer received]: {message}")
        received_messages.append(message)

    print("   ✓ Consumer defined")

    # Give it a moment to register
    await asyncio.sleep(1)

    # Publish to the topic
    print("\n3. Publishing message to topic...")
    await broker.publish(
        "Hello from post-start publisher!",
        topic="test-topic-after-start",
    )
    print("   ✓ Message published")

    # Wait for consumer to receive
    print("\n4. Waiting for consumer to receive message...")
    await asyncio.sleep(2)

    # Check results
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)

    if received_messages:
        print(f"✓ SUCCESS: Consumer received {len(received_messages)} message(s)")
        for msg in received_messages:
            print(f"  - {msg}")
    else:
        print("✗ FAILURE: Consumer did NOT receive any messages")
        print("  This suggests consumers cannot be added after broker.start()")

    print("\n" + "=" * 60)

    # Cleanup
    await broker.stop()
    print("Broker stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExperiment interrupted.")
    except Exception as e:
        print(f"\nExperiment failed with error: {e}")
        import traceback

        traceback.print_exc()
