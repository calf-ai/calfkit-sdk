import asyncio

from calfkit.client import Client


async def main():
    # ``async with`` shuts the client down cleanly on exit (producer/consumer
    # teardown). Delivery does not depend on it: ``send`` awaits the broker's
    # ack before returning, so the record is already on the topic.
    async with Client.connect("localhost:9092") as client:  # Connect to Kafka broker
        # Fire-and-forget: dispatch the request and return immediately. No reply
        # is produced and no client-side reply future is allocated — send
        # hands back only the correlation_id, for tracing/logging.
        correlation_id = await client.send(
            "What's the weather in Tokyo?",
            "weather_agent.input",  # The topic the agent subscribes to
        )
        print(f"Dispatched fire-and-forget — correlation_id={correlation_id}")

    # There is no reply to await. To observe the result, tap the agent's
    # publish_topic broadcast stream: run weather_sink.py (a @consumer on
    # weather_agent.output) and watch the terminal output stream by.


if __name__ == "__main__":
    asyncio.run(main())
