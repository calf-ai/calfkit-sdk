import asyncio

from calfkit.client import Client


async def main():
    # ``async with`` shuts the client down cleanly on exit (producer/consumer
    # teardown).
    async with Client.connect("localhost:9092") as client:  # Connect to Kafka broker
        # Send a request and await the response
        result = await client.execute(
            "What's the weather in Tokyo?",
            "weather_agent.input",  # The topic the agent subscribes to
        )
        print(f"Assistant: {result.output}")


if __name__ == "__main__":
    asyncio.run(main())
