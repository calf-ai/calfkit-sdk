import asyncio

from calfkit.client import Client


async def main():
    # ``async with`` shuts the client down cleanly on exit (producer/consumer
    # teardown).
    async with Client.connect("localhost:9092") as client:  # Connect to Kafka broker
        # Mint a typed gateway to the agent by name, then send a request and await the response.
        result = await client.agent("weather_agent").execute("What's the weather in Tokyo?")
        print(f"Assistant: {result.output}")


if __name__ == "__main__":
    asyncio.run(main())
