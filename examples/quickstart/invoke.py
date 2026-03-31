import asyncio

from calfkit.client import Client


async def main():
    client = Client.connect("localhost:9092")  # Connect to Kafka broker

    # Send a request and await the response
    result = await client.execute_node(
        "What's the weather in Tokyo?",
        "agent.input",  # The topic the agent subscribes to
    )
    print(f"Assistant: {result.output}")


if __name__ == "__main__":
    asyncio.run(main())
