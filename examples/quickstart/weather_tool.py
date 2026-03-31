import asyncio

from calfkit.client import Client
from calfkit.nodes import agent_tool
from calfkit.worker import Worker


# Define a tool — the @agent_tool decorator turns any function into a deployable tool node
@agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location"""
    return f"It's sunny in {location}"


async def main():
    client = Client.connect("localhost:9092")  # Connect to Kafka broker
    worker = Worker(client, nodes=[get_weather])  # Initialize a worker with the tool node
    await worker.run()  # (Blocking call) Deploy the service to start serving traffic


if __name__ == "__main__":
    asyncio.run(main())
