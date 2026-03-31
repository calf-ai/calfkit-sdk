import asyncio

from weather_tool import get_weather  # Import the tool definition (reusable)

from calfkit.client import Client
from calfkit.nodes import Agent
from calfkit.providers import OpenAIModelClient
from calfkit.worker import Worker

agent = Agent(
    "weather_agent",
    system_prompt="You are a helpful assistant.",
    subscribe_topics="weather_agent.input",
    model_client=OpenAIModelClient(model_name="gpt-5-nano"),
    tools=[get_weather],  # Register tool definitions with the agent
)


async def main():
    client = Client.connect("localhost:9092")  # Connect to Kafka broker
    worker = Worker(client, nodes=[agent])  # Initialize a worker with the agent node
    await worker.run()  # (Blocking call) Deploy the service to start serving traffic


if __name__ == "__main__":
    asyncio.run(main())
