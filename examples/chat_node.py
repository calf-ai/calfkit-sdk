import asyncio
import os
import sys

from dotenv import load_dotenv

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.chat_node import ChatNode
from calfkit.providers.pydantic_ai.openai import OpenAIModelClient
from calfkit.runners.service import NodesService

load_dotenv()

# Check for API key
if not os.getenv("OPENAI_API_KEY"):
    print("ERROR: OPENAI_API_KEY environment variable is not set")
    print("Please set it before running:")
    print(" export OPENAI_API_KEY='your-api-key'")
    sys.exit(1)


# Chat Node - Deploys the LLM chat worker.

# This runs independently and handles all LLM inference requests.

# Usage:
#     uv run python examples/real_broker/chat_node.py

# Prerequisites:
#     - Kafka broker running at localhost:9092
#     - OPENAI_API_KEY environment variable set


async def main():
    print("=" * 50)
    print("Chat Node Deployment")
    print("=" * 50)

    # Connect to the real Kafka broker
    print("\nConnecting to Kafka broker at localhost:9092...")
    broker = BrokerClient(bootstrap_servers="localhost:9092")

    # Configure the LLM model
    print("Configuring OpenAI model client...")
    model_client = OpenAIModelClient(model_name="gpt-5-nano", reasoning_effort="low")

    # Deploy the chat node
    print("Registering chat node...")
    chat_node = ChatNode(model_client)
    service = NodesService(broker)
    service.register_node(chat_node)
    print("  - ChatNode registered")
    print(f"    Subscribe topic: {chat_node.subscribed_topic}")
    print(f"    Publish topic: {chat_node.publish_to_topic}")

    print("\nChat node ready. Waiting for requests...")

    # Run the service (this blocks)
    await service.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nChat node stopped.")
