import asyncio
import os
import sys

from dotenv import load_dotenv

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.chat_node import ChatNode
from calfkit.providers.pydantic_ai.openai import OpenAIModelClient
from calfkit.runners.service import NodesService

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Chat Node — Deploys the LLM inference worker.
#
# Handles all LLM chat requests from the agent router nodes.
#
# Usage:
#     uv run python examples/auto_trading_bots/chat_node.py
#
# Prerequisites:
#     - Kafka broker running at localhost:9092
#     - OPENAI_API_KEY environment variable set

if not os.getenv("OPENAI_API_KEY"):
    print("ERROR: OPENAI_API_KEY environment variable is not set")
    print("Please set it before running:")
    print("  export OPENAI_API_KEY='your-api-key'")
    sys.exit(1)


async def main():
    print("=" * 50)
    print("Chat Node Deployment")
    print("=" * 50)

    print(f"\nConnecting to Kafka broker at {KAFKA_BOOTSTRAP_SERVERS}...")
    broker = BrokerClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    print("Configuring OpenAI model client...")
    model_client = OpenAIModelClient(model_name="gpt-5-nano", reasoning_effort="low")

    print("Registering chat node...")
    chat_node = ChatNode(model_client)
    service = NodesService(broker)
    service.register_node(chat_node, max_workers=10)
    print(f"  - ChatNode registered (topic: {chat_node.subscribed_topic})")

    print("\nChat node ready. Waiting for requests...")

    await service.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nChat node stopped.")
