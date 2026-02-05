import asyncio

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.chat_node import ChatNode
from calfkit.runners.service import NodesService
from calfkit.stores.in_memory import InMemoryMessageHistoryStore

# Import tools from tool_nodes - router needs schemas for LLM and topic routing
from examples.tool_nodes import get_exchange_rate, get_stock_price, get_weather

# Router Node - Deploys the agent router that orchestrates chat and tools.

# This runs independently and handles routing between chat and tool nodes.

# Usage:
#     uv run python examples/real_broker/router_node.py

# Prerequisites:
#     - Kafka broker running at localhost:9092


async def main():
    print("=" * 50)
    print("Router Node Deployment")
    print("=" * 50)

    # Connect to the real Kafka broker
    print("\nConnecting to Kafka broker at localhost:9092...")
    broker = BrokerClient(bootstrap_servers="localhost:9092")

    # Deploy the router node
    print("Registering router node...")
    router_node = AgentRouterNode(
        chat_node=ChatNode(),  # Reference for routing (deployment is in example/chat_node.py)
        tool_nodes=[
            get_exchange_rate,
            get_stock_price,
            get_weather,
        ],  # Tool references for routing (deployments are in example/tool_nodes.py)
        message_history_store=InMemoryMessageHistoryStore(),
        system_prompt="You are a helpful assistant. Use available tools when needed. Be concise.",
    )
    service = NodesService(broker)
    service.register_node(router_node)

    print("  - AgentRouterNode registered")
    print(f"    Subscribe topic: {router_node.subscribed_topic}")
    print(f"    Publish topic: {router_node.publish_to_topic}")
    print("    Tools registered:")
    for tool in [get_exchange_rate, get_stock_price, get_weather]:
        print(f"      - {tool.tool_schema().name} -> {tool.subscribed_topic}")

    print("\nRouter node ready. Waiting for requests...")

    # Run the service (this blocks)
    await service.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nRouter node stopped.")
