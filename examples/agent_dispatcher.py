import asyncio

from calfkit.broker.broker import BrokerClient
from calfkit.experimental.agent_dispatcher import AgentDispatcher
from calfkit.runners.service import NodesService
from examples.auto_trading_bots.trading_tools import make_trading_tools

# Agent Dispatcher - Deploys the dispatcher that dynamically spawns agent routers.

# When a message arrives on `dispatch_request`, the dispatcher creates an
# AgentRouterNode wired to the provided trading tools and starts its
# consumers on the already-running broker.

# Usage:
#     uv run python examples/agent_dispatcher.py

# Prerequisites:
#     - Kafka broker running at localhost:9092
#     - Chat node running (examples/chat_node.py)
#     - Trading tools running (examples/trading_tools.py)


async def main():
    print("=" * 50)
    print("Agent Dispatcher Deployment")
    print("=" * 50)

    print("\nConnecting to Kafka broker at localhost:9092...")
    broker = BrokerClient(bootstrap_servers="localhost:9092")

    print("Registering agent dispatcher...")
    dispatcher = AgentDispatcher(tool_nodes_factory=make_trading_tools)
    service = NodesService(broker)
    service.register_node(dispatcher)

    print("  - AgentDispatcher registered")
    print(f"    Subscribe topic: {dispatcher.subscribed_topic}")
    print(f"    Publish topic: {dispatcher.publish_to_topic}")

    print("\nAgent dispatcher ready. Waiting for dispatch requests...")

    await service.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nAgent dispatcher stopped.")
