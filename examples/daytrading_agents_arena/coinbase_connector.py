import asyncio
import logging
import os

from dotenv import load_dotenv

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.agent_router_node import AgentRouterNode
from examples.daytrading_agents_arena.coinbase_consumer import CandleBook
from examples.daytrading_agents_arena.coinbase_kafka_connector import (
    DEFAULT_PRODUCTS,
    CoinbaseKafkaConnector,
)

# Coinbase Connector — Streams live market data from the Coinbase
# Exchange WebSocket and invokes the deployed agent routers via
# RouterServiceClient on each price tick.
#
# Usage:
#     uv run python examples/daytrading_agents_arena/coinbase_connector.py
#
# Prerequisites:
#     - Kafka broker running (set KAFKA_BOOTSTRAP_SERVERS env var, default: localhost:9092)
#     - Router nodes deployed (deploy_router_node.py)
#     - Chat node deployed (deploy_chat_node.py)
#     - Tools deployed (tools_and_dispatcher.py)

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MIN_PUBLISH_INTERVAL = 60.0


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 50)
    print("Coinbase Connector Deployment")
    print("=" * 50)

    print(f"\nConnecting to Kafka broker at {KAFKA_BOOTSTRAP_SERVERS}...")
    broker = BrokerClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    # Reference router node for topic routing.
    # tool_nodes=None so the deployed routers use their own tools.
    router_node = AgentRouterNode()

    print(f"  Router topic: {router_node.subscribed_topic}")
    print(f"  Products: {', '.join(DEFAULT_PRODUCTS)}")
    print(f"  Min publish interval: {MIN_PUBLISH_INTERVAL}s")

    candle_book = CandleBook()

    connector = CoinbaseKafkaConnector(
        broker=broker,
        router_node=router_node,
        products=DEFAULT_PRODUCTS,
        min_publish_interval=MIN_PUBLISH_INTERVAL,
        candle_book=candle_book,
    )

    print("\nStarting Coinbase connector...")
    await connector.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCoinbase connector stopped.")
