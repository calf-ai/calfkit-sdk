import asyncio
import logging
import os

from dotenv import load_dotenv
from rich.live import Live

from calfkit.broker.broker import BrokerClient
from calfkit.runners.service import NodesService
from examples.daytrading_agents_arena.coinbase_kafka_connector import (
    PRICE_TOPIC,
    TickerMessage,
)
from examples.daytrading_agents_arena.trading_tools import (
    calculator,
    execute_trade,
    get_portfolio,
    price_book,
    view,
)

# Tools & Price Feed — Deploys trading tool workers and subscribes
# to the Kafka price topic published by the connector.
#
# The price subscriber hydrates the shared price book that the trading
# tools read from when executing trades.
#
# Usage:
#     uv run python examples/daytrading_agents_arena/tools_and_dispatcher.py
#
# Prerequisites:
#     - Kafka broker running at localhost:9092

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 50)
    print("Tools & Price Feed Deployment")
    print("=" * 50)

    print(f"\nConnecting to Kafka broker at {KAFKA_BOOTSTRAP_SERVERS}...")
    broker = BrokerClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    service = NodesService(broker)

    # ── Tool nodes ───────────────────────────────────────────────
    print("\nRegistering trading tool nodes...")
    for tool in (execute_trade, get_portfolio, calculator):
        service.register_node(tool)
        print(f"  - {tool.tool_schema.name} (topic: {tool.subscribed_topic})")

    # ── Price subscriber ─────────────────────────────────────────
    @broker.subscriber(PRICE_TOPIC, group_id="tools-dashboard")
    async def handle_price_update(ticker: TickerMessage) -> None:
        price_book.update(ticker.model_dump())
        view.rerender()

    print("\nStarting portfolio dashboard (prices via Kafka)...")

    with Live(view._build_layout(), auto_refresh=False, screen=True) as live:
        view.attach_live(live)
        await service.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTools and price feed stopped.")
