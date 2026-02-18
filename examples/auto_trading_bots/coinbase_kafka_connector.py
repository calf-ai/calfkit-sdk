"""
Coinbase-to-Kafka connector that streams real-time market data from
the Coinbase Exchange WebSocket and invokes an AgentRouterNode
for each price update (fire-and-forget).

Uses the ticker_batch channel for ~5-second batched price updates.

Usage:
    uv run python examples/coinbase_kafka_connector.py
    KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
        uv run python examples/coinbase_kafka_connector.py
    uv run python examples/coinbase_kafka_connector.py \
        --products BTC-USD ETH-USD SOL-USD
    uv run python examples/coinbase_kafka_connector.py \
        --min-interval 30

Prerequisites:
    - Kafka broker running (set KAFKA_BOOTSTRAP_SERVERS env var, default: localhost:9092)
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys

import uuid_utils
import websockets
from pydantic import BaseModel

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.agent_router_node import AgentRouterNode

logger = logging.getLogger(__name__)

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"

DEFAULT_PRODUCTS = [
    "BTC-USD",
    "ETH-USD",
    "SOL-USD",
    "XRP-USD",
    "DOGE-USD",
    "FARTCOIN-USD",
]

RECONNECT_DELAY_SECONDS = 3


class TickerMessage(BaseModel):
    """Coinbase ticker message published to Kafka."""

    product_id: str
    price: str
    best_bid: str
    best_bid_size: str
    best_ask: str
    best_ask_size: str
    side: str
    last_size: str
    open_24h: str
    high_24h: str
    low_24h: str
    volume_24h: str
    volume_30d: str
    trade_id: int
    sequence: int
    time: str


class CoinbaseKafkaConnector:
    """Streams Coinbase ticker data to an AgentRouterNode.

    Connects to the Coinbase Exchange WebSocket ticker_batch channel
    and invokes the configured AgentRouterNode with each price update
    using fire-and-forget publishes (no response subscriber).

    When min_publish_interval is set, incoming tickers are buffered per
    product ID. Only the latest data for each product is kept. A product's
    buffer is flushed once at least min_publish_interval seconds have
    elapsed since that product's last invocation.
    """

    def __init__(
        self,
        broker: BrokerClient,
        router_node: AgentRouterNode,
        products: list[str],
        min_publish_interval: float = 0.0,
    ) -> None:
        self._broker = broker
        self._router_node = router_node
        self._products = products
        self._min_interval = min_publish_interval
        self._running = True

        # Latest ticker per product — patched on every incoming message
        self._latest: dict[str, TickerMessage] = {}

    async def start(self) -> None:
        """Start the connector. Blocks until shutdown is triggered."""
        await self._broker.start()
        logger.info("Kafka broker connected")

        try:
            while self._running:
                try:
                    await self._consume_and_publish()
                except websockets.ConnectionClosed:
                    if not self._running:
                        break
                    logger.warning(
                        "WebSocket connection lost. Reconnecting in %ds...",
                        RECONNECT_DELAY_SECONDS,
                    )
                    await asyncio.sleep(RECONNECT_DELAY_SECONDS)
                except Exception:
                    if not self._running:
                        break
                    logger.exception(
                        "Unexpected error. Reconnecting in %ds...",
                        RECONNECT_DELAY_SECONDS,
                    )
                    await asyncio.sleep(RECONNECT_DELAY_SECONDS)
        finally:
            await self._broker.close()
            logger.info("Kafka broker closed")

    def stop(self) -> None:
        """Signal the connector to shut down gracefully."""
        self._running = False

    async def _publish_latest(self) -> None:
        """Snapshot and publish the current latest tickers as a single batch."""
        if not self._latest:
            return

        batch = list(self._latest.values())
        batch_json = json.dumps([t.model_dump() for t in batch])
        await self._router_node.invoke(
            user_prompt=(
                "Here is the latest ticker information. You should view your "
                "portfolio first before making any decisions to trade.\n\n"
                f"{batch_json}"
            ),
            broker=self._broker,
            correlation_id=uuid_utils.uuid7().hex,
        )

        summary = ", ".join(f"{t.product_id} @ ${t.price}" for t in batch)
        logger.info(
            "Published batch of %d ticker(s) to router: %s",
            len(batch),
            summary,
        )

    async def _periodic_publish(self) -> None:
        """Publish the latest snapshot on a fixed interval."""
        interval = max(self._min_interval, 1.0)
        while self._running:
            await asyncio.sleep(interval)
            await self._publish_latest()

    async def _consume_and_publish(self) -> None:
        """Connect to Coinbase WebSocket and buffer tickers for periodic publish."""
        self._latest.clear()

        async with websockets.connect(COINBASE_WS_URL) as ws:
            await ws.send(
                json.dumps(
                    {
                        "type": "subscribe",
                        "product_ids": self._products,
                        "channels": ["ticker_batch"],
                    }
                )
            )
            logger.info(
                "Subscribed to %d products on ticker_batch: %s",
                len(self._products),
                ", ".join(self._products),
            )

            flush_task = asyncio.create_task(self._periodic_publish())
            try:
                async for raw in ws:
                    if not self._running:
                        break

                    data = json.loads(raw)
                    if data.get("type") != "ticker":
                        continue

                    ticker = TickerMessage.model_validate(data)
                    self._latest[ticker.product_id] = ticker
            finally:
                flush_task.cancel()
                try:
                    await flush_task
                except asyncio.CancelledError:
                    pass


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream Coinbase market data to a Kafka topic.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: $KAFKA_BOOTSTRAP_SERVERS or localhost:9092).",
    )
    parser.add_argument(
        "--products",
        nargs="+",
        default=DEFAULT_PRODUCTS,
        help="Coinbase product IDs to subscribe to (default: %(default)s).",
    )
    parser.add_argument(
        "--min-interval",
        type=float,
        default=0.0,
        help=(
            "Minimum seconds between publishes per product. "
            "Incoming data is buffered and only the latest per product is published "
            "when the interval elapses. 0 = publish immediately (default: 0)."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    return parser.parse_args(argv)


async def run(args: argparse.Namespace, router_node: AgentRouterNode) -> None:
    broker = BrokerClient(bootstrap_servers=args.bootstrap_servers)
    connector = CoinbaseKafkaConnector(
        broker=broker,
        router_node=router_node,
        products=args.products,
        min_publish_interval=args.min_interval,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, connector.stop)

    logger.info("Starting Coinbase -> Kafka connector")
    logger.info("  Router topic:  %s", router_node.subscribed_topic)
    logger.info("  Broker:        %s", args.bootstrap_servers)
    logger.info("  Products:      %s", ", ".join(args.products))
    logger.info("  Min interval:  %ss", args.min_interval)

    await connector.start()


def main() -> None:
    from calfkit.nodes.chat_node import ChatNode
    from calfkit.stores.in_memory import InMemoryMessageHistoryStore

    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    router_node = AgentRouterNode(
        chat_node=ChatNode(),
        tool_nodes=[],
        message_history_store=InMemoryMessageHistoryStore(),
        system_prompt="You are a market data consumer.",
    )
    asyncio.run(run(args, router_node))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
