"""
Coinbase Exchange WebSocket consumer that maintains an up-to-date price book.

Subscribes to ticker_batch (~5s updates) for a set of products and keeps
a local dictionary of the latest bid/ask/price for each.

Usage:
    uv run python examples/coinbase_consumer.py
"""

import asyncio
import json
import signal
import sys
from datetime import datetime, timezone

import websockets

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"

PRODUCTS = [
    "BTC-USD",
    "ETH-USD",
    "SOL-USD",
    "XRP-USD",
    "DOGE-USD",
    "FARTCOIN-USD",
]


class PriceBook:
    """Maintains the latest price snapshot for each subscribed product."""

    def __init__(self):
        self._book: dict[str, dict] = {}

    def update(self, data: dict) -> None:
        self._book[data["product_id"]] = {
            "price": data["price"],
            "best_bid": data["best_bid"],
            "best_bid_size": data["best_bid_size"],
            "best_ask": data["best_ask"],
            "best_ask_size": data["best_ask_size"],
            "side": data["side"],
            "last_size": data["last_size"],
            "volume_24h": data["volume_24h"],
            "time": data["time"],
        }

    def get(self, product_id: str) -> dict | None:
        return self._book.get(product_id)

    def snapshot(self) -> dict[str, dict]:
        return dict(self._book)

    def display(self) -> None:
        if not self._book:
            return

        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"\n{'=' * 78}")
        print(f"  Price Book @ {now} UTC")
        print(f"{'=' * 78}")
        print(
            f"  {'Product':<14} {'Price':>12} {'Bid':>12} {'Ask':>12}"
            f" {'Spread':>10} {'Vol 24h':>14}"
        )
        print(f"  {'-' * 74}")

        for product_id in PRODUCTS:
            entry = self._book.get(product_id)
            if entry is None:
                print(f"  {product_id:<14} {'--':>12}")
                continue

            bid = float(entry["best_bid"])
            ask = float(entry["best_ask"])
            spread = ask - bid

            print(
                f"  {product_id:<14}"
                f" {entry['price']:>12}"
                f" {entry['best_bid']:>12}"
                f" {entry['best_ask']:>12}"
                f" {spread:>10.6f}"
                f" {float(entry['volume_24h']):>14,.2f}"
            )

        print(f"{'=' * 78}")


async def consume(price_book: PriceBook) -> None:
    while True:
        try:
            async with websockets.connect(COINBASE_WS_URL) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "type": "subscribe",
                            "product_ids": PRODUCTS,
                            "channels": ["ticker_batch"],
                        }
                    )
                )

                print(f"Connected. Subscribed to {len(PRODUCTS)} products.")
                print("Waiting for data (updates every ~5s)...\n")

                async for raw in ws:
                    data = json.loads(raw)

                    if data.get("type") == "ticker":
                        price_book.update(data)
                        price_book.display()

        except websockets.ConnectionClosed:
            print("\nConnection lost. Reconnecting in 3s...")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"\nError: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


def main() -> None:
    price_book = PriceBook()

    loop = asyncio.new_event_loop()

    def shutdown(sig, frame):
        print("\nShutting down...")
        loop.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    loop.run_until_complete(consume(price_book))


if __name__ == "__main__":
    main()
