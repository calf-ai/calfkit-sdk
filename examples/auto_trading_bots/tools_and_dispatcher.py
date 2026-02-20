import asyncio
import logging
import os

from rich.live import Live

from calfkit.broker.broker import BrokerClient
from calfkit.experimental.agent_dispatcher import AgentDispatcher
from calfkit.nodes.base_tool_node import BaseToolNode
from calfkit.runners.service import NodesService
from examples.auto_trading_bots.trading_tools import (
    AGENT_ID_A,
    AGENT_ID_B,
    AGENT_ID_C,
    execute_trade_A,
    execute_trade_B,
    execute_trade_C,
    get_portfolio_A,
    get_portfolio_B,
    get_portfolio_C,
    make_trading_tools,
    run_price_feed,
    store,
    view,
)

# Tools, Dispatcher & Price Feed — Deploys trading tool workers,
# the agent dispatcher, and the Coinbase price feed.
#
# The price feed hydrates the shared price book that the trading
# tools read from when executing trades.
#
# Usage:
#     uv run python examples/auto_trading_bots/tools_and_dispatcher.py
#
# Prerequisites:
#     - Kafka broker running at localhost:9092

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

ALL_TOOLS = [
    execute_trade_A,
    execute_trade_B,
    execute_trade_C,
    get_portfolio_A,
    get_portfolio_B,
    get_portfolio_C,
]


def create_agent_tools(group_id: str) -> list[BaseToolNode]:
    """Factory that creates trading tools and an account for a new agent."""
    store.get_or_create(group_id)
    tools = list(make_trading_tools(group_id))
    view.rerender()
    return tools


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 50)
    print("Tools, Dispatcher & Price Feed Deployment")
    print("=" * 50)

    # Pre-create agent accounts so they appear immediately
    for agent_id in (AGENT_ID_A, AGENT_ID_B, AGENT_ID_C):
        store.get_or_create(agent_id)

    print(f"\nConnecting to Kafka broker at {KAFKA_BOOTSTRAP_SERVERS}...")
    broker = BrokerClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    service = NodesService(broker)

    # ── Tool nodes ───────────────────────────────────────────────
    print("\nRegistering trading tool nodes...")
    for tool in ALL_TOOLS:
        service.register_node(tool)
        print(f"  - {tool.tool_schema.name} (topic: {tool.subscribed_topic})")

    # ── Agent dispatcher ─────────────────────────────────────────
    print("\nRegistering agent dispatcher...")
    dispatcher = AgentDispatcher(tool_nodes_factory=create_agent_tools)
    service.register_node(dispatcher)
    print(f"  - AgentDispatcher registered (topic: {dispatcher.subscribed_topic})")

    # ── Price feed ───────────────────────────────────────────────
    print("\nStarting Coinbase price feed...")
    price_feed_task = asyncio.create_task(run_price_feed())

    print("\nStarting portfolio dashboard...")

    with Live(view._build_layout(), auto_refresh=False, screen=True) as live:
        view.attach_live(live)
        try:
            await service.run()
        finally:
            price_feed_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTools and dispatcher stopped.")
