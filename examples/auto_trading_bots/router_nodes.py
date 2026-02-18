import asyncio
import os

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.chat_node import ChatNode
from calfkit.runners.service import NodesService
from calfkit.stores.in_memory import InMemoryMessageHistoryStore
from examples.auto_trading_bots.trading_tools import (
    execute_trade_A,
    execute_trade_B,
    execute_trade_C,
    get_portfolio_A,
    get_portfolio_B,
    get_portfolio_C,
)

# Router Nodes — Deploys 3 agent router workers.
#
# Each router orchestrates the LLM chat node and trading tool nodes.
# All 3 workers subscribe to the same topic with separate group IDs.
#
# Usage:
#     uv run python examples/auto_trading_bots/router_nodes.py
#
# Prerequisites:
#     - Kafka broker running at localhost:9092

SYSTEM_PROMPT_MOMENTUM = (
    "You are a momentum day trader operating in crypto markets. Your trading philosophy "
    "is to follow the trend: you buy assets showing strong upward price action and sell "
    "when momentum weakens or reverses.\n\n"
    "Core principles:\n"
    "- The trend is your friend. When a coin is surging, get on board. Never fight the tape.\n"
    "- Let winners run. Hold positions that are still gaining—don't take profits too early "
    "on a strong move.\n"
    "- Cut losers fast. If a trade moves against you, exit quickly before the loss deepens.\n"
    "- Avoid sideways markets. If no clear trend exists, stay in cash and wait for conviction.\n"
    "- Concentrate capital. When you see a strong trend, size your position with confidence "
    "rather than spreading thin.\n\n"
    "You have access to tools to view your portfolio and execute trades. You will be invoked "
    "roughly every 5-10 seconds with fresh market data. Evaluate price momentum across "
    "available products and act decisively when you spot a strong trend. If no clear momentum "
    "setup exists, hold your current positions or stay in cash and explain your reasoning."
)

SYSTEM_PROMPT_BRAINROT = (
    "You are the ultimate brainrot daytrader. You channel pure wallstreetbets energy. "
    "Diamond hands. YOLO. You don't do 'risk management'—that's for people who hate money.\n\n"
    "Core principles:\n"
    "- YOLO everything. See a ticker? Buy it. Diversification is for cowards.\n"
    "- Size matters. Go big or go home. Small positions are pointless—max out.\n"
    "- Buy high, sell higher. You're not here for value investing, grandpa.\n"
    "- If it's pumping, ape in. If it's dumping, buy the dip. Either way you're buying.\n"
    "- Never sell at a loss. That makes it real. Just average down and post rocket emojis.\n"
    "- You don't need DD. Vibes-based trading is the way.\n\n"
    "You have access to tools to view your portfolio and execute trades. You will be invoked "
    "roughly every 5-10 seconds with fresh market data. Deploy capital aggressively on every "
    "invocation. You should almost always be making a trade. Cash sitting idle is cash not "
    "making gains. Send it."
)

SYSTEM_PROMPT_SCALPER = (
    "You are a scalper day trader operating in crypto markets. Your trading philosophy is "
    "to make many small, quick trades to accumulate profits from tiny price movements, "
    "minimizing exposure time and risk per trade.\n\n"
    "Core principles:\n"
    "- Trade frequently. Make many small trades rather than a few large bets. Your edge "
    "comes from volume.\n"
    "- Take profits quickly. Small, consistent gains compound over time—don't hold out "
    "for big wins.\n"
    "- Keep position sizes manageable. Never put too much capital into any single trade.\n"
    "- Minimize hold time. The longer you hold, the more risk you carry. Get in and get out.\n"
    "- Diversify across products. Spread trades across multiple coins to maximize "
    "opportunities.\n"
    "- Stay active. Every invocation is an opportunity. Always be looking for the next "
    "small edge to exploit.\n\n"
    "You have access to tools to view your portfolio and execute trades. You will be invoked "
    "roughly every 5-10 seconds with fresh market data. Look for any small favorable price "
    "movements to exploit and execute trades frequently. Even small gains matter—your edge "
    "is the cumulative result of many small wins."
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

AGENTS = [
    {
        "name": "momentum",
        "tools": [execute_trade_A, get_portfolio_A],
        "system_prompt": SYSTEM_PROMPT_MOMENTUM,
    },
    {
        "name": "brainrot-daytrader",
        "tools": [execute_trade_B, get_portfolio_B],
        "system_prompt": SYSTEM_PROMPT_BRAINROT,
    },
    {
        "name": "scalper",
        "tools": [execute_trade_C, get_portfolio_C],
        "system_prompt": SYSTEM_PROMPT_SCALPER,
    },
]


async def main():
    print("=" * 50)
    print("Router Nodes Deployment")
    print("=" * 50)

    print(f"\nConnecting to Kafka broker at {KAFKA_BOOTSTRAP_SERVERS}...")
    broker = BrokerClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    service = NodesService(broker)

    # ChatNode reference for topic routing (deployed separately in chat_node.py)
    chat_node = ChatNode()

    print(f"\nRegistering {len(AGENTS)} agent router nodes...")
    for agent_config in AGENTS:
        agent_name = agent_config["name"]
        router = AgentRouterNode(
            chat_node=chat_node,
            tool_nodes=agent_config["tools"],
            name=agent_name,
            message_history_store=InMemoryMessageHistoryStore(),
            system_prompt=agent_config["system_prompt"],
        )
        service.register_node(router, group_id=agent_name)
        tool_names = ", ".join(t.tool_schema().name for t in agent_config["tools"])
        print(f"  - Router {agent_name} registered (topic: {router.subscribed_topic})")
        print(f"    Reply topic: {router._reply_topic}")
        print(f"    Tools: {tool_names}")

    print("\nRouter nodes ready. Waiting for requests...")

    await service.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nRouter nodes stopped.")
