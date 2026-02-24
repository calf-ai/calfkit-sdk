"""Deploy a single named AgentRouterNode for the daytrading arena.

Each router subscribes to the shared ``agent_router.input`` topic with its
own consumer group, so every agent receives every market tick independently.
The ``--chat-node-name`` flag targets a specific named ChatNode for LLM
inference.

Example:
    uv run python examples/daytrading_agents_arena/deploy_router_node.py \
        --name momentum --chat-node-name gpt5-nano --strategy momentum

    uv run python examples/daytrading_agents_arena/deploy_router_node.py \
        --name brainrot-daytrader --chat-node-name deepseek --strategy brainrot
"""

import argparse
import asyncio
import sys

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.chat_node import ChatNode
from calfkit.runners.service import NodesService
from calfkit.stores.in_memory import InMemoryMessageHistoryStore
from examples.daytrading_agents_arena.trading_tools import calculator, execute_trade, get_portfolio

STRATEGIES: dict[str, str] = {
    "default": (
        "You are a crypto day trader. Your goal is to maximize your total account balance "
        "(cash + portfolio value) over time.\n\n"
        "You will be invoked periodically with live market data including current "
        "prices, bid/ask spreads, and multi-timeframe candlestick charts (1-min, "
        "5-min, and 15-min) for several cryptocurrency products.\n\n"
        "You have access to tools to view your portfolio, execute trades (buy/sell at "
        "market price), and a calculator for math. Use the market data "
        "provided to make informed trading decisions. "
        "Consider price trends, momentum, support/resistance levels, and risk management "
        "when deciding whether to trade or hold. Explain your reasoning briefly."
    ),
    "momentum": (
        "You are a momentum day trader operating in crypto markets. Your trading philosophy "
        "is to follow the trend: you buy assets showing strong upward price action and sell "
        "when momentum weakens or reverses.\n\n"
        "Core principles:\n"
        "- The trend is your friend. When a coin is surging, get on board. Never fight the tape.\n"
        "- Let winners run. Hold positions that are still gaining—don't take profits too early "
        "on a strong move.\n"
        "- Cut losers fast. If a trade moves against you, exit quickly before the loss deepens.\n"
        "- Avoid sideways markets. If no clear trend exists, stay in cash "
        "and wait for conviction.\n"
        "- Concentrate capital. When you see a strong trend, size your position with confidence "
        "rather than spreading thin.\n\n"
        "You have access to tools to view your portfolio and execute trades. You will be invoked "
        "periodically with fresh market data. Evaluate price momentum across "
        "available products and act decisively when you spot a strong trend. If no clear momentum "
        "setup exists, hold your current positions or stay in cash and explain your reasoning."
    ),
    "brainrot": (
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
        "periodically with fresh market data. Deploy capital aggressively on every "
        "invocation. You should almost always be making a trade. Cash sitting idle is cash not "
        "making gains. Send it."
    ),
    "scalper": (
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
        "periodically with fresh market data. Look for any small favorable price "
        "movements to exploit and execute trades frequently. Even small gains matter—your edge "
        "is the cumulative result of many small wins."
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deploy a named AgentRouterNode for the daytrading arena.",
    )
    parser.add_argument(
        "--name",
        required=True,
        help="Agent name (used as consumer group + identity)",
    )
    parser.add_argument(
        "--chat-node-name",
        required=True,
        help="Name of the deployed ChatNode to target (e.g. gpt5-nano)",
    )
    parser.add_argument(
        "--strategy",
        required=True,
        choices=list(STRATEGIES.keys()),
        help="Trading strategy (selects system prompt)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Kafka bootstrap servers address",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    system_prompt = STRATEGIES.get(args.strategy)
    if system_prompt is None:
        print(f"ERROR: Unknown strategy '{args.strategy}'")
        print(f"Available: {', '.join(STRATEGIES.keys())}")
        sys.exit(1)

    print("=" * 50)
    print(f"Router Node Deployment: {args.name}")
    print("=" * 50)

    print(f"\nConnecting to Kafka broker at {args.bootstrap_servers}...")
    broker = BrokerClient(bootstrap_servers=args.bootstrap_servers)
    service = NodesService(broker)

    # ChatNode reference for topic routing (deployed separately via deploy_chat_node.py)
    chat_node = ChatNode(name=args.chat_node_name)

    tools = [execute_trade, get_portfolio, calculator]
    router = AgentRouterNode(
        chat_node=chat_node,
        tool_nodes=tools,
        name=args.name,
        message_history_store=InMemoryMessageHistoryStore(),
        system_prompt=system_prompt,
    )
    service.register_node(router, group_id=args.name)

    tool_names = ", ".join(t.tool_schema.name for t in tools)
    print(f"  - Agent:    {args.name}")
    print(f"  - Strategy: {args.strategy}")
    print(f"  - ChatNode: {args.chat_node_name} (topic: {chat_node.entrypoint_topic})")
    print(f"  - Input:    {router.subscribed_topic}")
    print(f"  - Reply:    {router.entrypoint_topic}")
    print(f"  - Tools:    {tool_names}")

    print("\nRouter node ready. Waiting for requests...")
    await service.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nRouter node stopped.")
