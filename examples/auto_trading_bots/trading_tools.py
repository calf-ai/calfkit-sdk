"""
Trading Tool — an @agent_tool that executes buy/sell trades against
an in-memory portfolio store and rerenders a Rich Live dashboard
after every trade.

Prices are sourced from the Coinbase Exchange WebSocket (ticker_batch)
which runs as a background task and keeps a live price book.

The account store is keyed by agent_id so multiple agent runtimes
can each maintain independent portfolios.  For now the agent_id
inside the tool is hardcoded.

Usage:
    uv run python examples/trading_tool.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime

import websockets
from rich.columns import Columns
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.base_tool_node import BaseToolNode, agent_tool
from calfkit.runners.service import NodesService
from examples.auto_trading_bots.coinbase_consumer import COINBASE_WS_URL, PriceBook

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────

AGENT_ID_A = "momentum"
AGENT_ID_B = "brainrot-daytrader"
AGENT_ID_C = "scalper"

INITIAL_CASH = 100_000.0

SUBSCRIBED_PRODUCTS = [
    "BTC-USD",
    "ETH-USD",
    "SOL-USD",
    "XRP-USD",
    "DOGE-USD",
    "AVAX-USD",
    "LINK-USD",
    "DOT-USD",
]

RECONNECT_DELAY_SECONDS = 3


# ── Data model ───────────────────────────────────────────────────


@dataclass
class TradeResult:
    success: bool
    message: str


@dataclass
class AgentAccount:
    cash: float = INITIAL_CASH
    positions: dict[str, int] = field(default_factory=dict)
    cost_basis: dict[str, float] = field(default_factory=dict)
    trade_count: int = 0

    def portfolio_value(self, price_book: PriceBook) -> float:
        """Total value: cash + mark-to-market of all positions using live prices."""
        positions_value = 0.0
        for pid, qty in self.positions.items():
            entry = price_book.get(pid)
            if entry is not None:
                positions_value += qty * float(entry["price"])
        return self.cash + positions_value

    def avg_cost_per_unit(self, product_id: str) -> float:
        """Average cost per unit for a position."""
        qty = self.positions.get(product_id, 0)
        if qty == 0:
            return 0.0
        return self.cost_basis.get(product_id, 0.0) / qty


# ── Account store ────────────────────────────────────────────────


class AccountStore:
    """In-memory trading account store, keyed by agent_id."""

    def __init__(self, price_book: PriceBook) -> None:
        self._accounts: dict[str, AgentAccount] = {}
        self._trade_log: list[tuple[str, str, str, str, int, float]] = []
        self._price_book = price_book

    def get_or_create(self, agent_id: str) -> AgentAccount:
        if agent_id not in self._accounts:
            self._accounts[agent_id] = AgentAccount()
        return self._accounts[agent_id]

    @property
    def accounts(self) -> dict[str, AgentAccount]:
        return self._accounts

    @property
    def price_book(self) -> PriceBook:
        return self._price_book

    @property
    def trade_log(self) -> list[tuple[str, str, str, str, int, float]]:
        return self._trade_log

    def execute_trade(
        self,
        agent_id: str,
        product_id: str,
        quantity: int,
        action: str,
    ) -> TradeResult:
        product_id = product_id.upper().strip()
        action = action.lower().strip()

        if action not in ("buy", "sell"):
            return TradeResult(False, f"Invalid action '{action}'. Must be 'buy' or 'sell'.")

        entry = self._price_book.get(product_id)
        if entry is None:
            available = ", ".join(sorted(self._price_book.snapshot().keys()))
            return TradeResult(
                False,
                f"No live price for '{product_id}'. "
                f"Available: {available or 'none (waiting for price data)'}",
            )

        if quantity <= 0:
            return TradeResult(False, "Quantity must be a positive integer.")

        account = self.get_or_create(agent_id)

        if action == "buy":
            price = float(entry["best_ask"])
            cost = price * quantity
            if cost > account.cash:
                return TradeResult(
                    False,
                    f"Insufficient cash. Need ${cost:,.2f} but only have ${account.cash:,.2f}.",
                )
            account.cash -= cost
            account.positions[product_id] = account.positions.get(product_id, 0) + quantity
            account.cost_basis[product_id] = account.cost_basis.get(product_id, 0.0) + cost
            account.trade_count += 1
            self._record_trade(agent_id, action, product_id, quantity, price)
            return TradeResult(
                True,
                f"Bought {quantity} {product_id} @ ${price:,.2f} for ${cost:,.2f}. "
                f"Cash remaining: ${account.cash:,.2f}.",
            )

        # sell
        price = float(entry["best_bid"])
        held = account.positions.get(product_id, 0)
        if quantity > held:
            return TradeResult(
                False,
                f"Insufficient holdings. Want to sell {quantity} {product_id} "
                f"but only hold {held}.",
            )
        proceeds = price * quantity
        account.cash += proceeds
        # Reduce cost basis proportionally (average cost method)
        avg_cost = account.avg_cost_per_unit(product_id)
        account.cost_basis[product_id] = account.cost_basis.get(product_id, 0.0) - (
            avg_cost * quantity
        )
        new_qty = held - quantity
        if new_qty == 0:
            del account.positions[product_id]
            del account.cost_basis[product_id]
        else:
            account.positions[product_id] = new_qty
        account.trade_count += 1
        self._record_trade(agent_id, action, product_id, quantity, price)
        return TradeResult(
            True,
            f"Sold {quantity} {product_id} @ ${price:,.2f} for ${proceeds:,.2f}. "
            f"Cash remaining: ${account.cash:,.2f}.",
        )

    def _record_trade(
        self,
        agent_id: str,
        action: str,
        product_id: str,
        quantity: int,
        price: float,
    ) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self._trade_log.append((ts, agent_id, action, product_id, quantity, price))
        if len(self._trade_log) > 20:
            self._trade_log.pop(0)


# ── Rich Live view ───────────────────────────────────────────────


class PortfolioView:
    """Builds and rerenders a Rich Live dashboard from AccountStore state."""

    def __init__(self, store: AccountStore) -> None:
        self._store = store
        self._live: Live | None = None

    def attach_live(self, live: Live) -> None:
        self._live = live

    def rerender(self) -> None:
        if self._live is not None:
            self._live.update(self._build_layout(), refresh=True)

    def _build_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="summary", size=7),
            Layout(name="positions", ratio=2),
            Layout(name="log", ratio=1),
            Layout(name="footer", size=3),
        )
        layout["header"].update(self._build_header())
        layout["summary"].update(self._build_summary_cards())
        layout["positions"].update(self._build_positions_table())
        layout["log"].update(self._build_trade_log())
        layout["footer"].update(self._build_footer())
        return layout

    def _build_header(self) -> Panel:
        now = datetime.now().strftime("%H:%M:%S")
        return Panel(
            Text.from_markup(f"[bold cyan]Portfolio Dashboard[/]  [dim]|  {now}[/]"),
            style="cyan",
            height=3,
        )

    def _build_summary_cards(self) -> Columns:
        accounts = self._store.accounts
        price_book = self._store.price_book

        cards = []
        for agent_id, account in accounts.items():
            value = account.portfolio_value(price_book)
            card = Panel(
                Text.from_markup(
                    f"[green]Cash:[/] ${account.cash:,.2f}\n"
                    f"[magenta]Value:[/] ${value:,.2f}\n"
                    f"[yellow]Positions:[/] {len(account.positions)}  "
                    f"[cyan]Trades:[/] {account.trade_count}"
                ),
                title=f"[bold]{agent_id}[/]",
                border_style="cyan",
            )
            cards.append(card)

        if not cards:
            cards.append(Panel("[dim]No accounts yet[/]", border_style="dim"))

        return Columns(cards, expand=True, equal=True)

    def _build_positions_table(self) -> Panel:
        table = Table(expand=True, show_lines=True)
        table.add_column("Agent", style="bold cyan", ratio=2)
        table.add_column("Trades", justify="right", ratio=1)
        table.add_column("Cash", justify="right", ratio=2)
        table.add_column("Ticker", ratio=2)
        table.add_column("Qty", justify="right", ratio=1)
        table.add_column("Cost Basis", justify="right", ratio=2)
        table.add_column("Mkt Value", justify="right", ratio=2)
        table.add_column("Total Value", justify="right", ratio=2)

        accounts = self._store.accounts
        price_book = self._store.price_book
        if not accounts:
            table.add_row("[dim]No accounts yet[/]", "", "", "", "", "", "", "")
        else:
            for agent_id, account in accounts.items():
                trades_str = str(account.trade_count)
                if not account.positions:
                    table.add_row(
                        agent_id,
                        trades_str,
                        f"[green]${account.cash:,.2f}[/]",
                        "[dim]—[/]",
                        "[dim]—[/]",
                        "[dim]—[/]",
                        "[dim]—[/]",
                        f"[bold]${account.portfolio_value(price_book):,.2f}[/]",
                    )
                else:
                    first = True
                    for pid, qty in sorted(account.positions.items()):
                        entry = price_book.get(pid)
                        price = float(entry["price"]) if entry else 0.0
                        mkt_val = price * qty
                        cost_basis = account.cost_basis.get(pid, 0.0)
                        table.add_row(
                            agent_id if first else "",
                            trades_str if first else "",
                            f"[green]${account.cash:,.2f}[/]" if first else "",
                            pid,
                            str(qty),
                            f"${cost_basis:,.2f}",
                            f"${mkt_val:,.2f}",
                            f"[bold]${account.portfolio_value(price_book):,.2f}[/]"
                            if first
                            else "",
                        )
                        first = False

        return Panel(table, title="[bold]Agent Portfolios[/]", border_style="green")

    def _build_trade_log(self) -> Panel:
        log = self._store.trade_log
        if not log:
            content = Text("No trades yet...", style="dim italic")
        else:
            from rich.console import Group

            rows = []
            for ts, agent_id, action, product_id, qty, price in log:
                action_style = "bold green" if action == "buy" else "bold red"
                rows.append(
                    Text.from_markup(
                        f"[dim]{ts}[/]  [{action_style}]{action.upper():>4}[/]  "
                        f"{qty:>6} {product_id:<12} @ ${price:>12,.2f}  "
                        f"[dim]{agent_id}[/]"
                    )
                )
            content = Group(*rows)

        return Panel(content, title="[bold]Trade Log[/]", border_style="yellow")

    def _build_footer(self) -> Panel:
        accounts = self._store.accounts
        total_trades = len(self._store.trade_log)
        text = (
            f"  [bold green]● LIVE[/]  [dim]|  "
            f"{len(accounts)} agent(s)  |  "
            f"{total_trades} trade(s)  |  "
            f"Ctrl+C to exit[/]"
        )
        return Panel(Text.from_markup(text), style="dim", height=3)


# ── Module-level singletons ──────────────────────────────────────

price_book = PriceBook()
store = AccountStore(price_book)
view = PortfolioView(store)


# ── Shared tool logic ────────────────────────────────────────────


def _execute_trade(agent_id: str, product_id: str, quantity: int, action: str) -> str:
    result = store.execute_trade(agent_id, product_id, quantity, action)
    view.rerender()
    return result.message


def _get_portfolio(agent_id: str) -> str:
    account = store.get_or_create(agent_id)
    pb = store.price_book

    lines = [f"Cash: ${account.cash:,.2f}"]

    if not account.positions:
        lines.append("Positions: none")
    else:
        lines.append("Positions:")
        for pid in sorted(account.positions):
            qty = account.positions[pid]
            avg_cost = account.avg_cost_per_unit(pid)
            total_cost = account.cost_basis.get(pid, 0.0)

            entry = pb.get(pid)
            if entry is not None:
                current_price = float(entry["price"])
                mkt_value = current_price * qty
                pnl = mkt_value - total_cost
                pnl_sign = "+" if pnl >= 0 else ""
                lines.append(
                    f"  {pid}: {qty} units | "
                    f"cost basis ${avg_cost:,.4f}/unit (${total_cost:,.2f} total) | "
                    f"current ${current_price:,.4f}/unit (${mkt_value:,.2f} mkt value) | "
                    f"P&L {pnl_sign}${pnl:,.2f}"
                )
            else:
                lines.append(
                    f"  {pid}: {qty} units | "
                    f"cost basis ${avg_cost:,.4f}/unit (${total_cost:,.2f} total) | "
                    f"current price unavailable"
                )

    portfolio_val = account.portfolio_value(pb)
    lines.append(f"Total portfolio value: ${portfolio_val:,.2f}")

    return "\n".join(lines)


# ── Tool factory ──────────────────────────────────────────────────


def make_trading_tools(agent_id: str) -> tuple[BaseToolNode, BaseToolNode]:
    """Create execute_trade and get_portfolio tool nodes for a given agent.

    Each returned tool has a unique ``__name__`` (and therefore unique Kafka
    topics) with the ``agent_id`` baked into its closure so callers never need
    to pass it explicitly.

    Args:
        agent_id: Unique identifier for the agent (e.g., "agent-A").

    Returns:
        A (execute_trade, get_portfolio) pair of BaseToolNode instances.
    """
    safe_id = agent_id.replace("-", "_")

    def execute_trade(product_id: str, quantity: int, action: str) -> str:
        """Execute a buy or sell trade for a crypto product.

        Args:
            product_id: The trading pair to trade (e.g., BTC-USD, ETH-USD, SOL-USD)
            quantity: The number of units to buy or sell (must be a positive integer)
            action: Either 'buy' or 'sell'

        Returns:
            A message describing the trade result including updated cash balance
        """
        return _execute_trade(agent_id, product_id, quantity, action)

    def get_portfolio() -> str:
        """Get the current portfolio summary including cash, positions, and cost basis.

        Returns:
            A detailed breakdown of cash held, each product owned with quantity,
            current market price, cost basis per unit, and total portfolio value
        """
        return _get_portfolio(agent_id)

    execute_trade.__name__ = f"execute_trade_{safe_id}"
    execute_trade.__qualname__ = f"execute_trade_{safe_id}"
    get_portfolio.__name__ = f"get_portfolio_{safe_id}"
    get_portfolio.__qualname__ = f"get_portfolio_{safe_id}"

    return agent_tool(execute_trade), agent_tool(get_portfolio)


# ── Agent tools (A / B / C) ─────────────────────────────────────


@agent_tool
def execute_trade_A(product_id: str, quantity: int, action: str) -> str:  # noqa: N802
    """Execute a buy or sell trade for a crypto product.

    Args:
        product_id: The trading pair to trade (e.g., BTC-USD, ETH-USD, SOL-USD)
        quantity: The number of units to buy or sell (must be a positive integer)
        action: Either 'buy' or 'sell'

    Returns:
        A message describing the trade result including updated cash balance
    """
    return _execute_trade(AGENT_ID_A, product_id, quantity, action)


@agent_tool
def execute_trade_B(product_id: str, quantity: int, action: str) -> str:  # noqa: N802
    """Execute a buy or sell trade for a crypto product.

    Args:
        product_id: The trading pair to trade (e.g., BTC-USD, ETH-USD, SOL-USD)
        quantity: The number of units to buy or sell (must be a positive integer)
        action: Either 'buy' or 'sell'

    Returns:
        A message describing the trade result including updated cash balance
    """
    return _execute_trade(AGENT_ID_B, product_id, quantity, action)


@agent_tool
def execute_trade_C(product_id: str, quantity: int, action: str) -> str:  # noqa: N802
    """Execute a buy or sell trade for a crypto product.

    Args:
        product_id: The trading pair to trade (e.g., BTC-USD, ETH-USD, SOL-USD)
        quantity: The number of units to buy or sell (must be a positive integer)
        action: Either 'buy' or 'sell'

    Returns:
        A message describing the trade result including updated cash balance
    """
    return _execute_trade(AGENT_ID_C, product_id, quantity, action)


@agent_tool
def get_portfolio_A() -> str:  # noqa: N802
    """Get the current portfolio summary including cash, positions, and cost basis.

    Returns:
        A detailed breakdown of cash held, each product owned with quantity,
        current market price, cost basis per unit, and total portfolio value
    """
    return _get_portfolio(AGENT_ID_A)


@agent_tool
def get_portfolio_B() -> str:  # noqa: N802
    """Get the current portfolio summary including cash, positions, and cost basis.

    Returns:
        A detailed breakdown of cash held, each product owned with quantity,
        current market price, cost basis per unit, and total portfolio value
    """
    return _get_portfolio(AGENT_ID_B)


@agent_tool
def get_portfolio_C() -> str:  # noqa: N802
    """Get the current portfolio summary including cash, positions, and cost basis.

    Returns:
        A detailed breakdown of cash held, each product owned with quantity,
        current market price, cost basis per unit, and total portfolio value
    """
    return _get_portfolio(AGENT_ID_C)


# ── Coinbase price feed ──────────────────────────────────────────


async def run_price_feed() -> None:
    """Background task that keeps the price book updated from Coinbase."""
    while True:
        try:
            async with websockets.connect(COINBASE_WS_URL) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "type": "subscribe",
                            "product_ids": SUBSCRIBED_PRODUCTS,
                            "channels": ["ticker_batch"],
                        }
                    )
                )
                logger.info("Coinbase price feed connected")

                async for raw in ws:
                    data = json.loads(raw)
                    if data.get("type") == "ticker":
                        price_book.update(data)
                        view.rerender()

        except websockets.ConnectionClosed:
            logger.warning(
                "Coinbase connection lost. Reconnecting in %ds...",
                RECONNECT_DELAY_SECONDS,
            )
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)
        except Exception:
            logger.exception(
                "Coinbase feed error. Reconnecting in %ds...",
                RECONNECT_DELAY_SECONDS,
            )
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)


# ── Entrypoint ───────────────────────────────────────────────────


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 50)
    print("Trading Tool Deployment")
    print("=" * 50)

    # Pre-create all agent accounts so they appear on the dashboard
    for agent_id in (AGENT_ID_A, AGENT_ID_B, AGENT_ID_C):
        store.get_or_create(agent_id)

    print(f"\nConnecting to Kafka broker at {KAFKA_BOOTSTRAP_SERVERS}...")
    broker = BrokerClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    service = NodesService(broker)
    all_tools = [
        execute_trade_A,
        execute_trade_B,
        execute_trade_C,
        get_portfolio_A,
        get_portfolio_B,
        get_portfolio_C,
    ]
    for tool in all_tools:
        service.register_node(tool)
        print(f"  - {tool.subscribed_topic} registered")

    print("\nStarting Coinbase price feed and portfolio dashboard...")

    with Live(view._build_layout(), auto_refresh=False, screen=True) as live:
        view.attach_live(live)
        price_feed_task = asyncio.create_task(run_price_feed())
        try:
            await service.run()
        finally:
            price_feed_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
