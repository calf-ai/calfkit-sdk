"""
Trading Tool — an @agent_tool that executes buy/sell trades against
an in-memory portfolio store and rerenders a Rich Live dashboard
after every trade.

Prices are sourced from a Kafka topic (market_data.prices) published
by the Coinbase connector, which keeps a live price book.

The account store is keyed by agent_id so multiple agent runtimes
can each maintain independent portfolios.  The agent_id is resolved
at runtime via ToolContext injection (ctx.agent_name).

Usage:
    uv run python examples/trading_tool.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import typing
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime

import plotext as plt
import sympy
from rich.ansi import AnsiDecoder
from rich.columns import Columns
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from calfkit.broker.broker import BrokerClient
from calfkit.models.tool_context import ToolContext
from calfkit.nodes.base_tool_node import agent_tool
from calfkit.runners.service import NodesService
from examples.daytrading_agents_arena.coinbase_consumer import PriceBook

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────

INITIAL_CASH = 100_000.0

MAX_BALANCE_HISTORY = 300  # ~25 min at 5s intervals

AGENT_COLORS: dict[str, str] = {
    "momentum": "cyan",
    "brainrot-daytrader": "magenta",
    "scalper": "yellow",
}
_FALLBACK_COLORS = ["green", "red", "blue", "orange", "white"]


# ── Data model ───────────────────────────────────────────────────


@dataclass
class TradeResult:
    success: bool
    message: str


@dataclass
class AgentAccount:
    cash: float = INITIAL_CASH
    positions: dict[str, float] = field(default_factory=dict)
    cost_basis: dict[str, float] = field(default_factory=dict)
    # Weighted-average entry timestamp (Unix epoch) per position
    avg_entry_ts: dict[str, float] = field(default_factory=dict)
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
        self._trade_log: list[tuple[str, str, str, str, float, float, float | None]] = []
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
    def trade_log(self) -> list[tuple[str, str, str, str, float, float, float | None]]:
        return self._trade_log

    def execute_trade(
        self,
        agent_id: str,
        product_id: str,
        quantity: float,
        action: str,
        latency: float | None = None,
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
            return TradeResult(False, "Quantity must be positive.")

        rounded = round(quantity, 1)
        if abs(quantity - rounded) > 1e-9:
            return TradeResult(
                False, "Quantity must have at most 1 decimal place (e.g., 0.5, 1.2)."
            )
        quantity = rounded

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
            existing_qty = account.positions.get(product_id, 0)
            now_ts = datetime.now().timestamp()
            existing_ts = account.avg_entry_ts.get(product_id, now_ts)
            account.avg_entry_ts[product_id] = (existing_qty * existing_ts + quantity * now_ts) / (
                existing_qty + quantity
            )
            account.positions[product_id] = existing_qty + quantity
            account.cost_basis[product_id] = account.cost_basis.get(product_id, 0.0) + cost
            account.trade_count += 1
            self._record_trade(agent_id, action, product_id, quantity, price, latency)
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
        new_qty = round(held - quantity, 1)
        if new_qty <= 0:
            del account.positions[product_id]
            del account.cost_basis[product_id]
            account.avg_entry_ts.pop(product_id, None)
        else:
            account.positions[product_id] = new_qty
        account.trade_count += 1
        self._record_trade(agent_id, action, product_id, quantity, price, latency)
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
        latency: float | None = None,
    ) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self._trade_log.append((ts, agent_id, action, product_id, quantity, price, latency))


# ── Rich Live view ───────────────────────────────────────────────


class PlotextChart:
    """Rich-compatible renderable that draws a plotext line chart."""

    def __init__(
        self,
        balance_history: dict[str, deque[tuple[str, float]]],
        chart_height: int = 12,
    ) -> None:
        self._balance_history = balance_history
        self._chart_height = chart_height

    def __rich_console__(
        self, console: object, options: object
    ) -> typing.Generator[Text, None, None]:
        width = getattr(options, "max_width", 80)

        plt.clf()
        plt.plotsize(width, self._chart_height)
        plt.theme("dark")
        plt.title("Portfolio Value Over Time")
        plt.ylabel("USD")

        has_data = any(len(d) > 0 for d in self._balance_history.values())

        if not has_data:
            plt.plot([0, 1], [INITIAL_CASH, INITIAL_CASH], label="waiting...", color="gray")
        else:
            # Right-align all series so the latest snapshot is always at
            # the right edge, regardless of when each agent started.
            max_len = max(len(h) for h in self._balance_history.values())

            fallback_idx = 0
            for agent_id, history in self._balance_history.items():
                if not history:
                    continue
                timestamps, values = zip(*history)
                n = len(values)
                offset = max_len - n
                x_indices = list(range(offset, offset + n))
                color = AGENT_COLORS.get(agent_id)
                if color is None:
                    color = _FALLBACK_COLORS[fallback_idx % len(_FALLBACK_COLORS)]
                    fallback_idx += 1
                plt.plot(x_indices, list(values), label=agent_id, color=color, marker="braille")

            # Build evenly-spaced time tick labels from the longest series
            longest = max(self._balance_history.values(), key=len)
            n = len(longest)
            num_ticks = min(7, n)
            if num_ticks > 1:
                step = (n - 1) / (num_ticks - 1)
                positions = [int(round(i * step)) for i in range(num_ticks)]
            else:
                positions = [0]
            labels = [longest[p][0] for p in positions]
            plt.xticks(positions, labels)

        canvas = plt.build()
        decoder = AnsiDecoder()
        yield from decoder.decode(canvas)


class PortfolioView:
    """Builds and rerenders a Rich Live dashboard from AccountStore state."""

    def __init__(self, store: AccountStore) -> None:
        self._store = store
        self._live: Live | None = None
        self._balance_history: dict[str, deque[tuple[str, float]]] = {}

    def attach_live(self, live: Live) -> None:
        self._live = live

    def rerender(self) -> None:
        if self._live is not None:
            self._capture_balance_snapshot()
            self._live.update(self._build_layout(), refresh=True)

    def _capture_balance_snapshot(self) -> None:
        price_book = self._store.price_book
        ts = datetime.now().strftime("%H:%M:%S")
        for agent_id, account in self._store.accounts.items():
            if agent_id not in self._balance_history:
                self._balance_history[agent_id] = deque(maxlen=MAX_BALANCE_HISTORY)
            value = account.portfolio_value(price_book)
            self._balance_history[agent_id].append((ts, value))

    def _build_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="summary_header", size=1),
            Layout(name="summary", size=7),
            Layout(name="body", ratio=2),
            Layout(name="chart", size=15),
        )
        layout["header"].update(self._build_header())
        layout["summary_header"].update(
            Text.from_markup("[bold]Agent Account Summaries[/]", justify="center")
        )
        layout["summary"].update(self._build_summary_cards())
        layout["body"].split_row(
            Layout(name="positions", ratio=3),
            Layout(name="log", ratio=2),
        )
        layout["positions"].update(self._build_positions_table())
        layout["log"].update(self._build_trade_log())
        layout["chart"].update(self._build_chart())
        return layout

    def _build_chart(self) -> Panel:
        chart = PlotextChart(self._balance_history, chart_height=12)
        return Panel(chart, border_style="blue")

    def _build_header(self) -> Panel:
        now = datetime.now().strftime("%H:%M:%S")
        return Panel(
            Text.from_markup(
                "[bold cyan]Portfolio Dashboard[/]  [bold red]●[/] "
                f"[bold green]LIVE[/]  [dim]|  {now}[/]"
            ),
            style="cyan",
            height=3,
        )

    def _build_summary_cards(self) -> Columns:
        accounts = self._store.accounts
        price_book = self._store.price_book

        cards = []
        sorted_accounts = sorted(
            accounts.items(),
            key=lambda item: item[1].portfolio_value(price_book),
            reverse=True,
        )
        for rank, (agent_id, account) in enumerate(sorted_accounts, start=1):
            value = account.portfolio_value(price_book)
            card = Panel(
                Text.from_markup(
                    f"[magenta]Total Value:[/] ${value:,.2f}\n"
                    f"[yellow]Positions:[/] {len(account.positions)}  "
                    f"[cyan]Trades:[/] {account.trade_count}"
                ),
                title=f"[bold]#{rank} {agent_id}[/]",
                border_style="cyan",
            )
            cards.append(card)

        if not cards:
            cards.append(Panel("[dim]No accounts yet[/]", border_style="dim"))

        return Columns(cards, expand=True, equal=True)

    def _build_positions_table(self) -> Panel:
        table = Table(expand=True, show_lines=False)
        table.add_column("Agent", style="bold cyan", ratio=2)
        table.add_column("Trades", justify="right", ratio=1)
        table.add_column("Cash", justify="right", ratio=2)
        table.add_column("Ticker", ratio=2)
        table.add_column("Qty", justify="right", ratio=1)
        table.add_column("Cost Basis", justify="right", ratio=2)
        table.add_column("Mkt Value", justify="right", ratio=2)
        table.add_column("P&L", justify="right", ratio=2)
        table.add_column("Total Value", justify="right", ratio=2)

        accounts = self._store.accounts
        price_book = self._store.price_book
        if not accounts:
            table.add_row("[dim]No accounts yet[/]", "", "", "", "", "", "", "", "")
        else:
            first = True
            for agent_id, account in accounts.items():
                if not first:
                    table.add_section()
                first = False
                total_value = account.portfolio_value(price_book)
                total_pnl = total_value - INITIAL_CASH
                # Agent header row with cash
                table.add_row(
                    agent_id,
                    str(account.trade_count),
                    f"[green]${account.cash:,.2f}[/]",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                )
                # Individual ticker rows
                if not account.positions:
                    table.add_row(
                        "",
                        "",
                        "",
                        "[dim]—[/]",
                        "[dim]—[/]",
                        "[dim]—[/]",
                        "[dim]—[/]",
                        "[dim]—[/]",
                        "",
                    )
                else:
                    for pid, qty in sorted(account.positions.items()):
                        entry = price_book.get(pid)
                        price = float(entry["price"]) if entry else 0.0
                        mkt_val = price * qty
                        cost_basis = account.cost_basis.get(pid, 0.0)
                        pnl = mkt_val - cost_basis
                        pnl_color = "green" if pnl >= 0 else "red"
                        pnl_sign = "+" if pnl >= 0 else ""
                        table.add_row(
                            "",
                            "",
                            "",
                            pid,
                            f"{qty:g}",
                            f"${cost_basis:,.2f}",
                            f"${mkt_val:,.2f}",
                            f"[{pnl_color}]{pnl_sign}${pnl:,.2f}[/]",
                            "",
                        )
                # Total value row
                total_pnl_color = "green" if total_pnl >= 0 else "red"
                total_pnl_sign = "+" if total_pnl >= 0 else ""
                table.add_row(
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "[bold]Total[/]",
                    f"[bold {total_pnl_color}]{total_pnl_sign}${total_pnl:,.2f}[/]",
                    f"[bold]${total_value:,.2f}[/]",
                )

        return Panel(table, title="[bold]Agent Portfolios[/]", border_style="green")

    def _build_trade_log(self) -> Panel:
        table = Table(expand=True, show_lines=False, show_header=True, box=None)
        table.add_column("Time", style="dim", ratio=1)
        table.add_column("Action", ratio=1)
        table.add_column("Qty", justify="right", ratio=1)
        table.add_column("Ticker", ratio=2)
        table.add_column("Unit Price", justify="right", ratio=2)
        table.add_column("Agent", style="dim", ratio=2)
        table.add_column("Latency", justify="right", style="dim", ratio=1)

        log = self._store.trade_log
        if not log:
            table.add_row("[dim italic]No trades yet...[/]", "", "", "", "", "", "")
        else:
            for ts, agent_id, action, product_id, qty, price, latency in reversed(log):
                action_style = "bold green" if action == "buy" else "bold red"
                latency_str = f"{latency:.1f}s" if latency is not None else ""
                table.add_row(
                    ts,
                    f"[{action_style}]{action.upper()}[/]",
                    f"{qty:g}",
                    product_id,
                    f"${price:,.2f}",
                    agent_id,
                    latency_str,
                )

        return Panel(table, title="[bold]Trade Log (most recent)[/]", border_style="yellow")


# ── Module-level singletons ──────────────────────────────────────

price_book = PriceBook()
store = AccountStore(price_book)
view = PortfolioView(store)


# ── Shared tool logic ────────────────────────────────────────────


def _execute_trade(
    agent_id: str, product_id: str, quantity: float, action: str, latency: float | None = None
) -> str:
    result = store.execute_trade(agent_id, product_id, quantity, action, latency=latency)
    view.rerender()
    return result.message


def _format_hold_time(entry_ts: float | None) -> str:
    """Format elapsed time since entry as a human-readable string."""
    if entry_ts is None:
        return "N/A"
    seconds = datetime.now().timestamp() - entry_ts
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{int(minutes)}m"
    hours = minutes / 60
    if hours < 24:
        return f"{hours:.1f}h"
    return f"{hours / 24:.1f}d"


def _get_portfolio(agent_id: str) -> str:
    account = store.get_or_create(agent_id)
    pb = store.price_book

    lines = [f"Cash: ${account.cash:,.2f}"]

    if not account.positions:
        lines.append("Positions: none")
    else:
        lines.append(
            "| Ticker | Qty | Avg Cost | Total Cost "
            "| Current Price | Mkt Value | P&L | Avg Time Held |"
        )
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
        for pid in sorted(account.positions):
            qty = account.positions[pid]
            avg_cost = account.avg_cost_per_unit(pid)
            total_cost = account.cost_basis.get(pid, 0.0)
            hold_str = _format_hold_time(account.avg_entry_ts.get(pid))

            entry = pb.get(pid)
            if entry is not None:
                current_price = float(entry["price"])
                mkt_value = current_price * qty
                pnl = mkt_value - total_cost
                pnl_sign = "+" if pnl >= 0 else ""
                lines.append(
                    f"| {pid} | {qty:g} | ${avg_cost:,.2f} | ${total_cost:,.2f} "
                    f"| ${current_price:,.2f} | ${mkt_value:,.2f} "
                    f"| {pnl_sign}${pnl:,.2f} | {hold_str} |"
                )
            else:
                lines.append(
                    f"| {pid} | {qty:g} | ${avg_cost:,.2f} | ${total_cost:,.2f} "
                    f"| N/A | N/A | N/A | {hold_str} |"
                )

    portfolio_val = account.portfolio_value(pb)
    lines.append(f"\nTotal portfolio value: ${portfolio_val:,.2f}")

    return "\n".join(lines)


# ── Shared agent tools (ToolContext injection) ───────────────────


@agent_tool
def execute_trade(ctx: ToolContext, product_id: str, quantity: float, action: str) -> str:
    """Execute a buy or sell trade (fill-or-cancel). The order fills immediately at the current
    market price if possible, or returns an error if it cannot be filled — it never waits or queues.
    Buys execute at the best ask price, sells at the best bid.
    Fractional share trading is allowed, but only to one decimal place (e.g., 0.5, 1.2).

    Args:
        product_id: Trading pair (e.g., BTC-USD, FARTCOIN-USD, SOL-USD)
        quantity: Number of units to trade (positive, up to 1 decimal place)
        action: 'buy' or 'sell'

    Returns:
        Trade confirmation with execution price and remaining cash, or an error message
    """
    latency: float | None = None
    if isinstance(ctx.deps, dict) and "invoked_at" in ctx.deps:
        latency = time.time() - ctx.deps["invoked_at"]
    return _execute_trade(ctx.agent_name, product_id, quantity, action, latency=latency)


@agent_tool
def get_portfolio(ctx: ToolContext) -> str:
    """View your portfolio: available cash, open positions, and total value.

    Returns:
        A table of positions with quantity, average cost basis, current market
        price, unrealized P&L, and average time held — plus cash and total value
    """
    return _get_portfolio(ctx.agent_name)


@agent_tool
def calculator(ctx: ToolContext, expression: str) -> str:
    """Evaluate a math expression. Use for financial calculations you can't do in your head,
    such as position sizing, P&L, percentage changes, or risk/reward ratios.

    Respects standard order of operations (PEMDAS).
    Supported operators: +, -, *, /, ** (power), % (modulo), parentheses for grouping.
    Functions: abs(), sqrt(), log(), floor(), ceil(), min(), max().

    Args:
        expression: A math expression (e.g., '100000 * 0.02', '64200 / 3',
            '(50000 - 32100) / 32100 * 100', 'max(10, 20)')

    Returns:
        The numeric result
    """

    try:
        result = sympy.sympify(expression)
        return str(result.evalf() if not result.is_number else result)
    except (sympy.SympifyError, TypeError) as e:
        return f"Invalid expression: {e}"


# ── Entrypoint ───────────────────────────────────────────────────


async def main() -> None:
    from examples.daytrading_agents_arena.coinbase_kafka_connector import (
        PRICE_TOPIC,
        TickerMessage,
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 50)
    print("Trading Tool Deployment")
    print("=" * 50)

    print(f"\nConnecting to Kafka broker at {KAFKA_BOOTSTRAP_SERVERS}...")
    broker = BrokerClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    service = NodesService(broker)
    all_tools = [execute_trade, get_portfolio, calculator]
    for tool in all_tools:
        service.register_node(tool)
        print(f"  - {tool.subscribed_topic} registered")

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
        pass
