"""Agent Activity Viewer — a standalone Rich Live dashboard that subscribes
to agent_router.output and displays all agent activity (tool calls, text
responses, and tool results) as they happen at every turn of the agentic loop.

Run this in a separate terminal alongside the main tools_and_dashboard
to get visibility into agent reasoning.

Example:
    uv run python examples/daytrading_agents_arena/response_viewer.py \
        --bootstrap-servers 46.225.138.55:9092

Prerequisites:
    - Kafka broker running
    - At least one agent router publishing to agent_router.output
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime

from dotenv import load_dotenv
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table, box
from rich.text import Text

from calfkit._vendor.pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from calfkit.broker.broker import BrokerClient
from calfkit.models.event_envelope import EventEnvelope

load_dotenv()

logger = logging.getLogger(__name__)


# ── Data model ───────────────────────────────────────────────────


@dataclass
class ActivityEntry:
    timestamp: str  # HH:MM:SS
    agent_name: str
    kind: str  # "TOOL CALL", "RESPONSE", "TOOL RESULT"
    details: str  # Formatted display string


# ── Style constants ──────────────────────────────────────────────

KIND_STYLES: dict[str, str] = {
    "TOOL CALL": "bold yellow",
    "RESPONSE": "bold green",
    "TOOL RESULT": "bold blue",
}


# ── Rich Live view ───────────────────────────────────────────────


class ActivityView:
    """Builds and rerenders a Rich Live dashboard showing all agent activity."""

    def __init__(self) -> None:
        self._log: list[ActivityEntry] = []
        self._seen: set[tuple[str, int]] = set()
        self._live: Live | None = None

    def attach_live(self, live: Live) -> None:
        self._live = live

    def record(
        self,
        agent_name: str,
        kind: str,
        details: str,
        trace_id: str | None = None,
        history_len: int = 0,
    ) -> None:
        if trace_id:
            key = (trace_id, history_len)
            if key in self._seen:
                return
            self._seen.add(key)
        ts = datetime.now().strftime("%H:%M:%S")
        self._log.append(
            ActivityEntry(timestamp=ts, agent_name=agent_name, kind=kind, details=details)
        )
        self._rerender()

    def _rerender(self) -> None:
        if self._live is not None:
            self._live.update(self._build_layout(), refresh=True)

    def _build_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body", ratio=1),
        )
        layout["header"].update(self._build_header())
        layout["body"].update(self._build_activity_log())
        return layout

    def _build_header(self) -> Panel:
        now = datetime.now().strftime("%H:%M:%S")
        count = len(self._log)
        return Panel(
            Text.from_markup(
                "[bold cyan]Agent Activity Viewer[/]  [bold red]●[/] "
                f"[bold green]LIVE[/]  [dim]|  {now}  |  "
                f"{count} event{'s' if count != 1 else ''}[/]"
            ),
            style="cyan",
            height=3,
        )

    def _build_activity_log(self) -> Panel:
        table = Table(expand=True, show_lines=True, show_header=True, box=box.HORIZONTALS)
        table.add_column("Time", style="dim", width=10, no_wrap=True)
        table.add_column("Agent", style="bold cyan", width=22, no_wrap=True)
        table.add_column("Type", width=13, no_wrap=True)
        table.add_column("Details", no_wrap=False)

        if not self._log:
            table.add_row("[dim italic]Waiting for agent activity...[/]", "", "", "")
        else:
            for entry in reversed(self._log):
                style = KIND_STYLES.get(entry.kind, "")
                kind_text = Text(entry.kind, style=style)
                table.add_row(entry.timestamp, entry.agent_name, kind_text, entry.details.strip())

        return Panel(
            table,
            title="[bold]Agent Activity (most recent first)[/]",
            border_style="bright_green",
        )


# ── Helpers ──────────────────────────────────────────────────────


def _format_tool_call(part: ToolCallPart) -> str:
    """Format a tool call as tool_name(arg=val, ...)."""
    try:
        args = part.args_as_dict()
    except Exception:
        args = {}
    if args:
        params = ", ".join(f"{k}={_truncate(json.dumps(v), 80)}" for k, v in args.items())
        return f"{part.tool_name}({params})"
    return f"{part.tool_name}()"


def _truncate(s: str, max_len: int) -> str:
    """Truncate a string with ellipsis if it exceeds max_len."""
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "\u2026"


# ── CLI ──────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deploy a live agent activity viewer.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Kafka bootstrap servers address",
    )
    return parser.parse_args()


# ── Entrypoint ───────────────────────────────────────────────────

view = ActivityView()


async def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 50)
    print("Agent Activity Viewer")
    print("=" * 50)

    print(f"\nConnecting to Kafka broker at {args.bootstrap_servers}...")
    broker = BrokerClient(bootstrap_servers=args.bootstrap_servers)

    @broker.subscriber("agent_router.output", group_id="activity-viewer")
    async def handle_agent_activity(envelope: EventEnvelope) -> None:
        last_msg = envelope.latest_message_in_history
        if last_msg is None:
            return

        agent_name = envelope.agent_name or "unknown"
        trace_id = envelope.trace_id
        history_len = len(envelope.message_history)

        if isinstance(last_msg, ModelResponse):
            tool_calls = [p for p in last_msg.parts if isinstance(p, ToolCallPart)]
            text_parts = [p.content for p in last_msg.parts if isinstance(p, TextPart)]

            if tool_calls:
                lines = [_format_tool_call(tc) for tc in tool_calls]
                if text_parts:
                    lines.append(f'"{" ".join(text_parts)}"')
                view.record(
                    agent_name=agent_name,
                    kind="TOOL CALL",
                    details="\n".join(lines),
                    trace_id=trace_id,
                    history_len=history_len,
                )
            elif text_parts:
                view.record(
                    agent_name=agent_name,
                    kind="RESPONSE",
                    details=" ".join(text_parts),
                    trace_id=trace_id,
                    history_len=history_len,
                )

        elif isinstance(last_msg, ModelRequest):
            tool_returns = [p for p in last_msg.parts if isinstance(p, ToolReturnPart)]
            if tool_returns:
                lines = [
                    f"{tr.tool_name} → {_truncate(tr.model_response_str(), 200)}"
                    for tr in tool_returns
                ]
                view.record(
                    agent_name=agent_name,
                    kind="TOOL RESULT",
                    details="\n".join(lines),
                    trace_id=trace_id,
                    history_len=history_len,
                )
            # Skip ModelRequest with only UserPromptPart (not interesting)

    print("\nStarting activity viewer (subscribing to agent_router.output)...")

    with Live(view._build_layout(), auto_refresh=False, screen=True) as live:
        view.attach_live(live)
        await broker.run_app()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nActivity viewer stopped.")
