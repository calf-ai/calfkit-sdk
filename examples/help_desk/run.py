"""Ask the help desk two things and watch it route to the right expert.

Run it with:  python run.py   (start service.py first — see the README.)
"""

import asyncio

from calfkit import (
    AgentMessageEvent,
    Client,
    HandoffEvent,
    RunCompleted,
    RunFailed,
    ToolCallEvent,
    ToolResultEvent,
)


def _text(parts) -> str:
    return " ".join(p.text for p in parts if p.kind == "text").strip()


def _show(event) -> None:
    """Render one intermediate step event, indented by call depth."""
    pad = "  " * (event.depth - 1)
    if isinstance(event, AgentMessageEvent):
        if text := _text(event.parts):
            print(f"{pad}💬 [{event.emitter}] {text}")
    elif isinstance(event, ToolCallEvent):
        print(f"{pad}🔧 [{event.emitter}] {event.name}({event.args})")
    elif isinstance(event, ToolResultEvent):
        print(f"{pad}↩  [{event.name}] replies: {_text(event.parts)}")
    elif isinstance(event, HandoffEvent):
        print(f"{pad}🤝 [{event.emitter}] hands off to [{event.target}] ({event.reason})")


async def _ask(client: Client, prompt: str) -> None:
    print(f"\n>>> {prompt}")
    handle = await client.agent("help_desk").start(prompt)
    async for event in handle.stream():
        if isinstance(event, RunFailed):
            print(f"🛑 run failed: {event.report}")
            return
        if isinstance(event, RunCompleted):
            continue  # the cached terminal — read it via result() below
        _show(event)
    print(f"--- answer ---\n{(await handle.result()).output}")


async def main() -> None:
    async with Client.connect("localhost:9092") as client:
        await _ask(client, "How many vacation days do I have left? I'm Sam Rivera.")  # → messages hr
        await _ask(client, "Please file a $400 reimbursement for my conference travel.")  # → hands off to finance


if __name__ == "__main__":
    asyncio.run(main())
