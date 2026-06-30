"""Submit one big expense and watch it climb the approval chain live.

`start()` + `handle.stream()` surface every step as it happens — each approver's
handoff up the chain — to this caller. Whoever is authorized approves it and answers
you directly, because each handoff transfers the conversation. (See ../streaming for
streaming on its own.)

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
    handle = await client.agent("team_lead").start(prompt)
    async for event in handle.stream():
        if isinstance(event, RunFailed):
            print(f"🛑 run failed: {event.report}")
            return
        if isinstance(event, RunCompleted):
            continue  # the cached terminal — read it via result() below
        _show(event)
    print(f"--- decision ---\n{(await handle.result()).output}")


async def main() -> None:
    async with Client.connect("localhost:9092") as client:
        # $40,000 is over the team lead's and the director's limits, so it climbs to the VP.
        await _ask(client, "Please approve a $40,000 expense for our annual company offsite.")


if __name__ == "__main__":
    asyncio.run(main())
