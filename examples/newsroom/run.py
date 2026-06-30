"""Give the editor a story brief, and watch the desk choreograph it live.

`start()` + `handle.stream()` surface every step as it happens — the editor's
consults of the researcher and fact-checker (messaging) and the handoff to the
writer — across all depths, to this caller. The writer answers you directly because
the editor handed the conversation off. (See ../streaming for streaming on its own.)

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
    """Render one intermediate step event, indented by call depth (a consulted peer streams deeper)."""
    pad = "  " * (event.depth - 1)
    if isinstance(event, AgentMessageEvent):
        if text := _text(event.parts):
            print(f"{pad}💬 [{event.emitter}] {text}")
    elif isinstance(event, ToolCallEvent):
        # message_agent renders like any tool call — name({json args}) — so the raw JSON is visible.
        print(f"{pad}🔧 [{event.emitter}] {event.name}({event.args})")
    elif isinstance(event, ToolResultEvent):
        print(f"{pad}↩  [{event.name}] replies: {_text(event.parts)}")
    elif isinstance(event, HandoffEvent):
        print(f"{pad}🤝 [{event.emitter}] hands off to [{event.target}] ({event.reason})")


async def _ask(client: Client, prompt: str) -> None:
    print(f"\n>>> {prompt}")
    handle = await client.agent("editor").start(prompt)
    async for event in handle.stream():
        if isinstance(event, RunFailed):
            print(f"🛑 run failed: {event.report}")
            return
        if isinstance(event, RunCompleted):
            continue  # the cached terminal — read it via result() below
        _show(event)
    print(f"--- the writer's brief ---\n{(await handle.result()).output}")


async def main() -> None:
    async with Client.connect("localhost:9092") as client:
        await _ask(client, "Write a short news brief about the city's new downtown bike-share program.")


if __name__ == "__main__":
    asyncio.run(main())
