"""Ask the release manager for a go/no-go, and watch the review choreograph live.

`start()` + `handle.stream()` surface every step as it happens — the release
manager's three consults (messaging) and each expert's reply — across all depths, to
this caller. The release manager keeps control the whole time and synthesizes the
final call itself; there is no handoff. (See ../streaming for streaming on its own.)

Run it with:  python run.py   (start service.py first — see the README.)
"""

import asyncio
import json

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
        args = event.args
        if isinstance(args, str):  # the raw model emission may arrive as a JSON string
            try:
                args = json.loads(args)
            except ValueError:
                pass
        if event.name == "message_agent" and isinstance(args, dict):
            print(f"{pad}📨 [{event.emitter}] asks [{args.get('name')}]: {args.get('message')}")
        else:
            print(f"{pad}🔧 [{event.emitter}] {event.name}({event.args})")
    elif isinstance(event, ToolResultEvent):
        print(f"{pad}↩  [{event.name}] replies: {_text(event.parts)}")
    elif isinstance(event, HandoffEvent):
        print(f"{pad}🤝 [{event.emitter}] hands off → [{event.target}] ({event.reason})")


async def _ask(client: Client, prompt: str) -> None:
    print(f"\n>>> {prompt}")
    handle = await client.agent("release_manager").start(prompt)
    async for event in handle.stream():
        if isinstance(event, RunFailed):
            print(f"🛑 run failed: {event.report}")
            return
        if isinstance(event, RunCompleted):
            continue  # the cached terminal — read it via result() below
        _show(event)
    print(f"--- recommendation ---\n{(await handle.result()).output}")


async def main() -> None:
    async with Client.connect("localhost:9092") as client:
        await _ask(client, "Are we go for the v2.0 launch on Friday?")


if __name__ == "__main__":
    asyncio.run(main())
