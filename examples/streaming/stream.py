"""Stream an agent's intermediate work to the caller as it happens.

``start()`` dispatches the run and returns a handle. ``handle.stream()`` yields
this run's events in order — zero or more intermediate **step events** (the
agent's preamble, each tool call, each tool result) and then the terminal
(``RunCompleted`` / ``RunFailed``) as the last element. The terminal is cached,
so ``handle.result()`` afterwards replays it as the typed final answer.

Step events are best-effort and carry **raw** parts (not the run's typed
output); the held handle is the lossless path. See
``docs/client-features.md`` → "Observing results" for the full contract.

Run it with:  python stream.py
(Start ``service.py`` first — see this folder's README.)
"""

import asyncio

from calfkit import (
    AgentMessageEvent,
    HandoffEvent,
    RunCompleted,
    RunFailed,
    ToolCallEvent,
    ToolResultEvent,
)
from calfkit.client import Client


def _text(parts) -> str:
    """Join the text of any TextParts in a step event's raw parts (for display)."""
    return " ".join(p.text for p in parts if p.kind == "text").strip()


async def main() -> None:
    async with Client.connect("localhost:9092") as client:
        handle = await client.agent("trip_planner").start("I'm visiting Kyoto tomorrow — what should I do?")

        print("── live progress ─────────────────────────────")
        # The stream yields intermediates in order, then the terminal as its last
        # element; the loop ends naturally after the terminal (no break needed).
        async for event in handle.stream():
            match event:
                case AgentMessageEvent(parts=parts):
                    print(f"  💬 {_text(parts)}")
                case ToolCallEvent(name=name, args=args):
                    print(f"  🔧 calling {name}({args})")
                case ToolResultEvent(name=name, parts=parts, outcome=outcome):
                    mark = {"failed": "⚠️ failed", "denied": "🚫 denied"}.get(outcome, "✅")
                    print(f"  {mark} {name} → {_text(parts)}")
                case HandoffEvent(target=target, reason=reason):
                    # Not emitted by this single-agent run — appears when an agent hands
                    # off to a peer (a consult streams the peer's own steps instead;
                    # see docs/agent-peers.md).
                    print(f"  🤝 handoff → {target}: {reason}")
                case RunCompleted():
                    print("  🏁 run completed")
                case RunFailed():
                    print("  🛑 run failed")
        print("──────────────────────────────────────────────")

        # Same channel as the stream: the cached terminal, projected to the typed output.
        result = await handle.result()
        print(f"\nFinal plan:\n{result.output}")


if __name__ == "__main__":
    asyncio.run(main())
