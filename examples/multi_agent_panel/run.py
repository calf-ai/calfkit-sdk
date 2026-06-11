"""Drive a two-round panel discussion over ONE shared transcript.

Each panelist's response is threaded into the next call via ``message_history``, so
the same transcript accumulates turns from all three agents. Once it holds more than
one distinct agent identity, every subsequent invocation is automatically projected
to that agent's point of view — its own turns stay assistant messages, the other
panelists read as ``<optimist>`` / ``<skeptic>`` / ``<pragmatist>``, and the
moderator prompts read as ``<user>``. There are no flags to set; it just works.

Start ``service.py`` first, then run this.
"""

import asyncio

from panel import PANEL

from calfkit.client import Client

TOPIC = "Should our team adopt a four-day work week?"
FOLLOW_UP = "React to the points the others raised and refine your position."


async def main() -> None:
    client = Client.connect("localhost:9092")

    history: list = []  # the shared transcript, grown one turn at a time
    for round_no in (1, 2):
        prompt = TOPIC if round_no == 1 else FOLLOW_UP
        print(f"\n===== Round {round_no} =====")
        for agent in PANEL:
            result = await client.execute(
                prompt,
                f"{agent.name}.input",
                message_history=history,  # every panelist sees the same growing transcript
            )
            history = result.message_history  # accumulate this panelist's response
            print(f"\n[{agent.name}] {result.output}")

    distinct_agents = {m.name for m in history if getattr(m, "name", None)}
    print(
        f"\nShared transcript: {len(history)} messages from {len(distinct_agents)} distinct agents "
        f"({', '.join(sorted(distinct_agents))}). Each agent saw the others as attributed participants."
    )


if __name__ == "__main__":
    asyncio.run(main())
