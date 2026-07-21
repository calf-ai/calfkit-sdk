"""Deploy the trip planner and its two tools on a single worker.

Run this first (it blocks), then run ``stream.py`` in another terminal. Requires
a running Calfkit broker on ``localhost:9092`` (see the repo README, "Start a
Calfkit Broker") and ``export OPENAI_API_KEY=sk-...``.
"""

import asyncio

from planner_agent import agent
from trip_tools import find_activities, get_weather

from calfkit.client import Client
from calfkit.worker import Worker


async def main() -> None:
    client = Client.connect("localhost:9092")
    # StatelessAgent + both tool nodes on one worker — the agent's tool Calls are served in-process.
    worker = Worker(client, nodes=[agent, get_weather, find_activities])
    await worker.run()  # (blocking) serve the trip planner


if __name__ == "__main__":
    asyncio.run(main())
