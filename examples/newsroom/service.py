"""Deploy the newsroom — editor, researcher, fact-checker, writer — on one worker.

Run this first (it blocks), then run `run.py` in another terminal. Requires a
running Calfkit broker on `localhost:9092` (see the repo README, "Running your
agents") and `export OPENAI_API_KEY=sk-...`.
"""

import asyncio

from agents import NODES

from calfkit.client import Client
from calfkit.worker import Worker


async def main() -> None:
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=NODES)  # all four agents + the two tools on one worker
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
