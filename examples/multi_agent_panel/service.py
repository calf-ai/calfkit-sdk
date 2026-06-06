"""Deploy the panel agents on a single worker.

Run this first (it blocks), then run ``run.py`` in another terminal. Requires a
running Calfkit broker on ``localhost:9092`` (see the repo README, "Start a
Calfkit Broker").
"""

import asyncio

from panel import PANEL

from calfkit.client import Client
from calfkit.worker import Worker


async def main() -> None:
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=PANEL)  # all three panelists on one worker
    await worker.run()  # (blocking) serve the panel


if __name__ == "__main__":
    asyncio.run(main())
