"""Bridge worker — hosts the MCP subprocess and dispatches tool calls.

Subscribes to ``mcp.everything.<tool>.input`` topics on Kafka. On each
envelope, forwards the call to the long-lived ``ClientSession`` (which
talks to the npx-spawned ``@modelcontextprotocol/server-everything``
subprocess) and publishes the result back.

Run alongside agent_service.py. Either can come up first — Kafka
buffers envelopes if the bridge isn't ready, and the bridge spawns its
MCP subprocess at startup before consuming.
"""

import asyncio

from shared import everything

from calfkit.client import Client
from calfkit.worker import Worker


async def main() -> None:
    client = Client.connect("localhost:9092")
    # The same `everything` McpServer object as agent_service imports —
    # but here it's in `nodes=` so the Worker spawns the MCP subprocess
    # and constructs an McpBridge per declared tool.
    worker = Worker(client, nodes=[everything])
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
