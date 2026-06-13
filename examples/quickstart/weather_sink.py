"""Independently deployable consumer node.

Listens on the weather agent's ``publish_topic`` and runs arbitrary Python
against every envelope that flows through it. The decorated function receives
the same client-facing ``NodeResult`` returned by ``Client.execute()``.

Because an agent's ``publish_topic`` carries every state transition (not just
the final reply), ``result.output`` is ``None`` on intermediate hops — pending
tool calls, tool returns, agent retries. Filter via a gate if you only want
agent terminals:

    gates=[lambda ctx: bool(ctx.output_parts)]

Run alongside the agent service:

    python weather_sink.py
"""

import asyncio

from calfkit.client import Client, NodeResult
from calfkit.nodes import consumer
from calfkit.worker import Worker


@consumer(subscribe_topics="weather_agent.output")
async def log_weather_output(result: NodeResult) -> None:
    if result.output is None:
        # Intermediate hop (e.g. agent about to call a tool). Observe and move on.
        print(f"[{result.correlation_id[:8]}] intermediate hop from {result.emitter_node_id}")
        return
    print(f"[{result.correlation_id[:8]}] final from {result.emitter_node_id}: {result.output!r}")


async def main() -> None:
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[log_weather_output])
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
