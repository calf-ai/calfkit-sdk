"""Independently deployable consumer node.

Listens on the weather agent's ``publish_topic`` and runs arbitrary Python
against every envelope that flows through it. The decorated function receives a
``ConsumerContext`` for every envelope on the topic.

Because an agent's ``publish_topic`` carries every state transition (not just
the final reply), ``ctx.output`` is ``None`` on intermediate hops — pending
tool calls, tool returns, agent retries. A consumer observes every event; to act
only on agent terminals, return early on those hops (as ``log_weather_output``
does below).

Run alongside the agent service:

    python weather_sink.py
"""

import asyncio

from calfkit.client import Client
from calfkit.models import ConsumerContext
from calfkit.nodes import consumer
from calfkit.worker import Worker


@consumer(subscribe_topics="weather_agent.output")
async def log_weather_output(ctx: ConsumerContext) -> None:
    if ctx.output is None:
        # Intermediate hop (e.g. agent about to call a tool). Observe and move on.
        print(f"[{ctx.correlation_id[:8]}] intermediate hop from {ctx.emitter_node_id}")
        return
    print(f"[{ctx.correlation_id[:8]}] final from {ctx.emitter_node_id}: {ctx.output!r}")


async def main() -> None:
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[log_weather_output])
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
