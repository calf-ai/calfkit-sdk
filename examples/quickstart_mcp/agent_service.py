"""Agent worker — uses the MCP server's tools via Kafka.

Does NOT spawn the MCP subprocess. The bridge worker (tools_service.py)
owns the subprocess; this process only knows about the tool schemas
(imported from the codegen-generated everything_schemas.py via shared.py).
"""

import asyncio

from shared import everything

from calfkit.client import Client
from calfkit.nodes import Agent
from calfkit.providers import OpenAIResponsesModelClient
from calfkit.worker import Worker

agent = Agent(
    "scribe",
    system_prompt="You are a helpful assistant. Use the available tools when appropriate.",
    subscribe_topics="scribe.input",
    publish_topic="scribe.output",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-nano"),
    tools=[everything],
)


async def main() -> None:
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[agent])
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
