"""One-shot client that invokes the agent.

Sends a single user prompt and prints the final response. The agent
internally calls one or more MCP tools via the bridge worker before
producing its answer.
"""

import asyncio

from calfkit.client import Client


async def main() -> None:
    client = Client.connect("localhost:9092")

    result = await client.execute_node(
        "Use the echo tool to say 'hello from the MCP adaptor'",
        "scribe.input",
    )
    print(f"Assistant: {result.output}")


if __name__ == "__main__":
    asyncio.run(main())
