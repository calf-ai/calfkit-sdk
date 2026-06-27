"""Ask the researcher agent to read and summarize a page.

Run it with:  python ask.py
"""

import asyncio

from calfkit.client import Client


async def main() -> None:
    async with Client.connect("localhost:9092") as client:
        result = await client.agent("researcher").execute("Summarize https://modelcontextprotocol.io/introduction in 3 bullet points.")
        print(f"Researcher: {result.output}")


if __name__ == "__main__":
    asyncio.run(main())
