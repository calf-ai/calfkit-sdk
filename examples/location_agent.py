"""
Agent with parallel tool execution for location queries.

Demonstrates:
- Tool definition with @calf.tool decorator
- Parallel tool execution (both tools run simultaneously)
- Event-driven performance advantage over sequential execution

Run:
    uv run examples/location_agent.py
"""

import asyncio
from datetime import datetime

from calf import (
    Agent,
    Calf,
    MemoryStateStore,
    OpenAIClient,
    RunContext,
    tool,
)


# ============== Calf SETUP ==============

calf = Calf()
state_store = MemoryStateStore()
model_client = OpenAIClient()


# ============== TOOLS ==============


@calf.tool
async def get_weather(ctx: RunContext, location: str) -> str:
    """Get current weather for a location.

    Args:
        location: City name (e.g., "Tokyo", "New York", "London")
    """
    start = datetime.now()
    print(f"[{start.strftime('%H:%M:%S.%f')[:-3]}] get_weather({location}) started")

    await asyncio.sleep(1.0)  # Simulate API call

    # Mock weather data
    weather_data = {
        "tokyo": "Partly Cloudy, 18°C, 65% humidity",
        "new york": "Sunny, 22°C, 45% humidity",
        "london": "Rainy, 12°C, 80% humidity",
        "paris": "Clear, 20°C, 55% humidity",
        "sydney": "Warm, 25°C, 60% humidity",
    }
    weather = weather_data.get(location.lower(), "Clear, 20°C, 50% humidity")

    end = datetime.now()
    print(f"[{end.strftime('%H:%M:%S.%f')[:-3]}] get_weather({location}) completed")

    return f"Weather in {location}: {weather}"


@calf.tool
async def get_local_time(ctx: RunContext, location: str) -> str:
    """Get current local time for a location.

    Args:
        location: City name (e.g., "Tokyo", "New York", "London")
    """
    start = datetime.now()
    print(f"[{start.strftime('%H:%M:%S.%f')[:-3]}] get_local_time({location}) started")

    await asyncio.sleep(1.0)  # Simulate API call

    # Mock time data
    time_data = {
        "tokyo": "3:45 AM JST (UTC+9)",
        "new york": "2:45 PM EST (UTC-5)",
        "london": "7:45 PM GMT (UTC+0)",
        "paris": "8:45 PM CET (UTC+1)",
        "sydney": "6:45 AM AEDT (UTC+11)",
    }
    local_time = time_data.get(location.lower(), "12:00 PM UTC")

    end = datetime.now()
    print(f"[{end.strftime('%H:%M:%S.%f')[:-3]}] get_local_time({location}) completed")

    return f"Local time in {location}: {local_time}"


agent = Agent(
    name="location",
    model="gpt-4o-mini",
    tools=[get_weather, get_local_time],
    system_prompt=(
        "You are a helpful travel assistant. "
        "When asked about a location, always use both tools to provide "
        "comprehensive weather and time information."
    ),
)

# Register agent with Calf runtime
calf.register(agent, state_store=state_store, model_client=model_client)


async def main() -> None:
    print("=" * 60)
    print("Location Agent - Parallel Tool Execution Demo")
    print("=" * 60)
    print("\nWatch the timestamps - both tools execute in parallel!")
    print("Each tool has a 1 second simulated delay.\n")

    queries = [
        "What's the weather and time in Tokyo?",
        "Tell me about the current conditions in London.",
    ]

    for query in queries:
        print("-" * 60)
        print(f"Query: {query}")
        print("-" * 60)

        start_time = datetime.now()
        result = await agent.run(query)
        elapsed = (datetime.now() - start_time).total_seconds()

        print(f"\nResponse: {result.output}")
        print(f"\nTotal time: {elapsed:.2f}s (parallel)")
        print("Sequential would take ~2.0s\n")

    print("=" * 60)
    print("Both tools started at the same time - that's parallel execution!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
