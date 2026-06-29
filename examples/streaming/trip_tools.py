"""Two tools the trip planner calls — each is its own deployable tool node.

The agent reaches them by name over the broker (it publishes a tool Call to each
tool's inbox); ``service.py`` co-hosts them with the agent on one worker. The
canned returns keep the demo deterministic and offline (no external API).
"""

from calfkit.nodes import agent_tool


# Run it with:  ck run trip_tools:get_weather
@agent_tool
def get_weather(city: str) -> str:
    """Get tomorrow's weather forecast for a city."""
    return f"Tomorrow in {city}: cloudy with light rain, 14°C."


# Run it with:  ck run trip_tools:find_activities
@agent_tool
def find_activities(city: str) -> str:
    """Find notable things to do in a city."""
    return f"In {city}: temple tours, a covered food market, and a tea ceremony."
