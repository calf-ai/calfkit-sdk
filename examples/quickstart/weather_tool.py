from calfkit.nodes import agent_tool


# Define a tool — the @agent_tool decorator turns any function into a deployable tool node.
# Run it with: calfkit run weather_tool:get_weather
@agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location"""
    return f"It's sunny in {location}"
