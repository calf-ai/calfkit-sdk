from weather_tool import get_weather  # Import the tool definition (reusable)

from calfkit.nodes import StatelessAgent
from calfkit.providers import OpenAIResponsesModelClient

# Run it with: ck run agent_service:agent
agent = StatelessAgent(
    "weather_agent",
    system_prompt="You are a helpful assistant.",
    subscribe_topics="weather_agent.input",
    publish_topic="weather_agent.output",  # Stream outputs for consumer nodes to tap
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-nano"),
    tools=[get_weather],  # Register tool definitions with the agent
)
