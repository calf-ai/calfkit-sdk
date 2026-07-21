"""A trip-planner agent that does its work in several hops.

The system prompt steers it to say what it's about to do, call the two tools,
then summarise — so a run produces a stream of intermediate steps (a preamble,
tool calls, tool results) before the final answer. ``service.py`` deploys this
agent together with its tools; ``stream.py`` calls it and prints the steps live.

Note there is no ``publish_topic`` here: step events flow to the *caller's* inbox
(the run's reply channel), not the agent's broadcast topic.
"""

from trip_tools import find_activities, get_weather

from calfkit.nodes import StatelessAgent
from calfkit.providers import OpenAIResponsesModelClient

# Run it with:  ck run planner_agent:agent
agent = StatelessAgent(
    "trip_planner",
    system_prompt=(
        "You are a trip planner. When asked about a destination, first check the weather, "
        "then find activities, then give a short plan that suits the weather. Before each "
        "tool call, briefly say what you are about to do."
    ),
    subscribe_topics="trip_planner.input",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-nano"),
    tools=[get_weather, find_activities],
)
