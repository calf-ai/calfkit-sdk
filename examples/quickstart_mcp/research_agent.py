"""Deploy an agent that uses the fetch toolbox — by name, in a separate process.

This agent never imports the toolbox definition or its connection config; it
references the toolbox with an identity-only ``MCPToolbox`` handle. The agent's
worker discovers the toolbox's tools over the capability control plane and
re-resolves them every turn, so bring-up order does not matter.

Prerequisites: a running broker, and ``export OPENAI_API_KEY=sk-...``.

Run it with:  calfkit run research_agent:agent
"""

from calfkit.mcp import MCPToolbox
from calfkit.nodes import Agent
from calfkit.providers import OpenAIResponsesModelClient

agent = Agent(
    "researcher",
    system_prompt=("You answer questions by fetching and reading web pages with your fetch tool. Always cite the URL you read."),
    subscribe_topics="researcher.input",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-nano"),
    # Reference the toolbox by name; `include` pins the exact tool we expect.
    tools=[MCPToolbox("fetcher", include=("fetch",))],
)
