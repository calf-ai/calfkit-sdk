"""Deploy an agent that uses the fetch toolbox — by name, in a separate process.

This agent never imports the toolbox definition or its connection config; it
selects the toolbox by name with an identity-only ``Toolboxes`` selector. The
agent's worker discovers the toolbox's tools over the capability control plane
and re-resolves them every turn, so bring-up order does not matter.

Prerequisites: a running broker, and ``export OPENAI_API_KEY=sk-...``.

Run it with:  ck run research_agent:agent
"""

from calfkit.nodes import StatelessAgent, Toolbox, Toolboxes
from calfkit.providers import OpenAIResponsesModelClient

agent = StatelessAgent(
    "researcher",
    system_prompt=("You answer questions by fetching and reading web pages with your fetch tool. Always cite the URL you read."),
    subscribe_topics="researcher.input",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-nano"),
    # Select the toolbox by name; the entry's `include` pins the exact tool we expect.
    tools=[Toolboxes(Toolbox("fetcher", include=("fetch",)))],
)
