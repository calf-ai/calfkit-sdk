"""An extra expert you can deploy AFTER the help desk is already running.

Start it in its own terminal — `ck run extra_expert:legal` — then ask the help
desk a legal question. The help desk discovers `legal` on the next turn and routes
to it, with no change to the help desk's code or restart. That is `discover=True`.
"""

from calfkit import Agent, OpenAIResponsesModelClient

legal = Agent(
    "legal",
    description="Legal: contracts, NDAs, and policy questions.",
    system_prompt=(
        "You are the legal team. Answer contract, NDA, and policy questions concisely, and flag when something needs formal review by counsel."
    ),
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-nano"),
)
