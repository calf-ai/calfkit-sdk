"""A panel of three persona agents that discuss a topic on a shared transcript.

Each agent's response is stamped with its identity and, at invocation time, the
shared transcript is projected to that agent's own point of view automatically: its
own turns stay assistant messages, the *other* panelists appear as attributed
``<name>`` participants, and a human prompt appears as ``<user>``. This needs no
configuration — it is on by default the moment a transcript holds more than one
distinct agent. A single agent talking to a user is unaffected (transparent).

This module just defines the agents; ``service.py`` deploys them and ``run.py``
drives the discussion.
"""

from calfkit.nodes import Agent
from calfkit.providers import OpenAIResponsesModelClient

MODEL = "gpt-5.4-nano"


def _panelist(name: str, persona: str) -> Agent:
    return Agent(
        name,
        system_prompt=(
            f"You are {name}, one panelist in a group discussion. {persona} "
            "Keep each contribution to 2-3 sentences, and address other panelists by "
            "name when you build on or push back against their points."
        ),
        model_client=OpenAIResponsesModelClient(model_name=MODEL),
    )


# Three distinct identities so the shared transcript carries multiple agent names.
PANEL = [
    _panelist("optimist", "You argue for the upside and what could go right."),
    _panelist("skeptic", "You probe the risks, costs, and what could go wrong."),
    _panelist("pragmatist", "You focus on what is actually feasible to do next."),
]
