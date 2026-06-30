"""An expense-approval chain: each approver clears what's within its limit, hands the rest up.

`team_lead` approves up to $1,000; `director` up to $10,000; `vp` approves anything.
A request above an approver's limit is HANDED OFF up the chain, and whoever clears it
answers the employee directly — a multi-hop handoff relay decided one tier at a time.
"""

from calfkit import Agent, Handoff, OpenAIResponsesModelClient

MODEL = "gpt-5.4-nano"


def _model() -> OpenAIResponsesModelClient:
    return OpenAIResponsesModelClient(model_name=MODEL)


team_lead = Agent(
    "team_lead",
    description="Team lead; approves expenses up to $1,000.",
    system_prompt=(
        "You are a team lead. You can approve expenses up to $1,000. If the amount is at or below "
        "$1,000, approve it and state the decision and amount. If it is above $1,000, hand off to the "
        "director, passing along the amount and purpose. Do not approve anything above your limit."
    ),
    model_client=_model(),
    peers=[Handoff("director")],
)

director = Agent(
    "director",
    description="Director; approves expenses up to $10,000.",
    system_prompt=(
        "You are a director. You can approve expenses up to $10,000. If the amount is at or below "
        "$10,000, approve it and state the decision and amount. If it is above $10,000, hand off to the "
        "vp, passing along the amount and purpose. Do not approve anything above your limit."
    ),
    model_client=_model(),
    peers=[Handoff("vp")],
)

vp = Agent(
    "vp",
    description="VP; final approver for any expense amount.",
    system_prompt=("You are a VP, the final approver. Approve or decline the expense with a brief reason, and state the amount."),
    model_client=_model(),
)

NODES = [team_lead, director, vp]
