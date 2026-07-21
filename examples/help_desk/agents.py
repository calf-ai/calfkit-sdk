"""An internal help desk that discovers expert teams at runtime and routes to them.

`help_desk` reaches every live expert via `discover=True` — it is never wired to a
fixed list. For a quick question it MESSAGES the right expert and relays the answer
(keeping control); for a task an expert should own, it HANDS OFF. Deploy a new
expert (see `extra_expert.py`) and `help_desk` can reach it on the next turn, with
no change here.
"""

from tools import account_status, file_reimbursement, pto_balance

from calfkit import Handoff, Messaging, OpenAIResponsesModelClient, StatelessAgent

MODEL = "gpt-5.4-nano"


def _model() -> OpenAIResponsesModelClient:
    return OpenAIResponsesModelClient(model_name=MODEL)


help_desk = StatelessAgent(
    "help_desk",
    description="Front desk that routes employee questions to the right expert team.",
    system_prompt=(
        "You are the front desk for a company's internal help. Expert teams come online and go offline; "
        "your message_agent tool lists the ones available right now. For a quick informational question, "
        "use message_agent to ask the single most relevant expert, then relay their answer to the "
        "employee. For a request an expert should take over and complete end to end (for example, "
        "actually filing a reimbursement), hand off to that expert instead of messaging. Route time-off, "
        "benefits, and people questions to hr; login, access, and device questions to it_support; and "
        "expense or reimbursement matters to finance."
    ),
    model_client=_model(),
    peers=[Messaging(discover=True), Handoff(discover=True)],
)

hr = StatelessAgent(
    "hr",
    description="HR: paid time off, benefits, and people policies.",
    system_prompt=(
        "You are the HR team. Answer time-off, benefits, and policy questions concisely. "
        "Use your pto_balance tool to look up a balance before answering."
    ),
    model_client=_model(),
    tools=[pto_balance],
)

it_support = StatelessAgent(
    "it_support",
    description="IT support: logins, access, and devices.",
    system_prompt=(
        "You are the IT support team. Answer access, login, and device questions concisely. "
        "Use your account_status tool to check an account before answering."
    ),
    model_client=_model(),
    tools=[account_status],
)

finance = StatelessAgent(
    "finance",
    description="Finance: reimbursements, expenses, and budgets.",
    system_prompt=(
        "You are the finance team. Handle reimbursement and expense requests. When asked to file a "
        "reimbursement, call your file_reimbursement tool and confirm the result to the employee."
    ),
    model_client=_model(),
    tools=[file_reimbursement],
)

# Everything the worker hosts: the front desk, the three experts, and their tools.
NODES = [help_desk, hr, it_support, finance, pto_balance, account_status, file_reimbursement]
