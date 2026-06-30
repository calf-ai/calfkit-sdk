"""A launch-readiness review: consult several experts, then synthesize — no handoff.

A `release_manager` MESSAGES `engineering`, `security`, and `legal` for their status,
then synthesizes a single GO / NO-GO recommendation, keeping control throughout. The
fan-out-and-synthesize counterpart to the newsroom's consult-then-handoff.
"""

from tools import build_status, compliance_status, security_scan

from calfkit import Agent, Messaging, OpenAIResponsesModelClient

MODEL = "gpt-5.4-nano"


def _model() -> OpenAIResponsesModelClient:
    return OpenAIResponsesModelClient(model_name=MODEL)


release_manager = Agent(
    "release_manager",
    description="Release manager who runs launch-readiness reviews.",
    system_prompt=(
        "You are a release manager running a launch-readiness review. Use message_agent to ask each of "
        "engineering, security, and legal for their current status — one message to each. After you "
        "have all three replies, synthesize a single recommendation: start with GO or NO-GO, then one "
        "short line per area. Consult all three before you decide."
    ),
    model_client=_model(),
    peers=[Messaging("engineering", "security", "legal")],
)

engineering = Agent(
    "engineering",
    description="Engineering; reports build and test readiness.",
    system_prompt="You are the engineering team. Report build and test readiness concisely. Use your build_status tool.",
    model_client=_model(),
    tools=[build_status],
)

security = Agent(
    "security",
    description="Security; reports scan results and risk.",
    system_prompt="You are the security team. Report scan results and any risk concisely. Use your security_scan tool.",
    model_client=_model(),
    tools=[security_scan],
)

legal = Agent(
    "legal",
    description="Legal; reports compliance and launch clearance.",
    system_prompt="You are the legal team. Report compliance and launch clearance concisely. Use your compliance_status tool.",
    model_client=_model(),
    tools=[compliance_status],
)

NODES = [release_manager, engineering, security, legal, build_status, security_scan, compliance_status]
