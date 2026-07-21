"""A newsroom editorial desk: consult, then hand off.

An `editor` runs the assignment. It MESSAGES the `researcher` for background and the
`fact_checker` to verify (keeping control), then HANDS OFF to the `writer`, who drafts
the final piece for the reader. Both peer verbs — messaging and handoff — compose
into one run.
"""

from tools import check_claim, search_archive

from calfkit import Handoff, Messaging, OpenAIResponsesModelClient, StatelessAgent

MODEL = "gpt-5.4-nano"


def _model() -> OpenAIResponsesModelClient:
    return OpenAIResponsesModelClient(model_name=MODEL)


editor = StatelessAgent(
    "editor",
    description="Assignment editor who runs a story from brief to draft.",
    system_prompt=(
        "You are an assignment editor. For each story brief, do these steps in order: (1) use "
        "message_agent to ask the researcher for background facts; (2) use message_agent to ask the "
        "fact_checker to verify the key claims; (3) hand off to the writer to produce the final piece, "
        "passing along the facts and caveats you gathered as context. Always consult both the researcher "
        "and the fact_checker before you hand off."
    ),
    model_client=_model(),
    peers=[Messaging("researcher", "fact_checker"), Handoff("writer")],
)

researcher = StatelessAgent(
    "researcher",
    description="News researcher who supplies background facts.",
    system_prompt=(
        "You are a news researcher. Call your search_archive tool once, then report 2-3 key facts from it "
        "in one or two sentences. Do not ask for clarification or add templates — treat the archive as the "
        "source of truth."
    ),
    model_client=_model(),
    tools=[search_archive],
)

fact_checker = StatelessAgent(
    "fact_checker",
    description="Fact-checker who verifies the story's claims.",
    system_prompt=(
        "You are a fact-checker. Given the story's claims, state what is verified and flag any caveats, concisely. Use your check_claim tool."
    ),
    model_client=_model(),
    tools=[check_claim],
)

writer = StatelessAgent(
    "writer",
    description="Staff writer who drafts the final article.",
    system_prompt=(
        "You are a staff writer. Using the facts and caveats you were handed, write a tight 3-4 sentence "
        "news brief for the reader. Output only the brief."
    ),
    model_client=_model(),
)

NODES = [editor, researcher, fact_checker, writer, search_archive, check_claim]
