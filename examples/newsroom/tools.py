"""Canned newsroom tools — deterministic and offline (the demo asks about bike-share)."""

from calfkit import agent_tool


@agent_tool
def search_archive(topic: str) -> str:
    """Search the newsroom archive for background facts on a topic."""
    return (
        "Archive on the bike-share program: city council approved it 7-2 on Tuesday; "
        "120 docks across 15 downtown stations; first month free for residents; "
        "operator is the same vendor as the neighboring county."
    )


@agent_tool
def check_claim(claim: str) -> str:
    """Verify a claim against the newsroom's confirmed record."""
    return f'"{claim}" — verified against the confirmed record.'
