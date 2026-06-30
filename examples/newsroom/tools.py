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
    return (
        "Verified: dock and station counts confirmed with the transit authority; "
        "'first month free' confirmed via the official press release. Caveat: the launch date is "
        "'late spring', not yet a firm calendar date."
    )
