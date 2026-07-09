"""S0 arbitration-kernel truth table for the handoff tool transport (spec §3/§3.1/§9).

Pure unit tests over synthetic ``ToolCallPart``s — no agent node, no broker, no view
(the ``directory.py`` test economics). ``arbitrate_handoff`` is the single turn-disposition
decision: first VALID handoff wins; siblings are stubbed iff a winner exists; invalid
handoffs are rejected with the spec §9 model-visible reasons.
"""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.peers.handoff import (
    _REJECT_FIELDS,
    _REJECT_NONE_ONLINE,
    _REJECT_SELF,
    _REJECT_UNREACHABLE,
    HANDOFF_TOOL,
    HandoffDisposition,
    arbitrate_handoff,
)

_LIVE = frozenset({"billing", "support"})
_SELF = "triage"


def _handoff(call_id: str, args: dict[str, Any] | str | None) -> ToolCallPart:
    return ToolCallPart(tool_name=HANDOFF_TOOL, args=args, tool_call_id=call_id)


def _tool(call_id: str, name: str = "search_flights") -> ToolCallPart:
    return ToolCallPart(tool_name=name, args={"q": "x"}, tool_call_id=call_id)


# --------------------------------------------------------------------------- #
# Disposition shape                                                            #
# --------------------------------------------------------------------------- #


def test_no_handoff_calls_is_a_no_op_disposition():
    """A response with only ordinary tool calls: no winner, nothing rejected, nothing
    stubbed — the caller proceeds through the normal dispatch loop untouched."""
    calls = [_tool("t1"), _tool("t2", name="book_hotel")]
    d = arbitrate_handoff(calls, _LIVE, _SELF)
    assert d.winner is None
    assert d.rejected == []
    assert d.stubbed == []


def test_disposition_is_frozen():
    d = arbitrate_handoff([], _LIVE, _SELF)
    assert isinstance(d, HandoffDisposition)
    with pytest.raises(dataclasses.FrozenInstanceError):
        d.winner = None  # type: ignore[misc]


# --------------------------------------------------------------------------- #
# Winner selection (spec §3, §3.1)                                             #
# --------------------------------------------------------------------------- #


def test_single_valid_handoff_wins():
    h = _handoff("h1", {"name": "billing", "message": "customer needs a refund"})
    d = arbitrate_handoff([h], _LIVE, _SELF)
    assert d.winner is h
    assert d.rejected == []
    assert d.stubbed == []


def test_winner_stubs_every_sibling_including_message_agent():
    """Early semantics (spec §3): iff a winner exists, every non-handoff call in the
    response — ordinary tools AND a message_agent consult — is stubbed, in emission
    order. Nothing is dispatched."""
    t1 = _tool("t1")
    ma = ToolCallPart(tool_name="message_agent", args={"name": "support", "message": "hi"}, tool_call_id="m1")
    h = _handoff("h1", {"name": "billing", "message": "take over"})
    t2 = _tool("t2", name="book_hotel")
    d = arbitrate_handoff([t1, ma, h, t2], _LIVE, _SELF)
    assert d.winner is h
    assert d.stubbed == [t1, ma, t2]
    assert d.rejected == []


def test_first_valid_handoff_wins_in_emission_order():
    """[invalid, valid, valid] → the first is rejected with its precise reason, the
    second wins, the third is stubbed (spec §3.1)."""
    bad = _handoff("h1", {"name": "billing", "message": "   "})
    good = _handoff("h2", {"name": "billing", "message": "take over"})
    later = _handoff("h3", {"name": "support", "message": "or you"})
    d = arbitrate_handoff([bad, good, later], _LIVE, _SELF)
    assert d.winner is good
    assert d.rejected == [(bad, _REJECT_FIELDS)]
    assert d.stubbed == [later]


def test_two_valid_handoffs_first_wins_second_stubbed():
    first = _handoff("h1", {"name": "billing", "message": "go"})
    second = _handoff("h2", {"name": "support", "message": "or go here"})
    d = arbitrate_handoff([first, second], _LIVE, _SELF)
    assert d.winner is first
    assert d.stubbed == [second]
    assert d.rejected == []


def test_no_valid_handoff_rejects_all_and_stubs_nothing():
    """No winner → every handoff call is rejected (mixed disposition: the sibling tool
    is NOT in `stubbed`; it proceeds through the normal loop)."""
    bad1 = _handoff("h1", {"name": "ghost", "message": "hi"})
    bad2 = _handoff("h2", {"message": "no name"})
    sibling = _tool("t1")
    d = arbitrate_handoff([bad1, sibling, bad2], _LIVE, _SELF)
    assert d.winner is None
    assert [call for call, _ in d.rejected] == [bad1, bad2]
    assert d.stubbed == []


# --------------------------------------------------------------------------- #
# Validity matrix (spec §9)                                                    #
# --------------------------------------------------------------------------- #


def test_json_string_args_are_valid():
    """Providers deliver args as JSON strings — a parseable string dict is valid."""
    h = _handoff("h1", '{"name": "billing", "message": "customer needs a refund"}')
    d = arbitrate_handoff([h], _LIVE, _SELF)
    assert d.winner is h


def test_extra_keys_are_ignored():
    """Spec §13.3: extras alongside valid name/message do not invalidate the call."""
    h = _handoff("h1", {"name": "billing", "message": "go", "urgency": "high"})
    d = arbitrate_handoff([h], _LIVE, _SELF)
    assert d.winner is h


def test_unparseable_args_are_malformed():
    h = _handoff("h1", "not json {{")
    d = arbitrate_handoff([h], _LIVE, _SELF)
    assert d.winner is None
    ((call, reason),) = d.rejected
    assert call is h
    assert reason.startswith("Malformed handoff_to_agent arguments: ")


def test_json_list_args_are_malformed():
    h = _handoff("h1", '["billing", "go"]')
    d = arbitrate_handoff([h], _LIVE, _SELF)
    ((call, reason),) = d.rejected
    assert reason.startswith("Malformed handoff_to_agent arguments: ")


@pytest.mark.parametrize(
    "args",
    [
        None,
        {},
        {"message": "no name"},
        {"name": "billing"},
        {"name": 123, "message": "go"},
        {"name": "  ", "message": "go"},
        {"name": "billing", "message": 7},
        {"name": "billing", "message": ""},
        {"name": "billing", "message": "   "},
    ],
)
def test_missing_or_blank_fields_are_rejected(args):
    d = arbitrate_handoff([_handoff("h1", args)], _LIVE, _SELF)
    assert d.winner is None
    ((_, reason),) = d.rejected
    assert reason == _REJECT_FIELDS


def test_self_target_is_rejected_with_its_own_reason():
    d = arbitrate_handoff([_handoff("h1", {"name": _SELF, "message": "loop"})], _LIVE, _SELF)
    ((_, reason),) = d.rejected
    assert reason == _REJECT_SELF.format(name=_SELF)


def test_unreachable_target_is_rejected():
    d = arbitrate_handoff([_handoff("h1", {"name": "ghost", "message": "hi"})], _LIVE, _SELF)
    ((_, reason),) = d.rejected
    assert reason == _REJECT_UNREACHABLE.format(name="ghost")


def test_empty_live_set_rejects_with_none_online_reason():
    d = arbitrate_handoff([_handoff("h1", {"name": "billing", "message": "hi"})], frozenset(), _SELF)
    ((_, reason),) = d.rejected
    assert reason == _REJECT_NONE_ONLINE
