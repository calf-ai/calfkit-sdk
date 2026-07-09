"""S0 tool-def + pinned-strings tests for the handoff tool transport (spec §2/§4/§9).

``handoff_tool_def`` renders the reserved ``handoff_to_agent`` external tool for a resolved
live directory — plain-string params (no enum), pinned preamble + directory description,
always renderable (empty directory → the "none reachable" sentinel body).

The byte-exact pin test is the single source of truth for every model-visible string
(spec §2 preamble, §4 stubs, §9 rejection reasons): a drive-by edit to any of them fails
here, deliberately.
"""

from __future__ import annotations

from calfkit.peers.directory import _NONE_REACHABLE, render_peer_directory
from calfkit.peers.handoff import (
    _HANDOFF_TOOL_PREAMBLE,
    _REJECT_FIELDS,
    _REJECT_MALFORMED,
    _REJECT_NONE_ONLINE,
    _REJECT_SELF,
    _REJECT_UNREACHABLE,
    _STUB_HANDOFF_NOT_EXECUTED,
    _STUB_TOOL_NOT_EXECUTED,
    _STUB_TRANSFERRED,
    HANDOFF_TOOL,
    handoff_tool_def,
)

_LIVE = [("billing", "handles refunds and invoices"), ("support", None)]


def test_tool_name_is_the_reserved_name():
    assert HANDOFF_TOOL == "handoff_to_agent"
    assert handoff_tool_def(_LIVE).name == HANDOFF_TOOL


def test_parameters_schema_is_plain_strings_no_enum():
    """Spec §2: plain-string ``name`` (an enum enforces nothing in-run and churns the
    schema per live-set) + required ``message``; advisory ``additionalProperties: False``
    (message_agent parity — calfkit-side validation stays lenient on extras)."""
    schema = handoff_tool_def(_LIVE).parameters_json_schema
    assert schema["type"] == "object"
    assert set(schema["required"]) == {"name", "message"}
    assert schema["properties"]["name"]["type"] == "string"
    assert schema["properties"]["message"]["type"] == "string"
    assert schema["additionalProperties"] is False
    assert "enum" not in str(schema)


def test_description_is_preamble_plus_rendered_directory():
    d = handoff_tool_def(_LIVE).description
    assert d == _HANDOFF_TOOL_PREAMBLE + render_peer_directory(_LIVE)
    assert "billing — handles refunds and invoices" in d
    assert "support" in d


def test_empty_directory_renders_the_none_reachable_sentinel():
    """Spec §2: the tool is ALWAYS injected; an empty live set renders the sentinel body
    (messaging parity) — the omit-plus-note behavior died with the ``Literal``."""
    d = handoff_tool_def([]).description
    assert d == _HANDOFF_TOOL_PREAMBLE + _NONE_REACHABLE


def test_distinct_live_sets_render_distinct_descriptions():
    assert handoff_tool_def(_LIVE).description != handoff_tool_def(_LIVE[:1]).description


def test_model_visible_strings_are_byte_exact():
    """The single-source-of-truth pin (spec §2/§4/§9, all CONFIRMED 2026-07-09).
    The literals are deliberately duplicated here — that is the point of the pin."""
    assert _HANDOFF_TOOL_PREAMBLE == (
        "Hand the whole conversation to a better-suited agent — when the request belongs to "
        "another agent's domain and should be handled by them, not just consulted. You "
        "relinquish control and will NOT regain it: the chosen agent continues with full "
        "context of the conversation and answers the original caller in your place.\n\n"
        "Agents (name — description):\n"
    )
    # §4 transcript stubs
    assert _STUB_TRANSFERRED == "Transferred to {name}."
    assert _STUB_TOOL_NOT_EXECUTED == "Tool not executed - the conversation was handed off to another agent."
    assert _STUB_HANDOFF_NOT_EXECUTED == "Handoff not executed - the conversation was already handed off."
    # §9 rejection reasons
    assert _REJECT_MALFORMED == "Malformed handoff_to_agent arguments: {etype}: {msg}"
    assert _REJECT_FIELDS == "handoff_to_agent requires non-empty string 'name' and 'message' arguments."
    assert _REJECT_SELF == "You cannot hand off to yourself ({name!r})."
    assert _REJECT_UNREACHABLE == (
        "Agent {name!r} is not currently reachable — it is offline or not in your handoff "
        "scope. Choose from the agents listed in the tool description."
    )
    assert _REJECT_NONE_ONLINE == "You cannot currently hand off this conversation or task to another agent, as no other agents are online."
