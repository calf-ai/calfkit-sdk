"""Tests for the agent-POV message-history projection (docs/designs/agent-pov-projection.md §5, §14).

The `project(history, viewer)` function is pure: it returns a new list of
`ModelMessage`, never mutates its input, and strips `name` from every emitted
message. These tests use the REAL vendored pydantic-ai types.
"""

from __future__ import annotations

import copy

from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    RetryPromptPart,
    TextPart,
    ThinkingPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from calfkit.nodes._projection import project, structured_output_preamble

# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def _user(text: str, *, name: str | None = None) -> ModelRequest:
    return ModelRequest(parts=[UserPromptPart(content=text, name=name)])


def _response(*parts, name: str | None = None) -> ModelResponse:
    return ModelResponse(parts=list(parts), name=name)


def _tool_return(tool_name: str, content: str, *, tool_call_id: str) -> ModelRequest:
    return ModelRequest(parts=[ToolReturnPart(tool_name=tool_name, content=content, tool_call_id=tool_call_id)])


def _user_prompt_texts(messages: list[ModelMessage]) -> list[str]:
    """All UserPromptPart string contents across the emitted ModelRequests."""
    out: list[str] = []
    for m in messages:
        if isinstance(m, ModelRequest):
            for p in m.parts:
                if isinstance(p, UserPromptPart) and isinstance(p.content, str):
                    out.append(p.content)
    return out


# --------------------------------------------------------------------------- #
# §14.1 Golden cases                                                           #
# --------------------------------------------------------------------------- #


def test_single_agent_transparent_passthrough_and_name_stripped():
    """One agent + unnamed human → transparent: roles kept, no prefixes, name stripped."""
    history: list[ModelMessage] = [
        _user("hello there"),
        _response(TextPart(content="hi, how can I help?"), name="scheduler"),
    ]

    out = project(history, viewer="scheduler")

    # roles preserved
    assert isinstance(out[0], ModelRequest)
    assert isinstance(out[1], ModelResponse)
    # no prefix added to the human turn (transparent)
    assert out[0].parts[0].content == "hello there"
    # self response preserved with its TextPart, but name stripped
    assert out[1].name is None
    assert isinstance(out[1].parts[0], TextPart)
    assert out[1].parts[0].content == "hi, how can I help?"


def test_two_agents_one_unnamed_human_is_projected():
    """Two agents + an unnamed human → projected (the primary channel case)."""
    history: list[ModelMessage] = [
        _user("what's friday?"),
        _response(TextPart(content="You have a 2pm sync."), name="scheduler"),
        _response(TextPart(content="On it."), name="researcher"),
    ]

    out = project(history, viewer="scheduler")

    texts = _user_prompt_texts(out)
    # human turn gets <user> prefix
    assert "<user> what's friday?" in texts
    # the other agent (researcher) is re-roled to an attributed user turn
    assert "<researcher>\nOn it." in texts
    # scheduler's own turn stays a ModelResponse with name stripped
    self_responses = [m for m in out if isinstance(m, ModelResponse)]
    assert len(self_responses) == 1
    assert self_responses[0].name is None
    assert self_responses[0].parts[0].content == "You have a 2pm sync."


def test_two_named_humans_one_agent_is_projected():
    """One agent + two named humans → projected; named humans get <user:name>."""
    history: list[ModelMessage] = [
        _user("hey", name="Alice"),
        _user("hi", name="Bob"),
        _response(TextPart(content="hello both"), name="scheduler"),
    ]

    out = project(history, viewer="scheduler")

    texts = _user_prompt_texts(out)
    assert "<user:Alice> hey" in texts
    assert "<user:Bob> hi" in texts
    # the agent's own turn stays a response, name stripped
    self_responses = [m for m in out if isinstance(m, ModelResponse)]
    assert len(self_responses) == 1
    assert self_responses[0].name is None


def test_agent_to_agent_other_surface_only():
    """Agent-to-agent: viewer sees the other purely as an attributed surface ModelRequest."""
    history: list[ModelMessage] = [
        _response(TextPart(content="ping"), name="alpha"),
        _response(TextPart(content="pong"), name="beta"),
    ]

    out = project(history, viewer="beta")

    # alpha's turn is re-roled to a user turn (agent surface uses a "\n" separator, §5.7)
    texts = _user_prompt_texts(out)
    assert "<alpha>\nping" in texts
    # beta's own turn is the only ModelResponse, name stripped
    responses = [m for m in out if isinstance(m, ModelResponse)]
    assert len(responses) == 1
    assert responses[0].name is None
    assert responses[0].parts[0].content == "pong"


def test_self_then_other():
    """Viewer's own turn stays full-fidelity; a subsequent other becomes a surface request."""
    history: list[ModelMessage] = [
        _user("kick off"),
        _response(TextPart(content="self speaking"), name="alpha"),
        _response(TextPart(content="other speaking"), name="beta"),
    ]

    out = project(history, viewer="alpha")

    # alpha self response is preserved as a response (name stripped)
    responses = [m for m in out if isinstance(m, ModelResponse)]
    assert len(responses) == 1
    assert responses[0].name is None
    assert responses[0].parts[0].content == "self speaking"
    # beta is surfaced as a user turn (agent surface uses a "\n" separator)
    assert "<beta>\nother speaking" in _user_prompt_texts(out)


def test_tool_mode_structured_surface_with_preamble():
    """tool-mode final response: TextPart preamble + final_result args both surfaced, "\\n"-joined."""
    history: list[ModelMessage] = [
        _response(TextPart(content="scheduler text"), name="scheduler"),
        _response(
            TextPart(content="On it."),
            ToolCallPart(tool_name="final_result", args={"flights": 3}, tool_call_id="c1"),
            name="researcher",
        ),
        _tool_return("final_result", "Final result processed.", tool_call_id="c1"),
    ]

    out = project(history, viewer="scheduler")

    texts = _user_prompt_texts(out)
    # preamble survives AND the structured args surface, joined by "\n", compact-sorted JSON
    assert '<researcher>\nOn it.\n{"flights":3}' in texts
    # the final_result tool-return ModelRequest (owned by researcher) is dropped
    assert not any(isinstance(m, ModelRequest) and any(isinstance(p, ToolReturnPart) for p in m.parts) for m in out)


def test_tool_mode_structured_surface_sorts_keys_and_compacts():
    """Structured args rendered as canonical compact JSON with sort_keys (provider-independent)."""
    history: list[ModelMessage] = [
        _response(TextPart(content="scheduler text"), name="scheduler"),
        _response(
            ToolCallPart(
                tool_name="final_result",
                args={"b": 2, "a": 1},
                tool_call_id="c1",
            ),
            name="researcher",
        ),
    ]

    out = project(history, viewer="scheduler")

    texts = _user_prompt_texts(out)
    # keys sorted, compact separators, no preamble → just prefix + JSON
    assert '<researcher>\n{"a":1,"b":2}' in texts


def test_tool_mode_structured_surface_args_as_json_string():
    """Provider that stores args as a JSON *string* still renders canonically."""
    history: list[ModelMessage] = [
        _response(TextPart(content="scheduler text"), name="scheduler"),
        _response(
            ToolCallPart(
                tool_name="final_result",
                args='{"flights": 3}',  # OpenAI-style: args stored as a JSON string with whitespace
                tool_call_id="c1",
            ),
            name="researcher",
        ),
    ]

    out = project(history, viewer="scheduler")

    # canonical compact rendering removes the provider whitespace
    assert '<researcher>\n{"flights":3}' in _user_prompt_texts(out)


def test_native_prompted_surface_is_text():
    """native/prompted structured output is a TextPart and is surfaced by the text rule."""
    history: list[ModelMessage] = [
        _response(TextPart(content="scheduler text"), name="scheduler"),
        _response(TextPart(content='{"flights": 3}'), name="researcher"),
    ]

    out = project(history, viewer="scheduler")

    assert '<researcher>\n{"flights": 3}' in _user_prompt_texts(out)


def test_multi_output_union_final_result_is_surfaced():
    """A multi-output union renames the tool final_result_<Type>; it must still surface cross-agent.

    pydantic-ai sets ``multiple=True`` for a 2+-output union and renames each output tool
    ``final_result_<TypeName>`` (``_output.py`` ``OutputToolset.build``). The exact-name match
    used to drop the renamed tool, omitting the whole structured turn.
    """
    history: list[ModelMessage] = [
        _response(TextPart(content="scheduler text"), name="scheduler"),
        _response(
            ToolCallPart(tool_name="final_result_SomeModel", args={"b": 2, "a": 1}, tool_call_id="c1"),
            name="researcher",
        ),
    ]

    out = project(history, viewer="scheduler")

    assert '<researcher>\n{"a":1,"b":2}' in _user_prompt_texts(out)


def test_multi_output_union_preamble_is_detected():
    """structured_output_preamble must detect a renamed final_result_<Type> tool call (tool mode).

    A union output is still tool mode, so the response's TextPart is a genuine preamble distinct
    from the structured answer. The exact-name match used to miss the renamed tool and wrongly
    return "" (treating it as native/prompted, dropping the preamble on the client path).
    """
    new_messages: list[ModelMessage] = [
        _response(
            TextPart(content="On it."),
            ToolCallPart(tool_name="final_result_SomeModel", args={"flights": 3}, tool_call_id="c1"),
            name="researcher",
        ),
    ]

    assert structured_output_preamble(new_messages) == "On it."


def test_structured_output_preamble_no_response_returns_empty():
    """No ModelResponse in the run's new messages → no preamble (defensive guard)."""
    assert structured_output_preamble([_user("hello")]) == ""


def test_legacy_structured_output_handoff_history_still_surfaces():
    """Spec §11 regression pin: OLD persisted histories from the retired structured-output
    handoff transport (a ``final_result_HandoffRequest`` output-tool call) must still
    deserialize and surface cross-agent via the ``_is_output_tool`` prefix rule — free
    behavior, not a compat feature; a future ``_is_output_tool`` refactor must not silently
    break legacy-history briefings.
    """
    history: list[ModelMessage] = [
        _response(
            ToolCallPart(
                tool_name="final_result_HandoffRequest",
                args={"name": "refunds", "message": "escalating"},
                tool_call_id="h1",
            ),
            name="triage",
        ),
    ]

    out = project(history, viewer="refunds")

    assert '<triage>\n{"message":"escalating","name":"refunds"}' in _user_prompt_texts(out)


def test_ordinary_function_tool_call_is_not_surfaced():
    """An ordinary (non-output) function tool call is internal → never surfaced cross-agent.

    Guards the ``final_result*`` prefix heuristic: only the output-tool namespace surfaces. A
    function tool (here ``search``) has its own name and stays dropped.
    """
    history: list[ModelMessage] = [
        _response(TextPart(content="scheduler text"), name="scheduler"),
        _response(
            ToolCallPart(tool_name="search", args={"q": "flights"}, tool_call_id="c1"),
            name="researcher",
        ),
    ]

    out = project(history, viewer="scheduler")

    # the search tool call produces no surface → no <researcher> request emitted
    assert not any("<researcher>" in t for t in _user_prompt_texts(out))


def test_prefix_lookalike_function_tool_is_not_surfaced():
    """A function tool sharing the 'final_result' prefix WITHOUT the '_' separator
    (e.g. 'final_results') must NOT be mistaken for an output tool.

    Guards the ``startswith(_FINAL_RESULT_TOOL_NAME + "_")`` separator: dropping the
    ``+ "_"`` would silently start surfacing such a function tool cross-agent (a leak).
    """
    history: list[ModelMessage] = [
        _response(TextPart(content="scheduler text"), name="scheduler"),
        _response(
            ToolCallPart(tool_name="final_results", args={"q": "x"}, tool_call_id="c1"),
            name="researcher",
        ),
    ]

    out = project(history, viewer="scheduler")

    assert not any("<researcher>" in t for t in _user_prompt_texts(out))


def test_renamed_union_tool_empty_args_is_omitted():
    """A renamed (union) output tool with falsy args produces no surface → turn omitted (§5.5).

    Locks the ``if p.args:`` truthiness branch for the renamed namespace (an empty renamed
    output tool must still be omitted, not surfaced as ``<author>\\n{}``).
    """
    for empty in (None, {}):
        history: list[ModelMessage] = [
            _response(TextPart(content="scheduler text"), name="scheduler"),
            _response(
                ToolCallPart(tool_name="final_result_SomeModel", args=empty, tool_call_id="c1"),
                name="researcher",
            ),
        ]

        out = project(history, viewer="scheduler")

        assert not any("<researcher>" in t for t in _user_prompt_texts(out))


def test_user_prefix_no_name():
    """Unnamed human gets <user> when projection is engaged."""
    history: list[ModelMessage] = [
        _user("plain human"),
        _response(TextPart(content="a"), name="alpha"),
        _response(TextPart(content="b"), name="beta"),
    ]

    out = project(history, viewer="alpha")

    assert "<user> plain human" in _user_prompt_texts(out)


def test_user_prefix_with_name():
    """Named human gets <user:name> when projection is engaged."""
    history: list[ModelMessage] = [
        _user("named human", name="Alice"),
        _response(TextPart(content="a"), name="alpha"),
        _response(TextPart(content="b"), name="beta"),
    ]

    out = project(history, viewer="alpha")

    assert "<user:Alice> named human" in _user_prompt_texts(out)


def test_tool_return_ownership_across_interleaved_boundary():
    """Tool returns are attributed to owners by tool_call_id, interleave-safe.

    Two agents each make a tool call; the returns arrive interleaved. The viewer
    keeps its own tool return and drops the other's.
    """
    history: list[ModelMessage] = [
        _response(
            ToolCallPart(tool_name="get_calendar", args={}, tool_call_id="own"),
            name="scheduler",
        ),
        _response(
            ToolCallPart(tool_name="search", args={}, tool_call_id="other"),
            name="researcher",
        ),
        # returns arrive interleaved (other first, then own)
        _tool_return("search", "other-result", tool_call_id="other"),
        _tool_return("get_calendar", "own-result", tool_call_id="own"),
    ]

    out = project(history, viewer="scheduler")

    # scheduler keeps its own tool-call response (full fidelity) and its own return
    own_returns = [p for m in out if isinstance(m, ModelRequest) for p in m.parts if isinstance(p, ToolReturnPart)]
    assert len(own_returns) == 1
    assert own_returns[0].tool_call_id == "own"
    assert own_returns[0].content == "own-result"
    # the other agent's tool-call response and its return must NOT appear
    assert not any(isinstance(m, ModelResponse) and any(isinstance(p, ToolCallPart) and p.tool_call_id == "other" for p in m.parts) for m in out)


def test_empty_surface_handoff_is_omitted():
    """A pure hand-off (no preamble, no structured args) is omitted, not an attribution-only prefix."""
    history: list[ModelMessage] = [
        _response(TextPart(content="scheduler text"), name="scheduler"),
        _response(
            ToolCallPart(tool_name="final_result", args=None, tool_call_id="c1"),
            name="researcher",
        ),
    ]

    out = project(history, viewer="scheduler")

    # the researcher turn produced no surface → no <researcher> request emitted
    assert not any("<researcher>" in t for t in _user_prompt_texts(out))


def test_empty_dict_args_handoff_is_omitted():
    """final_result(args={}) is empty → omitted (branch on `if tc.args:` truthiness)."""
    history: list[ModelMessage] = [
        _response(TextPart(content="scheduler text"), name="scheduler"),
        _response(
            ToolCallPart(tool_name="final_result", args={}, tool_call_id="c1"),
            name="researcher",
        ),
    ]

    out = project(history, viewer="scheduler")

    assert not any("<researcher>" in t for t in _user_prompt_texts(out))


def test_zero_valued_structured_args_are_surfaced():
    """final_result(args={"x": 0}) must surface (has_content would wrongly drop it)."""
    history: list[ModelMessage] = [
        _response(TextPart(content="scheduler text"), name="scheduler"),
        _response(
            ToolCallPart(tool_name="final_result", args={"x": 0}, tool_call_id="c1"),
            name="researcher",
        ),
    ]

    out = project(history, viewer="scheduler")

    assert '<researcher>\n{"x":0}' in _user_prompt_texts(out)


def test_unknown_author_unstamped_response():
    """A legacy un-named ModelResponse in a multi-agent history → <unknown> prefix."""
    history: list[ModelMessage] = [
        _response(TextPart(content="legacy turn"), name=None),  # un-stamped
        _response(TextPart(content="alpha turn"), name="alpha"),
        _response(TextPart(content="beta turn"), name="beta"),
    ]

    out = project(history, viewer="alpha")

    # the un-stamped legacy response is treated as other → <unknown> (agent surface, "\n" sep)
    assert "<unknown>\nlegacy turn" in _user_prompt_texts(out)


# --------------------------------------------------------------------------- #
# §14.2 Detection — every row of the §5.1 table                               #
# --------------------------------------------------------------------------- #


def test_detection_single_agent_any_unnamed_humans_transparent():
    """1 agent + unnamed humans → transparent (no prefixes)."""
    history: list[ModelMessage] = [
        _user("h1"),
        _response(TextPart(content="a"), name="solo"),
        _user("h2"),
    ]

    out = project(history, viewer="solo")

    # transparent: human content unprefixed
    assert "h1" in _user_prompt_texts(out)
    assert "h2" in _user_prompt_texts(out)
    assert not any(t.startswith("<user>") for t in _user_prompt_texts(out))


def test_detection_two_agents_unnamed_human_projected():
    """2 agents + unnamed human → projected."""
    history: list[ModelMessage] = [
        _user("h"),
        _response(TextPart(content="a"), name="alpha"),
        _response(TextPart(content="b"), name="beta"),
    ]

    out = project(history, viewer="alpha")

    assert "<user> h" in _user_prompt_texts(out)


def test_detection_two_agents_no_human_projected():
    """2 agents + no human → projected."""
    history: list[ModelMessage] = [
        _response(TextPart(content="a"), name="alpha"),
        _response(TextPart(content="b"), name="beta"),
    ]

    out = project(history, viewer="alpha")

    # beta is surfaced as a user turn → projection engaged (agent surface, "\n" sep)
    assert "<beta>\nb" in _user_prompt_texts(out)


def test_detection_one_agent_two_named_humans_projected():
    """1 agent + >=2 named humans → projected."""
    history: list[ModelMessage] = [
        _user("h1", name="Alice"),
        _user("h2", name="Bob"),
        _response(TextPart(content="a"), name="solo"),
    ]

    out = project(history, viewer="solo")

    assert "<user:Alice> h1" in _user_prompt_texts(out)
    assert "<user:Bob> h2" in _user_prompt_texts(out)


def test_detection_one_agent_one_named_human_transparent():
    """1 agent + 1 named human → transparent (only 1 distinct human name)."""
    history: list[ModelMessage] = [
        _user("h1", name="Alice"),
        _response(TextPart(content="a"), name="solo"),
    ]

    out = project(history, viewer="solo")

    # transparent: no <user:Alice> prefix added
    assert "h1" in _user_prompt_texts(out)
    assert not any(t.startswith("<user") for t in _user_prompt_texts(out))


def test_detection_new_agent_not_yet_spoken_sees_other_projected():
    """A viewer that has not spoken yet still projects when >=1 other agent present."""
    history: list[ModelMessage] = [
        _user("h"),
        _response(TextPart(content="a"), name="alpha"),
        _response(TextPart(content="b"), name="beta"),
    ]

    # viewer "gamma" owns nothing but sees 2 agents → projected
    out = project(history, viewer="gamma")

    assert "<alpha>\na" in _user_prompt_texts(out)
    assert "<beta>\nb" in _user_prompt_texts(out)
    # gamma owns no ModelResponse
    assert not any(isinstance(m, ModelResponse) for m in out)


def test_detection_single_other_agent_is_projected():
    """A history authored by ONE agent, viewed by a DIFFERENT agent (the handoff first-hop),
    is projected: the other agent's turn is attributed, not shown as the viewer's own.

    Regression: the gate counted distinct authors (>=2) instead of viewer-awareness, so a
    single other-agent's history fell into transparent mode and B saw A's turns as its own.
    """
    history: list[ModelMessage] = [
        _user("h"),
        _response(TextPart(content="a"), name="alpha"),
    ]

    # viewer "beta" has not spoken; the sole author is alpha (!= beta) → attributed
    out = project(history, viewer="beta")

    assert "<alpha>\na" in _user_prompt_texts(out)
    # beta owns no ModelResponse → alpha's turn is re-roled, never kept as beta's own assistant turn
    assert not any(isinstance(m, ModelResponse) for m in out)


def test_detection_empty_history_transparent():
    """Empty history → transparent (no participants), returns an empty list."""
    out = project([], viewer="solo")
    assert out == []


# --------------------------------------------------------------------------- #
# §14.3 Security property                                                      #
# --------------------------------------------------------------------------- #


def test_security_no_other_owned_internal_parts_appear():
    """No non-viewer-owned ThinkingPart/ToolCallPart/ToolReturnPart appears in any projection."""
    history: list[ModelMessage] = [
        _user("kick off"),
        # viewer (scheduler) internals — allowed to appear
        _response(
            ThinkingPart(content="my secret reasoning"),
            ToolCallPart(tool_name="get_calendar", args={}, tool_call_id="own"),
            name="scheduler",
        ),
        _tool_return("get_calendar", "2pm sync", tool_call_id="own"),
        _response(TextPart(content="You have a 2pm sync."), name="scheduler"),
        # other agent internals — must NOT appear
        _response(
            ThinkingPart(content="researcher secret reasoning"),
            ToolCallPart(tool_name="search", args={"q": "flights"}, tool_call_id="other"),
            name="researcher",
        ),
        _tool_return("search", "found 3 flights", tool_call_id="other"),
        _response(
            TextPart(content="Found flights."),
            ToolCallPart(tool_name="final_result", args={"flights": 3}, tool_call_id="fr"),
            name="researcher",
        ),
        _tool_return("final_result", "Final result processed.", tool_call_id="fr"),
    ]

    out = project(history, viewer="scheduler")

    def _owner_parts(messages):
        parts = []
        for m in messages:
            parts.extend(m.parts)
        return parts

    all_parts = _owner_parts(out)

    # No researcher reasoning leaks
    thinking = [p for p in all_parts if isinstance(p, ThinkingPart)]
    for tp in thinking:
        assert "researcher" not in tp.content

    # No researcher tool call leaks (only the viewer's own 'get_calendar' may appear)
    tool_calls = [p for p in all_parts if isinstance(p, ToolCallPart)]
    for tc in tool_calls:
        assert tc.tool_call_id == "own"
        assert tc.tool_name == "get_calendar"

    # No researcher tool return leaks (only the viewer's own may appear)
    tool_returns = [p for p in all_parts if isinstance(p, ToolReturnPart)]
    for tr in tool_returns:
        assert tr.tool_call_id == "own"

    # but the researcher's STRUCTURED ANSWER surface is allowed (it's the public surface)
    assert any("<researcher>" in t and "flights" in t for t in _user_prompt_texts(out))


def test_security_other_retry_prompt_not_leaked():
    """Another agent's RetryPromptPart (an internal tool-error round-trip) is dropped."""
    history: list[ModelMessage] = [
        _response(
            ToolCallPart(tool_name="search", args={}, tool_call_id="other"),
            name="researcher",
        ),
        ModelRequest(parts=[RetryPromptPart(content="bad args", tool_name="search", tool_call_id="other")]),
        _response(TextPart(content="done"), name="researcher"),
        _response(TextPart(content="ok"), name="scheduler"),
    ]

    out = project(history, viewer="scheduler")

    all_parts = [p for m in out for p in m.parts]
    assert not any(isinstance(p, RetryPromptPart) for p in all_parts)


# --------------------------------------------------------------------------- #
# §14.4 Purity + name-strip                                                    #
# --------------------------------------------------------------------------- #


def test_project_does_not_mutate_input():
    """project() never mutates the canonical history (deep-equality before/after)."""
    history: list[ModelMessage] = [
        _user("h"),
        _response(TextPart(content="a"), name="alpha"),
        _response(TextPart(content="b"), name="beta"),
    ]
    snapshot = copy.deepcopy(history)

    project(history, viewer="alpha")

    assert history == snapshot
    # names still present on the canonical responses
    assert history[1].name == "alpha"
    assert history[2].name == "beta"


def test_no_emitted_message_carries_name_transparent():
    """In transparent mode, no emitted ModelResponse carries name, no UserPromptPart carries name."""
    history: list[ModelMessage] = [
        _user("h", name="Alice"),
        _response(TextPart(content="a"), name="solo"),
    ]

    out = project(history, viewer="solo")

    for m in out:
        if isinstance(m, ModelResponse):
            assert m.name is None
        if isinstance(m, ModelRequest):
            for p in m.parts:
                if isinstance(p, UserPromptPart):
                    assert p.name is None


def test_no_emitted_message_carries_name_projected():
    """In projected mode, no emitted ModelResponse carries name, no UserPromptPart carries name."""
    history: list[ModelMessage] = [
        _user("h", name="Alice"),
        _user("h2", name="Bob"),
        _response(TextPart(content="a"), name="solo"),
    ]

    out = project(history, viewer="solo")

    for m in out:
        if isinstance(m, ModelResponse):
            assert m.name is None
        if isinstance(m, ModelRequest):
            for p in m.parts:
                if isinstance(p, UserPromptPart):
                    assert p.name is None


def test_self_response_is_new_object_via_replace():
    """The self-view ModelResponse is a NEW object (dataclasses.replace), not the input one."""
    self_resp = _response(TextPart(content="self"), name="alpha")
    history: list[ModelMessage] = [
        self_resp,
        _response(TextPart(content="other"), name="beta"),
    ]

    out = project(history, viewer="alpha")

    emitted = [m for m in out if isinstance(m, ModelResponse)]
    assert len(emitted) == 1
    # new object, name stripped, but parts unchanged (shared part objects are fine)
    assert emitted[0] is not self_resp
    assert emitted[0].name is None
    # Shallow ``replace`` shares the part list verbatim (parts are never mutated in
    # place, and the ids must survive for the §6.2 deferred-results re-entry).
    assert emitted[0].parts is self_resp.parts


# --------------------------------------------------------------------------- #
# Handoff TOOL transport surfacing (handoff-tool-transport-spec §6, S1)        #
# --------------------------------------------------------------------------- #


def _handoff_call_turn(*, author: str, target: str, message: str, tool_call_id: str = "h1") -> list[ModelMessage]:
    """A's winning handoff turn under the tool transport: the ``handoff_to_agent`` call
    plus the calfkit-authored closing ``ModelRequest`` (spec §4)."""
    return [
        _response(
            ToolCallPart(tool_name="handoff_to_agent", args={"name": target, "message": message}, tool_call_id=tool_call_id),
            name=author,
        ),
        _tool_return("handoff_to_agent", f"Transferred to {target}.", tool_call_id=tool_call_id),
    ]


def test_handoff_tool_call_surfaces_to_peer():
    """spec §6: the reserved ``handoff_to_agent`` call's args surface cross-agent exactly
    like output-tool args — B's only briefing channel under the tool transport."""
    history = _handoff_call_turn(author="triage", target="refunds", message="escalating")

    out = project(history, viewer="refunds")

    assert '<triage>\n{"message":"escalating","name":"refunds"}' in _user_prompt_texts(out)


def test_handoff_closing_request_stubs_dropped_for_peer():
    """The closing request's ToolReturnParts are A-owned → dropped for viewer B (existing
    owner-map rule, pinned here for the handoff turn shape)."""
    history = _handoff_call_turn(author="triage", target="refunds", message="escalating")

    out = project(history, viewer="refunds")

    assert not any(isinstance(p, ToolReturnPart) for m in out if isinstance(m, ModelRequest) for p in m.parts)


def test_handoff_tool_call_self_view_verbatim():
    """Viewer A (self) keeps its own handoff turn verbatim — the real ToolCallPart and the
    A-owned closing request survive (multi-participant history: B answered after)."""
    history = [
        *_handoff_call_turn(author="triage", target="refunds", message="escalating"),
        _response(TextPart(content="refund issued"), name="refunds"),
    ]

    out = project(history, viewer="triage")

    self_resp = next(m for m in out if isinstance(m, ModelResponse))
    assert any(isinstance(p, ToolCallPart) and p.tool_name == "handoff_to_agent" for p in self_resp.parts)
    assert any(isinstance(p, ToolReturnPart) and p.tool_call_id == "h1" for m in out if isinstance(m, ModelRequest) for p in m.parts)


def test_handoff_tool_call_unparseable_args_logged_and_skipped(caplog):
    """spec §6 totality pin: unparseable handoff args are WARNING-logged and skipped —
    the turn drops like an ordinary internal call, never raising (mirrors the
    output-tool arm's log-and-drop)."""
    history: list[ModelMessage] = [
        _response(
            ToolCallPart(tool_name="handoff_to_agent", args="not json {{", tool_call_id="h1"),
            name="triage",
        ),
        _response(TextPart(content="hello"), name="refunds"),
    ]

    import logging

    with caplog.at_level(logging.WARNING, logger="calfkit.nodes._projection"):
        out = project(history, viewer="refunds")

    assert not any("handoff_to_agent" in t or "not json" in t for t in _user_prompt_texts(out))
    assert any("omitting structured component" in r.message for r in caplog.records)
