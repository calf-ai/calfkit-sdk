"""Tests for the identity-stamping helper (docs/designs/agent-pov-projection.md §4, §14.5).

`CoreMessageState.extend_with_responses(messages, author)` appends run-produced
messages to the canonical history, stamping `name=author` on any `ModelResponse`
whose `name` is still `None`. Already-named responses are left untouched, and
`ModelRequest`s are never modified. The stamped `name` is a pre-existing optional
field on `ModelResponse`, so it round-trips through `State` (de)serialization
across the Kafka wire format.

These tests use the REAL vendored pydantic-ai types.
"""

from __future__ import annotations

from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    UserPromptPart,
)
from calfkit.models.state import CoreMessageState, State


def _user(text: str) -> ModelRequest:
    return ModelRequest(parts=[UserPromptPart(content=text)])


def _response(text: str, *, name: str | None = None) -> ModelResponse:
    return ModelResponse(parts=[TextPart(content=text)], name=name)


# --------------------------------------------------------------------------- #
# §14.5 Stamping helper                                                        #
# --------------------------------------------------------------------------- #


def test_stamps_unnamed_model_responses_with_author():
    """An un-named ModelResponse is stamped with the author id."""
    state = CoreMessageState()
    messages: list[ModelMessage] = [_response("hello")]

    state.extend_with_responses(messages, author="scheduler")

    assert len(state.message_history) == 1
    appended = state.message_history[0]
    assert isinstance(appended, ModelResponse)
    assert appended.name == "scheduler"


def test_leaves_already_named_responses_unchanged():
    """A ModelResponse that already carries a name is NOT overwritten."""
    state = CoreMessageState()
    messages: list[ModelMessage] = [_response("hi", name="researcher")]

    state.extend_with_responses(messages, author="scheduler")

    appended = state.message_history[0]
    assert isinstance(appended, ModelResponse)
    # the pre-existing name wins (idempotent guard lets inner-wrapper/provider name win)
    assert appended.name == "researcher"


def test_leaves_model_requests_untouched():
    """ModelRequests are appended verbatim and never gain a name."""
    state = CoreMessageState()
    req = _user("a tool return turn")
    messages: list[ModelMessage] = [req]

    state.extend_with_responses(messages, author="scheduler")

    assert len(state.message_history) == 1
    appended = state.message_history[0]
    assert isinstance(appended, ModelRequest)
    # no `name` attribute mutation on a request — it is the same object, unchanged
    assert appended == req
    # the UserPromptPart carries no author name
    assert appended.parts[0].name is None


def test_extends_existing_history_in_order():
    """The helper extends (not replaces) the history, preserving order."""
    state = CoreMessageState(message_history=[_user("kick off")])
    messages: list[ModelMessage] = [
        _response("preamble", name=None),
        _user("a tool return"),
        _response("named already", name="researcher"),
    ]

    state.extend_with_responses(messages, author="scheduler")

    assert len(state.message_history) == 4
    # original first message preserved
    assert isinstance(state.message_history[0], ModelRequest)
    assert state.message_history[0].parts[0].content == "kick off"
    # un-named response stamped
    assert state.message_history[1].name == "scheduler"
    # request untouched
    assert isinstance(state.message_history[2], ModelRequest)
    # already-named response unchanged
    assert state.message_history[3].name == "researcher"


def test_stamped_name_roundtrips_through_state_serialization():
    """The stamped `name` survives State JSON (de)serialization over the wire."""
    state = State()
    state.extend_with_responses([_response("you have a 2pm sync")], author="scheduler")

    json_body = state.model_dump_json()
    restored = State.model_validate_json(json_body)

    assert len(restored.message_history) == 1
    restored_resp = restored.message_history[0]
    assert isinstance(restored_resp, ModelResponse)
    assert restored_resp.name == "scheduler"
    # full equality round-trip
    assert restored == state
