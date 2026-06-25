"""PR-C Item 2: the ``HandoffRequest`` output union — the stable base model + the memoized per-turn
``create_model`` builder (``Literal[name]`` + directory ``__doc__`` + ``__base__=HandoffRequest``).

``HandoffRequest`` is added to the agent's per-run ``output_type`` as a BARE union member (not a
``ToolOutput`` wrapper), so the schema stays ``allow_text_output=True`` on every provider (L4 — only a
``ToolOutput`` forces ``tool_choice=required``, the Anthropic+thinking crash). The per-turn subclass is
built once per distinct live directory (memoized) and discriminated by ``isinstance`` against the stable
base. An empty live set is unbuildable (no empty ``Literal``) so the dispatch path omits the member (§5.3).
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError

from calfkit._vendor.pydantic_ai import DeferredToolRequests
from calfkit._vendor.pydantic_ai._output import AutoOutputSchema, OutputSchema, ToolOutputSchema
from calfkit.peers.handoff import HandoffRequest, _build_handoff_request


class _Answer(BaseModel):
    text: str


# ── the stable base model ──


def test_handoff_request_base_fields() -> None:
    hr = HandoffRequest(name="billing", message="please take over")
    assert hr.name == "billing"
    assert hr.message == "please take over"


def test_handoff_request_message_required_non_empty() -> None:
    # L5/C2: message is required non-empty (Field(min_length=1)) — an empty value is auto-retried by
    # pydantic-ai just like an out-of-Literal name (do NOT rely on projection to catch a populated "").
    with pytest.raises(ValidationError):
        HandoffRequest(name="billing", message="")


def test_handoff_request_message_rejects_whitespace_only() -> None:
    # Parity with message_agent (which rejects via `not message.strip()`): a whitespace-only message is
    # blank, not content. The per-turn subclass inherits this (the validator is on the base).
    with pytest.raises(ValidationError):
        HandoffRequest(name="billing", message="   \t ")
    with pytest.raises(ValidationError):
        _build_handoff_request((("billing", None),))(name="billing", message="  ")


# ── the memoized per-turn builder ──


def test_build_handoff_request_subclasses_the_stable_base() -> None:
    # __base__=HandoffRequest keeps isinstance discrimination valid across per-turn rebuilds.
    built = _build_handoff_request((("billing", "Billing."), ("refunds", None)))
    assert issubclass(built, HandoffRequest)
    assert isinstance(built(name="billing", message="ctx"), HandoffRequest)


def test_build_handoff_request_name_is_literal_over_live_names() -> None:
    built = _build_handoff_request((("billing", None), ("refunds", None)))
    built(name="billing", message="x")  # in the Literal
    built(name="refunds", message="x")
    with pytest.raises(ValidationError):
        built(name="ghost", message="x")  # out-of-directory -> pydantic rejects (auto-retried upstream)


def test_build_handoff_request_doc_is_the_handoff_directory() -> None:
    built = _build_handoff_request((("billing", "Billing questions."), ("refunds", None)))
    assert built.__doc__ is not None
    assert "billing — Billing questions." in built.__doc__
    assert "refunds" in built.__doc__


def test_build_handoff_request_inherits_message_min_length() -> None:
    built = _build_handoff_request((("billing", None),))
    with pytest.raises(ValidationError):
        built(name="billing", message="")


def test_build_handoff_request_memoized_by_live_set() -> None:
    # @lru_cache: identical live sets reuse one model + its compiled schema; different sets are distinct.
    a = _build_handoff_request((("billing", "B."), ("refunds", None)))
    b = _build_handoff_request((("billing", "B."), ("refunds", None)))
    assert a is b
    assert _build_handoff_request((("billing", "B."),)) is not a  # fewer peers -> distinct
    # description IS part of the key (the __doc__ depends on it).
    assert _build_handoff_request((("billing", "CHANGED."), ("refunds", None))) is not a


def test_build_handoff_request_empty_live_rejected() -> None:
    # An empty Literal is unbuildable, so the dispatch path MUST omit the member (§5.3) — the builder fails
    # loud rather than emitting a never-valid option the model could only fail.
    with pytest.raises(ValueError, match="non-empty"):
        _build_handoff_request(())


# ── thinking-safe schema modes (L4): the built model in the per-run output_type ──


def test_str_agent_union_builds_tool_schema_allowing_text() -> None:
    # A `str` final_output_type + bare HandoffRequest -> ToolOutputSchema but text-ALLOWED (tool_choice
    # stays `auto`, not `required`) -> thinking-safe (only a ToolOutput wrapper forces required).
    built = _build_handoff_request((("billing", None),))
    schema = OutputSchema.build([str, built, DeferredToolRequests])
    assert isinstance(schema, ToolOutputSchema)
    assert schema.allows_text is True


def test_structured_agent_union_builds_auto_schema_allowing_text() -> None:
    # A structured final_output_type + bare HandoffRequest -> AutoOutputSchema (auto mode, identical to a
    # no-handoff structured agent -> degrades to native/prompted under thinking) -> thinking-safe.
    built = _build_handoff_request((("billing", None),))
    schema = OutputSchema.build([_Answer, built, DeferredToolRequests])
    assert isinstance(schema, AutoOutputSchema)
    assert schema.allows_text is True
