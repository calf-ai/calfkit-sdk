"""Tests for the shared reply-parts renderer (``calfkit.models.payload.render_parts_as_text``).

One canonical "parts -> one string" routine, used by the client/consumer ``output_type=str``
projection (spec §2.2) and the agent's ``message_agent`` peer-reply fold (agent spec §5.2):
``TextPart`` verbatim, ``DataPart`` as JSON (``pydantic_core.to_json``), other parts via the
caller's ``render_other`` (``None`` = skip); newline-joined; empty -> the caller's sentinel.
"""

from __future__ import annotations

import pydantic_core

from calfkit.models.payload import ContentPart, DataPart, FilePart, TextPart, ToolCallPart, render_parts_as_text


def _skip(_p: ContentPart) -> None:
    """A ``render_other`` that drops every non-Text/Data part (the ``output_type=str`` config)."""
    return None


def test_text_part_rendered_verbatim() -> None:
    assert render_parts_as_text([TextPart(text="hi")], render_other=_skip, empty="") == "hi"


def test_data_part_rendered_as_json() -> None:
    assert render_parts_as_text([DataPart(data={"k": 1})], render_other=_skip, empty="") == pydantic_core.to_json({"k": 1}).decode()


def test_text_and_data_joined_with_newline() -> None:
    out = render_parts_as_text([TextPart(text="lead"), DataPart(data={"k": 1})], render_other=_skip, empty="")
    assert out == "lead\n" + pydantic_core.to_json({"k": 1}).decode()


def test_multiple_text_parts_joined_with_newline() -> None:
    assert render_parts_as_text([TextPart(text="a"), TextPart(text="b")], render_other=_skip, empty="") == "a\nb"


def test_empty_parts_returns_the_sentinel() -> None:
    assert render_parts_as_text([], render_other=_skip, empty="(none)") == "(none)"
    assert render_parts_as_text(None, render_other=_skip, empty="(none)") == "(none)"


def test_render_other_returning_none_skips_the_part() -> None:
    out = render_parts_as_text([TextPart(text="keep"), FilePart(media_type="image/png")], render_other=_skip, empty="")
    assert out == "keep"


def test_render_other_returning_a_string_is_included() -> None:
    out = render_parts_as_text(
        [TextPart(text="keep"), ToolCallPart(tool_call_id="x", kwargs={"a": 1}, tool_name="t")],
        render_other=lambda _p: "[other]",
        empty="",
    )
    assert out == "keep\n[other]"
