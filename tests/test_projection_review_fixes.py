"""Regression tests for issues found in the PR #185 deep review.

1. A final_result whose args can't render must NOT raise (would hang the agent).
2. Multimodal human content must be preserved, not stringified, in multi-participant mode.
3. structured_output_preamble must return "" in native/prompted mode (where the
   response TextPart IS the JSON answer) to avoid duplicating the structured value.
"""

from __future__ import annotations

from calfkit._vendor.pydantic_ai.messages import (
    BinaryContent,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from calfkit.nodes._projection import project, structured_output_preamble


def _texts(messages) -> list[str]:
    out: list[str] = []
    for m in messages:
        if isinstance(m, ModelRequest):
            for p in m.parts:
                if isinstance(p, UserPromptPart) and isinstance(p.content, str):
                    out.append(p.content)
    return out


# --- Bug 1: un-renderable final_result args must degrade, not raise/hang --- #


def test_other_agent_final_result_unrenderable_args_degrades_not_raises():
    history = [
        ModelRequest(parts=[UserPromptPart(content="kick off")]),
        # args is a JSON string that parses to a non-dict -> args_as_dict() raises
        ModelResponse(
            parts=[TextPart("here"), ToolCallPart(tool_name="final_result", args="[1,2,3]", tool_call_id="c1")],
            name="researcher",
        ),
        ModelResponse(parts=[TextPart("ok")], name="scheduler"),
    ]
    # Two distinct agent names -> multi-participant projection runs.
    out = project(history, viewer="scheduler")  # must NOT raise
    # The text preamble still surfaces; the un-renderable structured part is dropped.
    assert any("here" in t for t in _texts(out))


# --- Bug 2: multimodal human content preserved, prefix prepended as text --- #


def test_other_human_multimodal_content_preserves_binary_and_prefixes():
    img = BinaryContent(data=b"\x89PNG", media_type="image/png")
    history = [
        ModelRequest(parts=[UserPromptPart(content=["look:", img], name="alice")]),
        ModelRequest(parts=[UserPromptPart(content="hi", name="bob")]),  # 2 named humans -> multi
    ]
    out = project(history, viewer="some_agent")
    alice_req = out[0]
    content = alice_req.parts[0].content
    assert isinstance(content, list), "multimodal content must stay a list, not be stringified"
    assert img in content, "the binary image part must be preserved"
    assert any(isinstance(c, str) and "<user:alice>" in c for c in content), "prefix prepended as a text element"


# --- Bug 3: structured_output_preamble gated on tool mode (final_result call) --- #


def test_structured_preamble_tool_mode_returns_preamble_text():
    new_messages = [
        ModelResponse(parts=[TextPart("On it."), ToolCallPart(tool_name="final_result", args={"x": 1}, tool_call_id="c1")]),
        ModelRequest(parts=[ToolReturnPart(tool_name="final_result", content="Final result processed.", tool_call_id="c1")]),
    ]
    assert structured_output_preamble(new_messages) == "On it."


def test_structured_preamble_native_mode_returns_empty():
    # Native/prompted: the final response is just the JSON answer as a TextPart,
    # with NO final_result tool call. Returning it would duplicate the DataPart.
    new_messages = [ModelResponse(parts=[TextPart('{"x":1}')])]
    assert structured_output_preamble(new_messages) == ""
