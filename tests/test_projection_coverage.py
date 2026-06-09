"""Additional projection coverage closing gaps the PR #185 review flagged.

- <unknown>-owned tool-return is dropped for a real viewer (privacy; §5.3).
- A single ModelRequest carrying tool-returns for two owners keeps only the
  viewer-owned one (the partial-keep branch; §5.3).
- A projected history satisfies the structural invariants the strict Anthropic
  mapping needs after pydantic-ai's own normalizer runs (§14.7).
- The §7 [TextPart, DataPart] final_output_parts ordering is deserialized by
  TYPE, not position (§7).
"""

from __future__ import annotations

from calfkit._vendor.pydantic_ai._agent_graph import _clean_message_history
from calfkit._vendor.pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from calfkit.models import DataPart
from calfkit.models import TextPart as PayloadTextPart
from calfkit.models.node_result import _UNSET, _extract_output
from calfkit.nodes._projection import project


def _user(text: str, *, name: str | None = None) -> ModelRequest:
    return ModelRequest(parts=[UserPromptPart(content=text, name=name)])


def _tool_return(tool_name: str, content: str, *, tool_call_id: str) -> ModelRequest:
    return ModelRequest(parts=[ToolReturnPart(tool_name=tool_name, content=content, tool_call_id=tool_call_id)])


# --- <unknown>-owned tool-return must not leak to a real viewer (§5.3) --- #


def test_unknown_owned_tool_return_dropped_for_other_viewer():
    history = [
        # Un-named (legacy) response that called a tool → owner resolves to "unknown".
        ModelResponse(parts=[ToolCallPart(tool_name="search", args={}, tool_call_id="u1")], name=None),
        _tool_return("search", "SECRET", tool_call_id="u1"),
        ModelResponse(parts=[TextPart("done")], name="scheduler"),
        ModelResponse(parts=[TextPart("hi")], name="researcher"),  # 2 named agents → multi-participant
    ]
    out = project(history, viewer="scheduler")
    # The <unknown> agent's tool result is never surfaced to the viewer.
    for m in out:
        for p in m.parts:
            assert not (isinstance(p, ToolReturnPart) and p.content == "SECRET")
            assert not (isinstance(p, UserPromptPart) and isinstance(p.content, str) and "SECRET" in p.content)


# --- mixed-owner tool-return request keeps only the viewer-owned part (§5.3) --- #


def test_mixed_owner_tool_return_request_keeps_only_viewer_owned():
    history = [
        ModelResponse(parts=[ToolCallPart(tool_name="a", args={}, tool_call_id="a1")], name="scheduler"),
        ModelResponse(parts=[ToolCallPart(tool_name="b", args={}, tool_call_id="b1")], name="researcher"),
        # One request carrying BOTH agents' tool returns (the partial-keep branch).
        ModelRequest(
            parts=[
                ToolReturnPart(tool_name="a", content="for-scheduler", tool_call_id="a1"),
                ToolReturnPart(tool_name="b", content="for-researcher", tool_call_id="b1"),
            ]
        ),
    ]
    out = project(history, viewer="scheduler")
    returns = [p for m in out if isinstance(m, ModelRequest) for p in m.parts if isinstance(p, ToolReturnPart)]
    assert len(returns) == 1
    assert returns[0].tool_call_id == "a1"
    assert returns[0].content == "for-scheduler"


# --- §14.7 provider-validity: projected history is structurally valid --- #


def test_projected_history_is_provider_valid_after_clean():
    """A projected history, after pydantic-ai's own ``_clean_message_history`` (which
    the agent loop runs), must satisfy the invariants the strict Anthropic mapping
    requires: a leading user turn (human-led), no consecutive same-role messages, and
    no orphaned tool returns (every ToolReturnPart id has a preceding ToolCallPart).
    """
    history = [
        _user("kick off"),  # a human leads
        ModelResponse(parts=[ToolCallPart(tool_name="search", args={}, tool_call_id="s1")], name="alpha"),
        _tool_return("search", "result", tool_call_id="s1"),  # alpha's own
        ModelResponse(parts=[TextPart("found it")], name="alpha"),
        ModelResponse(
            parts=[TextPart("nice"), ToolCallPart(tool_name="final_result", args={"x": 1}, tool_call_id="f1")],
            name="beta",
        ),
        _tool_return("final_result", "Final result processed.", tool_call_id="f1"),
    ]
    cleaned = _clean_message_history(project(history, viewer="alpha"))

    assert isinstance(cleaned[0], ModelRequest), "human-led history must project to a leading user turn"
    for a, b in zip(cleaned, cleaned[1:]):
        assert not (isinstance(a, ModelResponse) and isinstance(b, ModelResponse)), "no consecutive assistant turns"

    seen_call_ids: set[str] = set()
    for m in cleaned:
        if isinstance(m, ModelResponse):
            seen_call_ids.update(tc.tool_call_id for tc in m.tool_calls)
        else:
            for p in m.parts:
                if isinstance(p, ToolReturnPart):
                    assert p.tool_call_id in seen_call_ids, "no orphaned tool_result"


# --- §7: deserialization selects by type, not position --- #


def test_deserialize_selects_by_type_not_position_with_preamble():
    parts = [PayloadTextPart(text="On it."), DataPart(data={"flights": 3})]
    # auto-detect (no output_type) still prefers the DataPart despite the leading TextPart
    assert _extract_output(parts, _UNSET) == {"flights": 3}
    # a structured caller gets the DataPart
    assert _extract_output(parts, dict) == {"flights": 3}
    # a str caller gets the preamble text
    assert _extract_output(parts, str) == "On it."
