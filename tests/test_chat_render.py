"""Unit tests for the pure ``ck chat`` renderer (``calfkit.cli._chat_render``).

The renderer returns ``list[str]`` and does no I/O, so every behavior is asserted
directly on the produced lines — the alignment/indentation IS the contract.
"""

from __future__ import annotations

from datetime import datetime, timezone

from calfkit.cli._chat_render import _placeholder, _render_answer, _render_fault, _render_step, format_picker
from calfkit.client.mesh import AgentInfo
from calfkit.exceptions import NodeFaultError
from calfkit.models.payload import DataPart, FilePart, TextPart, ToolCallPart
from calfkit.models.step import AgentMessageEvent, AgentThinkingEvent, HandoffEvent, ToolCallEvent, ToolResultEvent

# --- event/DTO builders (the base hop-identity fields are noise for rendering) ---


def _msg(emitter: str, parts: list) -> AgentMessageEvent:
    return AgentMessageEvent(correlation_id="c", depth=0, frame_id="f", emitter=emitter, parts=parts)


def _call(emitter: str, name: str, args: object = None) -> ToolCallEvent:
    return ToolCallEvent(correlation_id="c", depth=0, frame_id="f", emitter=emitter, tool_call_id="t1", name=name, args=args)


def _result(emitter: str, name: str, parts: list, *, is_error: bool = False) -> ToolResultEvent:
    return ToolResultEvent(correlation_id="c", depth=0, frame_id="f", emitter=emitter, tool_call_id="t1", name=name, parts=parts, is_error=is_error)


def _handoff(emitter: str, target: str, reason: str) -> HandoffEvent:
    return HandoffEvent(correlation_id="c", depth=0, frame_id="f", emitter=emitter, target=target, reason=reason)


def _agent(name: str, desc: str | None) -> AgentInfo:
    return AgentInfo(name=name, description=desc, last_seen=datetime(2026, 1, 1, tzinfo=timezone.utc))


# --- _render_step: tags, emitter header, block layout (content column = 17) ------


def test_message_emits_header_then_block() -> None:
    lines, emitter = _render_step(_msg("bot", [TextPart(text="Let me look.")]), None)
    assert lines == ["", "bot", "  [message]      Let me look."]
    assert emitter == "bot"


def test_no_header_when_emitter_unchanged() -> None:
    lines, emitter = _render_step(_call("bot", "lookup_order", {"id": "4471"}), "bot")
    assert lines == ["  [tool call]    lookup_order(id='4471')"]
    assert emitter == "bot"


def test_emitter_change_emits_new_header() -> None:
    lines, emitter = _render_step(_call("billing", "issue_refund"), "support-bot")
    assert lines == ["", "billing", "  [tool call]    issue_refund()"]
    assert emitter == "billing"


def test_tool_result_success_tag() -> None:
    lines, _ = _render_step(_result("bot", "lookup_order", [TextPart(text="in transit")]), "bot")
    assert lines == ["  [tool result]  in transit"]


def test_tool_result_error_tag() -> None:
    lines, _ = _render_step(_result("bot", "issue_refund", [TextPart(text="declined")], is_error=True), "bot")
    assert lines == ["  [tool error]   declined"]


def test_handoff_tag_and_content() -> None:
    lines, _ = _render_step(_handoff("bot", "billing", "refund authority required"), "bot")
    assert lines == ["  [handoff]      billing (refund authority required)"]


def test_multiline_content_block_indented_to_content_column() -> None:
    parts = [TextPart(text="Refund policy:\n- within 30 days: full\n- after: store credit")]
    lines, _ = _render_step(_result("bot", "search", parts), "bot")
    assert lines == [
        "  [tool result]  Refund policy:",
        "                 - within 30 days: full",
        "                 - after: store credit",
    ]


def test_multiline_message_is_also_block_indented() -> None:
    lines, _ = _render_step(_msg("bot", [TextPart(text="line one\nline two")]), "bot")
    assert lines == ["  [message]      line one", "                 line two"]


# --- tool-call args: dict / str / None ------------------------------------------


def test_tool_args_dict() -> None:
    lines, _ = _render_step(_call("bot", "f", {"a": 1, "b": "x"}), "bot")
    assert lines == ["  [tool call]    f(a=1, b='x')"]


def test_tool_args_raw_str_verbatim() -> None:
    lines, _ = _render_step(_call("bot", "f", '{"q": "x"}'), "bot")
    assert lines == ['  [tool call]    f({"q": "x"})']


def test_tool_args_none_is_empty_parens() -> None:
    lines, _ = _render_step(_call("bot", "ping", None), "bot")
    assert lines == ["  [tool call]    ping()"]


# --- File / ToolCall parts -> visible placeholder (never dropped, D7) ------------


def test_file_part_renders_placeholder() -> None:
    lines, _ = _render_step(_result("bot", "gen", [FilePart(media_type="image/png")]), "bot")
    assert lines == ["  [tool result]  [file: image/png]"]


def test_toolcall_part_renders_placeholder() -> None:
    lines, _ = _render_step(_msg("bot", [ToolCallPart(tool_call_id="t", tool_name="foo", kwargs={})]), "bot")
    assert lines == ["  [message]      [tool: foo]"]


def test_datapart_renders_as_json() -> None:
    lines, _ = _render_step(_result("bot", "q", [DataPart(data={"k": 1})]), "bot")
    assert lines == ['  [tool result]  {"k":1}']


# --- empty [message] is skipped entirely (no header, no line, emitter unchanged) -


def test_empty_message_skipped_emitter_unchanged() -> None:
    lines, emitter = _render_step(_msg("newbot", []), "oldbot")
    assert lines == []
    assert emitter == "oldbot"


# --- final answer (raw) and fault ------------------------------------------------


def test_render_answer_raw() -> None:
    assert _render_answer("support-bot", "Order shipped.") == ["", "support-bot > Order shipped."]


def test_render_answer_empty_is_placeholder() -> None:
    assert _render_answer("bot", "") == ["", "bot > (no text reply)"]


def test_render_fault_uses_report_message() -> None:
    err = NodeFaultError("billing.denied", message="quota exceeded")
    assert _render_fault("bot", err) == ["", "bot > [error] quota exceeded"]


def test_render_fault_falls_back_to_error_type() -> None:
    err = NodeFaultError("billing.denied")
    assert _render_fault("bot", err) == ["", "bot > [error] billing.denied"]


# --- picker: alphabetical, (no description), numbered ----------------------------


def test_format_picker_renders_given_order_and_no_description() -> None:
    agents = {
        "support-bot": _agent("support-bot", "Support"),
        "researcher": _agent("researcher", "Deep research"),
        "summarizer": _agent("summarizer", None),
    }
    names = ["researcher", "summarizer", "support-bot"]  # the caller sorts; the picker renders this order
    assert format_picker(names, agents) == [
        "Online agents",
        "",
        "  1  researcher   Deep research",
        "  2  summarizer   (no description)",
        "  3  support-bot  Support",
    ]


def test_format_picker_empty_does_not_crash() -> None:
    assert format_picker([], {}) == ["Online agents", ""]


def test_placeholder_drops_unhandled_part() -> None:
    # render_other only sees File/ToolCall in practice; anything else falls through to None (dropped).
    assert _placeholder(TextPart(text="x")) is None


def test_render_step_skips_unmodeled_event() -> None:
    # AgentThinkingEvent is a defined-but-not-emitted surface event: not one of the four rendered
    # kinds, so it is skipped with the emitter left unchanged.
    ev = AgentThinkingEvent(correlation_id="c", depth=0, frame_id="f", emitter="bot", parts=[TextPart(text="hmm")])
    assert _render_step(ev, "prev") == ([], "prev")
