"""Pure rendering for ``ck chat`` (plan §8): step events / picker -> ``list[str]``.

No ``print``, no I/O — ``_chat.py`` owns all output. Work-log layout: each line is
``INDENT + tag.ljust(TAG_W) + TAG_GAP + content``; a multi-line content's
continuation lines align to the content column. The final answer prints raw (no
tag, no wrap).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from calfkit.models.payload import FilePart, ToolCallPart, render_parts_as_text
from calfkit.models.step import AgentMessageEvent, HandoffEvent, ToolCallEvent, ToolResultEvent

if TYPE_CHECKING:
    from collections.abc import Mapping

    from calfkit.client.mesh import AgentInfo
    from calfkit.exceptions import NodeFaultError
    from calfkit.models.payload import ContentPart
    from calfkit.models.step import RunStepEvent

# One source of truth: each event TYPE -> its tag. Dispatch and TAG_W both read
# this map, so a tag can never drift between the two. ``ToolResultEvent`` maps to
# "[tool result]" or one of the per-outcome overrides below: ``failed`` -> "[tool error]",
# ``denied`` -> the neutral "[tool denied]" (a refusal-to-dispatch is not an error, spec §4).
_TAGS: dict[type, str] = {
    AgentMessageEvent: "[message]",
    ToolCallEvent: "[tool call]",
    ToolResultEvent: "[tool result]",
    HandoffEvent: "[handoff]",
}
_OUTCOME_TAGS = {"failed": "[tool error]", "denied": "[tool denied]"}
TAG_W = max(len(tag) for tag in (*_TAGS.values(), *_OUTCOME_TAGS.values()))  # 13, derived — never hardcoded
INDENT = "  "
TAG_GAP = "  "
_CONTENT_COL = len(INDENT) + TAG_W + len(TAG_GAP)


def _placeholder(part: ContentPart) -> str | None:
    """``render_other`` for the work-log: a visible placeholder for the parts the
    text renderer doesn't itself handle (File / ToolCall), so nothing is silently
    dropped (D7). Any other part falls through to ``None`` (dropped)."""
    if isinstance(part, FilePart):
        return f"[file: {part.media_type}]"
    if isinstance(part, ToolCallPart):
        return f"[tool: {part.tool_name}]"
    return None


def _parts_text(parts: list[ContentPart]) -> str:
    return render_parts_as_text(parts, render_other=_placeholder, empty="")


def _format_args(args: str | dict[str, Any] | None) -> str:
    """Render a tool call's args (the raw model emission) compactly."""
    if args is None:
        return ""
    if isinstance(args, dict):
        return ", ".join(f"{key}={value!r}" for key, value in args.items())
    return args  # a raw (possibly unparsed) string, shown verbatim


def _block(tag: str, content: str) -> list[str]:
    """One work-log entry: ``tag`` + ``content``, with multi-line content's
    continuation lines indented to the content column."""
    head, *rest = content.split("\n")
    lines = [f"{INDENT}{tag.ljust(TAG_W)}{TAG_GAP}{head}"]
    lines.extend(f"{' ' * _CONTENT_COL}{line}" for line in rest)
    return lines


def _render_step(ev: RunStepEvent, current_emitter: str | None) -> tuple[list[str], str | None]:
    """Render one step event to its work-log lines, prepending a blank line + the
    emitter name when the emitter changes. An empty ``[message]`` is skipped
    entirely (no header, no line) and leaves ``current_emitter`` unchanged."""
    if isinstance(ev, AgentMessageEvent):
        content = _parts_text(ev.parts)
        if not content:
            return [], current_emitter
    elif isinstance(ev, ToolCallEvent):
        content = f"{ev.name}({_format_args(ev.args)})"
    elif isinstance(ev, ToolResultEvent):
        content = _parts_text(ev.parts)
    elif isinstance(ev, HandoffEvent):
        content = f"{ev.target} ({ev.reason})"
    else:  # defensive: an unmodeled/future surface step event
        return [], current_emitter
    tag = _OUTCOME_TAGS.get(ev.outcome, _TAGS[ToolResultEvent]) if isinstance(ev, ToolResultEvent) else _TAGS[type(ev)]
    lines = ["", ev.emitter] if ev.emitter != current_emitter else []
    lines.extend(_block(tag, content))
    return lines, ev.emitter


def _render_answer(responder: str, output: str) -> list[str]:
    """The final answer, raw (D8). An empty reply (e.g. a File/ToolCall-only
    output) shows a placeholder rather than a bare ``responder >``."""
    return ["", f"{responder} > {output or '(no text reply)'}"]


def _error_line(agent_name: str, message: str) -> list[str]:
    """The ``‹agent› > [error] …`` line — shared by faults and turn-level errors."""
    return ["", f"{agent_name} > [error] {message}"]


def _render_fault(agent_name: str, err: NodeFaultError) -> list[str]:
    """The fault line for a turn whose ``result()`` raised ``NodeFaultError``."""
    report = err.report
    return _error_line(agent_name, report.message or report.error_type or "fault")


def format_picker(names: list[str], agents: Mapping[str, AgentInfo]) -> list[str]:
    """The numbered agent picker, rendering ``names`` in the given order (the caller
    sorts) — ``index · name · description`` (``(no description)`` when absent) (D9)."""
    idx_w = len(str(len(names)))
    name_w = max((len(name) for name in names), default=0)
    lines = ["Online agents", ""]
    for index, name in enumerate(names, start=1):
        description = agents[name].description or "(no description)"
        lines.append(f"{INDENT}{str(index).rjust(idx_w)}{TAG_GAP}{name.ljust(name_w)}{TAG_GAP}{description}")
    return lines
