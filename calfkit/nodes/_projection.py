"""Agent-POV message-history projection.

See ``docs/agent-pov-projection.md`` §5 for the authoritative design.

``project(history, viewer)`` is a **pure** function: it returns a new
``list[ModelMessage]``, constructs new message/part objects (never mutating the
canonical history, so re-projection for the next viewer is always clean), and
strips ``name`` from every message it emits (§5.5) so attribution never reaches a
provider.

Detection (§5.1): if a single role is unambiguous (one agent, ≤1 named human)
the history passes through transparently (byte-identical model input to today,
but with ``name`` stripped); otherwise other participants are re-roled to
attributed, surface-only ``ModelRequest`` user turns.
"""

from __future__ import annotations

import json
from dataclasses import replace

from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelRequestPart,
    ModelResponse,
    TextPart,
    ToolCallPart,
    UserPromptPart,
)

# The default name pydantic-ai gives the structured-output tool (§5.5). A user
# who customizes it via ``ToolOutput(Model, name=...)`` is a documented
# limitation (§16): its structured answer is not surfaced cross-agent.
_FINAL_RESULT_TOOL_NAME = "final_result"

# Fallback author for an un-``name``d ``ModelResponse`` (legacy / pre-feature
# histories). Treated as "other"; the prefix becomes ``<unknown>`` once the
# author is wrapped in angle brackets (§5.3, §5.4).
_UNKNOWN_AUTHOR = "unknown"


def project(history: list[ModelMessage], viewer: str) -> list[ModelMessage]:
    """Project ``history`` to ``viewer``'s point of view.

    Returns a fresh list of fresh message objects. The input is never mutated.
    """
    agent_names = {m.name for m in history if isinstance(m, ModelResponse) and m.name}
    human_names = {p.name for m in history if isinstance(m, ModelRequest) for p in m.parts if isinstance(p, UserPromptPart) and p.name}

    multi_participant = len(agent_names) >= 2 or len(human_names) >= 2

    if not multi_participant:
        return _project_transparent(history)
    return _project_multi(history, viewer)


# --------------------------------------------------------------------------- #
# Transparent pass-through (§5.1)                                             #
# --------------------------------------------------------------------------- #


def _project_transparent(history: list[ModelMessage]) -> list[ModelMessage]:
    """Keep roles, add no prefixes, strip ``name`` from every emitted message."""
    out: list[ModelMessage] = []
    for m in history:
        if isinstance(m, ModelResponse):
            out.append(replace(m, name=None))
        else:
            out.append(_strip_request_names(m))
    return out


def _strip_request_names(m: ModelRequest) -> ModelRequest:
    """Return a new ``ModelRequest`` with ``name`` stripped from every UserPromptPart."""
    new_parts: list[ModelRequestPart] = []
    changed = False
    for p in m.parts:
        if isinstance(p, UserPromptPart) and p.name is not None:
            new_parts.append(replace(p, name=None))
            changed = True
        else:
            new_parts.append(p)
    if not changed:
        return m
    return replace(m, parts=new_parts)


# --------------------------------------------------------------------------- #
# Multi-participant projection (§5.2–§5.5)                                     #
# --------------------------------------------------------------------------- #


def _project_multi(history: list[ModelMessage], viewer: str) -> list[ModelMessage]:
    owner_by_tool_call_id = _tool_call_owner_map(history)

    out: list[ModelMessage] = []
    for m in history:
        if isinstance(m, ModelResponse):
            out.extend(_project_response(m, viewer))
        else:  # ModelRequest
            out.extend(_project_request(m, viewer, owner_by_tool_call_id))
    return out


def _tool_call_owner_map(history: list[ModelMessage]) -> dict[str, str]:
    """Map ``tool_call_id`` → owning ``ModelResponse.name`` (or ``<unknown>``)."""
    owners: dict[str, str] = {}
    for m in history:
        if isinstance(m, ModelResponse):
            author = m.name or _UNKNOWN_AUTHOR
            for p in m.parts:
                if isinstance(p, ToolCallPart):
                    owners[p.tool_call_id] = author
    return owners


def _project_response(m: ModelResponse, viewer: str) -> list[ModelMessage]:
    author = m.name or _UNKNOWN_AUTHOR
    if author == viewer:
        # Self — full fidelity, name stripped, parts unchanged (§5.2).
        return [replace(m, name=None)]

    # Other — surface-only, attributed (§5.2, §5.5).
    surface = _surface(m)
    if not surface:
        return []  # empty surface (hand-off) → omitted (§5.5)
    prefix = f"<{author}>"
    return [ModelRequest(parts=[UserPromptPart(content=f"{prefix}\n{surface}")])]


def _project_request(
    m: ModelRequest,
    viewer: str,
    owner_by_tool_call_id: dict[str, str],
) -> list[ModelMessage]:
    # A ModelRequest carries either human UserPromptParts or tool returns /
    # retry prompts (an internal tool-exchange turn). Per §5.3 these are
    # mutually exclusive turn shapes in practice; we classify part-wise so a
    # mixed request is handled safely.
    has_user_prompt = any(isinstance(p, UserPromptPart) for p in m.parts)

    if has_user_prompt:
        # Human turn → attributed user request (§5.2).
        new_parts: list[ModelRequestPart] = []
        for p in m.parts:
            if isinstance(p, UserPromptPart):
                prefix = _human_prefix(p)
                new_parts.append(UserPromptPart(content=f"{prefix} {p.content}"))
            # Any non-UserPromptPart in a human request is internal — drop it.
        return [ModelRequest(parts=new_parts)] if new_parts else []

    # Tool-exchange request (ToolReturnPart / RetryPromptPart). Owner resolved by
    # tool_call_id (§5.3). For self: keep the whole request verbatim. For other:
    # drop. A request can only mix owners if the SDK user constructed it that
    # way; resolve per-tool-call-id and keep only the viewer-owned returns.
    kept_parts: list[ModelRequestPart] = []
    for p in m.parts:
        tool_call_id = getattr(p, "tool_call_id", None)
        owner = owner_by_tool_call_id.get(tool_call_id) if tool_call_id else None
        if owner == viewer:
            kept_parts.append(p)
    if not kept_parts:
        return []
    if len(kept_parts) == len(m.parts):
        # All parts kept → preserve the original request verbatim (self-view).
        return [m]
    return [ModelRequest(parts=kept_parts)]


def _human_prefix(p: UserPromptPart) -> str:
    if p.name:
        return f"<user:{p.name}>"
    return "<user>"


def _surface(m: ModelResponse) -> str:
    """Compute the public surface of an other-agent ``ModelResponse`` (§5.5).

    surface = concatenated ``TextPart`` text(s) + rendered ``final_result`` args,
    non-empty components joined with a single ``"\\n"``.
    """
    components: list[str] = []
    for p in m.parts:
        if isinstance(p, TextPart):
            if p.content:
                components.append(p.content)
        elif isinstance(p, ToolCallPart) and p.tool_name == _FINAL_RESULT_TOOL_NAME:
            # Branch on truthiness of args directly (NOT the rendered string, and
            # NOT has_content() which drops {"x": 0}). (§5.5)
            if p.args:
                components.append(_render_structured_args(p))
        # Ordinary tool calls / thinking / file parts are internal → dropped.
    return "\n".join(components)


def _render_structured_args(p: ToolCallPart) -> str:
    """Render structured args canonically (compact, sorted) — provider-independent (§5.5)."""
    return json.dumps(p.args_as_dict(), separators=(",", ":"), sort_keys=True)
