"""Agent-POV message-history projection and final-output surface helpers.

See ``docs/designs/agent-pov-projection.md`` §5/§7 for the authoritative design.

``project(history, viewer)`` is a **pure** function: it returns a new
``list[ModelMessage]``, constructs new message/part objects (never mutating the
canonical history, so re-projection for the next viewer is always clean), and
strips ``name`` from every message it emits (§5.5) so attribution never reaches a
provider.

Detection (§5.1, viewer-aware): if every authored turn is the viewer's own (no
agent *other than* the viewer) and there is ≤1 named human, the history passes
through transparently (byte-identical model input to today, but with ``name``
stripped); otherwise other participants — including a *single* other agent, e.g.
a handed-off conversation — are re-roled to attributed, surface-only
``ModelRequest`` user turns. (Counting distinct authors instead of comparing to
the viewer would miss a single other-agent's history.)

``structured_output_preamble`` is the client-facing sibling (§7): given a run's
new messages it returns the tool-mode text preamble that accompanies a structured
answer. It lives here because it shares the output-tool (``final_result*``, via
``_is_output_tool``) / ``TextPart`` shape with the projection surface (``_surface``).
"""

from __future__ import annotations

import json
import logging
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
from calfkit.peers.handoff import HANDOFF_TOOL

logger = logging.getLogger(__name__)

# The base name pydantic-ai gives the structured-output tool (§5.5). For a single
# output it is exactly ``final_result``; for a 2+-output union it sets
# ``multiple=True`` and renames each output tool ``final_result_<TypeName>``
# (``_output.py`` ``OutputToolset.build``). ``_is_output_tool`` recognizes both. A
# user who customizes it via ``ToolOutput(Model, name=...)`` falls outside this
# namespace and is the documented limitation (§16): not surfaced cross-agent.
_FINAL_RESULT_TOOL_NAME = "final_result"

# Fallback author for an un-``name``d ``ModelResponse`` (legacy / pre-feature
# histories). Treated as "other"; the prefix becomes ``<unknown>`` once the
# author is wrapped in angle brackets (§5.3, §5.4).
_UNKNOWN_AUTHOR = "unknown"


def _is_output_tool(tool_name: str) -> bool:
    """True for the auto-named structured-output tool(s) (§5.5).

    pydantic-ai names the output tool ``final_result`` for a single output, and
    ``final_result_<TypeName>`` for each member of a multi-output union
    (``_output.py`` ``OutputToolset.build``, ``multiple=True``). Both must be
    surfaced cross-agent. ``final_result*`` is treated as pydantic-ai's reserved
    output-tool namespace: a user-customized ``ToolOutput(name=...)`` falls outside
    it and remains the documented limitation (§16, not surfaced), while a *function*
    tool a user names ``final_result_*`` would be matched here and mis-surfaced — so
    keep function-tool names out of that namespace (§16).
    """
    return tool_name == _FINAL_RESULT_TOOL_NAME or tool_name.startswith(_FINAL_RESULT_TOOL_NAME + "_")


def _is_handoff_tool(tool_name: str) -> bool:
    """True for the reserved handoff transport tool (handoff-tool-transport-spec §6).

    ``handoff_to_agent`` is calfkit-reserved at agent construction whenever a ``Handoff``
    handle is present (the only agents that can emit a genuine handoff call), so — like the
    ``final_result*`` namespace above — surfacing it cross-agent cannot collide with a user
    tool on the emitting agent. Its args are the peer's ONLY briefing channel: the winning
    call carries ``{name, message}`` and must reach the receiving agent's view.
    """
    return tool_name == HANDOFF_TOOL


def project(history: list[ModelMessage], viewer: str) -> list[ModelMessage]:
    """Project ``history`` to ``viewer``'s point of view.

    Returns a fresh list of fresh message objects. The input is never mutated.
    """
    agent_names = {m.name for m in history if isinstance(m, ModelResponse) and m.name}
    human_names = {p.name for m in history if isinstance(m, ModelRequest) for p in m.parts if isinstance(p, UserPromptPart) and p.name}

    # Viewer-aware: attribute when any turn was authored by someone OTHER than the
    # viewer (another agent — incl. a single one, e.g. a handed-off conversation) or
    # there are multiple named humans to disambiguate. Counting distinct authors (the
    # old `len(agent_names) >= 2`) missed a single other-agent's history.
    multi_participant = bool(agent_names - {viewer}) or len(human_names) >= 2

    if not multi_participant:
        return _project_transparent(history)
    # The no-flag design has no other signal that projection engaged (and that the
    # prompt prefix — hence provider prompt caches — changed for ``viewer``). Log so
    # the engage point is greppable (§5.1).
    logger.debug(
        "projecting multi-participant POV for viewer=%s (agents=%d, named_humans=%d)",
        viewer,
        len(agent_names),
        len(human_names),
    )
    return _project_multi(history, viewer)


def structured_output_preamble(new_messages: list[ModelMessage]) -> str:
    """Text preamble accompanying a **tool-mode** structured final output (§7).

    Returns the concatenated ``TextPart`` text of the run's last ``ModelResponse``
    **only** when that response also carries an output tool call
    (``final_result`` or, for a multi-output union, ``final_result_<Type>`` — see
    ``_is_output_tool``) — i.e. tool mode (the default), where the text is a genuine
    preamble distinct from the structured answer. In ``native``/``prompted`` mode the
    response's ``TextPart`` *is* the JSON answer (no output tool call), so this returns
    ``""`` to avoid duplicating the structured value alongside the ``DataPart``.

    Reads via the vendored ``TextPart`` because ``new_messages`` are vendored
    message objects; ``calfkit.models.payload.TextPart`` is a different type.
    """
    final_resp = next((m for m in reversed(new_messages) if isinstance(m, ModelResponse)), None)
    if final_resp is None:
        return ""
    has_final_result = any(_is_output_tool(p.tool_name) for p in final_resp.tool_calls)
    if not has_final_result:
        return ""  # native/prompted: the TextPart is the answer, not a preamble
    return "".join(p.content for p in final_resp.parts if isinstance(p, TextPart))


def step_preamble(new_messages: list[ModelMessage]) -> str:
    """The agent's preamble text for a NON-terminal (tool-dispatch / handoff) hop's step event
    (intermediate-step-streaming spec §3.2): the concatenated ``TextPart`` text of the hop's
    **final** ``ModelResponse``, excluding thinking/tool-call/file parts. Empty ⇒ the caller authors
    no ``AgentMessage``.

    Final-``ModelResponse``-only is load-bearing: concatenating ALL ``ModelResponse``s would surface
    §2.2-out-of-scope internal-retry preamble. Unlike :func:`structured_output_preamble`, this needs
    **no** ``has_final_result`` guard: a step-emitting hop's final response is tool-call-bearing (a
    dispatch hop carries the calls; a winning handoff IS a tool call under the tool transport), so
    by the vendor's routing (tool calls beat text) its ``TextPart`` is a genuine preamble, never a
    final answer. One deliberate corner (handoff spec §8 rank 2 > rank 3): a prompted-mode agent
    co-emitting its JSON-text answer with a winning handoff surfaces that JSON as the preamble —
    the text lost the turn, so "the text this hop emitted" is exactly what the step should carry.
    """
    final_resp = next((m for m in reversed(new_messages) if isinstance(m, ModelResponse)), None)
    if final_resp is None:
        return ""
    return "".join(p.content for p in final_resp.parts if isinstance(p, TextPart))


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
    """Map ``tool_call_id`` → owning ``ModelResponse.name`` (or the bare string ``"unknown"``)."""
    owners: dict[str, str] = {}
    for m in history:
        if isinstance(m, ModelResponse):
            author = m.name or _UNKNOWN_AUTHOR
            for tc in m.tool_calls:
                owners[tc.tool_call_id] = author
    return owners


def _project_response(m: ModelResponse, viewer: str) -> list[ModelMessage]:
    author = m.name or _UNKNOWN_AUTHOR
    if author == viewer:
        # Self — full fidelity, name stripped, parts kept VERBATIM (§5.2). The
        # verbatim ToolCallPart ids matter: on a deferred-results re-entry pydantic-ai
        # reverse-scans the projected input for the viewer's last ModelResponse and
        # raises UserError if its in-flight tool calls are missing (_agent_graph.py
        # :301-308, §6.2). Do not drop tool-call-only self turns.
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
                new_parts.append(_prefix_user_prompt(p))
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
    # Fresh ModelRequest preserving the original's non-parts fields and (for the
    # all-kept self-view) the verbatim tool-call ids the §6.2 re-entry needs. Parts
    # are never mutated in place, so sharing the part objects is purity-safe (§5).
    return [replace(m, parts=kept_parts)]


def _prefix_user_prompt(p: UserPromptPart) -> UserPromptPart:
    """Attribute a human ``UserPromptPart`` with ``<user>``/``<user:name>``, preserving multimodal content.

    ``UserPromptPart.content`` is ``str | Sequence[UserContent]``. For a plain string
    we concatenate; for multimodal content (text + images/binaries) we prepend the
    prefix as a leading text element so non-text parts (e.g. ``BinaryContent``) are
    preserved verbatim rather than stringified to their repr.
    """
    prefix = f"<user:{p.name}>" if p.name else "<user>"
    if isinstance(p.content, str):
        return UserPromptPart(content=f"{prefix} {p.content}")
    return UserPromptPart(content=[prefix, *p.content])


def _surface(m: ModelResponse) -> str:
    """Compute the public surface of an other-agent ``ModelResponse`` (§5.5).

    surface = concatenated ``TextPart`` text(s) + rendered output-tool args
    (``final_result`` / ``final_result_<Type>``, see ``_is_output_tool``) + rendered
    handoff-tool args (``handoff_to_agent``, see ``_is_handoff_tool`` — the peer's
    briefing channel), non-empty components joined with a single ``"\\n"``.
    """
    components: list[str] = []
    for p in m.parts:
        if isinstance(p, TextPart):
            if p.content:
                components.append(p.content)
        elif isinstance(p, ToolCallPart) and (_is_output_tool(p.tool_name) or _is_handoff_tool(p.tool_name)):
            # Branch on truthiness of args directly (NOT the rendered string, and
            # NOT has_content() which drops {"x": 0}). (§5.5)
            if p.args:
                # ``args_as_dict()`` can raise on off-spec/legacy stored args (non-dict
                # JSON, non-str args). project() runs inside the agent handler with no
                # surrounding catch, so an escape here would hang the caller (the same
                # failure the dispatch path guards at agent.py). Degrade: log and drop
                # the structured component (any text preamble still surfaces).
                try:
                    components.append(_render_structured_args(p))
                except Exception:
                    logger.warning(
                        "could not render surfaced tool args for projection (tool_name=%s tool_call_id=%s); omitting structured component",
                        p.tool_name,
                        p.tool_call_id,
                        exc_info=True,
                    )
        # Ordinary tool calls / thinking / file parts are internal → dropped.
    return "\n".join(components)


def _render_structured_args(p: ToolCallPart) -> str:
    """Render structured args canonically (compact, sorted) — provider-independent (§5.5)."""
    return json.dumps(p.args_as_dict(), separators=(",", ":"), sort_keys=True)
