"""The echo marker rail's carriage type (echo-rail spec D1).

A typed, ``kind``-discriminated correlation *context* stamped on ``CallFrame.marker`` at dispatch
and echoed verbatim on the reply (``_ReplyBase.marker``) wherever ``in_reply_to``/``tag`` are echoed
today — ``CallFrame.tag`` grown up. A leaf module (only ``pydantic``/``typing`` deps) so it can sit
below every model that carries it (``session_context``, ``reply``, ``actions``, ``seam_context``,
``_tool_error``) with no import cycle.

GUARDRAIL LAWS (the anti-junk-drawer rules — do not violate when adding members):

1. **Placement:** universal-to-all-calls facts are frame *fields* (``tag``, ``caller_node_id``);
   per-call-*type* facts are marker *kinds*; content is payload/state — never the marker. A marker
   carries the **complete call identity** and nothing beyond it: state, results, histories never ride it.
2. **Closed and framework-owned:** the union grows by adding members (below), never by open
   registration — never a public plugin data slot.
3. **Callee-opaque, echoed verbatim:** the ``tag`` contract, stated as law — never interpreted or
   rewritten by the callee.
4. **One carriage:** any future "durable from call to result" fact goes through this object — one-off
   identity threads are prohibited.

Leak posture: marker contents — args included — are assumed leakable; do not put secrets in tool args
you would not put on the wire (they already ride the ``ToolCallRef`` payload today).
"""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class ToolCallMarker(BaseModel):
    """The one launch member: a registry-dispatched tool call (including ``message_agent`` —
    ``tool_name`` discriminates further). Carries the complete call identity: name, id, and the
    **parsed** args (spec D1 shape (i)).

    ``frozen`` gives immutability, **not** hashability — the ``args`` dict makes ``hash()`` raise, so
    the marker can never be a set/dict key (inert: ``CallFrame`` is never hashed/keyed).
    """

    model_config = ConfigDict(frozen=True)
    kind: Literal["tool_call"] = "tool_call"
    tool_name: str
    tool_call_id: str
    args: dict[str, Any] | None = None
    """PARSED args (the ``ToolCallRef.args`` precedent) — typed ``dict[str, Any]`` because values are
    JSON-native by provenance and no deep re-validation is wanted per hop. ``| None`` is decode-tolerance
    (clamp-don't-reject, the reply-model posture) — never set ``None`` by a calfkit producer."""


CallMarker = ToolCallMarker
"""The framework-owned closed union. One member at launch; future members extend this to
``Annotated[Union[ToolCallMarker, ...], Field(discriminator="kind")]`` — added by the framework only
(never by open registration). Adding a member is a coordinated deploy (D10): an older consumer cannot
validate an unknown ``kind``; an absent field decodes to ``None`` (old→new safe)."""
