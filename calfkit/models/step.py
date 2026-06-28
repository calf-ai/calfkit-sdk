"""Wire vocabulary for intermediate step streaming (spec §3.1 / §3.2).

A :class:`StepMessage` is its own top-level wire body (``x-calf-wire == "step"``), **not** a
member of ``Envelope.reply`` — it carries only correlation / hop-identity / events, so it
structurally cannot leak an ``Envelope``'s ``state``/``deps`` (spec §2.4). It lives in ``models/``
(not ``client/events.py``) because the node side publishes it and ``nodes/`` may depend only on
``models/``; ``client/events.py`` imports these types to widen ``RunEvent`` and the public
``calfkit`` surface re-exports them — the same "public wire type defined in ``models/``,
re-exported" shape as ``ContentPart``.
"""

from typing import Annotated, Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Discriminator, Field, model_validator

from calfkit.models.payload import ContentPart


class _StepEventBase(BaseModel):
    """Base for every step event. Hop identity (``correlation_id``/``depth``/``frame_id``/
    ``emitter``) is a real, readable public field that does **not** ride the wire
    (``Field(exclude=True)``): it is serialized once on the enclosing :class:`StepMessage` and
    back-filled onto each event caller-side by that message's validator (the "blessed factory",
    spec §3.1).

    **Non-frozen by design (load-bearing).** The validator stamps identity by in-place assignment,
    so these models must stay mutable — deliberately breaking from the codebase's frozen-value
    convention (e.g. ``ToolBinding``/``RunCompleted``/``RunFailed`` are frozen). A frozen model
    would raise ``ValidationError`` *during decode* → the lenient step decoder swallows it → every
    step is silently dropped forever (spec §3.1). Do not "fix" this to ``frozen=True``.
    """

    model_config = ConfigDict(frozen=False)

    correlation_id: str | None = Field(default=None, exclude=True)
    depth: int | None = Field(default=None, exclude=True)
    frame_id: str | None = Field(default=None, exclude=True)
    emitter: str | None = Field(default=None, exclude=True)


class AgentMessage(_StepEventBase):
    """The agent's preamble text for the hop (spec §3.2)."""

    kind: Literal["agent_message"] = "agent_message"
    parts: list[ContentPart]


class ToolCall(_StepEventBase):
    """A tool call the model requested this hop, sourced from the model emission (spec §3.2).

    ``kind`` is ``"tool_call"`` — deliberately distinct from ``ContentPart``'s ``ToolCallPart``
    (``"tool"``). ``args`` carries the *raw* model emission (``str`` | ``dict`` | ``None``): a call
    whose args failed to parse must still surface, with no parsed dict to carry.
    """

    kind: Literal["tool_call"] = "tool_call"
    tool_call_id: str
    name: str
    args: str | dict[str, Any] | None = None


class ToolResult(_StepEventBase):
    """The result of a tool call — success **or** error (spec §3.2). One type for both: a tool-node
    return, a consulted ``message_agent`` peer's reply, and an agent's pre-dispatch rejection are
    all "a tool call produced a result"; ``is_error`` (derived once at the producer) distinguishes
    them. There is no separate ``ToolFailed`` type.
    """

    kind: Literal["tool_result"] = "tool_result"
    tool_call_id: str
    name: str
    parts: list[ContentPart]
    is_error: bool = False


class Handoff(_StepEventBase):
    """A handoff to a peer agent (spec §3.2) — emitted even for an offline target."""

    kind: Literal["handoff"] = "handoff"
    target: str
    reason: str


class AgentThinking(_StepEventBase):
    """The agent's thinking text (spec §5). **Defined but never emitted in v1** — the
    ``calf.thinking`` marker mapping is documented, not wired."""

    kind: Literal["agent_thinking"] = "agent_thinking"
    parts: list[ContentPart]


StepEvent = Annotated[
    AgentMessage | ToolCall | ToolResult | Handoff | AgentThinking,
    Discriminator("kind"),
]
"""The closed, ``kind``-discriminated step-event union (spec §3.2)."""


class StepMessage(BaseModel):
    """The wire body for ``x-calf-wire == "step"`` (spec §3.1). Hop-level identity sits once here;
    the events ride bare on the wire and pick up their identity caller-side via
    :meth:`_stamp_identity`."""

    WIRE: ClassVar[str] = "step"

    correlation_id: str
    emitter: str
    depth: int
    frame_id: str
    events: list[StepEvent]

    @model_validator(mode="after")
    def _stamp_identity(self) -> "StepMessage":
        """The "blessed factory" (spec §3.1): back-fill each event's hop identity from the message,
        so a plain ``model_validate_json`` yields identity-stamped events. Requires non-frozen
        events (see :class:`_StepEventBase`)."""
        for event in self.events:
            event.correlation_id = self.correlation_id
            event.depth = self.depth
            event.frame_id = self.frame_id
            event.emitter = self.emitter
        return self
