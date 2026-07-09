"""Wire vocabulary + public surface for intermediate step streaming (spec §3.1/§3.2, impl-plan §6).

Two **parallel, frozen** families:

- **WIRE / DRAFT — ``*Step`` (frozen, NO identity).** Authored by the node side and serialized verbatim
  inside :class:`StepMessage`; hop identity (``correlation_id``/``depth``/``frame_id``/``emitter``) sits
  **once** on the message, never per event — so per-event identity is structurally unrepresentable on the
  wire.
- **SURFACE — ``*Event`` (frozen, identity REQUIRED).** The public ``RunEvent`` members the caller
  observes via ``stream()``/``events()``; identity is always present, stamped once caller-side by
  ``client.hub._to_surface``.

The two families are **separate** (not subclasses), so a surface ``*Event`` is not assignable where a wire
``*Step`` is expected — a surfaced event can never ride the wire (identity stays once-on-the-message). The
node side may depend only on ``models/``; ``client/events.py`` imports the surface types to widen
``RunEvent`` and the public ``calfkit`` surface re-exports them — the same "public wire type defined in
``models/``, re-exported" shape as ``ContentPart``. (ADR-0026.)
"""

from typing import Annotated, Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Discriminator

from calfkit.models.payload import ContentPart

# --------------------------------------------------------------------------- #
# WIRE / DRAFT family — `*Step` (frozen, no identity)                          #
# --------------------------------------------------------------------------- #


class _StepBase(BaseModel):
    """Base for every wire/draft step event: frozen, and carries **no** hop identity (identity rides once
    on the enclosing :class:`StepMessage`). Authored by the node side; serialized verbatim on the wire."""

    model_config = ConfigDict(frozen=True)


class AgentMessageStep(_StepBase):
    """The agent's preamble text for the hop (spec §3.2)."""

    kind: Literal["agent_message"] = "agent_message"
    parts: list[ContentPart]


class ToolCallStep(_StepBase):
    """A tool call the model requested this hop, sourced from the model emission (spec §3.2).

    ``kind`` is ``"tool_call"`` — deliberately distinct from ``ContentPart``'s ``ToolCallPart``
    (``"tool"``). ``args`` carries the *raw* model emission (``str`` | ``dict`` | ``None``): a call
    whose args failed to parse must still surface, with no parsed dict to carry.
    """

    kind: Literal["tool_call"] = "tool_call"
    tool_call_id: str
    name: str
    args: str | dict[str, Any] | None = None


class ToolResultStep(_StepBase):
    """The result of a tool call — success **or** error (spec §3.2). One type for both: a tool-node
    return, a consulted ``message_agent`` peer's reply, and an agent's pre-dispatch rejection are all "a
    tool call produced a result"; ``is_error`` (derived once at the producer) distinguishes them.
    """

    kind: Literal["tool_result"] = "tool_result"
    tool_call_id: str
    name: str
    parts: list[ContentPart]
    is_error: bool = False


class HandoffStep(_StepBase):
    """A handoff to a peer agent (spec §3.2) — emitted only when a transfer actually happens
    (ADR-0035: a stale/invalid target is a standard rejection and projects as an ``is_error``
    ``ToolResultStep`` pair instead)."""

    kind: Literal["handoff"] = "handoff"
    target: str
    reason: str


class AgentThinkingStep(_StepBase):
    """The agent's thinking text (spec §5). **Defined but never emitted in v1** — the ``calf.thinking``
    marker mapping is documented, not wired. It stays in the wire union so the decoder resolves every
    ``kind`` (e.g. a foreign producer's), but ``_on_step`` never surfaces it."""

    kind: Literal["agent_thinking"] = "agent_thinking"
    parts: list[ContentPart]


StepEvent = Annotated[
    AgentMessageStep | ToolCallStep | ToolResultStep | HandoffStep | AgentThinkingStep,
    Discriminator("kind"),
]
"""The closed, ``kind``-discriminated WIRE step-event union (what a :class:`StepMessage` carries)."""


class StepMessage(BaseModel):
    """The wire body for ``x-calf-wire == "step"`` (spec §3.1). Hop identity sits **once** here; the events
    ride bare (``*Step``, no identity) and are mapped onto frozen surface ``*Event``s caller-side by
    ``client.hub._to_surface``. Frozen, with **no validator** — wire events carry no identity, so there is
    nothing to back-fill."""

    model_config = ConfigDict(frozen=True)
    WIRE: ClassVar[str] = "step"

    correlation_id: str
    emitter: str
    depth: int
    frame_id: str
    events: list[StepEvent]


# --------------------------------------------------------------------------- #
# SURFACE family — `*Event` (frozen, identity required) — the RunEvent members #
# --------------------------------------------------------------------------- #


class _RunStepEventBase(BaseModel):
    """Base for every surfaced step event: frozen, with hop identity **always** present (stamped once in
    ``client.hub._to_surface`` from the enclosing :class:`StepMessage`). These are the public ``RunEvent``
    members the caller observes via ``stream()``/``events()`` — honest, non-null identity."""

    model_config = ConfigDict(frozen=True)

    correlation_id: str
    depth: int
    frame_id: str
    emitter: str


class AgentMessageEvent(_RunStepEventBase):
    """The agent's preamble text for the hop (spec §3.2) — the surfaced form of :class:`AgentMessageStep`."""

    kind: Literal["agent_message"] = "agent_message"
    parts: list[ContentPart]


class ToolCallEvent(_RunStepEventBase):
    """A tool call the model requested this hop (spec §3.2) — the surfaced form of :class:`ToolCallStep`."""

    kind: Literal["tool_call"] = "tool_call"
    tool_call_id: str
    name: str
    args: str | dict[str, Any] | None = None


class ToolResultEvent(_RunStepEventBase):
    """The result of a tool call, success or error (spec §3.2) — the surfaced form of
    :class:`ToolResultStep`. There is no separate ``ToolFailed`` type; ``is_error`` distinguishes them."""

    kind: Literal["tool_result"] = "tool_result"
    tool_call_id: str
    name: str
    parts: list[ContentPart]
    is_error: bool = False


class HandoffEvent(_RunStepEventBase):
    """A handoff to a peer agent (spec §3.2) — the surfaced form of :class:`HandoffStep`."""

    kind: Literal["handoff"] = "handoff"
    target: str
    reason: str


class AgentThinkingEvent(_RunStepEventBase):
    """The agent's thinking text (spec §5). **Defined but never emitted/surfaced in v1** — not a
    ``RunStepEvent``/``RunEvent`` member and not re-exported. The surfaced form of :class:`AgentThinkingStep`."""

    kind: Literal["agent_thinking"] = "agent_thinking"
    parts: list[ContentPart]


RunStepEvent = AgentMessageEvent | ToolCallEvent | ToolResultEvent | HandoffEvent
"""The surface step-event union — the step members of ``RunEvent`` (the four EMITTED kinds;
``AgentThinkingEvent`` is defined-not-emitted, §5, so it is excluded). The single source of truth for the
surfaced step events: ``client.events.RunEvent`` composes it with the terminals, and ``client.hub`` maps
the wire :data:`StepEvent` into it. A **plain** union (never a decode target, unlike the wire
:data:`StepEvent`), mirroring ``RunEvent`` itself."""
