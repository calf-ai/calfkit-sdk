"""The :class:`Handoff` peer-handoff capability handle (ADR-0019)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, Field, create_model, field_validator

from calfkit._handle_names import normalize_handle_names
from calfkit._safe import safe_exc_message
from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.peers.directory import render_peer_directory

if TYPE_CHECKING:
    from collections.abc import Collection

    from calfkit._vendor.pydantic_ai.messages import ToolCallPart


@dataclass(frozen=True)
class Handoff:
    """Identity-only handle declaring which peer agents this agent may **hand off** to (a Handoff —
    transfer of conversation control: the handing agent relinquishes and does **not** regain it; the peer
    continues the full conversation and returns to the original caller). Passed in
    ``Agent(peers=[Handoff(...)])``; gates and feeds the runtime-built ``HandoffRequest`` structured-output
    option. Two modes, mutually exclusive on one handle:

    - **curated** — ``Handoff("refunds", "billing")`` / ``Handoff(names=[...])``: the named peers are the
      only handoff targets (the v1 caller-side trust boundary, §7).
    - **open** — ``Handoff(discover=True)``: any live agent is a handoff target.

    Exactly one of {non-empty names, ``discover=True``} — both, or neither, raise. A byte-for-byte mirror of
    the :class:`Messaging` handle: frozen value semantics (names order-preserving-deduped, so equal handles
    compare and hash equal), a custom varargs ``__init__``, and no ``merge`` — multiple same-kind handles
    stay independent and the rendered directory dedupes by name. Deliberately does **not** implement the
    tool protocols (``resolve_tools``/``tool_bindings``), so a handle mistakenly placed in ``tools=`` falls
    through ``split_tool_declarations`` to its ``else -> TypeError`` rather than being silently absorbed
    (M4). The agent's-own-name reject lives in the ``Agent`` ctor (a handle can't see its enclosing agent's
    name, M2).
    """

    names: tuple[str, ...]
    discover: bool = False

    # ``*positional`` varargs (the common case) plus a keyword-only ``names=`` list; no name collision
    # because the varargs param is ``positional`` while the stored field is ``names`` (mirrors ``Tools``).
    # The curated-XOR-discover fail-loud rail is shared with ``Tools``/``Messaging`` via ``normalize_handle_names``.
    def __init__(self, *positional: str, names: Sequence[str] | None = None, discover: bool = False) -> None:
        object.__setattr__(self, "names", normalize_handle_names("Handoff", "agent", positional=positional, names=names, discover=discover))
        object.__setattr__(self, "discover", discover)


class HandoffRequest(BaseModel):
    """The model produces this as its turn's **output** to transfer control to a peer (§5.3, L4) — a BARE
    structured-output union member, NOT a tool. ``output_type = [final_output_type, HandoffRequest,
    DeferredToolRequests]`` keeps ``allow_text_output=True`` on every provider (a ``ToolOutput`` wrapper
    would force ``tool_choice=required``, which Anthropic rejects under extended thinking); calfkit
    discriminates by ``isinstance(result.output, HandoffRequest)`` for dispatch.

    This is the **stable base**. The per-turn subclass (:func:`_build_handoff_request`) narrows ``name`` to
    a ``Literal`` over the live directory and carries the directory as its ``__doc__``; ``__base__`` keeps
    ``isinstance`` valid across rebuilds.

    - ``name`` — the peer to hand off to (validated against the live ``Literal`` in the per-turn subclass).
    - ``message`` — the handing agent's summary/context for the peer, required non-empty (``min_length=1``),
      so an empty value is auto-retried by pydantic-ai like an out-of-``Literal`` ``name`` (L5/C2).

    Both fields carry a model-facing ``Field(description=...)`` that pydantic-ai forwards inside the schema's
    ``properties`` (the per-turn subclass inherits ``message``'s and re-declares ``name``'s alongside the
    ``Literal``); this is the structured-output analog of the ``message_agent`` tool's per-param descriptions.
    """

    name: str
    message: str = Field(min_length=1, description="Your summary or context for the peer, so it can continue the conversation.")

    @field_validator("message")
    @classmethod
    def _message_not_blank(cls, v: str) -> str:
        # Parity with `message_agent` (which rejects via `not message.strip()`): a whitespace-only message
        # is blank, not content. `Field(min_length=1)` stays as the model-visible JSON-schema hint; this
        # enforces the strip semantics so an auto-retry fires just like an out-of-`Literal` `name`.
        if not v.strip():
            raise ValueError("message must not be blank (whitespace-only)")
        return v


# The per-field "how to fill this" guidance lives in the field descriptions (`name`/`message` below), which
# pydantic-ai forwards inside the schema's `properties` — mirroring how the `message_agent` tool splits its
# top-level description from its per-param descriptions. The preamble keeps only the handoff *semantics* + the
# directory it points at, so the two are documented once each, not duplicated.
_HANDOFF_PREAMBLE = (
    "Use HandoffRequest to hand the whole conversation to a better-suited agent — when the request belongs to "
    "another agent's domain and should be handled by them, not just consulted. You relinquish control and will "
    "NOT regain it: the chosen agent continues with full context of the conversation and answers the original "
    "caller in your place.\n\n"
    "Agents (name — description):\n"
)

# Injected into the per-run instructions (NOT a persisted message part) when a Handoff handle is present but
# no in-scope peer is live, so the dormant capability stays legible while the member is omitted (§5.3).
# Self-heals: the moment a peer comes online the Literal member returns and this note disappears.
_HANDOFF_NO_PEERS_NOTE = "You cannot currently hand off this conversation or task to another agent, as no other agents are online."


@lru_cache(maxsize=128)
def _build_handoff_request(live: tuple[tuple[str, str | None], ...]) -> type[HandoffRequest]:
    """The per-turn :class:`HandoffRequest` subclass for a given live, sorted directory — built once per
    distinct live set (``@lru_cache``; an explicit bound, not the silent default-128 that reads as
    "unbounded"). Callers pass :func:`~calfkit.peers.directory.resolve_live_peers`' sorted-by-name output,
    so identical directories reuse one model **and** its compiled schema. ``description`` is part of the key
    (the ``__doc__`` depends on it). ``__base__=HandoffRequest`` keeps ``isinstance`` discrimination valid;
    ``name`` is a ``Literal`` over the live names (pydantic-ai natively rejects + auto-retries an
    out-of-directory value, no calfkit code); ``__doc__`` is the handoff directory (the structured-output
    analog of a tool docstring). **Requires a non-empty live set** — an empty ``Literal`` is unbuildable
    (raises at pydantic schema-build), so the dispatch path omits the member instead (§5.3)."""
    if not live:
        raise ValueError("_build_handoff_request requires a non-empty live directory — an empty Literal is unbuildable; omit the member instead")
    names = tuple(name for name, _ in live)
    return create_model(
        "HandoffRequest",
        __base__=HandoffRequest,
        __doc__=_HANDOFF_PREAMBLE + render_peer_directory(live),
        # The Literal carries the allowed values; the description is re-declared here (the base's `name` has
        # none, and this override would discard it anyway) so it rides into the schema's `properties`.
        name=(Literal[names], Field(description="The peer to hand off to (a name from the list of available agents).")),
    )


# --------------------------------------------------------------------------- #
# Tool transport (spec: handoff-tool-transport-spec.md, ADR-0035)              #
# --------------------------------------------------------------------------- #

HANDOFF_TOOL = "handoff_to_agent"
"""The reserved handoff tool name (spec §2). Reserved against user tools at construction
whenever ``Handoff`` handles are present; the dispatch fork and arbitration are gated on
the same condition (spec §3.0), so a handle-less agent's user tool of this name is never
intercepted."""

# §4 transcript stubs — the closing ModelRequest's ToolReturnPart contents on a winning
# turn. Byte-pinned by tests/test_handoff_tool_def.py (the single source of truth for
# every model-visible string; edits here must be deliberate).
_STUB_TRANSFERRED = "Transferred to {name}."
_STUB_TOOL_NOT_EXECUTED = "Tool not executed - the conversation was handed off to another agent."
_STUB_HANDOFF_NOT_EXECUTED = "Handoff not executed - the conversation was already handed off."

# §9 model-visible rejection reasons (voice-matched to `_message_agent_target_error`).
_REJECT_MALFORMED = "Malformed handoff_to_agent arguments: {etype}: {msg}"
_REJECT_FIELDS = "handoff_to_agent requires non-empty string 'name' and 'message' arguments."
_REJECT_SELF = "You cannot hand off to yourself ({name!r})."
_REJECT_UNREACHABLE = (
    "Agent {name!r} is not currently reachable — it is offline or not in your handoff scope. Choose from the agents listed in the tool description."
)
_REJECT_NONE_ONLINE = "You cannot currently hand off this conversation or task to another agent, as no other agents are online."

# §2 pinned preamble — the old `_HANDOFF_PREAMBLE` with its `HandoffRequest` lead-in
# reworded (that name is no longer model-visible under the tool transport).
_HANDOFF_TOOL_PREAMBLE = (
    "Hand the whole conversation to a better-suited agent — when the request belongs to "
    "another agent's domain and should be handled by them, not just consulted. You "
    "relinquish control and will NOT regain it: the chosen agent continues with full "
    "context of the conversation and answers the original caller in your place.\n\n"
    "Agents (name — description):\n"
)


def handoff_tool_def(live: Sequence[tuple[str, str | None]]) -> ToolDefinition:
    """The runtime-rendered ``handoff_to_agent`` external tool def for a resolved live
    directory (spec §2). Always renderable — an empty directory renders the "none
    reachable" sentinel body (messaging parity: the model keeps the capability while the
    mesh is empty). ``name`` is a plain string, deliberately NOT an enum:
    ``ExternalToolset`` attaches ``any_schema`` so an enum enforces nothing in-run, and it
    would churn the parameters schema (and provider prompt caches) per live-set.
    Uncached — a per-turn dict-literal def is what ``message_agent`` already does."""
    return ToolDefinition(
        name=HANDOFF_TOOL,
        description=_HANDOFF_TOOL_PREAMBLE + render_peer_directory(live),
        parameters_json_schema={
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "The peer to hand off to (a name from the list of available agents)."},
                "message": {"type": "string", "description": "Your summary or context for the peer, so it can continue the conversation."},
            },
            "required": ["name", "message"],
            "additionalProperties": False,
        },
    )


def _validate_handoff_args(tool_call: ToolCallPart, live_names: Collection[str], self_name: str) -> str | None:
    """Validate a ``handoff_to_agent`` call, returning the §9 rejection reason or ``None``
    when valid — a pure predicate mirroring ``_validate_message_agent``. Extra keys
    alongside valid ``name``/``message`` are ignored (spec §13.3: parity with both the old
    transport's ``extra="ignore"`` and ``message_agent``'s ``args.get`` pattern)."""
    try:
        args = tool_call.args_as_dict()
    except Exception as e:  # noqa: BLE001 — any parse failure is a model-visible rejection, never a raise
        return _REJECT_MALFORMED.format(etype=type(e).__name__, msg=safe_exc_message(e))
    name, message = args.get("name"), args.get("message")
    if not isinstance(name, str) or not name.strip() or not isinstance(message, str) or not message.strip():
        return _REJECT_FIELDS
    if name == self_name:
        return _REJECT_SELF.format(name=name)
    if not live_names:
        return _REJECT_NONE_ONLINE
    if name not in live_names:
        return _REJECT_UNREACHABLE.format(name=name)
    return None


@dataclass(frozen=True)
class HandoffDisposition:
    """The whole-response handoff decision (spec §3) — computed once by
    :func:`arbitrate_handoff`, executed by the agent's ``run()``.

    - ``winner`` — the first VALID handoff call (emission order), or ``None``.
    - ``rejected`` — invalid handoff calls with their §9 reasons. On a no-winner turn the
      executor lands each as a ``RetryPromptPart``; on a winning turn they are closed in
      the closing ``ModelRequest`` instead (§3.1 — A has no next model call to retry).
    - ``stubbed`` — every OTHER call in the response, iff a winner exists (early
      semantics: nothing is dispatched); empty when there is no winner (mixed
      disposition — remaining calls proceed through the normal loop).
    """

    winner: ToolCallPart | None
    rejected: list[tuple[ToolCallPart, str]] = field(default_factory=list)
    stubbed: list[ToolCallPart] = field(default_factory=list)


def arbitrate_handoff(calls: Sequence[ToolCallPart], live_names: Collection[str], self_name: str) -> HandoffDisposition:
    """Arbitrate a model response's tool calls under the handoff-wins rule (spec §3): the
    first VALID ``handoff_to_agent`` call wins the turn; every sibling call is stubbed;
    invalid handoff calls are rejected with precise reasons. Pure — the single
    turn-disposition seam (and the post-#230 home of the §8 precedence policy)."""
    winner: ToolCallPart | None = None
    rejected: list[tuple[ToolCallPart, str]] = []
    for call in calls:
        if call.tool_name != HANDOFF_TOOL or winner is not None:
            continue
        reason = _validate_handoff_args(call, live_names, self_name)
        if reason is None:
            winner = call
        else:
            rejected.append((call, reason))
    if winner is None:
        return HandoffDisposition(winner=None, rejected=rejected)
    # Identity-based exclusion (not equality): two byte-identical calls are still two calls.
    rejected_ids = {id(call) for call, _ in rejected}
    stubbed = [call for call in calls if call is not winner and id(call) not in rejected_ids]
    return HandoffDisposition(winner=winner, rejected=rejected, stubbed=stubbed)
