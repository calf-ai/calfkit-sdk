"""The :class:`Handoff` capability handle + the reserved handoff tool's pure kernel.

The handle declares WHO an agent may hand off to (ADR-0019's surface, unchanged); the
transport is the reserved ``handoff_to_agent`` external tool with calfkit-owned turn
arbitration (handoff-tool-transport-spec, ADR-0035): :func:`handoff_tool_def` renders the
per-turn tool def, and :func:`arbitrate_handoff` decides a whole response's disposition.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

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
    ``Agent(peers=[Handoff(...)])``; gates the reserved ``handoff_to_agent`` tool — its injection,
    name reservation, and arbitration (spec §2/§3.0). Two modes, mutually exclusive on one handle:

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

# §2 pinned preamble (CONFIRMED 2026-07-09) — the top of the tool description; the live
# directory renders beneath it.
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
      INVARIANT (consumers rely on it): ``winner is None`` ⇒ EVERY ``HANDOFF_TOOL`` call in
      the arbitrated response is in ``rejected`` (membership by object identity, not
      equality).
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
    invalid handoff calls are rejected with precise reasons; when NO handoff is valid,
    every handoff call lands in ``rejected`` (the totality invariant the dispatch fork's
    reason lookup relies on). Pure — the single turn-disposition seam (and the post-#230
    home of the §8 precedence policy)."""
    winner: ToolCallPart | None = None
    rejected: list[tuple[ToolCallPart, str]] = []
    for call in calls:
        if call.tool_name != HANDOFF_TOOL:
            continue
        reason = _validate_handoff_args(call, live_names, self_name)
        if reason is None:
            winner = call
            break  # first valid wins — later calls are classified by the post-pass below
        rejected.append((call, reason))
    if winner is None:
        return HandoffDisposition(winner=None, rejected=rejected)
    # Identity-based exclusion (not equality): two byte-identical calls are still two calls.
    rejected_ids = {id(call) for call, _ in rejected}
    stubbed = [call for call in calls if call is not winner and id(call) not in rejected_ids]
    return HandoffDisposition(winner=winner, rejected=rejected, stubbed=stubbed)


def stub_text(call: ToolCallPart) -> str:
    """The §4 closing-request stub for a non-winner call on a winning turn — the one
    disposition rule that picks between the pinned texts, kept beside them (a stubbed
    sibling handoff reads as a handoff-not-executed; everything else as tool-not-executed)."""
    return _STUB_HANDOFF_NOT_EXECUTED if call.tool_name == HANDOFF_TOOL else _STUB_TOOL_NOT_EXECUTED
