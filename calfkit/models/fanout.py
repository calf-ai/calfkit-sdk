"""Durable fan-out batch records (PR-4).

Fan-out batch state lives in two node-scoped compacted ktables, both keyed by
``fanout_id`` (the fan-out node's own inbound ``frame_id``):

- ``calf.fanout.{node_id}.state`` holds a :class:`FanoutState` — the registration
  plus accumulating per-slot outcomes, re-written once per fold (LWW).
- ``calf.fanout.{node_id}.basestate`` holds a :class:`FanoutBaseState` — the
  write-once closure snapshot, read back at the re-entry to rebuild context.

All values are plain pydantic models so ``KafkaTable.json(model=...)`` /
``KafkaTableWriter.json(model=...)`` decode/encode them as JSON. The cross-version
contract is drain-before-deploy (the batch state never spans a schema change), and
ktables' decoder swallows any record it cannot decode.
"""

from typing import Any

from pydantic import BaseModel, Field

from calfkit.models.error_report import ErrorReport
from calfkit.models.payload import ContentPart
from calfkit.models.session_context import WorkflowState
from calfkit.models.state import State


class SlotRef(BaseModel):
    """One expected sibling slot, fixed at OPEN."""

    frame_id: str
    """The callee frame id the framework minted for this sibling Call."""
    tag: str | None
    """The caller's correlation token echoed on the reply (the agent's tool_call_id)."""
    target_topic: str
    """The sibling callee's topic, captured at OPEN from ``Call.target_topic``. Sourced onto the
    matched outcome's :attr:`FanoutOutcome.target_topic` at fold so the fault group's per-slot
    topology has it without the reply needing to carry it (the reply echoes ``in_reply_to``/``tag``/
    ``marker``, none of which is the callee topic)."""


class FanoutOpen(BaseModel):
    """Small registration metadata written once at fan-out OPEN.

    The full snapshot lives in :class:`FanoutBaseState`, not here — this stays small
    because it is embedded in :class:`FanoutState` and re-written on every fold.
    """

    fanout_id: str
    """= the fan-out node's own inbound ``frame_id``; the batch key."""
    node_id: str
    """Diagnostic/self-describing record field, not read at runtime: the batch is
    namespaced by topic (``calf.fanout.{node_id}.*``) and keyed by ``fanout_id``."""
    expected: list[SlotRef] = Field(..., min_length=1)
    """The full slot set, fixed at OPEN. A batch registers iff the caller's state must survive the call
    independently of the round-trip: a true fan-out (N >= 2), OR a single ``isolate_state`` call (a lone
    ``message_agent`` — a degenerate one-element batch that snapshots/restores the caller's state, L13).
    An *unflagged* single ``Call`` / 1-element ``list`` stays a stateless continuation and never registers
    here. ``min_length=1`` keeps an empty batch unrepresentable while admitting the singleton."""


class FanoutOutcome(BaseModel):
    """One resolved slot — a sub-value of :attr:`FanoutState.outcomes` (fault-rail §6.9 / §7).

    Carries the callee's outcome on the reply-slot carriage: a RESOLVED slot (a plain return, or an
    ``on_callee_error`` substitute) sets :attr:`parts` (the typed ``ContentPart`` vocabulary, decoded
    TYPED via the discriminator); a FAILED slot (an unhandled callee fault, or a slot whose
    materialization failed) sets :attr:`fault` (a typed ``ErrorReport``). ``parts`` XOR ``fault`` by
    construction. The agent materializes a resolved outcome into ``tool_results`` at close
    (``_resolve_slot``); an unhandled fault escalates as the batch's fault group and never materializes.
    """

    slot: str
    """The resolved slot's frame id (== :attr:`SlotRef.frame_id`); the idempotent fold key."""
    tag: str | None
    """The caller's correlation token (the agent's ``tool_call_id``); the materialization key at close."""
    target_topic: str
    """The sibling callee's topic (sourced from the matched :class:`SlotRef` at fold). Carried on every
    outcome; consumed only on the fault arm (the §7.3 fault-group per-slot ``{tag, target_topic, ok|failed}``
    topology). Write-only until the deferred reception/budget PRs read it."""
    handled: bool
    """True ⇒ ``on_callee_error`` returned a substitute (a handled fault; counts as that tool's FAILURE
    for the deferred §7.6 budget). False ⇒ a fault-free return. A substitute and a return both carry
    ``parts``, so this flag (the fault-rail ``CalleeResult.handled``) is the discriminator the tally needs.
    Write-only until the deferred budget PR reads it."""
    parts: list[ContentPart] | None = None
    """A resolved slot's content (a return, or a handled substitute) — XOR :attr:`fault`."""
    fault: ErrorReport | None = None
    """A failed slot's typed fault (unhandled ``on_callee_error`` decline, or a materialization failure)
    — XOR :attr:`parts`. Recorded so the closure builds the batch's fault group from the failed slots."""


class FanoutState(BaseModel):
    """``calf.fanout.{node_id}.state`` value — re-written once per fold (LWW).

    Mutable across folds, so reads require a ktables ``barrier()`` first (read-your-own-
    writes); completion is ``outcomes.keys() == {s.frame_id for s in open.expected}``.
    """

    open: FanoutOpen
    outcomes: dict[str, FanoutOutcome] = Field(default_factory=dict)
    """Resolved slots by frame id. Folding a slot already present is an idempotent no-op
    (LWW on the same slot key absorbs a producer-retry duplicate)."""


class EnvelopeSnapshot(BaseModel):
    """The closure's correctness contract, captured pre-marker-stamp at OPEN.

    ``deps`` is wire data (it crosses the Kafka boundary on every hop), captured here
    because the re-entry envelope clears ``context.state`` and ``context.deps`` — this
    snapshot is the sole restore source at close. ``resources`` are node-local and
    re-stamped from the node's bag, so they are deliberately NOT snapshotted.
    """

    state: State
    """Conversation State as of fan-out (pre tool-results)."""
    stack: WorkflowState
    """The call stack with the node's own fan-out frame on top, no sibling frames pushed."""
    deps: dict[str, Any] = Field(default_factory=dict)


class FanoutBaseState(BaseModel):
    """``calf.fanout.{node_id}.basestate`` value — written ONCE at OPEN, read at close."""

    fanout_id: str
    snapshot: EnvelopeSnapshot
