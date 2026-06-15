"""Durable fan-out batch records (PR-4).

Fan-out batch state lives in two node-scoped compacted ktables, both keyed by
``fanout_id`` (the fan-out node's own inbound ``frame_id``):

- ``calf.fanout.{node_id}.state`` holds a :class:`FanoutState` — the registration
  plus accumulating per-slot outcomes, re-written once per fold (LWW).
- ``calf.fanout.{node_id}.basestate`` holds a :class:`FanoutBaseState` — the
  write-once closure snapshot, read back at the re-entry to rebuild context.

All values are plain pydantic models so ``KafkaTable.json(model=...)`` /
``KafkaTableWriter.json(model=...)`` decode/encode them as JSON, and each carries a
``version`` for forward-compatible schema evolution (drain-before-deploy is the
cross-version contract; ktables' decoder swallows undecodable records).
"""

from typing import Any

from pydantic import BaseModel, Field

from calfkit.models.session_context import WorkflowState
from calfkit.models.state import CalfToolResult, State


class SlotRef(BaseModel):
    """One expected sibling slot, fixed at OPEN."""

    frame_id: str
    """The callee frame id the framework minted for this sibling Call."""
    tag: str | None
    """The caller's correlation token echoed on the reply (the agent's tool_call_id)."""


class FanoutOpen(BaseModel):
    """Small registration metadata written once at fan-out OPEN.

    The full snapshot lives in :class:`FanoutBaseState`, not here — this stays small
    because it is embedded in :class:`FanoutState` and re-written on every fold.
    """

    version: int = 1
    fanout_id: str
    """= the fan-out node's own inbound ``frame_id``; the batch key."""
    node_id: str
    expected: list[SlotRef]
    """The full slot set, fixed at OPEN. Only a true fan-out (N >= 2) registers; a
    single ``Call`` / batch-of-one is never registered (it is a stateless continuation)."""


class FanoutOutcome(BaseModel):
    """One resolved slot — a sub-value of :attr:`FanoutState.outcomes`.

    ``result`` mirrors ``State.tool_results``' value type so a ``CalfToolResult``
    (including a ``FailedToolCall`` marker) round-trips through JSON as a TYPED
    instance via the same discriminator, not as a bare ``dict``. Return-only for now;
    the fault rail widens this field (a ``fault`` arm) without touching the carriage
    of resolved returns.
    """

    version: int = 1
    slot: str
    """The resolved slot's frame id (== :attr:`SlotRef.frame_id`); the idempotent fold key."""
    tag: str | None
    result: CalfToolResult | Any
    """The captured ``state.tool_results[tag]`` entry for this sibling."""


class FanoutState(BaseModel):
    """``calf.fanout.{node_id}.state`` value — re-written once per fold (LWW).

    Mutable across folds, so reads require a ktables ``barrier()`` first (read-your-own-
    writes); completion is ``outcomes.keys() == {s.frame_id for s in open.expected}``.
    """

    version: int = 1
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

    version: int = 1
    state: State
    """Conversation State as of fan-out (pre tool-results)."""
    stack: WorkflowState
    """The call stack with the node's own fan-out frame on top, no sibling frames pushed."""
    deps: dict[str, Any] = Field(default_factory=dict)


class FanoutBaseState(BaseModel):
    """``calf.fanout.{node_id}.basestate`` value — written ONCE at OPEN, read at close."""

    version: int = 1
    fanout_id: str
    snapshot: EnvelopeSnapshot
