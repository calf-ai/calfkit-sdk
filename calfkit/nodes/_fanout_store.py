"""The durable fan-out batch store seam + fold/close state machine (PR-4).

Fan-out batch state lives in two node-scoped compacted ktables. To keep the fold/
close logic a pure function over an injected store — unit-testable without a broker,
since ktables can't run under the synchronous ``TestKafkaBroker`` — the two tables
(``state`` + ``basestate``, each a reader/writer pair) sit behind one
:class:`FanoutBatchStore` Protocol. ``KtablesFanoutBatchStore`` implements it over real
ktables (the kafka lane); ``tests._fanout_fakes.FakeFanoutBatchStore`` backs the offline
lane.

:func:`fold_sibling`, :func:`close_batch`, and :func:`abort_batch` are the durable
analog of the agent's in-process ``_parallel_state_aggregation`` — store- and
caller-agnostic (no broker, no envelope), so the staged handler (the §6.8 ``_aggregate``
stage) wires them in with no re-home.
"""

import logging
from dataclasses import dataclass
from typing import Protocol

from calfkit.models.fanout import EnvelopeSnapshot, FanoutBaseState, FanoutOpen, FanoutOutcome, FanoutState

logger = logging.getLogger(__name__)

FANOUT_STORE_KEY = "calfkit.fanout.store"
"""Resource-bag key under which a fan-out agent's :class:`FanoutBatchStore` lives — a
node-owned ``@resource`` (ktables) in production, an injected fake in offline tests
(``agent.resources[FANOUT_STORE_KEY] = fake``)."""


class FanoutStoreUnavailableError(Exception):
    """The durable store is terminally unavailable, so freshness cannot be confirmed.

    Raised by a :class:`FanoutBatchStore` when the underlying reader has died (the real
    impl: ``barrier()`` returns ``False`` with ``status == "failed"``). The fold maps
    this to **abort** (spec §4.4: tombstone both tables + ERROR-log + strand) — it is NOT
    transient degradation (``loading``/``timeout``), which the store absorbs by
    blocking-and-retrying internally rather than raising.
    """


class FanoutBatchStore(Protocol):
    """Two node-scoped compacted ktables (``state`` + ``basestate``) behind one interface.

    Freshness contract: :meth:`read_state` / :meth:`read_basestate` return only
    confirmed-fresh data (the real impl barriers before reading; transient degradation
    blocks-and-retries internally). On terminal store death they raise
    :class:`FanoutStoreUnavailableError`, which the fold maps to abort. Writes await their
    broker acks.
    """

    async def open(self, fanout_id: str, reg: FanoutOpen, snapshot: EnvelopeSnapshot) -> None:
        """Register a batch: write the basestate snapshot THEN the state, awaiting both
        acks before returning — so *registration present ⟹ basestate present* (the spec
        §4.1 causality precondition the close relies on)."""
        ...

    async def read_state(self, fanout_id: str) -> FanoutState | None:
        """The accumulating :class:`FanoutState`, confirmed fresh (read-your-own-writes).
        ``None`` if the batch is absent or tombstoned."""
        ...

    async def fold(self, fanout_id: str, outcome: FanoutOutcome) -> FanoutState:
        """Merge one resolved-slot ``outcome`` into the batch's state and persist it
        (awaiting the ack); **idempotent per slot frame id** (LWW absorbs a re-fold).
        Returns the updated state."""
        ...

    async def read_basestate(self, fanout_id: str) -> FanoutBaseState | None:
        """The write-once open-time snapshot, confirmed fresh. ``None`` if absent or
        tombstoned."""
        ...

    async def tombstone(self, fanout_id: str) -> None:
        """Delete both the state and basestate records (awaiting acks). Idempotent —
        safe on an already-absent batch."""
        ...


# ── fold/close decision vocabulary (return-only; the rail adds a fault arm) ──────


@dataclass(frozen=True)
class FoldParked:
    """The slot was folded; the batch is still incomplete — park (no-reply mirror)."""


@dataclass(frozen=True)
class FoldComplete:
    """The slot was folded and the batch is now complete — publish the closure re-entry."""


@dataclass(frozen=True)
class FoldStray:
    """The reply did not fold a live pending slot — floor (foreign / post_closure) or
    idempotent-ignore (duplicate). ``reason`` drives the handler's log level."""

    reason: str


@dataclass(frozen=True)
class FoldAbort:
    """The store died mid-fold — abort (tombstone attempted; the caller strands)."""

    reason: str


FoldResult = FoldParked | FoldComplete | FoldStray | FoldAbort


@dataclass(frozen=True)
class CloseResume:
    """The batch closed cleanly — resume the body on this materialized snapshot."""

    snapshot: EnvelopeSnapshot


@dataclass(frozen=True)
class CloseAbandon:
    """No open batch under this key (already closed) — abandon the re-entry."""


@dataclass(frozen=True)
class CloseSpurious:
    """An open-but-incomplete batch saw a re-entry — spurious; WARN and ignore."""


@dataclass(frozen=True)
class CloseAbort:
    """The close cannot complete — tombstone (where possible) and strand. ``reason`` is
    ``basestate_missing`` | ``materialization_failed`` | ``store_unavailable``."""

    reason: str


CloseResult = CloseResume | CloseAbandon | CloseSpurious | CloseAbort


async def fold_sibling(store: FanoutBatchStore, fanout_id: str, outcome: FanoutOutcome) -> FoldResult:
    """Fold one marked sibling reply into the durable batch state.

    Reads the confirmed-fresh state, runs the stray check (§4.2) against it, and on a live
    pending slot persists the outcome and reports completion. Idempotent per slot frame id
    (a duplicate never re-folds or re-completes). A terminally-unavailable store aborts
    (best-effort tombstone + strand).
    """
    try:
        state = await store.read_state(fanout_id)
        if state is None:
            return FoldStray("post_closure")  # batch already closed/tombstoned
        expected = {s.frame_id for s in state.open.expected}
        if outcome.slot not in expected:
            return FoldStray("foreign")
        if outcome.slot in state.outcomes:
            return FoldStray("duplicate")
        updated = await store.fold(fanout_id, outcome)
        if set(updated.outcomes) == expected:
            return FoldComplete()
        return FoldParked()
    except FanoutStoreUnavailableError:
        await abort_batch(store, fanout_id)
        return FoldAbort("store_unavailable")


async def close_batch(store: FanoutBatchStore, fanout_id: str) -> CloseResult:
    """Close a fan-out batch on its re-entry: rebuild + materialize, then tombstone.

    Reads the basestate snapshot and state. ``state`` is the abandon gate (absent ⇒
    already closed); an incomplete state is spurious. On a complete batch it materializes
    every outcome into a fresh copy of the snapshot's ``State`` and **tombstones both
    records before returning** (tombstone-first, §4.3) so the resumed body runs on a closed
    batch. A missing basestate, a wire-unsafe materialization (no tag), or a dead store
    aborts deterministically — the close never hangs.
    """
    try:
        base = await store.read_basestate(fanout_id)
        state = await store.read_state(fanout_id)
        if state is None:
            return CloseAbandon()
        if base is None:
            return CloseAbort("basestate_missing")
        expected = {s.frame_id for s in state.open.expected}
        if set(state.outcomes) != expected:
            return CloseSpurious()
        snapshot = base.snapshot.model_copy(deep=True)
        for outcome in state.outcomes.values():
            if outcome.tag is None:
                await abort_batch(store, fanout_id)
                return CloseAbort("materialization_failed")
            snapshot.state.add_tool_result(outcome.tag, outcome.result)
        await store.tombstone(fanout_id)  # tombstone-first, before the body resumes
        return CloseResume(snapshot)
    except FanoutStoreUnavailableError:
        await abort_batch(store, fanout_id)
        return CloseAbort("store_unavailable")


async def abort_batch(store: FanoutBatchStore, fanout_id: str) -> None:
    """Best-effort tombstone of both records (the §4.4 abort cleanup).

    A dead store can't be tombstoned — the corpse is left for the aged-reclaim issue
    (#220) rather than raising, so an abort never masks itself behind a second failure.
    """
    try:
        await store.tombstone(fanout_id)
    except FanoutStoreUnavailableError:
        logger.error(
            "fan-out batch %s could not be tombstoned (store unavailable); the record strands until reclaimed",
            fanout_id,
        )
