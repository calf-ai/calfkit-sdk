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
from typing import TYPE_CHECKING, Any, Protocol

from calfkit.models.fanout import EnvelopeSnapshot, FanoutBaseState, FanoutOpen, FanoutOutcome, FanoutState

if TYPE_CHECKING:
    from ktables import KafkaTable, KafkaTableWriter

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
        state = await store.read_state(fanout_id)
        if state is None:
            return CloseAbandon()  # abandon gate first — short-circuit the already-closed path
        base = await store.read_basestate(fanout_id)
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

    A failed tombstone — a dead store (:class:`FanoutStoreUnavailableError`) or a writer error (a raw
    broker ``KafkaError`` from the real :class:`KtablesFanoutBatchStore`) — is swallowed and logged
    rather than raised: the corpse is left for the aged-reclaim issue (#220), so an abort never masks
    itself behind a second failure (which, escaping the handler, would re-open the silent-drop hole
    the abort exists to close).
    """
    try:
        await store.tombstone(fanout_id)
    except Exception:
        logger.exception(
            "fan-out batch %s could not be tombstoned (store error); the record strands until reclaimed",
            fanout_id,
        )


# ── the real ktables-backed store (the kafka lane) ───────────────────────────────


class KtablesFanoutBatchStore:
    """:class:`FanoutBatchStore` over two node-scoped compacted ktables — ``state`` (mutable,
    re-written per fold) and ``basestate`` (write-once) — each a reader (:class:`KafkaTable`)
    + writer (:class:`KafkaTableWriter`) pair.

    Freshness (spec §5): the mutable ``state`` barriers before every read (read-your-own-writes —
    an owner must see its own prior folds); the write-once ``basestate`` barriers only on a miss
    (present ⇒ correct, only absence is potentially stale). ``barrier()`` returns ``bool``; a
    ``False`` with reader ``status == "failed"`` is terminal → :class:`FanoutStoreUnavailableError`
    (the fold maps it to abort), while transient degradation (loading/timeout/stop-race) blocks and
    retries (spec §5.1/§4.2 as reconciled to the shipped ``bool`` API). Writers are idempotent
    (``acks=all``); ``delete`` is a null tombstone. The tables self-provision compacted via ktables'
    ``ensure_topic`` (spec §9) — no calfkit provisioner involvement.
    """

    _BARRIER_TIMEOUT_S = 30.0

    def __init__(self, *, bootstrap_servers: str, node_id: str, catchup_timeout: float | None = None) -> None:
        import ktables  # lazy: keep ktables off the offline import path (only a real store touches it)

        state_topic = f"calf.fanout.{node_id}.state"
        basestate_topic = f"calf.fanout.{node_id}.basestate"
        reader_kwargs: dict[str, Any] = {"ensure_topic": True}
        if catchup_timeout is not None:
            reader_kwargs["catchup_timeout"] = catchup_timeout
        self._state_reader: KafkaTable[FanoutState] = ktables.KafkaTable.json(
            bootstrap_servers=bootstrap_servers, topic=state_topic, model=FanoutState, **reader_kwargs
        )
        self._state_writer: KafkaTableWriter[FanoutState] = ktables.KafkaTableWriter.json(
            bootstrap_servers=bootstrap_servers, topic=state_topic, model=FanoutState, ensure_topic=True, enable_idempotence=True
        )
        self._base_reader: KafkaTable[FanoutBaseState] = ktables.KafkaTable.json(
            bootstrap_servers=bootstrap_servers, topic=basestate_topic, model=FanoutBaseState, **reader_kwargs
        )
        self._base_writer: KafkaTableWriter[FanoutBaseState] = ktables.KafkaTableWriter.json(
            bootstrap_servers=bootstrap_servers, topic=basestate_topic, model=FanoutBaseState, ensure_topic=True, enable_idempotence=True
        )

    async def start(self) -> None:
        """Open all four tables. Each reader's ``start()`` replays its compacted topic to the
        start-time end offsets (the catch-up gate) so the store serves a current view; writers
        connect their idempotent producers. Self-provisions the compacted topics if absent."""
        await self._state_reader.start()
        await self._base_reader.start()
        await self._state_writer.start()
        await self._base_writer.start()

    async def stop(self) -> None:
        for component in (self._state_writer, self._base_writer, self._state_reader, self._base_reader):
            await component.stop()

    async def _await_fresh(self, reader: "KafkaTable[Any]") -> None:
        """Block until ``reader`` is confirmed fresh (barrier returns ``True``). A ``False`` with
        ``status == "failed"`` is terminal (raise); any other ``False`` (timeout / stop-race /
        snapshot-error / loading / degraded) is transient and retried — a persistently degraded
        table is an operational failure (spec §7), not a floor."""
        while not await reader.barrier(timeout=self._BARRIER_TIMEOUT_S):
            if reader.status == "failed":
                raise FanoutStoreUnavailableError(f"fan-out table {reader.topic!r} reader died ({reader.failure!r})") from reader.failure
            logger.warning("fan-out table %r barrier not yet fresh (status=%s); retrying", reader.topic, reader.status)

    async def open(self, fanout_id: str, reg: FanoutOpen, snapshot: EnvelopeSnapshot) -> None:
        # basestate THEN state, each awaiting its ack — so *registration present ⟹ basestate present*.
        await self._base_writer.set(fanout_id, FanoutBaseState(fanout_id=fanout_id, snapshot=snapshot))
        await self._state_writer.set(fanout_id, FanoutState(open=reg, outcomes={}))

    async def read_state(self, fanout_id: str) -> FanoutState | None:
        await self._await_fresh(self._state_reader)  # mutable ⇒ barrier-before-read (RYOW)
        return self._state_reader.get(fanout_id)

    async def fold(self, fanout_id: str, outcome: FanoutOutcome) -> FanoutState:
        await self._await_fresh(self._state_reader)
        current = self._state_reader.get(fanout_id)
        if current is None:
            # Single-writer + the caller's preceding read_state make this unreachable; guard
            # loudly rather than silently re-creating a tombstoned batch.
            raise ValueError(f"fold on an unregistered fan-out batch {fanout_id!r}")
        updated = current.model_copy(update={"outcomes": {**current.outcomes, outcome.slot: outcome}})
        await self._state_writer.set(fanout_id, updated)
        return updated

    async def read_basestate(self, fanout_id: str) -> FanoutBaseState | None:
        value = self._base_reader.get(fanout_id)
        if value is None:  # write-once ⇒ barrier only on a miss (absence may be stale, presence is correct)
            await self._await_fresh(self._base_reader)
            value = self._base_reader.get(fanout_id)
        return value

    async def tombstone(self, fanout_id: str) -> None:
        await self._state_writer.delete(fanout_id)
        await self._base_writer.delete(fanout_id)
