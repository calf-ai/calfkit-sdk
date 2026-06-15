"""The durable fan-out batch store seam (PR-4).

Fan-out batch state lives in two node-scoped compacted ktables. To keep the fold/
close logic a pure function over an injected store — unit-testable without a broker,
since ktables can't run under the synchronous ``TestKafkaBroker`` — the two tables
(``state`` + ``basestate``, each a reader/writer pair) sit behind one
:class:`FanoutBatchStore` Protocol. ``KtablesFanoutBatchStore`` implements it over
real ktables (the kafka lane); ``tests._fanout_fakes.FakeFanoutBatchStore`` backs the
offline lane.
"""

from typing import Protocol

from calfkit.models.fanout import EnvelopeSnapshot, FanoutBaseState, FanoutOpen, FanoutOutcome, FanoutState


class FanoutStoreUnavailableError(Exception):
    """The durable store is terminally unavailable, so freshness cannot be confirmed.

    Raised by a :class:`FanoutBatchStore` when the underlying reader has died (the
    real impl: ``barrier()`` returns ``False`` with ``status == "failed"``). The fold
    maps this to **abort** (spec §4.4: tombstone both tables + ERROR-log + strand) — it
    is NOT transient degradation (``loading``/``timeout``), which the store absorbs by
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
