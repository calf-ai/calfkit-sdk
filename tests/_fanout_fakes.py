"""In-memory FanoutBatchStore double for the offline test lane (PR-4).

Mirrors ``tests._provisioning_fakes``: a broker-free fake so the pure fold/close logic
(step 4) and the staged handler (step 6) are testable without Kafka. The real
read-your-own-writes / lag behavior is proven against ktables in the kafka lane
(step 7), so this fake is always fresh and never simulates lag — it only simulates
terminal unavailability (:meth:`make_unavailable`), keeping the fold-abort path (spec
§4.4) testable offline.
"""

from calfkit.models.fanout import EnvelopeSnapshot, FanoutBaseState, FanoutOpen, FanoutOutcome, FanoutState
from calfkit.nodes._fanout_store import FanoutStoreUnavailableError


class FakeFanoutBatchStore:
    """An in-memory :class:`~calfkit.nodes._fanout_store.FanoutBatchStore`."""

    def __init__(self) -> None:
        self._state: dict[str, FanoutState] = {}
        self._basestate: dict[str, FanoutBaseState] = {}
        self._unavailable = False

    def make_unavailable(self) -> None:
        """Simulate terminal store death — every subsequent call raises
        :class:`FanoutStoreUnavailableError` (exercises the fold-abort path, spec §4.4)."""
        self._unavailable = True

    def _guard(self) -> None:
        if self._unavailable:
            raise FanoutStoreUnavailableError("fake fan-out store marked unavailable")

    async def open(self, fanout_id: str, reg: FanoutOpen, snapshot: EnvelopeSnapshot) -> None:
        self._guard()
        # basestate THEN state (registration present ⟹ basestate present). Store deep copies so the
        # fake matches the real KtablesFanoutBatchStore's freeze-on-write (it serializes on ``set``):
        # a later mutation of the caller's ``snapshot``/``reg`` must not bleed into the stored record.
        self._basestate[fanout_id] = FanoutBaseState(fanout_id=fanout_id, snapshot=snapshot.model_copy(deep=True))
        self._state[fanout_id] = FanoutState(open=reg.model_copy(deep=True), outcomes={})

    async def read_state(self, fanout_id: str) -> FanoutState | None:
        self._guard()
        return self._state.get(fanout_id)

    async def fold(self, fanout_id: str, outcome: FanoutOutcome) -> FanoutState:
        self._guard()
        current = self._state.get(fanout_id)
        if current is None:
            raise ValueError(f"fold on an unregistered fan-out batch {fanout_id!r}")
        updated = current.model_copy(update={"outcomes": {**current.outcomes, outcome.slot: outcome}})
        self._state[fanout_id] = updated
        return updated

    async def read_basestate(self, fanout_id: str) -> FanoutBaseState | None:
        self._guard()
        return self._basestate.get(fanout_id)

    async def tombstone(self, fanout_id: str) -> None:
        self._guard()
        self._state.pop(fanout_id, None)
        self._basestate.pop(fanout_id, None)
