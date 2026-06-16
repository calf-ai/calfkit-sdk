"""PR-4 step 3: the FanoutBatchStore contract.

Pins the Protocol contract that BOTH FakeFanoutBatchStore (in-memory, this offline
lane) and the real KtablesFanoutBatchStore (step 7, the kafka lane) must satisfy:
open writes basestate+state, read_state/read_basestate reflect them, fold merges an
outcome idempotently per slot frame id, tombstone deletes both, and a terminally
unavailable store raises FanoutStoreUnavailableError (which the fold maps to abort).
"""

import pytest

from calfkit._vendor.pydantic_ai.messages import ToolReturn
from calfkit.models.fanout import EnvelopeSnapshot, FanoutOpen, FanoutOutcome, SlotRef
from calfkit.models.session_context import CallFrame, Stack, WorkflowState
from calfkit.models.state import State
from calfkit.nodes._fanout_store import FanoutBatchStore, FanoutStoreUnavailableError
from tests._fanout_fakes import FakeFanoutBatchStore


def _open(fanout_id: str = "X") -> FanoutOpen:
    return FanoutOpen(
        fanout_id=fanout_id,
        node_id="agent",
        expected=[SlotRef(frame_id="f1", tag="tc1"), SlotRef(frame_id="f2", tag="tc2")],
    )


def _snapshot() -> EnvelopeSnapshot:
    stack = WorkflowState(call_stack=Stack([CallFrame(target_topic="t", callback_topic="cb")]))
    return EnvelopeSnapshot(state=State(), stack=stack, deps={})


def _outcome(slot: str = "f1", tag: str = "tc1", value: str = "ok") -> FanoutOutcome:
    return FanoutOutcome(slot=slot, tag=tag, result=ToolReturn(return_value=value))


@pytest.fixture
def store() -> FakeFanoutBatchStore:
    return FakeFanoutBatchStore()


async def test_open_writes_state_and_basestate(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    state = await store.read_state("X")
    assert state is not None
    assert state.open.fanout_id == "X"
    assert state.outcomes == {}
    base = await store.read_basestate("X")
    assert base is not None
    assert base.fanout_id == "X"


async def test_read_absent_returns_none(store: FakeFanoutBatchStore) -> None:
    assert await store.read_state("nope") is None
    assert await store.read_basestate("nope") is None


async def test_fold_adds_outcome_and_returns_updated_state(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    updated = await store.fold("X", _outcome("f1", "tc1"))
    assert set(updated.outcomes) == {"f1"}
    refreshed = await store.read_state("X")
    assert refreshed is not None
    assert refreshed.outcomes["f1"].tag == "tc1"


async def test_fold_is_idempotent_per_slot(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await store.fold("X", _outcome("f1", "tc1"))
    updated = await store.fold("X", _outcome("f1", "tc1"))  # same slot folded twice
    assert set(updated.outcomes) == {"f1"}


async def test_fold_accumulates_distinct_slots(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await store.fold("X", _outcome("f1", "tc1"))
    updated = await store.fold("X", _outcome("f2", "tc2"))
    assert set(updated.outcomes) == {"f1", "f2"}


async def test_tombstone_deletes_both_records(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await store.tombstone("X")
    assert await store.read_state("X") is None
    assert await store.read_basestate("X") is None


async def test_tombstone_absent_is_safe(store: FakeFanoutBatchStore) -> None:
    await store.tombstone("nope")  # idempotent — no raise on an absent batch


async def test_unavailable_store_raises_on_read(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    store.make_unavailable()
    with pytest.raises(FanoutStoreUnavailableError):
        await store.read_state("X")


def test_fake_satisfies_the_protocol() -> None:
    # Structural conformance (mypy-checked): the fake IS a FanoutBatchStore.
    typed: FanoutBatchStore = FakeFanoutBatchStore()
    assert typed is not None
