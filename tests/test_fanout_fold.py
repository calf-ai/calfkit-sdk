"""PR-4 step 4: the pure fold/close/abort state machine over a FanoutBatchStore.

These are the durable analog of the agent's in-process _parallel_state_aggregation —
store- and caller-agnostic pure fns (no broker, no envelope), exercised exhaustively
over the in-memory fake so wiring them into the staged handler (step 6) is no re-home:

- fold_sibling  : one marked sibling reply → park / complete / stray / abort
- close_batch   : the re-entry → resume (materialized) / abandon / spurious / abort
- abort_batch   : best-effort tombstone of both records (the §4.4 abort cleanup)
"""

import pytest

from calfkit._vendor.pydantic_ai.messages import ToolReturn
from calfkit.models.fanout import EnvelopeSnapshot, FanoutOpen, FanoutOutcome, SlotRef
from calfkit.models.session_context import CallFrame, Stack, WorkflowState
from calfkit.models.state import State
from calfkit.nodes._fanout_store import (
    CloseAbandon,
    CloseAbort,
    CloseResume,
    CloseSpurious,
    FoldAbort,
    FoldComplete,
    FoldParked,
    FoldStray,
    abort_batch,
    close_batch,
    fold_sibling,
)
from tests._fanout_fakes import FakeFanoutBatchStore


def _open(fanout_id: str = "X", slots: tuple[tuple[str, str | None], ...] = (("f1", "tc1"), ("f2", "tc2"))) -> FanoutOpen:
    return FanoutOpen(fanout_id=fanout_id, node_id="agent", expected=[SlotRef(frame_id=f, tag=t) for f, t in slots])


def _snapshot() -> EnvelopeSnapshot:
    stack = WorkflowState(call_stack=Stack([CallFrame(target_topic="t", callback_topic="cb")]))
    return EnvelopeSnapshot(state=State(), stack=stack, deps={})


def _outcome(slot: str = "f1", tag: str | None = "tc1", value: str = "ok") -> FanoutOutcome:
    return FanoutOutcome(slot=slot, tag=tag, result=ToolReturn(return_value=value))


@pytest.fixture
def store() -> FakeFanoutBatchStore:
    return FakeFanoutBatchStore()


# ── fold_sibling ─────────────────────────────────────────────────────────────


async def test_fold_first_sibling_parks(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    result = await fold_sibling(store, "X", _outcome("f1", "tc1"))
    assert isinstance(result, FoldParked)
    state = await store.read_state("X")
    assert state is not None
    assert set(state.outcomes) == {"f1"}


async def test_fold_last_sibling_completes(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await fold_sibling(store, "X", _outcome("f1", "tc1"))
    result = await fold_sibling(store, "X", _outcome("f2", "tc2"))
    assert isinstance(result, FoldComplete)


async def test_fold_foreign_slot_is_stray_and_leaves_state_untouched(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    result = await fold_sibling(store, "X", _outcome("f99", "tcX"))
    assert isinstance(result, FoldStray)
    assert result.reason == "foreign"
    state = await store.read_state("X")
    assert state is not None
    assert state.outcomes == {}


async def test_fold_after_tombstone_is_post_closure_stray(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await store.tombstone("X")
    result = await fold_sibling(store, "X", _outcome("f1", "tc1"))
    assert isinstance(result, FoldStray)
    assert result.reason == "post_closure"


async def test_fold_duplicate_slot_is_idempotent_stray(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await fold_sibling(store, "X", _outcome("f1", "tc1"))
    result = await fold_sibling(store, "X", _outcome("f1", "tc1"))
    assert isinstance(result, FoldStray)
    assert result.reason == "duplicate"
    state = await store.read_state("X")
    assert state is not None
    assert set(state.outcomes) == {"f1"}


async def test_fold_duplicate_after_complete_does_not_recomplete(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await fold_sibling(store, "X", _outcome("f1", "tc1"))
    await fold_sibling(store, "X", _outcome("f2", "tc2"))  # completes
    result = await fold_sibling(store, "X", _outcome("f1", "tc1"))  # dup after complete
    assert isinstance(result, FoldStray)
    assert result.reason == "duplicate"


async def test_fold_unavailable_store_aborts(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    store.make_unavailable()
    result = await fold_sibling(store, "X", _outcome("f1", "tc1"))
    assert isinstance(result, FoldAbort)
    assert result.reason == "store_unavailable"


# ── close_batch ──────────────────────────────────────────────────────────────


async def test_close_complete_batch_resumes_materialized_and_tombstones(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await fold_sibling(store, "X", _outcome("f1", "tc1", value="r1"))
    await fold_sibling(store, "X", _outcome("f2", "tc2", value="r2"))
    result = await close_batch(store, "X")
    assert isinstance(result, CloseResume)
    assert result.snapshot.state.get_tool_result("tc1") is not None
    assert result.snapshot.state.get_tool_result("tc2") is not None
    # tombstone-first: both records gone
    assert await store.read_state("X") is None
    assert await store.read_basestate("X") is None


async def test_close_incomplete_batch_is_spurious_and_keeps_state(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await fold_sibling(store, "X", _outcome("f1", "tc1"))  # only 1 of 2
    result = await close_batch(store, "X")
    assert isinstance(result, CloseSpurious)
    assert await store.read_state("X") is not None  # NOT tombstoned


async def test_close_absent_batch_abandons(store: FakeFanoutBatchStore) -> None:
    result = await close_batch(store, "X")  # never opened
    assert isinstance(result, CloseAbandon)


async def test_close_basestate_missing_aborts(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await fold_sibling(store, "X", _outcome("f1", "tc1"))
    await fold_sibling(store, "X", _outcome("f2", "tc2"))  # completes
    store._basestate.pop("X")  # white-box: simulate the impossible-by-ordering miss
    result = await close_batch(store, "X")
    assert isinstance(result, CloseAbort)
    assert result.reason == "basestate_missing"


async def test_close_unavailable_store_aborts(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await fold_sibling(store, "X", _outcome("f1", "tc1"))
    await fold_sibling(store, "X", _outcome("f2", "tc2"))
    store.make_unavailable()
    result = await close_batch(store, "X")
    assert isinstance(result, CloseAbort)
    assert result.reason == "store_unavailable"


async def test_close_materialization_failure_on_missing_tag_aborts(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(slots=(("f1", "tc1"),)), _snapshot())
    await fold_sibling(store, "X", _outcome("f1", tag=None))  # no tag → can't materialize
    result = await close_batch(store, "X")
    assert isinstance(result, CloseAbort)
    assert result.reason == "materialization_failed"
    assert await store.read_state("X") is None  # the un-closable batch is tombstoned


# ── abort_batch + sequential re-fan-out ──────────────────────────────────────


async def test_abort_batch_tombstones_both(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await abort_batch(store, "X")
    assert await store.read_state("X") is None
    assert await store.read_basestate("X") is None


async def test_abort_batch_on_unavailable_store_is_best_effort(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    store.make_unavailable()
    await abort_batch(store, "X")  # must not raise


async def test_reopen_after_close_starts_fresh_batch(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await fold_sibling(store, "X", _outcome("f1", "tc1"))
    await fold_sibling(store, "X", _outcome("f2", "tc2"))
    await close_batch(store, "X")  # tombstones
    await store.open("X", _open(), _snapshot())  # sequential re-fan-out, same key
    state = await store.read_state("X")
    assert state is not None
    assert state.outcomes == {}  # fresh registration, never the stale tombstone
