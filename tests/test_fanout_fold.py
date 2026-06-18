"""PR-4 + PR-6 4.4/4.6: the pure fold/close/abort state machine over a FanoutBatchStore.

Store- and caller-agnostic pure fns (no broker, no envelope), exercised over the in-memory fake:

- classify_sibling : read the confirmed-fresh state and classify a marked sibling slot BEFORE the
  seams run (decision 10 / fault-rail §6.7) → pending (the matched SlotRef) / stray / abort. The
  caller runs ``on_callee_error`` only on a live pending slot, then records.
- record_outcome   : persist one resolved outcome (parts XOR fault) → park / complete / abort.
- close_batch      : the re-entry → resume (the snapshot + the un-materialized outcomes; the agent
  materializes at ``_resolve_slot``) / abandon / spurious / abort. NO materialization here anymore.
- abort_batch      : best-effort tombstone of both records (the §4.4 abort cleanup).
"""

import logging

import pytest
from aiokafka.errors import KafkaError  # type: ignore[import-untyped]

from calfkit.models.error_report import ErrorReport
from calfkit.models.fanout import EnvelopeSnapshot, FanoutOpen, FanoutOutcome, SlotRef
from calfkit.models.payload import TextPart
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
    SiblingPending,
    abort_batch,
    classify_sibling,
    close_batch,
    record_outcome,
)
from tests._fanout_fakes import FakeFanoutBatchStore


def _open(fanout_id: str = "X", slots: tuple[tuple[str, str | None], ...] = (("f1", "tc1"), ("f2", "tc2"))) -> FanoutOpen:
    return FanoutOpen(fanout_id=fanout_id, node_id="agent", expected=[SlotRef(frame_id=f, tag=t, target_topic=f"tool.{f}") for f, t in slots])


def _snapshot() -> EnvelopeSnapshot:
    stack = WorkflowState(call_stack=Stack([CallFrame(target_topic="t", callback_topic="cb")]))
    return EnvelopeSnapshot(state=State(), stack=stack, deps={})


def _resolved(slot: str = "f1", tag: str | None = "tc1", value: str = "ok") -> FanoutOutcome:
    return FanoutOutcome(slot=slot, tag=tag, target_topic=f"tool.{slot}", handled=False, parts=[TextPart(text=value)])


def _failed(slot: str = "f1", tag: str | None = "tc1", error_type: str = "callee.boom") -> FanoutOutcome:
    return FanoutOutcome(slot=slot, tag=tag, target_topic=f"tool.{slot}", handled=False, fault=ErrorReport(error_type=error_type))


@pytest.fixture
def store() -> FakeFanoutBatchStore:
    return FakeFanoutBatchStore()


# ── classify_sibling (stray-check BEFORE the seams, decision 10) ──────────────────


async def test_classify_live_slot_returns_pending_with_matched_slot_ref(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    result = await classify_sibling(store, "X", "f1")
    assert isinstance(result, SiblingPending)
    assert result.slot_ref.frame_id == "f1" and result.slot_ref.target_topic == "tool.f1"


async def test_classify_foreign_slot_is_stray(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    result = await classify_sibling(store, "X", "f99")
    assert isinstance(result, FoldStray) and result.reason == "foreign"


async def test_classify_after_tombstone_is_post_closure_stray(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await store.tombstone("X")
    result = await classify_sibling(store, "X", "f1")
    assert isinstance(result, FoldStray) and result.reason == "post_closure"


async def test_classify_already_recorded_slot_is_duplicate_stray(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await record_outcome(store, "X", _resolved("f1", "tc1"))
    result = await classify_sibling(store, "X", "f1")
    assert isinstance(result, FoldStray) and result.reason == "duplicate"


async def test_classify_unavailable_store_aborts(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    store.make_unavailable()
    result = await classify_sibling(store, "X", "f1")
    assert isinstance(result, FoldAbort) and result.reason == "store_unavailable"


# ── record_outcome ───────────────────────────────────────────────────────────────


async def test_record_first_outcome_parks(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    result = await record_outcome(store, "X", _resolved("f1", "tc1"))
    assert isinstance(result, FoldParked)
    state = await store.read_state("X")
    assert state is not None and set(state.outcomes) == {"f1"}


async def test_record_last_outcome_completes(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await record_outcome(store, "X", _resolved("f1", "tc1"))
    result = await record_outcome(store, "X", _resolved("f2", "tc2"))
    assert isinstance(result, FoldComplete)


async def test_record_a_failed_outcome_persists_the_fault(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await record_outcome(store, "X", _failed("f1", "tc1"))
    state = await store.read_state("X")
    assert state is not None
    assert state.outcomes["f1"].fault is not None and state.outcomes["f1"].fault.error_type == "callee.boom"


async def test_record_unavailable_store_aborts(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    store.make_unavailable()
    result = await record_outcome(store, "X", _resolved("f1", "tc1"))
    assert isinstance(result, FoldAbort) and result.reason == "store_unavailable"


# ── close_batch (no materialization — returns the snapshot + the outcomes) ────────


async def test_close_complete_batch_returns_outcomes_and_tombstones(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await record_outcome(store, "X", _resolved("f1", "tc1", value="r1"))
    await record_outcome(store, "X", _resolved("f2", "tc2", value="r2"))
    result = await close_batch(store, "X")
    assert isinstance(result, CloseResume)
    # The snapshot is NOT materialized here (materialization moved to _aggregate/_resolve_slot)...
    assert result.snapshot.state.tool_results == {}
    # ...the un-materialized outcomes ride alongside, in fold order.
    assert [o.slot for o in result.outcomes] == ["f1", "f2"]
    # tombstone-first: both records gone
    assert await store.read_state("X") is None
    assert await store.read_basestate("X") is None


async def test_close_carries_both_resolved_and_failed_outcomes(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await record_outcome(store, "X", _resolved("f1", "tc1"))
    await record_outcome(store, "X", _failed("f2", "tc2"))  # completes
    result = await close_batch(store, "X")
    assert isinstance(result, CloseResume)
    by_slot = {o.slot: o for o in result.outcomes}
    assert by_slot["f1"].parts is not None and by_slot["f1"].fault is None
    assert by_slot["f2"].fault is not None and by_slot["f2"].parts is None


async def test_close_incomplete_batch_is_spurious_and_keeps_state(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await record_outcome(store, "X", _resolved("f1", "tc1"))  # only 1 of 2
    result = await close_batch(store, "X")
    assert isinstance(result, CloseSpurious)
    assert await store.read_state("X") is not None  # NOT tombstoned


async def test_close_absent_batch_abandons(store: FakeFanoutBatchStore) -> None:
    result = await close_batch(store, "X")  # never opened
    assert isinstance(result, CloseAbandon)


async def test_close_basestate_missing_aborts(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await record_outcome(store, "X", _resolved("f1", "tc1"))
    await record_outcome(store, "X", _resolved("f2", "tc2"))  # completes
    store._basestate.pop("X")  # white-box: simulate the impossible-by-ordering miss
    result = await close_batch(store, "X")
    assert isinstance(result, CloseAbort) and result.reason == "basestate_missing"


async def test_close_unavailable_store_aborts(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await record_outcome(store, "X", _resolved("f1", "tc1"))
    await record_outcome(store, "X", _resolved("f2", "tc2"))
    store.make_unavailable()
    result = await close_batch(store, "X")
    assert isinstance(result, CloseAbort) and result.reason == "store_unavailable"


# ── abort_batch + sequential re-fan-out ──────────────────────────────────────────


async def test_abort_batch_tombstones_both(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await abort_batch(store, "X")
    assert await store.read_state("X") is None
    assert await store.read_basestate("X") is None


async def test_abort_batch_on_unavailable_store_is_best_effort(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    store.make_unavailable()
    await abort_batch(store, "X")  # must not raise


async def test_abort_batch_swallows_non_store_unavailable_tombstone_failure(caplog: pytest.LogCaptureFixture) -> None:
    # abort_batch is best-effort: its docstring promises it "never masks itself behind a second
    # failure." A tombstone that raises a NON-FanoutStoreUnavailableError (e.g. a raw KafkaError from
    # the real writer's producer) must be swallowed + logged, not propagated — else the abort itself
    # would escape the handler and re-open the silent-drop hole the abort exists to close.
    class _TombstoneRaises(FakeFanoutBatchStore):
        async def tombstone(self, fanout_id: str) -> None:
            raise KafkaError("simulated writer failure during tombstone")

    store = _TombstoneRaises()
    await store.open("X", _open(), _snapshot())
    with caplog.at_level(logging.ERROR):
        await abort_batch(store, "X")  # must not raise
    assert any("could not be tombstoned" in r.getMessage() for r in caplog.records)


async def test_reopen_after_close_starts_fresh_batch(store: FakeFanoutBatchStore) -> None:
    await store.open("X", _open(), _snapshot())
    await record_outcome(store, "X", _resolved("f1", "tc1"))
    await record_outcome(store, "X", _resolved("f2", "tc2"))
    await close_batch(store, "X")  # tombstones
    await store.open("X", _open(), _snapshot())  # sequential re-fan-out, same key
    state = await store.read_state("X")
    assert state is not None and state.outcomes == {}  # fresh registration, never the stale tombstone
