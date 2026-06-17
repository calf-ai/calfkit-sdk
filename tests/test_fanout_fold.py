"""PR-4 step 4: the pure fold/close/abort state machine over a FanoutBatchStore.

These are the durable analog of the agent's in-process _parallel_state_aggregation —
store- and caller-agnostic pure fns (no broker, no envelope), exercised exhaustively
over the in-memory fake so wiring them into the staged handler (step 6) is no re-home:

- fold_sibling  : one marked sibling reply → park / complete / stray / abort
- close_batch   : the re-entry → resume (materialized) / abandon / spurious / abort
- abort_batch   : best-effort tombstone of both records (the §4.4 abort cleanup)
"""

import logging

import pytest
from aiokafka.errors import KafkaError  # type: ignore[import-untyped]

from calfkit._vendor.pydantic_ai.messages import ToolReturn
from calfkit.models.fanout import EnvelopeSnapshot, FanoutOpen, FanoutOutcome, SlotRef
from calfkit.models.session_context import CallFrame, Stack, WorkflowState
from calfkit.models.state import FailedToolCall, State
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


async def test_close_materializes_failed_tool_call_as_typed_marker(store: FakeFanoutBatchStore) -> None:
    # (coverage a) A return-only tool failure folds as a FailedToolCall outcome; on close it must
    # materialize into the snapshot's State as a TYPED FailedToolCall (via the discriminator), not a
    # bare dict or None — so the resumed agent body sees the marker and strands the caller.
    marker = FailedToolCall.build_safe(tool_name="t", tool_call_id="tc1", exc_type="ValueError", exc_message="boom")
    await store.open("X", _open(), _snapshot())
    await fold_sibling(store, "X", FanoutOutcome(slot="f1", tag="tc1", result=marker))
    await fold_sibling(store, "X", _outcome("f2", "tc2", value="ok"))  # completes
    result = await close_batch(store, "X")
    assert isinstance(result, CloseResume)
    materialized = result.snapshot.state.get_tool_result("tc1")
    assert isinstance(materialized, FailedToolCall)
    assert materialized.exc_type == "ValueError"
    assert materialized.tool_call_id == "tc1"


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
    await store.open("X", _open(), _snapshot())  # 2-slot batch (the N>=2 invariant)
    await fold_sibling(store, "X", _outcome("f1", "tc1"))
    await fold_sibling(store, "X", _outcome("f2", tag=None))  # no tag → can't materialize → completes the batch
    result = await close_batch(store, "X")
    assert isinstance(result, CloseAbort)
    assert result.reason == "materialization_failed"
    assert await store.read_state("X") is None  # the un-closable batch is tombstoned


async def test_close_duplicate_tag_materializes_last_write_wins(store: FakeFanoutBatchStore) -> None:
    # Two DISTINCT slots carrying the SAME tag (an anomaly — the agent mints distinct tool_call_ids,
    # so nothing produces this in practice; the types do not enforce tag-uniqueness across slots)
    # materialize into one tool_results[tag] key: close does a last-write-wins overwrite and still
    # resumes cleanly. Pinned so PR-6 (which re-homes the materialization loop onto parts/fault) is
    # aware of the behavior before touching it.
    reg = FanoutOpen(fanout_id="X", node_id="agent", expected=[SlotRef(frame_id="f1", tag="dup"), SlotRef(frame_id="f2", tag="dup")])
    await store.open("X", reg, _snapshot())
    await fold_sibling(store, "X", FanoutOutcome(slot="f1", tag="dup", result=ToolReturn(return_value="first")))
    await fold_sibling(store, "X", FanoutOutcome(slot="f2", tag="dup", result=ToolReturn(return_value="second")))
    result = await close_batch(store, "X")
    assert isinstance(result, CloseResume)
    # outcomes is insertion-ordered, so the second fold (f2) wins the last-write-wins overwrite.
    assert result.snapshot.state.get_tool_result("dup") == ToolReturn(return_value="second")


async def test_close_tag_present_result_none_resumes_without_crashing(store: FakeFanoutBatchStore) -> None:
    # A folded outcome with a tag but result=None — a marked sibling whose tag had no
    # state.tool_results entry (base.py reads tool_results.get(tag), which can be None) — is
    # materialized as add_tool_result(tag, None). This is a return-only anomaly; pin that close
    # RESUMES (does not crash or abort) and the None lands, so the batch never hangs. The semantics of
    # a None result are revisited by the rail (FanoutOutcome gains parts/fault).
    await store.open("X", _open(), _snapshot())
    await fold_sibling(store, "X", FanoutOutcome(slot="f1", tag="tc1", result=None))
    await fold_sibling(store, "X", _outcome("f2", "tc2", value="ok"))
    result = await close_batch(store, "X")
    assert isinstance(result, CloseResume)
    assert result.snapshot.state.get_tool_result("tc1") is None
    assert await store.read_state("X") is None  # closed + tombstoned


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
    await fold_sibling(store, "X", _outcome("f1", "tc1"))
    await fold_sibling(store, "X", _outcome("f2", "tc2"))
    await close_batch(store, "X")  # tombstones
    await store.open("X", _open(), _snapshot())  # sequential re-fan-out, same key
    state = await store.read_state("X")
    assert state is not None
    assert state.outcomes == {}  # fresh registration, never the stale tombstone
