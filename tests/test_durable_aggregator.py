"""End-to-end and component tests for the durable fan-out aggregator wired
into BaseAgentNodeDef.

The full happy path (LLM → parallel tools → aggregator → final answer) is
already exercised by ``tests/test_concurrent_tool_calls.py`` under the new
durable code path — the parallel test there now goes through the
aggregator transparently with no source change.

This module covers the aggregator-specific edge cases that don't depend
on a real LLM round-trip:

  - Idempotent dispatch (deterministic fan_out_id from inbound frame_id)
  - Duplicate-return dedup (tool_call_id in batch.received)
  - Late-return rejection after completion (recently_completed TTL)
  - Orphan-return drop (no active batch)
  - sequential_only_mode DeprecationWarning

These exercise ``BaseAgentNodeDef._aggregator_handler`` and
``_publish_parallel_with_aggregator`` against ``TestKafkaBroker`` /
``InMemoryAggregator`` directly, so a real LLM and tool execution loop
aren't required.

Rebalance + partition-scoped behaviour is gated behind the
``KAFKA_TESTCONTAINERS=1`` env var and lives in
``tests/integration_kafka/`` (deferred fast-follow milestone).
"""

from __future__ import annotations

import time
import warnings
from unittest.mock import AsyncMock, MagicMock

import pytest

from calfkit._vendor.pydantic_ai.models.function import FunctionModel
from calfkit.models.envelope import Envelope
from calfkit.models.session_context import (
    CallFrame,
    CallFrameStack,
    Deps,
    SessionRunContext,
    WorkflowState,
)
from calfkit.models.state import State
from calfkit.nodes.aggregator._in_memory_store import _InFlightBatch


def _make_envelope(
    correlation_id: str,
    state: State | None = None,
    frame_id: str = "frame-0",
    target_topic: str = "agent.input",
    callback_topic: str = "client.reply",
) -> Envelope:
    """Construct an envelope shaped like a tool's ReturnCall arriving on
    the aggregator's returns topic."""
    return Envelope(
        context=SessionRunContext(
            state=state if state is not None else State(),
            deps=Deps(correlation_id=correlation_id, provided_deps={}),
        ),
        internal_workflow_state=WorkflowState(
            call_stack=CallFrameStack(
                _internal_list=[
                    CallFrame(
                        target_topic=target_topic,
                        callback_topic=callback_topic,
                        frame_id=frame_id,
                    ),
                ],
            ),
        ),
    )


def _state_with_results(results: dict[str, object]) -> State:
    state = State()
    for tcid, result in results.items():
        state.add_tool_result(tcid, result)
    return state


@pytest.fixture
def agent() -> object:
    """Build a real BaseAgentNodeDef without going through the full Worker
    flow. We stub out the model client because none of these tests invoke
    the LLM — they exercise the aggregator handler / publish path."""
    from calfkit.nodes.agent import BaseAgentNodeDef

    # FunctionModel is a pydantic-ai test model that doesn't need network /
    # API keys; the underlying Agent accepts any callable. None of the tests
    # in this module invoke the model so the dummy callable never runs.
    model_client = FunctionModel(lambda messages, info: None)  # type: ignore[arg-type]
    agent = BaseAgentNodeDef(
        node_id="test_agent",
        subscribe_topics="test_agent.input",
        model_client=model_client,
    )
    return agent


@pytest.fixture
def primed_state_store(agent: object) -> tuple[object, MagicMock]:
    """Wire up a minimal aggregator runtime on the agent without going
    through Worker.setup. Replaces the broker with a MagicMock so
    publishes are observable; uses a stub rebalance listener since
    these tests don't simulate partition rebalances."""
    from calfkit.nodes.aggregator._kafka_state_store import _KafkaStateStore
    from calfkit.nodes.aggregator._rebalance import _StateStoreRebalanceListener
    from calfkit.nodes.aggregator._runtime import _FanOutRuntime

    broker = MagicMock()
    broker.publish = AsyncMock()
    state_store = _KafkaStateStore(
        broker=broker,
        state_topic="test_agent.fanout-state",
        bootstrap_servers="localhost:9092",
        partition_count=6,
    )
    rebalance_listener = _StateStoreRebalanceListener(
        state_store=state_store,
        returns_topic="test_agent.fanout-returns",
    )
    agent.aggregator._runtime = _FanOutRuntime(  # type: ignore[attr-defined]
        state_topic="test_agent.fanout-state",
        returns_topic="test_agent.fanout-returns",
        partition_count=6,
        state_store=state_store,
        rebalance_listener=rebalance_listener,
    )
    return agent, broker


# ---------------------------------------------------------------------------
# Deprecation warning
# ---------------------------------------------------------------------------


def test_sequential_only_mode_emits_deprecation_warning() -> None:
    from calfkit.nodes.agent import BaseAgentNodeDef

    # FunctionModel is a pydantic-ai test model that doesn't need network /
    # API keys; the underlying Agent accepts any callable. None of the tests
    # in this module invoke the model so the dummy callable never runs.
    model_client = FunctionModel(lambda messages, info: None)  # type: ignore[arg-type]
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        BaseAgentNodeDef(
            node_id="test_seq",
            subscribe_topics="test_seq.input",
            model_client=model_client,
            sequential_only_mode=True,
        )

    deprecation_warnings = [w for w in recorded if issubclass(w.category, DeprecationWarning)]
    assert deprecation_warnings, "expected a DeprecationWarning for sequential_only_mode=True"
    assert "sequential_only_mode" in str(deprecation_warnings[0].message)


def test_sequential_only_mode_default_false_emits_no_warning() -> None:
    from calfkit.nodes.agent import BaseAgentNodeDef

    # FunctionModel is a pydantic-ai test model that doesn't need network /
    # API keys; the underlying Agent accepts any callable. None of the tests
    # in this module invoke the model so the dummy callable never runs.
    model_client = FunctionModel(lambda messages, info: None)  # type: ignore[arg-type]
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        BaseAgentNodeDef(
            node_id="test_default",
            subscribe_topics="test_default.input",
            model_client=model_client,
        )

    deprecation_warnings = [w for w in recorded if issubclass(w.category, DeprecationWarning)]
    assert not any("sequential_only_mode" in str(w.message) for w in deprecation_warnings)


# ---------------------------------------------------------------------------
# Aggregator handler — idempotency, dedup, late returns, orphans
# ---------------------------------------------------------------------------


async def test_duplicate_return_is_deduped(primed_state_store: tuple[object, MagicMock]) -> None:
    """A redelivered tool return for an already-received tool_call_id is a no-op."""
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    # Seed the cache with an in-flight batch.
    key = ("corr-1", "fan-1")
    initial = _InFlightBatch(
        correlation_id="corr-1",
        fan_out_id="fan-1",
        expected_tool_call_ids=frozenset({"t1", "t2", "t3"}),
        base_state=State(),
        received={"t1": "result1"},
        started_at_ms=1000,
        last_updated_ms=1000,
        agent_topic="test_agent.input",
    )
    state_store._cache[key] = initial

    envelope = _make_envelope(
        "corr-1",
        state=_state_with_results({"t1": "different-result"}),  # duplicate
    )
    headers = {"x-calf-fanout-id": "fan-1"}

    # Handler should drop the duplicate without publishing or mutating cache.
    response = await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-1",
        headers=headers,
        broker=broker,
    )

    assert response is not None
    # No state-topic publish for the duplicate.
    broker.publish.assert_not_awaited()
    # Cache state unchanged.
    assert state_store._cache[key].received == {"t1": "result1"}


async def test_late_return_after_completion_is_dropped(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """A return arriving after the batch has been tombstoned is dropped."""
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    key = ("corr-late", "fan-late")
    state_store.mark_completed(key)  # simulate prior tombstone

    envelope = _make_envelope(
        "corr-late",
        state=_state_with_results({"t1": "too-late"}),
    )
    headers = {"x-calf-fanout-id": "fan-late"}

    response = await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-late",
        headers=headers,
        broker=broker,
    )

    assert response is not None
    broker.publish.assert_not_awaited()


async def test_orphan_return_on_unowned_partition_is_dropped(primed_state_store: tuple[object, MagicMock]) -> None:
    """A return for a fan_out_id with no active batch on a partition NOT
    currently owned by this worker is dropped — legitimate rebalance race
    (the new owner has the durable state)."""
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    # Simulate this worker owning partitions {1, 2} but the inbound arrived
    # on partition 0 (the old owner). Acking is correct here — the new
    # owner of partition 0 will see the durable state.
    state_store._owned_partitions = {1, 2}

    envelope = _make_envelope(
        "corr-orphan",
        state=_state_with_results({"t1": "no-active-batch"}),
    )
    headers = {"x-calf-fanout-id": "fan-orphan"}

    response = await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-orphan",
        headers=headers,
        broker=broker,
        partition=0,
    )

    assert response is not None
    broker.publish.assert_not_awaited()


async def test_return_without_fanout_header_raises(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """A return missing HDR_FANOUT_ID raises AggregatorStateStoreError.

    The previous WARN-and-ack swallowed a protocol violation (an upstream
    producer or forwarder failed to stamp the header). Raising surfaces
    the broken pipeline via NACK redelivery rather than silently dropping
    work.
    """
    from calfkit.nodes.aggregator.errors import AggregatorStateStoreError

    agent, broker = primed_state_store

    envelope = _make_envelope(
        "corr-noheader",
        state=_state_with_results({"t1": "no-header"}),
    )
    headers: dict[str, object] = {}  # no fanout_id

    with pytest.raises(AggregatorStateStoreError, match="missing"):
        await agent._aggregator_handler(  # type: ignore[attr-defined]
            envelope,
            correlation_id="corr-noheader",
            headers=headers,
            broker=broker,
        )

    broker.publish.assert_not_awaited()


async def test_return_advances_batch_and_publishes_state(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """A new tool_call_id is merged into the batch and the new state is
    published to the state topic."""
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    key = ("corr-A", "fan-A")
    initial = _InFlightBatch(
        correlation_id="corr-A",
        fan_out_id="fan-A",
        expected_tool_call_ids=frozenset({"t1", "t2", "t3"}),
        base_state=State(),
        received={"t1": "r1"},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )
    state_store._cache[key] = initial

    envelope = _make_envelope(
        "corr-A",
        state=_state_with_results({"t1": "r1", "t2": "r2"}),
    )
    headers = {"x-calf-fanout-id": "fan-A"}

    await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-A",
        headers=headers,
        broker=broker,
    )

    # The new state was published to the state topic, not the agent topic
    # (batch not yet complete — 2/3 received).
    assert broker.publish.await_count == 1
    state_publish_call = broker.publish.await_args
    assert state_publish_call.kwargs["topic"] == "test_agent.fanout-state"

    # Cache reflects the new tool result.
    updated = state_store._cache[key]
    assert updated.received == {"t1": "r1", "t2": "r2"}


async def test_completion_publishes_aggregated_and_tombstones(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """When the final tool returns, the merged state is published to the
    agent's main topic and the batch is tombstoned."""
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    key = ("corr-B", "fan-B")
    initial = _InFlightBatch(
        correlation_id="corr-B",
        fan_out_id="fan-B",
        expected_tool_call_ids=frozenset({"t1", "t2"}),
        base_state=State(),
        received={"t1": "r1"},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )
    state_store._cache[key] = initial

    envelope = _make_envelope(
        "corr-B",
        state=_state_with_results({"t1": "r1", "t2": "r2"}),
    )
    headers = {"x-calf-fanout-id": "fan-B"}

    await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-B",
        headers=headers,
        broker=broker,
    )

    # Three publishes total:
    #   1. state-topic update with the merged batch (after t2 received)
    #   2. agent-topic aggregated ReturnCall
    #   3. state-topic tombstone (value=None)
    assert broker.publish.await_count == 3
    topics_published = [call.kwargs["topic"] for call in broker.publish.await_args_list]
    assert topics_published == [
        "test_agent.fanout-state",  # batch state after t2 merged
        "test_agent.input",  # aggregated return to the agent's main topic
        "test_agent.fanout-state",  # tombstone
    ]
    # The tombstone publish has value=None (first positional arg).
    tombstone_call = broker.publish.await_args_list[2]
    assert tombstone_call.args[0] is None

    # Cache is empty after completion and the key is in recently-completed.
    assert state_store.get(key) is None
    assert state_store.was_recently_completed(key)


async def test_unexpected_tool_call_id_is_ignored(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """A return with a tool_call_id NOT in expected_tool_call_ids is dropped."""
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    key = ("corr-X", "fan-X")
    state_store._cache[key] = _InFlightBatch(
        correlation_id="corr-X",
        fan_out_id="fan-X",
        expected_tool_call_ids=frozenset({"t1", "t2"}),
        base_state=State(),
        received={},
        started_at_ms=1000,
        last_updated_ms=1000,
        agent_topic="test_agent.input",
    )

    envelope = _make_envelope(
        "corr-X",
        state=_state_with_results({"unexpected_tcid": "stray-result"}),
    )
    headers = {"x-calf-fanout-id": "fan-X"}

    await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-X",
        headers=headers,
        broker=broker,
    )

    broker.publish.assert_not_awaited()
    assert state_store._cache[key].received == {}


# ---------------------------------------------------------------------------
# Deterministic fan_out_id (idempotent dispatch)
# ---------------------------------------------------------------------------


async def test_fan_out_id_is_the_inbound_frame_id(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """The fan_out_id used as the state-store key MUST equal the inbound
    CallFrame.frame_id — that's how idempotent dispatch on redelivery
    works. Verify by dispatching once and inspecting the durable key."""
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    envelope = _make_envelope("corr-id", frame_id="my-stable-frame-id")
    calls = _make_calls("t1")

    await agent._publish_parallel_with_aggregator(  # type: ignore[attr-defined]
        calls,
        envelope,
        "corr-id",
        broker,
        partition=0,
    )

    # The state-store key uses the inbound frame_id as fan_out_id.
    assert ("corr-id", "my-stable-frame-id") in state_store._cache


# ---------------------------------------------------------------------------
# Idempotent dispatch on inbound redelivery
# ---------------------------------------------------------------------------


def _make_calls(*tool_call_ids: str, topic: str = "tool.input") -> list[object]:
    """Build a list of Call[State] objects shaped like
    BaseAgentNodeDef.run would produce for parallel fan-out."""
    from calfkit.models import Call

    return [Call[State](topic, State(), tcid) for tcid in tool_call_ids]


async def test_redispatch_preserves_received_for_in_flight_batch(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """An inbound redelivered for an in-flight batch must NOT overwrite
    the already-merged ``received`` map. The durable record must be left
    untouched; only the tool Calls re-publish (tool dedup is the tool's
    responsibility)."""
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    correlation_id = "corr-R"
    frame_id = "frame-R"
    key = (correlation_id, frame_id)

    # Seed: this is what we'd see if a prior dispatch already happened and
    # one of three tools has already returned and been merged.
    state_store._cache[key] = _InFlightBatch(
        correlation_id=correlation_id,
        fan_out_id=frame_id,
        expected_tool_call_ids=frozenset({"t1", "t2", "t3"}),
        base_state=State(),
        received={"t1": "already-merged-result"},
        started_at_ms=1000,
        last_updated_ms=2000,
        agent_topic="test_agent.input",
    )

    envelope = _make_envelope(correlation_id, frame_id=frame_id)
    calls = _make_calls("t1", "t2", "t3")

    await agent._publish_parallel_with_aggregator(  # type: ignore[attr-defined]
        calls,
        envelope,
        correlation_id,
        broker,
        partition=0,
    )

    # No state-topic publish — the existing batch was preserved.
    state_publishes = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "test_agent.fanout-state"]
    assert state_publishes == [], "expected no state-topic publish on redispatch"

    # Cache state is preserved exactly — received["t1"] survives.
    preserved = state_store._cache[key]
    assert preserved.received == {"t1": "already-merged-result"}
    assert preserved.last_updated_ms == 2000

    # Tool Calls were re-published (3 of them, one per tool).
    tool_publishes = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "tool.input"]
    assert len(tool_publishes) == 3


async def test_redispatch_skipped_for_completed_batch(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """An inbound redelivered for a batch that already completed (and was
    tombstoned within the recently-completed TTL) must not re-dispatch
    the tools or write any new state — the work is done."""
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    correlation_id = "corr-C"
    frame_id = "frame-C"
    key = (correlation_id, frame_id)

    state_store.mark_completed(key)  # simulate prior tombstone

    envelope = _make_envelope(correlation_id, frame_id=frame_id)
    calls = _make_calls("t1", "t2")

    await agent._publish_parallel_with_aggregator(  # type: ignore[attr-defined]
        calls,
        envelope,
        correlation_id,
        broker,
        partition=0,
    )

    broker.publish.assert_not_awaited()


async def test_first_time_dispatch_persists_initial_batch(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """When there's no prior state for the key, the initial batch is
    durably persisted before any tool Calls are published."""
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    correlation_id = "corr-F"
    frame_id = "frame-F"
    key = (correlation_id, frame_id)
    assert state_store.get(key) is None

    envelope = _make_envelope(correlation_id, frame_id=frame_id)
    calls = _make_calls("t1", "t2")

    await agent._publish_parallel_with_aggregator(  # type: ignore[attr-defined]
        calls,
        envelope,
        correlation_id,
        broker,
        partition=0,
    )

    persisted = state_store._cache[key]
    assert persisted.expected_tool_call_ids == frozenset({"t1", "t2"})
    assert persisted.received == {}

    # First publish was to the state topic; subsequent publishes were
    # the tool Calls.
    first_topic = broker.publish.await_args_list[0].kwargs["topic"]
    assert first_topic == "test_agent.fanout-state"


async def test_publish_failure_leaves_cache_unchanged(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """A failed durable publish during a merge must NOT mutate the cached
    batch. FastStream redelivers on raise; the redelivered handler must
    see the un-merged batch so the merge re-attempts (otherwise the
    in-memory dedup would skip the tcid and the merge is silently lost)."""
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    key = ("corr-pubfail", "fan-pubfail")
    state_store._cache[key] = _InFlightBatch(
        correlation_id="corr-pubfail",
        fan_out_id="fan-pubfail",
        expected_tool_call_ids=frozenset({"t1", "t2"}),
        base_state=State(),
        received={},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )

    broker.publish.side_effect = RuntimeError("simulated broker outage")

    envelope = _make_envelope(
        "corr-pubfail",
        state=_state_with_results({"t1": "result-1"}),
    )
    headers = {"x-calf-fanout-id": "fan-pubfail"}

    with pytest.raises(RuntimeError, match="simulated broker outage"):
        await agent._aggregator_handler(  # type: ignore[attr-defined]
            envelope,
            correlation_id="corr-pubfail",
            headers=headers,
            broker=broker,
        )

    # The cache must still reflect the pre-merge state. If we'd mutated
    # batch.received before publishing, this would fail and the next
    # delivery's dedup would mistakenly skip t1.
    cached = state_store._cache[key]
    assert cached.received == {}, "received must NOT contain t1 after publish failure"


# ---------------------------------------------------------------------------
# MergeErrorPolicy.FALLBACK_TO_DEFAULT — degraded-merge header
# ---------------------------------------------------------------------------


async def test_abort_policy_re_raises_as_aggregator_merge_error(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """ABORT: user's merge raises → handler re-raises AggregatorMergeError
    with structured correlation_id/fan_out_id. FastStream's
    ack_policy=NACK_ON_ERROR (set on the fanout-returns subscription) then
    rewinds the offset so the message is redelivered and the merge is
    re-attempted on the next consume.

    ABORT is no longer the default — must be opted into explicitly. The
    new default (:data:`MergeErrorPolicy.FALLBACK_TO_DEFAULT`) is covered
    by :func:`test_fallback_to_default_policy_stamps_degraded_header_on_publish`.
    """
    from calfkit.nodes.aggregator.aggregator import MergeErrorPolicy
    from calfkit.nodes.aggregator.errors import AggregatorMergeError
    from calfkit.nodes.aggregator.state import AggregatedReturn, AggregatorBatch

    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]
    agent.aggregator.merge_error_policy = MergeErrorPolicy.ABORT  # type: ignore[attr-defined]

    async def boom(batch: AggregatorBatch) -> AggregatedReturn:
        raise RuntimeError("user merge boom")

    agent.aggregator.merge = boom  # type: ignore[attr-defined,method-assign]

    key = ("corr-abort", "fan-abort")
    state_store._cache[key] = _InFlightBatch(
        correlation_id="corr-abort",
        fan_out_id="fan-abort",
        expected_tool_call_ids=frozenset({"t1"}),
        base_state=State(),
        received={},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )

    envelope = _make_envelope("corr-abort", state=_state_with_results({"t1": "r1"}))
    headers = {"x-calf-fanout-id": "fan-abort"}

    with pytest.raises(AggregatorMergeError) as excinfo:
        await agent._aggregator_handler(  # type: ignore[attr-defined]
            envelope,
            correlation_id="corr-abort",
            headers=headers,
            broker=broker,
        )

    assert excinfo.value.correlation_id == "corr-abort"
    assert excinfo.value.fan_out_id == "fan-abort"


async def test_retry_policy_succeeds_on_second_attempt(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """RETRY: first merge call raises, second succeeds, batch completes."""
    from calfkit.nodes.aggregator.aggregator import MergeErrorPolicy
    from calfkit.nodes.aggregator.state import AggregatedReturn, AggregatorBatch

    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]
    agent.aggregator.merge_error_policy = MergeErrorPolicy.RETRY  # type: ignore[attr-defined]

    call_count = [0]

    async def flaky(batch: AggregatorBatch) -> AggregatedReturn:
        call_count[0] += 1
        if call_count[0] == 1:
            raise RuntimeError("transient")
        return AggregatedReturn(state=batch.base_state)

    agent.aggregator.merge = flaky  # type: ignore[attr-defined,method-assign]

    key = ("corr-retry", "fan-retry")
    state_store._cache[key] = _InFlightBatch(
        correlation_id="corr-retry",
        fan_out_id="fan-retry",
        expected_tool_call_ids=frozenset({"t1"}),
        base_state=State(),
        received={},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )

    envelope = _make_envelope("corr-retry", state=_state_with_results({"t1": "r1"}))
    headers = {"x-calf-fanout-id": "fan-retry"}

    await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-retry",
        headers=headers,
        broker=broker,
    )

    assert call_count[0] == 2
    # Batch completed: aggregated return published to agent topic.
    agent_topic_publishes = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "test_agent.input"]
    assert len(agent_topic_publishes) == 1


async def test_retry_policy_exhausted_raises(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """RETRY: both attempts raise → handler re-raises AggregatorMergeError."""
    from calfkit.nodes.aggregator.aggregator import MergeErrorPolicy
    from calfkit.nodes.aggregator.errors import AggregatorMergeError
    from calfkit.nodes.aggregator.state import AggregatedReturn, AggregatorBatch

    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]
    agent.aggregator.merge_error_policy = MergeErrorPolicy.RETRY  # type: ignore[attr-defined]

    async def always_boom(batch: AggregatorBatch) -> AggregatedReturn:
        raise RuntimeError("permanent")

    agent.aggregator.merge = always_boom  # type: ignore[attr-defined,method-assign]

    key = ("corr-exh", "fan-exh")
    state_store._cache[key] = _InFlightBatch(
        correlation_id="corr-exh",
        fan_out_id="fan-exh",
        expected_tool_call_ids=frozenset({"t1"}),
        base_state=State(),
        received={},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )

    envelope = _make_envelope("corr-exh", state=_state_with_results({"t1": "r1"}))
    headers = {"x-calf-fanout-id": "fan-exh"}

    with pytest.raises(AggregatorMergeError):
        await agent._aggregator_handler(  # type: ignore[attr-defined]
            envelope,
            correlation_id="corr-exh",
            headers=headers,
            broker=broker,
        )


async def test_fallback_to_default_policy_stamps_degraded_header_on_publish(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """When the user's merge raises and MergeErrorPolicy.FALLBACK_TO_DEFAULT
    fires, the aggregator falls back to the default merge so the batch
    still completes — but the published envelope MUST carry
    HDR_DEGRADED_MERGE so operators can detect silently-degraded
    batches."""
    from calfkit._protocol import HDR_DEGRADED_MERGE
    from calfkit.nodes.aggregator.aggregator import MergeErrorPolicy
    from calfkit.nodes.aggregator.state import AggregatedReturn, AggregatorBatch

    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]
    agent.aggregator.merge_error_policy = MergeErrorPolicy.FALLBACK_TO_DEFAULT  # type: ignore[attr-defined]

    # Replace merge with a callable that always raises.
    async def boom(batch: AggregatorBatch) -> AggregatedReturn:
        raise RuntimeError("user merge boom")

    agent.aggregator.merge = boom  # type: ignore[attr-defined,method-assign]

    key = ("corr-fb2d", "fan-fb2d")
    state_store._cache[key] = _InFlightBatch(
        correlation_id="corr-fb2d",
        fan_out_id="fan-fb2d",
        expected_tool_call_ids=frozenset({"t1", "t2"}),
        base_state=State(),
        received={"t1": "r1"},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )

    envelope = _make_envelope(
        "corr-fb2d",
        state=_state_with_results({"t1": "r1", "t2": "r2"}),
    )
    headers = {"x-calf-fanout-id": "fan-fb2d"}

    await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-fb2d",
        headers=headers,
        broker=broker,
    )

    # The agent-topic publish (the aggregated return) is the second
    # publish call (after state-topic update, before tombstone).
    agent_topic_publishes = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "test_agent.input"]
    assert len(agent_topic_publishes) == 1
    headers_on_publish = agent_topic_publishes[0].kwargs["headers"]
    assert headers_on_publish.get(HDR_DEGRADED_MERGE) == "1"


async def test_redelivery_after_failed_downstream_publish_re_attempts_completion(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """Critical regression guard: when state_store.put() succeeds and the
    subsequent agent-topic publish (or tombstone) fails, FastStream
    redelivers the inbound. On redelivery the handler sees no new tcids
    (batch.received is fully populated) but MUST NOT short-circuit on the
    duplicate-return drop — it must re-attempt the completion path so the
    merged result reaches the agent at-least-once. Without this, the
    durable record sits orphaned and the agent never re-enters."""
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    # Seed: batch is fully merged (both t1 and t2 received) but not yet
    # tombstoned — the state after a successful put() whose downstream
    # publish or tombstone then failed and triggered FastStream redelivery.
    key = ("corr-stuck", "fan-stuck")
    state_store._cache[key] = _InFlightBatch(
        correlation_id="corr-stuck",
        fan_out_id="fan-stuck",
        expected_tool_call_ids=frozenset({"t1", "t2"}),
        base_state=State(),
        received={"t1": "r1", "t2": "r2"},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )

    # Redelivery: the same envelope as the prior (failed) attempt.
    # incoming_results tcids are already in batch.received → new_ids = {}.
    envelope = _make_envelope(
        "corr-stuck",
        state=_state_with_results({"t1": "r1", "t2": "r2"}),
    )
    headers = {"x-calf-fanout-id": "fan-stuck"}

    await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-stuck",
        headers=headers,
        broker=broker,
    )

    # The handler must re-publish the merged return to the agent topic
    # and tombstone the batch.
    agent_topic_publishes = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "test_agent.input"]
    assert len(agent_topic_publishes) == 1, (
        "Expected the handler to re-publish the merged result on redelivery; "
        "without this the durable record is orphaned and the agent never re-enters"
    )
    state_topic_publishes = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "test_agent.fanout-state"]
    tombstones = [c for c in state_topic_publishes if c.args[0] is None]
    assert len(tombstones) == 1, "Expected the handler to tombstone the batch"


async def test_normal_completion_does_not_stamp_degraded_header(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """Sanity check: the degraded-merge header MUST NOT be stamped on
    a normal (successful merge) completion."""
    from calfkit._protocol import HDR_DEGRADED_MERGE

    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    key = ("corr-ok", "fan-ok")
    state_store._cache[key] = _InFlightBatch(
        correlation_id="corr-ok",
        fan_out_id="fan-ok",
        expected_tool_call_ids=frozenset({"t1"}),
        base_state=State(),
        received={},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )

    envelope = _make_envelope(
        "corr-ok",
        state=_state_with_results({"t1": "r1"}),
    )
    headers = {"x-calf-fanout-id": "fan-ok"}

    await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-ok",
        headers=headers,
        broker=broker,
    )

    agent_topic_publishes = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "test_agent.input"]
    assert len(agent_topic_publishes) == 1
    headers_on_publish = agent_topic_publishes[0].kwargs["headers"]
    assert HDR_DEGRADED_MERGE not in headers_on_publish


async def test_redispatch_with_drifted_tool_call_ids_overwrites(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """If the agent loop produces a different expected_tool_call_ids set
    on re-entry, the stale state is overwritten so the new dispatch
    proceeds. This is a real upstream bug we surface via ERROR log; the
    aggregator can't silently keep stale state."""
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    correlation_id = "corr-D"
    frame_id = "frame-D"
    key = (correlation_id, frame_id)

    state_store._cache[key] = _InFlightBatch(
        correlation_id=correlation_id,
        fan_out_id=frame_id,
        expected_tool_call_ids=frozenset({"t1", "t2"}),
        base_state=State(),
        received={"t1": "stale-result"},
        started_at_ms=1000,
        last_updated_ms=2000,
        agent_topic="test_agent.input",
    )

    envelope = _make_envelope(correlation_id, frame_id=frame_id)
    # Different set than what's cached.
    calls = _make_calls("t1", "t2", "t3")

    await agent._publish_parallel_with_aggregator(  # type: ignore[attr-defined]
        calls,
        envelope,
        correlation_id,
        broker,
        partition=0,
    )

    overwritten = state_store._cache[key]
    assert overwritten.expected_tool_call_ids == frozenset({"t1", "t2", "t3"})
    assert overwritten.received == {}  # fresh batch, stale received discarded


# ---------------------------------------------------------------------------
# Re-entry frame synthesis (fan_out_id distinctness across LLM turns)
# ---------------------------------------------------------------------------


async def test_aggregator_reentry_synthesizes_new_frame(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """When the aggregator completes a batch and re-publishes to the agent's
    main topic, the re-entry envelope's ``current_frame.frame_id`` MUST be
    a fresh value, not the inbound's frame_id. This is what prevents
    fan_out_id collisions across consecutive LLM turns within a single
    invocation."""
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    inbound_frame_id = "inbound-frame-XYZ"
    key = ("corr-newframe", "fan-newframe")
    state_store._cache[key] = _InFlightBatch(
        correlation_id="corr-newframe",
        fan_out_id="fan-newframe",
        expected_tool_call_ids=frozenset({"t1"}),
        base_state=State(),
        received={},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )

    envelope = _make_envelope(
        "corr-newframe",
        state=_state_with_results({"t1": "r1"}),
        frame_id=inbound_frame_id,
    )
    headers = {"x-calf-fanout-id": "fan-newframe"}

    await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-newframe",
        headers=headers,
        broker=broker,
    )

    agent_topic_publishes = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "test_agent.input"]
    assert len(agent_topic_publishes) == 1
    publish_envelope = agent_topic_publishes[0].args[0]
    re_entry_frame_id = publish_envelope.internal_workflow_state.current_frame.frame_id

    assert re_entry_frame_id != inbound_frame_id, (
        "Re-entry envelope MUST have a fresh frame_id; otherwise a subsequent "
        "parallel fan-out within the same invocation will derive a colliding "
        "fan_out_id and hit was_recently_completed → silent skip → agent hangs."
    )

    inbound_frame = envelope.internal_workflow_state.current_frame
    re_entry_frame = publish_envelope.internal_workflow_state.current_frame
    assert re_entry_frame.target_topic == inbound_frame.target_topic
    assert re_entry_frame.callback_topic == inbound_frame.callback_topic


async def test_aggregator_reentry_stamps_new_frame_id_header(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """The HDR_FRAME_ID on the re-entry publish must match the synthesized
    frame's frame_id (not the inbound's), so external dedup / tracing
    sees the fresh hop identity."""
    from calfkit._protocol import HDR_FRAME_ID

    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    inbound_frame_id = "inbound-frame-HDR"
    key = ("corr-hdrframe", "fan-hdrframe")
    state_store._cache[key] = _InFlightBatch(
        correlation_id="corr-hdrframe",
        fan_out_id="fan-hdrframe",
        expected_tool_call_ids=frozenset({"t1"}),
        base_state=State(),
        received={},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )

    envelope = _make_envelope(
        "corr-hdrframe",
        state=_state_with_results({"t1": "r1"}),
        frame_id=inbound_frame_id,
    )
    headers = {"x-calf-fanout-id": "fan-hdrframe"}

    await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-hdrframe",
        headers=headers,
        broker=broker,
    )

    agent_topic_publishes = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "test_agent.input"]
    publish_envelope = agent_topic_publishes[0].args[0]
    publish_headers = agent_topic_publishes[0].kwargs["headers"]
    expected_frame_id = publish_envelope.internal_workflow_state.current_frame.frame_id

    assert publish_headers.get(HDR_FRAME_ID) == expected_frame_id
    assert publish_headers.get(HDR_FRAME_ID) != inbound_frame_id


async def test_multi_turn_parallel_fanout_within_single_invocation(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """Simulate two consecutive parallel fan-outs within ONE invocation
    (same correlation_id, simulating two consecutive LLM turns). After the
    first batch completes and tombstones, the second batch must dispatch
    successfully — NOT be skipped via was_recently_completed because the
    re-entry frame_id differs from the inbound's.
    """
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    correlation_id = "corr-multi"
    initial_frame_id = "frame-turn-1"

    # ---- Turn 1: agent receives initial inbound and dispatches parallel fan-out.
    envelope_turn_1 = _make_envelope(correlation_id, frame_id=initial_frame_id)
    calls_turn_1 = _make_calls("t1a", "t1b")

    await agent._publish_parallel_with_aggregator(  # type: ignore[attr-defined]
        calls_turn_1,
        envelope_turn_1,
        correlation_id,
        broker,
        partition=0,
    )

    # The batch is now in-flight under key=(corr, initial_frame_id).
    key_turn_1 = (correlation_id, initial_frame_id)
    assert state_store.get(key_turn_1) is not None

    # ---- Turn 1 completion: simulate both tools returning. The aggregator
    # handler accumulates and on the second return publishes the merged
    # state back to the agent topic + tombstones the batch.
    tool_return_1 = _make_envelope(
        correlation_id,
        state=_state_with_results({"t1a": "r1a", "t1b": "r1b"}),
        frame_id=initial_frame_id,  # tool unwound, agent frame on top
    )
    await agent._aggregator_handler(  # type: ignore[attr-defined]
        tool_return_1,
        correlation_id=correlation_id,
        headers={"x-calf-fanout-id": initial_frame_id},
        broker=broker,
    )
    assert state_store.was_recently_completed(key_turn_1)

    # Capture the re-entry envelope the aggregator just published to the
    # agent's main topic.
    agent_topic_publishes = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "test_agent.input"]
    assert len(agent_topic_publishes) == 1
    re_entry_envelope = agent_topic_publishes[0].args[0]
    turn_2_frame_id = re_entry_envelope.internal_workflow_state.current_frame.frame_id

    # ---- Turn 2: the agent has re-entered. Same correlation_id, but the
    # re-entry envelope MUST carry a different frame_id (otherwise this
    # fan-out's fan_out_id collides with turn 1's tombstoned key and the
    # dispatch is silently skipped).
    assert turn_2_frame_id != initial_frame_id

    broker.publish.reset_mock()  # focus assertions on turn 2
    calls_turn_2 = _make_calls("t2a", "t2b")

    await agent._publish_parallel_with_aggregator(  # type: ignore[attr-defined]
        calls_turn_2,
        re_entry_envelope,
        correlation_id,
        broker,
        partition=0,
    )

    # Turn 2 must actually dispatch (not skip on was_recently_completed).
    tool_publishes_turn_2 = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "tool.input"]
    assert len(tool_publishes_turn_2) == 2, (
        "Turn-2 parallel fan-out must dispatch all tool Calls; observed "
        f"{len(tool_publishes_turn_2)}. Most likely the re-entry frame_id "
        f"collided with turn-1's tombstoned key, causing silent skip."
    )

    # Turn 2's batch is now active under a key distinct from turn 1's.
    key_turn_2 = (correlation_id, turn_2_frame_id)
    assert key_turn_2 != key_turn_1
    assert state_store.get(key_turn_2) is not None


async def test_aggregator_reentry_redelivery_is_idempotent(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """When the aggregator's re-entry message is redelivered by Kafka
    (e.g., FastStream NACK_ON_ERROR fires between aggregator publish and
    handler ack), the redelivered envelope carries the SAME synthesized
    frame_id. The agent's redelivery handling at
    ``_publish_parallel_with_aggregator`` must recognise the second
    delivery as a redelivery for the in-flight batch (or
    recently-completed if turn-2 already finished) — NOT as a fresh
    dispatch.
    """
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    correlation_id = "corr-reidx"
    inbound_frame_id = "frame-reidx"

    # Drive a complete turn-1 cycle to capture the re-entry envelope.
    state_store._cache[(correlation_id, inbound_frame_id)] = _InFlightBatch(
        correlation_id=correlation_id,
        fan_out_id=inbound_frame_id,
        expected_tool_call_ids=frozenset({"t1"}),
        base_state=State(),
        received={},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )
    inbound_envelope = _make_envelope(
        correlation_id,
        state=_state_with_results({"t1": "r1"}),
        frame_id=inbound_frame_id,
    )
    await agent._aggregator_handler(  # type: ignore[attr-defined]
        inbound_envelope,
        correlation_id=correlation_id,
        headers={"x-calf-fanout-id": inbound_frame_id},
        broker=broker,
    )

    agent_topic_publishes = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "test_agent.input"]
    re_entry_envelope = agent_topic_publishes[0].args[0]
    re_entry_frame_id = re_entry_envelope.internal_workflow_state.current_frame.frame_id

    # ---- First delivery of the re-entry envelope: agent emits parallel
    # fan-out. First-time dispatch persists initial batch.
    broker.publish.reset_mock()
    calls = _make_calls("ta", "tb")
    await agent._publish_parallel_with_aggregator(  # type: ignore[attr-defined]
        calls,
        re_entry_envelope,
        correlation_id,
        broker,
        partition=0,
    )

    new_key = (correlation_id, re_entry_frame_id)
    persisted = state_store.get(new_key)
    assert persisted is not None
    # Verify first-time dispatch wrote the durable batch (state-topic publish).
    state_publishes_first = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "test_agent.fanout-state"]
    assert len(state_publishes_first) == 1, "first delivery must persist the initial batch"

    # ---- Second delivery (FastStream redelivery): same envelope, same
    # frame_id. Must hit the "in-flight redelivery" branch — NO new
    # state-topic publish, just re-publish the tool Calls.
    broker.publish.reset_mock()
    calls_redelivered = _make_calls("ta", "tb")
    await agent._publish_parallel_with_aggregator(  # type: ignore[attr-defined]
        calls_redelivered,
        re_entry_envelope,
        correlation_id,
        broker,
        partition=0,
    )

    state_publishes_second = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "test_agent.fanout-state"]
    assert len(state_publishes_second) == 0, (
        "Redelivery must NOT write a new state-topic record; the existing in-flight batch is preserved (in-flight redelivery branch)."
    )
    tool_publishes_second = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "tool.input"]
    assert len(tool_publishes_second) == 2


# ---------------------------------------------------------------------------
# Acknowledgement policy on fanout-returns subscription
# ---------------------------------------------------------------------------


def test_aggregator_subscription_uses_nack_on_error_policy(agent: object) -> None:
    """The fanout-returns subscription MUST set ack_policy=NACK_ON_ERROR.

    Several handler paths (ABORT merge policy, publish-failure recovery,
    redelivery-after-failed-completion) rely on FastStream redelivering
    when the handler raises. The default ACK_FIRST commits the offset
    BEFORE the handler runs, so raises do not produce redelivery — those
    recovery paths would silently drop the merge.

    NACK_ON_ERROR seeks the consumer back so the message is redelivered
    immediately (within the same consumer session, no restart required).
    """
    from faststream import AckPolicy

    subs = agent.kafka_subscriptions()  # type: ignore[attr-defined]
    fanout_returns_subs = [s for s in subs if "test_agent.fanout-returns" in s.topics]
    assert len(fanout_returns_subs) == 1
    sub = fanout_returns_subs[0]
    assert sub.ack_policy is AckPolicy.NACK_ON_ERROR, (
        f"fanout-returns subscription must set ack_policy=NACK_ON_ERROR, "
        f"got {sub.ack_policy!r}. Without this, FastStream defaults to "
        f"ACK_FIRST and handler raises will not trigger redelivery."
    )


def test_agent_main_subscription_uses_nack_on_error_policy(agent: object) -> None:
    """The agent's MAIN inbound subscription MUST set ack_policy=NACK_ON_ERROR.

    The default ACK_FIRST commits the inbound offset BEFORE the handler
    runs. If ``_publish_parallel_with_aggregator`` then raises (e.g., the
    durable ``state_store.put`` errors, or the per-Call publish loop
    fails mid-flight) the inbound is committed-then-lost: tool dispatch
    silently disappears and the user-facing request hangs forever.

    NACK_ON_ERROR rewinds the consumer offset on raise so the inbound is
    redelivered within the same consumer session, preserving the durable
    state-store correctness invariant ("publish then update cache" must
    be atomic from the consumer's perspective).
    """
    from faststream import AckPolicy

    subs = agent.kafka_subscriptions()  # type: ignore[attr-defined]
    main_subs = [s for s in subs if "test_agent.input" in s.topics]
    assert len(main_subs) == 1
    sub = main_subs[0]
    assert sub.ack_policy is AckPolicy.NACK_ON_ERROR, (
        f"agent main subscription must set ack_policy=NACK_ON_ERROR, "
        f"got {sub.ack_policy!r}. Without this, FastStream defaults to "
        f"ACK_FIRST and a raise in _publish_parallel_with_aggregator "
        f"would commit-then-lose the inbound."
    )


async def test_handler_state_store_failure_propagates_without_committing_offset(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """If state_store.put raises, the exception must propagate from the
    handler. The ack_policy=NACK_ON_ERROR (configured on the subscription)
    then ensures Kafka rewinds the offset so the message is redelivered.

    This test exercises the propagation half (the subscription-level ack
    policy is verified separately by
    test_aggregator_subscription_uses_nack_on_error_policy).
    """
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    key = ("corr-sserr", "fan-sserr")
    state_store._cache[key] = _InFlightBatch(
        correlation_id="corr-sserr",
        fan_out_id="fan-sserr",
        expected_tool_call_ids=frozenset({"t1", "t2"}),
        base_state=State(),
        received={},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )

    original_put = state_store.put

    async def boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("state store outage")

    state_store.put = boom  # type: ignore[method-assign]
    try:
        envelope = _make_envelope("corr-sserr", state=_state_with_results({"t1": "r1"}))
        headers = {"x-calf-fanout-id": "fan-sserr"}
        with pytest.raises(RuntimeError, match="state store outage"):
            await agent._aggregator_handler(  # type: ignore[attr-defined]
                envelope,
                correlation_id="corr-sserr",
                headers=headers,
                broker=broker,
            )
    finally:
        state_store.put = original_put  # type: ignore[method-assign]

    # Cache is unchanged — the failed put didn't mutate state.
    assert state_store.get(key) is not None
    assert state_store.get(key).received == {}


# ---------------------------------------------------------------------------
# Drifted expected_tool_call_ids overwrite -> stamp degraded header
# ---------------------------------------------------------------------------


async def test_drifted_expected_ids_overwrite_stamps_degraded_header(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """When a redelivered inbound dispatch carries a DIFFERENT
    ``expected_tool_call_ids`` set than the durable cached batch (a real
    upstream bug surfaced by the agent loop), the framework currently
    overwrites the stale state with the fresh dispatch. Any already-merged
    tool results in the previous ``received`` map are silently discarded.

    The eventual completion publish for the new batch MUST stamp
    :data:`HDR_DEGRADED_MERGE` so downstream observability captures the
    data-loss signal — operators inspecting the agent topic see the merge
    came from a path that discarded prior work.
    """
    from calfkit._protocol import HDR_DEGRADED_MERGE

    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    correlation_id = "corr-drift"
    frame_id = "frame-drift"
    key = (correlation_id, frame_id)

    # Pre-seed an in-flight batch with some received returns; these will
    # be silently discarded by the drifted overwrite.
    state_store._cache[key] = _InFlightBatch(
        correlation_id=correlation_id,
        fan_out_id=frame_id,
        expected_tool_call_ids=frozenset({"t1", "t2"}),
        base_state=State(),
        received={"t1": "stale-result"},
        started_at_ms=1000,
        last_updated_ms=2000,
        agent_topic="test_agent.input",
    )

    # Drift: re-dispatch with a different tool-call set.
    inbound_envelope = _make_envelope(correlation_id, frame_id=frame_id)
    new_calls = _make_calls("t3", "t4")
    await agent._publish_parallel_with_aggregator(  # type: ignore[attr-defined]
        new_calls,
        inbound_envelope,
        correlation_id,
        broker,
        partition=0,
    )

    # The cache should now reflect the fresh dispatch (drifted overwrite).
    overwritten = state_store._cache[key]
    assert overwritten.expected_tool_call_ids == frozenset({"t3", "t4"})
    assert overwritten.received == {}

    # Drive the new batch to completion: receive both returns.
    broker.publish.reset_mock()
    return_envelope = _make_envelope(
        correlation_id,
        state=_state_with_results({"t3": "r3", "t4": "r4"}),
    )
    headers = {"x-calf-fanout-id": frame_id}
    await agent._aggregator_handler(  # type: ignore[attr-defined]
        return_envelope,
        correlation_id=correlation_id,
        headers=headers,
        broker=broker,
    )

    # The completion publish MUST stamp HDR_DEGRADED_MERGE because the new
    # batch was born from a drifted-overwrite that discarded prior received
    # results. Downstream consumers / dashboards need this signal to
    # quantify the silent data loss.
    agent_topic_publishes = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "test_agent.input"]
    assert len(agent_topic_publishes) == 1
    publish_headers = agent_topic_publishes[0].kwargs["headers"]
    assert publish_headers.get(HDR_DEGRADED_MERGE) == "1", (
        f"Drifted-redispatch overwrite must mark the resulting completion as "
        f"degraded so observability sees the data-loss signal. Headers: "
        f"{publish_headers}"
    )

    # The degraded flag must live ON the durable batch record itself — not
    # only in a process-local set — so it survives worker restart.
    overwritten_batch = state_store.get(key)
    if overwritten_batch is not None:
        # The completion path tombstones the key once the publish lands.
        # If get() still returns a batch (e.g., a future change defers the
        # tombstone), assert degraded is set. If the key is already gone,
        # the rehydration test below covers the durability invariant.
        assert overwritten_batch.degraded is True


async def test_degraded_flag_survives_rehydration_and_completion_still_stamps_header(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """Architectural invariant: ``degraded`` must travel WITH the durable
    batch record on the compacted state topic — not in a process-local
    set. After a worker crash + NACK redelivery, a fresh worker rehydrates
    from the durable log and MUST still stamp HDR_DEGRADED_MERGE on the
    completion publish. Otherwise operators see one degraded completion +
    one clean completion for the same logical batch → mis-attribution of
    silent data loss.
    """
    from calfkit._protocol import HDR_DEGRADED_MERGE
    from calfkit.nodes.aggregator._kafka_state_store import _KafkaStateStore

    agent, broker = primed_state_store

    correlation_id = "corr-rehydrate"
    fan_out_id = "fan-rehydrate"
    key = (correlation_id, fan_out_id)

    # Simulate a drifted-overwrite batch that was flagged degraded at
    # dispatch time and durably persisted. The degraded flag is ON the
    # batch (not in a process-local set).
    drifted_batch = _InFlightBatch(
        correlation_id=correlation_id,
        fan_out_id=fan_out_id,
        expected_tool_call_ids=frozenset({"t1", "t2"}),
        base_state=State(),
        received={},
        started_at_ms=1000,
        last_updated_ms=2000,
        agent_topic="test_agent.input",
        degraded=True,
    )

    # Serialize to wire format and rehydrate into a fresh state store —
    # this models worker restart: process-local memory wiped, durable log
    # replayed.
    wire_record = drifted_batch.to_fanout_state()
    assert wire_record.degraded is True, "wire format must carry the degraded flag"

    fresh_store = _KafkaStateStore(
        broker=broker,
        state_topic="test_agent.fanout-state",
        bootstrap_servers="localhost:9092",
        partition_count=6,
    )
    # Manually replay the durable record as the rehydration path would.
    key_bytes = f"{correlation_id}|{fan_out_id}".encode()
    fresh_store._apply_record(
        partition=0,
        key_bytes=key_bytes,
        value_bytes=wire_record.model_dump_json().encode(),
    )

    rehydrated = fresh_store.get(key)
    assert rehydrated is not None
    assert rehydrated.degraded is True, "rehydrated batch must preserve degraded=True"

    # Swap the agent's state store for the freshly rehydrated one — models
    # the post-restart worker picking up where the previous one died.
    # ``_FanOutRuntime`` is a frozen dataclass; use ``dataclasses.replace``.
    import dataclasses as _dc

    agent.aggregator._runtime = _dc.replace(  # type: ignore[attr-defined]
        agent.aggregator._runtime,  # type: ignore[attr-defined]
        state_store=fresh_store,
    )

    # Drive the batch to completion on the rehydrated worker.
    return_envelope = _make_envelope(
        correlation_id,
        state=_state_with_results({"t1": "r1", "t2": "r2"}),
    )
    headers = {"x-calf-fanout-id": fan_out_id}
    await agent._aggregator_handler(  # type: ignore[attr-defined]
        return_envelope,
        correlation_id=correlation_id,
        headers=headers,
        broker=broker,
    )

    agent_topic_publishes = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "test_agent.input"]
    assert len(agent_topic_publishes) == 1
    publish_headers = agent_topic_publishes[0].kwargs["headers"]
    assert publish_headers.get(HDR_DEGRADED_MERGE) == "1", (
        f"Degraded flag must survive worker restart via the durable log. "
        f"The rehydrated completion publish lost the degraded signal. "
        f"Headers: {publish_headers}"
    )


# ---------------------------------------------------------------------------
# FALLBACK_TO_DEFAULT — defensive try/except around the fallback merge
# ---------------------------------------------------------------------------


async def test_fallback_to_default_when_default_also_raises_falls_through_to_abort(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """When ``merge_error_policy=FALLBACK_TO_DEFAULT`` is configured, the
    user's :meth:`merge` raising triggers a fallback to the framework's
    default merge. If THAT default merge also raises (e.g., a broken
    ``State.add_tool_result``, malformed result), the framework MUST NOT
    let the inner exception escape — it should treat the situation as
    ABORT so the operator sees a structured ``AggregatorMergeError`` and
    Kafka NACK redelivers the inbound.

    Without the defensive try/except wrap, the inner exception bypasses
    the policy entirely: the user configured FALLBACK_TO_DEFAULT expecting
    'log and continue,' yet they would get a raw exception with no
    AggregatorMergeError wrapping, no degraded-merge stamp, and no
    operator-visible context.
    """
    from calfkit.nodes.aggregator.aggregator import MergeErrorPolicy
    from calfkit.nodes.aggregator.errors import AggregatorMergeError
    from calfkit.nodes.aggregator.state import AggregatedReturn, AggregatorBatch

    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]
    agent.aggregator.merge_error_policy = MergeErrorPolicy.FALLBACK_TO_DEFAULT  # type: ignore[attr-defined]

    # Both the user merge and the framework default merge raise. The
    # default merge sits on FanOutAggregator.merge; we monkey-patch it on
    # the instance so the FALLBACK_TO_DEFAULT branch's
    # ``FanOutAggregator.merge(self.aggregator, view)`` call hits a raise.
    async def user_boom(batch: AggregatorBatch) -> AggregatedReturn:
        raise RuntimeError("user merge boom")

    # Make the default merge also raise: patch it on the FanOutAggregator
    # class (unbound) since the handler calls
    # ``FanOutAggregator.merge(self.aggregator, view)`` explicitly.
    from calfkit.nodes.aggregator import FanOutAggregator

    original_default_merge = FanOutAggregator.merge

    async def default_boom(self: FanOutAggregator, batch: AggregatorBatch) -> AggregatedReturn:
        raise RuntimeError("default merge also boom")

    FanOutAggregator.merge = default_boom  # type: ignore[method-assign]

    try:
        agent.aggregator.merge = user_boom  # type: ignore[attr-defined,method-assign]

        key = ("corr-double", "fan-double")
        state_store._cache[key] = _InFlightBatch(
            correlation_id="corr-double",
            fan_out_id="fan-double",
            expected_tool_call_ids=frozenset({"t1"}),
            base_state=State(),
            received={},
            started_at_ms=int(time.time() * 1000),
            last_updated_ms=int(time.time() * 1000),
            agent_topic="test_agent.input",
        )

        envelope = _make_envelope("corr-double", state=_state_with_results({"t1": "r1"}))
        headers = {"x-calf-fanout-id": "fan-double"}

        # Expect AggregatorMergeError (ABORT-style propagation), NOT the
        # raw RuntimeError leaking from the fallback default merge.
        with pytest.raises(AggregatorMergeError) as excinfo:
            await agent._aggregator_handler(  # type: ignore[attr-defined]
                envelope,
                correlation_id="corr-double",
                headers=headers,
                broker=broker,
            )

        assert excinfo.value.correlation_id == "corr-double"
        assert excinfo.value.fan_out_id == "fan-double"
    finally:
        FanOutAggregator.merge = original_default_merge  # type: ignore[method-assign]


# ---------------------------------------------------------------------------
# should_complete is invoked exactly once per handler call
# ---------------------------------------------------------------------------


async def test_should_complete_called_once_on_no_new_tcids_redelivery(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """The no-new-tcids redelivery branch (used to re-attempt completion
    after a failed downstream publish) must not invoke ``should_complete``
    twice. User overrides may carry side effects (logging, metrics, a
    cached LLM call); double-invocation is observable and surprising.
    """
    from calfkit.nodes.aggregator.state import AggregatorBatch

    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    call_count = [0]

    async def counting_should_complete(batch: AggregatorBatch) -> bool:
        call_count[0] += 1
        return batch.is_complete_by_count

    agent.aggregator.should_complete = counting_should_complete  # type: ignore[attr-defined,method-assign]

    # Seed a fully-merged batch that hasn't been tombstoned yet — simulates
    # the state after a successful state-store put whose downstream publish
    # or tombstone failed, triggering FastStream redelivery.
    key = ("corr-sc1", "fan-sc1")
    state_store._cache[key] = _InFlightBatch(
        correlation_id="corr-sc1",
        fan_out_id="fan-sc1",
        expected_tool_call_ids=frozenset({"t1", "t2"}),
        base_state=State(),
        received={"t1": "r1", "t2": "r2"},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )

    # Redelivered envelope: all tcids already in batch.received → new_ids = {}.
    envelope = _make_envelope(
        "corr-sc1",
        state=_state_with_results({"t1": "r1", "t2": "r2"}),
    )
    headers = {"x-calf-fanout-id": "fan-sc1"}

    await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-sc1",
        headers=headers,
        broker=broker,
    )

    assert call_count[0] == 1, (
        f"should_complete must be invoked exactly once per handler call; "
        f"observed {call_count[0]} invocations. The no-new-tcids redelivery "
        f"branch was double-calling it."
    )


async def test_should_complete_called_once_on_new_tcid_completion(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """Sanity check for the new-tcid completion path: ``should_complete`` is
    invoked exactly once per handler call, after ``on_partial`` updates the
    view."""
    from calfkit.nodes.aggregator.state import AggregatorBatch

    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    call_count = [0]

    async def counting_should_complete(batch: AggregatorBatch) -> bool:
        call_count[0] += 1
        return batch.is_complete_by_count

    agent.aggregator.should_complete = counting_should_complete  # type: ignore[attr-defined,method-assign]

    key = ("corr-sc2", "fan-sc2")
    state_store._cache[key] = _InFlightBatch(
        correlation_id="corr-sc2",
        fan_out_id="fan-sc2",
        expected_tool_call_ids=frozenset({"t1", "t2"}),
        base_state=State(),
        received={"t1": "r1"},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )

    envelope = _make_envelope(
        "corr-sc2",
        state=_state_with_results({"t1": "r1", "t2": "r2"}),
    )
    headers = {"x-calf-fanout-id": "fan-sc2"}

    await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-sc2",
        headers=headers,
        broker=broker,
    )

    assert call_count[0] == 1, f"should_complete invoked {call_count[0]} times; expected 1"


# ---------------------------------------------------------------------------
# _batch_view defensive deep-copy of base_state
# ---------------------------------------------------------------------------


async def test_user_merge_mutating_base_state_does_not_corrupt_cache(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """The :class:`AggregatorBatch` view passed to user overrides is documented
    as immutable, but ``base_state`` is a mutable Pydantic model. A buggy user
    ``merge()`` that mutates ``batch.base_state`` (instead of operating on a
    copy) must NOT corrupt the framework's cached ``_InFlightBatch.base_state``.

    Without a defensive deep-copy in ``_batch_view``, the user merge shares the
    same ``State`` object reference with the cache; a mutation there would
    bleed into any subsequent rehydration / retry / re-merge attempt.
    """
    from calfkit._vendor.pydantic_ai.messages import ModelRequest, UserPromptPart
    from calfkit.nodes.aggregator.state import AggregatedReturn, AggregatorBatch

    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    # Seed cache with a batch whose base_state has a non-trivial message_history
    # so mutation is observable.
    seeded_state = State()
    seeded_state.message_history.append(
        ModelRequest(parts=[UserPromptPart(content="original")]),
    )
    key = ("corr-mut", "fan-mut")
    state_store._cache[key] = _InFlightBatch(
        correlation_id="corr-mut",
        fan_out_id="fan-mut",
        expected_tool_call_ids=frozenset({"t1"}),
        base_state=seeded_state,
        received={},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )

    # User merge mutates batch.base_state directly — a coding mistake we want
    # to defend against, not require the user to avoid.
    async def buggy_merge(batch: AggregatorBatch) -> AggregatedReturn:
        batch.base_state.message_history.append(
            ModelRequest(parts=[UserPromptPart(content="injected by merge")]),
        )
        return AggregatedReturn(state=batch.base_state)

    agent.aggregator.merge = buggy_merge  # type: ignore[attr-defined,method-assign]

    envelope = _make_envelope("corr-mut", state=_state_with_results({"t1": "r1"}))
    headers = {"x-calf-fanout-id": "fan-mut"}

    await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-mut",
        headers=headers,
        broker=broker,
    )

    # The cached _InFlightBatch.base_state must be UNCHANGED — the framework
    # owns the cache and a user merge should never reach into it.
    assert len(seeded_state.message_history) == 1, (
        "User merge mutated framework-cached base_state. The _batch_view must deep-copy base_state so user merge can't corrupt the cache."
    )


# ---------------------------------------------------------------------------
# New: default policy, orphan-return raise semantics, deterministic re-entry,
# handler-identity matching, single deep-copy invariant.
# ---------------------------------------------------------------------------


def test_default_policy_is_fallback_to_default() -> None:
    """A bare ``FanOutAggregator()`` defaults to FALLBACK_TO_DEFAULT.

    The plain default must be safe-by-default: a bug in a user's merge
    shouldn't be able to stall a partition on an unbounded NACK redelivery
    loop. Users who want fail-loud ABORT semantics opt in explicitly.
    """
    from calfkit.nodes.aggregator import FanOutAggregator
    from calfkit.nodes.aggregator.aggregator import MergeErrorPolicy

    agg = FanOutAggregator()
    assert agg.merge_error_policy == MergeErrorPolicy.FALLBACK_TO_DEFAULT


async def test_orphan_return_with_owned_partition_raises(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """When ``state_store.get(key)`` returns ``None`` AND the partition IS
    owned by this worker, the previous WARN-and-drop hid a real durability
    violation (state-store entry vanished). Raise so NACK redelivers and
    the operator sees a stuck partition instead of a silently lost batch.
    """
    from calfkit.nodes.aggregator.errors import AggregatorStateStoreError

    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    # Simulate this worker owning partition 0 (where the inbound arrives).
    state_store._owned_partitions = {0}

    envelope = _make_envelope(
        "corr-orphan-owned",
        state=_state_with_results({"t1": "no-state-record"}),
    )
    headers = {"x-calf-fanout-id": "fan-orphan-owned"}

    with pytest.raises(AggregatorStateStoreError, match="orphan return"):
        await agent._aggregator_handler(  # type: ignore[attr-defined]
            envelope,
            correlation_id="corr-orphan-owned",
            headers=headers,
            broker=broker,
            partition=0,
        )

    # No phantom publishes from the half-handled handler call.
    broker.publish.assert_not_awaited()


async def test_orphan_return_with_unowned_partition_acks(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """When the partition is NOT owned (legitimate rebalance race), the
    handler must ack quietly so the new owner can process the message
    against its durable state. Raising here would loop on a message we
    can't act on."""
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    # Owner of partition 0 has moved elsewhere; this worker now owns {1, 2}.
    state_store._owned_partitions = {1, 2}

    envelope = _make_envelope(
        "corr-orphan-unowned",
        state=_state_with_results({"t1": "for-the-other-owner"}),
    )
    headers = {"x-calf-fanout-id": "fan-orphan-unowned"}

    response = await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-orphan-unowned",
        headers=headers,
        broker=broker,
        partition=0,
    )

    assert response is not None
    broker.publish.assert_not_awaited()


async def test_missing_fanout_id_header_raises(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """A fanout-returns message missing HDR_FANOUT_ID is a protocol
    violation. The previous WARN-and-ack swallowed the failure; raising
    forces NACK redelivery and operator attention."""
    from calfkit.nodes.aggregator.errors import AggregatorStateStoreError

    agent, broker = primed_state_store

    envelope = _make_envelope(
        "corr-missing-hdr",
        state=_state_with_results({"t1": "r1"}),
    )

    with pytest.raises(AggregatorStateStoreError, match="missing"):
        await agent._aggregator_handler(  # type: ignore[attr-defined]
            envelope,
            correlation_id="corr-missing-hdr",
            headers={},  # no HDR_FANOUT_ID
            broker=broker,
        )

    broker.publish.assert_not_awaited()


async def test_reentry_frame_id_is_deterministic(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """Two completions of the same logical batch must produce the SAME
    re-entry frame_id.

    This is what lets downstream stateful sinks dedup redelivered re-entry
    envelopes (e.g., a NACK between agent-topic publish and tombstone
    causes the same merged batch to re-fire the completion path; each
    redelivery must look like the same event downstream).

    Also asserts the existing invariant: the re-entry frame_id differs
    from the inbound's (so a subsequent fan-out within the same
    invocation doesn't collide with the just-tombstoned key).
    """
    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    inbound_frame_id = "stable-inbound-frame"

    async def drive_once(suffix: str) -> tuple[str, str]:
        """Seed and drive a complete batch, return (inbound_frame_id, re_entry_frame_id)."""
        key = (f"corr-det-{suffix}", f"fan-det-{suffix}")
        state_store._cache[key] = _InFlightBatch(
            correlation_id=key[0],
            fan_out_id=key[1],
            expected_tool_call_ids=frozenset({"t1"}),
            base_state=State(),
            received={},
            started_at_ms=int(time.time() * 1000),
            last_updated_ms=int(time.time() * 1000),
            agent_topic="test_agent.input",
        )
        broker.publish.reset_mock()
        env = _make_envelope(
            key[0],
            state=_state_with_results({"t1": "r1"}),
            frame_id=inbound_frame_id,
        )
        await agent._aggregator_handler(  # type: ignore[attr-defined]
            env,
            correlation_id=key[0],
            headers={"x-calf-fanout-id": key[1]},
            broker=broker,
        )
        agent_topic_publishes = [c for c in broker.publish.await_args_list if c.kwargs.get("topic") == "test_agent.input"]
        assert len(agent_topic_publishes) == 1
        publish_env = agent_topic_publishes[0].args[0]
        return inbound_frame_id, publish_env.internal_workflow_state.current_frame.frame_id

    in_a, out_a = await drive_once("a")
    in_b, out_b = await drive_once("b")

    # Determinism: same inbound frame_id → same re-entry frame_id.
    assert out_a == out_b, (
        f"Re-entry frame_id must be deterministic from inbound frame_id. "
        f"Two runs of the same inbound produced different ids: {out_a!r} vs {out_b!r}. "
        f"Without determinism, NACK redelivery between agent-topic publish and "
        f"tombstone emits distinct re-entry envelopes that downstream dedup can't fold."
    )
    # Distinctness preserved from the pre-existing invariant.
    assert out_a != in_a, (
        "Re-entry frame_id must differ from inbound frame_id; otherwise a "
        "subsequent fan-out within the same invocation collides with the "
        "tombstoned key (silent skip, agent hangs)."
    )


async def test_reentry_logs_parent_chain(
    primed_state_store: tuple[object, MagicMock],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The re-entry path must emit a structured INFO log carrying both the
    new frame_id and the parent frame_id so operators can reconstruct the
    agent-invocation lineage via grep."""
    import logging as _logging

    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    inbound_frame_id = "parent-frame-XYZ"
    key = ("corr-logchain", "fan-logchain")
    state_store._cache[key] = _InFlightBatch(
        correlation_id=key[0],
        fan_out_id=key[1],
        expected_tool_call_ids=frozenset({"t1"}),
        base_state=State(),
        received={},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )

    envelope = _make_envelope(
        key[0],
        state=_state_with_results({"t1": "r1"}),
        frame_id=inbound_frame_id,
    )

    with caplog.at_level(_logging.INFO, logger="calfkit.nodes.agent"):
        await agent._aggregator_handler(  # type: ignore[attr-defined]
            envelope,
            correlation_id=key[0],
            headers={"x-calf-fanout-id": key[1]},
            broker=broker,
        )

    # Find the re-entry log line.
    re_entry_records = [r for r in caplog.records if "agent re-entry" in r.getMessage()]
    assert len(re_entry_records) == 1, (
        f"expected exactly one 'agent re-entry' INFO log line; got {len(re_entry_records)}: {[r.getMessage() for r in caplog.records]}"
    )
    msg = re_entry_records[0].getMessage()
    # Parent frame_id must be present verbatim; new frame_id is derived
    # (SHA-256 truncated to 32 hex chars) so we only assert it's present
    # in some form alongside the parent.
    assert inbound_frame_id in msg, f"re-entry log must include the parent frame_id={inbound_frame_id!r}; got: {msg!r}"
    assert "frame=" in msg and "parent_frame=" in msg, f"re-entry log must include 'frame=' and 'parent_frame=' tokens; got: {msg!r}"


def test_kafka_subscriptions_applies_nack_on_handler_identity(agent: object) -> None:
    """The NACK_ON_ERROR override is applied by matching on the handler
    reference, not by checking topic-list equality. We verify this by
    patching ``BaseNodeDef.kafka_subscriptions`` to return a subscription
    whose ``topics`` differs from ``self.subscribe_topics`` but whose
    ``handler`` is still ``self.handler``. The old equality-on-topics
    check would miss that subscription and silently leave it on the
    (unsafe) default ack policy.
    """
    from unittest.mock import patch

    from faststream import AckPolicy

    from calfkit.nodes.base import BaseNodeDef, _KafkaSubscription

    # Build a base subscription that uses the same handler instance as the
    # agent's main handler but advertises a different topic list. This
    # simulates a future subclass that decorates topics post-hoc, or a
    # framework feature that rewrites the topic list before the agent's
    # ``kafka_subscriptions`` runs.
    def fake_base_kafka_subscriptions(self: BaseNodeDef) -> list[_KafkaSubscription]:
        return [
            _KafkaSubscription(
                topics=["totally.different.topic"],
                handler=self.handler,  # identical reference; equal under bound-method ==
                publish_topic=None,
            )
        ]

    with patch.object(BaseNodeDef, "kafka_subscriptions", fake_base_kafka_subscriptions):
        subs = agent.kafka_subscriptions()  # type: ignore[attr-defined]

    main_subs = [s for s in subs if s.handler == agent.handler]  # type: ignore[attr-defined]
    assert len(main_subs) == 1, f"expected exactly one main-handler subscription; got {len(main_subs)}"
    sub = main_subs[0]
    assert sub.topics == ["totally.different.topic"], (
        "topic list should reflect the (faked) base subscription, proving the match criterion is NOT based on topic-list equality"
    )
    assert sub.ack_policy is AckPolicy.NACK_ON_ERROR, (
        f"main-handler subscription must receive NACK_ON_ERROR via handler-identity match; got {sub.ack_policy!r} on topics={sub.topics!r}"
    )


async def test_aggregator_batch_view_reuses_single_deep_copy(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """A single ``_aggregator_handler`` invocation must produce AT MOST ONE
    framework deep-copy of ``batch.base_state`` (the one shared across the
    up-to-three override calls).

    The previous implementation deep-copied inside every ``_batch_view``
    call site — up to three times per handler invocation. The merge
    contract (``should_complete`` and ``on_partial`` MUST NOT mutate;
    ``merge`` MAY mutate last) makes the shared copy safe.

    A user override may choose to make additional copies of its own (the
    default ``merge`` does so defensively); those are out of scope of this
    test. We isolate the framework's copies by replacing ``merge`` with
    one that does no copy.
    """
    from calfkit.nodes.aggregator.state import AggregatedReturn, AggregatorBatch

    agent, broker = primed_state_store
    state_store = agent.aggregator.runtime.state_store  # type: ignore[attr-defined]

    # User merge that does NOT copy — isolates the framework's deep-copy
    # count from the default merge's own defensive copy.
    async def non_copying_merge(batch: AggregatorBatch) -> AggregatedReturn:
        return AggregatedReturn(state=batch.base_state)

    agent.aggregator.merge = non_copying_merge  # type: ignore[attr-defined,method-assign]

    # Seed a batch that exercises the new-tcid completion path — the
    # worst case for copy count (on_partial + should_complete + merge,
    # plus the post-update view).
    key = ("corr-copies", "fan-copies")
    seeded_state = State()
    state_store._cache[key] = _InFlightBatch(
        correlation_id=key[0],
        fan_out_id=key[1],
        expected_tool_call_ids=frozenset({"t1"}),
        base_state=seeded_state,
        received={},
        started_at_ms=int(time.time() * 1000),
        last_updated_ms=int(time.time() * 1000),
        agent_topic="test_agent.input",
    )

    # Patch ``State.model_copy`` to count deep copies. Only count
    # ``deep=True`` invocations (shallow copies are cheap and used
    # internally by Pydantic for unrelated purposes).
    deep_copy_count = [0]
    original_model_copy = State.model_copy

    def counting_copy(self: State, **kwargs: object) -> State:
        if kwargs.get("deep"):
            deep_copy_count[0] += 1
        return original_model_copy(self, **kwargs)

    State.model_copy = counting_copy  # type: ignore[method-assign,assignment]
    try:
        envelope = _make_envelope(
            key[0],
            state=_state_with_results({"t1": "r1"}),
        )
        await agent._aggregator_handler(  # type: ignore[attr-defined]
            envelope,
            correlation_id=key[0],
            headers={"x-calf-fanout-id": key[1]},
            broker=broker,
        )
    finally:
        State.model_copy = original_model_copy  # type: ignore[method-assign]

    assert deep_copy_count[0] <= 1, (
        f"Expected at most one framework deep copy of base_state per handler invocation; "
        f"got {deep_copy_count[0]}. Per-call-site _batch_view deep-copy regressed."
    )
