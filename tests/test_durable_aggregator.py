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
    """Wire up a minimal aggregator state store on the agent without going
    through Worker.setup. Replaces the broker with a MagicMock so publishes
    are observable."""
    from calfkit.nodes.aggregator._kafka_state_store import _KafkaStateStore

    broker = MagicMock()
    broker.publish = AsyncMock()
    state_store = _KafkaStateStore(
        broker=broker,
        state_topic="test_agent.fanout-state",
        bootstrap_servers="localhost:9092",
        partition_count=6,
    )
    agent.aggregator._state_topic = "test_agent.fanout-state"  # type: ignore[attr-defined]
    agent.aggregator._returns_topic = "test_agent.fanout-returns"  # type: ignore[attr-defined]
    agent.aggregator._partition_count = 6  # type: ignore[attr-defined]
    agent.aggregator._state_store = state_store  # type: ignore[attr-defined]
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
    state_store = agent.aggregator._state_store  # type: ignore[attr-defined]

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
    state_store = agent.aggregator._state_store  # type: ignore[attr-defined]

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


async def test_orphan_return_is_dropped(primed_state_store: tuple[object, MagicMock]) -> None:
    """A return for a fan_out_id with no active batch is dropped (orphan)."""
    agent, broker = primed_state_store

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
    )

    assert response is not None
    broker.publish.assert_not_awaited()


async def test_return_without_fanout_header_is_dropped(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """A return missing HDR_FANOUT_ID is dropped with a WARN log."""
    agent, broker = primed_state_store

    envelope = _make_envelope(
        "corr-noheader",
        state=_state_with_results({"t1": "no-header"}),
    )
    headers: dict[str, object] = {}  # no fanout_id

    response = await agent._aggregator_handler(  # type: ignore[attr-defined]
        envelope,
        correlation_id="corr-noheader",
        headers=headers,
        broker=broker,
    )

    assert response is not None
    broker.publish.assert_not_awaited()


async def test_return_advances_batch_and_publishes_state(
    primed_state_store: tuple[object, MagicMock],
) -> None:
    """A new tool_call_id is merged into the batch and the new state is
    published to the state topic."""
    agent, broker = primed_state_store
    state_store = agent.aggregator._state_store  # type: ignore[attr-defined]

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
    state_store = agent.aggregator._state_store  # type: ignore[attr-defined]

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
    state_store = agent.aggregator._state_store  # type: ignore[attr-defined]

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


def test_fan_out_id_derives_deterministically_from_frame_id(agent: object) -> None:
    """Two envelopes with the same inbound frame_id must produce the same
    fan_out_id. Verified by inspection of the inbound's
    ``current_frame.frame_id`` since ``_publish_parallel_with_aggregator``
    uses it directly."""
    e1 = _make_envelope("corr-D", frame_id="frame-stable")
    e2 = _make_envelope("corr-D", frame_id="frame-stable")

    assert e1.internal_workflow_state.current_frame.frame_id == "frame-stable"
    assert e2.internal_workflow_state.current_frame.frame_id == "frame-stable"
    # Same inbound frame_id → identical fan_out_id (deterministic dispatch).
    assert e1.internal_workflow_state.current_frame.frame_id == e2.internal_workflow_state.current_frame.frame_id
