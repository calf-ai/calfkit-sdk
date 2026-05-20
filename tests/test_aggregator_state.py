"""Tests for AggregatorBatch immutability and tool_call_id propagation."""

from __future__ import annotations

import copy
import dataclasses
from types import MappingProxyType

import pydantic
import pytest

from calfkit.models.state import State
from calfkit.nodes.aggregator._in_memory_store import _InFlightBatch
from calfkit.nodes.aggregator.state import AggregatorBatch, FanOutState


def _make_batch(received: dict[str, object] | None = None) -> AggregatorBatch:
    return AggregatorBatch(
        correlation_id="c",
        fan_out_id="f",
        expected_tool_call_ids=frozenset({"t1"}),
        received=received if received is not None else {"t1": "result"},
        base_state=State(),
        started_at_ms=0,
        last_updated_ms=0,
    )


def test_aggregator_batch_received_is_immutable() -> None:
    """The 'immutable view' claim must be enforced — users cannot
    mutate batch.received from inside a merge/should_complete/on_partial
    override and corrupt the framework's in-flight state."""
    batch = _make_batch()
    # mypy/pyright already reject this assignment (Mapping is not MutableMapping),
    # but the runtime must also reject so a `# type: ignore` user can't get past it.
    with pytest.raises(TypeError):
        batch.received["t1"] = "tampered"  # type: ignore[index]


def test_aggregator_batch_deepcopy_does_not_raise_on_mappingproxy() -> None:
    """``copy.deepcopy(batch)`` is a natural defensive idiom for users
    snapshotting the view inside a merge override. Without an explicit
    ``__deepcopy__``, the default deepcopy crashes on the inner
    MappingProxyType (which has no native deepcopy support). The
    override must return a fresh AggregatorBatch with a fresh
    immutable view."""
    batch = _make_batch(received={"t1": ["result-list"]})  # nested mutable

    cloned = copy.deepcopy(batch)

    assert cloned is not batch
    assert cloned.received == {"t1": ["result-list"]}
    # Immutability preserved on the clone.
    assert isinstance(cloned.received, MappingProxyType)
    with pytest.raises(TypeError):
        cloned.received["t1"] = "tampered"  # type: ignore[index]
    # Nested values are deep-copied (not shared) — mutating the clone
    # does not affect the original.
    cloned.received["t1"].append("extra")  # type: ignore[attr-defined]
    assert batch.received["t1"] == ["result-list"]


def test_aggregator_batch_dataclasses_replace_preserves_immutability() -> None:
    """``dataclasses.replace`` runs ``__post_init__`` on the new instance,
    so the replacement's ``received`` must end up wrapped regardless of
    the dict passed in."""
    batch = _make_batch()
    new_batch = dataclasses.replace(batch, received={"t1": "r2"})

    assert isinstance(new_batch.received, MappingProxyType)
    with pytest.raises(TypeError):
        new_batch.received["t1"] = "tampered"  # type: ignore[index]


# ---------------------------------------------------------------------------
# _InFlightBatch frozen-dataclass invariants
# ---------------------------------------------------------------------------


def _make_inflight(received: dict[str, object] | None = None) -> _InFlightBatch:
    return _InFlightBatch(
        correlation_id="c",
        fan_out_id="f",
        expected_tool_call_ids=frozenset({"t1"}),
        base_state=State(),
        started_at_ms=1000,
        last_updated_ms=1000,
        agent_topic="agent.in",
        received=received if received is not None else {},
    )


def test_inflight_batch_is_frozen() -> None:
    """The aggregator's correctness story hinges on 'publish then update
    cache' with no in-place mutation. Freezing the dataclass enforces
    that invariant in the type system: reassigning a field raises."""
    batch = _make_inflight()
    with pytest.raises(dataclasses.FrozenInstanceError):
        batch.agent_topic = "other.in"  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        batch.last_updated_ms = 2000  # type: ignore[misc]


def test_inflight_batch_received_immutable_via_with_received() -> None:
    """``with_received`` is the only sanctioned way to evolve ``received``.
    The returned instance must be a fresh batch; the original's ``received``
    is unchanged."""
    original = _make_inflight()
    updated = original.with_received({"t1": "r1"}, last_updated_ms=2000)

    assert updated is not original
    assert updated.received == {"t1": "r1"}
    assert original.received == {}
    assert updated.last_updated_ms == 2000
    assert original.last_updated_ms == 1000
    # The updated instance's view is also immutable.
    with pytest.raises(TypeError):
        updated.received["t1"] = "tampered"  # type: ignore[index]


def test_inflight_batch_received_field_is_mapping_proxy() -> None:
    """Even when constructed with a plain dict, ``__post_init__`` must
    wrap ``received`` in a MappingProxyType so direct callers can't mutate
    the cached batch's results."""
    batch = _make_inflight(received={"t1": "r1"})
    assert isinstance(batch.received, MappingProxyType)
    with pytest.raises(TypeError):
        batch.received["t2"] = "tampered"  # type: ignore[index]


def test_inflight_batch_requires_all_fields() -> None:
    """Sentinel defaults (``agent_topic=""``, ``started_at_ms=0``,
    ``last_updated_ms=0``) are gone — constructing without them must
    raise so meaningless instances cannot reach the durable log."""
    with pytest.raises(TypeError):
        _InFlightBatch(correlation_id="x")  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        # Missing agent_topic / started_at_ms / last_updated_ms.
        _InFlightBatch(  # type: ignore[call-arg]
            correlation_id="x",
            fan_out_id="y",
            expected_tool_call_ids=frozenset(),
            base_state=State(),
        )


# ---------------------------------------------------------------------------
# FanOutState frozen invariants
# ---------------------------------------------------------------------------


def _make_fanout_state(**overrides: object) -> FanOutState:
    fields: dict[str, object] = {
        "correlation_id": "c",
        "fan_out_id": "f",
        "expected_tool_call_ids": frozenset({"t1"}),
        "received": {},
        "base_state": State(),
        "started_at_ms": 1000,
        "last_updated_ms": 1000,
        "agent_topic": "agent.in",
    }
    fields.update(overrides)
    return FanOutState.model_validate(fields)


def test_fanout_state_is_frozen() -> None:
    """The durable wire-format record must be frozen so a deserialised
    instance cannot drift from what was published."""
    state = _make_fanout_state()
    with pytest.raises(pydantic.ValidationError):
        state.agent_topic = "other.in"  # type: ignore[misc]
    with pytest.raises(pydantic.ValidationError):
        state.received = {"t1": "tampered"}  # type: ignore[misc]


def test_fanout_state_deserialises_round_trip() -> None:
    """Frozen=True must not break ``model_validate_json``: the durable
    log is parsed on rehydration and the type is load-bearing."""
    state = _make_fanout_state(received={"t1": "r1"})
    raw = state.model_dump_json()

    rebuilt = FanOutState.model_validate_json(raw)
    assert rebuilt.received == {"t1": "r1"}
    assert rebuilt.correlation_id == "c"
    assert rebuilt.agent_topic == "agent.in"
