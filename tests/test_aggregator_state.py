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


def test_fanout_state_received_is_immutable_after_construction() -> None:
    """The ``received`` mapping on a :class:`FanOutState` must be wrapped
    in :class:`MappingProxyType` so the durable wire-format record cannot
    be mutated in place. ``frozen=True`` blocks REASSIGNING the field
    (covered by ``test_fanout_state_is_frozen``); this test guards the
    second half — preventing direct ``state.received["k"] = v`` mutations
    that would otherwise leave the field reference unchanged but corrupt
    the underlying dict.

    Also exercises the deserialisation path: ``model_validate_json`` must
    re-wrap the inner mapping after parsing because the
    ``@model_validator(mode='after')`` runs on both fresh construction
    and on validation-from-JSON.
    """
    state = _make_fanout_state(received={"t1": "r1"})
    assert isinstance(state.received, MappingProxyType)
    with pytest.raises(TypeError):
        state.received["new_key"] = "tampered"  # type: ignore[index]

    # Deserialise from JSON and assert the rehydrated instance is also
    # wrapped. The rehydration call site (state-store ``_apply_record``)
    # depends on this — otherwise an attacker / buggy operator with raw
    # broker access could mutate the in-memory cached batch by reaching
    # into ``state.received``.
    rebuilt = FanOutState.model_validate_json(state.model_dump_json())
    assert isinstance(rebuilt.received, MappingProxyType)
    with pytest.raises(TypeError):
        rebuilt.received["t2"] = "tampered"  # type: ignore[index]


def test_fanout_state_degraded_round_trips_via_json() -> None:
    """The ``degraded`` flag is the durable signal for "this batch's
    completion publish must carry HDR_DEGRADED_MERGE". It MUST survive
    a JSON round-trip so worker-restart rehydration preserves the
    degraded marker; otherwise a process-local set would not survive
    NACK redelivery after a crash.
    """
    degraded_state = _make_fanout_state(degraded=True)
    rebuilt_degraded = FanOutState.model_validate_json(degraded_state.model_dump_json())
    assert rebuilt_degraded.degraded is True

    clean_state = _make_fanout_state()  # default
    rebuilt_clean = FanOutState.model_validate_json(clean_state.model_dump_json())
    assert rebuilt_clean.degraded is False


def test_inflight_batch_degraded_propagates_through_fanout_state() -> None:
    """``_InFlightBatch.degraded`` must convert to/from ``FanOutState.degraded``
    so the durable wire format and the in-memory representation agree.
    ``with_received`` must preserve the flag — the dispatch path sets
    degraded=True once; subsequent merge updates must NOT clear it."""
    batch = _InFlightBatch(
        correlation_id="c",
        fan_out_id="f",
        expected_tool_call_ids=frozenset({"t1"}),
        base_state=State(),
        started_at_ms=1000,
        last_updated_ms=1000,
        agent_topic="agent.in",
        degraded=True,
    )

    wire = batch.to_fanout_state()
    assert wire.degraded is True

    rebuilt = _InFlightBatch.from_fanout_state(wire)
    assert rebuilt.degraded is True

    advanced = batch.with_received({"t1": "r1"}, last_updated_ms=2000)
    assert advanced.degraded is True, (
        "with_received must preserve degraded; otherwise the next state-topic write would silently un-degrade the batch on the first tool return."
    )


# ---------------------------------------------------------------------------
# FanOutState.schema_version wire-format versioning
# ---------------------------------------------------------------------------


def test_schema_version_default_is_one() -> None:
    """Constructing a ``FanOutState`` without specifying ``schema_version``
    must default to ``1`` — current wire format. The default keeps the
    Python-side construction call sites (used by ``_InFlightBatch.to_fanout_state``)
    unchanged while still stamping a version on every published record."""
    state = _make_fanout_state()
    assert state.schema_version == 1


def test_schema_version_roundtrip() -> None:
    """``schema_version`` must survive a JSON round-trip — the durable
    log is the source of truth for partition rehydration, and the
    version is what enables future readers to detect incompatible
    record shapes."""
    state = _make_fanout_state(schema_version=1)
    rebuilt = FanOutState.model_validate_json(state.model_dump_json())
    assert rebuilt.schema_version == 1


def test_schema_version_forward_compat_higher_version_with_unknown_fields() -> None:
    """Forward compatibility: a writer running a future build can stamp
    ``schema_version=99`` and tack on fields this build doesn't know
    about; the current reader must still deserialise the record by
    dropping the unknown fields (``extra="ignore"``) and preserving the
    higher version marker so any later consumer can re-detect the
    wire-format generation."""
    raw = (
        '{"schema_version": 99, "correlation_id": "c", "fan_out_id": "f", '
        '"expected_tool_call_ids": ["t1"], "received": {}, '
        '"base_state": {}, "started_at_ms": 1000, "last_updated_ms": 1000, '
        '"agent_topic": "agent.in", '
        '"future_field_a": "some new value", "future_field_b": 42}'
    )
    state = FanOutState.model_validate_json(raw)
    assert state.schema_version == 99
    assert state.correlation_id == "c"
    # Unknown fields are dropped (extra="ignore") — they are not exposed
    # as attributes on the parsed instance.
    assert not hasattr(state, "future_field_a")
    assert not hasattr(state, "future_field_b")


def test_schema_version_rejects_below_minimum() -> None:
    """Backward incompatibility: a record stamped with a version below
    ``_MIN_SUPPORTED_SCHEMA_VERSION`` must raise ``ValidationError`` at
    deserialisation time. The rehydration callsite treats this as a
    partition-activation failure rather than silently dropping the
    record, so an operator gets a loud failure instead of in-flight
    batches vanishing on rehydrate."""
    payload = {
        "schema_version": 0,
        "correlation_id": "c",
        "fan_out_id": "f",
        "expected_tool_call_ids": ["t1"],
        "received": {},
        "base_state": {},
        "started_at_ms": 1000,
        "last_updated_ms": 1000,
        "agent_topic": "agent.in",
    }
    with pytest.raises(pydantic.ValidationError) as exc_info:
        FanOutState.model_validate(payload)
    # The observed version must appear in the error so operators can
    # diagnose the producer mismatch quickly.
    assert "schema_version=0" in str(exc_info.value)


def test_schema_version_legacy_records_default_to_one() -> None:
    """Pre-versioning legacy records: a record written by older code
    before ``schema_version`` existed will not carry the field. The
    field MUST default to ``1`` (not be required) so those records
    deserialise cleanly during rehydration — a required-field design
    would treat every legacy record as a ValidationError and abort
    partition activation, which would be a worse data-loss bug than
    the one this versioning scheme was introduced to prevent."""
    legacy_payload = {
        # Note: no "schema_version" key — simulates a record written by
        # a build that predates this field.
        "correlation_id": "c",
        "fan_out_id": "f",
        "expected_tool_call_ids": ["t1"],
        "received": {},
        "base_state": {},
        "started_at_ms": 1000,
        "last_updated_ms": 1000,
        "agent_topic": "agent.in",
    }
    state = FanOutState.model_validate(legacy_payload)
    assert state.schema_version == 1
