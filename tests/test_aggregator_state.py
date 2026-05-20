"""Tests for AggregatorBatch immutability and ToolCallId propagation."""

from __future__ import annotations

import copy
import dataclasses
from types import MappingProxyType

import pytest

from calfkit.models.state import State
from calfkit.nodes.aggregator.state import AggregatorBatch


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
