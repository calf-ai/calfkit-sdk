"""Tests for AggregatorBatch immutability and ToolCallId propagation."""

from __future__ import annotations

import pytest

from calfkit.models.state import State
from calfkit.nodes.aggregator.state import AggregatorBatch


def test_aggregator_batch_received_is_immutable() -> None:
    """The 'immutable view' claim must be enforced — users cannot
    mutate batch.received from inside a merge/should_complete/on_partial
    override and corrupt the framework's in-flight state."""
    batch = AggregatorBatch(
        correlation_id="c",
        fan_out_id="f",
        expected_tool_call_ids=frozenset({"t1"}),
        received={"t1": "result"},
        base_state=State(),
        started_at_ms=0,
        last_updated_ms=0,
    )
    # mypy/pyright already reject this assignment (Mapping is not MutableMapping),
    # but the runtime must also reject so a `# type: ignore` user can't get past it.
    with pytest.raises(TypeError):
        batch.received["t1"] = "tampered"  # type: ignore[index]
