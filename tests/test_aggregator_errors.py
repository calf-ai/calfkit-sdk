"""Tests for the structured exception context introduced in B9.

The aggregator's exception types now carry typed attributes (correlation_id,
fan_out_id, state_topic) so callers can act programmatically on the
exception without re-parsing the message string. A Sentry / Statsig
handler can group failures by run or batch; an operator dashboard can
filter by affected state topic.
"""

from __future__ import annotations

import pytest

from calfkit.exceptions import CalfkitError
from calfkit.nodes.aggregator.errors import (
    AggregatorError,
    AggregatorMergeError,
    AggregatorStateStoreError,
)


def test_merge_error_carries_correlation_and_fan_out_ids() -> None:
    """AggregatorMergeError must expose correlation_id and fan_out_id as
    typed attributes so a downstream handler can route / dedupe failures
    by run identity."""
    err = AggregatorMergeError(
        "merge() raised",
        correlation_id="corr-abc",
        fan_out_id="fan-xyz",
    )
    assert err.correlation_id == "corr-abc"
    assert err.fan_out_id == "fan-xyz"
    assert "merge() raised" in str(err)


def test_merge_error_attributes_default_to_none() -> None:
    """Constructing with just a message must still work; the attributes
    default to None for callers that don't have batch context (or for
    historical raise sites that haven't been updated)."""
    err = AggregatorMergeError("generic boom")
    assert err.correlation_id is None
    assert err.fan_out_id is None
    assert err.state_topic is None


def test_aggregator_merge_error_carries_state_topic() -> None:
    """AggregatorMergeError carries state_topic so multi-agent operators
    alerting on merge failures across multiple agents in the same worker
    can attribute the failure to the right agent's aggregator."""
    err = AggregatorMergeError(
        "merge() raised",
        correlation_id="corr-abc",
        fan_out_id="fan-xyz",
        state_topic="agent.fanout-state",
    )
    assert err.state_topic == "agent.fanout-state"
    assert err.correlation_id == "corr-abc"
    assert err.fan_out_id == "fan-xyz"


def test_state_store_error_carries_state_topic() -> None:
    """AggregatorStateStoreError carries the affected state topic so a
    multi-agent worker can attribute the failure to the right agent."""
    err = AggregatorStateStoreError(
        "broker stalled",
        state_topic="planner.fanout-state",
    )
    assert err.state_topic == "planner.fanout-state"


def test_aggregator_errors_inherit_from_calfkit_base() -> None:
    """All aggregator errors must be catchable via the CalfkitError base —
    the SDK contract for users wanting a single except clause for all
    framework-raised exceptions."""
    assert issubclass(AggregatorError, CalfkitError)
    assert issubclass(AggregatorMergeError, AggregatorError)
    assert issubclass(AggregatorStateStoreError, AggregatorError)

    with pytest.raises(CalfkitError):
        raise AggregatorMergeError("from merge")
    with pytest.raises(CalfkitError):
        raise AggregatorStateStoreError("from store")
