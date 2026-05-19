"""Wire format and public-view types for the durable fan-out aggregator.

The compacted ``{node_id}.fanout-state`` topic stores one record per in-flight
batch, keyed by ``(correlation_id, fan_out_id)``. The value is a serialised
:class:`FanOutState`.

User code never sees ``FanOutState`` directly — :class:`FanOutAggregator`
overrides receive an immutable :class:`AggregatorBatch` view, and the
completion path returns an :class:`AggregatedReturn`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, NewType

from pydantic import BaseModel, ConfigDict, Field

from calfkit.models.state import State

ToolCallId = NewType("ToolCallId", str)
"""Type alias for the LLM-assigned identifier of a single tool call.

Distinct from ``correlation_id`` (per logical agent run) and ``fan_out_id``
(per parallel-fan-out batch within a run); together they form the dedup
triple ``(correlation_id, fan_out_id, tool_call_id)``.
"""


class FanOutState(BaseModel):
    """Durable wire-format record for an in-flight fan-out batch.

    Stored on the ``{node_id}.fanout-state`` compacted topic, one record per
    ``(correlation_id, fan_out_id)`` key. Tombstoned (``value=None``) when the
    batch completes; the tombstone is retained for ``delete.retention.ms``
    long enough to disambiguate "tombstoned" from "never existed" during a
    rebalance window.

    Uses plain ``BaseModel`` (not ``CompactBaseModel``) because every field
    is load-bearing and we don't want ``exclude_unset`` / ``exclude_none``
    serialisation gotchas in a durable wire format.
    """

    model_config = ConfigDict(extra="ignore")

    correlation_id: str
    """The logical run's correlation_id. Same as the inbound message's correlation_id."""

    fan_out_id: str
    """Stable identifier for this fan-out batch. Derived deterministically from
    the agent's inbound :attr:`CallFrame.frame_id` so redelivered inbounds
    produce the same fan-out_id (idempotent dispatch)."""

    expected_tool_call_ids: frozenset[str]
    """The set of tool_call_ids the agent dispatched for this batch."""

    received: dict[str, Any] = Field(default_factory=dict)
    """Map of tool_call_id → tool result, accumulated as tool returns arrive."""

    base_state: State
    """Snapshot of :class:`State` at dispatch time, before any results arrived.
    The completion path merges :attr:`received` into a copy of this state."""

    started_at_ms: int
    """Monotonic millisecond timestamp at dispatch time. For lag metrics."""

    last_updated_ms: int
    """Monotonic millisecond timestamp of the most recent ``received`` mutation.
    Used by idle-timeout reaping and by observability."""

    agent_topic: str
    """The agent's main input topic. The aggregated ``AggregatedReturn`` is
    published here on batch completion (so the agent re-enters with the
    merged state)."""

    traceparent: str | None = None
    """W3C OTel ``traceparent`` header captured at dispatch time. Propagated
    through the aggregator so the entire fan-out lives in one trace."""

    tracestate: str | None = None
    """W3C OTel ``tracestate`` header captured at dispatch time."""


@dataclass(frozen=True)
class AggregatorBatch:
    """Immutable view of an in-flight batch, passed to
    :class:`FanOutAggregator` override methods.

    User code reads fields directly. Convenience properties (
    :attr:`num_received`, :attr:`missing_tool_call_ids`, etc.) cover the
    common decision shapes (count-based completion, "is the slow tool the
    one we're waiting on", etc.) without exposing the internal cache.
    """

    correlation_id: str
    fan_out_id: str
    expected_tool_call_ids: frozenset[str]
    received: dict[str, Any]
    base_state: State
    started_at_ms: int
    last_updated_ms: int

    @property
    def num_expected(self) -> int:
        return len(self.expected_tool_call_ids)

    @property
    def num_received(self) -> int:
        return len(self.received)

    @property
    def is_complete_by_count(self) -> bool:
        """True when all expected tool_call_ids have results in ``received``."""
        return self.expected_tool_call_ids <= frozenset(self.received.keys())

    @property
    def missing_tool_call_ids(self) -> frozenset[str]:
        """Tool_call_ids that were dispatched but haven't returned yet."""
        return self.expected_tool_call_ids - frozenset(self.received.keys())


@dataclass(frozen=True)
class AggregatedReturn:
    """Result of :meth:`FanOutAggregator.merge`.

    The framework publishes ``state`` back to the agent's main topic as a
    ``ReturnCall``, so the agent re-enters with the merged state.
    """

    state: State
    """The merged state with all tool results (or a custom transformation
    thereof) applied. Sent to the agent's main topic as the aggregated
    return."""
