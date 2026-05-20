"""Wire format and public-view types for the durable fan-out aggregator.

The compacted ``{node_id}.fanout-state`` topic stores one record per in-flight
batch, keyed by ``(correlation_id, fan_out_id)``. The value is a serialised
:class:`FanOutState`.

User code never sees ``FanOutState`` directly — :class:`FanOutAggregator`
overrides receive an immutable :class:`AggregatorBatch` view, and the
completion path returns an :class:`AggregatedReturn`.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from types import MappingProxyType
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

    expected_tool_call_ids: frozenset[ToolCallId]
    """The set of tool_call_ids the agent dispatched for this batch."""

    received: dict[ToolCallId, Any] = Field(default_factory=dict)
    """Map of tool_call_id → tool result, accumulated as tool returns arrive."""

    base_state: State
    """Snapshot of :class:`State` at dispatch time, before any results arrived.
    The completion path merges :attr:`received` into a copy of this state."""

    started_at_ms: int
    """Monotonic millisecond timestamp at dispatch time. For lag metrics."""

    last_updated_ms: int
    """Monotonic millisecond timestamp of the most recent ``received`` mutation.
    Recorded for observability (lag, batch-age metrics)."""

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
    expected_tool_call_ids: frozenset[ToolCallId]
    received: Mapping[ToolCallId, Any]
    base_state: State
    started_at_ms: int
    last_updated_ms: int

    def __post_init__(self) -> None:
        # Enforce the "immutable view" contract regardless of caller.
        # ``object.__setattr__`` is needed because the dataclass is frozen.
        if not isinstance(self.received, MappingProxyType):
            object.__setattr__(self, "received", MappingProxyType(dict(self.received)))

    def __deepcopy__(self, memo: dict[int, Any]) -> AggregatorBatch:
        # MappingProxyType doesn't survive ``copy.deepcopy`` via the default
        # mechanism (raises ``TypeError: cannot pickle 'mappingproxy'``).
        # Users defensively snapshotting the view with ``copy.deepcopy(batch)``
        # would otherwise crash. Re-wrap a deep copy of the underlying dict;
        # the new instance goes through ``__post_init__`` and reinstates
        # the MappingProxyType.
        return AggregatorBatch(
            correlation_id=self.correlation_id,
            fan_out_id=self.fan_out_id,
            expected_tool_call_ids=self.expected_tool_call_ids,
            received=deepcopy(dict(self.received), memo),
            base_state=deepcopy(self.base_state, memo),
            started_at_ms=self.started_at_ms,
            last_updated_ms=self.last_updated_ms,
        )

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
    def missing_tool_call_ids(self) -> frozenset[ToolCallId]:
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

    degraded: bool = False
    """``True`` when this result came from the
    :data:`MergeErrorPolicy.DROP` fallback path (user's :meth:`merge`
    raised; framework fell back to the default merge). The framework
    stamps :data:`HDR_DEGRADED_MERGE` on the published envelope so
    operators can detect silently-degraded batches; users overriding
    :meth:`merge` and returning ``AggregatedReturn`` directly can also
    set this flag to signal a known-degraded result."""
