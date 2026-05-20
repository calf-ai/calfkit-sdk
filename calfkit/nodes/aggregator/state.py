"""Wire format and public-view types for the durable fan-out aggregator.

The compacted ``{node_id}.fanout-state`` topic stores one record per in-flight
batch, keyed by ``(correlation_id, fan_out_id)``. The value is a serialised
:class:`FanOutState`.

User code never sees ``FanOutState`` directly — :class:`FanOutAggregator`
overrides receive an immutable :class:`AggregatorBatch` view, and the
completion path returns an :class:`AggregatedReturn`.

Tool call IDs (the LLM-assigned identifier of a single tool invocation) are
plain ``str`` throughout. Conceptually distinct from ``correlation_id`` (per
logical agent run) and ``fan_out_id`` (per parallel-fan-out batch); together
they form the dedup triple ``(correlation_id, fan_out_id, tool_call_id)``.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_serializer, model_validator

from calfkit.models.state import State

_MIN_SUPPORTED_SCHEMA_VERSION = 1
"""The lowest :attr:`FanOutState.schema_version` this build will deserialise.

Framework-internal (leading underscore) — not a tuning knob for end users.
See :class:`FanOutState` docstring for the rollout protocol when bumping
this value."""


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

    Frozen so a parsed wire record cannot drift from what was published —
    the aggregator's invariant is "publish then update cache" and any
    in-place tweak to a deserialised state would violate that.

    Wire-format versioning
    ----------------------
    :attr:`schema_version` (default ``1``) is the explicit version marker
    for this record's wire format. Two compatibility rules apply:

    * **Forward compatibility (higher writer / lower reader).** Receivers
      tolerate records with a ``schema_version`` greater than they know
      about, plus any unknown extra fields, via ``extra="ignore"``. A
      newer writer that adds optional fields can publish to a topic read
      by older deployments without breaking deserialisation.
    * **Backward compatibility (lower writer / higher reader).** Receivers
      MUST refuse to deserialise records with
      ``schema_version < _MIN_SUPPORTED_SCHEMA_VERSION``. The
      ``@model_validator`` below enforces this — the resulting
      ``ValidationError`` surfaces at the rehydration call site (state
      store ``_apply_record``), aborting partition activation rather than
      silently corrupting in-flight batches.

    Rollout protocol for adding a new REQUIRED field
    ------------------------------------------------
    Adding a required field is a true wire-format break. Stage it across
    two releases so old readers stay compatible during the overlap:

    1. **Release N (writer bump only).** Add the new field with a default
       so old records still deserialise. Bump the writer to publish the
       new field. Leave ``_MIN_SUPPORTED_SCHEMA_VERSION`` at the OLD
       value so deployments still running the previous build can read
       records produced by the new writer. Bump
       :attr:`schema_version`'s default to the new value (e.g. ``2``).
    2. **Release N+1 (reader cutover).** Once all writers in the fleet
       are on release N or later, bump
       ``_MIN_SUPPORTED_SCHEMA_VERSION`` to the new value. From here on,
       readers refuse pre-N records — safe because no writer is still
       producing them.

    A no-op schema bump (e.g. renaming a field with backward-compatible
    aliasing) does not need this dance; just bump the default version.
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

    schema_version: int = 1
    """Wire-format version for this record. Defaults to ``1``.

    Defaulted (not required) so legacy records written before this field
    existed deserialise cleanly during rehydration — Pydantic would treat
    a missing required field as a ``ValidationError`` and abort partition
    activation, which would be a worse outcome than tolerating an
    implicit ``schema_version=1``. See the class docstring for the
    forward/backward compatibility contract and rollout protocol."""

    correlation_id: str
    """The logical run's correlation_id. Same as the inbound message's correlation_id."""

    fan_out_id: str
    """Stable identifier for this fan-out batch. Derived deterministically from
    the agent's inbound :attr:`CallFrame.frame_id` so redelivered inbounds
    produce the same fan-out_id (idempotent dispatch)."""

    expected_tool_call_ids: frozenset[str]
    """The set of tool_call_ids the agent dispatched for this batch."""

    received: Mapping[str, Any] = Field(default_factory=dict)
    """Map of tool_call_id → tool result, accumulated as tool returns arrive.

    Annotated as :class:`collections.abc.Mapping` (covariant, read-only)
    rather than ``dict`` because the runtime value after construction is
    a :class:`types.MappingProxyType` — annotating as ``dict`` would
    silently lie to mypy and let access-site mutations like
    ``state.received[k] = v`` pass type-checking even though they fail at
    runtime. The ``Mapping`` annotation makes the read-only contract
    statically enforceable.

    Pydantic's ``frozen=True`` prevents reassigning the field to a
    different mapping. After model construction (and after deserialisation
    via ``model_validate`` / ``model_validate_json``) the inner mapping
    is wrapped in :class:`types.MappingProxyType` by the
    ``_wrap_received_immutable`` validator below, so it cannot be mutated
    by accident — each update writes a NEW :class:`FanOutState` via
    :class:`_InFlightBatch.with_received`."""

    base_state: State
    """Snapshot of :class:`State` at dispatch time, before any results arrived.
    The completion path merges :attr:`received` into a copy of this state."""

    started_at_ms: int
    """Wall-clock millisecond timestamp (epoch ms) at dispatch time.

    Wall-clock — not monotonic — so timestamps remain comparable across
    processes (producer, aggregator, and any consumer reading the
    durable log). Used for lag and batch-age metrics."""

    last_updated_ms: int
    """Wall-clock millisecond timestamp (epoch ms) of the most recent
    ``received`` mutation. Same cross-process comparability rationale as
    :attr:`started_at_ms`. Recorded for observability (lag, batch-age
    metrics)."""

    agent_topic: str
    """The agent's main input topic. The aggregated ``AggregatedReturn`` is
    published here on batch completion (so the agent re-enters with the
    merged state)."""

    degraded: bool = False
    """True when this batch was marked degraded at dispatch time (currently
    set on the drift-overwrite path that discards prior received results).
    Survives rehydration: the completion publish stamps
    :data:`HDR_DEGRADED_MERGE` if either this OR
    :attr:`AggregatedReturn.degraded` is True, so a worker restart between
    dispatch and completion cannot lose the degraded signal."""

    traceparent: str | None = None
    """W3C OTel ``traceparent`` header captured at dispatch time. Propagated
    through the aggregator so the entire fan-out lives in one trace."""

    tracestate: str | None = None
    """W3C OTel ``tracestate`` header captured at dispatch time."""

    def __deepcopy__(self, memo: dict[int, Any] | None = None) -> FanOutState:
        # MappingProxyType isn't supported by Pydantic's default deepcopy
        # path (raises ``TypeError: cannot ... 'mappingproxy' object``).
        # Without this override ``copy.deepcopy(fs)`` AND ``fs.model_copy
        # (deep=True)`` both crash — the latter is the production rebalance /
        # restart-safety path. Mirrors :meth:`AggregatorBatch.__deepcopy__`:
        # unwrap the proxy, deep-copy each field, construct a fresh
        # ``FanOutState``. The ``@model_validator(mode="after")`` re-wraps
        # ``received`` on the new instance so immutability is preserved.
        #
        # ``memo`` defaults to ``None`` because Pydantic's
        # ``BaseModel.model_copy(deep=True)`` invokes ``__deepcopy__()``
        # with no argument; the stdlib ``copy.deepcopy()`` always passes a
        # memo dict. Either path must work.
        if memo is None:
            memo = {}
        return FanOutState(
            schema_version=self.schema_version,
            correlation_id=self.correlation_id,
            fan_out_id=self.fan_out_id,
            expected_tool_call_ids=self.expected_tool_call_ids,
            received=deepcopy(dict(self.received), memo),
            base_state=deepcopy(self.base_state, memo),
            started_at_ms=self.started_at_ms,
            last_updated_ms=self.last_updated_ms,
            agent_topic=self.agent_topic,
            degraded=self.degraded,
            traceparent=self.traceparent,
            tracestate=self.tracestate,
        )

    @model_validator(mode="after")
    def _wrap_received_immutable(self) -> FanOutState:
        """Wrap :attr:`received` in :class:`MappingProxyType` after
        construction so the inner mapping cannot be mutated.

        Mirrors the pattern in :meth:`AggregatorBatch.__post_init__`. The
        model is frozen (``model_config.frozen=True``) so direct assignment
        would raise; ``object.__setattr__`` bypasses the frozen guard the
        same way Pydantic's own ``__init__`` does. Runs on both fresh
        construction and on ``model_validate`` / ``model_validate_json``
        because Pydantic invokes ``@model_validator(mode="after")``
        validators in both paths.
        """
        if not isinstance(self.received, MappingProxyType):
            # Defensive ``dict(...)`` copy first: the caller may have
            # passed a reference they continue to mutate, and we want the
            # proxy to view our own snapshot, not theirs.
            object.__setattr__(self, "received", MappingProxyType(dict(self.received)))
        return self

    @field_serializer("received")
    def _serialize_received(self, received: Mapping[str, Any]) -> dict[str, Any]:
        """Render :attr:`received` as a plain ``dict`` on serialisation.

        Pydantic's JSON serialiser does not natively support
        :class:`MappingProxyType` (raises ``PydanticSerializationError:
        Unable to serialize unknown type: mappingproxy``). The validator
        above wraps the field in a proxy for runtime immutability; this
        serializer unwraps it on the way out so ``model_dump`` /
        ``model_dump_json`` produce a normal dict the durable log can
        carry. The round-trip via ``model_validate_json`` re-wraps it
        because the validator runs again on deserialise.
        """
        return dict(received)

    @model_validator(mode="after")
    def _enforce_min_schema_version(self) -> FanOutState:
        """Refuse to deserialise records older than this build supports.

        Raises ``ValueError`` (wrapped by Pydantic into ``ValidationError``)
        when ``schema_version < _MIN_SUPPORTED_SCHEMA_VERSION``. The
        rehydration path treats this as a fatal partition-activation
        failure rather than silently dropping the record — see the
        ``_KafkaStateStore._apply_record`` callsite for handling.
        """
        if self.schema_version < _MIN_SUPPORTED_SCHEMA_VERSION:
            raise ValueError(
                f"FanOutState schema_version={self.schema_version} is below the "
                f"minimum supported version {_MIN_SUPPORTED_SCHEMA_VERSION}; "
                "this record was written by a build older than the current "
                "reader's compatibility window."
            )
        return self


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
    received: Mapping[str, Any]
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

    degraded: bool = False
    """``True`` when this result came from the
    :data:`MergeErrorPolicy.FALLBACK_TO_DEFAULT` fallback path (user's
    :meth:`merge` raised; framework fell back to the default merge). The
    framework stamps :data:`HDR_DEGRADED_MERGE` on the published envelope
    so operators can detect silently-degraded batches; users overriding
    :meth:`merge` and returning ``AggregatedReturn`` directly can also
    set this flag to signal a known-degraded result."""
