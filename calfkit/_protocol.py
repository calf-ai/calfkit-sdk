"""Wire-protocol constants shared across the SDK.

Lives at the package root with no internal imports so any layer — client,
worker, nodes — can depend on it without creating circular import chains.

Do not add imports from ``calfkit.*`` to this module.
"""

from typing import Any, Literal

NodeKind = Literal["node", "agent", "tool", "client", "consumer"]
"""Closed value space for the ``x-calf-emitter-kind`` Kafka header. Subclasses of
``BaseNodeDef`` declare their kind via ``_node_kind: ClassVar[NodeKind]``; the
client publishes with ``CLIENT_KIND`` directly.
"""

CLIENT_KIND: NodeKind = "client"
"""``NodeKind`` value the client stamps on every outbound publish (it isn't a
node subclass, so it has no ``_node_kind`` ClassVar)."""

HDR_EMITTER = "x-calf-emitter"
"""Kafka header carrying the emitting node's id on every calfkit-published message."""

HDR_EMITTER_KIND = "x-calf-emitter-kind"
"""Kafka header carrying the emitting node's :data:`NodeKind`."""

HDR_FRAME_ID = "x-calf-frame-id"
"""Kafka header carrying the ``CallFrame.frame_id`` of the message's current invocation frame.

Stable cross-hop identifier (uuid7); useful as a dedup key for at-least-once
delivery and for downstream consumers that need to correlate events without
parsing the envelope body.
"""

HDR_FANOUT_ID = "x-calf-fanout-id"
"""Kafka header carrying the fan-out batch id for messages dispatched as part of
a parallel tool fan-out.

Derived deterministically from the agent's inbound ``frame_id`` so redelivered
inbounds produce the same fan-out id (idempotent dispatch). Receivers use it
together with ``correlation_id`` and ``tool_call_id`` as the dedup triple for
durable aggregation.
"""

HDR_DEGRADED_MERGE = "x-calf-degraded-merge"
"""Kafka header stamped on the aggregated fan-out return when the batch's
completion is known to be degraded.

Value is the string ``"1"`` when set. Stamped on two distinct paths:

1. **User merge raised under FALLBACK_TO_DEFAULT.** The user's custom
   :meth:`FanOutAggregator.merge` raised and ``merge_error_policy`` is
   :data:`MergeErrorPolicy.FALLBACK_TO_DEFAULT`; the framework fell back
   to the default merge. Signalled via ``AggregatedReturn.degraded=True``
   on the fallback return (process-local; computed at completion time).
2. **Batch overwritten on drift detection.** A redelivered inbound
   dispatched a different ``expected_tool_call_ids`` set than the durable
   cached batch; the framework overwrites the stale state, silently
   discarding prior received results. Signalled via
   ``FanOutState.degraded=True`` set at overwrite time and persisted to
   the compacted state log — survives worker restart and NACK redelivery
   so the completion publish stamps this header even on a rehydrated
   batch.

Operators / observability tooling filter on this header to surface batches
whose downstream state may be incomplete relative to the user's intent.
"""


def decode_header_str(value: Any) -> str | None:
    """Coerce an inbound Kafka header value to ``str | None``.

    Handles bytes (utf-8 with replacement on invalid sequences), str (passthrough),
    and any other type (coerced to ``None``). Does not log — callers warn when the
    missing/bad value matters in their context.
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value
    return None
