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
"""Kafka header stamped on the aggregated fan-out return when the user's
custom :meth:`FanOutAggregator.merge` raised and the configured
``merge_error_policy`` is :data:`MergeErrorPolicy.FALLBACK_TO_DEFAULT`.

Value is the string ``"1"`` when set. Operators / observability tooling can
filter on this header to surface fan-out batches whose downstream state was
produced by the framework's default merge instead of the user's overridden
behaviour — without this signal the FALLBACK_TO_DEFAULT policy silently
delivers potentially-incomplete state to the agent.
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
