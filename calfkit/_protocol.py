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
