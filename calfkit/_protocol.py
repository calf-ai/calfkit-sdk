"""Wire-protocol constants shared across the SDK.

Lives at the package root with no internal imports so any layer — client,
worker, nodes — can depend on it without creating circular import chains.

Do not add imports from ``calfkit.*`` to this module.
"""

import re
from typing import Any, Literal

NodeKind = Literal["node", "agent", "tool", "client", "consumer", "toolbox"]
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

HDR_ROUTE = "x-calf-route"
"""Kafka header carrying the concrete route key used for header route dispatch.

Ingress-only: set by a client invocation or an explicit peer ``Call(route=...)``;
never auto-propagated across control-flow republishes. A node with registered
``@handler`` routes dispatches on this header (see ``BaseNodeDef.handler``)."""

HDR_KIND = "x-calf-kind"
"""Kafka header classifying every calfkit delivery (spec §4.1). Framework-stamped
on every publish, never user-set. ``call`` = "do work" (Call / fan-out / TailCall /
client send); ``return`` = "your call resolved" (a ``ReturnCall`` reply). PR-C adds
``fault``. A missing header reads as ``call`` (raw-producer ingress norm)."""

MessageKind = Literal["call", "return"]
"""Closed value space for :data:`HDR_KIND` in PR-A. PR-C widens it with ``fault``."""


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


# Kafka topic-name legality: charset [a-zA-Z0-9._-], 1-249 chars, with "." and ".."
# reserved. Used to validate whole topic names (client reply topics/addresses) and the
# name components interpolated into framework topics (a node_id in
# "{node_id}.private.return"). Rejecting at construction turns an otherwise-remote failure
# — an illegal name surfaces only as an argument-less UnknownTopicOrPartitionError /
# request-timeout stall on another process — into a loud, local error.
_KAFKA_TOPIC_RE = re.compile(r"[a-zA-Z0-9._-]{1,249}")


def is_topic_safe(name: str) -> bool:
    """Return ``True`` iff *name* is legal for a Kafka topic name.

    Checks the charset (``[a-zA-Z0-9._-]``), the 1-249 length, and the reserved
    ``"."`` / ``".."``. When *name* is a component interpolated into a longer topic
    (e.g. a ``node_id`` in ``{node_id}.private.return``) this bounds the *component*,
    not the assembled topic — a 249-char component can still yield an over-length topic.
    """
    return bool(_KAFKA_TOPIC_RE.fullmatch(name)) and name not in (".", "..")
