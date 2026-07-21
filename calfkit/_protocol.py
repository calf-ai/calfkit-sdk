"""Wire-protocol constants shared across the SDK.

Lives at the package root with no internal imports so any layer — client,
worker, nodes — can depend on it without creating circular import chains.

Do not add imports from ``calfkit.*`` to this module.
"""

import re
from collections.abc import Callable
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
client send); ``return`` = "your call resolved" (a ``ReturnCall`` reply); ``fault`` =
"your call failed" (a fault publish, accompanied by :data:`HDR_ERROR_TYPE`). A missing
header reads as ``call`` (raw-producer ingress norm). The ``fault`` value is in the
value space now; only the rail stamps it."""

HDR_ERROR_TYPE = "x-calf-error-type"
"""Kafka header carrying a fault's ``error_type`` (spec §4.1), stamped alongside
``x-calf-kind: fault``. Lets ops filter faults at the broker without deserializing
the body. Framework-stamped, never user-set."""

MessageKind = Literal["call", "return", "fault"]
"""Closed value space for :data:`HDR_KIND` (spec §4.1)."""

HDR_TASK = "x-calf-task"
"""Kafka header carrying the run's ``task_id`` — the mesh-wide partition-affinity key
(task-keying prep spec §2). Minted once at origin (the client's ``_publish_call``; no
node originates a run) and forwarded unchanged on every framework publish path, exactly
as ``correlation_id`` travels — never re-minted mid-run. Read at ingress by the identity
middleware, which scopes it for handler injection and mints one for envelope-wire
deliveries that arrive without it (raw-producer entry). Framework-stamped, never
user-set; step messages do not carry it yet (their header stamp is the durable PR's).
"""

HDR_WIRE = "x-calf-wire"
"""Kafka header carrying the body's *wire schema* (intermediate-step-streaming spec §2.4):
``envelope`` (an :class:`~calfkit.models.envelope.Envelope`) or ``step`` (a ``StepMessage``).
Framework-stamped on every calfkit publish and matched by strict *positive* subscriber filters
(no absent-fallback), so an unstamped message is dropped. Distinct from :data:`HDR_KIND` (the
business kind), which is unchanged. The value is each wire model's ``WIRE`` ClassVar — the single
source for both the outbound stamp and the inbound filter, so they cannot drift."""

WireKind = Literal["envelope", "step"]
"""Closed value space for :data:`HDR_WIRE` (spec §2.4)."""


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


def wire_filter(model: Any) -> Callable[[Any], bool]:
    """A FastStream subscriber ``filter`` matching a message's :data:`HDR_WIRE` header against
    ``model.WIRE`` — strict positive equality, no absent-fallback (spec §2.4).

    The filter runs in ``is_suitable`` *before* the body is decoded into the handler's type, so a
    non-matching body never triggers ``model``'s validation (verified against FastStream 0.7.1).
    Returns a *sync* predicate; FastStream wraps it via ``to_async`` at subscriber registration.
    """
    wire = model.WIRE
    return lambda message: decode_header_str(message.headers.get(HDR_WIRE)) == wire


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
