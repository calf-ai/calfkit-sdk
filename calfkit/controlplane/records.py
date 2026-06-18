"""Control-plane record base + identity envelope (spec §5).

The shared base every concrete control-plane record extends: identity (which
node, which worker instance) plus liveness (when it last heartbeat, and the
writer's cadence). The control-plane view's staleness filter depends on this
base and nothing else, so it stays generic over the concrete record type.
"""

from __future__ import annotations

from pydantic import AwareDatetime, BaseModel, ConfigDict


class ControlPlaneRecord(BaseModel):
    """One node instance's self-published advertisement on a control-plane topic.

    Identity + liveness only; concrete records (capability, agent card, ...) add
    their use-case content. The base intentionally declares ``schema_version``
    with **no default** so it is abstract-by-omission: every concrete subclass
    must set ``schema_version: int = N``. That default is load-bearing — the
    control-plane view derives its reader version from it and rejects a record
    type that lacks one.

    ``heartbeat_interval`` is the *writer's* cadence, carried on the wire so any
    reader (including a config-less out-of-process one) can judge staleness
    against the writer's interval rather than its own config (spec §9).
    """

    model_config = ConfigDict(extra="ignore", frozen=True)  # tolerant reader; immutable value object

    schema_version: int
    node_id: str
    worker_id: str
    started_at: AwareDatetime
    last_heartbeat_at: AwareDatetime
    heartbeat_interval: float


class ControlPlaneIdentity(BaseModel):
    """The identity + liveness envelope the publisher stamps on each tick.

    Exactly the non-``schema_version`` fields of :class:`ControlPlaneRecord`, so a
    factory composes a complete record as ``R(**identity.model_dump(), <content>)``.
    Frozen: built fresh per tick, never mutated.
    """

    model_config = ConfigDict(frozen=True)

    node_id: str
    worker_id: str
    started_at: AwareDatetime
    last_heartbeat_at: AwareDatetime
    heartbeat_interval: float
