"""Control-plane record base + identity envelope (spec §5).

The shared base every concrete control-plane record extends: identity (which
node, which worker instance) plus liveness (when it last heartbeat, and the
writer's cadence). The control-plane view's staleness filter depends on this
base and nothing else, so it stays generic over the concrete record type.
"""

from __future__ import annotations

from pydantic import AwareDatetime, BaseModel, ConfigDict


class ControlPlaneStamp(BaseModel):
    """The worker-stamped per-instance fields, spread into every record by the publisher.

    Boot time + liveness + the writer's cadence — everything the *worker* (not the node)
    owns and stamps on each tick. Node identity (``node_id`` × ``worker_id``) is the wire
    **key**, never duplicated here. Frozen: built fresh per tick, never mutated.

    ``heartbeat_interval`` is the writer's cadence, carried on the wire so any reader
    (including a config-less out-of-process one) can judge staleness against the writer's
    interval rather than its own config (spec §9).
    """

    model_config = ConfigDict(frozen=True)

    started_at: AwareDatetime  # this instance's boot time -> uptime / restart detection
    last_heartbeat_at: AwareDatetime  # liveness basis; refreshed every heartbeat tick
    heartbeat_interval: float  # the writer's cadence (seconds)


class ControlPlaneRecord(ControlPlaneStamp):
    """One node instance's self-published advertisement on a control-plane topic.

    The worker :class:`ControlPlaneStamp` (inherited) plus ``schema_version``; concrete
    records (capability, agent card, ...) add their use-case content. **Identity
    (``node_id`` × ``worker_id``) is the wire key, never carried in the value** — every
    reader derives it from the key, so there is nothing to drift.

    The base intentionally declares ``schema_version`` with **no default** so it is
    abstract-by-omission: every concrete subclass must set ``schema_version: int = N``.
    That default is load-bearing — the control-plane view derives its reader version from
    it and rejects a record type that lacks one.
    """

    model_config = ConfigDict(extra="ignore", frozen=True)  # tolerant reader; immutable value object

    schema_version: int
