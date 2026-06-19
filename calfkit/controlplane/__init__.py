"""The reusable control-plane substrate.

Generic machinery — a record base, a worker-owned publisher, and a per-topic
view — that any control plane (capability discovery, agent discovery, and future
access-policy / reconfig planes) builds on to advertise nodes to a compacted
topic and read a live view of them. See
``docs/designs/control-plane-substrate-spec.md`` for the full design.
"""

from calfkit.controlplane.advert import advertises
from calfkit.controlplane.config import ControlPlaneConfig
from calfkit.controlplane.records import ControlPlaneRecord, ControlPlaneStamp
from calfkit.controlplane.view import ControlPlaneView

__all__ = [
    "ControlPlaneConfig",
    "ControlPlaneRecord",
    "ControlPlaneStamp",
    "ControlPlaneView",
    "advertises",
]
