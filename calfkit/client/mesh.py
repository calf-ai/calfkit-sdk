"""Caller-side mesh view — ``client.mesh`` (spec ``docs/designs/caller-side-mesh-view-spec.md``).

A supported, read-only window onto the live control plane for a non-agent process
(a gateway, a bridge, a CLI, a dashboard): ``client.mesh.get_agents()`` /
``get_tools()`` return a fresh, name-keyed ``Mapping`` of the agents and tools
currently online, projecting the internal control-plane records to public DTOs.

The surface, DTOs, cache, and projections are built up across this feature's commits.
"""

from __future__ import annotations
