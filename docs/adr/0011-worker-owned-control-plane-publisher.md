---
status: proposed
---

# The worker owns the control-plane publisher; nodes contribute only record content

A node advertises a record to a control-plane topic, re-published periodically as a liveness
heartbeat and tombstoned on clean shutdown. The obvious approach — the one the shipped MCP capability
plane uses — is per-node: each node owns its own writer, its own heartbeat loop, and its own
tombstone (`MCPToolboxNode._capability_writer` / `_heartbeat_loop` / `_tombstone_on_shutdown`). We move
all of that up to the worker.

The deciding fact is that **liveness is a per-process fact, while record content is a per-node
fact.** A node cannot be "down" while its hosting worker is "up" — there is no per-node crash
independent of the process. So a per-node heartbeat emits N liveness signals for one liveness fact,
and duplicates the genuinely tricky cancel-then-tombstone ordering N times (the flag-first /
cancel-the-loop / then-`delete` dance that stops a straggler `set()` from resurrecting a tombstoned
record).

We therefore make the **worker** own a single `ControlPlanePublisher`: one heartbeat loop and one
ordered tombstone pass for every hosted node, owning the instance identity (`worker_id`), the writes
themselves, and the shutdown ordering. A **node contributes only its record content** via a
class-level `@advertises(topic, record)` factory that the worker calls with a worker-stamped
identity envelope (`node_id`, `worker_id`, `started_at`, `last_heartbeat_at`). The worker treats the
returned record opaquely — it never imports `CapabilityRecord` or `AgentCard` — which is what keeps
the substrate generic across every control plane (capability, agents, and the future access-policy
and reconfig planes).

Considered and rejected: **per-node writers** (the shipped pattern). Rejected for the N-signals and
N-tombstone-dances duplication, and because centralizing the resurrection-safe ordering *once* is
the substrate's whole reason to exist.

Consequences: content propagation is **pull** — the loop calls each advert factory once per tick, so
a content change lands at the next heartbeat rather than instantly. All writes funnel through the
publisher (serialized against the tombstone pass), so a later `publish_now()` push for immediate
propagation is an additive extension rather than a redesign. The node/worker split means a node
author writes a small content factory, not lifecycle or transport plumbing; the publisher is
auto-registered on a worker whenever any hosted node declares an advert, so there is no wiring to do.
