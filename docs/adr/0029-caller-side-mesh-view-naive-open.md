---
status: accepted
---

# A caller-side mesh view opens naively; the directory topic's existence is an operational precondition (graceful-empty tolerance rejected, and the observer does not ensure)

A caller-side observer can connect before any agent or tool has advertised, so on a no-auto-create broker
the `calf.agents` / `calf.capabilities` topic may not exist yet. [Issue #301](https://github.com/calf-ai/calfkit-sdk/issues/301)
says such a reader "should not fail or hang on a missing directory topic — it should **tolerate OR
ensure** the topic's existence." There are two ways to honour that: *tolerate* (serve an empty view that
self-heals) or *ensure* (the topic is created/provisioned, and a genuinely-missing topic is a loud,
actionable error). The key question this ADR settles is **who** does the ensuring.

We first pursued **tolerate** (graceful-empty). A design review showed it is by far the dominant
complexity of the feature: a `tolerate_missing_topic` capability in ktables (plus a major version bump),
a caller-side bounded open that leaked a consumer, and a dependency on aiokafka's metadata-refresh cadence
(a heal window of minutes). Rejected.

Decision: **the observer opens naively and never participates in topic creation.** The directory topic's
existence is a documented **operational precondition**. The mesh view opens with **`ensure_topic=False`,
always**, and consults **no** deployment config — in particular *not* `client._provisioning` (which
governs the client's own inbox/data topics, issue #180, not the cluster control plane). The control-plane
topics are created by the agents' **workers** (which `ensure` when provisioning is enabled, else the
broker auto-creates on the first advert) or by **ops**; the observer **never** issues an admin
create/ensure of its own. On a **no-auto-create broker** — which production control planes **must** be,
since broker auto-create yields the wrong `cleanup.policy=delete` (a compacted control-plane topic
requires `compact`) — a missing topic **fails loud**: `get_*` raises `MeshUnavailableError(reason="open_failed")`
with an actionable cause, *without* classifying the error (no string-matching).

**The line we draw is best-effort naive, and it is about *explicit* client action, not provable
side-effect-freedom.** The invariant is: **the client sets nothing on the deployment** — no admin
create/ensure, no `_provisioning` consult, `ensure_topic=False`. What the *broker* does on a plain consumer
subscribe is the broker's own policy, not the client's request: on an auto-create broker (e.g. a dev/CI
Redpanda `dev-container`) the consumer's metadata request triggers broker-side auto-create (with the wrong
policy), so a missing topic yields an *empty* roster there — the same behaviour the worker's own
writer/consumer already has, and not something the observer asked for (aiokafka 0.13 has no
`allow_auto_create_topics=False` lever anyway, but enforcing that is the broker's job, via no-auto-create,
which production control planes must use). This is the *document-don't-police-deployment* posture: the
"ensure" half of issue #301 is satisfied by the workers / ops that own the deployment, never by an admin
call the observer makes.

Considered options. **Tolerate (graceful-empty)** — rejected (complexity, a dependency change, a
minutes-long heal window). **Ensure-when-provisioning** (mirror the worker: `ensure_topic =
client._provisioning.enabled`) — rejected: it lets a pure observer create cluster-wide control-plane
topics as a side-effect of *reading*, conflates the client's own-topic provisioning with the control
plane, and couples the observer to deployment config it should never touch. **Naive observer (chosen)** —
the observer only reads; topic creation stays with the deployment (workers/ops); minimal, dependency-free,
and the cleanest expression of document-don't-police.

Consequences. The one case that doesn't "just work" is an observer that reads before any agent has *ever*
advertised on a cluster with no ops provisioning — it fails loud (actionable), and once any agent has ever
booted the compacted topic exists permanently. Deliberately divergent from the worker, which *does* ensure
in dev/CI (the worker is part of the deployment; an observer is not) — so the mesh inlines its own naive
open rather than sharing the worker's ensure-bearing dance. The entire cold-start apparatus (ktables
prerequisite, bounded open, self-heal, a `pending` status, a supervisor) and any provisioning coupling are
gone. A transient broker outage after establishment does not raise (ktables serves frozen state and
resumes; staleness ages nodes out); only a non-retriable reader death surfaces, by the next read raising
`reason="reader_dead"`. Specified in [the caller-side mesh-view spec](../designs/caller-side-mesh-view-spec.md);
the surface-existence decision is [ADR-0028](0028-caller-side-mesh-view-over-control-plane.md).
