---
status: proposed
---

# Caller-side reads of the control plane are a projected `client.mesh` surface, not exported internals

[Issue #301](https://github.com/calf-ai/calfkit-sdk/issues/301) asks for a supported, public,
caller-side way for a *non-agent* process (a gateway, a Discord bridge, a CLI) to observe the live
agent directory — and, by extension, the live tool directory. Today only agents read that directory,
through a per-reader [Control Plane View](../../CONTEXT.md) over `calf.agents` / `calf.capabilities`;
the wire records (`AgentCard`, `CapabilityRecord`) and the topic names are deliberately
internal/unexported, so any downstream that reads them today must couple to calfkit internals.

Decision: add a **caller-side `client.mesh`** facade with two read methods — `get_agents()` /
`get_tools()` — each returning a **name-keyed `Mapping`** over the **same** per-reader Control Plane View
the worker uses, **projecting** each wire record to a **public DTO** — `AgentInfo` for agents, and a
`match`-friendly `ToolInfo = ToolNodeInfo | ToolboxInfo` union for tools. The wire records and topic
names stay unexported; the caller depends only on the projected types and `client.mesh`. Liveness reuses
[Advisory staleness](../../CONTEXT.md) (presence in the Mapping ⇔ online). There is no held live-view
handle — the surface is `get_*` → `Mapping`, so there is no use-after-close hazard.

Considered options. **(a) Export the wire records + topic names** so callers read the Control Plane
View directly — rejected: it couples every downstream to a versioned, internal wire model
(`schema_version`, `node_kind`, per-instance worker stamps) and to hard-coded topic strings; the moment
the record evolves, callers break. **(b) Run a parallel presence plane** on the public control-plane
substrate (the issue's "Alternative considered") — rejected: it duplicates the directory every agent
already advertises, for a second plane to maintain. **(c) The chosen projection surface** — one
supported read, decoupled from internals, reusing the substrate with zero new wire surface.

Consequences. The projected DTOs (`AgentInfo`, `ToolNodeInfo`, `ToolboxInfo`, `ToolSpec`) are now public
API and evolve under public-API rules, while the wire records stay free to change behind them.
`client.mesh` is a **cached singleton** that owns **one kind-scoped Control Plane View per kind**
(agents, tools), lazily opened on first `get_*` (per-kind single-flight) — so a polled read pays the
open + catch-up once and is O(1) thereafter. Each view's Kafka consumer is **independent of the client's
inbox / Hub** (bootstrap derived from the client's connection), so a pure-observer client reads the mesh
without bringing up its reply path; the client owns teardown (`aclose()` stops the views before the
broker). Health is by behaviour — `get_*` raises a typed `MeshUnavailableError` carrying a `reason`
(`establishing` / `reader_dead` / `open_failed`) when the view is not usable, and returns the `Mapping`
otherwise — so the `Mapping` return needs no status field, and a poller routes (retry vs alert) on the
`reason` without string-matching. This is *deliberately divergent* from the agent-side readers of the same
views (which warn-degrade to an empty result, never raising into a run loop): a polling caller can act on
the exception, an agent mid-turn cannot. The full surface and semantics are
specified in [the caller-side mesh-view spec](../designs/caller-side-mesh-view-spec.md). What a view does
when the topic does not exist yet — fail loud, the topic is a precondition — is recorded in ADR-0029.
