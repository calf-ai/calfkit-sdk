---
status: accepted
---

# Every node has a deterministic, name-scoped private input topic

Agent-to-agent messaging needs a caller to reach a *specific* named peer, but a
node's user-configured `subscribe_topics` are not guaranteed to be node-unique
(different nodes may deliberately share a work topic). More broadly, the SDK had no
deterministic way to map a node's `name` to a topic — callers had to be handed or
memorize topics, and the capability plane has to *advertise* each tool node's
`dispatch_topic` because it can't be derived.

We add a framework-owned **private input topic** to **every** node at the base-node
level: `_private_input_topic = f"{node_kind}.{name}.private.input"`. It is contributed
at registration the same way `{node_id}.private.return` is — a property read by
`register_handlers` and `topics_for_nodes` and **flagged framework-owned** (so it
never receives user topic configs) — *not* appended into the `subscribe_topics` list
in `__init__` (the `@dataclass` tool-node `__init__` bypasses `BaseNodeDef.__init__`, so a
list-append there would be silently missed; a property sidesteps the init-time divergence
across every node kind). It parallels the private return
topic: that is the continuation inbox, this is the inbound inbox.

The point is the **deterministic `name → topic` mapping**, and we make it **universal
(every node kind), not agents-only**, deliberately:
- Agent peer-messaging (ADR-0015) is the first consumer — which is why the
  `AgentCard` carries no topic field (the caller derives `agent.{name}.private.input`
  from the name it has from the directory).
- A future "call any node by name" caller surface reuses the same derivation across
  kinds. A *conditional* further payoff: **if** tool nodes' public inboxes are later
  standardized onto this derived topic, their dispatch topics become derivable and
  the capability plane could stop advertising `dispatch_topic`. That is not automatic
  from this primitive alone — a tool node's current `dispatch_topic` is its arbitrary
  `subscribe_topics[0]`, not a derived value — so it is a future opportunity this
  unlocks, not a guarantee it delivers.

The cost is one provisioned topic per node, and a *dormant* inbox on non-agent nodes
in v1 (nothing dispatches peer messages to a tool or consumer yet; a stray
non-`ToolCallRef` body sent to a tool's inbox would fault, but nothing sends there).
This is an accepted, deliberate trade for the universal mapping — recorded here so it
is not re-flagged as a per-node-only-needs-it concern.

Considered and rejected: **scoping the topic to agents only** (loses the universal
mapping and the future derivable tool dispatch topics — the reviewer's suggestion,
overridden); **a dedicated per-agent ask topic** (solves only agents); **advertising
the topic on the AgentCard** (redundant once derivable); **reusing the private return
topic** (conflates fresh inbound calls with continuations).

Consequences: every node also consumes `{node_kind}.{name}.private.input`, so that
topic must be provisioned (same operational contract as any subscribe topic) and is
framework-owned. Every provisioning surface (the worker's startup ensurer and the
`ck topics provision` CLI) derives its framework-owned set from one authority,
`framework_topics_for_nodes`, so the return and input inboxes are classified
identically everywhere. The derived key is unique per `(kind, name)`. Node `name`
(= `node_id`) must remain cluster-wide unique (existing contract). Full design in
`docs/designs/agent-mesh-spec.md` §4.1.
