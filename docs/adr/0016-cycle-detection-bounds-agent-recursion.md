---
status: accepted
---

# Agent-to-agent recursion is bounded by cycle detection via per-frame caller identity

Agents calling agents can recurse without terminating — directly (A messages B
messages A) or through a longer ring — in a way ordinary tool calls cannot, and
parallel peer messages amplify it. An unchecked cycle is an unbounded flood of Kafka
messages. No execution guard exists today; resource budgets are deferred to #251.

We ship a **cycle check** in v1 and **defer resource budgets** to #251. The check
matches the target agent's **identity** against the chain of ancestors currently
suspended awaiting a return. To make that identity available where it is needed, we
record it **on the call frame**: `CallFrame` gains `caller_node_id` (and
`caller_node_kind`) — the node that *dispatched* the call, **not** the callee the
frame targets. (It is named `caller_*` to avoid confusion with the existing
`ctx.emitter_node_id`, which is the *immediate-hop* sender, overwritten every hop and
insufficient for multi-hop detection.) It is stamped **only at the genuine
`invoke_frame` push** (where the dispatching node's id/kind are in scope, the same
values stamped on `x-calf-emitter`) and **preserved verbatim by the `replace()`** in
`TailCall` and `mark_fanout` — never re-stamped to `self`, or a self-retry would
manufacture a false self-cycle. It is auto-tracked by the stack's push/pop and rides
the wire in `WorkflowState.call_stack` (an additive, pre-1.0 frame change).
`prepare_context` then derives a read-only `ctx.ancestor_callers` from the inbound
frames, and the agent's dispatch resolver — in `run()`, the only layer that can emit
a model-visible `RetryPromptPart` — rejects a peer message whose target
`(name, "agent")` is already an ancestor (and rejects self explicitly).

The reason this is principled: **cycle detection alone guarantees termination in a
finite mesh.** Any infinite path must revisit some agent (a cycle); a non-revisiting
path is bounded by the agent count. A finite-but-large fan-out *tree* is a resource
concern, which is #251's, not a termination one.

Two earlier shapes failed and are recorded so they aren't retried:
- **Matching the derived input topic against frame `target_topic`s** missed cycles
  where an agent was first entered via its *public* subscribe topic (its entry
  frame's `target_topic` is the public one, not `agent.{name}.private.input`).
  Matching the emitter *identity* on the frame is entry-topic-agnostic, so the
  guarantee holds for any chain.
- **Doing the check at the publish layer** (where the stack is reachable) cannot
  produce a model-visible retry; doing it in `run()` lacks the stack. Recording
  identity on the frame and surfacing it via `ctx.ancestor_callers` puts both the
  identity and the retry in `run()`.

Considered and rejected: a depth/hop budget in v1 (overlaps #251; unnecessary for
termination); deferring the cycle check too (ships an infinite-loop footgun — a graph
cycle is a logic error, not a resource budget); a separately-maintained ancestor set
on the context (the frame is the natural lifetime owner; a set would need manual
pop-sync); a hard fault on cycle (a model-visible retry is graceful and consistent
with the rest of target validation).

Consequences: a2a recursion always terminates in v1. Resource exhaustion from a large
but finite fan-out is not bounded until #251 (documented). `CallFrame` carries one
new optional field. Self-messaging is handled separately (render-exclusion + an
**Agent-ctor** construction-time self check — the `peers=` handles can't see the
agent's own name); the ancestor check covers 2+ hop rings.
Full design in `docs/designs/agent-mesh-spec.md` §8.
