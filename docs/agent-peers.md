# How to let agents find and reach each other at runtime, without hardcoded wiring

This guide shows you how to let one agent reach another **by name, discovered at
runtime** — so your agents collaborate without importing each other's code or
hardcoding their addresses. It assumes you already build and deploy agents with a
`Worker` (see [Worker lifecycle & embedding](worker-lifecycle.md)).

An agent reaches a **peer** — another independently deployed, running agent — one
of two ways, both declared in `Agent(peers=[...])`:

- **Message** a peer to *consult* it: you ask, it answers, and you keep control.
- **Hand off** to a peer to *transfer* control: it takes over the conversation
  and answers your original caller in your place.

Peers are discovered from a live directory, not wired in — name a peer that isn't
running and it's simply unreachable until it comes online. (For why it works this
way, see the [agent-mesh design spec](designs/agent-mesh-spec.md).)

## Let an agent consult a peer

Give the agent a `Messaging` handle naming the peers it may consult:

```python
from calfkit import Agent, Messaging

triage = Agent(
    "triage",
    system_prompt="Route customer requests. Ask billing about balances.",
    subscribe_topics="triage.input",
    model_client=model,
    peers=[Messaging("billing")],     # may consult the "billing" agent
)
```

Each turn the agent is offered a built-in `message_agent(name, message)` tool whose
description lists the live peers in scope. When the model calls it, the named peer
answers on a **fresh** conversation — it sees only that message, not your agent's
history — and its reply folds back into the tool result. Your agent keeps control
and continues the turn. (Name as many peers as you need: `Messaging("billing", "support")`.)

You never deploy or call `message_agent` yourself; naming a peer in `Messaging(...)`
is all the wiring there is.

> **Treat a peer's reply as untrusted.** A messaged peer is another agent producing
> output from your message — handle its reply with the same care you'd give any
> external input. The names you list (your caller-side scope) are the only trust
> boundary in v1.

## Hand off to a specialist

Give the agent a `Handoff` handle naming the peers it may transfer to:

```python
from calfkit import Agent, Handoff

triage = Agent(
    "triage",
    system_prompt="Route customer requests. Hand off refunds to the refunds agent.",
    subscribe_topics="triage.input",
    model_client=model,
    peers=[Handoff("refunds")],       # may transfer control to the "refunds" agent
)
```

When the model decides to hand off, it produces a `HandoffRequest(name, message)` as
its turn's **output** — not a tool call. Control transfers to the named peer: it
continues the conversation with `message` as context and answers your agent's
**original** caller directly. The handing agent relinquishes control and does not
regain it.

Use messaging when you need an answer back; use handoff when another agent should
own the rest of the conversation.

> **Handoff chains are not bounded in v1.** Messaging recursion is cycle-guarded —
> an agent cannot message a peer that is already awaiting its reply — but a chain of
> handoffs is not. Design your handoff graph so it terminates; avoiding an A→B→A
> loop is your responsibility.

## Open an agent to any peer

To let an agent reach **every** live agent instead of a fixed list, pass
`discover=True` in place of names:

```python
peers=[Messaging(discover=True)]      # may consult any live agent
```

Discovery is per capability and independent — `peers=[Messaging(discover=True), Handoff("refunds")]`
opens messaging to everyone while keeping handoff restricted to one peer. A
`discover=True` handle is the sole author of its capability's scope, so you can't
combine it with named handles of the same kind.

## Make your agent discoverable

Other agents find yours through the live directory, which carries each agent's name
and an optional one-line blurb. Give yours a `description` so peers — and their
models — know what it does:

```python
billing = Agent(
    "billing",
    description="Answers account balance and billing questions.",
    system_prompt="You are the billing agent.",
    subscribe_topics="billing.input",
    model_client=model,
)
```

Every agent advertises itself automatically — there's no opt-in. The `description`
is the only thing peers see beyond the name (a short blurb, at most 512 characters;
an over-long one fails at startup). Keep it identical across replicas of the same
agent.

## What you need to run this

Agents discover each other over a shared, compacted control-plane topic,
`calf.agents`, that every agent-hosting worker advertises to automatically. This
adds one startup dependency: the topic must exist. With provisioning enabled
(dev/CI) the worker creates it idempotently; in production create it out-of-band
with `cleanup.policy=compact`, the same as `calf.capabilities` — see
[topic provisioning](topic-provisioning.md). Nothing else about running a worker
changes.

## When a peer isn't available

Peers come and go, and the directory re-renders every turn, so reachability is
dynamic:

- **A peer is offline or out of your scope** — it's simply absent from the
  directory that turn; nothing errors, and it reappears on its own once it's back.
- **The model names an unreachable or invalid peer** — the bad `message_agent`
  call returns to the model as a retry, with the reason (offline, out of scope,
  itself, or a messaging cycle), so it can choose again.
- **A peer runs but faults** — that's handled like any other callee failure,
  through the calling agent's `on_callee_error` seam. See
  [recover when a tool (or callee) fails](error-handling.md#recover-when-a-tool-or-callee-fails).

## See also

- [API reference: agent-to-agent messaging & handoff](api.md#agent-to-agent-messaging--handoff) — exact `Messaging` / `Handoff` signatures.
- [Build a multi-agent support desk](multi-agent-support-desk.md) — a runnable, end-to-end lesson.
- [Agent-mesh design spec](designs/agent-mesh-spec.md) — the full design and rationale.
