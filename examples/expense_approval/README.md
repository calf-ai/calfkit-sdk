# Expense approval — a handoff relay

An approval chain where control transfers **up a hierarchy**, one tier at a time. A
`team_lead` approves what's within its limit and **hands off** anything bigger to a
`director`, who hands off anything bigger still to a `vp`. Whoever is authorized
approves it and answers the employee directly. This is the multi-hop side of the
[How to let agents find and reach each other at runtime](../../docs/agent-peers.md)
guide — a chain of handoffs, each decided at runtime by the agent holding the request.

The driver streams the run with `handle.stream()`, so you watch the request climb the
chain hop by hop.

## What's here

| File | Role |
| --- | --- |
| `agents.py` | `team_lead` (≤ $1,000, `Handoff("director")`) → `director` (≤ $10,000, `Handoff("vp")`) → `vp` (any amount). |
| `service.py` | Deploys all three approvers on one worker. |
| `run.py` | Submits one large expense and prints each handoff from `handle.stream()` live. |

No tools here — the choreography *is* the example; each approver decides from the
amount alone.

## Prerequisites

- A running [Calfkit broker](../../README.md#running-your-agents) on `localhost:9092`.
- calfkit installed: `pip install calfkit`.
- An LLM API key: `export OPENAI_API_KEY=sk-...`.

## Run it (two terminals)

```console
# 1) the approval chain, on one worker
$ python service.py

# 2) submit a big expense and watch it escalate
$ python run.py
```

The $40,000 request is over the team lead's *and* the director's limits, so it climbs
all the way to the VP, who approves and answers you:

```text
>>> Please approve a $40,000 expense for our annual company offsite.
🤝 [team_lead] hands off → director ($40,000 exceeds my $1,000 limit)
🤝 [director] hands off → vp ($40,000 exceeds my $10,000 limit)
--- decision ---
Approved: $40,000 for the annual company offsite. — VP
```

Edit the amount in `run.py` to see where it stops: $250 is approved by the team lead
outright (no handoff), $7,500 stops at the director.

## See also

- [How to let agents find and reach each other at runtime](../../docs/agent-peers.md) — including the note that handoff chains are not cycle-bounded in v1, so design yours to terminate.
