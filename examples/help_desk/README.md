# Internal help desk — runtime discovery

A front-desk agent that routes employee questions to whichever **expert team is
online**, discovered at runtime. It never holds a list of who exists: it consults
the right expert with **`Messaging`** (and keeps control), or **hands off** a task
an expert should own with **`Handoff`** — both opened to *every* live agent with
`discover=True`. This is the open-mesh side of the
[How to let agents find and reach each other at runtime](../../docs/agent-peers.md)
guide.

## What's here

| File | Role |
| --- | --- |
| `agents.py` | The `help_desk` (`peers=[Messaging(discover=True), Handoff(discover=True)]`) and three experts — `hr`, `it_support`, `finance`. |
| `tools.py` | One canned lookup tool per expert. |
| `service.py` | Deploys the help desk + experts + tools on one worker. |
| `run.py` | Asks two questions and prints each step of the run. |
| `extra_expert.py` | A `legal` expert you deploy *later* to see discovery pick it up with no code change. |

## Prerequisites

- A running [Calfkit broker](../../README.md#running-your-agents) on `localhost:9092`.
- calfkit installed: `pip install calfkit`.
- An LLM API key: `export OPENAI_API_KEY=sk-...`.

## Run it (two terminals)

```console
# 1) the help desk + its experts, on one worker
$ python service.py

# 2) ask it two things
$ python run.py
```

The first question is a quick lookup, so the desk **messages** `hr` and relays the
answer. The second is a task to own, so the desk **hands off** to `finance`, which
files it and answers you directly:

```text
>>> How many vacation days do I have left? I'm Sam Rivera.
🔧 [help_desk] message_agent({'name': 'hr', 'message': 'How many PTO days does Sam Rivera have left?'})
  🔧 [hr] pto_balance({'employee': 'Sam Rivera'})
  ↩  [pto_balance] replies: Sam Rivera has 12 paid-time-off days remaining this year.
↩  [message_agent] replies: Sam Rivera has 12 paid-time-off days remaining this year.
--- answer ---
You have 12 paid-time-off days remaining this year.

>>> Please file a $400 reimbursement for my conference travel.
🤝 [help_desk] hands off to [finance] (file a reimbursement)
🔧 [finance] file_reimbursement({'amount_usd': 400, 'purpose': 'conference travel'})
↩  [file_reimbursement] replies: Filed reimbursement #4821 for $400.00.
--- answer ---
Filed reimbursement #4821 for $400.00 (conference travel); payout in 3-5 business days.
```

The model decides which expert to reach and whether to message or hand off, so your
output will vary.

## Things to try — discovery in action

Leave `service.py` and add a **new** expert in a third terminal, *after* everything
is already running:

```console
$ ck run extra_expert:legal
```

Now ask a legal question (edit a prompt in `run.py`, e.g. *"Can I share our standard
NDA with a vendor?"*). The help desk discovers `legal` on the next turn and routes
to it — you never touched the help desk's code or restarted it. That is the point of
`discover=True`: your team grows by **deploying a process, not rewiring a router**.

## See also

- [How to let agents find and reach each other at runtime](../../docs/agent-peers.md)
- [Build a multi-agent support desk](../../docs/multi-agent-support-desk.md) — the named-peer tutorial.
