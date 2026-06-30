# Launch review — fan out, then synthesize

A cross-functional readiness review where one lead **consults several experts and
keeps control**. A `release_manager` **messages** `engineering`, `security`, and
`legal` for their status, then synthesizes a single GO / NO-GO recommendation itself
— no handoff. This is the fan-out side of the
[How to let agents find and reach each other at runtime](../../docs/agent-peers.md)
guide: gather many expert opinions via messaging, then decide.

## What's here

| File | Role |
| --- | --- |
| `agents.py` | The `release_manager` (`peers=[Messaging("engineering", "security", "legal")]`), plus the three experts. |
| `tools.py` | One canned status tool per expert. |
| `service.py` | Deploys the release manager + three experts + their tools on one worker. |
| `run.py` | Asks for a go/no-go and prints each step of the run. |

## Prerequisites

- A running [Calfkit broker](../../README.md#running-your-agents) on `localhost:9092`.
- calfkit installed: `pip install calfkit`.
- An LLM API key: `export OPENAI_API_KEY=sk-...`.

## Run it (two terminals)

```console
# 1) the review board, on one worker
$ python service.py

# 2) ask for a go/no-go
$ python run.py
```

The release manager consults all three experts, then synthesizes — and answers you
itself, because it never handed off:

```text
>>> Are we go for the v2.0 launch on Friday?
🔧 [release_manager] message_agent({"name": "engineering", "message": "What's the build and test status?"})
  🔧 [engineering] build_status()
↩  [engineering] replies: Build green; 2 flaky tests (non-blocking); release branch cut.
🔧 [release_manager] message_agent({"name": "security", "message": "Any blocking findings?"})
  🔧 [security] security_scan()
↩  [security] replies: No critical/high; 1 medium scheduled next sprint.
🔧 [release_manager] message_agent({"name": "legal", "message": "Are we cleared?"})
  🔧 [legal] compliance_status()
↩  [legal] replies: ToS approved; DPA signed; cleared for launch.
--- recommendation ---
GO.
- Engineering: green, only non-blocking flakes.
- Security: no blocking findings.
- Legal: cleared.
```

The model decides the order and wording, so your output will vary.

## See also

- [How to let agents find and reach each other at runtime](../../docs/agent-peers.md)
- [`../newsroom`](../newsroom) — the same fan-out consult, but it **hands off** to a writer to finish (control transfers).
