# Newsroom — consult, then hand off

An editorial desk that uses **both** peer verbs in a single run. An `editor`
**messages** a `researcher` and a `fact_checker` to gather and verify the story
(keeping control), then **hands off** to a `writer`, who drafts the final piece and
answers the reader directly. This is the composition side of the
[How to let agents find and reach each other at runtime](../../docs/agent-peers.md)
guide — messaging and handoff in one emergent workflow.

The driver streams the run with `handle.stream()`, so you watch the editor's two
consults stream back (indented, one depth deeper) and then the handoff to the writer.

## What's here

| File | Role |
| --- | --- |
| `agents.py` | The `editor` (`peers=[Messaging("researcher", "fact_checker"), Handoff("writer")]`), plus `researcher`, `fact_checker`, `writer`. |
| `tools.py` | Canned archive-search and fact-check tools. |
| `service.py` | Deploys all four agents + the two tools on one worker. |
| `run.py` | Gives the editor a brief and prints each step from `handle.stream()` live. |

## Prerequisites

- A running [Calfkit broker](../../README.md#running-your-agents) on `localhost:9092`.
- calfkit installed: `pip install calfkit`.
- An LLM API key: `export OPENAI_API_KEY=sk-...`.

## Run it (two terminals)

```console
# 1) the newsroom, on one worker
$ python service.py

# 2) give the editor a story brief and watch it choreograph
$ python run.py
```

The editor consults both specialists, then hands the piece to the writer — who
answers you, because the handoff transferred the conversation:

```text
>>> Write a short news brief about the city's new downtown bike-share program.
📨 [editor] asks researcher: Background on the downtown bike-share program?
  🔧 [researcher] search_archive({'topic': 'downtown bike-share program'})
↩  researcher replies: City council approved it 7-2; 120 docks across 15 stations; first month free...
📨 [editor] asks fact_checker: Verify the dock counts and the 'first month free' claim.
  🔧 [fact_checker] check_claim({'claim': '120 docks, 15 stations, first month free'})
↩  fact_checker replies: Verified with the transit authority; caveat: launch date is 'late spring'...
🤝 [editor] hands off → writer (draft the final brief)
--- the writer's brief ---
The city council has approved a downtown bike-share program in a 7-2 vote ...
```

The model decides the order and wording, so your output will vary.

## See also

- [How to let agents find and reach each other at runtime](../../docs/agent-peers.md)
- [`../launch_review`](../launch_review) — the same fan-out consult, but the lead **keeps** control and synthesizes (no handoff).
