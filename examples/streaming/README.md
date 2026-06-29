# Streaming intermediate work

Watch an agent's progress **live**, hop by hop, instead of waiting for the final
answer. As the run unfolds, the caller receives **step events** — the agent's
preamble, each tool call, and each tool result — and prints them as they arrive.
This is the runnable version of the
[How to call agents from a client](../../docs/client-features.md#observing-results)
guide's "Observing results" section.

The agent is a small trip planner: given a city, it checks the weather, finds
activities, then gives a short plan — so one run produces several intermediate
steps before it completes.

## What's here

| File | Role |
| --- | --- |
| `trip_tools.py` | Two tool nodes — `get_weather` and `find_activities` (canned, offline returns). |
| `planner_agent.py` | The `trip_planner` agent; calls both tools, narrates each step. |
| `service.py` | Deploys the agent **and** both tools on one worker. |
| `stream.py` | Calls the agent with `start()` and prints each step from `handle.stream()` live. |

## Prerequisites

- A running [Calfkit broker](../../README.md#running-your-agents) on `localhost:9092`.
- calfkit installed: `pip install calfkit`.
- An LLM API key: `export OPENAI_API_KEY=sk-...`.

## Run it (two terminals)

```console
# 1) the agent + its tools, on one worker
$ python service.py

# 2) call the agent and watch its work stream in
$ python stream.py
```

You'll see the steps print as the run progresses, then the final plan:

```text
── live progress ─────────────────────────────
  💬 Let me check tomorrow's weather in Kyoto.
  🔧 calling get_weather({'city': 'Kyoto'})
  ✅ get_weather → Tomorrow in Kyoto: cloudy with light rain, 14°C.
  🔧 calling find_activities({'city': 'Kyoto'})
  ✅ find_activities → In Kyoto: temple tours, a covered food market, and a tea ceremony.
  🏁 run completed
──────────────────────────────────────────────

Final plan:
...
```

The exact preamble text and the order of the tool calls are up to the model, so
your output will vary.

## Things to try

- **Stop early.** To break out of the loop before the terminal, wrap the stream
  in `contextlib.aclosing(handle.stream())` so its guard releases promptly — a
  handle allows only one live `stream()` at a time
  ([guide](../../docs/client-features.md#observing-results)).
- **Observe across runs.** `handle.stream()` follows one run; `client.events()`
  is a firehose over **every** run on the client's inbox (and the only way to
  observe a `send()` run). It is best-effort — see the guide.
- **See handoffs and sub-agents.** A `HandoffEvent`, and tool/peer steps at
  `depth > 1`, appear once an agent hands off or consults a peer — add a peer
  with [`Messaging` / `Handoff`](../../docs/agent-peers.md).
