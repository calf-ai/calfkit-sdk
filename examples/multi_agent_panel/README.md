# Multi-agent panel

Three persona agents — `optimist`, `skeptic`, `pragmatist` — discuss a topic over a
single shared transcript. Each agent's response accumulates into one `message_history`
that is threaded to the next agent.

The point of the example: once that transcript contains responses from more than one
agent, each agent is automatically invoked with the conversation **projected to its
own point of view** — its own turns stay assistant messages, the other panelists read
as attributed `<optimist>` / `<skeptic>` / `<pragmatist>` participants, and the
moderator prompts read as `<user>`. No flags, no wiring, no setup — it is on by
default, and a plain single-agent conversation is unaffected.

## Run

1. Start a Calfkit broker on `localhost:9092` (see the repo README → "Start a Calfkit
   Broker") and `export OPENAI_API_KEY=sk-...`.
2. Deploy the panel (blocks):
   ```shell
   cd examples/multi_agent_panel && python service.py
   ```
3. In another terminal, drive the discussion:
   ```shell
   cd examples/multi_agent_panel && python run.py
   ```

- `panel.py` — the three panelist agent definitions.
- `service.py` — deploys them on one worker.
- `run.py` — runs a two-round discussion over the shared transcript.
