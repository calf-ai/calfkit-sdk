# How to tap a topic with a consumer node

A **consumer node** is a terminal sink — it subscribes to one or more topics and
runs arbitrary Python logic against every event flowing through. The consumer
function receives a `ConsumerContext`: the projected `output`, the full session
`state` (with `ctx.message_history` and `ctx.metadata` conveniences), and the
inbound producer `deps` via `ctx.deps["key"]` — the same data tools read as
`ctx.deps["key"]`. See the [API reference](api.md#context-objects) for its full
shape.

Deploy a consumer as its own service. Wire it to an agent's `publish_topic` (or
any topic carrying calfkit envelopes) to observe outputs from agents, tools, and
intermediate hops:

```python
# weather_sink.py
import asyncio
from calfkit.client import Client
from calfkit.models import ConsumerContext
from calfkit.nodes import consumer
from calfkit.worker import Worker

@consumer(subscribe_topics="weather_agent.output")
async def log_weather(ctx: ConsumerContext) -> None:
    if ctx.output is None:
        return  # intermediate hop — no final output yet
    print(f"[{ctx.correlation_id[:8]}] {ctx.output}")

async def main():
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[log_weather])  # Deploy the consumer node
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
```

Run alongside the agent service:

```console
$ python weather_sink.py
```

An agent's `publish_topic` emits on **every** state transition — intermediate
hops, tool completions, and terminals — so `ctx.output` is `None` on
intermediate (call-kind) hops that carry no reply slot. A consumer is an
observer and handles every event; to act only on agent terminals, return early
inside the body on the intermediate hops:

```python
@consumer(subscribe_topics="weather_agent.output")
async def save_final(ctx: ConsumerContext) -> None:
    if ctx.output is None:
        return  # intermediate hop — nothing final yet
    await db.save(ctx.output)  # only terminals reach here
```

## Requirements & error policy

- **Upstream requirement:** the upstream agent or tool must have a `publish_topic`
  set for consumers to tap (e.g. `publish_topic="weather_agent.output"` on the
  agent).
- **Error policy:** exceptions from the consumer function are logged and swallowed
  by default. The `Worker` registers subscribers with FastStream's default
  `AckPolicy.ACK_FIRST`, which commits the Kafka offset *before* the handler runs,
  so `re_raise=True` only restarts the consumer task and surfaces the exception in
  logs — the message is **not** redelivered and there is no built-in DLQ. Use it
  for fail-loud development; for true retry/DLQ, set the subscriber's `ack_policy`
  via the `Worker`'s `extra_subscribe_kwargs`.

See also: [Client-side features](client-features.md) for the invocation
patterns, and the [API reference](api.md#context-objects) for the
`ConsumerContext` shape.
