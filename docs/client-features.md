# How to call nodes from a client

The `Client` connects to the broker once and mints a typed **gateway** per agent.
Each gateway speaks one verb triad — `send` / `start` / `execute` — and supports
multi-turn conversations, runtime dependency injection, temporary instruction
overrides, and per-call structured output, all without redeploying the agent.

See also: [CLI reference](cli.md) · [Worker lifecycle](worker-lifecycle.md) ·
[Consumer nodes](consumer-nodes.md) · [Policy seams](policy-seams.md) ·
[Errors & faults](error-handling.md).

## Connect once, then mint a gateway

`Client.connect()` is **synchronous and lazy** — it does no I/O until the first
dispatch, so a connection error surfaces from your first `send`/`start`/`execute`,
not from `connect()`. Mint a gateway to one agent with `client.agent(name)`:

```python
from calfkit import Client

client = Client.connect("localhost:9092")
result = await client.agent("weather_agent").execute("What's the weather in Tokyo?")
print(result.output)
```

`client.agent(name)` addresses a **deployed agent by name** — the name derives the
agent's private input topic, so you never handle a raw topic. For the rare case of
a topic that is *not* an agent's derived input (e.g. a shared work topic), use the
escape hatch `client.agent(topic="some.topic")`.

The client is an async context manager (one long-lived client per app); `aclose()`
is graceful:

```python
async with Client.connect("localhost:9092") as client:
    ...
```

## Invocation patterns

A gateway offers three verbs:

| Verb | Pattern | Returns |
| --- | --- | --- |
| `execute(...)` | Request/reply — dispatch and await the result in one call. | `InvocationResult` |
| `start(...)` | Dispatch and get a handle; `await handle.result()` later. | `InvocationHandle` |
| `send(...)` | Dispatch without awaiting the *result* (it still awaits the broker's durable accept); observe the reply on the firehose. | `Dispatch` |

`send` returns a **`Dispatch`** (a fire token carrying `.correlation_id`) —
deliberately *not* a handle, so the type itself tells you the result isn't
retrievable by id. Use `start` when you want to await the result later, and
`execute` for synchronous request/reply.

```python
gw = client.agent("weather_agent")

result = await gw.execute("What's the weather in Tokyo?")     # InvocationResult
handle = await gw.start("What's the weather in Osaka?")       # InvocationHandle
dispatch = await gw.send("Re-index the catalog.")             # Dispatch (cid only)
```

## Typed output with `output_type`

`output_type` binds **once when you mint the gateway** and defaults to `str`:

```python
# Default str: the reply is coerced to a string. Every reply part is rendered
# (text verbatim, a structured DataPart as its JSON string) and newline-joined.
text = (await client.agent("weather_agent").execute("...")).output   # str

# Pass output_type to get the typed object back, validated against your type:
from dataclasses import dataclass

@dataclass
class WeatherReport:
    location: str
    summary: str

report = (await client.agent("weather_agent", output_type=WeatherReport).execute("...")).output
print(report.location)   # "Tokyo"
```

> **Heads-up — the default `str` is loud about being a string, not silent about your intent.**
> With `output_type=str` (the default), a **structured agent's reply comes back as its JSON
> *string*, not an object — and no error is raised.** If you expect a typed object, you **must**
> pass `output_type=Model`; otherwise `result.output` is a `str` you might accidentally `.upper()`
> or render raw. (Inspect `result.output_parts` if you're unsure what came back.)

Pass the matching `output_type=Model` to deserialize into your type; a reply that is present but
fails that model raises [`DeserializationError`](#errors). With `output_type=str` there is never a
`DeserializationError` — every reply coerces to a string.

## Multi-turn conversations

Feed the previous result's `message_history` into the next call:

```python
gw = client.agent("weather_agent")
r1 = await gw.execute("What's the weather in Tokyo?")
r2 = await gw.execute("How about in Osaka?", message_history=r1.message_history)
```

The same `message_history` can carry turns from *multiple* agents — see
[`examples/multi_agent_panel/`](../examples/multi_agent_panel/) for a multi-agent
discussion over one shared transcript.

## Per-call knobs

`send` / `start` / `execute` take the same input-shaping arguments (the prompt is a
plain `str`; `execute` additionally takes `timeout`):

- **`deps`** — per-call, JSON-serializable data passed to the run's tools
  (`ctx.deps["key"]`). Merged over the client's `deps_factory` seed, if set.
- **`temp_instructions`** — system-level instructions injected into this turn only.
- **`message_history`** — prior turns for multi-turn (above).
- **`model_settings`** — per-call model settings (e.g. `{"temperature": 0}`).
- **`tool_overrides`** — runtime agent-tool overrides for this invocation.
- **`author`** — the human author of the prompt; surfaces as `<user:author>`
  attribution when two or more named humans share a channel.
- **`correlation_id`** — the demux key. Omit it (the safe default) to auto-mint a
  collision-free uuid7; supply your own only for idempotency / external correlation,
  and then *you* own its uniqueness within the inbox.

```python
result = await client.agent("weather_agent").execute(
    "What's my phone number?",
    deps={"user_id": "usr_123"},
    temp_instructions="Always respond in Japanese.",
)
```

A non-JSON-serializable `deps` / `model_settings` surfaces as a serialization error
when the call publishes — there is no call-site pre-flight.

## Observing results

There are two ways to observe a run, and which you use determines the delivery
guarantee.

### Per-run — hold the handle (lossless)

`start` / `execute` register a per-run channel and return a handle (or its result).
The handle is the **only** way to get this run's result by id, and it works for as
long as you hold it — there is no reattach-by-`correlation_id`:

```python
handle = await client.agent("support").start("Where is my order?")
result = await handle.result()              # await the terminal (lossless)
```

`handle.stream()` yields this run's events in order, terminal-bearing — the last
element is always the terminal. (Today the fabric emits no intermediate events, so
`stream()` yields exactly one element: the terminal.)

```python
async for event in handle.stream():
    ...                                     # the run's events, ending in the terminal
```

### Cross-run — the `events()` firehose (best-effort)

`client.events()` is a firehose over the client's one inbox — **every** reply on it
while open, across all runs. You demux by `correlation_id` and do your own dedup.
It is best-effort: a reader that falls behind drops its **oldest** buffered events
(signaled by `EventStream.dropped` + a WARNING) so it can never block the hub.

```python
async with client.events(terminal_only=True) as stream:
    dispatch = await client.agent("notifier").send("Ping everyone.")
    async for event in stream:
        if event.correlation_id == dispatch.correlation_id:
            ...                             # observe the send()'s reply
            break
```

> **Guaranteed delivery is hold-the-handle (`start`/`execute`) or a `@consumer`
> node** — the firehose is observation, not a delivery guarantee. A held handle is
> lossless *in-process* and survives a transient reconnect, but it is **not** immune to
> broker-side loss: a reply that ages out of the inbox's log retention while the client
> is disconnected longer than that retention is gone (and a `result()` with no `timeout=`
> would then wait forever — so bound request/reply with `timeout=`). For a stateless
> deployment (a web handler that can't hold a handle across a request boundary),
> route replies to a named `inbox_topic` and consume them with a
> [`@consumer` node](consumer-nodes.md).

## A shared, durable inbox with `inbox_topic`

By default each client gets an ephemeral per-client inbox. Set `inbox_topic` once at
`connect()` for a **durable, named** inbox another process can consume:

```python
client = Client.connect("localhost:9092", inbox_topic="orders.inbox")
```

The hub and the firehose both read this one inbox, so `events()` can never drift off
it. To *observe* an arbitrary topic, connect a separate client with that topic as its
own `inbox_topic` and call `events()` there.

## Bounding patience with `timeout`

`result()` and `execute()` await the terminal indefinitely by design — a durable run
may legitimately pause (a long task, a slow tool). To bound *the client's* patience,
pass `timeout`:

```python
result = await client.agent("support").execute("...", timeout=30)
# or, on a handle:
result = await handle.result(timeout=30)
```

On a timeout the client raises **`ClientTimeoutError`** and **the run is
unaffected** — a later terminal still resolves the channel. There is no `reply_ttl`:
abandoned runs garbage-collect on their own (the hub holds the handle weakly), so
memory is bounded by the handles you hold.

## Errors

The client surface has a small, typed error set over **run outcomes** (importable
from `calfkit`):

| Condition | Type |
| --- | --- |
| The run (or a node it called) faulted | **`NodeFaultError`** — branch on `e.report.find(FaultTypes.X)` |
| A successful reply fails a **structured** `output_type` | **`DeserializationError`** |
| This client stopped waiting (`result`/`execute` `timeout=`) | **`ClientTimeoutError`** (run unaffected) |
| The client was closed (`aclose()`) with the run in flight | **`ClientClosedError`** (run unaffected) |

```python
from calfkit import NodeFaultError, FaultTypes

try:
    result = await client.agent("billing").execute("refund order 1234", timeout=30)
except NodeFaultError as e:
    if e.report.find(FaultTypes.MODEL_CONTEXT_WINDOW_EXCEEDED):
        ...
```

See [How to handle errors and faults](error-handling.md) for the in-node fault seams
and `ErrorReport` details.
