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

`handle.stream()` yields this run's events in order, **terminal-bearing** — zero or
more intermediate **step events** that report the run's progress hop by hop, then the
terminal (`RunCompleted` / `RunFailed`) as the last element:

```python
from calfkit import AgentMessageEvent, ToolCallEvent, ToolResultEvent, HandoffEvent, RunCompleted, RunFailed

handle = await client.agent("support").start("Where is my order?")
async for event in handle.stream():
    match event:
        case AgentMessageEvent(parts=parts):
            ...                             # the agent's preamble text for a hop
        case ToolCallEvent(name=name, args=args):
            ...                             # a tool the model is about to call
        case ToolResultEvent(name=name, outcome=outcome):
            ...                             # a call's result: "success" / "failed" / "denied"
        case HandoffEvent(target=target, reason=reason):
            ...                             # control transferred to another agent
        case RunCompleted() | RunFailed():
            break                           # the terminal — always last
```

`stream()` reads the **same** per-run channel as `result()`, so you can `await
handle.result()` after (or instead of) streaming to get the typed terminal — it is cached
and replayable.

#### What a step event carries

`stream()` (and the `events()` firehose below) surface the `RunEvent` union — the two
terminals plus four intermediate step-event types, all importable from `calfkit`:

| Event | Emitted by | Carries |
| --- | --- | --- |
| `AgentMessageEvent` | an agent hop | `parts` — the agent's preamble text for the hop |
| `ToolCallEvent` | the calling agent's dispatch hop | `tool_call_id`, `name`, `args` (the parsed dict for a dispatched call; a denied call keeps the raw model emission) |
| `ToolResultEvent` | the **folding caller** — the agent that made the call mints the result as the reply folds | `tool_call_id`, `name`, `parts`, `outcome` (`"success"` / `"failed"` / `"denied"`) |
| `HandoffEvent` | an agent hop | `target` (the peer) and `reason` |

Every step event also carries hop identity — `correlation_id`, `depth`, `frame_id`,
`emitter` — so you can place an event within a multi-agent run: a consulted peer or
sub-agent streams its own hops at `depth > 1`, all to the original caller. Pair a
`ToolCallEvent` with its `ToolResultEvent` by **`tool_call_id`**; the `name` is consistent
on both halves (a peer consult's call **and** result both read `message_agent`). Branch
with `isinstance` / `match` on the closed union.

**Who answered a peer consult?** A consult's result is minted by the *calling* agent as it
folds the reply, so `ToolResultEvent.name` is `message_agent`, not the answering agent's
name. The peer you *requested* is in the call event's `args`; if control was handed off
along the way, the subtree's `HandoffEvent`s stream to your inbox too — a UI that wants to
show who actually answered composes the two.

> `AgentThinkingEvent` is **defined but not emitted** in v1 — it is not a `RunEvent`
> member and never appears on a stream.

**Step events are best-effort, not a delivery guarantee.** They are at-most-once, never
de-duplicated, and **never** the cause of a run failure — a step that can't be emitted is
logged and dropped, and a real failure arrives only as the terminal `RunFailed`. The
intermediate queue is **consume-once**: a late `stream()` drains whatever steps are still
buffered, once. For a guaranteed record of a run's outcome, use the terminal (`result()`),
not the steps.

**Pair closure is a documented contract, not a synthesized guarantee.** Every call an agent
dispatches gets exactly one result event from its caller, delivered best-effort. Three rules
let a consumer close its books:

1. **The terminal is the closing bracket.** `RunCompleted` / `RunFailed` closes every
   still-open pair — treat an unresolved call as finished-unobserved.
2. **A `failed` result for a peer/sub-agent call closes its subtree.** The callee is gone;
   its descendants' open pairs will never resolve. (If an ancestor *substitutes a success*
   over a dead subtree, that result reads `success` — rule 1 still closes the rest.)
3. **Any single step may be lost** (at-most-once). The pair law is a soft wire invariant;
   the hub never fabricates a closing event.

**Step events surface RAW parts.** A `ToolResultEvent` or `AgentMessageEvent` carries the
unprojected reply parts — they are **not** coerced to your `output_type`. Only `result()`
projects the terminal to `output_type` (a typed object, or a string under the default `str`).

To stop streaming early without holding the single-live-`stream()` guard open until garbage
collection, wrap the iterator in `contextlib.aclosing` so its cleanup runs promptly on `break`:

```python
from contextlib import aclosing

async with aclosing(handle.stream()) as stream:
    async for event in stream:
        if isinstance(event, ToolCallEvent):
            print(event.name)
            break          # aclosing() releases the stream, so a later handle.stream() is allowed
```

### Cross-run — the `events()` firehose (best-effort)

`client.events()` is a firehose over the client's one inbox — **every** event on it
while open, across all runs: terminal replies **and** the same intermediate step events
`stream()` surfaces (pass `terminal_only=True` to keep only the terminals). You demux by
`correlation_id` and do your own dedup. It is best-effort: a reader that falls behind drops
its **oldest** buffered events (signaled by `EventStream.dropped` + a WARNING) so it can
never block the hub. A `send()` run registers no per-run handle, so its progress is
observable **only** here, never via `stream()`.

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

> **Read-access to the inbox is as sensitive as the run's full content.** Step events
> stream the **whole transitive trace** of a run — every tool call, tool result, and
> handoff, at **all depths**, including the work of consulted peers and sub-agents — so
> anything that can read this inbox can see everything done on the run's behalf. Treat
> read-access to a shared `inbox_topic` accordingly. This is a deliberate design goal (an
> end-to-end live progress view), documented rather than policed; cross-trust-boundary
> confinement, if ever needed, is a future depth/redaction knob, not v1.

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
