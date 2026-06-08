# Fire-and-forget emit

| | |
|---|---|
| Status | Implemented |
| Closes | #132 (`Client.invoke_node` is documented as "fire-and-forget" but always allocates per-call reply state and triggers reply traffic) |
| Breaking | Yes — `feat!` (the wire `CallFrame.callback_topic` becomes nullable; old workers can't read a `None` terminal) |

## Problem

`Client.invoke_node` was advertised as "fire-and-forget", but tracing the wire path
end-to-end shows it is anything but. Both `invoke_node` and `execute_node` route through
`BaseClient._invoke`, which **always** registers an `asyncio.Future` in
`_ReplyDispatcher._pending` (`base.py`) and pushes a `CallFrame` whose `callback_topic` is
the client's ephemeral reply inbox. That has three costs:

- **Per-call client state.** Every call pins a `Future` keyed by `correlation_id`, even when
  the caller never awaits it.
- **Unconditional reply traffic.** The worker's terminal `ReturnCall` **always** publishes a
  reply to the bottom frame's `callback_topic` — the client's reply inbox. So even a caller
  who never reads the result causes a full reply serialize + Kafka publish, and the client's
  own dispatcher consumes it and logs `"reply received but no pending future"`.
- **A genuine permanent leak.** If a reply is lost / never arrives, or an `invoke_node` handle
  is abandoned, the future is pinned in `_pending` forever. (`execute_node` self-cleans *only when
  called with an explicit `timeout`* — its `wait_for(timeout)` cancel evicts the future; with the
  default `timeout=None` it awaits the future directly and leaks just like `invoke_node`, which
  never self-cleans.)

There was no true one-way send: no way to dispatch work to a node, allocate zero per-call
state, and produce zero reply traffic.

## The key architectural fact: two independent output channels

The change is safe — and traceable — because calfkit emits on **two independent output
channels** per hop:

1. **The callback / reply channel** (`CallFrame.callback_topic`) — a point-to-point return
   address that routes the terminal result back to whoever requested it (for a client
   invocation, the client's ephemeral reply inbox).
2. **The broadcast channel** (`publish_topic`) — every hop's envelope, *including the terminal
   one carrying `final_output_parts`*, is published to the node's configured `publish_topic`
   via the worker's `@publisher` wiring.

These fire independently. In the worker's terminal `ReturnCall` branch, the same
`publish_envelope` is both `publish`ed to the callback topic **and** `return`ed from the
handler — and the publisher decorator broadcasts that returned envelope to `publish_topic`.

The consequence is the load-bearing insight of this design: **suppressing the callback does
not lose traceability.** The terminal result still rides `publish_topic` as a durable Kafka
record — exactly the stream that `@consumer` nodes already tap (see the consumer-node section
of the README), and exactly how `execute_node` results are already observable. A
fire-and-forget send is therefore *not* a blind send: its outcome is fully observable on the
target node's `publish_topic`, just without a point-to-point reply.

The only residual gap is a target node with **no** `publish_topic` configured — then a
fire-and-forget send is genuinely unobservable. We close that with guidance (below), not new
API, per maintainer direction.

## Solution

A true one-way client method, `Client.emit_to_node`, that allocates zero per-call state and
triggers zero reply traffic; a nullable `callback_topic` on the wire so the terminal hop can
be callback-less; honest docstrings; and an opt-in TTL backstop for the existing handle path.

### 1. Wire model — nullable callback

`CallFrame.callback_topic` becomes `str | None`. `None` means "fire-and-forget; there is no
requester to return to." `WorkflowState.invoke_frame(call, callback_topic: str | None)` is
widened to match — it only stores the value, and `TailCall` already re-pushes the inherited
callback, so a `None` propagates harmlessly down a tail chain. `CallFrame` is a frozen stdlib
dataclass with no validators, so the pydantic schema accepts `null` and no existing
constructor call site breaks.

### 2. Worker — tolerate a callback-less terminal

In `_publish_action`'s `ReturnCall` branch (`calfkit/nodes/base.py`), the point-to-point
publish is guarded:

```python
frame = envelope.internal_workflow_state.unwind_frame()
publish_envelope = Envelope(...)
if frame.callback_topic is not None:
    await broker.publish(publish_envelope, topic=frame.callback_topic, ...)
else:
    logger.debug("[%s] no-callback fire-and-forget terminal node=%s", ...)
return publish_envelope
```

The `return publish_envelope` is **unchanged** — the terminal result is still broadcast to
`publish_topic` (the traceability channel). Tool round-trips use `_return_topic`, never the
client callback, so only the bottom (client) frame is ever `None`; no intermediate hop is
affected.

### 3. Client — `emit_to_node`

`BaseClient._emit(...)` mirrors `_invoke` minus `self._dispatcher.expect(...)`, the
`InvocationHandle`, and the `reply_topic`. It pushes a
`CallFrame(target_topic=topic, callback_topic=None, ...)`, publishes with the same emitter
headers as `_invoke`, and **returns the `correlation_id`** (a `str`) for tracing/correlation.

`Client.emit_to_node(...)` takes the same input-shaping arguments as `invoke_node`
(`tool_overrides`, `temp_instructions`, `message_history`, `run_args`, `deps`,
`model_settings`, `correlation_id`) **minus** `reply_topic` and `output_type` — both are
meaningless when there is no reply to route or deserialize. It reuses `invoke_node`'s
`model_settings` JSON-serializability guard, `State` construction, and `OverridesState` build,
and returns the `correlation_id`.

The name `emit_to_node` mirrors calfkit's own `Emit` node action ("fire-and-forget publish, no
reply expected") and forms a clean three-way set:

| Method | Pattern | Returns | Per-call state |
|---|---|---|---|
| `emit_to_node` | one-way fire-and-forget | `correlation_id: str` | none |
| `invoke_node` | async, reply via handle | `InvocationHandle` | a pending `Future` until resolved/evicted |
| `execute_node` | sync request/reply | `NodeResult` | a pending `Future` (self-cleaning only when called with an explicit `timeout`) |

A runnable end-to-end example lives at [`examples/quickstart/emit.py`](../../examples/quickstart/emit.py).
Pair it with [`examples/quickstart/weather_sink.py`](../../examples/quickstart/weather_sink.py) — a
`@consumer` tapping the agent's `publish_topic` — to watch the fire-and-forget result arrive on the
broadcast channel even though no point-to-point reply is returned.

### 4. Docstring honesty

The `Client` class docstring and `invoke_node` docstring are relabeled to the three patterns
above. `invoke_node` is no longer called "fire-and-forget" — it is the *async-with-handle*
pattern; `emit_to_node` is the true fire-and-forget.

### 5. `publish_topic` guidance for traceability

Because suppressing the callback preserves the `publish_topic` broadcast, the guidance for
fire-and-forget is simple and concrete:

- **Set a `publish_topic` on any node you `emit_to_node` to** if you want its outcome to be
  observable. Tap that topic with a `@consumer` node (or any envelope-aware subscriber) to log,
  persist, or alert on terminals — `result.output` is populated on the terminal hop exactly as
  it is for `execute_node`.
- **Residual gap:** a node with no `publish_topic` produces *no* observable record for a
  fire-and-forget send — there is neither a reply nor a broadcast. This is the one case where
  `emit_to_node` is genuinely blind. The fix is configuration (add a `publish_topic`), not API;
  it is called out here so the tradeoff is explicit rather than surprising.

### 6. Opt-in reply TTL backstop

The permanent-leak failure mode (an abandoned `invoke_node` handle, or a lost reply) is
addressed by an **opt-in** TTL on the reply dispatcher. It is *off by default* — a deliberate
caller-responsibility choice, not a back-compat concession. Callers who want `_pending` bounded
under lost replies / abandoned handles **must** set a TTL; there is no silently-applied safety
ceiling.

- `_ReplyDispatcher.__init__(self, reply_ttl: float | None = None)` — `None` means no eviction.
- `_pending` holds a `_PendingEntry` (future + optional `asyncio.TimerHandle`) instead of a bare
  `Future`.
- `expect()` schedules `loop.call_later(reply_ttl, self._evict, correlation_id)` when a TTL is set.
- `_evict()` sets `ReplyExpiredError(correlation_id, ttl)` on the future if it is still pending.
- The done-callback pops the entry, cancels the timer (so a stale timer never fires after the
  future completes), and retrieves the eviction exception so an abandoned future — one nobody
  awaits — does not trigger asyncio's "Future exception was never retrieved" log. `_on_reply`
  (the dispatch logic split out of the `_handle_reply` subscriber closure) guards on
  `future.done()` so a reply that arrives after eviction is dropped at `debug` rather than warning.
- `close()` mirrors that retrieval for a future evicted but not yet discarded (the `_evict`→
  `_discard` `call_soon` window), so shutdown never leaks the same warning.
- `close()` cancels timers and futures.
- `reply_ttl` is threaded through `Client.connect(..., reply_ttl=None)` →
  `_ReplyDispatcher(reply_ttl=...)`.

`ReplyExpiredError` is added to `calfkit/exceptions.py` carrying `correlation_id` and `ttl`, so
`handle.result()` raises something actionable on eviction instead of a bare `CancelledError`.

`emit_to_node` registers **no** future, so the TTL does not apply to it — it has nothing to
evict. The TTL exists solely to bound the `invoke_node` handle path.

## Conscious decisions

- **Hard break, no shims.** Per maintainer direction this lands as a `feat!` with a
  `BREAKING CHANGE:` footer (mirroring `feat!: deps as dict ...`). The wire `callback_topic`
  becomes nullable; an old worker would not understand a `None` terminal. There is no
  version-skew shim and no mixed old/new worker+client compatibility layer — deploy a uniform
  calfkit version (framework-internal envelope; 0.x). Roll workers before clients so a callback-
  less terminal is never produced against a worker that can't handle it.
- **TTL defaults to `None`.** Bounding `_pending` is the caller's responsibility and is
  documented as such. A silent default ceiling would surprise callers who legitimately wait a
  long time for a reply; an explicit opt-in is honest about the tradeoff.
- **No defensive copy / no new hang path.** `emit_to_node` reuses the exact `State` /
  `OverridesState` / publish machinery as `invoke_node`; the only delta is "don't register a
  future, push a `None` callback." `_invoke` is left intact for the handle path.

## Out of scope / deferred

- **Delayed / scheduled send** (e.g. a Restate-style `WithDelay`) — a possible follow-up; not
  part of this change.
- **Wiring the unused `Emit` node action into the `NodeResult` union** so a node (not just a
  client) can fire-and-forget to another node — deferred.
