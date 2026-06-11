# Client send API: `send` / `start` / `execute` + settable return address (`reply_to`)

| | |
|---|---|
| Status | Implemented (TDD, 2026-06-10) |
| Follows | [fire-and-forget-emit.md](fire-and-forget-emit.md) (#132) |
| Breaking | Yes — `feat!`: renames all three client send methods and removes the per-call `reply_topic` param from the request/reply pair. **No wire change** — `CallFrame.callback_topic` is already nullable and already carries arbitrary topics; no worker/client rollout ordering constraint. |

## Problem

Three defects in the client send surface: a missing capability, a latent bug, and
mis-vocabularied names.

1. **Missing capability.** There is no way to send fire-and-forget *and* direct the terminal
   result to a topic of the caller's choosing — the Return Address pattern (EIP), which every
   mature messaging surface supports on its one-way verb (AMQP `reply_to`, JMS `JMSReplyTo`,
   NServiceBus `RouteReplyTo`, FastStream's own `broker.publish(reply_to=...)`).
   `emit_to_node` hardcodes `callback_topic=None`.

2. **Latent bug.** `invoke_node(reply_topic=...)` *pretends* to provide this but breaks: the
   reply dispatcher subscribes to exactly one topic — the client's own inbox, registered once
   at `connect()` (`reply_dispatcher.py:50`, `base.py:198`) — while `_invoke` unconditionally
   registers a future (`base.py:258`). A custom `reply_topic` therefore delivers the reply
   elsewhere AND pins a future that nothing can ever resolve: it dangles forever, or dies with
   a misleading `ReplyExpiredError` under a TTL. No call site in the repo passes a custom value.

3. **Misleading names.** In messaging vocabulary, *emit/publish* = broadcast an event
   (pub-sub, no addressee); *send* = point-to-point command (addressed, correlated, may carry
   a return address). `emit_to_node` is a command send, not an event emission — and with
   `reply_to` added, `emit(..., reply_to=...)` reads as a contradiction. (The original
   "mirrors the node-side `Emit` action" justification points at dead code: `actions.py:95`
   has zero construction sites and no worker dispatch branch.) `invoke` vs `execute` are
   near-synonyms — neither telegraphs which one waits (cf. AWS Lambda's `InvocationType`
   confusion). The `_node` suffix discriminates nothing: every client method targets a node,
   and the `topic` argument carries the addressing.

The coherent model: with one `callback_topic` slot on the wire, "await a future" and "deliver
the reply elsewhere" are mutually exclusive. A future is resolvable **iff** the callback is the
client's own inbox.

| `callback_topic` on the wire | Future | Verb (new name) |
|---|---|---|
| `None` (worker suppresses terminal callback) | no | `send` (today's `emit_to_node` default) |
| arbitrary topic, someone else consumes | no | `send(reply_to=...)` — **new capability** |
| client's own inbox | yes | `start` / `execute` (always, after this spec) |
| arbitrary topic + future | — | the bug; becomes unrepresentable |

## Decisions (locked 2026-06-10)

1. The settable return address lives on the one-way verb: `send(reply_to=...)`.
   No flag on the request verbs — a boolean that flips the return type
   (`str | InvocationHandle`) needs `Literal` overload soup, breaks inference through
   wrappers, and has no precedent in any mature messaging SDK.
2. The per-call `reply_topic` param is **removed** from the request/reply pair (both overload
   sets + impls). They always reply to the client's own inbox — the only combination that can
   resolve. Hard break, no validation shim.
3. The return-address param is named **`reply_to`** — the literal field name in AMQP, JMS,
   and FastStream's `publish(reply_to=)`; avoids implying the *client* receives the reply.
4. The methods are renamed **`emit_to_node` → `send`**, **`invoke_node` → `start`**,
   **`execute_node` → `execute`** (Set A):
   - `send` — the standard one-way command verb (MassTransit/NServiceBus/JMS `Send`);
     `send(reply_to=)` is the literal AMQP/JMS/FastStream shape.
   - `start` — the one verb that unambiguously means "returns a handle, doesn't wait"
     (Temporal `start_workflow` → handle; cf. Python `Executor.submit` → `Future`).
   - `execute` — dispatch and await the result (Temporal `execute_workflow`); already the
     right verb, now paired so the `start`/`execute` duo telegraphs the contract.
   - `_node` suffix dropped everywhere (v1 doc direction). No deprecated aliases (pre-1.0,
     hard-break convention).

## API changes

```python
# ONE-WAY (renamed; gains the return address; return type unchanged)
async def send(
    self, user_prompt: str, topic: str, *,
    reply_to: str | None = None,          # NEW: topic the terminal result is delivered to,
                                          # point-to-point, for SOMEONE ELSE to consume.
                                          # None (default) = no callback at all.
    tool_overrides=..., correlation_id=..., temp_instructions=...,
    message_history=..., deps=..., model_settings=..., author=...,
    route=..., body=...,
) -> str: ...                             # still always the correlation_id

# REQUEST/REPLY (renamed; lose the footgun; otherwise unchanged)
async def start(self, user_prompt, topic, *, ...) -> InvocationHandle: ...
async def execute(self, user_prompt, topic, *, ..., timeout=None) -> NodeResult: ...
# `reply_topic` removed from both impls and all four overload stanzas.
# `output_type` stays OFF send: deserialization belongs to whoever consumes the reply.
# InvocationHandle keeps its name (it is still a handle to an invocation; cf. Temporal's
# WorkflowHandle). The client `reply_topic` property (inbox name) is unchanged.
```

## Pseudocode

`calfkit/client/client.py` — `send` (was `emit_to_node`) threads the new param through:

```python
async def send(self, user_prompt, topic, *, reply_to=None, ...) -> str:
    correlation_id, state, overrides = self._build_state_and_overrides(...)  # unchanged
    return await self._send(
        topic=topic, correlation_id=correlation_id, state=state,
        overrides=overrides, deps=deps, route=route, body=body,
        reply_to=reply_to,                                   # NEW
    )
```

`calfkit/client/base.py` — `_send` (was `_emit`) validates and forwards as the wire callback:

```python
async def _send(self, topic, correlation_id, state, overrides=None, deps=None,
                route=None, body=None, reply_to: str | None = None) -> str:
    if reply_to is not None:
        if not reply_to.strip():
            raise ValueError("reply_to must be a non-empty topic name or None")
        if not _KAFKA_TOPIC_RE.fullmatch(reply_to) or reply_to in (".", ".."):
            # Kafka legality ([a-zA-Z0-9._-]{1,249}): an illegal name would never get
            # metadata at the worker's terminal publish — reject loudly here instead.
            raise ValueError(f"reply_to {reply_to!r} is not a valid Kafka topic name (...)")
        if reply_to == self._reply_topic:
            raise ValueError(
                "reply_to is this client's own reply inbox; send() registers no "
                "future, so the reply would arrive and be dropped ('no pending future'). "
                "Use start()/execute() to await a reply."
            )
    await self._publish_call(
        topic=topic, correlation_id=correlation_id,
        callback_topic=reply_to,                              # was hardcoded None
        state=state, overrides=overrides, deps=deps, route=route, body=body,
    )
    return correlation_id
```

`calfkit/client/client.py` — `start` / `execute` (were `invoke_node` / `execute_node`)
lose the param:

```python
async def start(self, user_prompt, topic, *, ...):            # no reply_topic param
    correlation_id, state, overrides = self._build_state_and_overrides(...)
    return await self._start(
        topic=topic,                                          # callback is always the own
        correlation_id=correlation_id, state=state,           # inbox, hardwired in _start
        overrides=overrides,
        deps=deps, route=route, body=body, output_type=output_type,
    )

async def execute(self, user_prompt, topic, *, ..., timeout=None):
    handle = await self.start(...)                            # unchanged sugar
    return await handle.result(timeout=timeout)
```

Internal seams renamed for coherence (underscore-private, pre-1.0): `_emit` → `_send`,
`_invoke` → `_start`. `_publish_call` and `_build_state_and_overrides` keep their names —
they describe what they do, not a public verb. **Round-1 review amendment (2026-06-11):**
`_start` lost its `reply_topic` parameter entirely — the wire callback is hardwired to
`self._reply_topic`. A subclass passing a foreign topic through the seam would recreate the
exact dangling-future bug this spec removes, and with one legal value the parameter was
decorative.

`calfkit/nodes/base.py` (worker) — the `ReturnCall` branch already publishes the terminal
envelope to any non-`None` `frame.callback_topic` (`nodes/base.py:293-299`); the same
`publish_envelope` object is still returned for the `publish_topic` broadcast, so both
channels carry byte-identical terminal envelopes. `TailCall` re-pushes the inherited callback
(`nodes/base.py:304`), so a `reply_to` survives tail chains exactly like the client inbox does.
**Round-1 review amendment (2026-06-11):** the terminal callback publish is wrapped in a
narrow `except KafkaError` — a failed point-to-point delivery (e.g. a `reply_to` topic
missing with auto-create off, or unauthorized) logs an ERROR naming the topic, correlation
id, and node, and still returns `publish_envelope` so the `publish_topic` broadcast (the
documented traceability fallback) survives the failure instead of dying with it. Without the
catch, the raised exception aborted the handler before its return value reached the
`@publisher`, so a failed `reply_to` delivery silently erased BOTH channels (and FastStream's
default `ACK_FIRST` had already committed the offset — no redelivery). No retry/DLQ here:
redelivery policy belongs to the fault rail (#193 successor). The same catch also enriches
the receiver-side `no pending future` warning with the emitter id (`reply_dispatcher.py`) so
a mis-addressed `send(reply_to=<other client's inbox>)` is diagnosable.

## Semantics

- A `@consumer` node subscribed to the `reply_to` topic receives the terminal envelope exactly
  as one tapping `publish_topic` does (the reply dispatcher already proves callback-channel
  envelopes parse; same `Envelope`, same `NodeResult.from_envelope` path) — except it sees
  **only terminal results addressed to it**, not every hop of every invocation of the node.
- The reply still ALSO rides the target node's `publish_topic` broadcast (unchanged dual-channel
  model from #132).
- `reply_to` pointing at another *agent node's* input topic is not a chaining mechanism — the
  call stack is unwound at the terminal; consumers, sinks, or other clients' inboxes are the
  intended audience. Documented, not validated (a topic name is just a topic name).

## Validation rules

| Input | Behavior |
|---|---|
| `send(reply_to=None)` (default) | callback suppressed — today's emit, byte-for-byte |
| `send(reply_to="orders.done")` | `CallFrame.callback_topic="orders.done"`, no future |
| `send(reply_to="")` / whitespace | `ValueError` (would publish to an invalid/empty topic at the worker) |
| `send(reply_to=<Kafka-illegal name>)` | `ValueError` — names outside `[a-zA-Z0-9._-]{1,249}` (or `.`/`..`) would stall `request_timeout_ms` at the worker into an argument-less `UnknownTopicOrPartitionError`; rejected at call time instead (round-1 amendment) |
| `send(reply_to=client.reply_topic)` | `ValueError` — provably useless: the own-inbox dispatcher would consume and drop it ("no pending future" warning); awaiting is what start/execute are for |
| `start(reply_topic=...)` / `execute(reply_topic=...)` | `TypeError` (param gone) |
| `emit_to_node` / `invoke_node` / `execute_node` | `AttributeError` (renamed, no aliases) |

## Test plan (TDD order, all in `tests/test_fire_and_forget.py` unless noted)

Each written first, watched RED, then implemented:

1. `test_send_reply_to_sets_callback_topic_on_wire` — TestKafkaBroker capture; bottom
   `CallFrame.callback_topic == "sink.topic"`. RED: `TypeError`/`AttributeError`.
2. `test_send_reply_to_registers_no_future` — `dispatcher._pending` stays empty (mirrors
   existing zero-state test at `test_fire_and_forget.py:126`).
3. `test_send_reply_to_delivers_terminal_to_topic` (e2e) — agent worker + subscriber on the
   `reply_to` topic receives the terminal envelope with `final_output_parts` populated; the
   client's own inbox receives nothing (mirrors the broadcast e2e at `:164`).
4. `test_send_reply_to_rejects_blank_topic` — `ValueError`.
5. `test_send_reply_to_rejects_own_inbox` — `ValueError`.
6. `test_start_callback_is_always_client_inbox` — wire capture: `start` produces
   `CallFrame.callback_topic == client.reply_topic`; `reply_topic=` kwarg raises `TypeError`.
7. Rename sweep: mechanical update of every `emit_to_node`/`invoke_node`/`execute_node` call
   site in tests (15 files) and examples (4 files); suite green proves behavior is untouched
   by the rename.
8. Regression: existing send-default/start/execute behavior tests stay green with only the
   name swap (default paths byte-for-byte unchanged).

## Docs plan

- `docs/client-features.md` — patterns table (`:10-21`): rename all three + the `send` row
  gains "optional `reply_to` return address"; fire-and-forget section (`:66-91`) renamed to
  `send` + gains a `reply_to` sub-example; `reply_ttl` note (`:93-106`) reworded to `start`.
- `README.md` — the two `execute_node` examples (`:175`, `:221`) and the quick-reference
  table (`:279-280`).
- `examples/quickstart/{emit.py → send.py, invoke.py}`, `examples/multi_agent_panel/run.py`,
  `examples/quickstart/weather_sink.py` — rename call sites (and the example filenames: `emit.py` → `send.py`, `invoke.py` → `execute.py` — the latter demonstrates `execute`).
- `docs/designs/fire-and-forget-emit.md` — superseded-in-part note: the method is now
  `send`, and "`reply_topic` is meaningless for emit" no longer holds; link here.
- `docs/designs/calfkit-v1-design.md` — update the migration table (`:3054`, `:3431`):
  v1 targets are now `send`/`start`/`execute`, not `invoke`/`execute`.
- Docstrings: `Client` class pattern table, all three methods, `BaseClient._send`/`._start`,
  `InvocationHandle.result` (its `RuntimeError` note references `emit_to_node`).
- ADR at PR time: "the return address rides the one-way verb (`send`); request verbs
  (`start`/`execute`) always reply to the caller's own inbox" + the naming decision.

## Migration (breaking notes)

| Before | After |
|---|---|
| `await client.emit_to_node(p, t)` | `await client.send(p, t)` |
| *(not expressible)* | `await client.send(p, t, reply_to="orders.done")` |
| `await client.invoke_node(p, t)` | `await client.start(p, t)` |
| `await client.execute_node(p, t, timeout=30)` | `await client.execute(p, t, timeout=30)` |
| `invoke_node(reply_topic=...)` / `execute_node(reply_topic=...)` | drop the argument — if you passed a custom value you were leaking an unresolvable future; use `send(reply_to=...)` and consume the reply where you sent it |

No wire migration: workers already handle arbitrary callback topics; mixed-version fleets
are unaffected. No deprecated aliases (pre-1.0 hard-break convention).

## Conscious decisions

- **`reply_to` topic provisioning is the consumer-owner's responsibility.** The client never
  subscribes to it and doesn't own it; it is not declared into the client's startup ensurer
  (which runs at broker start, before per-call topics are known). On auto-create-off brokers
  the topic must exist before the worker's terminal publish.
- **Own-inbox `ValueError` over a warning.** With no future, an own-inbox reply is consumed
  and dropped by the client's own dispatcher — there is no scenario where it is useful, so
  fail loudly at call time rather than warn per-reply at the dispatcher.
- **`output_type` stays off `send`.** Deserialization is the consumer's concern; adding it
  would re-create the "param that silently does nothing" class this spec deletes.
- **Rename and `reply_to` land in one `feat!` PR.** One breaking PR, one migration story; the
  rename blast radius overlaps almost entirely with files the `reply_to` change touches.

## Out of scope

- Pruning `execute` (orthogonal surface question; deliberately untouched).
- The dead node-side `Emit` action (`actions.py:95`, zero construction sites): deleting or
  wiring it is an open follow-up from #132 — not decided here; left untouched.
- `reply_to` on node-side actions (`Emit` action wiring — still deferred from #132).
- Per-call provisioning of `reply_to` topics.
- Signature changes beyond the above (e.g. v1's planned `user_prompt` decoupling).
