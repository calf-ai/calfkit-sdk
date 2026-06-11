# How to call nodes from a client

The `Client` supports multi-turn conversations, runtime dependency injection,
temporary instruction overrides, and one-way dispatch with an optional return
address — all without redeploying the agent.

See also: [CLI reference](cli.md) · [Worker lifecycle](worker-lifecycle.md) ·
[Consumer nodes](consumer-nodes.md) · [Gating](gating.md).

## Invocation patterns

`Client` offers three ways to call a node:

| Method | Pattern | Returns |
| --- | --- | --- |
| `execute(...)` | Request/reply — publish and await the result in one call. | `NodeResult` |
| `start(...)` | Publish and get a handle; `await handle.result()` later. | `InvocationHandle` |
| `send(...)` | One-way — dispatch and return immediately; no reply future. Optional `reply_to` return address. | `correlation_id` (str) |

Use `send` for one-way sends (fire-and-forget, or with a `reply_to` topic some
other consumer owns), `start` for async dispatch with a handle to await later,
and `execute` for synchronous request/reply. Replies for `start`/`execute` are
always delivered to the client's own reply inbox — the only address whose reply
future can resolve.

## Multi-turn conversations

Pass the message history from a previous result to maintain context:

```python
result = await client.execute("What's the weather in Tokyo?", "agent.input")

# Continue the conversation with full context
result = await client.execute(
    "How about in Osaka?",
    "agent.input",
    message_history=result.message_history,
)
```

The same `message_history` can carry turns from *multiple* agents — see
[`examples/multi_agent_panel/`](../examples/multi_agent_panel/) for a multi-agent
discussion over one shared transcript.

## Runtime dependency injection

Pass runtime data to tools via the `deps` parameter:

```python
result = await client.execute(
    "What's my phone number?",
    "agent.input",
    deps={"user_id": "usr_123"},  # Available to tools via ctx.deps["user_id"]
)
```

## Temporary instructions

Temporarily add system-level instructions scoped per request:

```python
result = await client.execute(
    "What's the weather in Tokyo?",
    "agent.input",
    temp_instructions="Always respond in Japanese.",
)
```

## One-way sends

Dispatch work to a node without registering a reply future via `send`:

```python
correlation_id = await client.send(
    "Re-index the catalog.",
    "indexer.input",
)
# Returns the correlation_id immediately; no reply is produced and no
# client-side reply future is allocated.
```

`send` takes the same input-shaping arguments as `start` (`deps`,
`temp_instructions`, `message_history`, `route`, `body`, `model_settings`,
`tool_overrides`, `author`, `correlation_id`) — but no `output_type`, since
deserialization belongs to whoever consumes the result.

### Directing the result with `reply_to`

Pass a `reply_to` topic to have the worker deliver the terminal result there,
point-to-point — the [Return Address pattern](https://www.enterpriseintegrationpatterns.com/patterns/messaging/ReturnAddress.html).
The consumer of that topic is **someone else**: a [consumer node](consumer-nodes.md),
a sink service, another system — never the sending client, which has no reply
future to resolve.

```python
correlation_id = await client.send(
    "Re-index the catalog.",
    "indexer.input",
    reply_to="indexer.done",  # terminal result lands here, point-to-point
)
```

A `@consumer` on `indexer.done` receives the terminal result exactly as one
tapping a `publish_topic` does — but it sees only terminals addressed to it,
not every hop of every invocation of the node. Two caveats:

- `reply_to` is not a chaining mechanism: the call stack is unwound at the
  terminal, so address consumers/sinks, not agent input topics.
- The `reply_to` topic is owned by its consumer — on brokers with auto-create
  disabled, it must exist before the worker's terminal publish.

`send(reply_to=client.reply_topic)` raises `ValueError`: with no future
registered the client's own dispatcher would consume and drop the reply. Use
`start`/`execute` to await a reply.

### Traceability without a reply

With or without `reply_to`, the terminal result also rides the target node's
`publish_topic` broadcast stream. Set a `publish_topic` on the node you send to
and tap it with a [consumer node](consumer-nodes.md) to observe terminals
(`result.output` is populated exactly as it is for `execute`). With
`reply_to=None`, a node with no `publish_topic` produces no observable record
for the send — there is neither a reply nor a broadcast.

## Bounding `start` memory

Each pending `start` handle holds a reply future until it resolves. If a
reply is lost or a handle is abandoned, that future leaks. Pass an opt-in TTL to
bound it:

```python
client = Client.connect("localhost:9092", reply_ttl=30.0)
```

When set, an unanswered handle is evicted after `reply_ttl` seconds and
`handle.result()` raises `ReplyExpiredError` (importable from
`calfkit.exceptions`). The default (`None`) waits indefinitely. `send`
allocates no future, so the TTL does not apply to it.
