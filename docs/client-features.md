# How to call nodes from a client

The `Client` supports multi-turn conversations, runtime dependency injection,
temporary instruction overrides, and fire-and-forget dispatch — all without
redeploying the agent.

See also: [CLI reference](cli.md) · [Worker lifecycle](worker-lifecycle.md) ·
[Consumer nodes](consumer-nodes.md) · [Gating](gating.md).

## Invocation patterns

`Client` offers three ways to call a node:

| Method | Pattern | Returns |
| --- | --- | --- |
| `execute_node(...)` | Request/reply — publish and await the result in one call. | `NodeResult` |
| `invoke_node(...)` | Publish and get a handle; `await handle.result()` later. | `InvocationHandle` |
| `emit_to_node(...)` | Fire-and-forget — dispatch and return immediately, no reply. | `correlation_id` (str) |

Use `emit_to_node` for true one-way sends, `invoke_node` for async dispatch with
a handle to await later, and `execute_node` for synchronous request/reply.

## Multi-turn conversations

Pass the message history from a previous result to maintain context:

```python
result = await client.execute_node("What's the weather in Tokyo?", "agent.input")

# Continue the conversation with full context
result = await client.execute_node(
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
result = await client.execute_node(
    "What's my phone number?",
    "agent.input",
    deps={"user_id": "usr_123"},  # Available to tools via ctx.deps["user_id"]
)
```

## Temporary instructions

Temporarily add system-level instructions scoped per request:

```python
result = await client.execute_node(
    "What's the weather in Tokyo?",
    "agent.input",
    temp_instructions="Always respond in Japanese.",
)
```

## Fire-and-forget

Dispatch work to a node without waiting for (or producing) a reply via
`emit_to_node`:

```python
correlation_id = await client.emit_to_node(
    "Re-index the catalog.",
    "indexer.input",
)
# Returns the correlation_id immediately; no reply is produced and no
# client-side reply future is allocated.
```

`emit_to_node` takes the same input-shaping arguments as `invoke_node` (`deps`,
`temp_instructions`, `message_history`, `route`, `body`, `model_settings`,
`tool_overrides`, `author`, `correlation_id`) — but no `reply_topic` or
`output_type`, since there is nothing to route back or deserialize.

Because there's no reply, **traceability comes from the target node's
`publish_topic` broadcast stream**, not a point-to-point callback. Set a
`publish_topic` on the node you emit to and tap it with a
[consumer node](consumer-nodes.md) to observe terminals (`result.output` is
populated exactly as it is for `execute_node`). A node with no `publish_topic`
produces no observable record for a fire-and-forget send — there is neither a
reply nor a broadcast.

## Bounding `invoke_node` memory

Each pending `invoke_node` handle holds a reply future until it resolves. If a
reply is lost or a handle is abandoned, that future leaks. Pass an opt-in TTL to
bound it:

```python
client = Client.connect("localhost:9092", reply_ttl=30.0)
```

When set, an unanswered handle is evicted after `reply_ttl` seconds and
`handle.result()` raises `ReplyExpiredError` (importable from
`calfkit.exceptions`). The default (`None`) waits indefinitely. `emit_to_node`
allocates no future, so the TTL does not apply to it.
