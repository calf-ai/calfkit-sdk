# How to gate node invocations

When multiple agents share an input topic (each with its own consumer group),
every agent receives every message published to that topic. A **gate stack** lets
a node decide whether to handle an inbound event *before* `run()` runs — avoiding
wasted LLM tokens on messages addressed elsewhere.

Gates are predicates: `Callable[[SessionRunContext], bool | Awaitable[bool]]`.
They stack with **AND semantics** in registration order and short-circuit on the
first `False`, exception, or non-bool return. When any gate rejects, `run()` is
skipped and the envelope is returned unchanged — the Kafka offset still commits.

## Constructor form

Good for shared, cross-cutting predicates passed in as values:

```python
def is_scheduler_target(ctx) -> bool:
    discord = ctx.deps.get("discord", {})
    return discord.get("slash_target") == "scheduler"

scheduler = Agent(
    "scheduler",
    subscribe_topics="discord.thread.123",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-nano"),
    gates=[is_scheduler_target],
)
```

## Decorator form

Good for node-specific gates defined inline:

```python
scheduler = Agent("scheduler", subscribe_topics="discord.thread.123", model_client=...)

@scheduler.gate
def is_scheduler_target(ctx) -> bool:
    discord = ctx.deps.get("discord", {})
    return discord.get("slash_target") == "scheduler"
```

Constructor and decorator forms can be combined; constructor gates run first.

## Guarantees & guidance

- **Idempotency requirement:** Kafka may redeliver an event before its offset
  commits, so gates may run more than once for the same logical message. Keep
  gate functions deterministic and side-effect-free.
- **Failure behavior:** if a gate raises or returns a non-bool, the framework
  logs the failure and rejects the message (fail-safe). Place cheap fast-reject
  gates first to maximize short-circuit efficiency.
- **Tool-node gating:** pass `gates=[...]` to `ToolNodeDef.create_tool_node(...)`
  directly; the `@agent_tool` decorator doesn't expose `gates=` because tool
  topics are typically 1:1.

See also: [Consumer nodes](consumer-nodes.md) for gating a sink, and
[Client-side features](client-features.md) for the invocation patterns gates
filter.
