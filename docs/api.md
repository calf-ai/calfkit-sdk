# Calfkit API reference

The full public surface is re-exported from the top-level `calfkit` package:

```python
from calfkit import (
    Client, InvocationHandle, InvocationResult,          # client
    Agent, agent_tool, consumer,                         # node authoring
    BaseNodeDef, NodeDef, ToolNodeDef, ConsumerNode,     # node types
    ConsumerFn, GateFunction,                            # node typing helpers
    ToolContext,                                         # tool-side context
    OpenAIModelClient, OpenAIResponsesModelClient, AnthropicModelClient,  # providers
    Worker, LifecycleContext, ResourceSetupContext, ServingContext,       # worker + lifecycle
    ProvisioningConfig,                                  # provisioning (config only)
    DeserializationError, LifecycleConfigError, ToolExecutionError,       # exceptions
)
```

`calfkit.__version__` exposes the installed package version.

## Public surface

### Client and results

| Symbol | Purpose |
| --- | --- |
| `Client` | High-level client for invoking nodes over the broker. |
| `InvocationHandle` | Handle returned by `Client.start()`; its `result()` awaits the reply. |
| `InvocationResult` | Client-facing projection of a node's session state after it returns (`output`, `state`, `correlation_id`, …). |

### Node authoring

| Symbol | Purpose |
| --- | --- |
| `Agent` | An agent node — an LLM-backed node that consumes prompts, calls tools, and publishes output. |
| `agent_tool` | Decorator that turns a function into a deployable tool node. |
| `consumer` | Decorator that turns a function into a deployable consumer node (a terminal sink on a topic). |

### Node types

The definition types that node authoring produces or builds on.

| Symbol | Purpose |
| --- | --- |
| `BaseNodeDef` | Common base type for every node definition; the element type of `Worker(nodes=[...])`. |
| `NodeDef` | Base class for a custom node — subclass and override `run()` (or add `@handler(route)` methods) for logic that is neither an agent nor a tool. Generic over the node's output type; also directly instantiable. |
| `ToolNodeDef` | The node-definition type produced by `@agent_tool`. |
| `ConsumerNode` | The node-definition type produced by `@consumer`. |

### Node typing helpers

| Symbol | Purpose |
| --- | --- |
| `ConsumerFn` | Type alias for a consumer's callable: `Callable[[ConsumerContext[OutputT]], None \| Awaitable[None]]`. |
| `GateFunction` | Type alias for a gate predicate: `Callable[[BaseSessionRunContext], bool \| Awaitable[bool]]`. |

### Tool-side context

| Symbol | Purpose |
| --- | --- |
| `ToolContext` | Run context passed to a tool node's function; exposes `deps`, `resources`, and the call metadata. |

### Providers

| Symbol | Purpose |
| --- | --- |
| `OpenAIModelClient` | Model client for the OpenAI Chat Completions API. |
| `OpenAIResponsesModelClient` | Model client for the OpenAI Responses API. |
| `AnthropicModelClient` | Model client for the Anthropic API. |

### Worker and lifecycle

| Symbol | Purpose |
| --- | --- |
| `Worker` | Hosts one or more nodes against the broker and manages their lifecycle. |
| `LifecycleContext` | Context for `on_startup` / `after_shutdown` hooks; resources are writable. |
| `ResourceSetupContext` | Context for a `@resource` body; resources are read-only-typed. |
| `ServingContext` | Context for `after_startup` / `on_shutdown` hooks; read-only resources plus the broker. |

### Provisioning

| Symbol | Purpose |
| --- | --- |
| `ProvisioningConfig` | Opt-in configuration for best-effort Kafka topic auto-creation. The full provisioning surface lives at `calfkit.provisioning`. |

### Exceptions

| Symbol | Purpose |
| --- | --- |
| `DeserializationError` | Raised when client-side output deserialization fails. |
| `LifecycleConfigError` | Raised when a node or worker lifecycle configuration is invalid. |
| `ToolExecutionError` | Raised on the client when a tool node fails while executing. |

## Key entry points

### `Client.connect`

```python
Client.connect(
    server_urls: str | Iterable[str] | None = None,
    reply_topic: str | None = None,
    reply_ttl: float | None = None,
    *,
    provisioning: ProvisioningConfig | None = None,
    **broker_kwargs,
) -> Client
```

Connect to the broker and return a `Client`. `server_urls` defaults to the `CALF_HOST_URL` environment variable, then `"localhost"`. `reply_topic` defaults to a generated per-client reply inbox. `reply_ttl` bounds how long an un-answered reply future is retained before it expires.

### `Client.execute`

```python
await Client.execute(
    user_prompt: str,
    topic: str,
    *,
    author=None, tool_overrides=None, output_type=..., correlation_id=None,
    temp_instructions=None, message_history=None, deps=None, model_settings=None,
    route=None, body=None, timeout=None,
) -> InvocationResult
```

Request/reply: publish `user_prompt` to `topic` and await the `InvocationResult`. `output_type` selects how the reply is deserialized; when omitted, the type is auto-detected.

### `Client.start`

```python
await Client.start(...) -> InvocationHandle
```

The async-handle variant of `execute`. Takes the same arguments except `timeout`, and returns an `InvocationHandle` whose `result()` is awaited separately for the reply.

### `Client.send`

```python
await Client.send(
    user_prompt: str,
    topic: str,
    *,
    reply_to: str | None = None,
    ...,
) -> str
```

One-way send. Publishes `user_prompt` to `topic` and returns the correlation id; no reply future is created. Pass `reply_to` to route any reply to another address for a different node or consumer to handle.

### `Agent`

```python
Agent(
    node_id: str,
    *,
    system_prompt: str = "You are a helpful AI assistant.",
    subscribe_topics: str | list[str],
    publish_topic: str | None = None,
    gates: list[GateFunction] | None = None,
    tools: Sequence[...] | None = None,
    model_client: PydanticModelClient,
    final_output_type: type = str,
    sequential_only_mode: bool = False,
    model_settings: ModelSettings | dict | None = None,
)
```

An agent node. Consumes prompts from `subscribe_topics`, calls its `tools`, and publishes its output to `publish_topic` (when set). `final_output_type` enforces a structured output type (default: plain `str`).

### `@agent_tool`

```python
@agent_tool
def get_weather(location: str) -> str:        # -> ToolNodeDef
    """Get the current weather at a location."""
    ...
```

Turns a function into a tool node. The function's parameters and type annotations become the tool's argument schema, and its docstring becomes the description shown to the calling LLM. Declare an optional first `ctx: ToolContext` parameter to receive the [context](#context-objects) — it is hidden from the LLM schema. The return value must be JSON-serializable. Sync and async functions are both supported. The node's topics default to `tool.<function-name>.input` and `tool.<function-name>.output`.

### `@consumer`

```python
@consumer(subscribe_topics="agent.output", agent_output_type=str)
async def log_output(ctx: ConsumerContext) -> None: ...   # -> ConsumerNode
```

Turns a function into a consumer node — a terminal sink. The function receives a [`ConsumerContext`](#context-objects). Set `agent_output_type` to deserialize `ctx.output` into a specific type; `gates` accept or decline the inbound event before the body runs.

### `Worker`

```python
Worker(
    client: Client,
    nodes: list[BaseNodeDef] | None = None,
    max_workers: int = 1,
    group_id: str | None = None,
    ...,
)
```

Hosts the given `nodes` against the broker. Drive it with `await worker.run()` (blocking), the embeddable `await worker.start()` / `await worker.stop()` pair, or `async with worker:`.

## Context objects

A node's own code receives a context object as its parameter. The shape depends on the kind of node:

| Your code | Receives | Import from |
| --- | --- | --- |
| `@agent_tool` function | `ToolContext` (optional first parameter) | `calfkit.models` |
| `@consumer` function | `ConsumerContext` | `calfkit.models` |
| gate predicate (`gates=[...]`) | `SessionRunContext` | `calfkit.models` |
| lifecycle hooks / `@resource` | `LifecycleContext` / `ServingContext` / `ResourceSetupContext` | `calfkit.worker` |

### `ToolContext`

The context injected as the first parameter of an `@agent_tool` function (when declared). It subclasses pydantic-ai's `RunContext[dict]` and is hidden from the LLM tool schema.

| Attribute | Type | Description |
| --- | --- | --- |
| `deps` | `Mapping[str, Any]` | Producer-supplied dependencies — the mapping passed to `Client.execute(deps=...)` (or `start` / `send`). Must be JSON-serializable. Read as `ctx.deps["key"]`. |
| `resources` | `Mapping[str, Any]` | The owning node's lifecycle-managed resources (read-only). Read as `ctx.resources["key"]`. Empty when the node owns none. |
| `correlation_id` | `str` | The correlation id for this invocation. |
| `agent_name` | `str \| None` | Node id of the calling agent. |

Run metadata inherited from pydantic-ai's `RunContext` — for example `tool_name`, `tool_call_id`, and `retry` — is also available.

### `ConsumerContext`

The context handed to a `@consumer` function, once per inbound envelope. It, its `state`, and its `deps` are read-only.

| Attribute | Type | Description |
| --- | --- | --- |
| `output` | `OutputT \| None` | Deserialized final output (typed via the consumer's `agent_output_type`). `None` on intermediate hops that carry no reply. |
| `state` | `State` | Full session state at this hop (message history, tool calls/results, metadata). |
| `correlation_id` | `str` | The correlation id that ties this hop to its invocation. |
| `output_parts` | `list[ContentPart]` | The raw reply parts this observation was projected from; `[]` on an intermediate hop. |
| `emitter_node_id` | `str \| None` | Node id of the upstream emitter. |
| `emitter_node_kind` | `str \| None` | Coarse classification of the emitter. |
| `deps` | `Mapping[str, Any]` | Inbound producer dependencies; read as `ctx.deps["key"]`. |
| `resources` | `Mapping[str, Any]` | The consuming node's lifecycle-managed resources (read-only). |
| `message_history` | `list[ModelMessage]` | Convenience for `state.message_history`. |
| `metadata` | `Any` | Convenience for `state.metadata`. |

The contexts for **gate predicates** (`SessionRunContext`) and **worker lifecycle hooks** (`LifecycleContext` / `ServingContext` / `ResourceSetupContext`) are described in [How to gate node invocations](gating.md) and [Worker lifecycle & embedding](worker-lifecycle.md).

## Other public modules

The top-level package re-exports the symbols above. A few public capabilities live in submodules:

| Import from | Provides |
| --- | --- |
| `calfkit.models` | `ConsumerContext` — the value passed to a `@consumer` function (`output`, `correlation_id`, `deps`, `resources`) — plus the wire/state models (`State`, `Envelope`, `ToolBinding`, …). |
| `calfkit.mcp` | `MCPToolboxNode`, `MCPToolbox`, `StdioServerParameters`, `StreamableHttpParameters` — `MCPToolboxNode` hosts an MCP server's tools as a deployable node; `MCPToolbox` is the name-only handle agents put in `tools=[...]`. |
| `calfkit.provisioning` | The full topic-provisioning surface: `TopicProvisioner`, `provision_topics`, `topics_for_nodes`, `ProvisionReport`, `StartupTopicEnsurer`, `MissingTopicsError`. |
| `calfkit.exceptions` | The complete exception set, including `ReplyExpiredError` (raised by `execute` / `start` when a `reply_ttl` elapses), `RegistryConfigError`, and `MissingTopicsError`. |

## See also

- **[How to call nodes from a client](client-features.md)** — the `execute` / `start` / `send` patterns, multi-turn conversations, dependency injection, and reply memory.
- **[How to tap a topic with a consumer node](consumer-nodes.md)** — terminal sinks built with `@consumer`.
- **[How to gate node invocations](gating.md)** — predicate gate stacks (`GateFunction`).
- **[How to give agents MCP tools](mcp-tool-discovery.md)** — fronting an MCP server as a toolbox.
- **[Worker lifecycle & embedding](worker-lifecycle.md)** — `Worker`, the lifecycle contexts, and `@resource`.
- **[CLI reference](cli.md)** — the `calfkit run` and `calfkit topics` commands.
- **[Topic provisioning](topic-provisioning.md)** — `ProvisioningConfig` and the opt-in topic-creation helper.

Full signatures and behavior live in the source docstrings.
