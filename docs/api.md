# Calfkit API reference

The full public surface is re-exported from the top-level `calfkit` package:

```python
from calfkit import (
    Client, InvocationHandle, InvocationResult,          # client
    Agent, agent_tool, consumer,                         # node authoring
    BaseNodeDef, NodeDef, ToolNodeDef, ConsumerNode,     # node types
    ConsumerFn,                                          # node typing helpers
    ToolContext,                                         # tool-side context
    OpenAIModelClient, OpenAIResponsesModelClient, AnthropicModelClient,  # providers
    Worker, LifecycleContext, ResourceSetupContext, ServingContext,       # worker + lifecycle
    ProvisioningConfig,                                  # provisioning (config only)
    NodeFaultError, ErrorReport, FaultTypes,             # faults
    DeserializationError, LifecycleConfigError,          # exceptions
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
| `NodeFaultError` | A terminal typed fault — `raise NodeFaultError(error_type, ...)` from node or seam code to mint one; carries an `ErrorReport` on `.report`. See [Errors & faults](#errors--faults). |

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

Turns a function into a consumer node — a terminal sink. The function receives a [`ConsumerContext`](#context-objects). Set `agent_output_type` to deserialize `ctx.output` into a specific type. A consumer is an observer — it handles every inbound event and cannot register policy seams.

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

## Policy seams

Caller-capable nodes (`Agent`, `NodeDef`, tool nodes) expose four **policy seams** — callbacks that run inside the message flow to guard input, reshape output, and handle failures. Each is registered as a constructor argument (`Agent`/`NodeDef`; a single callable or a list) or as a repeatable instance decorator (any node — and the only form for tool nodes); constructor entries precede decorator entries. A chain runs in registration order, resolving on the **first non-`None` return** (sync or async handlers). Observer nodes (`@consumer`) have no seams.

| Seam | Handler signature | A `None` return… | A non-`None` return… |
| --- | --- | --- | --- |
| `before_node` | `(ctx)` | proceeds to the node body | short-circuits the body; the value becomes the node's output |
| `after_node` | `(ctx, output)` | keeps the produced output | replaces the output (output values only) |
| `on_node_error` | `(ctx, fault)` | escalates the original fault | recovers — the value becomes the node's output |
| `on_callee_error` | `(ctx, fault)` | the failed call escalates | substitutes a result for that call |

`ctx` is a [`SeamContext`](#seamcontext); for the error seams, `fault` is an [`ErrorReport`](#errors--faults). (Returning a node *action* — a `Call`/`ReturnCall`/…, the kind a node body returns to dispatch work — is accepted from `before_node` but raises `SeamContractError` from `after_node`.)

**Minting:** `raise NodeFaultError(error_type, ...)` from any seam (or the node body) converts to a fault verbatim and **bypasses `on_node_error`** (the mint rule). Inside `on_node_error` / `on_callee_error`, raising `NodeFaultError` mints; raising any *other* exception is treated as that handler declining.

Registration and recipes: [How to guard and transform node invocations](policy-seams.md) and [How to handle errors and faults](error-handling.md).

## Errors & faults

A failure surfaces as a typed [`ErrorReport`](#errorreport) that travels the result rail and escalates up the call chain. Faults are handled **inside a node** (the `on_node_error` / `on_callee_error` seams) or **observed** on a `publish_topic` tap; there is no client-side typed fault reception yet.

### `NodeFaultError`

```python
raise NodeFaultError(
    error_type: str,                          # an open dotted code you choose, e.g. "billing.quota_exceeded"
    *,
    message: str = "",
    retryable: bool = False,                  # advisory only — the framework does not auto-retry
    details: dict[str, Any] | None = None,    # must be JSON-serializable
)
```

Mint a deliberate typed fault from node or seam code. `error_type` must be non-empty and **must not** begin with the framework-reserved `calf.` prefix (nor may any `details` key); the resulting `ErrorReport` is carried on `NodeFaultError.report`.

### `ErrorReport`

The typed fault value (frozen). Match on it with `.find()`, never a bare `==` — faults compose, so a fan-out wraps siblings in a group and a top-level equality check would silently stop matching.

| Attribute | Type | Description |
| --- | --- | --- |
| `error_type` | `str` | Open dotted code (e.g. `calf.model.context_window_exceeded`); tolerate unknown values. |
| `message` | `str` | Human summary (clamped, never rejected). |
| `retryable` | `bool` | **Advisory only** — the framework never auto-retries on it. |
| `details` | `dict[str, Any]` | Open, JSON-serializable extension slot. |
| `causes` | `list[ErrorReport]` | Nested faults — non-empty for a fault group, conversion, or recovery-then-failure. |
| `frame_chain` | `list[FrameRef]` | Call-frame topology at fault time (`FrameRef` = `frame_id` + `target_topic`; no payloads). |
| `report_id` | `str` | Framework-minted UUID7, stable across hops; the dedup key. |
| `origin_node_id` / `origin_frame_id` | `str \| None` | Where the fault originated. |

| Method | Returns |
| --- | --- |
| `report.find(error_type)` | The first report in `walk()` order with that `error_type`, or `None`. |
| `report.walk()` | Iterator over this report then every nested cause (pre-order, cycle-guarded). |

### `FaultTypes`

Framework-minted `error_type` codes (all under the reserved `calf.` prefix), shipped as constants so you match without typing magic strings:

| Constant | Code | Meaning |
| --- | --- | --- |
| `FAULT_GROUP` | `calf.fault_group` | Multiple sibling faults composed under `causes` (e.g. a fan-out). |
| `UNHANDLED` | `calf.unhandled` | An arbitrary exception synthesized into a fault. |
| `FANOUT_ABORTED` | `calf.fanout.aborted` | A fan-out batch could not complete (`details["reason"]` says why). |
| `MODEL_CONTEXT_WINDOW_EXCEEDED` | `calf.model.context_window_exceeded` | The model's context window was exceeded. |
| `DELIVERY_REJECTED` | `calf.delivery.rejected` | A reply-owing delivery declined its body. |
| `DELIVERY_UNDECODABLE` | `calf.delivery.undecodable` | An inbound delivery could not be decoded. |
| `SLOT_MATERIALIZATION_FAILED` | `calf.slot.materialization_failed` | A seam substitute could not be materialized to wire parts. |
| `AGENT_SELF_RETRY_EXHAUSTED` | `calf.agent.self_retry_exhausted` | An agent exhausted its self-retry budget. |

`FaultTypes` also defines `details`-key constants (e.g. `SEAM_ERRORS`, `FANOUT_TOPOLOGY`); see the source for the full set.

## Context objects

A node's own code receives a context object as its parameter. The shape depends on the kind of node:

| Your code | Receives | Import from |
| --- | --- | --- |
| `@agent_tool` function | `ToolContext` (optional first parameter) | `calfkit.models` |
| `@consumer` function | `ConsumerContext` | `calfkit.models` |
| policy seam (`before_node` / `after_node` / `on_node_error` / `on_callee_error`) | `SeamContext` | received as `ctx` (not imported) |
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

### `SeamContext`

The context handed to every [policy seam](#policy-seams), received as `ctx` — it is **not** importable; annotate as needed or leave it untyped. Mutable on `state` only; the per-stage fields are set by the framework.

| Attribute | Type | Description |
| --- | --- | --- |
| `state` | `StateT` | App state — **mutable**; the official input-transform channel (mutate in place, return `None`). |
| `deps` | `Mapping[str, Any]` | Producer-supplied dependencies (read-only). |
| `resources` | `Mapping[str, Any]` | The node's lifecycle-managed resources (read-only). |
| `payload` | `Any \| None` | The inbound frame payload (a tool sees its `ToolCallRef`). |
| `route` | `str \| None` | Inbound route key — set on call-kind ingress only. |
| `delivery_kind` | `MessageKind` | `call` / `return` / `fault` — distinguishes ingress from continuation/closure firings. |
| `callee_results` | `list[CalleeResult]` | All resolved callee calls in dispatch order; `[]` on ingress. |
| `failing_call` | `CalleeResult \| None` | The failed call being handled — set during `on_callee_error` only. |
| `exception` | `BaseException \| None` | The live exception — set during `on_node_error` only. |

Also carries `node_id`, `correlation_id`, `emitter_node_id`, `awaiting_reply`, and the `callee_result` convenience (the single resolved call, for the non-fan-out case).

### Lifecycle contexts

Passed to the worker/node lifecycle hooks and `@resource` bodies. All three carry `owner` (the node or worker the hook was registered on); they differ in `resources` access and whether the live broker is present. Import from `calfkit.worker` (or `calfkit`).

| Context | Used by | `resources` | `broker` |
| --- | --- | --- | --- |
| `LifecycleContext` | `on_startup` / `after_shutdown` | **writable** (`ctx.resources["k"] = v`) | — |
| `ResourceSetupContext` | a `@resource` body | read-only | — |
| `ServingContext` | `after_startup` / `on_shutdown` | read-only | `ctx.broker` (live `KafkaBroker`) |

See [Worker lifecycle & embedding](worker-lifecycle.md) for the full lifecycle how-to.

## MCP toolboxes

Host an MCP server's tools as a deployable node and reference them from agents. The toolbox types are imported from `calfkit.mcp`.

| Symbol | Purpose |
| --- | --- |
| `MCPToolboxNode` | The deployable host — one per MCP server, placed in `Worker(nodes=[...])`. |
| `MCPToolbox` | A frozen, identity-only handle to a toolbox by name, placed in an agent's `tools=[...]`. |
| `StdioServerParameters` | Connection params for a local stdio (subprocess) MCP server. |
| `StreamableHttpParameters` | Connection params for a Streamable HTTP MCP server. |

### `MCPToolboxNode`

```python
MCPToolboxNode(
    name: str,
    connection_params: StdioServerParameters | StreamableHttpParameters,
)
```

Hosts the tools of one MCP server; `name` is the toolbox identity agents reference. `node.select(*, include: Sequence[str] | None = None) -> MCPToolbox` returns a handle to this toolbox, optionally scoped to `include` tool names.

### `MCPToolbox`

```python
MCPToolbox(name: str, include: tuple[str, ...] | None = None)
```

A frozen, identity-only handle to a toolbox. `name` must match the hosting `MCPToolboxNode`. `include` pins the tool names the agent may use; `None` exposes all of the toolbox's tools. The handle carries no connection params — passing connection details, or deploying a handle as a node, raises.

### Connection params

| Symbol | Key fields |
| --- | --- |
| `StdioServerParameters` | `command: str`, `args: list[str] = []`, `env`, `cwd`, `encoding` |
| `StreamableHttpParameters` | `url: str`, `headers`, `timeout: float = 30.0`, `auth` |

### `ControlPlaneConfig`

The worker's control-plane configuration (`from calfkit import ControlPlaneConfig`), used by toolbox discovery. Passed as `Worker(control_plane=ControlPlaneConfig(...))`; every field has a default. The control-plane topic name is fixed (`calf.capabilities`).

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `heartbeat_interval` | `float` | `30.0` | Seconds between a toolbox's liveness re-publishes. |
| `stale_after` | `float \| None` | `None` | Seconds after which an un-refreshed toolbox is hidden from agents; `None` uses the built-in default. |
| `catchup_timeout` | `float` | `30.0` | Seconds a worker waits at boot to catch up the control-plane view before serving. |
| `bootstrap_servers` | `str \| None` | `None` | Run the control plane on a separate Kafka cluster; `None` uses the worker's broker. |

See also: [How to give agents MCP tools](mcp-tool-discovery.md).

## Other public modules

The top-level package re-exports the symbols above. A few public capabilities live in submodules:

| Import from | Provides |
| --- | --- |
| `calfkit.models` | `ConsumerContext` — the value passed to a `@consumer` function (`output`, `correlation_id`, `deps`, `resources`) — plus the wire/state models (`State`, `Envelope`, `ToolBinding`, …). |
| `calfkit.mcp` | `MCPToolboxNode`, `MCPToolbox`, `StdioServerParameters`, `StreamableHttpParameters` — see [MCP toolboxes](#mcp-toolboxes). |
| `calfkit.provisioning` | The full topic-provisioning surface: `TopicProvisioner`, `provision_topics`, `topics_for_nodes`, `ProvisionReport`, `StartupTopicEnsurer`, `MissingTopicsError`. |
| `calfkit.exceptions` | The complete exception set, including `ReplyExpiredError` (raised by `execute` / `start` when a `reply_ttl` elapses), `RegistryConfigError`, and `MissingTopicsError`. |

## See also

- **[How to call nodes from a client](client-features.md)** — the `execute` / `start` / `send` patterns, multi-turn conversations, dependency injection, and reply memory.
- **[How to tap a topic with a consumer node](consumer-nodes.md)** — terminal sinks built with `@consumer`.
- **[How to guard and transform node invocations](policy-seams.md)** — `before_node` / `after_node` recipes.
- **[How to handle errors and faults](error-handling.md)** — `on_node_error` / `on_callee_error`, minting `NodeFaultError`, and inspecting an `ErrorReport`.
- **[How to give agents MCP tools](mcp-tool-discovery.md)** — fronting an MCP server as a toolbox.
- **[Worker lifecycle & embedding](worker-lifecycle.md)** — `Worker`, the lifecycle contexts, and `@resource`.
- **[CLI reference](cli.md)** — the `calfkit run` and `calfkit topics` commands.
- **[Topic provisioning](topic-provisioning.md)** — `ProvisioningConfig` and the opt-in topic-creation helper.

Full signatures and behavior live in the source docstrings.
