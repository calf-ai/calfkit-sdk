# Calfkit API reference

The full public surface is re-exported from the top-level `calfkit` package:

```python
from calfkit import (
    Client, AgentGateway, InvocationHandle, InvocationResult,   # client
    Dispatch, EventStream, RunEvent, RunCompleted, RunFailed,   # client — fire token + firehose events
    AgentMessageEvent, ToolCallEvent, ToolResultEvent, HandoffEvent,  # intermediate step events (RunEvent members)
    Agent, agent_tool, consumer,                         # node authoring
    Tools,                                               # reference deployed tool nodes by name
    Messaging, Handoff,                                  # agent-to-agent peers
    BaseNodeDef, NodeDef, ToolNodeDef, ConsumerNode,     # node types
    ConsumerFn,                                          # node typing helpers
    ToolContext,                                         # tool-side context
    OpenAIModelClient, OpenAIResponsesModelClient, AnthropicModelClient,  # providers
    Worker, LifecycleContext, ResourceSetupContext, ServingContext,       # worker + lifecycle
    ProvisioningConfig,                                  # provisioning (config only)
    KTableReaderTuning, FanoutConfig,                    # ktables tuning (config only)
    NodeFaultError, ErrorReport, ExceptionInfo, FaultTypes,   # faults
    DeserializationError, ClientTimeoutError, ClientClosedError, LifecycleConfigError,   # exceptions
)
```

`calfkit.__version__` exposes the installed package version.

## Public surface

### Client and results

| Symbol | Purpose |
| --- | --- |
| `Client` | High-level client. `connect()` once, then mint a gateway per agent with `client.agent(name)`; `events()` is the cross-run firehose. |
| `AgentGateway` | A typed gateway to one agent (from `client.agent(...)`); speaks the verb triad `send` / `start` / `execute`. |
| `Dispatch` | What `send()` returns — a fire token carrying `.correlation_id` (deliberately not a handle: no `result()`). |
| `InvocationHandle` | Handle returned by `gateway.start()`; its `result()` awaits the reply and `stream()` yields the run's events. |
| `InvocationResult` | Client-facing projection of a node's session state after it returns (`output`, `state`, `correlation_id`, `message_history`, …). |
| `EventStream` | The `client.events()` firehose — an async context manager + async iterator of every reply on the inbox; `.dropped` counts shed events. |
| `RunEvent` | The closed union yielded by `stream()` / `events()`: the terminals `RunCompleted` / `RunFailed` plus the intermediate step events `AgentMessageEvent` / `ToolCallEvent` / `ToolResultEvent` / `HandoffEvent`. |

### Intermediate step events

Members of the `RunEvent` union, surfaced by `stream()` / `events()` as a run progresses (best-effort, at-most-once, **raw** — not `output_type`-coerced). All re-exported from `calfkit`.

| Symbol | `kind` | Emitted by | Carries |
| --- | --- | --- | --- |
| `AgentMessageEvent` | `"agent_message"` | an agent hop | `parts` — the hop's preamble text |
| `ToolCallEvent` | `"tool_call"` | the calling agent's dispatch hop | `tool_call_id`, `name`, `args: str \| dict \| None` — the **parsed dict** for a dispatched call; a denied (never-dispatched) call keeps the raw model emission |
| `ToolResultEvent` | `"tool_result"` | the **folding caller** — the agent that made the call mints the result as the reply folds (tool return, consulted peer's answer, and rejection alike) | `tool_call_id`, `name`, `parts`, `outcome: "success" \| "failed" \| "denied"` |
| `HandoffEvent` | `"handoff"` | an agent hop — emitted only when a transfer actually happens (a stale/invalid target is a rejection and streams as a `denied` `ToolResultEvent` pair instead) | `target`, `reason` |

Each is a **frozen** model discriminated on its `kind` literal, and also carries hop identity (`correlation_id`, `depth`, `frame_id`, `emitter`). A fifth type, `AgentThinkingEvent` (`kind="agent_thinking"`), is **defined but not emitted in v1** — it is not in the `RunEvent` union and not top-level re-exported; it lives at `calfkit.models.step` for forward compatibility.

### Node authoring

| Symbol | Purpose |
| --- | --- |
| `Agent` | An agent node — an LLM-backed node that consumes prompts, calls tools, and publishes output. |
| `agent_tool` | Decorator that turns a function into a deployable tool node (`@agent_tool(name=...)` or `agent_tool(func, name=...)` overrides its name). |
| `Tools` | Identity-only handle that references deployed tool nodes — by name, or every live one with `discover=True`; pass in `Agent(tools=[...])` to discover their schemas at runtime. |
| `consumer` | Decorator that turns a function into a deployable consumer node (a terminal sink on a topic). |

### Agent tool-error reception

The agent-only `on_tool_error` surface for converting a faulting tool result into an in-band, model-visible error (see [Policy seams](#policy-seams) and [How to handle errors and faults](error-handling.md)).

| Symbol | Purpose |
| --- | --- |
| `surface_to_model` | Zero-argument prebuilt `on_tool_error` handler: converts *every* tool failure into a model-visible error result (level-A text). Pass as `Agent(on_tool_error=[surface_to_model()])`. |
| `ToolCall` | calfkit's name for the vendored model-request tool-call type (`.tool_name` / `.args`) an `on_tool_error` handler receives — distinct from the wire `ToolCallPart` (`.kwargs`). |
| `render_fault_for_model` | `render_fault_for_model(report) -> str` — the level-A renderer: the top exception line (`exception.type: message`, or `message` alone for a framework fault). |
| `retry_text_part` | Build a `calf.retry`-marked text part; return it from a seam handler to show the model an error (the result streams as `outcome="failed"`). |
| `AgentSeamContext` | The agent-enriched [`SeamContext`](#seamcontext) (adds a lazy `ctx.tool_call`); the `ctx` an `on_tool_error` handler receives. |
| `ToolErrorHandler` | Type alias for an `on_tool_error` handler — `Callable[[ToolCall, AgentSeamContext, ErrorReport], …]`. |

### Agent-to-agent (peers)

| Symbol | Purpose |
| --- | --- |
| `Messaging` | Identity-only handle declaring peers an agent may **message** — consult and keep control — by name, or every live agent with `discover=True`; pass in `Agent(peers=[...])`. |
| `Handoff` | Identity-only handle declaring peers an agent may **hand off** to — transfer control — by name, or every live agent with `discover=True`; pass in `Agent(peers=[...])`. |

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

### Ktables tuning

Optional worker-level tuning for the ktables-backed substrates (the control-plane capability view and fan-out stores). All knobs default to ktables' own values, so omitting them changes nothing.

| Symbol | Purpose |
| --- | --- |
| `KTableReaderTuning` | Reader-cadence knobs (`poll_timeout_ms`, `fetch_max_wait_ms`) for a ktables reader. Lower both to cut idle `barrier()` latency (`~ max(fetch_max_wait_ms, poll_timeout_ms)`). Composed by `ControlPlaneConfig` (view) and `FanoutConfig`. |
| `FanoutConfig` | Worker-level tuning for fan-out agents' durable batch stores (reader cadence, catch-up + barrier timeouts). Passed as `Worker(fanout=...)`. |

### Exceptions

| Symbol | Purpose |
| --- | --- |
| `NodeFaultError` | A terminal typed fault. `raise NodeFaultError(error_type, ...)` from node or seam code to mint one; the client surfaces a run's fault by raising it (`except NodeFaultError`, branch on `e.report.find(...)`). Carries an `ErrorReport` on `.report`. See [Errors & faults](#errors--faults). |
| `DeserializationError` | A successful reply was present but failed a **structured** `output_type` (`output_type=str` never raises this — it coerces). |
| `ClientTimeoutError` | A client `result(timeout=)` / `execute(timeout=)` elapsed. A typed, run-survives signal (never a bare `asyncio.TimeoutError`); carries `.correlation_id` + `.timeout`. |
| `ClientClosedError` | The client was closed (`aclose()`) with a run's `result()` still pending. A typed, run-survives signal; carries `.correlation_id`. |
| `LifecycleConfigError` | Raised when a node or worker lifecycle configuration is invalid. |

## Key entry points

### `Client.connect`

```python
Client.connect(
    server_urls: str | Iterable[str] | None = None,
    *,
    inbox_topic: str | None = None,
    deps_factory: Callable[[], dict] | None = None,
    firehose_buffer_size: int = DEFAULT_FIREHOSE_BUFFER_SIZE,
    provisioning: ProvisioningConfig | None = None,
    enable_idempotence: bool | None = None,
    **broker_kwargs,
) -> Client
```

Build a `Client` — **synchronous and lazy**: no I/O until the first dispatch / `events()`, so a connection error surfaces there, not from `connect()`. `server_urls` defaults to the `CALFKIT_MESH_URL` environment variable, then `"localhost"`. `inbox_topic` is the named topic this client receives its runs' replies on and routes callbacks to — `None` gives an ephemeral per-client inbox; set it for a durable, shareable one. `deps_factory` seeds ambient `deps` merged under each call's `deps`. `firehose_buffer_size` bounds each `events()` observer's drop-oldest buffer. Configure auth with a FastStream `security=` object in `broker_kwargs` (raw security kwargs are rejected).

`enable_idempotence` is the single knob for producer idempotence across **every** producer this client drives — the shared broker producer and, for a co-located `Worker`, its control-plane and fan-out writers. Left unset (`None`), calfkit imposes nothing and the library defaults apply (no idempotence; aiokafka `acks=1`), which keeps the SDK working against brokers that lack producer-id support (e.g. Tansu). Set it to `True` to turn idempotence on consistently, or `False` to force it off. `acks` is not set by calfkit either; override it per-producer via `broker_kwargs` (e.g. `acks="all"`).

### `Client.agent`

```python
client.agent(name: str, *, output_type: type[OutT] = str) -> AgentGateway[OutT]
client.agent(*, topic: str, output_type: type[OutT] = str) -> AgentGateway[OutT]
```

Mint a typed gateway to one destination, addressed **exactly one** of two ways: by `name` (a deployed agent — the name derives its private input topic) or by `topic=` (the escape hatch for a non-derived topic). `output_type` binds once at mint and defaults to `str` (which coerces the reply to a string); pass `output_type=Model` for a typed, validated object.

### The verb triad — `send` / `start` / `execute`

```python
await gateway.execute(prompt: str, *, timeout=None, correlation_id=None,
    message_history=None, deps=None, model_settings=None, temp_instructions=None,
    tool_overrides=None, author=None) -> InvocationResult[OutT]
await gateway.start(prompt: str, *, <same knobs, no timeout>) -> InvocationHandle[OutT]
await gateway.send(prompt: str, *, <same knobs, no timeout>) -> Dispatch
```

`execute` is request/reply — dispatch and await the result; `timeout` bounds *the client's* patience (→ `ClientTimeoutError`; the run survives). `start` dispatches and returns a handle to await later. `send` dispatches without awaiting and returns a `Dispatch` (`.correlation_id`); its reply lands on the inbox, observed via `events()`.

### `InvocationHandle`

```python
handle.correlation_id: str
await handle.result(*, timeout=None) -> InvocationResult[OutT]
handle.stream() -> AsyncIterator[RunEvent]
```

The per-run handle from `start()` — the **only** way to get this run's result by id (hold it for the run's lifetime; there is no reattach-by-`correlation_id`). `result()` awaits the terminal and maps it to the typed `InvocationResult` (or raises `NodeFaultError` / `DeserializationError` / `ClientTimeoutError`). `stream()` yields the run's events, terminal-bearing.

### `client.events`

```python
client.events(*, terminal_only: bool = False) -> EventStream
```

The cross-run firehose over the client's one inbox — every reply while open, demuxed by the caller. Best-effort (bounded drop-oldest, signaled by `EventStream.dropped`); for guaranteed delivery hold the handle (`start`/`execute`) or run a [`@consumer` node](consumer-nodes.md).

### `Agent`

```python
Agent(
    name: str,
    *,
    system_prompt: str = "You are a helpful AI assistant.",
    description: str | None = None,
    subscribe_topics: str | list[str] | None = None,
    publish_topic: str | None = None,
    tools: Sequence[...] | None = None,
    model_client: PydanticModelClient,
    final_output_type: type = str,
    model_settings: ModelSettings | dict | None = None,
    peers: Sequence[Messaging | Handoff] | None = None,
)
```

An agent node. It is always reachable by name — every caller (the [client gateway](#client), `message_agent`, and handoff) addresses it on its name-derived private inbox (`agent.<name>.private.input`), so `subscribe_topics` is **optional**: omit it for a name-addressed agent, or pass one or more topics to also consume from those public inboxes. It calls its `tools` and publishes its output to `publish_topic` (when set). `final_output_type` enforces a structured output type (default: plain `str`). `description` is a short public blurb (≤512 chars) advertised on the `calf.agents` directory so other agents can discover this one. `peers` declares the agents it may reach — `Messaging` (consult) and `Handoff` (transfer); see [Agent-to-agent messaging & handoff](#agent-to-agent-messaging--handoff).

### `@agent_tool`

```python
@agent_tool
def get_weather(location: str) -> str:        # -> ToolNodeDef
    """Get the current weather at a location."""
    ...
```

Turns a function into a tool node. The function's parameters and type annotations become the tool's argument schema, and its docstring becomes the description shown to the calling LLM. Declare an optional first `ctx: ToolContext` parameter to receive the [context](#context-objects) — it is hidden from the LLM schema. The return value must be JSON-serializable. Sync and async functions are both supported. The tool name defaults to the function name and is the node's identity — the name the LLM calls and the name a `Tools(...)` handle references; `@agent_tool(name="...")` (or `agent_tool(func, name="...")`) overrides it. The node's topics derive from that name (`tool.<name>.input` / `tool.<name>.output`).

### `Tools`

```python
agent = Agent("researcher", subscribe_topics="researcher.input", model_client=model,
              tools=[Tools("add", "subtract")])    # named: reference specific tool nodes
# or
agent = Agent("researcher", subscribe_topics="researcher.input", model_client=model,
              tools=[Tools(discover=True)])         # discover: every live tool node
```

A frozen, identity-only handle to deployed function tool nodes, resolved per turn from the capability control plane (the agent discovers schemas at runtime instead of importing the tool's code). Two mutually exclusive modes: **named** — `Tools("add", "subtract")` / `Tools(names=[...])` references specific tool nodes; **discover** — `Tools(discover=True)` selects every live tool node (`node_kind == "tool"`; never an MCP toolbox's tools), carrying no names. Exactly one of {non-empty names, `discover=True`} holds — both, or the empty handle, raise. An unresolved selection warns and degrades. The agent's tool surface admits no duplicate tool names, and `Tools(discover=True)` owns the tool-node surface (no eager tool node or named `Tools` alongside it — an `MCPToolbox` may); a violation raises at construction. See [discoverable tool nodes](tool-discovery.md).

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

`max_workers` is the per-node concurrency cap. Caller-capable nodes (`Agent`, `NodeDef`, tool nodes) consume **key-ordered**: up to `max_workers` messages process in parallel across conversations, while messages sharing a `correlation_id` stay strictly serial, in order — so raising it never reorders a workflow's steps. Observer nodes (`@consumer`) use the stock concurrent subscriber, which makes no ordering promises between messages. The default (`1`) processes serially, one message at a time.

## Policy seams

Caller-capable nodes (`Agent`, `NodeDef`, tool nodes) expose four **policy seams** — callbacks that run inside the message flow to guard input, reshape output, and handle failures. Each is registered as a constructor argument (`Agent`/`NodeDef`; a single callable or a list) or as a repeatable instance decorator (any node — and the only form for tool nodes); constructor entries precede decorator entries. A chain runs in registration order, resolving on the **first non-`None` return** (sync or async handlers). Observer nodes (`@consumer`) have no seams. Agents additionally expose **`on_tool_error`**, a promoted surface over `on_callee_error` with the failing tool first-class (below the table).

| Seam | Handler signature | A `None` return… | A non-`None` return… |
| --- | --- | --- | --- |
| `before_node` | `(ctx)` | proceeds to the node body | short-circuits the body; the value becomes the node's output |
| `after_node` | `(ctx, output)` | keeps the produced output | replaces the output (output values only) |
| `on_node_error` | `(ctx, fault)` | escalates the original fault | recovers — the value becomes the node's output |
| `on_callee_error` | `(ctx, fault)` | the failed call escalates | substitutes a result for that call |
| `on_tool_error` *(agents)* | `(tool_call, ctx, report)` | the tool failure escalates | substitutes a result for that tool call |

`ctx` is a [`SeamContext`](#seamcontext); for the error seams, `fault` is an [`ErrorReport`](#errors--faults). (Returning a node *action* — a `Call`/`ReturnCall`/…, the kind a node body returns to dispatch work — is accepted from `before_node` but raises `SeamContractError` from `after_node`.)

**`on_tool_error`** (agents only) is sugar over `on_callee_error`: it hoists the failing tool to a flat `tool_call: ToolCall` param (`.tool_name` / `.args`) so a handler branches on `tool_call.tool_name`. Its `ctx` is an `AgentSeamContext` (a [`SeamContext`](#seamcontext) with a lazy `ctx.tool_call`). Return a value to substitute a successful result (streams as `outcome="success"` — the model's view) or `retry_text_part(msg)` to show the model an error (streams as `outcome="failed"`); **constructor-registered** `on_tool_error` handlers run before **constructor** `on_callee_error` on the same chain (a decorator entry, of either seam, appends after all constructor entries). The zero-argument prebuilt **`surface_to_model()`** converts *every* tool failure into a model-visible error — the top exception line, via `render_fault_for_model(report)`. Recipes: [How to handle errors and faults](error-handling.md).

**Minting:** `raise NodeFaultError(error_type, ...)` from any seam (or the node body) converts to a fault verbatim and **bypasses `on_node_error`** (the mint rule). Inside `on_node_error` / `on_callee_error`, raising `NodeFaultError` mints; raising any *other* exception is treated as that handler declining.

Registration and recipes: [How to guard and transform node invocations](policy-seams.md) and [How to handle errors and faults](error-handling.md).

## Errors & faults

A failure surfaces as a typed [`ErrorReport`](#errorreport) that travels the result rail and escalates up the call chain. Faults are handled **inside a node** (the `on_node_error` / `on_callee_error` seams), **observed** on a `publish_topic` tap, or **received at the client** — a run's fault raises `NodeFaultError` from `result()` / `execute()` (see [Client-side errors](error-handling.md#client-side-errors)).

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
| `causes` | `list[ErrorReport]` | Nested faults — non-empty for a fault group, conversion, recovery-then-failure, or an uncaught exception's `__cause__` chain. |
| `exception` | `ExceptionInfo \| None` | Structured projection of an uncaught exception (`type`/`module`/`attrs`), populated only on the from-exception synthesis path; `None` on minted and framework faults. |
| `frame_chain` | `list[FrameRef]` | Call-frame topology at fault time (`FrameRef` = `frame_id` + `target_topic`; no payloads). |
| `report_id` | `str` | Framework-minted UUID7, stable across hops; the dedup key. |
| `origin_node_id` / `origin_frame_id` | `str \| None` | Where the fault originated. |

| Method | Returns |
| --- | --- |
| `report.find(error_type)` | The first report in `walk()` order with that `error_type`, or `None`. |
| `report.walk()` | Iterator over this report then every nested cause (pre-order, cycle-guarded). |

### `ExceptionInfo`

The structured projection of an uncaught exception, carried on `ErrorReport.exception` (and on each `__cause__`-chain link). Ships **data, never the exception object** — the receiver needs no producer class importable to read it.

| Attribute | Type | Description |
| --- | --- | --- |
| `type` | `str` | The exception class name, e.g. `"ModelHTTPError"`. |
| `module` | `str \| None` | The exception class module. |
| `attrs` | `dict[str, Any]` | The exception's own instance attributes (`vars(exc)`), JSON-native after sanitize; `message` is dropped (redundant with `ErrorReport.message`). |

`exception` is **diagnostic-first**: branch on the stable `error_type` (+ `find()`); read `exception.type` / `.attrs` for logging and debugging "what failed." `walk()` + `exception.type` is forensic (it keys on a class name), not a durable branching API.

### `FaultTypes`

Framework-minted `error_type` codes (all under the reserved `calf.` prefix), shipped as constants so you match without typing magic strings:

| Constant | Code | Meaning |
| --- | --- | --- |
| `FAULT_GROUP` | `calf.fault_group` | Multiple sibling faults composed under `causes` (e.g. a fan-out). |
| `EXCEPTION` | `calf.exception` | An arbitrary exception synthesized into a fault. |
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

The context handed to every [policy seam](#policy-seams), received as `ctx` — the base `SeamContext` is **not** importable (annotate as needed or leave it untyped). Mutable on `state` only; the per-stage fields are set by the framework. For an **agent**, the error seams receive an **`AgentSeamContext`** — a `SeamContext` with one added attribute, a lazy **`ctx.tool_call`** (`ToolCall | None`) that resolves the failing tool (the same value `on_tool_error` hoists to its flat `tool_call` param) — which **is** importable (`from calfkit import AgentSeamContext`) for annotating `on_tool_error` handlers.

> Note on the `state` mutation channel: a `state` mutation from an error seam handling a **fan-out sibling** lands on a throwaway copy and is discarded when the batch closes (the closure restores the pre-fan-out snapshot). It persists for a single (non-fan-out) call. Return a substitute value rather than relying on a fold-time `state` mutation surviving a fan-out.

| Attribute | Type | Description |
| --- | --- | --- |
| `state` | `StateT` | App state — **mutable**; the official input-transform channel (mutate in place, return `None`). See the fan-out caveat above. |
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

A frozen, identity-only handle to a toolbox. `name` must match the hosting `MCPToolboxNode`. `include` pins the tool names the agent may use **by their bare server-side name** (`search`, not `docs_server__search`); `None` exposes all of the toolbox's tools. The LLM-facing name of each tool is namespaced `<name>__<tool>` (e.g. `docs_server__search`) and stripped back to the bare name before dispatch to the MCP server (ADR-0018); the combined name must fit the provider's tool-name charset (`[a-zA-Z0-9_-]`) and length limit. The handle carries no connection params — passing connection details, or deploying a handle as a node, raises.

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

## Agent-to-agent messaging & handoff

Let an agent reach other agents discovered at runtime by name. Both handles are re-exported from the top-level `calfkit` package and passed in `Agent(peers=[...])`.

| Symbol | Purpose |
| --- | --- |
| `Messaging` | Handle declaring peers an agent may **message** — a consult: the agent keeps control and the peer's reply folds into the call. |
| `Handoff` | Handle declaring peers an agent may **hand off** to — a transfer: control moves to the peer, which answers the original caller. |

### `Messaging`

```python
agent = Agent("triage", subscribe_topics="triage.input", model_client=model,
              peers=[Messaging("billing", "support")])   # named: consult specific peers
# or
agent = Agent("triage", subscribe_topics="triage.input", model_client=model,
              peers=[Messaging(discover=True)])           # discover: any live agent
```

A frozen, identity-only handle to peer agents, resolved per turn from the `calf.agents` control plane. Two mutually exclusive modes: **named** — `Messaging("billing", "support")` / `Messaging(names=[...])` lists the messageable peers; **discover** — `Messaging(discover=True)` opens messaging to every live agent, carrying no names. Exactly one of {non-empty names, `discover=True`} holds — both, or the empty handle, raise `ValueError`. Supplies the agent a built-in `message_agent(name, message)` tool whose description lists the live, in-scope peers; the peer answers on a fresh conversation and its reply folds into the tool result.

### `Handoff`

```python
agent = Agent("triage", subscribe_topics="triage.input", model_client=model,
              peers=[Handoff("refunds")])                 # named: transfer to specific peers
# or
agent = Agent("triage", subscribe_topics="triage.input", model_client=model,
              peers=[Handoff(discover=True)])             # discover: any live agent
```

A frozen, identity-only handle to peer agents, with the same construction rules as `Messaging` (named XOR `discover=True`; both, or the empty handle, raise `ValueError`). Names the peers an agent may transfer control to: the handle injects the reserved built-in `handoff_to_agent(name, message)` tool (its description carries the live peer directory, re-rendered every turn; an empty directory renders a "none reachable" sentinel). When the model calls it with a valid live target, the handoff wins the turn (unless the same response carries a validated structured final answer, which outranks it) — sibling tool calls in the same response are not executed and are closed with stub results — control moves to the named peer, and the handing agent does not regain it. An invalid or offline target is rejected with model-visible feedback (the standard tool-rejection path). The tool name is reserved against user tools whenever a `Handoff` handle is present. Handles are independent across capabilities, and a `discover=True` handle is the sole author of its own capability's scope (no named handle of the same kind alongside it). Naming the agent's own name in either handle raises `ValueError` at `Agent` construction.

See also: [How to let agents find and reach each other at runtime](agent-peers.md).

## Installation extras

`pip install calfkit` is pure-Python. One extra exists:

| Extra | Contents | Purpose |
| --- | --- | --- |
| `calfkit[mesh]` | `calfkit-mesh` (the bundled [Tansu](https://tansu.io) dev-broker binary + its locator) and `psutil` | The zero-setup local mesh behind [`ck dev`](cli.md#ck-dev). Dev-only — never needed by a production install, and `ck dev` against an already-reachable broker works without it. |

The env var `CALF_TANSU_BIN` points `ck dev` at your own broker binary instead
of the bundled one, with or without the extra installed (resolution order:
`CALF_TANSU_BIN` → bundled → `tansu` on `PATH`) — the escape hatch for a Unix
platform without a bundled wheel, or a pinned custom build. Native Windows is
unsupported (the broker is Unix-only); use WSL2.

## Other public modules

The top-level package re-exports the symbols above. A few public capabilities live in submodules:

| Import from | Provides |
| --- | --- |
| `calfkit.models` | `ConsumerContext` — the value passed to a `@consumer` function (`output`, `correlation_id`, `deps`, `resources`) — plus the wire/state models (`State`, `Envelope`, `ToolBinding`, …). |
| `calfkit.models.step` | `AgentThinkingEvent` — the defined-but-not-emitted step event (the four emitted step events, plus `RunEvent`, re-export from the top level). |
| `calfkit.mcp` | `MCPToolboxNode`, `MCPToolbox`, `StdioServerParameters`, `StreamableHttpParameters` — see [MCP toolboxes](#mcp-toolboxes). |
| `calfkit.provisioning` | The full topic-provisioning surface: `TopicProvisioner`, `provision_topics`, `topics_for_nodes`, `ProvisionReport`, `StartupTopicEnsurer`, `MissingTopicsError`. |
| `calfkit.exceptions` | The complete exception set, including the client run-outcome errors (`NodeFaultError`, `DeserializationError`, `ClientTimeoutError`, `ClientClosedError`), `RegistryConfigError`, and `MissingTopicsError`. |

## See also

- **[How to call nodes from a client](client-features.md)** — the `agent(name)` gateway, the `send` / `start` / `execute` triad, multi-turn conversations, dependency injection, the `events()` firehose, and the typed client errors.
- **[How to tap a topic with a consumer node](consumer-nodes.md)** — terminal sinks built with `@consumer`.
- **[How to guard and transform node invocations](policy-seams.md)** — `before_node` / `after_node` recipes.
- **[How to handle errors and faults](error-handling.md)** — `on_node_error` / `on_callee_error`, minting `NodeFaultError`, and inspecting an `ErrorReport`.
- **[How to give agents MCP tools](mcp-tool-discovery.md)** — fronting an MCP server as a toolbox.
- **[How to let agents discover and use tools at runtime](tool-discovery.md)** — referencing deployed function tool nodes by name (or every live one with `discover=True`) with `Tools`.
- **[How to let agents find and reach each other at runtime](agent-peers.md)** — agent-to-agent messaging and handoff with `Messaging` / `Handoff`.
- **[Worker lifecycle & embedding](worker-lifecycle.md)** — `Worker`, the lifecycle contexts, and `@resource`.
- **[How to run a local mesh with `ck dev`](local-dev-mesh.md)** — the zero-setup dev broker behind the `[mesh]` extra.
- **[CLI reference](cli.md)** — the `ck run`, `ck chat`, `ck dev`, and `ck topics` commands.
- **[Topic provisioning](topic-provisioning.md)** — `ProvisioningConfig` and the opt-in topic-creation helper.

Full signatures and behavior live in the source docstrings.
