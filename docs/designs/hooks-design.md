# Calfkit Hook System — Design Document

**Status:** Draft proposal
**Document version:** 1.0
**Last updated:** 2026-05-18

---

## 1. Summary

This document proposes a hook system for calfkit nodes (and agent nodes specifically) that brings calfkit to at-or-above-parity with modern AI agent SDKs (Pydantic AI, LangGraph v1 middleware, OpenAI Agents SDK) while honestly reflecting calfkit's event-driven Kafka architecture.

The design is a **two-layer middleware system** layered over the existing `BaseNodeDef.handler()` pipeline:

- **`NodeMiddleware`** — applies to every node type (tools, custom nodes, agents).
- **`AgentMiddleware(NodeMiddleware)`** — extends with hooks specific to agent nodes (model calls, tool boundaries, per-run lifecycle).

The system adds:

- **Around-advice primitives** (`wrap_handler`, `wrap_publish`, `wrap_model_call`) as the core composition unit.
- **Named-hook sugar** (`before_*`, `after_*`, `on_*`) for ergonomic observers over the same primitives.
- **Typed Request dataclasses** for every interceptable point.
- **First-class outbound interception** of Kafka publishes.
- **Honest split** of the tool boundary into `on_tool_dispatch` (in-process) and `on_tool_return` (separate handler invocation).
- **A new `headers` field** on `Envelope` for cross-cutting propagation context.

The existing `gates` system is kept as a separate Guard role and is deliberately **not** merged into the hook system.

---

## 2. Motivation

### 2.1 What users cannot do today

A user authoring a `BaseNodeDef` or `BaseAgentNodeDef` subclass cannot, without forking the SDK:

- Mutate state before `run()` executes (e.g. enrich state, stamp timestamps, mask PII).
- Observe state after `run()` returns (e.g. audit log, scrub fields, emit metrics).
- Wrap a model call with retry, caching, or cost tracking.
- Add headers (auth, tracing context, baggage) to outbound Kafka publishes.
- Audit "this node published a Call to topic X" centrally.
- Per-step filter the tool set the model sees.
- Observe agent-run boundaries (first turn, last turn) cleanly.
- Compose multiple cross-cutting concerns without subclassing.

The only extension points today are `gates` (`calfkit/nodes/base.py:78-86`) — by-contract bool predicates that cannot mutate state — and the abstract `run()` method (`calfkit/nodes/base.py:120-139`).

### 2.2 What modern AI agent SDKs offer

| SDK | Hook surface |
|---|---|
| Pydantic AI | `@agent.system_prompt`, `@agent.instructions`, `@agent.output_validator`, `@agent.tool(prepare=...)`, `event_stream_handler` |
| OpenAI Agents | `RunHooks` / `AgentHooks` lifecycle observers + `@input_guardrail` / `@output_guardrail` |
| LangGraph v1 | `AgentMiddleware`: `before_agent`, `before_model`, `after_model`, `after_agent`, `wrap_model_call`, `wrap_tool_call` |
| LangChain (legacy) | `BaseCallbackHandler` with ~15 narrow methods |

Calfkit currently provides **zero** of the seven boundaries (agent start/end, pre/post model, tool start/end, handoff) that every modern AI agent SDK considers table stakes.

### 2.3 The architectural constraint

Calfkit is not in-process. The "agent loop" is a chain of Kafka round-trips joined by `correlation_id`. Each `handler()` invocation is one hop. This rules out simply copying any other SDK's hook system wholesale — the design must reflect message-driven reality, particularly for tool boundaries that span processes.

---

## 3. Goals and non-goals

### 3.1 Goals

- Bring calfkit to at-or-above-parity with LangGraph v1 middleware in functional coverage.
- Provide a composable cross-cutting extension mechanism without subclassing.
- Surface first-class interception of Kafka publishes (calfkit's analog to "tool call dispatch").
- Preserve all existing public API (no breaking changes to `BaseNodeDef`, `gates`, `run()`, `Worker` semantics).
- Be honest about Kafka boundaries; do not paper over cross-process tool execution.

### 3.2 Non-goals

- Full Temporal-style 4-axis interceptor hierarchy. Calfkit has one execution unit (the Node), not two.
- Streaming / event-token hooks. The Agent does not stream today.
- A cross-invocation "wrap the whole agent run" hook with durable state. Punted; derived per-run sugar covers most cases.
- Hook priority numbers or other registration-order customization.
- Auto-discovery of middleware via import-time side effects.
- Global middleware registries.

---

## 4. Background

### 4.1 Current node pipeline

`BaseNodeDef.handler()` (`calfkit/nodes/base.py:226-245`) executes four steps in order:

```
handler(envelope, correlation_id, broker):
    1. ctx = prepare_context(envelope)          # base.py:141
    2. if not evaluate_gates(ctx): return       # base.py:88
    3. result = run(ctx, *input_args)           # base.py:120, abstract
    4. return _publish_action(result, ...)      # base.py:147 -- sealed
```

### 4.2 Agent.run() today

`BaseAgentNodeDef.run()` (`calfkit/nodes/agent.py:71-221`) is a ~150-line monolithic method. Phases visible by inspection:

- Tool registry resolution (`agent.py:72-76`).
- Parallel result aggregation (`agent.py:86-90`).
- Pending tool call routing (`agent.py:92-118`).
- Message commit (`agent.py:122-123`).
- Model call (`agent.py:125-131`).
- Branch on `DeferredToolRequests` vs final output (`agent.py:132-221`).

None of these phases are independently interceptable.

### 4.3 Adjacent primitives

| Primitive | Location | Role |
|---|---|---|
| `gates` | `nodes/base.py:78-86` | Predicate filters (Guard role). Bool-typed contract. |
| `ContextInjectionMiddleware` | `client/middleware.py:9-23` | FastStream broker-level middleware for `correlation_id`. Below the hook layer. |
| `_parallel_state_aggregation` | `nodes/agent.py:58-69` | Node-specific aggregation; not a general hook. |
| `WorkflowState.metadata` | `models/session_context.py:50-53` | Application-level data field; not a propagation channel. |

---

## 5. Design overview

### 5.1 Three-scope architecture

```
+---------------------------------------------------------------------+
| Worker                                                              |
|   middlewares = [M1, M2, ...]   (worker-level; outermost in chain)  |
|                                                                     |
|   +-------------------------------------------------------------+   |
|   | Node (BaseNodeDef or BaseAgentNodeDef)                      |   |
|   |   gates = [g1, g2, ...]   (Guard role; runs BEFORE chain)   |   |
|   |   middlewares = [Mn1, Mn2, ...]   (node-level; inner)       |   |
|   |                                                             |   |
|   |   handler(envelope, ...):                                   |   |
|   |     ctx = prepare_context(envelope)                         |   |
|   |     if not evaluate_gates(ctx): return envelope             |   |
|   |                                                             |   |
|   |     +-------------------------------------------------+     |   |
|   |     | wrap_handler chain -- around run()              |     |   |
|   |     |   before_handler -> run() -> after_handler      |     |   |
|   |     +-------------------------------------------------+     |   |
|   |                                                             |   |
|   |     +-------------------------------------------------+     |   |
|   |     | wrap_publish chain -- around each broker publish|     |   |
|   |     +-------------------------------------------------+     |   |
|   +-------------------------------------------------------------+   |
+---------------------------------------------------------------------+
```

Three things to note:

1. **Worker-level middleware is outermost.** It runs first on entry, last on exit. Matches Temporal and LangGraph conventions.
2. **Gates are outside the middleware chain.** Rejected messages produce zero hook invocations. This preserves the Guard semantic.
3. **The middleware chain has two phase chains** — one around `run()`, one around each `broker.publish()`. Both are populated from the same middleware list; each middleware can override either, both, or neither.

### 5.2 Two-layer class hierarchy

```
        NodeMiddleware
        |-- wrap_handler        (around-advice)
        |-- wrap_publish        (around-advice)
        |-- before_handler      (named sugar)
        |-- after_handler       (named sugar)
        +-- on_error            (named sugar)

             ^
             | inherits
             |
        AgentMiddleware
        |-- wrap_model_call     (around-advice)
        |-- before_model        (named sugar)
        |-- after_model         (named sugar)
        |-- prepare_tools       (transform)
        |-- on_tool_dispatch    (observer; in-process)
        |-- on_tool_return      (observer; new handler invocation)
        |-- before_agent_run    (derived sugar; first turn)
        +-- after_agent_run     (derived sugar; final turn)
```

Tool nodes accept any `NodeMiddleware` (including `AgentMiddleware`, treating its agent-specific methods as inert). Agent nodes accept any `NodeMiddleware` and additionally use the agent-specific methods of any `AgentMiddleware` instances.

### 5.3 Onion composition model

```
        +-------- M1 (outermost) ---------+
        |  pre-logic                       |
        |  +---- M2 ------------------+    |
        |  |  pre-logic                |    |
        |  |  +-- M3 ----------------+ |    |
        |  |  |  pre-logic            | |    |
        |  |  |  +-- inner ---------+ | |    |
        |  |  |  |  before_handler  | | |    |
        |  |  |  |  user run()      | | |    |
        |  |  |  |  after_handler   | | |    |
        |  |  |  +------------------+ | |    |
        |  |  |  post-logic           | |    |
        |  |  +----------------------+ |    |
        |  |  post-logic                |    |
        |  +--------------------------+    |
        |  post-logic                       |
        +----------------------------------+
```

Classic onion. M1 (registered first) is outermost; inside M1's body, `await call_next(req)` invokes M2; and so on. Pre-logic on either side of the `await` runs in nested order: pre-logic outermost-first, post-logic innermost-first.

---

## 6. Detailed design

### 6.1 Middleware base classes

```python
class NodeMiddleware:
    """Base middleware applicable to any node (tools, custom nodes, agents)."""

    # Around-advice primary primitives
    async def wrap_handler(
        self,
        req: HandlerRequest,
        call_next: Callable[[HandlerRequest], Awaitable[NodeResult]],
    ) -> NodeResult:
        return await call_next(req)

    async def wrap_publish(
        self,
        req: PublishRequest,
        call_next: Callable[[PublishRequest], Awaitable[None]],
    ) -> None:
        return await call_next(req)

    # Named-hook sugar (declarative form of the same thing)
    async def before_handler(self, ctx: SessionRunContext) -> None: ...
    async def after_handler(self, ctx: SessionRunContext, result: NodeResult) -> None: ...
    async def on_error(self, ctx: SessionRunContext, exc: Exception) -> None: ...
```

```python
class AgentMiddleware(NodeMiddleware):
    """Adds hooks specific to agent nodes."""

    # In-process LLM call (around-advice)
    async def wrap_model_call(
        self,
        req: ModelRequest,
        call_next: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        return await call_next(req)

    # Model call sugar
    async def before_model(self, ctx: SessionRunContext) -> None: ...
    async def after_model(self, ctx: SessionRunContext, resp: ModelResponse) -> None: ...

    # Pydantic-AI-style per-step tool filter
    async def prepare_tools(
        self,
        ctx: SessionRunContext,
        tools: list[ToolNodeDef],
    ) -> list[ToolNodeDef]:
        return tools

    # Tool boundary (split across handler invocations)
    async def on_tool_dispatch(self, ctx: SessionRunContext, dispatch: ToolDispatchEvent) -> None: ...
    async def on_tool_return(self, ctx: SessionRunContext, result: ToolReturnEvent) -> None: ...

    # Per-logical-run sugar (derived from state inspection)
    async def before_agent_run(self, ctx: SessionRunContext) -> None: ...
    async def after_agent_run(self, ctx: SessionRunContext, output: AgentRunOutput) -> None: ...
```

All methods default to no-ops (or pass-through for around-advice / transform methods). Users override only what they need.

### 6.2 Typed Request dataclasses

```python
@dataclass(frozen=True)
class HandlerRequest:
    ctx: SessionRunContext
    node_id: str
    headers: dict[str, Any]          # same reference as Envelope.headers
    input_args: Sequence[Any] | None

@dataclass
class PublishRequest:
    action: NodeResult               # the Call/ReturnCall/TailCall about to be published
    target_topic: str
    state: State                     # state being sent to next hop
    correlation_id: str
    headers: dict[str, Any]          # mutable; merged into outbound envelope
    # broker is held by the terminal handler; not exposed to user middleware

@dataclass
class ModelRequest:
    ctx: SessionRunContext
    message_history: list[ModelMessage]
    instructions: str | None
    tools: list[BaseToolNodeSchema]
    deferred_tool_results: DeferredToolResults | None
    deps: dict[str, Any]
    headers: dict[str, Any]

@dataclass
class ModelResponse:
    output: Any                      # AgentOutputT | DeferredToolRequests
    new_messages: list[ModelMessage]
    usage: ModelUsage                # token / cost accumulator (new type)

@dataclass(frozen=True)
class ToolDispatchEvent:
    tool_call_id: str
    tool_name: str
    target_topic: str
    args: dict[str, Any]

@dataclass(frozen=True)
class ToolReturnEvent:
    tool_call_id: str
    tool_name: str
    result: Any

@dataclass(frozen=True)
class AgentRunOutput:
    final_output_parts: list[TextPart | DataPart]
    message_history: list[ModelMessage]
    total_turns: int                 # count of model invocations in this logical run
```

### 6.3 Execution model — per `handler()` invocation

The exact pipeline. Numbered so edge cases can be reasoned about precisely.

```
[1] Kafka message arrives -> FastStream dispatches -> handler() entry

[2] ctx = prepare_context(envelope)                  # existing, unchanged

[3] if not await evaluate_gates(ctx, correlation_id):
        return envelope                              # Guard rejection -- no hooks fire

[4] middlewares = worker.middlewares + node.middlewares      # worker outermost

[5] result = await run_handler_chain(ctx, envelope, middlewares)

[6] return await run_publish_chain(result, envelope, broker, middlewares)
```

#### 6.3.1 `run_handler_chain` internals

```
run_handler_chain(ctx, envelope, middlewares):
    req = HandlerRequest(
        ctx=ctx,
        node_id=self.node_id,
        headers=envelope.headers,
        input_args=envelope.internal_workflow_state.current_frame.input_args,
    )

    innermost = build_innermost(req, middlewares, run=self.run)
    chain = innermost
    for m in reversed(middlewares):
        chain = wrap_with(m.wrap_handler, chain)

    return await chain(req)


build_innermost(req, middlewares, run):
    async def innermost(req):
        for m in middlewares:                         # before_handler: registration order
            await m.before_handler(req.ctx)
        try:
            if accepts_input(run):
                result = await run(req.ctx, *req.input_args)
            else:
                result = await run(req.ctx)
        except Exception as exc:
            for m in reversed(middlewares):           # on_error: reverse order, swallowed
                try:
                    await m.on_error(req.ctx, exc)
                except Exception:
                    logger.exception("on_error swallowed")
            raise
        for m in reversed(middlewares):               # after_handler: reverse order
            await m.after_handler(req.ctx, result)
        return result
    return innermost
```

#### 6.3.2 `run_publish_chain` internals

For each Call/ReturnCall/TailCall in the result (or once for a single result), a `PublishRequest` is built and threaded through the chain. The terminal handler executes the actual `broker.publish(...)` call that today lives inline in `_publish_action` (`base.py:147-224`).

For parallel fan-out (`list[Call]`), the chain runs once per Call, sequentially, each with its own `PublishRequest`.

`Silent` results skip the publish chain entirely.

### 6.4 Agent-specific execution model

`BaseAgentNodeDef.run()` is refactored to fire hooks at each named phase. The structure replaces the existing monolithic body (`agent.py:71-221`):

```
async def run(self, ctx):
    agent_mws = [m for m in self._active_middlewares if isinstance(m, AgentMiddleware)]

    # --- Tool preparation ---
    tools = self._resolve_initial_tools(ctx)               # existing logic
    for m in agent_mws:
        tools = await m.prepare_tools(ctx, tools)
    tools_registry = {t.tool_schema.name: t for t in tools}

    # --- Parallel state aggregation (existing) ---
    if not self.sequential_only_mode:
        self._parallel_state_aggregation(ctx)
        batch = self._pending_batches.get(ctx.deps.correlation_id)
        if batch and not batch.is_complete:
            return Silent()

    # --- Tool return event (if we got here via a tool ReturnCall) ---
    for returning in self._detect_tool_returns(ctx):
        for m in agent_mws:
            await m.on_tool_return(ctx, returning)

    # --- Pending tool handling (sequential mode + parallel error path; existing) ---
    ...

    if ctx.state.uncommitted_message is not None:
        ctx.state.commit_message_to_history()

    # --- First-turn boundary ---
    if ctx.state.is_first_turn():
        for m in agent_mws:
            await m.before_agent_run(ctx)

    # --- Model call (wrapped) ---
    for m in agent_mws:
        await m.before_model(ctx)

    model_req = ModelRequest(ctx=ctx, ...)
    chain = self._build_model_chain(agent_mws, terminal=self._call_agent_loop)
    response = await chain(model_req)

    for m in reversed(agent_mws):
        await m.after_model(ctx, response)

    # --- Branch ---
    if isinstance(response.output, DeferredToolRequests):
        ctx.state.message_history.extend(response.new_messages)
        for tool_call in response.output.calls:
            ctx.state.add_tool_call(tool_call)
            # existing validation for unknown / unreachable tools
            for m in agent_mws:
                await m.on_tool_dispatch(ctx, ToolDispatchEvent(
                    tool_call_id=tool_call.tool_call_id,
                    tool_name=tool_call.tool_name,
                    target_topic=tools_registry[tool_call.tool_name].subscribe_topics[0],
                    args=tool_call.args_as_dict(),
                ))
        return self._build_call_or_parallel(...)           # existing routing logic
    else:
        ctx.state.message_history.extend(response.new_messages)
        output = AgentRunOutput(
            final_output_parts=...,
            message_history=ctx.state.message_history,
            total_turns=self._count_turns(ctx),
        )
        for m in reversed(agent_mws):
            await m.after_agent_run(ctx, output)
        return ReturnCall[State](state=ctx.state)
```

### 6.5 Tool boundary — sequence across handler invocations

This is the diagram that captures the calfkit-specific architectural truth:

```
HANDLER INVOCATION #1                    HANDLER INVOCATION #2
(agent dispatches tool call)             (agent re-enters with tool result)

agent.run(ctx)                            agent.run(ctx)
  |                                         |
  |- before_model                           |- (sees tool result in state)
  |- wrap_model_call                        |- on_tool_return  <----+
  |    -> LLM returns DeferredToolRequests  |- (commit message)     |
  |- after_model                            |- before_model         |
  |- for each tool call:                    |- wrap_model_call      |
  |    on_tool_dispatch  ----+              |    -> LLM returns     |
  |- return Call/list[Call]  |              |       final output    |
                             |              |- after_model          |
wrap_publish chain  <--------+              |- after_agent_run      |
  |                                         |- return ReturnCall    |
  v                                         |                        |
broker.publish                              v                        |
  |                                       wrap_publish chain         |
  v                                                                  |
Kafka topic                                                          |
  |                                                                  |
  v                                                                  |
Tool node handler (separate worker)                                  |
  |                                                                  |
  |- evaluate_gates                                                  |
  |- wrap_handler chain around run()                                 |
  |- tool.run executes the function                                  |
  |- returns ReturnCall(state)                                       |
  |                                                                  |
  v                                                                  |
wrap_publish chain                                                   |
  |                                                                  |
  v                                                                  |
broker.publish                                                       |
  |                                                                  |
  v                                                                  |
Kafka topic (agent's input)                                          |
  |                                                                  |
  +-----------------> next handler invocation -----------------------+
```

**Key invariant**: `on_tool_dispatch` and `on_tool_return` fire in **different `handler()` invocations**, joined only by `correlation_id`. They may run on **different worker processes**. They cannot share Python state; they share the serialized `State` carried by the `Envelope` and any propagated `headers`.

This is the single biggest architectural divergence from in-process AI SDKs (LangGraph, Pydantic AI, OpenAI Agents), and it is why the design splits the tool boundary rather than offering a unified `wrap_tool_call`.

### 6.6 Composition rules

| Hook category | Order |
|---|---|
| `wrap_*` (around-advice) | First registered = outermost. Onion model. |
| `before_*` | Registration order (first-to-last). |
| `after_*` | Reverse order (last-to-first). LIFO. |
| `on_error` | Reverse order. Errors swallowed and logged per-middleware. |
| `on_tool_dispatch`, `on_tool_return` | Registration order. Errors swallowed and logged. |
| `prepare_tools` | Registration order. Each receives the previous result as input. |

Worker-level middlewares are concatenated **before** node-level middlewares, so the effective list is `worker_mws + node_mws` with worker-level outermost.

### 6.7 Registration model

**Worker-level** (cross-cutting; outermost):

```python
worker = Worker(
    client=client,
    nodes=[agent_node, weather_tool, ...],
    middlewares=[OTelMiddleware(), AuditMiddleware()],   # new kwarg
)
```

**Node-level** (local; inner):

```python
agent_node.add_middleware(RateLimitMiddleware())
agent_node.add_middleware(RetryMiddleware())

# Or decorator form, paralleling the @node.gate pattern at base.py:78-86:
@agent_node.middleware
class MyMiddleware(AgentMiddleware):
    async def wrap_model_call(self, req, call_next):
        return await call_next(req)
```

At `handler()` execution time, the effective chain is:

```
effective = worker.middlewares + node.middlewares
```

with `effective[0]` outermost. This mirrors how FastStream resolves broker-level middleware before subscriber-level middleware, and is consistent with the `ContextInjectionMiddleware` pattern already in the codebase.

### 6.8 Gates remain a separate Guard role

| Contract dimension | `gate` | `middleware` |
|---|---|---|
| Returns | `bool` | `NodeResult` (or transforms one) |
| On reject | Skip everything, return envelope unchanged | Not its concern; middleware can short-circuit by not calling `call_next` |
| Can mutate state | No (predicate contract) | Yes |
| Can wrap with try/finally | No | Yes (the entire point) |
| Order | First-to-last, AND semantics | Onion |

Gates fire at step 3 of the `handler()` pipeline. If they reject, **no middleware fires**. The rationale: gates exist precisely so users can say "this message never entered the pipeline" with confidence. Merging would muddle the contract.

If an audit middleware needs to see gate-rejected messages, the right answer is a worker-level plugin or FastStream-level middleware, not the hook chain.

### 6.9 State mutation contract

Hooks mutate `ctx.state` in place by reference. This matches calfkit's existing idiom (`gates`, `run()`, and `_parallel_state_aggregation` all do this).

**Known gotcha** carried from existing project memory: `CompactBaseModel` uses `exclude_unset=True` and `exclude_none=True`. List `.append()` does **not** mark a field as "set". Hooks that mutate state lists must use assignment:

```python
# WRONG -- won't serialize across Kafka
ctx.state.my_list.append(item)

# RIGHT
ctx.state.my_list = [*ctx.state.my_list, item]
```

The middleware base-class docstrings and the design's official examples must call this out prominently.

### 6.10 Required prerequisite: `headers` on `Envelope`

For typed Request objects to carry propagation context, `Envelope` (`calfkit/models/envelope.py`) gains a top-level `headers` field:

```python
class Envelope(BaseModel):
    context: SessionRunContext
    internal_workflow_state: WorkflowState
    headers: dict[str, Any] = Field(default_factory=dict)   # NEW
```

`HandlerRequest`, `PublishRequest`, and `ModelRequest` carry `headers` as a reference to `envelope.headers` (mutation flows naturally). `Deps` (`session_context.py:73-78`) remains frozen.

`Envelope` already uses plain `BaseModel` (per its docstring at `envelope.py:9-13`), so adding a new field is safe with respect to the `CompactBaseModel` serialization gotcha.

### 6.11 New State helpers

```python
class State(...):
    def is_first_turn(self) -> bool:
        """True if no assistant message has been recorded yet in this run."""
        return not any(self._is_assistant_message(m) for m in self.message_history)

    def has_final_output(self) -> bool:
        """True if final_output_parts has been populated (used to detect after_agent_run boundary)."""
        return bool(self.final_output_parts)
```

These exist so users can also check these conditions outside the middleware system, and so the derived hooks (`before_agent_run`, `after_agent_run`) have a clean, testable implementation.

---

## 7. Design decisions with justifications

Each subsection: the decision, the rationale, alternatives considered, and the trade-offs accepted.

### 7.1 Around-advice as the primary primitive

**Decision.** Make `wrap_handler`, `wrap_publish`, and `wrap_model_call` the core composition primitive. Named hooks (`before_*`, `after_*`, `on_error`) are sugar over the same primitive.

**Rationale.** Cross-industry consensus. Temporal interceptors, LangGraph v1 `wrap_model_call`, Pluggy `hookwrapper`, Koa middleware, ASP.NET Core middleware, NestJS interceptors — every modern process-orchestration or agent SDK that gets praised for DX picks around-advice. Reasons:

1. One `try/except/finally` around `await call_next()` collapses what would otherwise be three+ separate named hooks (before, after, error, success-only, failure-only) into one composable function.
2. State that needs to flow between "before" and "after" (timing, span context, transaction handles) lives naturally as closure variables, eliminating instance-variable plumbing.
3. Short-circuit semantics are explicit: don't call `call_next`. No magic sentinel return values.

**Alternatives considered.**

- *Only named hooks (before/after pairs).* Forces instance-variable plumbing for cross-cutting state. Cannot express retry or caching without contorted control flow.
- *Hookwrapper-only (Pluggy style).* Discards the named-hook sugar that makes 80% of simple cases ergonomic.
- *Decorator-per-concern (Pydantic AI style).* Works for a small, fixed set of concerns. Doesn't scale to user-defined cross-cutting (auth, audit, custom metrics).

**Trade-off accepted.** Slightly more ceremony for trivial observers (`async def wrap_handler(self, req, call_next): result = await call_next(req); log(...); return result`). Mitigated by named-hook sugar (`async def after_handler(self, ctx, result): log(...)`).

### 7.2 Two-layer middleware hierarchy (`NodeMiddleware`, `AgentMiddleware`)

**Decision.** Provide two base classes: `NodeMiddleware` (applies to every node type) and `AgentMiddleware(NodeMiddleware)` (adds agent-specific methods).

**Rationale.** Calfkit has two node specializations with genuinely different surfaces. A `ToolNodeDef` does not make a model call; an `Agent` does. Putting `wrap_model_call` on a flat `NodeMiddleware` would expose hooks that don't apply to tools and clutter autocomplete. Two layers map cleanly to the existing class hierarchy (`BaseAgentNodeDef(BaseNodeDef)`).

**Alternatives considered.**

- *Flat `NodeMiddleware` with all hooks on one class.* Confusing for tool authors; agent-specific methods would never fire on tools. Conflates concerns.
- *Three layers (`NodeMiddleware`, `ToolMiddleware`, `AgentMiddleware`).* Premature. Tool nodes today don't need their own hook surface — `NodeMiddleware` covers everything tools need. Add `ToolMiddleware` only if a real tool-specific need emerges.
- *No hierarchy; use protocols.* Less discoverable; users have to know which methods are valid for which node type without IDE help.

**Trade-off accepted.** One inheritance edge. Users targeting "every node" use `NodeMiddleware`; users targeting "every agent" use `AgentMiddleware`. Slight learning curve, but well-precedented (LangGraph: `AgentMiddleware`; Temporal: `ActivityInboundInterceptor` vs `WorkflowInboundInterceptor`).

### 7.3 Splitting the tool boundary (`on_tool_dispatch` + `on_tool_return`)

**Decision.** Do not provide a unified `wrap_tool_call`. Provide two observer hooks: `on_tool_dispatch` (fires when agent emits a `Call` to a tool topic) and `on_tool_return` (fires when agent re-enters its handler with a tool result).

**Rationale.** The defining architectural difference between calfkit and in-process AI SDKs is that tool execution happens on a different node, in a different `handler()` invocation, possibly on a different worker process. A unified `wrap_tool_call(req, handler)` would lie:

- Closure variables in `wrap_tool_call`'s pre-section would not survive to the post-section.
- The "tool handler" passed to it could not be invoked synchronously; the call goes through Kafka.
- Error semantics would be nonsensical (the tool's exception happens in another process; the agent's `try/except` cannot catch it).

Splitting the hooks makes the architectural truth explicit and prevents users from building incorrect mental models.

**Alternatives considered.**

- *Unified `wrap_tool_call` that fakes around-advice across Kafka.* Tempting but dishonest. Would require either (a) blocking the dispatch invocation until the return comes back — defeating async / event-driven — or (b) faking the closure with cross-invocation state. Both bad.
- *Only an observer pair `on_tool_dispatch`/`on_tool_return` (no separate `wrap_publish`).* Loses the ability to mutate outbound headers or short-circuit a tool dispatch. `wrap_publish` covers the mutation/control case at the Kafka level.

**Trade-off accepted.** Users coming from LangGraph have to learn that calfkit splits the tool boundary. Documented prominently with the sequence diagram in section 6.5.

### 7.4 First-class outbound interception (`wrap_publish`)

**Decision.** Refactor `_publish_action` (`base.py:147-224`) so each NodeResult branch dispatches into a `wrap_publish` middleware chain whose terminal handler is the actual `broker.publish(...)`.

**Rationale.** In calfkit, **outbound publish IS the tool call**. Other AI SDKs surface "wrap tool call" because tools execute in-process; calfkit's equivalent is the Kafka publish. Without `wrap_publish`:

- No way to add auth/tracing headers to outbound messages.
- No way to audit "this node emitted a Call to topic X" centrally.
- No way to dedupe or idempotency-key outbound publishes.
- No way to inject rate limits on downstream dispatches.

These are production necessities, not exotic asks. LangGraph users get `wrap_tool_call` for this; Temporal users get `WorkflowOutboundInterceptor.start_activity`; calfkit users currently get nothing.

**Alternatives considered.**

- *Hidden outbound — keep `_publish_action` sealed.* Status quo. Forces users to fork the SDK for any of the above.
- *Outbound on a separate `OutboundMiddleware` class.* Could be cleaner conceptually, but doubles the API surface. Composition is simpler with one middleware class that can override both inbound and outbound.

**Trade-off accepted.** `wrap_publish` fires once per Call in a parallel-publish list (`list[Call]`), which costs N chain traversals. For agents that fan out 20+ tool calls, this is N=20 chain runs. Acceptable; modern middleware chains are cheap; chains are typically short.

### 7.5 Gates kept as a separate Guard role

**Decision.** Do not merge `gates` into the middleware system. Gates fire before the middleware chain; rejected messages produce zero hook invocations.

**Rationale.** Gates have a different contract from middleware:

- Bool predicate, no state mutation, no around-advice.
- Explicit "this message never entered the pipeline" semantic.
- AND-chain composition with short-circuit on first reject.

Merging would muddle the contract. Django signals' lack of a separate Guard concept is a documented antipattern that conflates filtering with mutation and leads to subtle bugs.

**Alternatives considered.**

- *Gates become `wrap_handler` middlewares internally.* Confusing — gates' bool return value doesn't fit middleware's NodeResult return. Would require an adapter.
- *Gates become a `before_handler` that returns False to reject.* Conflates rejection (Guard) with observation (hook). Loses the "this message never ran" guarantee.

**Trade-off accepted.** Two extension mechanisms instead of one. Users learn `gates` for predicates and `middleware` for around-advice. Documentation explicitly covers when to use which.

### 7.6 `headers` on `Envelope`, not on `Deps`

**Decision.** Add `headers: dict[str, Any]` to `Envelope` (`calfkit/models/envelope.py`), not to `Deps` (`calfkit/models/session_context.py`).

**Rationale.**

- `Deps` is documented frozen (`session_context.py:76`). Mutating it would break the contract.
- Headers are wire-format propagation context. `Envelope` is the wire format.
- Headers must serialize across Kafka. `Envelope` already does that.
- Middleware needs to mutate headers. Mutation flows naturally with a non-frozen dict on `Envelope`.

**Alternatives considered.**

- *Headers on `Deps`.* Requires unfreezing `Deps`, which is a contract change with farther-reaching implications. Rejected.
- *Headers on `WorkflowState.metadata`.* `metadata` is already documented for application-level data (`session_context.py:50-53`). Overloading it for propagation context would muddle two concerns.
- *Headers on a separate top-level object.* Unnecessary additional object; `Envelope` is already the wire wrapper.

**Trade-off accepted.** `Envelope` becomes mutable in one field. Documented as such. The rest of `Envelope` remains immutable in practice.

### 7.7 In-place state mutation, not return-dict diffs

**Decision.** Hooks mutate `ctx.state` in place by reference. They do not return state diffs.

**Rationale.** Matches calfkit's existing idiom. `gates`, `run()`, and `_parallel_state_aggregation` all take `ctx` and mutate. Forcing a return-dict pattern would be inconsistent with the rest of the SDK.

LangGraph uses return-dict because LangGraph has graph reducers that merge diffs across parallel branches. Calfkit's reducer story is different: state lives in the `Envelope` and is mutated by each node in sequence (or aggregated explicitly by `_parallel_state_aggregation`). Return-dict would buy nothing here.

**Alternatives considered.**

- *Return-dict for parity with LangGraph.* Inconsistent with calfkit's idioms; no reciprocal benefit.
- *Pass-by-value with explicit `state.replace(...)`.* More functional; less ergonomic for users coming from the existing calfkit API.

**Trade-off accepted.** The `CompactBaseModel` "assignment, not append" gotcha (project memory) applies. Documented in middleware docstrings and primary examples.

### 7.8 Registration order, no priorities

**Decision.** Middleware composition order is registration order. No priority numbers, no `before_X`/`after_X` ordering strings.

**Rationale.** Simpler. Easier to reason about. Easier to debug. The vast majority of production middleware systems use registration order (Koa, Express, ASP.NET Core, NestJS, LangGraph). Priorities are an antipattern that creates non-local reasoning ("why did M3 run before M1?").

**Alternatives considered.**

- *Priority integers.* Used by Tapable (webpack), criticized by everyone else.
- *Named ordering ("before:Telemetry").* Used by Pluggy. Creates name-string coupling between unrelated middleware.

**Trade-off accepted.** Users with cross-cutting concerns that must run in a specific order have to explicitly register in that order. Acceptable; the explicit order is local information at the registration site.

### 7.9 Derived per-run hooks (`before_agent_run`, `after_agent_run`)

**Decision.** Provide `before_agent_run` and `after_agent_run` as derived sugar that fires conditionally inside the per-handler chain — `before_agent_run` when `state.is_first_turn()`, `after_agent_run` when the result is a final `ReturnCall`.

**Rationale.** Users repeatedly want per-run boundaries (initialize per-run state once, log usage once at completion). In an in-process SDK, these are real framework events. In calfkit, they are derived from state inspection. We provide the sugar so users don't have to write the detection logic in every middleware.

**Alternatives considered.**

- *Don't provide them; users use `before_handler` and check state themselves.* Forces every user to re-implement the detection. Reasons against: error-prone, not DRY.
- *Provide them as framework events with cross-invocation durable hook state.* Would require persistent hook-scoped state keyed by correlation_id. Too much complexity for v1.

**Trade-off accepted.** Hooks are slightly magical (fire conditionally). Documented as "derived from state inspection." Users with custom semantics ("first turn after first user message") use `before_handler` directly.

### 7.10 Per-Call (not batch) `wrap_publish` for parallel fan-out

**Decision.** For a `list[Call]` result (parallel fan-out at `base.py:150-165`), `wrap_publish` runs once per Call in the list, sequentially.

**Rationale.** Matches the mental model: each Call is one outbound message; each gets one chain traversal. Middleware that wants per-Call decisions (per-tool rate limit, per-target-topic header) works naturally. Middleware that wants batch awareness can use `after_handler` to observe the list before publishes begin.

**Alternatives considered.**

- *Batch-aware single `wrap_publish_batch(reqs, call_next)`.* Useful for batch publish optimizations but more complex. Punt; add later if a real use case emerges.

**Trade-off accepted.** Slight overhead for very large fan-outs. Bounded; agents don't typically fan out hundreds of tool calls at once.

### 7.11 `on_error` semantics — propagate, don't swallow

**Decision.** When `run()` raises, `on_error` fires for all middlewares (reverse order, swallowed if `on_error` itself raises). The exception then propagates out of `wrap_handler`. Middlewares that want to swallow can do so via their own `try/except` around `await call_next()`.

**Rationale.** Modern consensus. LangChain's legacy callbacks swallow by default and it's widely criticized. Temporal interceptors propagate. LangGraph middleware propagates. OpenAI Agents has no error hook at all (errors just propagate). Swallowing creates production debugging nightmares.

**Alternatives considered.**

- *Swallow by default.* Django-signals-style. Rejected for the reasons above.
- *No `on_error` hook; users use `wrap_handler` try/except.* Cleaner but loses the observer convenience. Compromise: provide `on_error` as observation only (cannot suppress); use `wrap_handler` if you actually need to swallow.

**Trade-off accepted.** A buggy `on_error` implementation is logged and ignored, so it doesn't compound failures.

### 7.12 Typed Request dataclasses, not raw kwargs

**Decision.** Every interceptable point takes a typed Request dataclass (`HandlerRequest`, `PublishRequest`, `ModelRequest`, etc.).

**Rationale.** Cross-industry consensus. Reasons:

- Forward-compatible: framework adds new fields without breaking existing middleware.
- IDE autocomplete works.
- Self-documenting surface.
- Carries `headers` as a first-class field.

**Alternatives considered.**

- *Positional args (`ctx, headers, ...`).* No autocomplete; brittle to signature evolution.
- *Untyped dict.* No types; every refactor breaks every middleware.

**Trade-off accepted.** More types to learn (one per interception point). Mitigated by consistent naming (`<Operation>Request`).

### 7.13 No streaming hooks in v1

**Decision.** Do not add streaming-token hooks (Pydantic AI's `event_stream_handler`, LangChain's `on_llm_new_token`).

**Rationale.** Calfkit's Agent does not stream today (`agent.py:125-131` calls `_agent_loop.run(...)`, not `_agent_loop.iter(...)`). Designing streaming hooks in advance of streaming support would be premature.

**Alternatives considered.** N/A — defer until streaming is added.

**Trade-off accepted.** Users who want streaming today must wait. Acceptable; streaming is its own design.

### 7.14 No global registry

**Decision.** Middleware registers at the Worker or Node level. No process-global registry, no auto-discovery, no import-time side effects.

**Rationale.** Modern consensus. Django signals' implicit auto-wiring is a well-documented antipattern (hidden side effects, hard to test, hard to grep).

**Alternatives considered.**

- *Process-global registry (`calfkit.register_middleware(...)`).* Reduces boilerplate but creates hidden coupling. Rejected.
- *Discovery via entry points (pyproject.toml plugins).* Useful for distributing reusable middleware packages; deferred until demand exists.

**Trade-off accepted.** Users register middleware explicitly on each Worker. Slightly more code; clear ownership.

---

## 8. Required prerequisites and migration

### 8.1 New types

- `calfkit/middleware/__init__.py` — public API surface.
- `calfkit/middleware/base.py` — `NodeMiddleware`, `AgentMiddleware`, Request dataclasses.
- `calfkit/middleware/chain.py` — chain composition helpers.
- `calfkit/models/usage.py` — `ModelUsage` accumulator.

### 8.2 Modified files

| File | Change |
|---|---|
| `calfkit/models/envelope.py` | Add `headers: dict[str, Any]` field. |
| `calfkit/models/state.py` | Add helpers: `is_first_turn()`, `has_final_output()`. |
| `calfkit/nodes/base.py` | `BaseNodeDef.__init__` accepts `middlewares=`; `handler()` refactored to use chain composer; `_publish_action` becomes `_run_publish_chain`. |
| `calfkit/nodes/agent.py` | `BaseAgentNodeDef.run()` refactored to fire agent hooks at the phases described in section 6.4. |
| `calfkit/nodes/tool.py` | No semantic change; tool nodes auto-get the `NodeMiddleware` surface via base class. |
| `calfkit/worker/worker.py` | `Worker.__init__` accepts `middlewares=` and threads to each node. |
| `calfkit/nodes/__init__.py` | Re-export middleware types for convenience. |

### 8.3 Backward compatibility

No existing public API changes. Adding middlewares is purely opt-in. Existing nodes work unmodified. Existing gates work unmodified. Existing `run()` signatures are unchanged.

The single visible-on-wire change is the addition of `headers` to `Envelope`. Since it defaults to an empty dict and `Envelope` uses plain `BaseModel` (not `CompactBaseModel`), older publishers / consumers interoperate cleanly with newer ones.

---

## 9. Worked examples

### 9.1 Time every LLM call

```python
class TimingMiddleware(AgentMiddleware):
    async def wrap_model_call(self, req, call_next):
        start = time.monotonic()
        try:
            return await call_next(req)
        finally:
            metrics.histogram(
                "model.duration_ms",
                (time.monotonic() - start) * 1000,
                tags={"correlation_id": req.ctx.deps.correlation_id},
            )
```

One method, try/finally for both success and failure paths. No state plumbing between before/after.

### 9.2 Auth headers on every outbound publish

```python
class AuthHeaderMiddleware(NodeMiddleware):
    def __init__(self, token: str):
        self.token = token

    async def wrap_publish(self, req, call_next):
        req.headers["authorization"] = f"Bearer {self.token}"
        return await call_next(req)
```

Registered at the Worker level. Every Call / ReturnCall / TailCall this worker emits carries the header. Downstream nodes receive `envelope.headers["authorization"]`.

### 9.3 Dynamic tool gating

```python
class ConfirmationGate(AgentMiddleware):
    async def prepare_tools(self, ctx, tools):
        if not ctx.state.user_confirmed:
            return [t for t in tools if t.tool_schema.name != "delete_resource"]
        return tools
```

The `delete_resource` tool is invisible to the model until the agent flow has set `user_confirmed=True`. Stolen directly from Pydantic AI's tool `prepare`.

### 9.4 Retry on rate-limit

```python
class RateLimitRetry(AgentMiddleware):
    async def wrap_model_call(self, req, call_next):
        for attempt in range(3):
            try:
                return await call_next(req)
            except RateLimitError:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)
```

Retries the model call in place without re-entering the handler or republishing on Kafka.

### 9.5 Observe per-run boundaries

```python
class RunLogger(AgentMiddleware):
    async def before_agent_run(self, ctx):
        log.info(f"run start cid={ctx.deps.correlation_id}")

    async def after_agent_run(self, ctx, output):
        log.info(
            f"run end cid={ctx.deps.correlation_id} "
            f"turns={output.total_turns}"
        )
```

Fires only on the first and last logical turns, despite each handler invocation being structurally identical.

### 9.6 Distributed tracing across Kafka

```python
class TracingMiddleware(NodeMiddleware):
    async def wrap_handler(self, req, call_next):
        parent_ctx = TraceContext.from_headers(req.headers)
        with tracer.start_as_current_span(req.node_id, context=parent_ctx) as span:
            try:
                return await call_next(req)
            except Exception as e:
                span.record_exception(e)
                raise

    async def wrap_publish(self, req, call_next):
        TraceContext.current().inject_into(req.headers)
        return await call_next(req)
```

Reads W3C trace context from incoming `headers`, runs the node inside the span, and propagates context to outbound publishes. This pattern is impossible in calfkit today.

---

## 10. Edge cases

| Case | Resolved behavior |
|---|---|
| Gate rejects | No middleware fires. Envelope returned unchanged. |
| `run()` raises | `on_error` fires for all middlewares in reverse order (own raises swallowed); exception propagates out of `wrap_handler`. Middlewares can `try/except` around `await call_next()`. |
| Middleware raises in `before_handler` | Propagates. Subsequent `before_handler` calls do not fire. `on_error` does fire (cleanup is still useful). |
| Middleware raises in `after_handler` | Propagates after the remaining `after_handler` calls run in a try/except. A buggy hook can fail a successful invocation — documented trade-off. |
| Middleware raises in `on_error` | Swallowed with a warning log. Do not compound failures. |
| Result is `Silent` | No publish happens. `wrap_publish` chain does not fire. `after_handler` still fires with the Silent result. |
| Result is `list[Call]` (parallel fan-out) | `wrap_publish` runs once per Call, sequentially. Each Call carries its own PublishRequest with its own headers (initially cloned from envelope.headers). |
| Tool node receives a middleware list containing `AgentMiddleware` | Only `NodeMiddleware` methods fire. Agent-specific methods silently inert. Documented. |
| First-turn detection wrong | `state.is_first_turn()` is conservative: `not any(isinstance(m, ModelResponse) for m in message_history)`. Configurable per agent if a custom heuristic is needed. |
| Middleware mutates state list with `.append()` | Won't serialize (CompactBaseModel gotcha). Documented prominently in base-class docstrings. |
| Middleware adds same header key twice | Last write wins — dict semantics. No magic merging. |
| Middleware chains depth | Practically unbounded; recursion depth is `len(middlewares)`. Reasonable values (< 20) are fine. |

---

## 11. Implementation phasing

### 11.1 V1 — Node middleware foundation

Ships with a usable cross-cutting hook surface for every node type. Deliverables:

- `NodeMiddleware` with `wrap_handler`, `wrap_publish`, `before_handler`, `after_handler`, `on_error`.
- `headers` field on `Envelope`.
- `HandlerRequest`, `PublishRequest` typed dataclasses.
- Chain composer in `calfkit/middleware/chain.py`.
- Worker-level and Node-level registration.
- Composition rules (registration order, LIFO post-hooks).
- Refactored `BaseNodeDef.handler` and `_publish_action`.
- Tests for chain composition, ordering, error propagation.

V1 alone unlocks distributed tracing, audit, header propagation, retry — every cross-cutting concern that doesn't require AI-specific knowledge.

### 11.2 V2 — Agent-specific surface

Brings calfkit to parity with LangGraph v1 middleware on AI-shaped hooks. Deliverables:

- `AgentMiddleware(NodeMiddleware)`.
- `wrap_model_call`, `before_model`, `after_model`.
- `prepare_tools`.
- `on_tool_dispatch`, `on_tool_return`.
- `before_agent_run`, `after_agent_run` derived sugar.
- `ModelRequest`, `ModelResponse`, `ToolDispatchEvent`, `ToolReturnEvent`, `AgentRunOutput` typed dataclasses.
- `ModelUsage` accumulator type.
- `State` helpers: `is_first_turn()`, `has_final_output()`.
- Refactored `BaseAgentNodeDef.run()` to fire hooks at the phases in section 6.4.
- Tests for each agent-specific hook.

### 11.3 Suggested merge cadence

Two PRs, V1 first, V2 second. V2 depends on V1's middleware infrastructure but does not depend on user adoption of V1.

---

## 12. Open questions

These were resolved with reasonable defaults but should be confirmed:

1. **Headers field name and type.** Proposed: `headers: dict[str, Any] = Field(default_factory=dict)`. Alternative: `dict[str, str]` (HTTP-style, no nested values). Recommendation: `dict[str, Any]` for flexibility, with documentation discouraging deeply nested values.

2. **First-turn detection heuristic.** Proposed: `not any assistant message in message_history`. Alternative: an explicit `turn_counter` on `State`. Recommendation: start with the heuristic; promote to an explicit counter only if real-world detection bugs surface.

3. **Should `wrap_handler` see the `Envelope` or only `ctx`?** Proposed: `HandlerRequest` carries `headers` (the mutable bit) and `input_args` but not the full Envelope. Rationale: Envelope is framework-internal per its docstring. Middleware that genuinely needs envelope access is a smell; surface specific fields instead.

4. **Decorator registration syntax.** Proposed: `@agent_node.middleware` (class decorator) and `agent_node.add_middleware(instance)`. Alternative: only `add_middleware`. Recommendation: support both, matching the existing `@node.gate` pattern.

5. **Should `prepare_tools` chain monotonically (each receives previous result) or all run with the original tools?** Proposed: chained — each receives previous output. Alternative: all see original. Recommendation: chained, matching Pydantic AI's per-tool `prepare` semantics composed.

---

## 13. Appendix — references

### 13.1 Existing calfkit code

- `calfkit/nodes/base.py:78-86` — `gates` registration API (model for `add_middleware`).
- `calfkit/nodes/base.py:120-139` — abstract `run()` signature.
- `calfkit/nodes/base.py:147-224` — `_publish_action` (refactored to `wrap_publish` chain).
- `calfkit/nodes/base.py:226-245` — current `handler()` pipeline (refactored to use chain composer).
- `calfkit/nodes/agent.py:71-221` — current monolithic `run()` (refactored per section 6.4).
- `calfkit/models/envelope.py:9-17` — `Envelope` (adds `headers` field).
- `calfkit/models/session_context.py:73-78` — `Deps` (kept frozen).
- `calfkit/client/middleware.py:9-23` — FastStream `ContextInjectionMiddleware` (broker-level; below the hook layer).
- `calfkit/worker/worker.py` — `Worker` (accepts `middlewares` kwarg).

### 13.2 External references

- Temporal Python SDK interceptors: `temporalio/worker/_interceptor.py` in github.com/temporalio/sdk-python. The 4-axis design (workflow/activity x inbound/outbound) informs the around-advice primitive but is collapsed to a single hierarchy for calfkit.
- LangGraph v1 middleware: `docs.langchain.com/oss/python/langchain/middleware/custom`. The hybrid node-style + wrap-style pattern is the closest parallel to this design.
- Pydantic AI vendored in `calfkit/_vendor/pydantic_ai/`. The `@agent.tool(prepare=...)` pattern at `tools.py:273` is the model for `prepare_tools`.
- OpenAI Agents SDK: `github.com/openai/openai-agents-python`. `RunHooks` / `AgentHooks` inform the named-hook sugar; their observer-only contract is deliberately exceeded by this design (around-advice plus mutation).
- Koa middleware (Node.js) and ASP.NET Core middleware (C#). The onion model and registration-order composition are stolen directly from these.

### 13.3 Antipatterns deliberately rejected

- Django signals — implicit registration, error swallowing by default.
- LangChain's `BaseCallbackHandler` — 15+ narrow methods that overlap (`on_llm_start` vs `on_chat_model_start`).
- ActiveRecord callbacks — too many specific lifecycle hooks (`before_save`, `before_create`, `before_validate`, ...).
- Tapable priority-string ordering — non-local reasoning about hook execution order.
