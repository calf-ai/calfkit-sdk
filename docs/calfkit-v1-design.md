# Calfkit 1.0 — Design Document

| | |
|---|---|
| Status      | Draft proposal — pre-implementation |
| Author      | event-driven-architect agent |
| Last updated| 2026-05-18 |
| Supersedes  | `docs/hooks-design.md` (the two-layer middleware proposal); the `BaseNodeDef`/`BaseAgentNodeDef`/`NodeResult` shape in `calfkit/nodes/` |
| Audience    | A senior engineer deciding whether to greenlight a 1.0 rewrite and what to build |

## Executive summary

Calfkit 1.0 is a clean break from the 0.x series. **It is a two-layer SDK.**

- **The user-facing primary surface** is an Agent SDK in the modern Pydantic-AI / OpenAI-Agents idiom: a small declarative `Agent(name=..., model=..., instructions=..., tools=[...], output_type=...)` class plus a handful of instance methods on it (`agent.handoffs_to(...)`, `@agent.output_validator`, `@agent.interrupt_when(...)`). Tools are first-class Kafka services authored at module scope (`@tool`, `Tool.external(...)`, `Tool.from_agent(...)`) and attached to one or more agents via `tools=[...]`. Registered with a Worker in one line (`worker.add(agent)`). This is what 80%+ of users author. They never write a Handler, never see an Action, never touch a topic name unless they want to.
- **The runtime primitive layer** — five concepts: **Handler, Action, Run, State, Worker** — is what the Agent SDK compiles down to. It is the surface for tool authors, advanced orchestration (custom nodes that aren't agents), Extension authors, and the SDK itself. Always documented, always accessible.

The unique value of calfkit is the *combination*: you author your agent at the Pydantic-AI tier of ergonomics, and the SDK deploys it as a distributed, Kafka-native, multi-language-tool-capable, durable, replayable service — with no wiring code from the user. **Both orchestration and choreography are first-class.** Agents may be invoked directly (request/reply over Kafka), wired into hierarchies (handoff, sub-agents, parallel fan-out — see §11.A), *and* subscribed to event streams from other agents or arbitrary Kafka topics (linear pipelines, pub/sub fan-out, schema-typed event reactions — see §11.B). **Choreography wiring lives on the Worker, not on the Agent**: an `Agent(...)` declaration is portable across deployments, and the deployment-time `worker.wire(source=..., target=..., payload=...)` is the canonical place that topology lives. If a developer can get the same result by stuffing Pydantic AI inside a Temporal activity, calfkit has no reason to exist. The Agent SDK is the answer to that question. See §4 for the two-layer surface and a head-to-head ergonomics comparison against "Temporal + Pydantic AI."

The runtime layer ships an Action algebra as the single side-effect channel for all Kafka traffic. Durability comes from Kafka alone: an at-least-once log plus two compacted topics (runs-state, fan-out aggregator) replace the in-process `_pending_batches` dict (§9.1) — the canonical bug class this version is designed to eliminate. Tools become first-class Kafka topics (cross-language by construction), the agent loop stops being a `BaseAgentNodeDef.run()` god-method, and the FastStream coupling is replaced by direct `aiokafka` so the runtime can own rebalance, header typing, and the fan-out aggregator. Extensions get exactly three primitives (`around_invoke`, `around_publish`, `on_event`) — the named-sugar hierarchy proposed in `docs/hooks-design.md` is rejected because it lies about cross-process boundaries.

**One distinguishing constraint, stated up front.** Calfkit ships no `agent.run(input)` in-process invocation. Pydantic AI and OpenAI Agents put `.run()` on the agent because they *are* in-process libraries — the agent IS the runtime. Calfkit's agent is a declaration that gets executed by a Worker over a transport. Local development, REPL exploration, and unit tests use `InMemoryWorker` — a transport variant whose backing is an in-process queue rather than a Kafka broker. Same runtime code, same Action interpreter, same Extension chain; different transport. This is the Starlette / FastAPI / httpx test-client pattern. See §4.2 hello-world and §17 testing for the idiom. The cost is one extra line of test setup; the win is that local-dev behavior is a real subset of production behavior — no parallel "in-process path" to drift.

---

## Table of contents

1. [Motivation and goals](#1-motivation-and-goals)
2. [The five-concept mental model](#2-the-five-concept-mental-model)
3. [The Action algebra](#3-the-action-algebra)
4. [The two-layer authoring API](#4-the-two-layer-authoring-api)
5. [Architecture layers](#5-architecture-layers)
6. [Wire format](#6-wire-format)
7. [State model](#7-state-model)
8. [Run lifecycle](#8-run-lifecycle)
9. [Fan-out and aggregation](#9-fan-out-and-aggregation)
10. [Tool model](#10-tool-model)
11. [Multi-agent patterns](#11-multi-agent-patterns)
12. [Extension / hook system](#12-extension--hook-system)
13. [Deps injection](#13-deps-injection)
14. [Observability](#14-observability)
15. [Error handling](#15-error-handling)
16. [Idempotency contract](#16-idempotency-contract)
17. [Testing strategy](#17-testing-strategy)
18. [Deployment / runner](#18-deployment--runner)
19. [Multi-tenancy](#19-multi-tenancy)
20. [Direct aiokafka vs FastStream](#20-direct-aiokafka-vs-faststream)
21. [What's discarded from current calfkit](#21-whats-discarded-from-current-calfkit)
22. [Comparison table](#22-comparison-table)
23. [Implementation phasing](#23-implementation-phasing)
24. [Open questions and unresolved trade-offs](#24-open-questions-and-unresolved-trade-offs)
25. [Appendix](#25-appendix)

---

## 1. Motivation and goals

### 1.1 Why a rewrite, not a refactor

The 0.x branch has shipped real value — `@agent_tool`, the Kafka-backed agent loop, gating — but it carries three fundamental shape problems that cannot be patched without breaking every public surface:

1. **The agent loop is one god-method.** `BaseAgentNodeDef.run()` at `calfkit/nodes/agent.py:74-221` is ~150 lines mixing tool-registry resolution, parallel aggregation, model invocation, and `Call`/`TailCall`/`ReturnCall` selection. Every cross-cutting concern (retry, tracing, audit, tool filtering) wants to interpose, but it has no seams.

2. **Durability is half-Kafka, half-Python-dict.** Parallel tool fan-out state lives in an in-process dict lost on restart and rebalance (§9.1 has the failure mode). Any non-trivial production deployment will eventually hit this.

3. **Extension is structurally impossible.** Today's only extension points are `gates` and overriding `run()`. The `docs/hooks-design.md` middleware proposal introduces named-sugar methods (`before_agent_run`/`after_agent_run`) that fire in *different Kafka handler invocations* — different Python processes — without that fact being visible in the API. That design lies about distributed reality.

A refactor that fixes (1) breaks public surface. A refactor that fixes (2) requires a new wire field, a new compacted topic, and runtime-owned aggregator code none of which the current `BaseNodeDef` knows about. A refactor that fixes (3) introduces a competing middleware API that conflicts with `gates`. Three concurrent breaking refactors is a 1.0 rewrite.

### 1.2 What problem the SDK solves and for whom

**Problem:** Engineers building production AI agent systems want both (a) Pydantic-AI / OpenAI-Agents-tier authoring ergonomics (declarative class, typed tools, typed output) *and* (b) event-driven decoupling for deployment (independent scaling, replayability, cross-team integration, polyglot tools, durability). In-process agent frameworks (LangGraph, Pydantic AI, OpenAI Agents, AutoGen, CrewAI) deliver (a) but fail at (b). Durable-execution platforms (Temporal, Restate, Inngest) deliver (b) but offer no agent-specific authoring surface — users stitch a Pydantic AI agent inside a Temporal Activity, write an HTTP gateway for cross-language tools, and maintain that glue. Calfkit delivers both in a single SDK with no manual stitching. See §4.10 for the head-to-head.

**For whom:**
- Application engineers who want to author agents at the Pydantic-AI / OpenAI-Agents tier of DX *and* deploy them as decoupled, Kafka-native services without writing transport glue.
- Platform/infra engineers placing AI agents inside an existing Kafka data plane.
- Teams running multi-language toolchains where the model service is Python but the tools are Go/Rust/Java.
- Teams that need durable, replayable agent runs *without* adopting a proprietary runtime (Temporal cluster, Restate cluster, Inngest cloud).
- Teams who treat agents as microservices and want consumer-group elasticity, not in-process concurrency primitives.

### 1.3 Competitive positioning

| System              | Execution shape                    | Durability source                  | Multi-language tools | Open-source runtime |
|---------------------|-------------------------------------|------------------------------------|----------------------|---------------------|
| **Calfkit 1.0**     | Distributed; one Node = one Kafka hop | Kafka log + compacted topics       | Native (topic = tool)| Yes (only needs Kafka) |
| Temporal            | Workflow + Activity                 | Temporal proprietary cluster       | Via per-language SDKs| Yes, but needs Temporal cluster |
| Restate             | Service handlers w/ durable invocation | Restate proprietary log         | Yes, via SDKs        | Yes, but needs Restate runtime |
| Inngest             | Function steps                      | Inngest cloud (or self-host)       | Yes                  | Cloud-first, self-host possible |
| Trigger.dev         | Function steps                      | Trigger.dev cloud (or self-host)   | TS-leaning           | Cloud-first |
| LangGraph           | In-process graph                    | Optional Postgres checkpointer     | No (in-process)      | Yes, lib only |
| Pydantic AI         | In-process agent                    | None (caller's problem)            | No (in-process)      | Yes, lib only |
| OpenAI Agents SDK   | In-process agent                    | None (caller's problem)            | No (in-process)      | Yes, lib only |
| AutoGen / CrewAI    | In-process multi-agent              | None                               | No                   | Yes, lib only |

**Calfkit's distinctive position:** the only SDK in this space that delivers durable, multi-language, microservice-shaped agents on top of *a broker the team likely already runs*. The closest peer is Temporal — but Temporal requires adopting their proprietary cluster, and Temporal's "Activity" model maps poorly to "tool that an LLM picks dynamically." Calfkit trades Temporal's stronger determinism guarantees for Kafka-native shape and zero extra infrastructure.

**The DX target, separately:** the Agent SDK (§4) targets Pydantic-AI-class authoring ergonomics — an `Agent(name=..., model=..., instructions=..., tools=[...], output_type=...)` constructor that a Pydantic AI user would not distinguish from theirs on first inspection. The deliberate divergence is G13: no `agent.run(input)` callable. Calfkit's value sits at the intersection — in-process SDKs cannot offer durability or polyglot tools; Temporal/Restate cannot offer Pydantic-AI-class authoring. §4.10 walks through the head-to-head.

### 1.4 Goals (explicit)

- **G0.** **The primary authoring surface is the Agent SDK** (§4) — a small declarative `Agent(name=..., model=..., instructions=..., tools=[...], output_type=...)` at the Pydantic-AI / OpenAI-Agents DX tier, plus a few instance methods (`agent.handoffs_to(...)`, `@agent.output_validator`, `@agent.interrupt_when(...)`). Tools are first-class Kafka services authored at module scope (`@tool`, `Tool.external(...)`, `Tool.from_agent(...)`), never owned by an Agent (§10). Hello-world is `Agent(...); worker.add(agent); asyncio.run(worker.run())` — nothing else. Runtime primitives (G1) live below.
- **G1.** Five-concept runtime model — Handler, Action, Run, State, Worker — is the underlying primitive layer. The Agent SDK compiles down to them. See §2.
- **G2.** Durability comes from Kafka log + compacted topics. No proprietary cluster, no Postgres dependency.
- **G3.** Parallel fan-out is durable across restart and rebalance. The 0.x `_pending_batches` hole is closed.
- **G4.** Tools are Kafka topics. A Go tool can serve a Python agent without an FFI layer.
- **G5.** Extensions ship with a three-primitive API (`around_invoke`, `around_publish`, `on_event`). No named-sugar hierarchy.
- **G6.** No graph DSL. Topology is emergent from topic wiring.
- **G7.** No `dict[str, Any]` on the wire. Discriminated unions or typed payloads only.
- **G8.** Async-only. No sync handler support.
- **G9.** Multi-tenancy via a tenant header by default; topic-per-tenant is opt-in.
- **G10.** HITL via `Interrupt`/`Resume` actions backed by the runs-state compacted topic. Agent SDK surface is `@tool(requires_approval=True, ...)` (per-tool gating) and `@agent.interrupt_when(...)` (per-output gating). See §4.5.
- **G11.** Testing has three honest layers: pure handler unit, `InMemoryWorker` (in-process queue transport), real Kafka via testcontainers.
- **G12.** OTel-native observability by default.
- **G13. The transport is never bypassed.** No `agent.run(input)` in-process shortcut. `InMemoryWorker` is the local-dev / REPL / test idiom — a transport variant (in-process queue) of the same runtime as production Kafka, not a parallel code path. An `Agent` is a declaration the Worker executes, not a callable. See §17.2.
- **G14. Topic naming is a deployment decision, not a definition decision (at Layer A).** Layer A declarations (`Agent`, `@tool`) carry identity (`name=` / function name); topics auto-derive from it. Overrides happen at registration (`worker.add(thing, topic="...")`). The honest exceptions — `Tool.external` and Layer B `@handler` — are explicit topic contracts with peer systems. See §10.1.

### 1.5 Non-goals (explicit)

- Sync handlers. Drop entirely.
- A graph builder DSL (`add_node` / `add_edge`). Reject.
- A proprietary durable store. Reject.
- Exactly-once semantics across all Actions. We commit to at-least-once + idempotency framing.
- A model abstraction layer beyond what's needed to plug in OpenAI/Anthropic/etc. We don't compete with LiteLLM.
- Streaming tokens through the SDK. Out of scope for 1.0.
- A managed-cloud offering. The OSS lib must stand alone.
- Replaying historical Kafka offsets as a built-in debugger. Possible, but punted.

---

## 2. The five-concept mental model

This section is the heart of the doc. Every other section is an unfolding of these five concepts.

These five concepts are the *runtime* primitives — what the SDK executes, what Extensions hook into, what the wire format encodes. They are NOT the primary authoring surface for agent developers (that is the Agent SDK in §4). Read this section to understand what the SDK does; read §4 to understand what users write.

### 2.1 Handler

A **Handler** is an `async` function from `(Context, Message) -> Action`. It is the atomic unit of *runtime* user code in calfkit — meaning, it is what the SDK dispatcher invokes on each Kafka message. Most agent authors do not write Handlers directly; they write `Agent(...)` declarations (§4) and the SDK synthesizes the Handler. But Handlers are first-class, fully documented, and used by tool authors, custom-orchestration users, and SDK internals.

```python
async def my_handler(ctx: Context[Deps], msg: InputMsg) -> Action:
    ...
```

- **Stateless across hops.** A Handler receives state via `ctx.state` (deserialized from the Envelope) and may produce a new state via an Action. There is no in-process state survival between handler invocations on the same logical Run — durability is the Envelope and the runs-state topic.
- **Pure function over its inputs.** Side effects (Kafka publish, OTel span, model call) happen via Actions, Extensions, and Deps — not by importing and calling things at module top level.
- **Bound to one topic at registration.** A Handler is registered with a topic via the Worker. One Handler = one consumer.
- **Async only.** No sync support.
- **Receives a typed message.** Pydantic or msgspec model. `dict[str, Any]` is rejected.

What a Handler **is not**:
- It is not a graph node. There is no graph.
- It is not an Activity (Temporal). Tools are Handlers too; they are not a separate runtime concept.
- It is not the "agent loop." The agent loop is a *recipe* — a Handler that returns a `Call` action to a tool topic and re-enters itself on `ReturnCall`.

### 2.2 Action

An **Action** is the *only* way a Handler causes a side effect. User code never calls `broker.publish()` directly. The Handler returns an Action; the SDK runtime interprets it.

```python
return Emit(topic="orders.created", payload=order)
return Call(topic="tool.classify.input", payload=req)
return Fan(calls=[Call(...), Call(...), Call(...)])
return Reply(payload=final_output)
return Interrupt(reason="awaiting human approval", resume_topic="approvals.in")
return Done()
```

- **Closed algebra.** See [Section 3](#3-the-action-algebra) for the full set.
- **Discriminated union on the wire** for those Actions that produce envelopes.
- **The single point of cross-cutting interposition.** `around_publish` wraps Action execution; this is what lets retry, OTel, header propagation, and the fan-out aggregator all live in one place.

What an Action **is not**:
- It is not a Promise. The Action is *describing what the runtime should do*, not the result of doing it.
- It is not a Kafka publish. `Emit` is a publish; `Reply` is a publish; `Done` is *not* a publish.
- It is not first-class composition. There is no `Action.then(...)`. Composition is by Handler chaining (a Handler returns `Call`, the called Handler later returns `Reply`, the caller re-enters).

### 2.3 Run

A **Run** is the logical agent execution — the durable aggregate joining N Handler invocations into one user-meaningful unit of work.

```python
@dataclass(frozen=True)
class RunId:
    value: str  # uuid7 hex
```

- **Durable across hops.** A `RunId` is minted at the originating client publish and rides on `x-calf-run-id` Kafka header on every subsequent envelope in the chain.
- **Distinct from `correlation_id`.** In v1, `correlation_id` is the per-hop request/reply joining key (for client `execute_node`-style request/reply). `RunId` is the run-aggregate key — different concept. The current code at `calfkit/client/base.py:148` conflates them (a single uuid7 stamped on the client publish doubles as both); v1 separates them explicitly.
- **Has a lifecycle.** `RunStarted` → (handler hops) → `RunCompleted` or `RunInterrupted` or `RunFailed`. Lifecycle is observable via `on_event`.
- **Has a state checkpoint** in the runs-state compacted topic, keyed by `RunId`.
- **Has a parent** (optional). Sub-runs (e.g. agent A invokes agent B as a sub-agent) have `parent_run_id` for telemetry rollups.

What a Run **is not**:
- It is not a single Handler invocation (that's the *frame*, see Section 7).
- It is not a Kafka transaction. We do not promise atomicity across hops.
- It is not a Workflow (Temporal sense). It has no determinism constraint on user code.

### 2.4 State

**State** is the typed, user-defined value that survives across hops within a Run. It rides in the Envelope, opaque to the SDK runtime.

```python
class MyState(BaseModel):
    user_id: str
    cart: list[Item]
    last_seen: datetime
```

- **User-typed.** The SDK does not impose a state shape. (The 0.x `State` class at `calfkit/models/state.py:92` mixes message history, tool results, overrides, and metadata into one god-model — v1 unbundles this.)
- **Opaque bytes on the wire.** The runtime sees `state: bytes` on the Envelope. Serialization is the Handler's concern; `ctx.state` is exposed typed via signature inference from the Handler (or as `Any` if not declared). State is *not* a generic parameter on `Context` — the `Context[Deps]` single-parameter generic matches Pydantic AI / OpenAI Agents.
- **Versioned.** A `state_schema_version: str` field on the Envelope lets a Handler reject incompatible states or migrate them.
- **Has explicit scopes.** See [Section 7](#7-state-model) for run-scoped vs frame-scoped vs long-term.

The critical motivation for opaque-bytes: today, `State` extends `BaseModel` with no `exclude_unset` discipline, but the *parts of it the agent loop manipulates* (`tool_calls`, `tool_results`, `message_history`) carry the `CompactBaseModel`-style footguns documented in user memory: optional fields without `= None` get excluded then fail re-validation; `.append()` doesn't mark a field as set. Opaque bytes pushes the serialization choice down to the user, so the SDK never has to reason about its shape.

### 2.5 Worker

A **Worker** is the process that hosts handlers, consumes from their topics, and runs the SDK runtime. The constructor accepts only transport identity; all composition is done via methods so the constructor stays small and discoverable.

```python
worker = (
    Worker(bootstrap_servers="kafka:9092", group_id_prefix="myapp-prod")
    .use(OTelExtension(), RetryExtension(max=3))
    .deps_from(create_deps)
    .add(order_classifier, fraud_checker, decision_agent)
)
await worker.run()
```

- **Hosts ≥1 Handler or Agent.** `worker.add(*items)` registers Agents (which compile to Handlers, plus tool Handlers) and bare `@handler`-decorated functions. Each registered Handler maps to one Kafka consumer subscription.
- **Owns the runtime.** Deserializing envelopes, dispatching to Handlers, interpreting Actions, running the Extension chain, owning the fan-out aggregator, publishing on behalf of the Handler.
- **Scales horizontally** via Kafka consumer groups. Adding a Worker process to the same group increases parallelism on the same topics.
- **Owns the rebalance listener** (a key reason for direct aiokafka). On partition revocation, the Worker checkpoints any pending aggregator state to the compacted topic before releasing the partition.
- **Owns lifecycle hooks** (graceful shutdown, signal handling, draining).
- **Owns the deployment topology.** Choreography wiring lives on the Worker (`worker.wire(...)`, `worker.publish(...)`, `worker.subscribe(...)`), not on the Agent. Agents are portable across deployments; the Worker is the place where "this deployment of these agents" is described.

Each method returns `self`, so the fluent builder reads top-to-bottom. The trio of internal-topic overrides (`runs_state_topic`, `fanout_topic`, `dlq_topic_pattern`) — which 99% of users should never touch — live on `worker.runtime(...)`, not in the constructor.

What a Worker **is not**:
- It is not a router. There is no central router process. Routing is implicit in topic wiring.
- It is not a scheduler. Kafka brokers do the scheduling.
- It is not coupled to FastStream. v1 bypasses FastStream entirely.

### 2.6 How the five concepts compose

```
+-------- Run (durable aggregate, keyed by RunId) ---------+
|                                                          |
|   +------+        +------+        +------+               |
|   |Hndlr | --Call->|Hndlr | --Call->|Hndlr | --Reply->...|
|   |  A   |         |  B   |         |  C   |             |
|   +------+        +------+        +------+               |
|   ^  |             ^  |            ^   |                 |
|   |  v             |  v            |   v                 |
|   state            state           state                 |
|   (one frame each, user-typed)                           |
|                                                          |
+----------------------------------------------------------+
       ^                                  ^
       | hosted by Worker process P1      | hosted by Worker P2
       | (Handler A's topic)              | (Handler B/C's topic)

  Actions returned by handlers (Call/Reply/Fan/...) are the only
  channel that produces Kafka traffic. The runtime — owned by the
  Worker — interprets them, runs the Extension chain, publishes.
```

Every other surface in this document is an unfolding of those interactions.

---

## 3. The Action algebra

This section enumerates every Action. Anything *not* in this list is not first-class — it's a recipe expressed in terms of these.

### 3.1 The algebra

| Action       | Signature                                                | Produces wire traffic? | Idempotency unit       | Purpose |
|--------------|----------------------------------------------------------|------------------------|------------------------|---------|
| `Emit`       | `Emit(topic: str, payload: M, key: bytes \| None=None)` | Yes (one publish)      | `(run_id, action_id)`  | Fire-and-forget publish; no reply expected. **This is the choreography primitive** — any agent or handler subscribed to `topic` will receive `payload` as an event (see §11.B). |
| `Call`       | `Call(topic: str, payload: M, key: bytes \| None=None, timeout: timedelta \| None=None)` | Yes (one publish, push frame) | `(run_id, frame_id)` | RPC-shaped: invoke target topic, expect a `Reply` back later. Pushes a frame onto the call stack. |
| `Reply`      | `Reply(payload: M)`                                      | Yes (one publish, pop frame) | `(run_id, frame_id)` | Pop the current frame; publish `payload` to the parent's callback topic. |
| `Fan`        | `Fan(calls: list[Call], aggregator: AggregatorSpec)`     | Yes (N publishes)      | `(run_id, fan_id)`     | Parallel fan-out. Runtime owns the aggregator (durable, compacted topic). |
| `Interrupt`  | `Interrupt(reason: str, resume_topic: str, payload: M \| None=None)` | Checkpoint write to runs-state | `(run_id, interrupt_id)` | Halt the Run. Persist state. Wait for `Resume`. |
| `Resume`     | (client-side) `client.resume(run_id, payload)`           | One publish to `resume_topic` | `(run_id, interrupt_id)` | Resume a paused Run. Strictly speaking *not* a Handler-returned Action — see §3.3 below. |
| `Continue`   | `Continue(state: M)`                                     | No                     | n/a (in-process)       | "Re-dispatch myself with this updated state" — used by Extensions to short-circuit a Handler invocation with a transformed state. Internal-leaning. |
| `Done`       | `Done()`                                                 | No                     | n/a                    | Terminate this hop. No publish. Marks the Run as complete if there's no parent frame. Equivalent to today's `Silent` but with explicit terminal semantics. **Registration-time check:** the SDK rejects a Handler that can return `Done()` *and* is reachable via `Call` (a caller awaiting `Reply` would hang forever). The Agent SDK never emits `Done` from a Call-reachable agent topic. |
| `Fail`       | `Fail(error: ErrorPayload, retry: RetryHint = AUTO)`     | DLQ publish (or retry topic) | `(run_id, frame_id, attempt)` | Explicit failure. Differs from raising an exception: gives the runtime a typed error and retry hint. |

### 3.2 First-class vs recipe

**First-class Actions:** `Emit`, `Call`, `Reply`, `Fan`, `Interrupt`, `Done`, `Fail`, `Continue`, `TailCall`.

**Recipes** (expressed in the above): agent loop (`Call` to tool topic, re-enter on Reply); handoff (`Call` peer agent's topic); supervisor-worker (`Fan` over `Call`s); sequential pipeline (`Call`/`Reply` chain); linear choreography (`Emit` + subscription, see §11.B); pub/sub fan-out (`Emit` + N subscribers).

The 0.x `Sequential`, `Delegate`, `Parallel` collapse into this set. `TailCall` is promoted to first-class because partial-state-aggregation benefits from explicit frame-replacement semantics; `Done` remains the strict terminator.

### 3.3 Why `Resume` is not in the Handler-returned set

`Resume` is asymmetric with `Interrupt`. A Handler that returns `Interrupt` checkpoints its state and exits — the Run is paused. Resuming is initiated by *someone else*, typically:
- A human via a UI that talks to `client.resume(run_id, payload)`
- A scheduled job (`schedule` skill) that resumes after a delay
- Another agent that decides the interrupt's condition is satisfied

So `Resume` is a client-side method (and a wire-level message) but not a Handler return type. The Handler whose `Interrupt` is being resumed simply re-enters with the checkpoint state plus the resume payload. See [Section 8](#8-run-lifecycle) for details.

### 3.4 Idempotency contract per Action

Under at-least-once delivery, every Action may be replayed. The runtime guarantees:

- `Emit` with `(run_id, action_id)` is deduped by the SDK's publish-layer idempotency table (a small compacted topic keyed by that tuple, retention 24h).
- `Call` is deduped by `(run_id, frame_id)`. The receiver checks the runs-state topic to see if this frame already produced a `Reply`; if so, the second `Call` is dropped.
- `Reply` is deduped by `(run_id, frame_id)`. Same mechanism.
- `Fan` is deduped by `(run_id, fan_id)`. Re-execution of a Handler that returns `Fan` is detected by checking the aggregator topic for an existing `fan_id`; if present, the Action becomes a no-op (the prior fan-out continues running).
- `Interrupt` is deduped by `(run_id, interrupt_id)`. A second `Interrupt` for the same `interrupt_id` is a no-op.

User code that has its own non-idempotent side effects (HTTP POST, DB write) uses `ctx.once(key)` (see §16) which is a small SDK helper backed by the same idempotency topic.

### 3.5 Runtime interpretation

```
Handler returns Action ->
  Worker runtime:
    1. Stamp action_id (uuid7) if not set
    2. Run Extension chain (around_publish onion)
    3. Resolve effect:
       Emit       -> broker.send(topic, envelope, headers)
       Call       -> push frame to call_stack; broker.send(topic, envelope, headers)
       Reply      -> pop frame; broker.send(parent.callback_topic, envelope, headers)
       Fan        -> for each call: push frame to fan_id-keyed aggregator topic;
                     broker.send(call.topic, ..., headers)
       TailCall   -> swap top frame; broker.send(topic, envelope, headers)
       Interrupt  -> upsert runs-state with state + interrupt sentinel; emit RunInterrupted event
       Done       -> commit offset; emit RunCompleted if no parent
       Fail       -> emit RunFailed event; broker.send(dlq_topic, ...) per retry policy
       Continue   -> (in-process) re-dispatch handler with updated state; no publish
    4. Emit lifecycle event(s) to local OTel + on_event chain
    5. Commit consumer offset
```

The crucial invariant: **step (5) — offset commit — happens after step (3) succeeds**. If publish fails (broker unavailable), the offset does not advance, the message will be redelivered, and the dedup table at step (3) absorbs the redelivery.

---

## 4. The two-layer authoring API

This is the most important section in the doc for the SDK's value proposition. Calfkit is a **two-layer SDK**: a Pydantic-AI-shaped Agent SDK on top, the runtime primitives of §2-§3 underneath. The Agent SDK *compiles* into Handlers and Actions — users who want stay at the top layer; users who need escape hatches drop one level.

### 4.1 The two layers — what they are, and what each is for

| Layer                  | Surface                                                        | Who writes it                                                                                  | Concepts they touch                                                                                  |
|------------------------|----------------------------------------------------------------|------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| **Layer A: Agent SDK** | The `Agent` class with a narrow constructor and agent-instance methods/decorators (LLM-output and routing only): `@agent.interrupt_when(...)`, `@agent.output_validator`, `agent.handoffs_to(...)`. Tools live at module scope: `@tool`, `@tool(requires_approval=True)`, `Tool.external(...)`, `Tool.from_agent(...)`, attached via `tools=[...]`. Plus deployment-side: `worker.add(agent)`, `worker.wire(...)`, `worker.publish(...)`, `worker.subscribe(...)`. | Agent developers (the 80%+ case). The "I want to ship an agent over Kafka" audience.            | `Agent`, `Tool`, `Context[Deps]`, `Worker`, `Client`. Optionally `Subscription` for power users wiring choreography declaratively (most users use `worker.wire(...)` kwargs instead). |
| **Layer B: Runtime**   | `@handler` + Action algebra: `@handler`, the `Action` types from `calfkit.actions` (`Emit`/`Call`/`Reply`/`Fan`/`Interrupt`/`Done`/`Fail`/`TailCall`/`Continue`), `Extension` from `calfkit.extensions`. | Tool authors. Authors of custom orchestration nodes that aren't agents. Extension authors. SDK internals. | All five from §2 plus the Action algebra from §3.                                                    |

The defining property: **every Layer A construct is a recipe over Layer B**. There is nothing in the Agent SDK that requires a wire-format change, a new Action variant, or a runtime concept that doesn't already exist for Layer B. The Agent SDK is purely an authoring-time compiler that materializes a `BaseAgentRecipe`-driven Handler at registration time. If a user inspects what `worker.add(agent)` produced, they see a normal Handler registered against a normal topic, with the agent's behavior implemented via the standard `Call`/`Reply` agent loop, just as they could have written by hand.

**The constructor stays narrow on purpose** — it accepts only fields that describe *what the agent is*, not *how it is deployed* or *what it reacts to*. The signature is fixed at six fields:

```python
Agent(
    name: str,
    *,
    model: str | Model,
    instructions: str | Callable[[Context[Deps]], str | Awaitable[str]] | None = None,
    output_type: type[Any] = str,
    deps_type: type[Deps] = NoneType,
    tools: Sequence[Tool] = (),
)
```

Everything else moves to instance methods or decorators (see §4.9 surface map). Topology lives on the Worker; HITL lives on the tool decorator.

### 4.2 Hello world — the user-facing primary surface

This is what 80%+ of users write. Every line is calfkit; no Pydantic AI under the hood (the Agent SDK is calfkit-native, with type-driven schema synthesis and structured output ported from the Pydantic AI conventions but owned by calfkit).

```python
# weather_agent.py
import asyncio
from calfkit import Agent, Context, InMemoryWorker, Worker, tool
from pydantic import BaseModel

class WeatherReport(BaseModel):
    location: str
    summary: str
    temperature_c: float

@tool
async def get_weather(ctx: Context, location: str) -> dict:
    """Fetch current weather for a location."""
    ...

@tool
async def get_forecast(ctx: Context, location: str, days: int = 1) -> list[dict]:
    """Fetch a weather forecast for the next N days."""
    ...

weather_agent = Agent(
    name="weather-bot",
    model="openai:gpt-5.4",
    instructions="You help users find current weather and short-term forecasts.",
    output_type=WeatherReport,
    tools=[get_weather, get_forecast],
)


async def main() -> None:
    # Production — over Kafka
    worker = Worker(bootstrap_servers="kafka:9092").add(weather_agent)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

That's the whole file. No `@handler`, no `Action`, no hand-written topic strings. Each `@tool` is its own Kafka service (its own topic, consumer group, scale unit); multiple agents may attach the same tool by referencing it in their `tools=[...]`.

**Local-dev / REPL / unit test — same agent, in-process transport.** Calfkit does not ship an `agent.run(input)` shortcut: an agent is a declaration, not a callable. To exercise it without a Kafka broker, use `InMemoryWorker`, which is the same `Worker` runtime with an in-process queue transport instead of Kafka. The idiom is one extra line:

```python
async def smoke_test() -> None:
    wrk = InMemoryWorker.testing(weather_agent)
    result = await wrk.invoke(weather_agent, "What's the weather in Berlin?")
    assert result.output.location == "Berlin"
```

Same code path as production; only the transport is swapped (Starlette-TestClient / httpx-MockTransport pattern). See §17.2 for the InMemoryWorker reference.

`worker.add(weather_agent)` derives topics from identity (`agent.weather-bot.in`, `tool.get_weather`, ...), synthesizes the Handler from the agent recipe, and registers one Handler per agent and per referenced tool. Topology is overridable at registration (`worker.add(thing, topic=...)`) or by dropping to Layer B (§4.6). See §4.7 for the full lifecycle.

### 4.3 Agent with cross-language tools — the calfkit moat

This is calfkit's structural differentiator vs Pydantic AI / OpenAI Agents / LangGraph. A Python agent can use a tool implemented in Go, Rust, Java, or TypeScript — without an FFI layer, without an HTTP gateway, without any change to the agent's authoring code.

```python
# trading_agent.py
import asyncio
from calfkit import Agent, Context, Tool, Worker, tool
from pydantic import BaseModel
from decimal import Decimal

class TradeRequest(BaseModel):
    user_id: str
    natural_language: str

class TradeResult(BaseModel):
    confirmation_id: str
    symbol: str
    side: str
    qty: int
    fill_price: Decimal

# Cross-language tool — DECLARED here, IMPLEMENTED in a Go service.
# Tool.external accepts topic= (unlike @tool) because there is no function
# to derive from — the topic IS the wire contract with the peer. See §10.1.
execute_trade = Tool.external(
    name="execute_trade",
    description="Submit a buy or sell order; returns confirmation id and fill price.",
    input=BuyOrSellOrder,
    output=TradeConfirmation,
    # topic= defaults to f"tool.{name}"; override if the peer expects a different topic.
)

# Python tools — module-level @tool decorator; standalone Kafka services
@tool
async def fetch_quote(ctx: Context, symbol: str) -> dict:
    """Get the latest market quote for a symbol."""
    ...

@tool
async def fetch_portfolio(ctx: Context, user_id: str) -> dict:
    """Get the user's current portfolio."""
    ...

trading_agent = Agent(
    name="trader",
    model="openai:gpt-5.4",
    instructions="You execute trades on behalf of users. Always confirm risk before placing orders.",
    output_type=TradeResult,
    tools=[execute_trade, fetch_quote, fetch_portfolio],   # all tools attach via tools=[...]
)


async def main() -> None:
    worker = Worker(bootstrap_servers="kafka:9092").add(trading_agent)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

From the Python agent's perspective, `execute_trade` is just a tool. The LLM picks it; the Agent SDK turns it into a `Call` to `tool.execute_trade`; some other process — possibly Go, possibly in another data center, possibly owned by another team — receives that Kafka message and Replies. None of the in-process agent SDKs offer this without a hand-built transport layer.

**One unified `Tool` concept** with three construction paths — `@tool` (Python in-codebase), `Tool.external(...)` (cross-language stub), `Tool.from_agent(other_agent, ...)` (sub-agent as tool). All three produce the same on-the-wire shape: a Handler addressable by topic. See §10 for the full Tool model and §10.1 for "never owned by an Agent."

The Go-side sketch lives in §4.10 as part of the head-to-head ergonomics comparison. The Python agent doesn't know or care it's a Go tool — same wire format, same Envelope, same headers.

### 4.4 Multi-agent: handoff, sub-agents, parallel sub-agents

The Agent SDK absorbs the patterns LangGraph / OpenAI Agents express via graph nodes or `Handoff` types. All compile to `Call`/`Reply`/`Fan`. The Agent constructor stays narrow; topology lives on instance methods or on the Worker.

**Handoff (OpenAI Agents-style):** an agent decides which peer to route to next. Peers are attached after construction via an instance method.

```python
@tool
async def lookup_invoice(ctx: Context, invoice_id: str) -> dict: ...

@tool
async def issue_refund(ctx: Context, charge_id: str, amount: Decimal) -> dict: ...

@tool
async def run_diagnostics(ctx: Context, account_id: str) -> dict: ...

@tool
async def fetch_logs(ctx: Context, account_id: str, since: datetime) -> list[dict]: ...

billing_agent = Agent(
    name="billing-agent",
    model="openai:gpt-5.4",
    instructions="You handle billing questions.",
    output_type=BillingAnswer,
    tools=[lookup_invoice, issue_refund],
)

tech_agent = Agent(
    name="tech-agent",
    model="openai:gpt-5.4",
    instructions="You handle technical support questions.",
    output_type=TechAnswer,
    tools=[run_diagnostics, fetch_logs],
)

triage_agent = Agent(
    name="triage",
    model="openai:gpt-5.4",
    instructions="Route user requests to the right specialist.",
    output_type=str,  # if no handoff, give a direct answer
)
triage_agent.handoffs_to(billing_agent, tech_agent)  # method, not constructor kwarg

worker = Worker(bootstrap_servers="kafka:9092").add(billing_agent, tech_agent, triage_agent)
```

`agent.handoffs_to(*peers)` is a recipe over `Call`. When the LLM picks a handoff target, the compiled Handler returns `Call(topic="agent.billing-agent.in", payload=...)`. The called agent's `Reply` flows back to the original client via the frame stack. The handoff list is inspectable via `triage_agent.handoffs`. See §11 intro for the principled distinction between handoffs (orchestration, on the agent) and subscriptions (choreography, on the Worker).

**Sub-agents (Pydantic AI / LangGraph-style):** an agent invokes another agent as a synchronous-feeling helper inside its own reasoning. Sub-agent output flows back to the parent's LLM as a tool result. Exposing one agent as a tool to another is a factory call on `Tool`: `Tool.from_agent(other_agent, name=...)`, paralleling `Tool.external(...)`. There is no `@agent.tool` or `agent.as_tool()` — see §10.1.

```python
@tool
async def web_search(ctx: Context, query: str) -> list[dict]: ...

@tool
async def read_url(ctx: Context, url: str) -> str: ...

research_agent = Agent(
    name="researcher",
    model="openai:gpt-5.4",
    instructions="You research a topic deeply.",
    output_type=ResearchReport,
    tools=[web_search, read_url],
)

writer_agent = Agent(
    name="writer",
    model="openai:gpt-5.4",
    instructions="You write an article based on the research provided.",
    output_type=Article,
    tools=[Tool.from_agent(research_agent, name="research_topic")],   # sub-agent as tool
)

worker.add(research_agent, writer_agent)
```

`Tool.from_agent(research_agent, name="research_topic")` returns a `Tool` value backed by the research agent's request topic. Under the hood, `writer_agent`'s LLM sees a tool named `research_topic` with input/output schemas synthesized from the sub-agent's input/output types. When the LLM picks that tool, the writer agent emits `Call(topic="agent.researcher.in", payload=...)` — i.e., a normal agent-to-agent Call. The sub-agent's `Reply` (a `ResearchReport`) is returned to the writer's LLM as a tool result, exactly as if it had been a regular tool. The two agents may run on different Worker processes, scale independently, even be written in different languages (if the sub-agent is `external`).

**Parallel sub-agents (LangGraph fan-out style):** sub-agents are tools (via `Tool.from_agent(...)`), so parallel sub-agents are just multiple tools. The LLM's own parallel-tool-call behavior drives the fan-out; the Agent recipe emits `Fan` and the aggregator (§9) collects the Replies. See §4.8 for the no-`parallel_tools=` rationale.

```python
aggregator_agent = Agent(
    name="aggregator",
    model="openai:gpt-5.4",
    instructions=(
        "Synthesize answers from multiple specialists. "
        "When the question requires multiple perspectives, query the specialists in parallel."
    ),
    output_type=ConsensusAnswer,
    tools=[
        Tool.from_agent(specialist_a, name="ask_specialist_a"),
        Tool.from_agent(specialist_b, name="ask_specialist_b"),
        Tool.from_agent(specialist_c, name="ask_specialist_c"),
    ],
)
```

The Agent recipe inspects the LLM's tool-selection each turn: one tool → `Call`; multiple → `Fan`. The user never touches the aggregator topic, `fan_id`, or partial-failure handlers unless they want to. Tuning via runtime config or `worker.add(...)` overrides — not constructor kwargs.

### 4.4.B Choreography — agents that subscribe to event streams

Everything in §4.4 is **orchestration**: an agent explicitly invokes another agent (handoff, sub-agent, parallel sub-agents). The caller knows the callee exists. The caller initiates.

**Choreography is the opposite shape.** An agent's output is published as an event stream; other agents subscribe and react. The producer does not know consumers exist; wiring is declared at deployment time, not in the producer's code. §11.B describes the full model; this subsection shows the Layer A surface.

**Choreography wiring lives on the Worker, not on the Agent.** An `Agent(...)` declaration describes *what the agent is*, not what topics it publishes to or reacts to. Two deployments may reuse the same `Agent` declaration with different choreography topologies (the FastAPI pattern: portable routes, app-level middleware).

The three Worker-side methods that compose a choreography topology:

| Method | Purpose |
|---|---|
| `worker.publish(agent, as_event=EventType)` | After this agent's Runs terminate, also `Emit` the output to `agent.<name>.events`. |
| `worker.wire(source, target, payload, ...)` | Subscribe `target` (an Agent) to `source`'s events; trigger a new Run per event. |
| `worker.subscribe(target, topic, payload, ...)` | Subscribe `target` (an Agent) to an arbitrary Kafka topic; trigger a new Run per event. |

`worker.wire(...)` and `worker.subscribe(...)` accept the kwargs that used to live in a `Subscription(...)` dataclass (`filter`, `react`, `group_id`, `start_from`, `max_concurrency`, `cross_tenant`). A `Subscription(...)` dataclass still exists for power users who want to declare a subscription as a value and pass it around (see §11.B.7), but the common path is method calls on the Worker.

**Linear choreography (B always reads A's output stream).** A classifier emits `Classified` events; an enricher reacts to each one.

```python
# Authoring — Agent declarations are portable, no choreography wiring
classifier = Agent(
    name="classifier",
    model="openai:gpt-5.4",
    instructions="Classify the input.",
    output_type=Classified,
)

enricher = Agent(
    name="enricher",
    model="openai:gpt-5.4",
    instructions="Enrich the classified record with geo data.",
    output_type=Enriched,
)

# Deployment — topology lives on the Worker
async def main() -> None:
    worker = (
        Worker(bootstrap_servers="kafka:9092")
        .add(classifier, enricher)
    )
    worker.publish(classifier, as_event=Classified)
    worker.wire(source=classifier, target=enricher, payload=Classified)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

The classifier does not import `enricher`, never names its topic, and is unchanged whether 0, 1, or N consumers subscribe to its events. The enricher is unchanged whether the classifier exists yet or not. This is choreography. Critically, both `classifier` and `enricher` are pure declarations — they can live in a shared library and be reused across deployments with different wiring.

> **Topics do not auto-create by default.** The decoupling above is about *declarations*, not topic existence. Whether the underlying Kafka topic exists is a separate, deployment-time concern: it is created either by your broker's `auto.create.topics.enable` (a broker property calfkit can't assume), by an ops-governed pipeline, or by calfkit's EXPERIMENTAL opt-in `ProvisioningConfig` (off by default — see [docs/topic-provisioning.md](topic-provisioning.md) and open question #11).

**Pub/sub fan-out (one producer, N independent consumers).** A news-watcher emits `NewsArticle` events; three independent agents react in parallel.

```python
class NewsArticle(BaseModel):
    id: str
    headline: str
    body: str
    urgency: int   # 1-5

watcher = Agent(
    name="news-watcher",
    model="openai:gpt-5.4",
    instructions="Identify the most relevant new articles and emit them.",
    output_type=NewsArticle,
)

summarizer = Agent(name="summarizer", model="openai:gpt-5.4",
    instructions="Produce a 1-paragraph summary of the article.",
    output_type=Summary)

sentiment = Agent(name="sentiment", model="openai:gpt-5.4",
    instructions="Score the sentiment of the article.",
    output_type=Sentiment)

alerter = Agent(name="alerter", model="openai:gpt-5.4",
    instructions="Send an alert for urgent articles.",
    output_type=AlertSent)

# Deployment topology — all wiring in one place
worker = (
    Worker(bootstrap_servers="kafka:9092")
    .add(watcher, summarizer, sentiment, alerter)
)
worker.publish(watcher, as_event=NewsArticle)
worker.wire(source=watcher, target=summarizer, payload=NewsArticle)
worker.wire(source=watcher, target=sentiment,  payload=NewsArticle)
worker.wire(
    source=watcher,
    target=alerter,
    payload=NewsArticle,
    filter=lambda ev: ev.urgency >= 4,   # predicate; only urgent articles
)
```

Three agents independently subscribe to `agent.news-watcher.events`. Each runs in its own consumer group (`{group_prefix}.agent.<consumer-name>.sub.<source>`), so each receives every message. Filters run *before* the LLM is invoked — non-matching events are acknowledged and skipped without LLM cost. Adding a fourth observer is one new `Agent(...)` declaration + one new `worker.wire(...)` line; the watcher is unchanged.

**Subscribing to arbitrary Kafka topics (not just other agents' streams).** Calfkit doesn't require the producer to be a calfkit agent. Subscribe to any Kafka topic whose payload deserializes into a Pydantic model:

```python
audit_agent = Agent(
    name="audit",
    model="openai:gpt-5.4",
    instructions="Summarize completed transactions for the audit log.",
    output_type=AuditEntry,
)

worker.add(audit_agent)
worker.subscribe(
    audit_agent,
    topic="events.transactions.completed",    # raw Kafka topic string
    payload=TransactionCompleted,             # Pydantic model the SDK uses to decode
)
```

This is how calfkit integrates with non-calfkit producers — a transactions service in Java that publishes Avro to `events.transactions.completed` can drive a calfkit agent without any calfkit-side wiring (Avro mode requires Schema Registry config; see §6.3). The `audit_agent` declaration is identical to one that doesn't subscribe to anything; only the deployment-side `worker.subscribe(...)` call changes.

**Why no `subscribes_to=` / `publishes=` on the Agent constructor.** Topology on the constructor breaks reusable agent libraries (cross-package import coupling), creates a two-way-to-do-it surface, and conflates "what the agent is" with "what reacts to it." See §11.B.6 for the full rationale. The `Subscription(...)` dataclass (§11.B.7) exists for power users — passed to `worker.wire(subscription=...)`, not to the Agent constructor.

**Mixed orchestration + choreography (the realistic case).** An agent may have request input AND tool calls AND handoffs — at the Agent level — AND be wired into event subscriptions AND publish events — at the Worker level. This is normal.

```python
@tool
async def lookup_user(ctx: Context, user_id: str) -> dict: ...

@tool
async def fetch_history(ctx: Context, user_id: str) -> list[dict]: ...

support_agent = Agent(
    name="support-bot",
    model="openai:gpt-5.4",
    instructions="Handle support inquiries. For billing issues, hand off to billing.",
    output_type=SupportResponse,
    tools=[lookup_user, fetch_history],
)
support_agent.handoffs_to(billing_agent)

# Deployment — wire choreography in
worker.add(support_agent)
worker.subscribe(
    support_agent,
    topic="events.customer.churn-risk",
    payload=ChurnRisk,
)
worker.publish(support_agent, as_event=SupportResponse)
```

This agent can be invoked directly (`client.invoke(support_agent, req)`), reacts to churn-risk events from elsewhere in the company, can hand off to billing mid-conversation, uses tools to answer, and emits a `SupportResponse` event for any downstream consumer (analytics, summarization, archival) — none of which it needs to know about. The agent declaration is portable; the deployment-side wiring composes it into the topology.

**Consumer semantics in the LLM loop:** an event-triggered run looks identical to a request-triggered run from the LLM's perspective (same instructions, same tools, same output type). Externally it differs only in that there's no caller waiting for `Reply` — the run terminates with an `Emit` to the agent's events topic if `worker.publish(...)` is set. See §11.B.8.

### 4.5 Human-in-the-loop on the Agent SDK

Calfkit exposes HITL through two shapes. **Tool-level approval is the primary form** — for "the LLM picked an action that needs a human's OK." **Output-type interrupts** are secondary, for the rarer case of "the LLM's output itself needs review."

**Form A (primary) — `@tool(requires_approval=True)`.** The gate is a property of the *tool* (the side effect being gated), not the agent. Every agent that references the tool inherits the gate. When the LLM picks the tool, the runtime emits `Interrupt` *before* the body runs, persists state, and waits for a `Resume` carrying a typed approval payload.

```python
from calfkit import Agent, Context, Worker, tool
from pydantic import BaseModel

class HumanDecision(BaseModel):
    approved: bool
    notes: str = ""

@tool(requires_approval=True, resume_with=HumanDecision)
async def execute_trade(ctx: Context, order: BuyOrSellOrder) -> TradeConfirmation:
    """Place the order via the exchange."""
    ...  # runs only after the human approves

trading_agent = Agent(
    name="trader",
    model="openai:gpt-5.4",
    instructions="You execute trades on behalf of users. Always confirm risk before placing orders.",
    output_type=TradeResult,
    tools=[execute_trade],
)

# Client-side
handle = await client.invoke(trading_agent, TradeRequest(...))
state = await handle.wait_for_interrupt()      # blocks until Interrupted or Completed
if state.kind == "interrupted":
    decision = await ui.show_approval_modal(state.pending_tool_call)
    await client.resume(state.run_id, decision)
result = await handle.result()
```

The LLM mental model is "this is a tool the agent can call." The HITL gate is invisible to the LLM (the tool advertises the same schema either way); it's the runtime that pauses before invocation. On `client.resume(state.run_id, HumanDecision(approved=True, ...))`, the runtime checks the decision: if approved, the tool runs and `Reply`s; if not, the LLM sees a "the human declined this action" tool-result and continues reasoning.

**Per-agent override of a tool's gate is out of scope for v1.** The tool author owns the gate. If two policies are needed, declare two tools (`execute_trade` gated, `execute_trade_unattended` ungated) and assign each agent the right one — explicit, no hidden override surface.

**Form B (secondary) — `@agent.interrupt_when(...)`.** For the rare case where the LLM output itself signals "I want a human to look at this," declare a decorator that runs after each LLM turn:

```python
class ApprovalRequired(BaseModel):
    proposed_action: str
    risk_score: float

@trading_agent.interrupt_when(output_type=ApprovalRequired, resume_with=HumanDecision)
async def needs_review(ctx: Context, output: ApprovalRequired) -> bool:
    """Decide whether to pause for human input based on the LLM's structured output."""
    return output.risk_score > 0.5
```

Form B matches *by output type* (or typed predicate) — not an arbitrary lambda over LLM output. Lambdas over free-text are non-deterministic across at-least-once replays; typed predicates on the output schema are deterministic. `resume_with=` declares the type the caller sends via `client.resume(...)`.

**Rejected: `interrupts={"name": Interrupt(when=lambda ...)}` dict on the constructor.** The name was never used; the lambda was a replay footgun; and HITL belongs on the tool (the action being gated) or the output validator (the LLM's claim), not an opaque rule-list. Both Form A and Form B compile to `Interrupt` (§3.1); see §4.11 for the Layer B sketch.

### 4.6 Escape hatch: dropping to Layer B (the runtime API)

A handful of advanced cases need to step outside the Agent recipe — custom orchestration nodes that aren't agents (a router, a saga coordinator, a translator between two agents with incompatible schemas), tool authors implementing tools directly, or users who want full control over an unusual flow. For these, write a Handler.

```python
from calfkit import Context, Worker, handler
from calfkit.actions import Call, Done, Reply

@handler(topic="ingest.csv.row")
async def csv_ingester(ctx: Context, row: CSVRow) -> Reply[Ack] | Call:
    # Custom orchestration: validate, enrich, then route to the right agent.
    if not row.is_valid():
        return Reply(Ack(status="invalid"))
    enriched = await enrich(row)
    if enriched.suspicious:
        return Call(topic="agent.fraud-detector.in", payload=enriched)
    return Call(topic="agent.normal-processor.in", payload=enriched)

# Register at Layer B alongside agents:
worker.add(csv_ingester, fraud_detector_agent, normal_processor_agent)
```

Layer A and Layer B coexist in the same Worker. `worker.add(...)` accepts `Agent` instances, `@handler`-decorated functions, and `Tool` values via one varargs entrypoint. Namespace split: Action types live under `calfkit.actions`; the primary surface (`Agent`, `Worker`, `Context`, `Tool`, `Client`, `InMemoryWorker`, `Subscription`) lives at `calfkit`. The runtime layer is not internal — it is the canonical low-level surface.

### 4.7 `worker.add(agent)` — the full lifecycle, demystified

What happens when a user calls `worker.add(agent)`? This subsection unpacks the compilation step, because the magic only feels good if you can see through it on demand.

**Step-by-step, with concrete artifacts:**

```python
@tool
async def get_weather(ctx: Context, location: str) -> dict: ...

@tool
async def get_forecast(ctx: Context, location: str, days: int = 1) -> list[dict]: ...

weather_agent = Agent(
    name="weather-bot",
    model="openai:gpt-5.4",
    instructions="...",
    output_type=WeatherReport,
    tools=[get_weather, get_forecast],
)

worker.add(weather_agent)
```

1. **Topic derivation.** Agent input: `f"agent.{agent.name}.in"` (override via `worker.add(agent, topic=...)`). Agent events: `f"agent.{agent.name}.events"` (only when `worker.publish(agent, as_event=...)` is set). Tool input: `f"tool.{fn.__name__}"` (override at registration; `@tool` never accepts `topic=` — see §10.1). Subscriptions add one consumer subscription per `worker.wire(...)`/`worker.subscribe(...)` call. Reply routing rides the Envelope, not a fixed topic.

2. **Schema synthesis.** The agent's input/output Pydantic models register for envelope encode/decode. Each tool's JSON schema for the LLM is synthesized from its signature — parameter types become input properties, the docstring becomes the description, `ctx: Context` is dropped.

3. **Handler synthesis.** The Agent SDK constructs a Handler equivalent to:
   ```python
   # Synthesized — the user does not write this.
   @handler(topic="agent.weather-bot.in", reply=WeatherReport)
   async def _synthesized_weather_bot_handler(ctx, req):
       return await _AgentLoopRecipe.run(
           ctx=ctx,
           agent=weather_agent,  # the Agent declaration
           request=req,
           # The recipe internally:
           #   1. Calls the LLM with instructions, history, tool schemas, user msg.
           #   2. If LLM selected a tool: returns Call(topic=tool_topic, payload=tool_args).
           #      - If the tool was marked `requires_approval=True`, return Interrupt instead.
           #   3. If LLM produced a final answer: returns Reply(output_type(answer)).
           #      - If `@agent.interrupt_when(...)` matches, return Interrupt instead.
           #   4. On Reply re-entry (tool result back): appends to history, loops back to (1).
       )
   ```
   This Handler is registered with the runtime exactly as if the user had written it. From the runtime's perspective, there is no difference between a "synthesized agent handler" and a hand-written `@handler`.

4. **Tool Handler registration.** Each `Tool` value referenced by an agent (whether produced by `@tool`, `Tool.external(...)`, or `Tool.from_agent(...)`) registers a runtime `@handler` for its tool topic — if it isn't already registered. A tool is a Handler whose input is the tool args and output is the tool result. The runtime decodes/encodes, dispatches the tool function, and Replies. `Tool.external(...)` registers no Python body — the deployment expects another service (possibly in another language) to consume the tool topic. `Tool.from_agent(...)` doesn't add a new Handler — the agent already has its request-topic Handler registered; the factory just wraps the reference. Multiple agents referencing the same tool register the Handler exactly once: tools are shared services, not per-agent copies.

5. **Consumer subscriptions wired.** The Worker now subscribes to:
   - `agent.weather-bot.in` (consumer group: `{group_prefix}.agent.weather-bot`) — the request entrypoint.
   - One additional consumer subscription per `worker.wire(...)` / `worker.subscribe(...)` targeting this agent (consumer group: `{group_prefix}.agent.weather-bot.sub.<source-or-topic>`) — each subscription is its own consumer group so an agent's reactions don't compete with other consumers of the same source events.
   - `tool.get_weather` (consumer group: `{group_prefix}.tool.get_weather`)
   - `tool.get_forecast` (consumer group: `{group_prefix}.tool.get_forecast`)
   - The shared runtime topics: `calf.fanout-agg`, `calf.runs-state` (if needed).

6. **Inspection.** `worker.inspect(format=...)` renders the materialized topology in text/dict/dot formats — a meaningful DX moment for production deployments.

7. **Overrides at registration time.** Defaults are overridden on `worker.add(...)` — the Agent declaration itself stays portable.
   ```python
   worker.add(
       weather_agent,
       topic="custom.weather.in",          # override request topic
       group_id="custom-weather-group",    # override consumer group
       max_turns=10,                       # cap on LLM iterations
       on_tool_error="continue",           # "continue" | "fail" | callable
   )
   ```

   Agent-level behaviors that ride with the agent (output validators, dynamic instructions, interrupts) attach via instance decorators on the agent itself — the Pydantic-AI shape — not as constructor kwargs:

   ```python
   @weather_agent.output_validator
   async def validate_no_pii(ctx: Context, output: WeatherReport) -> WeatherReport:
       """Reject outputs that leak user PII; raise ModelRetry to let the LLM try again."""
       ...
   ```

   Workers carry deps via `worker.deps_from(create_deps)`; per-call deps don't survive replays and are an anti-pattern (§13).

See §4.9 for the full agent-level / worker-level surface map. Defaults deliver Pydantic-AI-class ergonomics; escape hatches are uniformly available — each on the right object (deployment on Worker, behavior on Agent, topology on Worker).

### 4.8 Real-time fan-out via Agent SDK — the user never sees `Fan`

```python
@tool
async def fetch_profile(ctx: Context, user_id: str) -> dict: ...

@tool
async def fetch_risk(ctx: Context, user_id: str) -> dict: ...

@tool
async def fetch_history(ctx: Context, user_id: str) -> dict: ...

enrichment_agent = Agent(
    name="enricher",
    model="openai:gpt-5.4",
    instructions=(
        "Enrich the user record with profile, risk, and history data. "
        "Query all three sources in parallel when possible."
    ),
    output_type=EnrichedRecord,
    tools=[fetch_profile, fetch_risk, fetch_history],
)

worker.add(enrichment_agent)
```

If the LLM picks all three tools in one turn (its native parallel-tool-call behavior + prompt instructions), the Agent recipe emits `Fan` automatically. The aggregator (§9) collects Replies and re-enters the agent with all three results visible to the next LLM turn. The user never writes `Fan`, never touches `fan_id`. Layer B power is preserved (an Extension can intercept `FanDispatched` for telemetry); Layer A users are shielded.

No `parallel_tools=` constructor kwarg: parallel selection is a property of the LLM and the prompt, not the tools. A uniform on/off bit doesn't match how OpenAI / Anthropic expose the dispatch shape.

### 4.9 Surface map — Pydantic AI → calfkit Agent SDK

A Pydantic AI user re-authoring in calfkit learns only what changes when the agent runs distributed.

| Pydantic AI                          | Calfkit Agent SDK                                | Notes                                                                                              |
|--------------------------------------|--------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `Agent(model=..., instructions=...)` | `Agent(model=..., instructions=..., tools=[...])` | `system_prompt=` is a deprecated alias. Tools attach via `tools=[...]`, never via an instance decorator. |
| `@agent.tool` / `@agent.tool_plain` (in-process registry) | `@tool` (module-level only) — see §10              | Deliberate divergence — calfkit tools are standalone Kafka services, not agent-bound. See §10.1. |
| `RunContext[Deps]`                   | `Context[Deps]`                                  | Single generic parameter, matching Pydantic AI and OpenAI Agents. State surfaces as `ctx.state` (typed via inference, or `Any` if not declared). |
| `output_type=MyModel`                | `output_type=MyModel`                            | Identical surface; behavior differs by type — see the §4.9 note below. |
| `@agent.output_validator`            | `@agent.output_validator`                        | Decorator on the agent instance. Raises `ModelRetry` to retry. Agent-bound — operates on the agent's LLM output. |
| `agent.instructions(dynamic_fn)` / dynamic callable | `Agent(instructions=dynamic_fn)` where `dynamic_fn: (ctx: Context[Deps]) -> str \| Awaitable[str]` | Matches Pydantic AI's single-arg shape (rejecting OpenAI Agents' two-arg form). |
| `Agent.run(input)` (in-process call) | **Not provided.** Use `InMemoryWorker.testing(agent)` for tests/REPL or `client.invoke(agent, req)` for production. | See G13 / §17.2. |
| `Runner.run(agent, input)` (OpenAI Agents) | `client.invoke(agent, req)` (Kafka) or `wrk.invoke(agent, req)` (InMemoryWorker) | Caller passes an agent reference; topic-string is the escape hatch (`ClientAgentRef`, §4.11). |
| Per-call `deps=`                     | `worker.deps_from(factory)` (Worker-scoped)      | Per-call deps don't survive replays; Worker-scoped is the canonical form. |
| Streaming output (`agent.run_stream`)| Not in v1.0                                      | Streaming tokens is out of scope for v1.0; see §1.5. |
| Tool-call retry via `ModelRetry`     | `ModelRetry` + RetryExtension | `ModelRetry` for LLM-retry-on-validation; RetryExtension for transport-level (exception, network). |
| Sub-agent as a tool (`agent.as_tool(...)`) | `Tool.from_agent(other_agent, name=...)`         | Factory paralleling `Tool.external(...)`. No `agent.as_tool()` — see §10.1. |
| `handoffs=[...]` (OpenAI Agents kwarg) | `agent.handoffs_to(*peers)` method               | Method instead of kwarg keeps the constructor narrow. |
| HITL via `requires_approval` on tool | `@tool(requires_approval=True, resume_with=DecisionT)` (primary); `@agent.interrupt_when(output_type=..., resume_with=...)` (secondary) | See §4.5. |
| `model="openai:gpt-5.4"`             | `model: Model \| KnownModelName \| str` | Adopts Pydantic AI's `KnownModelName` `Literal[...]` for IDE autocomplete; raw `str` is the escape hatch. |

**Behavior note on `output_type`.** `output_type=str` (default) lets the LLM produce free text; `output_type=MyPydanticModel` forces structured output via provider-native enforcement (OpenAI's structured outputs, Anthropic's tool-use). Discriminated unions (`Foo | Bar`) are supported per Pydantic AI's pattern. Migration guide should call out this LLM-API shape change prominently.

**What's on the Agent instance** (methods/decorators after construction):

| Method / decorator | Purpose |
|---|---|
| `@agent.output_validator` | Post-output validator on the agent's LLM output; raises `ModelRetry` to retry. |
| `@agent.interrupt_when(output_type=..., resume_with=...)` | Output-type-based HITL gate on the agent's LLM output. |
| `agent.handoffs_to(*peers)` | Register handoff peers. |

Notably absent: any `@agent.tool` or `agent.as_tool()`. See §10.1.

**Module-level decorators and factories for tools:**

| Decorator / factory | Purpose |
|---|---|
| `@tool` / `@tool(requires_approval=..., resume_with=...)` | Author a Python tool as a standalone Kafka service. Topic auto-derives from the function name. |
| `Tool.external(name=..., input=..., output=...)` | Declare a tool implemented in another service/language. |
| `Tool.from_agent(agent, name=..., description=...)` | Wrap an agent as a tool for another agent (sub-agent pattern). |

**What's on the Worker instance** (deployment-time methods):

| Method | Purpose |
|---|---|
| `worker.add(*items, topic=None, group_id=None, max_turns=None, on_tool_error=None)` | Register agents, handlers, and tools. `topic=`/`group_id=` are the canonical deployment-side topology overrides (G14). |
| `worker.use(*extensions)` | Attach extensions (onion order). |
| `worker.deps_from(factory)` | Set the deps factory (called once at startup). |
| `worker.publish(agent, as_event=EventType)` | Mark this agent's output as a published event stream. |
| `worker.wire(source, target, payload, *, filter=None, react=None, group_id=None, start_from='latest', max_concurrency=None, cross_tenant=False)` | Subscribe `target` to `source`'s events. |
| `worker.subscribe(target, topic, payload, **kwargs)` | Subscribe `target` to an arbitrary Kafka topic. |
| `worker.runtime(...)` | Internal-topic and dedup overrides; 99% of users skip this. |
| `worker.inspect(format='text' \| 'dict' \| 'dot')` | Print/return materialized topology. |
| `worker.run()` (async) | Main loop. Use `asyncio.run(worker.run())` to block. |
| `worker.drain()` | Stop consuming new messages; finish in-flight. |

### 4.10 Head-to-head — "Temporal + Pydantic AI" vs Calfkit

If a developer can get the same outcome by running Pydantic AI inside a Temporal activity, calfkit has no reason to exist. Below is what each looks like, and why calfkit's surface is materially smaller for the *same* feature set.

**Goal:** ship an agent with three tools, one of which is implemented in Go, that produces durable, replayable runs, scales horizontally, and supports human-in-the-loop approval.

**Approach 1 — Temporal + Pydantic AI.**

The workflow file (~30 lines): a `@workflow.defn` class with `execute_activity(run_pydantic_ai_agent, ...)`, a `@workflow.signal` for HITL approval, plus `workflow.wait_condition(...)` to gate the second activity. The worker file (~15 lines): `Client.connect(...)` + `Worker(client, task_queue=..., workflows=[...], activities=[...])`. The core code is in activities and the cross-language service:

```python
# activities.py (Python — Pydantic AI agent + Python tools)
from temporalio import activity
from pydantic_ai import Agent, RunContext
import requests  # for calling the Go service over HTTP

agent = Agent(
    model="openai:gpt-5.4",
    system_prompt="...",
)

@agent.tool
def fetch_quote(ctx: RunContext, symbol: str) -> dict:
    ...

@agent.tool
def fetch_portfolio(ctx: RunContext, user_id: str) -> dict:
    ...

@agent.tool
def execute_trade(ctx: RunContext, order: BuyOrSellOrder) -> TradeConfirmation:
    # Cross-language: must build an HTTP gateway or gRPC client to the Go service.
    response = requests.post("http://go-trade-service:8080/execute", json=order.model_dump())
    return TradeConfirmation.model_validate(response.json())

@activity.defn
async def run_pydantic_ai_agent(req: TradeRequest) -> AgentOutput:
    result = await agent.run(req.natural_language)
    return AgentOutput(result=result.data, requires_approval=needs_approval(result))


# go_trade_service/main.go (Go — separate HTTP service)
package main

import "net/http"

func main() {
    http.HandleFunc("/execute", handleExecute)
    http.ListenAndServe(":8080", nil)
}

func handleExecute(w http.ResponseWriter, r *http.Request) {
    // Parse JSON, call exchange API, return JSON. Manual transport.
}
```

Plus a Temporal cluster (3-5 nodes), Temporal UI, Postgres/Cassandra backing Temporal, and the Go service deployed separately with its own HTTP server.

**Count:**
- Python LOC (workflow + activities + worker + agent): **~80 lines** of orchestration code, **plus** the tool implementations.
- Go LOC: **~30 lines** of HTTP server boilerplate, **plus** the tool implementation.
- Concepts the developer must learn: Temporal Workflow, Activity, signal, query, retry policy, task queue, Temporal client, Pydantic AI Agent, RunContext, HTTP gateway, Go HTTP server, request/response JSON marshaling.
- Infrastructure components: Temporal cluster (3-5 nodes), Temporal UI, Postgres/Cassandra for Temporal, Pydantic AI runtime, Go HTTP service, HTTP load balancer or service mesh.
- "Wiring code" the developer writes: the HTTP gateway, the JSON marshaling, the Temporal signal plumbing for HITL, the activity timeouts, the Workflow → Activity dispatch.

**Approach 2 — Calfkit.**

```python
# trading_agent.py (Python — calfkit)
import asyncio
from calfkit import Agent, Context, Tool, Worker, tool
from pydantic import BaseModel

class TradeRequest(BaseModel):
    user_id: str
    natural_language: str

class TradeResult(BaseModel):
    confirmation_id: str
    fill_price: float

class HumanDecision(BaseModel):
    approved: bool

execute_trade = Tool.external(
    name="execute_trade",
    description="Submit a buy or sell order.",
    input=BuyOrSellOrder,
    output=TradeConfirmation,
    requires_approval=True,         # the external tool is HITL-gated
    resume_with=HumanDecision,
)

@tool
async def fetch_quote(ctx: Context, symbol: str) -> dict:
    """Get the latest market quote."""
    ...

@tool
async def fetch_portfolio(ctx: Context, user_id: str) -> dict:
    """Get the user's portfolio."""
    ...

trading_agent = Agent(
    name="trader",
    model="openai:gpt-5.4",
    instructions="You execute trades on behalf of users.",
    output_type=TradeResult,
    tools=[execute_trade, fetch_quote, fetch_portfolio],
)


async def main() -> None:
    worker = Worker(bootstrap_servers="kafka:9092").add(trading_agent)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

```go
// execute_trade.go (Go — calfkit-go)
package main

import "github.com/calfkit/calfkit-go"

func main() {
    w := calfkit.NewWorker("kafka:9092")
    w.AddTool("execute_trade", executeTrade)
    w.Run()
}

func executeTrade(ctx calfkit.Context, req BuyOrSellOrder) (TradeConfirmation, error) {
    // place the order ...
}
```

**Count:**
- Python LOC: **~40 lines**, including the Agent declaration, three tools (one with HITL), one external tool, and worker bootstrap.
- Go LOC: **~12 lines** (no HTTP server, no JSON marshaling — calfkit-go handles transport and serialization).
- Concepts the developer must learn: Agent, `@tool`, `Tool.external`, Worker, `requires_approval`. That's it for a hello-world distributed agent. (Five concepts at Layer A; the Layer B concepts are not required for this example.)
- Infrastructure components: Kafka (likely already deployed). No Temporal cluster. No HTTP service mesh. No Postgres for the SDK.
- "Wiring code" the developer writes: zero.

**Where calfkit wins this comparison:**
- ~50% less Python code, ~60% less Go code, with no manual transport.
- No new cluster to operate (Kafka is already present in most environments where this scenario is realistic).
- Cross-language tools are *the wire format*, not an HTTP gateway. Schema synthesis is automatic on both sides.
- HITL is one config block on the Agent, not a Workflow signal + Activity dispatch round-trip.
- Replay/durability is implicit in Kafka. The runs-state topic is the audit log; you don't query Temporal's history.

**Where Temporal still wins:** strong determinism (workflows deterministic by construction); mature operational tooling (time-travel debugging UI); track record at scale.

**The thesis:** calfkit's reason to exist is the *combination* of (a) Pydantic-AI-tier authoring ergonomics, (b) Kafka-native distributed deployment, (c) cross-language tools as a wire-format primitive, (d) broker-native durability. No other system in this space offers that combination.

### 4.11 Layer B reference examples — what the runtime surface looks like

Illustrative Layer B sketches — what the Agent SDK compiles down to. Most users do not author at this level; these exist for Extension/tool authors and the curious. §11 has the deeper orchestration/choreography patterns.

```python
from calfkit import Context, handler
from calfkit.actions import Call, Fan, Interrupt, Reply
```

**Handoff** (what `agent.handoffs_to(...)` synthesizes — `Call` to the peer agent's topic; its `Reply` flows back via the frame stack):

```python
@handler(topic="agent.triage.in", reply=str)
async def triage(ctx: Context, req: UserReq) -> Reply[str] | Call:
    intent = await classify(req.text)
    if intent == "billing":
        return Call(topic="agent.billing.in", payload=BillingReq(text=req.text))
    return Reply("I'm not sure how to help with that.")
```

**Parallel fan-out** (what the Agent recipe synthesizes for multi-tool LLM turns — first entry returns `Fan`; aggregator re-enters with populated `ctx.fan_results`):

```python
@handler(topic="enrich.in", reply=EnrichResp)
async def enrich(ctx: Context, req: EnrichReq) -> Fan | Reply[EnrichResp]:
    if ctx.fan_results is None:
        return Fan(
            calls=[
                Call(topic="lookup.profile.in", payload=ProfileReq(req.user_id), key=b"profile"),
                Call(topic="lookup.risk.in",    payload=RiskReq(req.user_id),    key=b"risk"),
            ],
            aggregator=AggregatorSpec(timeout=timedelta(seconds=30), on_partial="error"),
        )
    return Reply(EnrichResp(profile=ctx.fan_results[b"profile"], risk=ctx.fan_results[b"risk"]))
```

**HITL** (what `@tool(requires_approval=True)` and `@agent.interrupt_when(...)` synthesize — first entry returns `Interrupt`; resume populates `ctx.resume_payload`):

```python
@handler(topic="approval.agent.in", reply=ApproveResp)
async def approval_agent(ctx: Context, req: ApproveReq) -> Reply[ApproveResp] | Interrupt:
    if not ctx.resume_payload:
        return Interrupt(reason=f"awaiting approval of: {req.proposed_action}",
                         resume_topic="approval.agent.resume")
    decision: HumanDecision = ctx.resume_payload
    return Reply(ApproveResp(approved=decision.approved, notes=decision.notes))
```

The Handler re-runs from the top on resume; `ctx.once()` (§16) gates non-idempotent side effects. Custom Extensions are in §12; in-memory dispatcher tests are in §17.2.

**Client refs.** When the client doesn't import the Agent (typical distributed deployment — client is a web service in another repo), use a `ClientAgentRef`:

```python
from calfkit import ClientAgentRef

trader_ref = ClientAgentRef(name="trader", input_type=TradeRequest, output_type=TradeResult)
handle = await client.invoke(trader_ref, req)
```

The proxy resolves to `agent.trader.in`. A bare-topic-string form (`client.invoke_topic(...)`) is the escape hatch for ad-hoc tooling.

---

## 5. Architecture layers

```
+---------------------------------------------------------------+
| Layer 4: USER CODE                                            |
|   Handlers (async functions)                                  |
|   Extensions (3 primitives)                                   |
|   Deps (user-provided)                                        |
|   State schemas (Pydantic / msgspec / user choice)            |
+---------------------------------------------------------------+
                            ^   |
                  Action /  |   |  Context / Message
                  Lifecycle |   v
+---------------------------------------------------------------+
| Layer 3: SDK RUNTIME (calfkit core)                           |
|   - Handler dispatcher                                        |
|   - Action interpreter                                        |
|   - Extension chain (around_invoke, around_publish, on_event) |
|   - Run lifecycle manager (RunStarted/Interrupted/Completed)  |
|   - Fan-out aggregator (compacted-topic-backed)               |
|   - Idempotency table (publish dedup)                         |
|   - Runs-state writer/reader                                  |
|   - Tool catalog (JSON schema synthesis)                      |
|   - OTel emitter                                              |
+---------------------------------------------------------------+
                            ^   |
              Envelope +    |   |  Envelope + headers
              headers       |   v
+---------------------------------------------------------------+
| Layer 2: TRANSPORT                                            |
|   - aiokafka consumer (per-handler subscription)              |
|   - aiokafka producer (per-Worker, shared)                    |
|   - Rebalance listener (checkpoints aggregator state)         |
|   - Header codec (str-typed, x-calf-* reserved)               |
|   - Partition strategy (key = run_id by default)              |
|   - Serializer/deserializer (lenient JSON or Schema Registry) |
+---------------------------------------------------------------+
                            ^   |
              Kafka         |   |  Kafka
              wire          |   v
+---------------------------------------------------------------+
| Layer 1: KAFKA CLUSTER                                        |
|   - Topics per Handler                                        |
|   - calf.runs-state (compacted)                               |
|   - calf.fanout-agg (compacted)                               |
|   - calf.idempotency (compacted, short retention)             |
|   - calf.dlq.<source> (log, long retention)                   |
|   - Consumer groups per Worker.handler                        |
+---------------------------------------------------------------+
```

### 5.1 Layer responsibilities and contracts

**Layer 4 (User Code)**

Two sub-tiers — Layer 4A (Agent SDK, the default) and Layer 4B (runtime primitives, the escape hatch) — see §4 for the full discussion. Both share the same contract with Layer 3:
- Provides Deps via Worker config.
- Does *not* call `broker.publish`. Does *not* serialize/deserialize Envelopes. Does *not* touch `correlation_id` directly.

**Layer 3 (SDK Runtime)**
- Pulls a `ConsumerRecord` from Layer 2; deserializes into `Envelope`; constructs `Context` for the user.
- Resolves the message's *invocation kind* (request, event, tool-reply, resume) from `x-calf-action-kind` and the Handler's binding (an agent's request input topic produces a request-shaped run; an event subscription produces an event-shaped run).
- Calls user Handler via the `around_invoke` Extension chain. For event-triggered Agent runs, the runtime sets `ctx.trigger.kind = "event"` so observers can distinguish.
- Interprets returned Action via the `around_publish` Extension chain.
- Writes to runs-state on `Interrupt`/`RunStarted`/`RunCompleted`.
- Owns the fan-out aggregator entirely. User Handlers never touch aggregator topics.
- **If the deployment registered `worker.publish(agent, as_event=EventType)`**: after a Run terminates via `Reply`, the runtime additionally `Emit`s the payload to `agent.<name>.events`. For event-triggered Runs (no waiting caller), this is the sole final-publish; for request-triggered Runs, both the `Reply` to the caller and the `Emit` to the events topic happen, deduped by `(run_id, action_id)`.
- Emits OTel spans and `on_event` lifecycle events.
- Commits Kafka offset only after Action is fully resolved.

**Layer 2 (Transport)**
- Wraps aiokafka with calfkit-specific header typing (`HDR_RUN_ID`, `HDR_FRAME_ID`, etc.).
- Owns the partitioner: by default, `key = run_id` so the same Run's hops land on the same partition (helps with frame ordering and the aggregator). Override per-Handler if needed.
- Owns the rebalance listener — on partition revocation, the Worker pauses dispatch and finalizes any partition-scoped state.
- Owns the serializer. Default: lenient JSON. Opt-in: Confluent Schema Registry (Avro/Protobuf).

**Layer 1 (Kafka)**
- Pure infrastructure. The SDK assumes Kafka 3.0+ for KRaft, idempotent producers, and exactly-once-semantics support on the transactional producer (we don't currently use EOS; we may in v1.1).

### 5.2 What's user-visible vs internal

**User-visible** (Layer A and B — see §4.9 for the full surface map):
- `Agent`, `@tool`/`Tool.external`/`Tool.from_agent`, `Worker`, `Client`, `InMemoryWorker`, `Subscription`, `Context[Deps]`, `Handler`, `Action` algebra, `Extension` with three primitives, `LifecycleEvent`, `ClientAgentRef`, `ModelRetry`, the reserved `x-calf-*` header names.

**Internal** (subject to change):
- `Envelope` shape, frame stack, aggregator topic names, idempotency table, header decoding, rebalance listener, consumer-group naming convention, aiokafka usage.

---

## 6. Wire format

### 6.1 Envelope shape

```python
class Envelope(BaseModel):
    """Wire format. Stable across calfkit versions starting at v1.0."""
    schema_version: Literal["1.0"]
    action_kind: Literal["call", "reply", "emit", "tailcall", "fan_child"]
    # "fan_child" = one Call within a Fan; "emit" = choreography (no caller awaits).

    run_id: str             # uuid7 hex; durable run aggregate key
    correlation_id: str     # per-hop request/reply joining key
    frame_id: str           # uuid7 hex; this frame's id (the frame this Action created)
    parent_frame_id: str | None  # the frame this Action is a child of

    state_schema: str | None  # opaque to runtime; user-supplied type identifier
    state_schema_version: str | None
    state: bytes              # opaque to runtime; user-deserialized

    payload_schema: str
    payload: bytes            # the Action's payload (msgspec/Pydantic-serialized)

    call_stack: list[Frame]   # for Call/Reply; the frame stack snapshot.
                              # For action_kind="emit" envelopes, call_stack is always empty:
                              # there is no caller to reply to.

    # For Fan envelopes only:
    fan_id: str | None
    fan_index: int | None
    fan_total: int | None
    fan_aggregator_topic: str | None  # where to publish the Reply for aggregation
```

**Why no new envelope variant for "event":** an event is just an `Emit` envelope (`action_kind="emit"`) that someone subscribes to. The producer's behavior is identical whether 0, 1, or N consumers subscribe. The consumer's reaction shape (start a new Run, observe, filter) is configured via the `Subscription` declaration at the consumer side, not signaled on the wire. This keeps the wire surface small and lets producers and consumers evolve independently — the choreography wiring is fully outside the envelope.

**Why discriminated union with opaque state bytes:**

The current `Envelope` at `calfkit/models/envelope.py:9` puts the strongly-typed `SessionRunContext` inline. That's pleasant for in-process inspection but it forces the SDK to know the user's state type, which couples the wire format to user schema evolution. Opaque-bytes lets:

1. The user evolve `State` without bumping `Envelope.schema_version`.
2. The runtime skip deserializing state when handling pure routing concerns (e.g., the aggregator only reads `fan_id`, `fan_index`, and the Reply payload; it never deserializes State).
3. Schema Registry mode coexist cleanly: state is a separate, independently-versioned schema; envelope wraps it.

The cost is one extra deserialization step in the Handler-entry path — measured at sub-millisecond for typical payloads.

### 6.2 Reserved Kafka headers

All headers are UTF-8 strings, encoded to bytes per aiokafka convention. Header names follow `x-calf-*` reserved prefix.

| Header                | Type | Lifecycle                                  | Purpose |
|-----------------------|------|--------------------------------------------|---------|
| `x-calf-run-id`       | str  | Set on first publish in a Run; copied through every hop | Cross-hop run aggregate key. Cheap to inspect for dedup, OTel, DLQ filters. |
| `x-calf-correlation-id` | str | Per-hop; new value for each Action publish | Per-hop request/reply joining (for client `execute` calls). |
| `x-calf-frame-id`     | str  | Set on each publish; matches Envelope.frame_id | Frame-level dedup; promoted from envelope to header per prior design memory. |
| `x-calf-parent-frame-id` | str | Set if this publish is a reply/child       | Allows OTel parent-span reconstruction without deserializing envelope. |
| `x-calf-tenant-id`    | str  | Set by client; copied through every hop    | Multi-tenancy default. |
| `x-calf-emitter`      | str  | Set on every publish                       | Emitting handler/node id. Already exists at `_protocol.py:21`. |
| `x-calf-emitter-kind` | str  | Set on every publish                       | Closed-set: `handler|tool|client|aggregator|runtime`. Replaces the 0.x `NodeKind`. |
| `x-calf-action-kind`  | str  | Set on every publish                       | Matches `Envelope.action_kind`; cheap routing header. |
| `x-calf-trace-context`| str  | Set if OTel is active                      | W3C `traceparent` value for OTel propagation. |
| `x-calf-action-id`    | str  | Set on every publish                       | Per-Action idempotency key. |
| `x-calf-fan-id`       | str  | Set only on fan-children and aggregator-state writes | Aggregator partition key. |
| `x-calf-attempt`      | str (int) | Incremented by retry extension            | For DLQ filters and OTel attributes. |
| `x-calf-schema-version`| str | Set on every publish                       | Matches `Envelope.schema_version`. Cheap to inspect for old-consumer rejection. |
| `x-calf-event-type`   | str  | Set on `Emit` publishes (choreography envelopes) | Fully-qualified payload type (e.g. `myapp.events.NewsArticle`) — enables broker-level filtering and observability without payload deserialization. Matches `Envelope.payload_schema` but is cheap to inspect from header tooling. |
| `x-calf-producer-agent` | str | Set on `Emit` publishes from an Agent      | The name of the Agent that produced the event (e.g. `news-watcher`). Lets a subscriber's filter, observability, or routing decide based on producer identity without payload inspection. Distinct from `x-calf-emitter` (which is the *handler/topic-level* identifier — for an event published by the runtime as part of an Agent recipe, `x-calf-emitter` is the runtime and `x-calf-producer-agent` is the Agent name). |

User-defined headers are allowed; they MUST NOT use the `x-calf-` prefix. Recommended convention: `x-<org>-<name>`.

Why these are headers and not body fields: cheap broker-level inspection (kafka-console-consumer, Connect SMTs, ksqlDB, MirrorMaker), tracing collectors that inject `traceparent` natively, header-based routing in topology rewrites. The existing `calfkit/_protocol.py` already enshrines this pattern.

### 6.3 Schema strategies

**Mode A — Lenient JSON (default).**

- `payload` and `state` are JSON-encoded UTF-8 bytes.
- `payload_schema` is the fully-qualified Python class name (e.g. `myapp.models.OrderReq`) used for Pydantic round-trip.
- No central schema registry needed. Two services in different repos can publish to the same topic as long as their class shapes are compatible (Pydantic `extra="ignore"` plus field-default discipline).
- Suitable for early-stage, small-team deployments.

**Mode B — Schema Registry (opt-in).**

- `payload_schema` is a Confluent Schema Registry subject URN (e.g. `topic.weather.agent.in-value`).
- Payload is Avro or Protobuf binary (chosen at Worker config time).
- The SDK loads schemas at startup and caches; rejects payloads that fail compatibility.
- Suitable for cross-team contracts; tracks compatibility, evolves schemas independently of code.

**Recommendation:** ship Mode A in v1.0. Add Mode B as v1.1 as a thin adapter over the same Envelope shape (replace the bytes encoding).

### 6.4 Schema evolution rules

| Change                              | Lenient JSON | Schema Registry (BACKWARD compatibility) |
|-------------------------------------|--------------|------------------------------------------|
| Add optional field with default     | Safe         | Safe                                     |
| Add required field                  | Breaks new consumers reading old messages | Rejected |
| Remove field                        | Safe (`extra="ignore"`); semantic risk | Rejected for required fields |
| Rename field                        | Breaks       | Rejected                                 |
| Change field type compatibly (int→long) | Pydantic widens; usually safe | Allowed                                  |
| Change field type incompatibly      | Breaks       | Rejected                                 |
| Add new Action variant              | Old consumers fall into `_unknown` branch; SDK logs and DLQs | Bump `Envelope.schema_version` |

The `Envelope.schema_version` is bumped only on breaking envelope-level changes. User payload changes do not bump it.

### 6.5 Example envelopes (lenient JSON)

**Call** (the canonical shape; other variants are diffs against this):

```json
{
  "schema_version": "1.0",
  "action_kind": "call",
  "run_id": "01933a2c-...",
  "correlation_id": "01933a2c-...",
  "frame_id": "01933a2d-...",
  "parent_frame_id": null,
  "state_schema": "myapp.OrderState",
  "state_schema_version": "1",
  "state": "eyJ1c2VyX2lkIjoidTEifQ==",
  "payload_schema": "myapp.ClassifyReq",
  "payload": "eyJ0ZXh0IjoiaGVsbG8ifQ==",
  "call_stack": [
    {"frame_id": "01933a2d-...", "callback_topic": "client.reply.xyz"}
  ]
}
```

**Reply** (state propagates back): same envelope as the originating Call, with `action_kind: "reply"`, an updated `state`/`payload` reflecting the result, and `call_stack: []` (frame popped).

**Event** (`Emit` from an Agent registered with `worker.publish(watcher, as_event=NewsArticle)`): same envelope, with `action_kind: "emit"`, `state_schema: null` and `state: ""` (no caller state), `call_stack: []`. Headers carry `x-calf-event-type=myapp.events.NewsArticle` and `x-calf-producer-agent=news-watcher` for cheap broker-level inspection. Subscribers receive the envelope and create their own Run; the original `run_id` is propagated as `parent_run_id` on the new Run (see §11.B for lineage).

**Fan child**: same envelope with `action_kind: "fan_child"` plus the fan-only fields `fan_id`, `fan_index`, `fan_total`, and `fan_aggregator_topic` (the aggregator subscribes to that topic, indexed by `fan_id`).

---

## 7. State model

### 7.1 Three scopes

| Scope         | Lifetime              | Storage                         | Mutated by                |
|---------------|-----------------------|----------------------------------|---------------------------|
| **Frame**     | One Handler invocation| `Context.frame_state` (in-memory)| Handler body              |
| **Run**       | One full Run          | `Envelope.state` (opaque bytes); checkpoint to `calf.runs-state` on Interrupt | Handler body via `ctx.state` |
| **Long-term** | Across runs           | User-provided store (DB, vector, etc.) | User code via `ctx.deps`  |

- **Frame state** is throwaway — local variables in your handler.
- **Run state** is the typed user value that survives across hops. It lives in the Envelope (rides with every publish) and snapshots to `calf.runs-state` on `Interrupt` / `RunCompleted`.
- **Long-term memory** is *not* the SDK's concern. The SDK does not bundle a vector store, embedding model, or conversation database. Users plug their own via Deps.

### 7.2 Why opaque bytes on the wire

The 0.x `State` class (`calfkit/models/state.py:92`) is one Pydantic model holding `message_history`, `tool_calls`, `tool_results`, `final_output_parts`, `metadata`, `overrides`, `temp_instructions`, etc. — all framework-specific. Users wanting to extend state run into the `CompactBaseModel` footguns documented in the project memory:

- Optional fields without `= None` are excluded by `exclude_unset=True` then fail on re-validation.
- `.append()` on a list field does not mark the field as "set."

These are real bugs that bit the team historically. The structural fix is: the SDK has no business owning the user's state shape. v1's `state: bytes` field is opaque; the *Handler* (or a typed wrapper around it) chooses Pydantic, msgspec, dataclasses-json, or whatever. The SDK only needs to round-trip bytes.

What about the agent loop's message history, tool calls, tool results? Those are *agent-loop-recipe* concerns, not SDK-runtime concerns. The agent_loop helper (§4.2) owns a small typed `AgentLoopState` of its own and packs/unpacks it inside the user's state byte field, or layers it via composition (the user's State includes an `agent_loop: AgentLoopState` field).

### 7.3 Surviving restart, rebalance, scale-out

The state byte field rides on the wire. Therefore:

- **Process restart in the middle of a hop:** the consumer offset wasn't committed, the message is redelivered, the Handler re-runs with the same state bytes. No loss.
- **Partition rebalance:** the new owner of the partition reads the same offset, deserializes the same Envelope, runs the same Handler. No loss.
- **Scale-out:** adding a Worker to the consumer group just adds capacity. State lives in the wire, not in the Worker's RAM.
- **Worker crash mid-Action:** depends on which Action and how far through it. See §15 for failure modes.

What does *not* survive in-process: `Context.frame_state`, the Extension chain's local state per-invocation, anything in `Worker.__init__` that wasn't persisted somewhere. That's intentional — the contract is "all durable state crosses Kafka." Anything else is best-effort.

---

## 8. Run lifecycle

### 8.1 Run vs Hop vs Frame

- **Run:** logical agent execution. One `RunId`.
- **Hop:** one trip from publisher to consumer. Many hops per Run.
- **Frame:** one (caller, callee) pair within a Run. The call_stack tracks frames.

```
Client publishes Call to topic A   -> Hop 1, Run starts, Frame 0
  Handler A returns Call to topic B -> Hop 2,            Frame 1 pushed
    Handler B returns Reply         -> Hop 3,            Frame 1 popped
  Handler A re-enters via Reply     -> Hop 4 (same frame as Hop 1)
  Handler A returns Reply           -> Hop 5,            Frame 0 popped (Run ends)
```

### 8.2 State transitions

```
            +-> RunStarted ---> RunRunning ---> RunCompleted
            |                       |  ^
client.invoke                       |  | Resume
            |                       v  |
            |                  RunInterrupted
            |                       |
            +---------------------> RunFailed (via Fail or unhandled exception > retry limit)
```

| Event             | Trigger                                            | Side effect                          |
|-------------------|----------------------------------------------------|--------------------------------------|
| `RunStarted`      | First Action in a chain (`Emit`/`Call`) by a client | Upsert runs-state with `kind=running`|
| `RunRunning`      | Implicit — between RunStarted and a terminal       | No write; just the absence of a terminal |
| `RunInterrupted`  | Handler returns `Interrupt`                        | Upsert runs-state with `kind=interrupted` + state checkpoint + `resume_topic` |
| `RunCompleted`    | Reply pops the last frame, or `Done` at root       | Upsert runs-state with `kind=completed`; tombstone after grace period |
| `RunFailed`       | `Fail` Action, or unhandled exception past retry   | Upsert runs-state with `kind=failed` + error; tombstone after grace |

### 8.3 The runs-state compacted topic

```
Topic:      calf.runs-state
Key:        run_id (string, UTF-8)
Value:      RunStateRecord (JSON or Avro)
Cleanup:    compact, delete.retention.ms = 24h after tombstone
Partitions: high (default 50); key = run_id
```

```python
class RunStateRecord(BaseModel):
    run_id: str
    kind: Literal["running", "interrupted", "completed", "failed"]
    started_at: datetime
    last_updated: datetime
    parent_run_id: str | None
    tenant_id: str | None
    state_schema: str | None
    state_schema_version: str | None
    state: bytes | None         # checkpoint, populated on Interrupt
    resume_topic: str | None    # populated on Interrupt
    interrupt_reason: str | None
    interrupt_id: str | None
    completed_payload: bytes | None  # populated on Completed (optional, for late readers)
    error: ErrorPayload | None
    attempts: int
```

The topic is **runtime-owned**. User Handlers do not subscribe to it. The Worker writes to it; a small subset of client APIs read from it (`client.resume`, `client.get_run_state`).

### 8.4 ID generation and relationships

- `run_id` = uuid7 minted by the client (or by `client.invoke`) at Run start.
- `frame_id` = uuid7 minted by the runtime at each frame push.
- `correlation_id` = uuid7 minted per request/reply pair. For a client `execute` (request/reply), it's set on the outbound publish and matched on the inbound Reply. For internal hops, it's per-hop.
- `action_id` = uuid7 minted per Action emission (for idempotency).

Why uuid7 (time-ordered): preserves natural sort by time, useful for OTel traces and partition affinity. Compatible with existing 0.x usage at `client/base.py:98`.

---

## 9. Fan-out and aggregation

This is the most consequential design decision in v1. Going long.

### 9.1 The problem (current code)

`BaseAgentNodeDef._pending_batches: dict[str, PendingToolBatch]` at `calfkit/nodes/agent.py:50` is the parallel-fan-out aggregator. It's an in-process Python dict. The agent itself acknowledges the fragility at `agent.py:117-120`:

> `f"This indicates lost PendingToolBatch state (e.g. partition rebalance or process restart)."`

Failure mode:
- Worker P1 owns partition X. Agent receives a tool-result Reply, aggregates into `_pending_batches[cid]`. Batch is `n-1` of `n` complete.
- P1 crashes (or P1 leaves the consumer group, and P2 takes partition X).
- P2 receives the next Reply. `_pending_batches[cid]` doesn't exist in P2's memory. Batch state is gone. The aggregation is lost forever.

`ctx.state.tool_results` survives (it's in the Envelope) — but the *expectation* of "I dispatched 5 calls and am waiting for all 5" lives only in `_pending_batches`. Without it, the agent doesn't know whether to wait for more or proceed.

### 9.2 Design: compacted-topic-backed aggregator

```
+--- Worker P1 (also: Worker P2, P3, ...) ----------+
|                                                  |
|  Handler returns Fan(calls=[C1, C2, C3])         |
|       |                                          |
|       v                                          |
|  Runtime publishes 3 fan-child envelopes,        |
|    each annotated with fan_id + fan_index + fan_total
|    + fan_aggregator_topic = "calf.fanout-agg"    |
|                                                  |
+--------------|-----------|------------|----------+
               |           |            |
               v           v            v
        +-----------+ +-----------+ +-----------+
        | tool A    | | tool B    | | tool C    |
        +-----------+ +-----------+ +-----------+
               |           |            |
               | reply     | reply      | reply
               v           v            v
        +-------------------------------------------+
        |  calf.fanout-agg topic                    |
        |  partitioned by fan_id (key = fan_id)     |
        |  log-compacted, retention = 7d            |
        +-------------------------------------------+
                            |
                            v
        +---- Aggregator runtime (per Worker) ------+
        | On each fan-child Reply received:         |
        |   1. Read all messages with key=fan_id    |
        |      from the aggregator topic (from log) |
        |   2. If count == fan_total:               |
        |      - Re-dispatch the parent Handler     |
        |        with ctx.fan_results populated     |
        |      - Tombstone the fan_id key           |
        |   3. Else: do nothing, wait for more      |
        +-------------------------------------------+
```

### 9.3 The aggregator runtime

**Where it runs.** I considered three options:

| Option | Description | Recommendation |
|---|---|---|
| A. Inline in every Worker | Every Worker subscribes to `calf.fanout-agg` and aggregates | Reject — N×wasted bandwidth |
| B. Dedicated aggregator service | A separate process the user deploys | Reject — extra deployment burden |
| **C. Per-Worker, partition-affine** | Each Worker subscribes to `calf.fanout-agg` only for partitions assigned to it, keyed by `fan_id`. The same Worker that dispatched the Fan owns the aggregation. | **Pick this** |

Concretely:

- The aggregator topic has its own partitioning (key=`fan_id`).
- Every Worker process subscribes to it in a single consumer group `calf-fanout-agg-<cluster-id>`.
- Each Worker handles whichever partitions Kafka assigns it — the dispatching Worker is *not* guaranteed to be the one that aggregates. That's fine because the message stream is the state; the aggregator is stateless beyond it.

On Reply collection complete, the aggregator publishes a re-entry envelope back to the **parent Handler's topic**, carrying:
- The original parent's frame state (carried through the Fan envelopes)
- A `fan_results: dict[bytes, Payload]` field reconstructed from the collected children
- A header `x-calf-fanout-aggregator-reentry: true` so the parent Handler's Worker knows to populate `ctx.fan_results`

Aggregator state thus lives in *one place*: the compacted topic. No in-process map. No `_pending_batches`.

### 9.4 Concurrency / race handling

- Two consecutive Reply messages for the same `fan_id` land on the same partition (because key=`fan_id`), so they're serialized within Kafka. One Worker processes them in order.
- If the partition rebalances mid-aggregation: the new owner reads the log from the last committed offset; the partial state is reconstructed from already-published Replies (they're all in the topic).
- Duplicate Replies (at-least-once redelivery): the aggregator dedupes by `(fan_id, fan_index)`. A duplicate is a no-op.
- The fan-dispatching Handler may itself be redelivered (its inbound message wasn't ack'd before Fan was emitted). In that case, the *same* Fan with the *same* `fan_id` is emitted; the aggregator topic already has the prior fan-child Replies; the dedup table prevents re-publishing the child Calls; the parent re-entry already fired or is idempotent.

### 9.5 Tombstoning

When the aggregator publishes the re-entry to the parent and the parent's offset is committed (the re-entry was successfully consumed), the aggregator publishes a tombstone (null value) for the `fan_id` key. The topic's `delete.retention.ms` deletes the old records after the configured grace.

Tombstoning *timing matters*: if we tombstone too early, a late-arriving duplicate Reply will look like a fresh fan-out. The recommendation:

- Tombstone after the parent's re-entry is successfully consumed (we can observe this via OTel completion event or, more robustly, a 5-minute grace).
- `delete.retention.ms` = 24h after tombstone so MirrorMaker and slow consumers have a window to catch up.

### 9.6 Worker restart / rebalance behavior

- Worker P1 (aggregator partition owner) crashes mid-aggregation. State is in the topic, not in P1.
- Kafka reassigns P1's partition to P2.
- P2's aggregator subscriber rewinds to last committed offset (which is *before* the un-aggregated Replies, because P1 didn't commit those offsets when aggregation was incomplete).
- P2 re-reads the same Replies, deduces the same partial-state, and continues.

The single critical invariant: **don't commit the aggregator's consumer offset until the parent re-entry has been published**. Otherwise the partial state is lost on rebalance. The current FastStream router can't express this discipline cleanly; aiokafka can.

### 9.7 User-visible API for fan-out

```python
return Fan(
    calls=[
        Call(topic="lookup.profile.in", payload=req, key=b"profile"),
        Call(topic="lookup.risk.in",    payload=req, key=b"risk"),
    ],
    aggregator=AggregatorSpec(
        timeout=timedelta(seconds=30),
        on_partial="error",   # one of: "error", "use_partial", "continue"
        # When timeout fires:
        #   error: emit Fail Action on the parent re-entry
        #   use_partial: re-enter parent with ctx.fan_results containing only collected
        #   continue: re-enter parent with ctx.fan_results=None; user decides
    ),
)
```

`Call.key` is the user-friendly key (bytes) for indexing `ctx.fan_results` on re-entry. The runtime correlates it via `fan_index` internally.

### 9.8 Concrete walked-through example

Imagine a user requests enrichment for `user_id=u1`. The `enrich.in` handler fans out three lookups:

```
Hop 0 (Client publishes Call to enrich.in)
   x-calf-run-id=R1, frame_id=F0, action_kind=call
   |
   v
Hop 1 (Handler enrich receives at Worker W1)
   handler returns Fan(calls=[C_profile, C_risk, C_history])
   Worker W1 runtime:
     - mints fan_id=FAN1
     - publishes 3 envelopes to lookup.profile.in / lookup.risk.in / lookup.history.in
       each with: fan_id=FAN1, fan_index=0/1/2, fan_total=3,
                  fan_aggregator_topic=calf.fanout-agg,
                  parent_frame=F0
     - persists "FAN1 awaiting 3 replies, parent re-entry envelope template" to its
       own outbound buffer (not yet to Kafka, see below)
     - commits inbound offset
   |
   v
Hops 2a, 2b, 2c (lookup handlers run, possibly on different Workers)
   Each returns Reply(payload=...)
   Each Reply's destination = fan_aggregator_topic, with x-calf-fan-id=FAN1
   |
   v
Hop 3a (W2 receives fan-child Reply 0 at calf.fanout-agg, partition assigned to W2)
   Aggregator runtime:
     - log-state for FAN1 = [Reply0]
     - count 1/3, not yet complete
     - does NOT commit offset (this is the key invariant)
   ...
Hop 3b (W2 receives fan-child Reply 1)
     - log-state for FAN1 = [Reply0, Reply1]
     - count 2/3, not yet complete
     - does NOT commit offset
   ...
Hop 3c (W2 receives fan-child Reply 2)
     - log-state for FAN1 = [Reply0, Reply1, Reply2]
     - count 3/3, complete
     - publishes parent re-entry envelope to enrich.in with:
         action_kind=call (re-entry is structured as a self-call)
         x-calf-fanout-aggregator-reentry=true
         x-calf-fan-id=FAN1
         payload = parent's original payload
         fan_results = {b"profile": ..., b"risk": ..., b"history": ...}
     - commits the offset
   |
   v
Hop 4 (Worker W3 receives re-entry at enrich.in)
   Handler enrich receives with ctx.fan_results populated
   Returns Reply(EnrichResp(...))
   |
   v
Hop 5 (Client receives at reply topic)
```

If W2 crashes between Hop 3b and Hop 3c, Kafka reassigns calf.fanout-agg's partition; W4 takes over, reads from the un-committed offset (which is before Reply0), re-reads all three Replies, and emits the re-entry exactly once. (Dedup at the parent enrich Handler on `(run_id, fan_id)` ensures even if the re-entry is published twice, the parent only acts once.)

---

## 10. Tool model

### 10.1 Tools are first-class Kafka services — never owned by an Agent

A **Tool** is a single concept — a standalone Handler addressable by its own Kafka topic, with a synthesized JSON schema for the LLM. **Tools are never owned by an Agent.** Tool decorators/factories live at module scope; an Agent references tools via `tools=[...]`.

This is calfkit's structural divergence from Pydantic AI. Pydantic AI's `@agent.tool` decorator binds tools to an in-process `tool_registry` — appropriate because its tools execute inside the agent's Python process. Calfkit's tools execute as their own Kafka consumers: distinct services with their own topic, consumer group, scale unit, possibly their own language. The benefit is significant: one tool serves N agents without duplication; cross-language tools (§10.2) are a natural construction path, not an exception.

Three construction paths — one decorator and two factory classmethods — all producing the same `Tool` value:

**Form 1 (primary): `@tool` — the module-level Python tool.** Authored independently of any agent. Attached to N agents via `tools=[fn]`.

```python
from calfkit import tool, Context

@tool
async def get_weather(ctx: Context, location: str) -> dict:
    """Fetch current weather for a location."""
    ...

@tool
async def fetch_quote(ctx: Context, symbol: str) -> dict:
    """Fetch the latest market quote for a symbol."""
    ...

# Multiple agents can call the same tool — same Kafka topic, same consumer group, shared scale unit
weather_agent = Agent(name="weather-bot", ..., tools=[get_weather])
trading_agent = Agent(name="trader",      ..., tools=[fetch_quote, get_weather])
```

- **Topic is auto-derived** from the function name: `f"tool.{fn.__name__}"` → `"tool.get_weather"`. The `@tool` decorator does NOT accept a `topic=` kwarg. Explicit topic overrides — for cross-language interop with an existing topic convention, or for legacy compatibility — happen at deployment time: `worker.add(get_weather, topic="tool.weather.v2")`. See "Topic naming is a deployment decision" below.
- **LLM-facing name and description** come from the function name and docstring.
- **Input JSON schema** is synthesized from parameter types (Pydantic-AI-style). If the first parameter is annotated `ctx: Context` (or any parameter is typed as `Context`/`Context[Deps]`), it is dropped from the LLM-visible schema and injected by the runtime. Tools that don't need `ctx` simply omit it from the signature.
- **Output JSON schema** is synthesized from the return type.
- **HITL:** pass `requires_approval=True, resume_with=DecisionT` to gate execution behind human approval (§4.5). The gate is a property of the tool itself, not of the agent that calls it — if `execute_trade` needs human approval, every agent that uses it is gated. (HITL is intrinsic to the tool — see "Why `requires_approval=` IS on `@tool`" below.)
- **Bare-vs-call syntax:** `@tool` without parens is the common case (`@tool` then `async def fn(...)`). Use `@tool(...)` with parens when supplying `requires_approval=`, `resume_with=`, or other intrinsic tool options.

**Form 2 (external / cross-language): `Tool.external(...)`.** No Python body. The implementation lives in another service or language.

```python
execute_trade = Tool.external(
    name="execute_trade",
    description="Submit a buy or sell order; returns confirmation id.",
    input=BuyOrSellOrder,
    output=TradeConfirmation,
    requires_approval=True,           # HITL gate, optional
    resume_with=HumanDecision,
    # topic= defaults to f"tool.{name}"; override for an existing peer topic.
)

trading_agent = Agent(name="trader", ..., tools=[execute_trade])
```

The Pydantic models drive schema synthesis on both sides; the wire format is shared across calfkit's language SDKs. A Go service subscribed to `tool.execute_trade` with the calfkit-go SDK (eventual; v1.x roadmap) publishes Replies in the same Envelope format. The Python agent never knows or cares it was a Go tool. For users who'd rather not import Pydantic models from a shared contracts package, raw JSON schemas are also accepted: `Tool.external(name=..., input_schema={...}, output_schema={...})`.

**Sub-agents-as-tools: `Tool.from_agent(...)`.** An agent is already a Kafka service on its own request topic. To expose one agent as a tool to another, wrap it at the call site via a factory classmethod that parallels `Tool.external(...)`:

```python
research_agent = Agent(name="researcher", ..., output_type=ResearchReport)

writer_agent = Agent(
    name="writer",
    model="openai:gpt-5.4",
    instructions="You write articles from research material.",
    output_type=Article,
    tools=[Tool.from_agent(research_agent, name="research_topic")],
)
```

`Tool.from_agent(...)` produces a `Tool` value whose underlying topic is the source agent's request topic (`agent.researcher.in`). The LLM in `writer_agent` sees a tool named `research_topic` with input/output schemas drawn from `research_agent`'s input/output types. When the LLM picks it, the writer agent emits `Call(topic="agent.researcher.in", payload=...)`, exactly as if it had called any other tool. The two agents may run on different Workers, scale independently, even be written in different languages.

The factory shape — `Tool.external(...)` for cross-language, `Tool.from_agent(...)` for sub-agent — is deliberate: both produce a `Tool` value at the call site, both reuse the `tools=[...]` attachment point, neither requires a special instance method on `Agent`. There is no `agent.as_tool()` — that would imply the agent "owns" the tool, which contradicts the model.

**One `Tool` concept, three construction paths.** The user's mental model is "a tool is a callable thing the LLM can invoke." That maps to a single `Tool` class; construction paths map to user intent (Python here / external service / sub-agent). No `external_tool(...)` function, no `@function_tool` alias, no `agent.as_tool()` method. No `@agent.tool` either — Pydantic AI's `@agent.tool` reflects an in-process tool_registry dict, which calfkit doesn't have; a calfkit `@agent.tool` would lie about where the tool runs.

**Tools are orchestration-only.** A tool exists to be called by an agent (`Call` → `Reply`); tools are not subscribers. For "every time X happens, run Y," use an Agent wired via `worker.subscribe(...)` — not a tool. Tools satisfy LLM tool-call requests; choreography subscribers react to events the LLM never asked about.

**Topic naming is a deployment decision (G14).** Applied symmetrically to Layer A:

- `Agent(...)` declares identity via `name=`; topic auto-derives (`agent.<name>.in`). No `topic=`/`reply_topic=`/`events_topic=`/`group_id=` on the constructor.
- `@tool`-decorated functions declare identity via the function name; topic auto-derives (`tool.<function_name>`). No `topic=` on the decorator.
- Overrides at deployment registration: `worker.add(thing, topic="...")` for both Agents and Tools.
- `Tool.external(name=..., topic=...)` is the one Layer A spot `topic=` appears on a definition — there is no Python function to derive from, so the topic IS the wire contract with the peer.
- **Layer B `@handler(topic="...")` is the escape hatch.** Layer B handlers bind to topics dictated by external systems, so the topic IS the declaration's purpose.

**Why `requires_approval=` lives on `@tool` (not `worker.add(...)`)**. The gate is a property of the side effect, not the deployment. If `execute_trade` needs approval, every deployment that imports it inherits the gate. A deployment-time form would create a footgun (forget it once → silently ungated). `resume_with=DecisionT` is the typed payload shape, also intrinsic.

### 10.2 Cross-language tools — the moat

See `Tool.external(...)` in §10.1. This is the **moat** vs in-process SDKs (LangGraph, Pydantic AI, OpenAI Agents). They cannot do this without an FFI layer or an HTTP gateway. See §4.3 for a worked example and §4.10 for the head-to-head against "Temporal + Pydantic AI" where this cross-language story is concrete.

### 10.3 Tool catalog and discovery

For v1.0: tools register at Worker startup; the catalog is a per-Worker registry. Agents pass tool references explicitly.

For v1.1+: a topic-based catalog where tools publish their schema to `calf.tools.catalog` on startup; agents subscribe and discover. This is a clear evolution path, not a v1.0 requirement.

### 10.4 LLM-facing schema synthesis

```python
# From this:
@tool
async def fetch_quote(ctx: Context, symbol: str) -> dict:
    """Fetch the latest market quote for a symbol."""

# Synthesize this for the LLM (OpenAI function-calling format):
{
  "name": "fetch_quote",
  "description": "Fetch the latest market quote for a symbol.",
  "parameters": {"type": "object", "properties": {"symbol": {"type": "string"}}, "required": ["symbol"]}
}
```

`ctx` is dropped — invisible to the model, populated by the SDK. `@tool`, `Tool.external(...)`, and `Tool.from_agent(...)` all synthesize the same LLM-facing schema shape; the difference is purely where the implementation runs (this process, another service, or another agent).

### 10.5 Tool retries and DLQ

Tool failure modes:
- **Exception inside tool body**: caught by the runtime; subject to the Retry Extension (if registered). Default = 3 attempts with exponential backoff. After exhaustion → DLQ + `Fail` returned to caller.
- **Tool timeout** (no Reply within the configured budget): the caller's `Call` Action carries a `timeout`. If exceeded, the runtime emits a `Fail` to the caller. The tool itself may still complete and Reply later — the late Reply is dropped by the dedup table.
- **Tool deserialization failure**: the tool's worker logs and DLQs the inbound message; the caller times out as above.

DLQ topic naming: `calf.dlq.<source-topic>` (one DLQ per source). Justification: easier inspection ("show me everything that failed for `tool.execute_trade`"). Shared DLQs would lose this. Cost is more topics; modern Kafka clusters handle thousands fine.

### 10.6 Tool versioning

Tools are addressed by topic. To version: use a new topic (`tool.fetch_quote.v2`). The catalog can carry version metadata. Agents choose which version they bind to at Worker startup. Schema-Registry mode (Mode B) handles in-place evolution within a topic; topic-versioning handles breaking changes.

---

## 11. Multi-agent patterns

Multi-agent systems split into two structurally distinct shapes. Calfkit supports both as first-class patterns. **Choose deliberately** — they have different fault, scaling, and observability characteristics.

| Dimension                | Orchestration (§11.A)                     | Choreography (§11.B)                                 |
|--------------------------|-------------------------------------------|------------------------------------------------------|
| **Producer knows consumer?** | Yes — caller names the callee (via `agent.handoffs_to(...)`, `Tool.from_agent(...)` in `tools=[...]`, or a Layer B `Call`) | No — producer emits; subscribers are wired on the Worker |
| **Wiring location**      | On the Agent (handoffs, sub-agents-as-tools) | On the Worker (`worker.publish(...)`, `worker.wire(...)`, `worker.subscribe(...)`) |
| **Reply expected?**      | Yes — `Reply` flows back through the call stack | No — `Emit` is fire-and-forget; consumers may emit their own events downstream |
| **Coupling**             | Strong — caller breaks if callee is renamed / removed | Loose — producer doesn't know consumers; topic is the contract |
| **Run lineage**          | Single Run across all hops (sub-Runs are explicit opt-in) | Each consumer reaction starts a new Run with `parent_run_id` set |
| **Failure containment**  | A failed callee fails the parent's frame; bubbles to caller | A failed consumer fails only that consumer's Run; producer is unaffected |
| **Adding a consumer**    | Requires editing the caller / Worker     | Add a new agent and one `worker.wire(...)` line; no producer change |
| **Canonical Action**     | `Call` / `Reply` / `Fan`                 | `Emit` + Subscription                                |
| **When to use**          | "I need an answer from B to continue."   | "Whenever X happens, run B (and C and D) independently." |

All multi-agent patterns — both flavors — are recipes over the Action algebra. None require new wire-level Actions. At the Agent SDK tier (§4), they are declarative one-liners; at the runtime tier, they compile to `Emit`/`Call`/`Reply`/`Fan` Actions plus Subscription bindings.

---

## 11.A Orchestration patterns

In all of these, the caller explicitly names the callee. Replies flow back through the call stack.

### 11.A.1 Handoff (OpenAI Agents-style)

The Layer A surface (`triage_agent.handoffs_to(billing_agent, tech_agent)`) compiles to a `Call` to the chosen peer's topic; the peer's `Reply` flows back through the stack. See §4.11 for the Layer B sketch.

### 11.A.2 Supervisor-Worker and parallel sub-agents

Both are `Fan` over Calls to sub-agent topics. Sub-agents are exposed as tools via `Tool.from_agent(other_agent, name=...)`. When the LLM picks multiple in one turn (its native parallel-tool-call behavior), the Agent recipe emits `Fan`; the aggregator (§9) collects Replies and re-enters with `ctx.fan_results` populated. Sub-agents may recursively fan out.

```python
supervisor_agent = Agent(
    name="supervisor",
    model="openai:gpt-5.4",
    instructions="When you need multiple perspectives, query workers in parallel.",
    output_type=Summary,
    tools=[Tool.from_agent(worker_a, name="ask_a"), Tool.from_agent(worker_b, name="ask_b")],
)
```

### 11.A.3 Pipeline (sequential orchestration)

For a pure data pipeline that isn't agent-shaped (no LLM in the steps), drop to Layer B. This is orchestration because each stage explicitly names the next:

```python
@handler(topic="stage1.in")
async def stage1(ctx, req) -> Call:
    return Call(topic="stage2.in", payload=transform(req))

@handler(topic="stage2.in")
async def stage2(ctx, req) -> Call:
    return Call(topic="stage3.in", payload=transform2(req))

@handler(topic="stage3.in")
async def stage3(ctx, req) -> Reply:
    return Reply(final(req))
```

Each stage `Call`s the next; the `Reply` from stage3 unwinds back to the client. For one-way pipelines, use `Emit` instead of `Call`. Pure-data pipelines are the canonical Layer B case — no LLM, no tool catalog, so `@handler` is natural. For the same shape under choreography semantics, see §11.B.1.

### 11.A.4 Hybrid: hierarchical with handoffs and fan-outs

Combine the above — handoffs into agents that themselves fan out over `Tool.from_agent(...)` sub-agents. No new primitives.

---

## 11.B Choreography patterns

In choreography, producers don't know who consumes their events. Consumers are wired on the Worker, outside the producer's Agent declaration. The canonical Kafka pattern — exposed through three deployment-side Worker methods and one Layer B equivalent:

| Construct                                  | Layer | Purpose                                                                                   |
|--------------------------------------------|-------|-------------------------------------------------------------------------------------------|
| `worker.publish(agent, as_event=EventType)` | A     | Declares the agent's *output event stream*. After a Run terminates, the runtime emits the final payload to `agent.<name>.events`. |
| `worker.wire(source, target, payload, **opts)` | A     | Subscribe `target` (an Agent) to `source`'s event stream. Each call creates an independent Kafka subscription that triggers a new Run on each event. |
| `worker.subscribe(target, topic, payload, **opts)` | A     | Subscribe `target` (an Agent) to an arbitrary Kafka topic (not necessarily produced by a calfkit agent). |
| `Emit(topic=..., payload=...)` + `@handler(topic=...)` | B     | The bare-bones Layer B equivalent — produce events with `Emit`, consume by registering a handler on the topic. |

A `Subscription(...)` dataclass exists (§11.B.7) as a power-user value type — you can build a Subscription and pass it to `worker.wire(subscription=...)` — but the common path is `worker.wire(...)` / `worker.subscribe(...)` kwargs.

### 11.B.1 Linear choreography — B always consumes A's output stream

The simplest pattern — three agents chained classifier → enricher → scorer via `worker.publish(...)` + `worker.wire(...)`. See §4.4.B for the worked example. Each agent owns its own Run lifecycle, retry policy, observability. The producer is mute — `classifier` doesn't know `enricher` exists, so adding `scorer` is one new `worker.wire(...)` line.

At Layer B, the producer side returns `Reply` (the runtime additionally `Emit`s to the events topic when `worker.publish(...)` is set); the consumer side is a `@handler(topic="agent.X.events")` that returns `Emit` to its own events topic instead of `Reply` (no caller is awaiting). Layer A hides this asymmetry.

### 11.B.2 Pub/sub fan-out — N independent consumers of one producer

One producer, N reactive consumers, each with its own consumer group (every consumer sees every event). The shape (see §4.4.B for the worked example): one `worker.publish(producer, as_event=...)` plus N `worker.wire(source=producer, target=consumer_i, payload=...)` calls.

Each agent independently scales (`num_partitions × num_consumers`); a slow consumer does not back-pressure peers. Adding consumer N+1 is one new `Agent(...)` + one `worker.wire(...)` — zero producer changes.

### 11.B.3 Filtered subscription

Filters run *before* the LLM is invoked. Non-matching events are acknowledged and skipped without LLM cost. The filter must be pure (no I/O — the runtime calls it synchronously inside the dispatch path).

```python
alerter = Agent(name="alerter", model="openai:gpt-5.4",
    instructions="Compose and send an alert for an urgent article.",
    output_type=AlertSent)

worker.wire(
    source=watcher,
    target=alerter,
    payload=NewsArticle,
    filter=lambda ev: ev.urgency >= 4,
)
```

For more complex filters (DB lookup, external service), use `react=...` (§11.B.7) and return `Done()` to skip.

### 11.B.4 Subscribing to arbitrary Kafka topics

Calfkit does not require the producer to be a calfkit agent. Subscribe to any Kafka topic with a Pydantic-decodable payload:

```python
class TransactionCompleted(BaseModel):
    txn_id: str
    user_id: str
    amount: Decimal
    completed_at: datetime

audit_agent = Agent(name="audit", model="openai:gpt-5.4",
    instructions="...", output_type=AuditEntry)

worker.add(audit_agent)
worker.subscribe(
    audit_agent,
    topic="events.transactions.completed",
    payload=TransactionCompleted,
)
```

This is the integration path with non-calfkit services. A Java/Avro producer can drive a Python calfkit agent if the schemas align (use Mode B with Schema Registry for cross-team Avro/Protobuf contracts).

### 11.B.5 Schema-based subscription (sugar)

If the event type uniquely identifies its topic (one canonical topic per event type), subscribers can name the type instead of the topic. The SDK resolves the topic from the type:

```python
# Convention: event.<module>.<TypeName> — overridable via a class attribute
class NewsArticle(BaseModel):
    class Config:
        calf_topic = "events.news.articles"      # explicit override
    id: str
    headline: str

# Type-only subscription — the worker resolves the topic from NewsArticle
worker.subscribe(alerter, payload=NewsArticle)
```

Reasonable convention for the implicit form: `event.<module>.<TypeName>`. We don't recommend this for v1.0 because cross-package import cycles get awkward; explicit `topic=` or `source=` is clearer. The schema-only form is documented but not the default.

### 11.B.6 Deployment-time wiring as the canonical form

All choreography wiring is deployment-time. Agents are pure declarations; wiring lives on the Worker. This is the *only* form — there is no in-Agent `subscribes_to=` or `publishes=` alternative.

```python
# shared_agents.py — library, no deployment specifics
classifier = Agent(name="classifier", model="openai:gpt-5.4", instructions="...", output_type=Classified)
enricher   = Agent(name="enricher",   model="openai:gpt-5.4", instructions="...", output_type=Enriched)
archiver   = Agent(name="archiver",   model="openai:gpt-5.4", instructions="...", output_type=ArchivedRef)

# main.py — deployment composition
worker = Worker(...).add(classifier, enricher, archiver)
worker.publish(classifier, as_event=Classified)
worker.publish(enricher,   as_event=Enriched)

worker.wire(source=classifier, target=enricher, payload=Classified)
worker.wire(source=classifier, target=archiver, payload=Classified)
worker.wire(source=enricher,   target=archiver, payload=Enriched, filter=lambda e: e.flagged)

asyncio.run(worker.run())
```

This puts the choreography topology in one inspectable place rather than scattered across N agent files, and keeps each `Agent(...)` declaration portable across deployments. `worker.inspect()` prints the wired topology so users can confirm the deployment looks how they expect.

### 11.B.7 The `Subscription` declaration in full

`Subscription` is a power-user value type. The common case uses `worker.wire(...)` / `worker.subscribe(...)` kwargs directly. `Subscription` exists so users composing reusable wiring fragments can build a value, pass it around, and apply it via `worker.wire(subscription=sub)`.

```python
@dataclass
class Subscription:
    # Source — exactly one of source= or topic= (implicit: topic = source.events_topic).
    source: Agent | None = None
    topic: str | None = None

    payload: type[BaseModel]                # decode the envelope payload

    # Reaction shape (default: each event starts a new Agent Run).
    # If react= is set, the SDK calls it instead of running the LLM loop;
    # return any Action (Call/Emit/Done/Fail/etc.). Use Done() to skip.
    react: Callable[[Context, Payload], Awaitable[Action]] | None = None

    # Pure synchronous predicate; runs before react/LLM.
    filter: Callable[[Payload], bool] | None = None

    # Overrides: consumer group naming, start offset, concurrency cap.
    group_id: str | None = None
    start_from: Literal["latest", "earliest"] = "latest"   # "earliest" replays history
    max_concurrency: int | None = None

    # If True, drop the default tenant filter (see §11.B.10).
    cross_tenant: bool = False
```

Usage:

```python
# Common path — kwargs on worker.wire(...) / worker.subscribe(...)
worker.wire(source=watcher, target=alerter, payload=NewsArticle, filter=urgent_filter)

# Power-user path — Subscription value passed in
news_to_alerter = Subscription(source=watcher, payload=NewsArticle, filter=urgent_filter)
worker.wire(target=alerter, subscription=news_to_alerter)
```

The two forms are equivalent. Most users use the first; library authors composing reusable wiring may prefer the second.

### 11.B.8 Consumer-loop semantics — what does the LLM see?

When an event arrives on a subscription, three things can happen, in order of evaluation:

1. **Filter (if any) is evaluated.** Pure synchronous predicate. If False → ack, skip, emit `EventSkipped` lifecycle event. No LLM call.
2. **`react=` function (if any) is called.** Async function, returns an Action. The user can do anything — call the LLM via `ctx.llm(...)`, return `Done()` to ack-and-skip, return `Call(topic=...)` to invoke a tool or another agent, return `Emit(topic=...)` to publish a derived event. The LLM is NOT invoked unless the react function calls it.
3. **Default (no react): the agent's LLM is invoked with the event as the user-message.** The agent's `instructions`, `tools`, and `output_type` are used identically to a request-triggered run. The event payload becomes the LLM's user-role content. The final answer is published to `agent.<name>.events` (if `worker.publish(...)` was called for that agent), and the Run terminates without a `Reply`.

**Rejected alternatives**: per-subscription system prompts (bloats the model; "different prompt per source" is really a different agent); event-as-tool-result (a tool result is a response to a Call; events are unsolicited); raw-react only (pushes the common case into user code).

The chosen model: **events default to invoking the LLM like a user message; `react=` is the escape hatch.**

`ctx.trigger` exposes the origin to user code that needs it: `ctx.trigger.kind in {"request", "event", "resume"}`; for events, `ctx.trigger.source_topic`, `ctx.trigger.source_agent`, `ctx.trigger.event_type` are populated. Extensions and `on_event` observers can use this for cardinality-safe OTel attributes.

### 11.B.9 Run lineage across event boundaries

When an event from one agent triggers another agent's Run, the two Runs are *distinct* — each has its own RunId, its own runs-state record, its own lifecycle. They are linked via `parent_run_id`:

- The producer's Envelope has `run_id = X`.
- When the consumer's Worker dispatches the event, it mints a new `RunId = Y` for the consumer's Run, and sets `parent_run_id = X` on the new Run.
- Observability tooling (OTel, runs-state UI) can roll up child Runs under the parent.

**Why distinct Runs, not a continuation:** the producer's Run terminates on `Reply`/`Emit`; consumers react asynchronously (possibly never), so there is no single end-to-end Run. Fault isolation prevents a failed consumer from retroactively failing the producer; distinct Runs with parent linkage scale to N consumers cleanly. This is the **opposite** of orchestration's `Call`, where the same RunId carries through every hop — the Run-lineage choice is what makes orchestration vs choreography structurally distinct.

### 11.B.10 Multi-tenancy and cross-tenant subscriptions

By default, subscriptions are **tenant-scoped**: consumers only react to events carrying the same `x-calf-tenant-id` as the Worker. Cross-tenant events are filtered at the dispatch layer before filter/react/LLM. This safe default prevents accidental cross-tenant data flow.

**Opt-in:** `Subscription(..., cross_tenant=True)`. The agent receives events from all tenants and shards by `ctx.trigger.tenant_id` (or is explicitly tenant-agnostic, e.g. an audit agent).

### 11.B.11 Failure modes specific to choreography

| Failure                                       | Behavior                                                                                              |
|-----------------------------------------------|-------------------------------------------------------------------------------------------------------|
| Subscription handler raises                   | Retried per the Retry Extension policy. After exhaustion → DLQ (`calf.dlq.agent.<name>.sub.<source>`). The producer is unaffected. |
| Filter raises                                 | Treated as filter=False (skip) and logged. A raising filter is a user bug; emit `FilterError` lifecycle event for visibility. |
| Producer dies mid-Emit                        | At-least-once: the event publish is retried by the producer's commit-after-publish protocol (§3.5). Consumers will see the event 1+ times; dedupe by `x-calf-frame-id`. |
| Consumer dies mid-Run                         | Standard Run recovery: the consumer's offset wasn't committed, the event is redelivered, the consumer's Run restarts. Side effects must be idempotent (see §16). |
| Producer renames event topic                  | Consumers silently stop receiving until they're updated. No automatic detection. Use `worker.inspect()` in CI to detect orphaned subscriptions. (Open question §24.18.) |
| Consumer cannot decode payload (schema drift) | The event is DLQ'd, the run never starts, `PayloadDecodeFailed` lifecycle event fires. Producer is unaffected. |

The recurring theme: **choreography decouples failure domains**. A producer failure does not fail consumers; a consumer failure does not fail producers. This is a deliberate trade-off vs orchestration: orchestration gives strong end-to-end semantics ("the Run succeeded" means everything succeeded); choreography gives weak end-to-end semantics but strong fault containment.

### 11.B.12 Worked example — mixed orchestration + choreography

A realistic production topology often combines both shapes. Here, a customer-support workflow uses orchestration for the in-conversation tool/handoff dispatch and choreography for the post-conversation analytics/archival:

```python
# Authoring — pure Agent declarations, no topology
billing_agent = Agent(name="billing", model="openai:gpt-5.4", instructions="...", output_type=BillingAnswer)
tech_agent    = Agent(name="tech",    model="openai:gpt-5.4", instructions="...", output_type=TechAnswer)
triage_agent  = Agent(name="triage",  model="openai:gpt-5.4", instructions="...", output_type=str)
triage_agent.handoffs_to(billing_agent, tech_agent)            # orchestration: handoffs

analytics = Agent(name="analytics", model="openai:gpt-5.4",
    instructions="Score the quality of the support answer.", output_type=QualityScore)

archive = Agent(name="archive", model="openai:gpt-5.4",
    instructions="Persist the answer to the archive.", output_type=ArchivedRef)


# Deployment — wire the topology
worker = Worker(bootstrap_servers="kafka:9092").add(
    triage_agent, billing_agent, tech_agent, analytics, archive
)

# Orchestration is already in the Agent declarations via handoffs_to.
# Choreography lives here:
worker.publish(billing_agent, as_event=BillingAnswer)
worker.publish(tech_agent,    as_event=TechAnswer)

worker.wire(source=billing_agent, target=analytics, payload=BillingAnswer)
worker.wire(source=tech_agent,    target=analytics, payload=TechAnswer)

# Archive uses a pure react function (no LLM) — pass via the react= kwarg
worker.wire(
    source=billing_agent,
    target=archive,
    payload=BillingAnswer,
    react=lambda ctx, ev: Emit(topic="archive.s3.dump", payload=ev),
)
worker.wire(
    source=tech_agent,
    target=archive,
    payload=TechAnswer,
    react=lambda ctx, ev: Emit(topic="archive.s3.dump", payload=ev),
)
```

The customer-facing path is orchestration (triage handoff to billing/tech is synchronous request/reply over Kafka). The post-conversation analytics and archival pipelines are choreography — they react to every answer without the triage agent knowing they exist. Adding a fourth observer (a fraud-detection agent that runs on billing answers only) is one new file with one new `Agent(...)` declaration plus one `worker.wire(...)` line — zero changes to the support path.

This is the realistic mixed shape calfkit is designed for. Pure orchestration is a subset (no `worker.publish(...)` or `worker.wire(...)` / `worker.subscribe(...)` calls). Pure choreography is a subset (no `agent.handoffs_to(...)`, no sub-agent-as-tools).

---

## 12. Extension / hook system

This section supersedes `docs/hooks-design.md`.

### 12.1 Why three primitives, not a hierarchy

The previous proposal in `docs/hooks-design.md` (sections 5.2 and below) introduced `NodeMiddleware` + `AgentMiddleware` with named-sugar methods (`before_handler`/`after_handler`/`before_agent_run`/`after_agent_run`/etc.). Three structural problems:

1. **It lies about cross-process boundaries.** `before_agent_run` and `after_agent_run` fire in *different Handler invocations* — different Kafka messages, possibly different Worker processes, definitely different Python interpreter instances. A user writing one class with both methods naturally assumes Python state survives between them (`self.start_time = time.time()` in `before_agent_run`, `elapsed = time.time() - self.start_time` in `after_agent_run`), but the *second invocation* is a fresh instance in a fresh process. The API doesn't make this visible.

2. It conflates in-process boundaries (`on_tool_dispatch`) with cross-process ones (`on_tool_return`) under one mental model.

3. It has registration-order subtleties: `before_*` forward, `after_*`/`on_error` reverse — multiple rules to remember.

The v1 design rejects this. Three primitives:

```python
class Extension:
    async def around_invoke(self, req: InvokeReq, call_next) -> InvokeResp:
        """Onion-wrap each Handler invocation. Try/finally idiom for most use cases."""
        return await call_next(req)

    async def around_publish(self, req: PublishReq, call_next) -> PublishResp:
        """Onion-wrap each outbound publish (each Action effect)."""
        return await call_next(req)

    async def on_event(self, event: LifecycleEvent) -> None:
        """Typed observer. Fired in addition to around_* for lifecycle events.
        Must be idempotent — at-least-once delivery means duplicates."""
        pass
```

### 12.2 What `around_invoke` wraps

```
       --- around_invoke chain (M1 outer, M2 inner) ---
       M1.around_invoke
         M2.around_invoke
           user Handler body
         (back through M2)
       (back through M1)
```

`req.envelope`, `req.context`, `req.handler` are visible. The Extension may inspect or replace these via the `req`-recreation pattern. Common uses:
- OTel span around the Handler call.
- Retry: catch exceptions, re-invoke with attempt+1.
- Timing: wall-clock around `call_next`.
- State validation: assert input schema before the user sees it.

### 12.3 What `around_publish` wraps

```
       Handler returned Action ->
       --- around_publish chain (M1 outer, M2 inner) ---
       M1.around_publish
         M2.around_publish
           runtime publishes envelope
         (back through M2)
       (back through M1)
```

`req.action`, `req.target_topic`, `req.envelope`, `req.headers` are visible. Common uses:
- Tracing header injection (`x-calf-trace-context`).
- Output validation.
- Rate limiting outbound publishes.
- Auditing every publish to an audit topic.

### 12.4 `on_event` and `LifecycleEvent`

A discriminated union, closed at SDK level:

```python
LifecycleEvent = Union[
    # Run lifecycle
    RunStarted, RunCompleted, RunInterrupted, RunResumed, RunFailed,
    # Handler lifecycle (around_invoke covers the around case; events for observers)
    HandlerEntered, HandlerExited, HandlerFailed,
    # Action lifecycle
    ActionEmitted, PublishSucceeded, PublishFailed,
    # Tool lifecycle (sugar layered over Call/Reply when the recipe is the agent_loop)
    ToolCallStarted, ToolCallCompleted, ToolCallFailed,
    # Model lifecycle (for agent_loop)
    ModelCallStarted, ModelCallCompleted, ModelCallFailed,
    # Fan lifecycle
    FanDispatched, FanChildReceived, FanAggregated, FanTimedOut,
    # Schema / wire issues
    DeserializationFailed,
]
```

Each event has typed fields. e.g.:

```python
@dataclass(frozen=True)
class ToolCallStarted(LifecycleEvent):
    run_id: str
    frame_id: str
    tool_name: str
    tool_topic: str
    args: bytes  # opaque
    timestamp: datetime
```

The user dispatches via `isinstance` or `match`:

```python
async def on_event(self, event):
    match event:
        case ToolCallStarted(tool_name=name):
            metrics.tool_starts.labels(name).inc()
        case ToolCallCompleted(tool_name=name, latency_ms=ms):
            metrics.tool_latency.labels(name).observe(ms)
        case _:
            pass
```

The "named sugar isn't shorter in practice" point from prior feedback memory applies: three lines of `match` get the same effect as four method definitions, with the advantage of types being inspectable, shared state across cases, and the cross-process boundary visible (each `event` is independent; you can't accidentally close over Python-process state).

### 12.5 Registration model

```python
worker = (
    Worker(bootstrap_servers="kafka:9092")
    .use(
        OTelExtension(),         # outermost
        RetryExtension(max=3),
        RateLimitExtension(qps=100),
        AuditExtension(topic="audit.events"),  # innermost
    )
    .add(h1, h2, h3)
)

# Or per-handler at registration:
worker.add(h1, extensions=[H1OnlyExtension()])
```

Worker-level extensions wrap every Handler. Per-Handler extensions (passed via `worker.add(..., extensions=[...])`) wrap only that Handler, inside the Worker-level chain. (Order: worker-outer → worker-inner → handler-outer → handler-inner → user code, then back out.)

### 12.6 Composition rules

- **`around_invoke` and `around_publish`** are pure onion: M1 outer means M1's pre-logic runs first and post-logic runs last.
- **`on_event`** is fired in registration order. Each handler must be idempotent.
- **An Extension can implement any subset** of the three. Unimplemented = no-op.
- **Extension instances are constructed once per Worker.** They may hold long-lived deps (HTTP client to OTel collector, etc.) but **must not** assume Python-process state survives across Handler invocations *for the same Run*. If you need cross-invocation state for the same Run, persist via the runs-state topic or an external store.

### 12.7 Idempotency framing for `on_event`

Under at-least-once delivery, every Action may be redelivered, every Handler invocation may be retried, every `on_event` callback may fire 1+ times for the "same" logical event.

The contract is:
> `on_event` handlers MUST be idempotent. The dedup unit is `(run_id, frame_id, event_type)`.

For pure observers (metrics, structured logs that the downstream can dedupe), duplication is acceptable as noise. For non-idempotent side effects (sending an email), use the runs-state topic or `ctx.once(key)` pattern:

```python
async def on_event(self, event):
    if isinstance(event, RunCompleted):
        async with ctx.once(f"send-email:{event.run_id}") as gate:
            if gate.first_time:
                await send_completion_email(event.run_id)
```

`ctx.once` is backed by the idempotency topic.

### 12.8 Worked examples

**Tracing extension:**

```python
class OTelExtension(Extension):
    async def around_invoke(self, req, call_next):
        with tracer.start_as_current_span(
            f"handler.{req.handler_name}",
            attributes={"run_id": req.run_id, "frame_id": req.frame_id},
        ):
            try:
                return await call_next(req)
            except Exception as e:
                trace.get_current_span().record_exception(e)
                raise

    async def around_publish(self, req, call_next):
        # Inject W3C traceparent into headers
        carrier = {}
        TraceContextTextMapPropagator().inject(carrier)
        req.headers["x-calf-trace-context"] = carrier.get("traceparent", "")
        return await call_next(req)
```

**Retry extension:**

```python
class RetryExtension(Extension):
    def __init__(self, max_attempts: int = 3): self.max = max_attempts

    async def around_invoke(self, req, call_next):
        attempt = int(req.headers.get("x-calf-attempt", "0"))
        try:
            return await call_next(req)
        except RetryableError as e:
            if attempt < self.max:
                # Re-publish self with attempt+1, backoff via headers
                req.requeue(attempt=attempt + 1, delay=backoff(attempt))
            else:
                req.dlq(reason=str(e))
            raise
```

**Rate limiting** (see §4.6) and **audit** (`on_event` + idempotent publish to an audit topic) follow the same shape as the Tracing extension. **Tool filtering** (the LangGraph `wrap_tool_call` equivalent) inspects `req.target_topic` in `around_publish` and raises `ToolNotAllowed` if the topic doesn't match an allowlist.

### 12.9 Testing extensions

```python
async def test_rate_limit_blocks_overage():
    wrk = (
        InMemoryWorker(group_id_prefix="test")
        .use(RateLimit({"my.in": 1.0}))
        .add(my_handler)
    )
    # First call: OK
    await wrk.invoke("my.in", Req())
    # Second within the second: rejected
    with pytest.raises(RateLimitExceeded):
        await wrk.invoke("my.in", Req())
```

`InMemoryWorker` exercises the full Extension chain — no Kafka required. This is by design (see §17).

---

## 13. Deps injection

### 13.1 Where Deps live in the type system

```python
class TradingDeps(BaseModel):
    db: AsyncSession
    coinbase: CoinbaseClient
    feature_flags: dict[str, bool]

@handler(topic="trade.in", reply=str, deps=TradingDeps)
async def trade(ctx: Context[TradingDeps], req: TradeReq) -> Reply[str]:
    quote = await ctx.deps.coinbase.get_quote(req.symbol)
    ...
```

`Context` is parameterized on a single type parameter, `Deps`, matching Pydantic AI's `RunContext[Deps]` and OpenAI Agents' `RunContextWrapper[T]`. State surfaces as `ctx.state` typed via inference from the Handler's signature (or as `Any` if not declared); it does not appear as a second generic parameter on `Context`. The Worker is configured with the actual deps via the `deps_from(factory)` builder method:

```python
worker = (
    Worker(bootstrap_servers="kafka:9092")
    .deps_from(create_deps)
    .add(trade)
)

def create_deps() -> TradingDeps:
    return TradingDeps(
        db=session_factory(),
        coinbase=CoinbaseClient(key=os.getenv("CB_KEY")),
        feature_flags=load_flags(),
    )
```

For Agents, the `Deps` type is declared on the constructor:

```python
trading_agent = Agent(
    name="trader",
    model="openai:gpt-5.4",
    instructions="...",
    output_type=TradeResult,
    deps_type=TradingDeps,   # IDE/mypy will type-check ctx.deps inside @tool callbacks the agent uses
)
```

### 13.2 Lifecycle

- Deps are **instantiated at Worker startup** (synchronous or async via `worker.deps_from(factory)`).
- Deps are **scoped to the Worker process**, not per-Handler-invocation. (Per-invocation deps are an anti-pattern — they don't survive replays.)
- For per-invocation context (current Run's tenant, current user_id), use the State, not Deps.

```python
worker = (
    Worker(bootstrap_servers="kafka:9092")
    .deps_from(lambda: TradingDeps(db=await create_pool()))
    .add(...)
)
```

### 13.3 Testing deps swap

```python
async def test_trade_with_mock_coinbase():
    wrk = (
        InMemoryWorker(group_id_prefix="test")
        .deps_from(lambda: TradingDeps(
            db=mock_db(),
            coinbase=MockCoinbase(),
            feature_flags={},
        ))
        .add(trade)
    )
    result = await wrk.invoke("trade.in", TradeReq(...))
    assert result.output == "..."
```

Deps swap is trivial because the Handler signature is generic on `Deps`. The Worker just passes whichever Deps the configured factory produced. For tests where you need the simpler shape, `InMemoryWorker.testing(trade, deps=mock_deps)` is the shortcut (the `testing` factory accepts an optional `deps=` kwarg as a deps_factory shortcut).

### 13.4 What about secrets?

Deps may carry secrets (API keys, DB passwords). Secrets MUST NOT be serialized into envelopes. The Context exposes `ctx.deps` (live Python object) and `ctx.state` (serialized bytes); the wire format never includes Deps. This is structurally enforced by the wire format definition (§6) which has no Deps field.

---

## 14. Observability

### 14.1 OTel-native by default

The SDK ships with an `OTelExtension` registered by default unless explicitly disabled. It exports:

- **Spans** for each Handler invocation, each Action effect (publish), each tool call, each model call.
- **Metrics** for hop counts, publish latencies, retry counts, fan-out aggregator latencies, DLQ counts.
- **Logs** correlated to spans via `trace_id` and `span_id`.

### 14.2 Span hierarchy

```
client.invoke (span: client.invoke, attributes: run_id, target_topic)
  publish.call (span: action.call)
    handler.dispatch (span: handler.<name>, attributes: run_id, frame_id, attempt)
      around_invoke chain
      user_handler_body
      around_publish chain (for each Action emission)
        publish.call / publish.reply / publish.emit (span)
      handler.return
    handler.dispatch (next Handler in chain)
      ...
```

Cross-hop propagation via the `x-calf-trace-context` header (W3C `traceparent`). The OTelExtension injects on publish and reads on receive.

### 14.3 Cardinality discipline

Metric labels (Prometheus / OTel) MUST come from a closed set:
- `handler_name` — closed at Worker startup
- `action_kind` — closed enum (`call`, `reply`, `emit`, `fan_child`, `tailcall`)
- `tool_name` — closed at Worker startup (tools registered at boot)
- `tenant_id` — *bounded but not closed*; warn at 1000+ distinct values; consider a sample/hash strategy

Labels NEVER include: `run_id`, `frame_id`, `correlation_id`, `user_id` — these are unbounded. Use them as span attributes, not metric labels.

### 14.4 Standard attributes and naming

| Attribute              | Semantic |
|------------------------|----------|
| `calfkit.run_id`       | The Run aggregate |
| `calfkit.frame_id`     | Current frame |
| `calfkit.correlation_id` | Per-hop |
| `calfkit.handler_name` | Handler this invocation belongs to |
| `calfkit.action_kind`  | Action enum value |
| `calfkit.tenant_id`    | If multi-tenant |
| `calfkit.attempt`      | Retry count |
| `calfkit.tool_name`    | For tool spans |
| `calfkit.model.provider` | "openai", "anthropic", etc. |
| `calfkit.model.name`   | "gpt-5.4", "claude-opus-4.7", etc. |
| `calfkit.fan_id`       | For Fan-related spans |
| `calfkit.fan_size`     | `fan_total` |

### 14.5 User-defined Extensions adding telemetry

```python
class MyExtension(Extension):
    async def around_invoke(self, req, call_next):
        span = trace.get_current_span()  # already started by OTelExtension
        span.set_attribute("myapp.user_id", req.context.state.user_id)
        return await call_next(req)
```

Extensions can enrich the active span; they shouldn't create new top-level spans (use child spans if needed).

### 14.6 Per-Run cost tracking

The `LifecycleEvent` for model calls includes token usage:

```python
@dataclass(frozen=True)
class ModelCallCompleted(LifecycleEvent):
    run_id: str
    model_name: str
    input_tokens: int
    output_tokens: int
    cost_estimate_usd: float | None
    latency_ms: int
```

A `CostExtension` aggregates per-Run by listening to these events and publishing to a per-Run cost record on `calf.runs-cost` (compacted, key=`run_id`). Querying that topic at any time gives the running cost for any Run.

This is a recipe, not a built-in. The SDK provides the event; the user (or a community extension) provides the aggregation.

---

## 15. Error handling

### 15.1 Failure modes enumerated

| Mode | Where | Detection | Default action |
|---|---|---|---|
| Handler raises | User code in `run` | `try` in dispatcher | RetryExtension (if registered) or DLQ |
| Tool timeout | Caller waits past `Call.timeout` | Timer in runtime | `Fail` Action on caller re-entry |
| Publish fails | aiokafka send rejected | `KafkaError` | Re-attempt (idempotent producer); if persistent, retry queue then DLQ |
| Deserialization fails | Inbound envelope or payload | Pydantic / Avro decode error | DLQ; emit `DeserializationFailed` event |
| Model API fails (5xx, timeout, rate limit) | Inside agent_loop recipe | LLM client raises | Subject to RetryExtension on the agent's Handler |
| Fan-out partial | Aggregator timeout | Aggregator runtime | Per `AggregatorSpec.on_partial`: error/use_partial/continue |
| Worker crash mid-aggregator | OS-level | Kafka rebalance | New owner replays from un-committed offset |
| Worker crash mid-Action publish | OS-level | Inbound offset not committed | Re-deliver; idempotency table dedupes the re-publish |
| Schema mismatch (`schema_version` rejected) | Inbound envelope | SDK checks `Envelope.schema_version` | DLQ; emit `DeserializationFailed` |
| Tenant header missing on a tenant-required topic | Inbound | Tenant Extension (opt-in) | DLQ; emit `TenancyViolation` event |

### 15.2 Retry policy

The Retry Extension owns retry. It listens for handler exceptions (via `around_invoke`) and:
- If `exception is RetryableError` and `attempt < max`: re-publish self with `attempt+1`, optional delay.
- Else: DLQ + `Fail` Action.

Why retry is an Extension, not built-in:
- Different Handlers want different policies (a tool with side effects wants conservative retry; a pure classification call wants aggressive retry).
- Retry policy intersects with cost tracking, rate limiting, circuit breakers — keeping it composable in one place (Extensions) keeps the runtime small.

Default RetryExtension shipped by SDK with sensible defaults; users override.

### 15.3 Circuit breakers

Not in v1.0. A future Extension. Pattern would be:
- Track failure rate per `target_topic` in a sliding window.
- If failure rate > threshold, `around_publish` short-circuits with a `CircuitOpen` error.
- Half-open after cooldown.

### 15.4 DLQ shape

```
Topic: calf.dlq.<source-topic-name>
Key:   run_id
Value: DLQRecord
```

```python
class DLQRecord(BaseModel):
    original_envelope: Envelope
    original_headers: dict[str, str]
    failure_reason: Literal["handler_exception", "deserialization", "timeout", "schema_mismatch", "tenant_violation", "tool_unreachable"]
    error_message: str
    error_type: str
    stack_trace: str | None
    attempt_count: int
    first_seen: datetime
    last_seen: datetime
```

Per-source-topic DLQ (not one shared) chosen because operators want to inspect "what failed for tool X" without filtering. Trade-off is more topics; on a modern Kafka cluster with KRaft this is fine (we've seen 10k+ topics in production at peers).

### 15.5 Partial-failure semantics in fan-out

The `AggregatorSpec.on_partial` enum encodes user intent:

- `"error"`: aggregator emits `FanTimedOut` event + the parent re-entry has `ctx.fan_results = None`, `ctx.fan_error = FanTimeoutError(collected_indices=[...])`. The user's Handler decides whether to recover or `Fail`.
- `"use_partial"`: parent re-entry has `ctx.fan_results = {only_what_was_collected}`. User decides.
- `"continue"`: parent re-entry has `ctx.fan_results = None` regardless. User can poll the aggregator topic via `ctx.deps.fanout_view(fan_id)` for live data, or use it as a fire-and-forget signal.

### 15.6 Exception hierarchy

All SDK-defined exceptions live under `calfkit.errors` with a clear inheritance tree:

```
calfkit.errors.CalfkitError              # base
    .ConfigError                          # bad worker config
    .TransportError                       # Kafka unavailable
    .EnvelopeDecodeError                  # bad envelope
    .HandlerError                         # base for handler-side errors
        .RetryableError                   # explicit "retry me"
        .ToolError                        # tool failure
            .ToolTimeoutError
            .ToolNotAllowedError          # used by ToolFilter Extension
        .ModelRetry                       # output validation retry signal
    .RunError                             # run-level
        .RunFailedError
        .RunInterruptedError
        .RunNotFoundError
    .ChoreographyError                    # subscription-level
        .OrphanedSubscriptionError
    .FanTimeoutError                      # aggregator timeout (used in ctx.fan_error)
    .CircuitOpen                          # circuit breaker (v1.x)
    .RateLimitExceeded                    # rate limiter
```

User code should catch `RetryableError` for explicit retry signals; transport, runtime, and SDK-internal errors share `CalfkitError` so a single `except CalfkitError` catches them all. Exception chaining (`raise ... from e`) is used throughout — the original cause is always preserved.

---

## 16. Idempotency contract

### 16.1 What the SDK guarantees

- Each Action emission is deduped by `(run_id, action_id)` in the idempotency topic.
- Each Reply is deduped by `(run_id, frame_id)`.
- Each Fan re-entry to the parent is deduped by `(run_id, fan_id)`.
- Each Interrupt checkpoint is deduped by `(run_id, interrupt_id)`.
- The idempotency topic (`calf.idempotency`) is compacted, retention 24h.

### 16.2 What the user must do

For non-idempotent side effects in user code, use `ctx.once(key)`:

```python
async with ctx.once(f"charge-card:{order_id}") as gate:
    if gate.first_time:
        await stripe.charge(order_id, ...)
```

`ctx.once` writes to the idempotency topic with key=`f"{run_id}:{user_key}"`. If a duplicate is seen, `gate.first_time = False` and the user skips the side effect.

This is the explicit escape hatch for "I have a side effect that must run exactly once across replays."

### 16.3 Dedup keys

| Action / Surface | Dedup key |
|---|---|
| `Emit` (a publish)         | `(run_id, action_id)` |
| `Call` (target's receipt)  | `(run_id, frame_id)` |
| `Reply` (parent's receipt) | `(run_id, frame_id)` |
| `Fan` (fan dispatch)       | `(run_id, fan_id)` |
| `Fan` aggregator re-entry  | `(run_id, fan_id)` |
| `Interrupt` checkpoint     | `(run_id, interrupt_id)` |
| `Resume`                   | `(run_id, interrupt_id)` |
| User code side effect      | `(run_id, user_supplied_key)` via `ctx.once` |

### 16.4 Implementation of the idempotency topic

```
Topic: calf.idempotency
Key:   "{run_id}:{action_id}" or other dedup key (UTF-8 string)
Value: 1-byte marker + timestamp (small footprint)
Cleanup: compact + delete.retention.ms = 24h (configurable)
```

Each Worker maintains an LRU cache of recently-seen keys in memory (per-partition) and on cache miss consults the topic. The topic is the source of truth; the in-memory cache is a hot path.

A user-tunable knob: for very-high-throughput topics, the dedup can be turned off (`Worker(dedup="off")` per-handler). Default is on.

---

## 17. Testing strategy

Three layers. Each tests something the other can't.

### 17.1 Layer 1 — Pure Handler unit tests

Test a Handler in pure isolation — no Kafka, no broker, no Extensions, no runtime. Appropriate for Layer B handlers. For Agent SDK code, prefer L2 (the agent's behavior depends on the LLM loop recipe, which lives in the runtime).

```python
async def test_classifier_handler_pure():
    ctx = Context(
        run_id="r1",
        deps=MyDeps(...),
        state=MyState(...),
    )
    action = await classifier_handler(ctx, ClassifyReq(text="hi"))
    assert isinstance(action, Reply)
    assert action.payload.label == "greeting"
```

**What it tests:** the function shape. The user's logic.
**What it doesn't:** anything about Kafka, the Extension chain, retries, the aggregator. This is just a pure function call.

### 17.2 Layer 2 — InMemoryWorker (in-process queue transport)

`InMemoryWorker` is the canonical local-dev / REPL / test idiom and the substitute for `agent.run(input)` (G13). It is the **same `Worker` runtime class** as production, with the Kafka transport adapter (`KafkaTransport`) replaced by an in-process queue (`MemoryTransport`). The full Action interpreter, Extension chain, fan-out aggregator, and idempotency table run unchanged — the in-memory path is a real subset of production behavior, not a parallel implementation.

```python
from calfkit import InMemoryWorker

async def test_weather_agent():
    wrk = InMemoryWorker.testing(weather_agent)
    result = await wrk.invoke(weather_agent, "What's the weather in Berlin?")
    assert result.output.location == "Berlin"
    assert wrk.assert_published("tool.get_weather", count=1)
```

For multi-agent integration:

```python
async def test_multi_agent_pipeline_in_memory():
    wrk = (
        InMemoryWorker(group_id_prefix="test")
        .use(OTelExtension(), RetryExtension(max=2))
        .add(triage_agent, billing_agent, tech_agent)
    )
    result = await wrk.invoke(triage_agent, UserReq(text="my bill is wrong"))
    assert result.output.startswith("I'll help you")
    assert wrk.assert_published("agent.billing-agent.in", count=1)
    assert wrk.assert_published("agent.tech-agent.in", count=0)
```

**The InMemoryWorker surface (mirrors `Worker` exactly plus three test-helper methods):**

| Method | Purpose |
|---|---|
| `InMemoryWorker.testing(*agents)` | Ergonomic factory: registers agents, no run loop required. |
| `InMemoryWorker(...)` | Full builder matching `Worker(...)`; for Extensions, deps, custom group_id. |
| `wrk.invoke(agent_or_ref_or_topic, payload)` | Synchronous request/reply via the in-process queue. Matches `client.invoke(...)`. |
| `wrk.emit(topic, payload)` | Publish a choreography event so subscribers fire. |
| `wrk.assert_published(topic, count=None)` | Count envelopes landed on a topic during the test. |
| `wrk.published_envelopes(topic=None)` | Return published envelopes (useful for snapshot testing). |
| `wrk.runs_for_agent(name)` | Count Runs the named agent processed. |
| `wrk.drain()` | Wait for all triggered Runs to complete. |

**Choreography testing pattern** — assert that a published event triggers a subscribed Agent:

```python
async def test_pubsub_fanout_in_memory():
    wrk = (
        InMemoryWorker(group_id_prefix="test")
        .add(watcher, summarizer, sentiment, archiver)
    )
    wrk.publish(watcher, as_event=NewsArticle)
    wrk.wire(source=watcher, target=summarizer, payload=NewsArticle)
    wrk.wire(source=watcher, target=sentiment,  payload=NewsArticle)
    wrk.wire(source=watcher, target=archiver,   payload=NewsArticle)

    article = NewsArticle(id="1", headline="Test", body="...", urgency=4)
    await wrk.emit("agent.news-watcher.events", article)
    await wrk.drain()

    assert wrk.runs_for_agent("summarizer") == 1
    assert wrk.runs_for_agent("sentiment") == 1
    assert wrk.runs_for_agent("archiver") == 1
    # Each consumer saw the event independently (pub/sub fan-out).
```

**What it tests:** multi-handler integration. Extension chain composition. Fan-out aggregation. Idempotency. Run lifecycle events. Most assertions about wiring.

**What it doesn't:** real Kafka behavior — partition rebalances, broker downtime, consumer-group dynamics, real-network latency, real serialization (it uses Python objects directly by default).

`MemoryTransport.force_serialize=True` opts into bytes round-trip, catching serialization bugs without a broker.

### 17.3 Layer 3 — Real Kafka via testcontainers

```python
@pytest.fixture
async def kafka():
    with KafkaContainer() as k:
        yield k

async def test_rebalance_during_fanout(kafka):
    # Start Worker A, run a Fan, while Replies are arriving, kill A.
    # Verify Worker B takes over and the parent re-entry still happens.
    ...
```

**What it tests:** broker behavior. Rebalance. Process crashes. Real wire format. Schema Registry. The aggregator's restart-recovery story (§9.6).
**What it doesn't:** UI flows, long-haul deployment behavior.

### 17.4 Why three layers, not two

The 0.x test pattern (`TestKafkaBroker` for FastStream) is one layer. It conflates "unit-ish" with "integration-ish" — fast but not the same as production. v1 separates:

- **L1** for "did I write this Handler correctly?" — sub-millisecond. Layer B authors mostly.
- **L2** for "do my agents, tools, Extensions, and Actions compose?" — single-digit milliseconds. **This is where most Agent SDK tests live.**
- **L3** for "does this survive Kafka shenanigans?" — testcontainers, multi-second.

CI runs all three. Pre-commit runs L1+L2. Any drift between `InMemoryWorker` and `Worker` is a defect, not a design choice.

---

## 18. Deployment / runner

### 18.1 What a calfkit Worker process looks like

```python
# worker_main.py
import asyncio
import os
from calfkit import Worker
from calfkit.extensions import OTelExtension, RetryExtension
from myapp.agents import triage, billing, tech, analytics, archive
from myapp.deps import create_deps

async def main() -> None:
    worker = (
        Worker(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"],
            group_id_prefix="myapp-prod",
        )
        .use(
            OTelExtension(endpoint=os.environ["OTEL_ENDPOINT"]),
            RetryExtension(max=3),
        )
        .deps_from(create_deps)
        .add(triage, billing, tech, analytics, archive)
    )

    # Choreography topology
    worker.publish(billing, as_event=BillingAnswer)
    worker.publish(tech,    as_event=TechAnswer)
    worker.wire(source=billing, target=analytics, payload=BillingAnswer)
    worker.wire(source=tech,    target=analytics, payload=TechAnswer)

    # Internal-topic overrides — almost never needed
    # worker.runtime(
    #     runs_state_topic="calf.runs-state",
    #     fanout_topic="calf.fanout-agg",
    #     dlq_topic_pattern="calf.dlq.{source}",
    #     dedup="on",
    #     partition_strategy="run_id",
    # )

    await worker.run()  # blocks until SIGTERM

if __name__ == "__main__":
    asyncio.run(main())
```

Constructor takes two kwargs (transport identity and group prefix). Everything else attaches via methods. The runtime-overrides trio that 99% of users never touch (`runs_state_topic`, `fanout_topic`, `dlq_topic_pattern`) lives behind `worker.runtime(...)`, discoverable but out of the way.

### 18.2 Configuration

Three layers, in precedence order:
1. **Programmatic** (`Worker(...)` constructor + method calls)
2. **Environment variables** (`CALFKIT_KAFKA_BOOTSTRAP`, `CALFKIT_OTEL_ENDPOINT`, etc.; the env-var prefix is `CALFKIT_`)
3. **Config file** (`calfkit.yaml`, opt-in via `CALFKIT_CONFIG_FILE`)

The SDK ships a `WorkerSettings(BaseSettings)` Pydantic model (`pydantic_settings.BaseSettings`) that is the source-of-truth schema; env vars and config file map to it. Programmatic args override.

### 18.3 Horizontal scaling

- Run N Worker processes with the same Handlers, same `group_id_prefix`.
- Each Handler's group is `f"{group_id_prefix}.{handler.name}"`.
- Kafka assigns partitions to Worker instances; parallelism = min(N_workers, N_partitions_on_topic).
- For higher parallelism than Workers, increase topic partition count.

For per-Handler concurrency within a single Worker, the SDK uses `max_concurrent_handlers` (default = 16 = aiokafka's reasonable default). Higher = more in-flight Handler invocations per partition.

### 18.4 Container image story

The SDK does not bundle a Dockerfile (Docker is the user's deployment choice, not the SDK's). The recommended pattern is documented:

```dockerfile
FROM python:3.13-slim
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen
COPY . .
CMD ["uv", "run", "python", "worker_main.py"]
```

Health check endpoint optional via `worker.health_check(port=8080)` (a builder method, on by `worker.runtime(...)` for advanced overrides). By default, the Worker has no HTTP surface — it's pure Kafka.

### 18.5 Operational concerns

- **Graceful shutdown**: on SIGTERM, stop consuming, drain in-flight Handlers, finalize aggregator state, commit offsets, close consumer + producer. Configurable timeout (default 30s); after that, force-quit.
- **Draining**: a user calls `await worker.drain()` to stop consuming new messages but finish in-flight; useful for blue-green deploys.
- **Signal handling**: SIGTERM = drain + shutdown. SIGINT = same. SIGKILL = OS forces termination, Kafka rebalance picks up.
- **Liveness vs readiness**: by default, no HTTP probes. With `health_check_port` set, GET `/healthz` (liveness, always 200 once started) and GET `/readyz` (readiness, 200 once consumer group is joined).

---

## 19. Multi-tenancy

### 19.1 Tenant-as-header default

```python
await client.execute(
    "trading.agent.in",
    req,
    tenant_id="tenant-acme",
)
```

The client sets `x-calf-tenant-id` on the outbound publish. The runtime copies it through every hop. Handlers read via `ctx.tenant_id` (helper) or directly from the inbound headers.

Why headers, not envelope body:
- Visibility at broker level (kafka-console-consumer can filter by tenant).
- Connect SMTs and MirrorMaker can route by tenant header.
- Consistent with existing `x-calf-emitter` pattern at `_protocol.py`.
- The prior memory `project_kafka_headers_design_decision.md` enshrined this.

### 19.2 Topic-per-tenant (opt-in)

For tenants needing strict isolation:

```python
@handler(topic_template="trading.agent.in.{tenant_id}")
async def trading(ctx, req): ...
```

The SDK creates a separate topic per tenant. Each topic gets its own consumer group. Useful for strict resource isolation but expensive in topic count.

Trade-off: a Worker subscribes via wildcard (`trading.agent.in.*`) and Kafka feeds it everything. Or a Worker is dedicated per tenant. The SDK supports both; recommendation in docs is to start with header-based and migrate to topic-per-tenant only when isolation requirements demand it.

### 19.3 Quotas and isolation

Not implemented in the SDK. Quotas live in:
- Kafka cluster (broker-side quotas per client.id).
- Topic-per-tenant + topic-level quotas.
- The application layer (the Handler checks `ctx.tenant_id`).

The SDK provides `TenancyExtension` (opt-in) that enforces:
- Every inbound has a non-empty `x-calf-tenant-id`.
- Every outbound carries it forward.
- Optional allow-list of valid tenants.

### 19.4 What's NOT solved

The SDK does *not* enforce that Handler code respects tenant boundaries. If a Handler queries the DB without filtering by tenant_id, that's a bug in user code, not a hole in the SDK. This is the classic multi-tenant gotcha and we don't pretend to fix it at the framework level.

---

## 20. Direct aiokafka vs FastStream

### 20.1 The case for direct aiokafka

The 0.x code at `calfkit/worker/worker.py:5` uses FastStream. FastStream is well-designed for HTTP-shaped consumer/producer patterns. It is **not designed for**:

1. **Custom rebalance listeners.** The fan-out aggregator needs to know "you're about to lose this partition" so it can finalize state before release. FastStream's broker abstraction does not expose `ConsumerRebalanceListener` cleanly.

2. **Strict header typing.** FastStream's parser at `kafka/parser.py:41` decodes headers to `dict[str, Any]` (per project memory). The runtime needs typed access; we re-decode at every consumer site (`_protocol.py:28-39`'s `decode_header_str`). Direct aiokafka lets us own the codec.

3. **The Action algebra.** FastStream's `@subscriber` + return-value-publishes-to-`@publisher`-decorator pattern is incompatible with the multi-Action-per-Handler model. The current code at `worker.py:51-53` wires it manually, and the result is fragile (the FastStream `Response(body, headers=...)` pattern only naturally supports a single outbound publish).

4. **The aggregator subscription.** The aggregator needs to subscribe to its own topic with specific offset-commit discipline (don't commit until aggregation is complete). FastStream's auto-commit and exactly-once paths don't fit cleanly.

5. **Multi-broker support we don't use.** FastStream supports Kafka, RabbitMQ, NATS, Redis, etc. Calfkit only targets Kafka. The abstraction tax is real (every FastStream upgrade is a multi-broker compatibility matrix we don't need).

### 20.2 What we lose

- **`TestKafkaBroker`**: FastStream's in-memory test broker. We replace with `MemoryTransport` (§17.2) which we own.
- **The FastStream `@subscriber`/`@publisher` decorator pattern**: replaced with our `@handler` decorator that returns Actions.
- **The FastStream router DSL**: not used in 0.x meaningfully (we don't use `KafkaRouter` for cross-router composition). No loss.

### 20.3 What we gain

- Control over the rebalance lifecycle.
- Direct header typing.
- A single-purpose Action interpreter that doesn't have to fit inside FastStream's pipeline.
- One fewer dependency, faster cold start.
- No need to hold FastStream's API stable in our docs.

### 20.4 Migration story

There is no migration story. v1 is a clean break. The 0.x code stays on 0.x for users who don't want to upgrade. v1 ships under a new major version (1.0). Users who upgrade rewrite their handlers; the doc ships migration recipes:

- `BaseAgentNodeDef` subclass → `@handler` + agent_loop recipe
- `BaseNodeDef` subclass → `@handler`
- `agent_tool` → `@tool` (module-level), `Tool.external(...)` (cross-language), or `Tool.from_agent(...)` (sub-agent as tool)
- `Worker(client, nodes=[...])` → `Worker(bootstrap_servers=..., handlers=[...])`
- `Client.execute_node(...)` → `Client.execute(topic, req)`

The migration is non-trivial but mechanical. There is no automatic codemod; we provide a side-by-side recipes doc.

---

## 21. What's discarded from current calfkit

A precise list. Each entry includes the file:line and the v1 replacement.

| Discarded | Location | Replacement |
|---|---|---|
| `BaseNodeDef` class hierarchy | `calfkit/nodes/base.py:42` | `@handler` function decorator |
| `BaseAgentNodeDef` god-method | `calfkit/nodes/agent.py:27` | `agent_loop` recipe over `@handler` |
| `ToolNodeDef` | `calfkit/nodes/tool.py:26` | Unified `Tool` class — `@tool`, `Tool.external(...)`, `Tool.from_agent(...)` (§10.1) |
| `NodeDef` (empty stub) | `calfkit/nodes/node.py:7` | Removed; not needed |
| `_pending_batches: dict[str, PendingToolBatch]` | `agent.py:50` | Compacted topic + runtime aggregator (§9) |
| `_parallel_state_aggregation()` | `agent.py:61` | Runtime-owned, not Handler-owned |
| The `RuntimeError("This indicates lost PendingToolBatch state")` | `agent.py:116-120` | Aggregator durability eliminates the case |
| `NodeResult = Union[Silent, Call, list[Call], ReturnCall, TailCall]` | `models/actions.py:117` | New Action algebra (§3) |
| `Delegate`, `Sequential`, `Parallel`, `Reply` (the 0.x dataclass) | `models/actions.py:19-107` | Either absorbed into the algebra or expressed as recipes |
| `Silent` | `models/actions.py:110` | `Done` (terminal, explicit) |
| `Envelope` with inline `SessionRunContext` | `models/envelope.py:9` | New `Envelope` with opaque state bytes (§6.1) |
| `State` god-model | `models/state.py:92` | User-defined `StateT` (opaque to SDK) |
| `CompactBaseModel` patterns and footguns | (User memory) | Sidestepped by user-owned serialization |
| `SessionRunContext` + `WorkflowState` mixing | `models/session_context.py:81,45` | Separate `Context`, `Run`, `Frame` concepts (§7) |
| `Worker(client, nodes=[...])` requiring a Client | `worker/worker.py:13` | `Worker(bootstrap_servers=...)` standalone |
| FastStream coupling | `worker/worker.py:5` | Direct aiokafka (§20) |
| `_protocol.py` `HDR_EMITTER`/`HDR_EMITTER_KIND` only | `_protocol.py:21-24` | Expanded reserved header set (§6.2) |
| Two-layer middleware proposal (`NodeMiddleware`/`AgentMiddleware`) | `docs/hooks-design.md` | Three-primitive `Extension` (§12) |
| `gates` as separate concept | `nodes/base.py:85` | `around_invoke` short-circuit (gates become a one-line Extension recipe) |
| `decode_header_str` | `_protocol.py:28` | Internal helper inside header codec module (still needed; the algorithm survives) |
| `_emitter_headers()` returning fixed dict | `nodes/base.py:161` | Comprehensive header set emitted by runtime |
| `_publish_action()` dispatch | `nodes/base.py:164` | Runtime's Action interpreter (§3.5) |
| `tools_topic_registry` ambiguity (per user memory: returns *first* private topic) | (Project memory) | Each Handler has exactly one topic; no ambiguity |
| `ToolContext(RunContext[Deps])` from vendored pydantic_ai | `models/tool_context.py:8` | `Context[Deps]` SDK-owned (single generic param), no vendored dependency |
| Vendored `pydantic_ai` subset | `calfkit/_vendor/pydantic_ai/` | Either un-vendor (depend on upstream) or shrink to a tiny interface |

Two notes:

1. **Gates as Extension recipe**: today `gates` predicates short-circuit before `run()`. In v1, this is one Extension:
   ```python
   class GateExtension(Extension):
       def __init__(self, gates: list[Predicate]): self.gates = gates
       async def around_invoke(self, req, call_next):
           for g in self.gates:
               if not await g(req.context):
                   return InvokeResp(action=Done())
           return await call_next(req)
   ```
   No new concept needed.

2. **Vendored pydantic_ai**: out of scope to fully rip out in v1.0; the agent_loop recipe is implemented by either continuing to vendor or by writing a minimal LLM-tool-call abstraction. Recommendation: in v1.0, depend on upstream `pydantic_ai` (un-vendor) but expose only the tool-call schema synthesis; in v1.1, consider an SDK-native model abstraction.

---

## 22. Comparison table

| Dimension                | Calfkit v1.0                | LangGraph                    | Temporal                       | Restate                          | OpenAI Agents SDK         | Pydantic AI               | Inngest                       |
|--------------------------|------------------------------|------------------------------|---------------------------------|-----------------------------------|----------------------------|----------------------------|--------------------------------|
| **Primary authoring surface** | `Agent(...)` + `handoffs_to`; tools at module scope (`@tool` / `Tool.external` / `Tool.from_agent`). Pydantic-AI-shaped; tools are Kafka services, not agent-owned. | Graph DSL (`add_node`/`add_edge`) | `@workflow.defn` + `execute_activity` (deterministic) | Service handlers (`@restate.service`) | `Agent(...)` in-process | `Agent(...)` in-process | `step.run(...)` |
| **Execution shape**      | Distributed, one Node per Kafka hop | In-process graph | Workflow + Activity (distributed via SDK) | Distributed handlers (durable invocation) | In-process agent | In-process agent | Function steps (cloud or self-host) |
| **Durable execution**    | Yes (Kafka log + compacted topics) | Optional Postgres checkpointer | Yes (Temporal cluster) | Yes (Restate runtime) | No | No | Yes (Inngest store) |
| **Required runtime infra** | Kafka only | None (or Postgres) | Temporal cluster | Restate runtime | None | None | Inngest service (or self-host) |
| **Graph DSL**            | No (topology emergent)       | Yes (`add_node` / `add_edge`)| No (workflow code is plain) | No (handler code is plain) | No (orchestrator code) | No | No (`step.run` etc.) |
| **Hook surface**         | 3 primitives (`around_invoke`, `around_publish`, `on_event`) | `before_*`/`after_*`/`wrap_*` middleware | Interceptors (4 axes) | Middleware | `RunHooks`/`AgentHooks` | `@tool(prepare=...)`, `system_prompt` | Middleware |
| **Tool model**           | First-class Kafka topic; cross-language native | In-process Python function | Activity (cross-language via SDK) | Service handler | In-process Python | In-process Python | In-process |
| **Cross-language tools** | Yes, by construction         | No                            | Yes via SDKs                   | Yes via SDKs                     | No                         | No                         | TS-focused; some other-lang   |
| **Multi-tenancy**        | Header default, topic opt-in | App-level                    | App-level + namespaces          | App-level                        | App-level                  | App-level                  | App-level                     |
| **Orchestration**        | `handoffs_to(...)` + `Tool.from_agent(...)` | `add_edge`/`add_node` | `execute_activity` | Handler calls | Handoffs | Delegate | `step.invoke` |
| **Choreography (pub/sub agents)** | **First-class** (§11.B) | App-level | App-level (point-to-point signals) | App-level | App-level | App-level | App-level (events, not agent streams) |
| **Type safety**          | Pydantic generics (`Context[Deps]`, `Agent[Deps, Output]`) | TypedDicts | Per-SDK | Per-SDK | Pydantic | Pydantic | TS |
| **Determinism constraint** | None | None | Strict (deterministic workflows) | None | None | None | Idempotent steps |
| **HITL**                 | Native — `@tool(requires_approval=True)`, `@agent.interrupt_when(...)`, `Interrupt`/`Resume` | Interrupts, breakpoints | Signal, query | Awakeable | Limited | Limited | `step.waitForEvent` |
| **Deployment**           | Worker = Kafka consumer process | Run anywhere                  | Worker + Temporal cluster      | Worker + Restate runtime         | Inline                     | Inline                     | Inngest functions             |
| **Open source runtime**  | Yes (Kafka is OSS)           | Yes (lib only)                | Yes                            | Yes                              | Yes (lib only)             | Yes (lib only)             | Open client, server is closed (or partial) |

**Where Calfkit wins:** Pydantic-AI-class authoring DX + durable, multi-language, zero-extra-cluster deployment + first-class choreography. No other system in this matrix offers that triple. **Where Calfkit loses:** in-process simplicity (LangGraph, Pydantic AI), strong determinism (Temporal), TS-native (Inngest), operational maturity (Temporal). See §4.10 for the head-to-head.

---

## 23. Implementation phasing

### Milestone M0 — Scaffolding (1 week)

- New repo layout (or branch). Empty pyproject.
- `aiokafka` dependency. `pydantic` v2 dependency. No FastStream.
- `Envelope`, headers codec, basic serializer.
- One Handler, one Worker, smoke test: client → Handler → Reply → client.
- L1 test infra (pure handler unit tests).

**Risk:** low. Mostly typing.

### Milestone M1 — Action algebra and runtime (2 weeks)

- `Emit`, `Call`, `Reply`, `Done`, `TailCall`, `Fail` Actions.
- Frame stack mechanics.
- Action interpreter in the Worker.
- `Context` API.
- L2 in-memory dispatcher (`MemoryTransport`).

**Risk:** low-medium. The frame-stack mechanics are inherited from 0.x; the interpreter is new but straightforward.

### Milestone M2 — Extensions and lifecycle events (1 week)

- `Extension` base class.
- `around_invoke` chain wiring.
- `around_publish` chain wiring.
- `LifecycleEvent` discriminated union.
- `on_event` dispatch.
- Worked example: OTel Extension.
- Tests for Extension composition order.

**Risk:** low. Pure plumbing.

### Milestone M3 — Run lifecycle and runs-state (1.5 weeks)

- `RunId` distinct from `correlation_id`.
- `RunStartedEvent`, `RunCompletedEvent`.
- `calf.runs-state` compacted topic; writes on terminal events.
- `client.invoke` / `client.execute` / `client.get_run_state`.
- Run lifecycle in OTel.

**Risk:** medium. The conflation in 0.x means this is the first time we draw the line; will surface design decisions about what state is checkpointed when.

### Milestone M4 — Fan-out and aggregator (2.5 weeks) ★ CRITICAL

This is the hardest milestone. The compacted-topic-backed aggregator (§9) is the load-bearing v1 contract. Things to validate empirically:

- Aggregator under partition rebalance (does state really survive?).
- Aggregator under Worker crash with mixed in-flight messages.
- Aggregator under bursts (many Fans concurrent on the same partition).
- Aggregator partial-collection semantics (`use_partial`, `continue` modes).
- Late Reply after tombstone — does dedup hold?

Suggested validation: chaos-testing via testcontainers with deliberately-injected kills and rebalances. The 0.x `_pending_batches` bug is the exact thing this milestone is designed to fix; if M4 lands without solid chaos coverage, we'll have rewritten the same bug.

**Risk:** high. This is the deepest behavioral change.

### Milestone M5 — HITL and Interrupt/Resume (1.5 weeks)

- `Interrupt` Action at Layer B (used directly via `@handler` for power users).
- Runs-state checkpoint on Interrupt.
- `client.resume(run_id, payload)` and the resume topic.
- Worker recovery path: on startup, no special action; resume is just a normal publish.
- Layer A surface: `@tool(requires_approval=True, resume_with=DecisionT)` — gates a specific tool invocation behind approval. Declared on the module-level tool decorator itself (the gate is a property of the tool, not the calling agent). The agent recipe emits `Interrupt` before invoking the tool body when the LLM picks the tool; on resume, if the approval payload says approved, the tool runs; if denied, the LLM sees a "human declined" tool-result and continues reasoning.
- Layer A surface: `@agent.interrupt_when(output_type=..., resume_with=...)` — gates a specific LLM output shape behind approval. The agent recipe inspects the final-output type each turn; if it matches, emits `Interrupt`. Rejected the prior dict-of-named-rules / lambda-over-output approach (§4.5 rationale).
- L2 test: round-trip Interrupt/Resume via `InMemoryWorker.testing(agent)` — invoke, assert interrupted, resume, assert completed.
- L3 test for crash-during-Interrupt.

**Risk:** medium. Less critical than M4 but novel. The Layer A HITL ergonomics need to match Pydantic AI's `requires_approval=True` shape; if they don't, the §1.3 "Pydantic-AI-class authoring DX" claim weakens.

### Milestone M6 — Tool model and cross-language scaffolding (1 week)

- Unified `Tool` class.
- `@tool` module-level decorator (the only Python-tool authoring form; with-parens variant accepts `requires_approval=`, `resume_with=`, and other intrinsic tool options — but **never** `topic=`, per G14). Topic auto-derives from the function name; overrides happen at `worker.add(fn, topic="...")`. No `@agent.tool` and no instance-decorator equivalent.
- `Tool.external(name=..., input=..., output=...)` classmethod (cross-language stub).
- `Tool.from_agent(agent, name=..., description=...)` classmethod (sub-agent as tool).
- LLM schema synthesis from the function signature (`@tool`) or `input` Pydantic model (`Tool.external` / `Tool.from_agent`).
- Tool registry per Worker. Same tool referenced from multiple agents registers once.
- (Defer cross-language SDKs; just document the wire format and provide a Python-only smoke test.)

**Risk:** low.

### Milestone M7 — Agent SDK (the primary user-facing surface) (2.5 weeks) ★ CRITICAL FOR DX

Delivers Layer A (§4) — without it, the SDK is a Kafka-with-Actions library, not an agent SDK. Treat ergonomics as load-bearing as the M4 aggregator.

Acceptance checklist:

- `Agent` narrow constructor (≤7 fields per §4.1); `system_prompt=` deprecated alias.
- Agent instance methods: `@agent.output_validator`, `@agent.interrupt_when(...)`, `agent.handoffs_to(*peers)`. No tool-owning surface (§10.1).
- Unified `Tool` type — `@tool`, `Tool.external(..., topic=...)`, `Tool.from_agent(...)`. All synthesize the same LLM-facing JSON schema.
- `Worker` builder — `.use`, `.deps_from`, `.add`, `.publish`, `.wire`, `.subscribe`, `.runtime`, `.inspect`, `.run`, `.drain`. No `handlers=`/`extensions=` constructor kwargs.
- `InMemoryWorker.testing(*agents)` plus the full builder; `wrk.invoke`/`emit`/`assert_published`/`published_envelopes`/`runs_for_agent`/`drain`.
- `Client` surface — `invoke(agent | ClientAgentRef)`, `invoke_topic` escape hatch, `emit`, `resume`.
- `Subscription(...)` value type (§11.B.7); `ctx.trigger` per-invocation origin info.
- Agent Loop recipe: `Call` on tool selection, `Fan` on parallel-tool-call output, `Reply` on final output, `Interrupt` for `requires_approval`/`interrupt_when`, `Emit` for event-triggered terminal runs.
- Choreography lifecycle events (`EventReceived`/`EventSkipped`/`EventDispatched`/`EventPublished`).
- Done-reachability check at registration (Call-reachable handlers cannot return `Done`).
- Namespace split: `calfkit` / `calfkit.actions` / `calfkit.extensions` / `calfkit.errors`.
- `KnownModelName` `Literal[...]` re-export; `py.typed` marker.
- Worked examples for §4.2, §4.3, §4.4, §4.5, §11.B.1-4, §11.B.6, §11.B.12.
- L2 test: stubbed LLM + tools + `InMemoryWorker.testing`; choreography assertion via `wrk.runs_for_agent`. L3 test: agent + tools across two Worker processes; 3-agent choreography pipeline.

**Risk:** medium-high. Pydantic-AI-migrant acceptance test: hello-world in <30 minutes. Document the `@agent.tool` → module-level `@tool` divergence prominently — migrants will type the wrong form from muscle memory.

### Milestone M8 — Observability defaults (1 week)

- `OTelExtension` shipped, on by default.
- Standard span hierarchy.
- Standard metrics.
- Cost-tracking event (`ModelCallCompleted` with token counts).
- L3 test: trace propagation across hops.

**Risk:** low.

### Milestone M9 — Error handling, retry, DLQ (1.5 weeks)

- `RetryExtension` with policies.
- DLQ writer.
- `Fail` Action handling.
- L2 + L3 tests.

**Risk:** low-medium.

### Milestone M10 — Schema Registry mode (deferred to v1.1)

- Avro/Protobuf encoders.
- Confluent Schema Registry client.
- Compatibility checking on Worker startup.

**Risk:** medium. Defer past 1.0.

### Total estimate

~15 weeks of focused engineering for v1.0. Two critical paths: M4 (fan-out aggregator) for correctness, M7 (Agent SDK) for adoption. M0-M3 can proceed in parallel with M4 spec work; M7 design can begin in parallel with M3 as it primarily depends on the Action algebra (M1) and runs-state (M3) being settled. M5-M9 are sequenced after M4 because they assume the aggregator pattern is solid.

### What to validate empirically vs analytically

| Item | Validation |
|---|---|
| Fan-out under chaos | **Empirical** — testcontainers + chaos |
| Idempotency under at-least-once | **Empirical** — duplicate injection |
| Frame-stack correctness | Analytical (small state machine; proof by case analysis) |
| Header codec | Analytical |
| Run lifecycle correctness | **Empirical** — crash injection at every state transition |
| Schema evolution | Analytical + L1 unit tests |
| Multi-tenant isolation | Analytical + L3 test for header propagation |
| Aggregator partial-collection modes | **Empirical** — slow Reply injection |
| OTel propagation across hops | **Empirical** — collector inspection |
| DLQ message integrity | **Empirical** — failure injection |

---

## 24. Open questions and unresolved trade-offs

I list each with my current lean. None are blocking the start of implementation, but they should be settled before the surface is frozen.

1. **Should `RunId` be the partition key for everything, or per-Handler tunable?**
   *Lean:* default `key=run_id`, override per-Handler. Ensures same-Run hops are partition-affine (helps aggregator and ordering).

2. **Does the Envelope `call_stack` field replicate on every hop, or is it kept in runs-state and just `current_frame_id` rides on the wire?**
   *Lean:* keep full stack on the wire. It's small (typically 1-3 frames) and avoids a runs-state read on every hop.

3. **Should `Continue` action exist, or is it always expressible as `TailCall` to self?**
   *Lean:* keep `Continue` as in-process (no publish). It's the clean way for an Extension to short-circuit a Handler invocation without a Kafka round-trip.

4. **Ship a default `LLMExtension` for centralized rate-limit/retry/cost?**
   *Lean:* default handles cost-tracking only; rate-limit and retry are user-tunable.

5. **How does `Worker.deps_factory` interact with hot-reload / config changes?**
   *Lean:* not in v1.0. Workers are deploy-stop-redeploy. Hot deps reload is a v1.x concern.

6. **What happens to a Reply when the parent topic no longer has a consumer?** (e.g., parent Handler was removed in a deploy)
   *Lean:* DLQ to `calf.dlq.orphan-replies` after a configurable retention. Emit `OrphanReply` event.

7. **Multi-Handler-on-same-topic?**
   *Lean:* one Handler per topic per Worker (registration error otherwise). Multiple Workers on the same topic is the broadcast pattern.

8. **Should `Fan.calls` allow heterogeneous payload types or only one type?**
   *Lean:* heterogeneous. The aggregator doesn't care; the user reconstructs by key. The type checker will complain unless `ctx.fan_results` is typed `dict[bytes, Any]`.

9. **Long-running tool — tool takes 10 minutes, parent's `Call.timeout` expires.**
   *Lean:* tool runs to completion (`timeout` is the parent's hint, not a kill); late Reply is dedup-dropped; parent receives `Fail`. At-least-once cost.

10. **Should the runs-state topic carry the *full* state or just a pointer/version?**
    *Lean:* full state on `Interrupt` (we need it for resume); just metadata on `RunStarted`/`Completed` (we don't need to checkpoint mid-run by default).

11. **Should tool topics be auto-created or must they pre-exist?**
    *Resolved (EXPERIMENTAL, opt-in, OFF by default):* not auto-created by default — topics must pre-exist (via the broker's `auto.create.topics.enable` or an ops-governed pipeline). Calfkit ships an opt-in `ProvisioningConfig` that best-effort creates them; it is passed to the **client**, not the worker — `Client.connect(provisioning=ProvisioningConfig(enabled=True))` — because provisioning needs the broker URL + credentials that already live on the client, and the client's own reply topic needs the same switch. The earlier `Worker(auto_create_topics=True)` shape is **rejected** for that reason. When enabled, the worker provisions every topic its registered nodes reference at startup (before consumption), the client provisions its reply topic once, and `calfkit topics provision --nodes module:attr` covers the static/CI path. `replication_factor=1` and no ACLs make this a dev convenience, not a production provisioning story. See [docs/topic-provisioning.md](topic-provisioning.md).

12. **Envelope `schema_version` bumps mid-deploy?**
    *Lean:* Workers ship a one-version-back decoder; unknown future versions DLQ. Schema bumps are coordinated cluster-wide.

13. **Cost tracking with sub-Runs — same RunId vs new logical Run?**
    *Lean:* Handoffs share the parent's RunId (cost rolls up naturally); explicit `client.invoke` sub-Runs get a fresh RunId with `parent_run_id` for query-time roll-up.

14. **`agent_loop` internal state (`message_history`, `tool_calls`, `tool_results`) — in user `State` or a recipe-owned sub-field?**
    *Lean:* recipe-owned sub-field. Optional `agent_loop: AgentLoopState` slot; users who don't use `agent_loop` don't pay the State complexity.

15. **(Surfaced.) For the Schema Registry mode, do we require Confluent Schema Registry or accept Apicurio / Karapace / others?**
    *Lean:* Confluent Schema Registry first (most common). Add Apicurio/Karapace as adapters in v1.1.

16. **(Choreography.) When `worker.publish(...)` is set AND a caller awaits a `Reply`, emit the event topic + reply concurrently or sequentially?**
    *Lean:* sequentially, reply first then event — semantically clearer; observers can't see the event before the caller has been answered. Revisit under load testing.

17. **(Choreography.) Should an agent accept multiple `worker.wire(...)` / `worker.subscribe(...)` calls whose payloads are the same type but different sources?**
    *Lean:* yes (no constraint). The dispatch routes by topic; the handler receives the payload + `ctx.trigger.source_topic` to disambiguate. The LLM is invoked with the payload; if the agent needs source-specific behavior, the user can branch on `ctx.trigger`.

18. **(Choreography.) Orphaned subscriptions when a producer renames its events topic?**
    *Lean:* `worker.inspect()` in CI lists resolved topics; an optional `--strict-wiring` flag fails Worker startup if a Subscription's source-agent isn't co-registered (auto-detection in production is hard).

19. **(Choreography.) Should the runs-state topic for an event-triggered Run carry the parent producer's `run_id` (lineage) as part of the value, or just as the `parent_run_id` field?**
    *Lean:* `parent_run_id` field is enough. Don't denormalize producer state into the consumer's record — that's coupling. The OTel span hierarchy + runs-state index by `parent_run_id` is the join path.

20. **(Choreography.) Type→topic resolution for schema-typed subscriptions?**
    *Lean:* require explicit `class Config: calf_topic = ...` in v1.0. Implicit conventions are cross-package footguns. Revisit for v1.1.

21. **(Choreography.) N Agents subscribed to the same topic — one shared consumer or N separate?**
    *Lean:* N separate. Each subscription needs its own consumer group (the point of pub/sub fan-out). Cost: more consumer connections. Acceptable.

---

## 25. Appendix

### 25.1 Glossary

- **Action** — A typed value returned by a Handler that describes a side effect (publish, checkpoint, terminate) to be performed by the runtime. Lives in `calfkit.actions`.
- **Aggregator** — Runtime component that collects Fan-children Replies and re-enters the parent Handler with all results.
- **Agent** — A declarative class describing the identity, model, instructions, output schema, and tools of an LLM-driven Handler. Compiled to a Handler by `worker.add(agent)`. Not directly callable — see G13 and `InMemoryWorker`.
- **Choreography** — Producers publish events; consumers independently subscribe. Producers don't know consumers exist. Wiring lives on the Worker. See §11.B. Contrast with Orchestration.
- **ClientAgentRef** — A typed proxy for an Agent that the client process doesn't import. Carries `name`, `input_type`, `output_type`. Used by `client.invoke(ref, req)` in distributed deployments where the agent is hosted elsewhere.
- **Context** — The per-invocation object passed to a Handler, parameterized as `Context[Deps]`. Exposes `run_id`, `frame_id`, `state`, `deps`, `fan_results`, `resume_payload`, `trigger`, `once()`, `llm()`.
- **Deps** — User-provided dependency object (DB conn, HTTP client, etc.) supplied at Worker startup via `worker.deps_from(factory)`, scoped to the Worker process.
- **Envelope** — The wire-format message carrying state bytes, payload bytes, frame stack, IDs.
- **Event** — A wire-level envelope with `action_kind="emit"` and empty `call_stack`. Produced by `Emit`; consumed by Subscriptions. The choreography unit.
- **Extension** — User-provided cross-cutting concern, implementing one or more of `around_invoke`, `around_publish`, `on_event`. Lives in `calfkit.extensions`.
- **Fan** — An Action that dispatches N parallel Calls; the aggregator re-enters the parent with all Replies.
- **Frame** — One (caller, callee) pair within a Run; tracked in the Envelope's call_stack.
- **Handler** — User function from `(Context, Msg) -> Action`. The atomic unit of user code at Layer B.
- **HITL** — Human-in-the-loop. Layer A: `@tool(requires_approval=True, resume_with=...)` (primary) or `@agent.interrupt_when(...)` (secondary). Layer B: `Interrupt` Action + `client.resume(...)`. See §4.5.
- **Hop** — One trip from publisher to consumer over Kafka.
- **InMemoryWorker** — The `Worker` runtime with `MemoryTransport` instead of Kafka. Canonical local-dev / REPL / test idiom; `InMemoryWorker.testing(*agents)` is the factory. See §17.2.
- **LifecycleEvent** — Typed observable event (RunStarted, ToolCallStarted, EventReceived, EventSkipped, etc.) consumed via `on_event`.
- **Orchestration** — Caller explicitly invokes callee via `Call`/`Reply`/`Fan`. Wired via `agent.handoffs_to(...)` and `Tool.from_agent(...)`. See §11.A. Contrast with Choreography.
- **Run** — The durable agent-execution aggregate, keyed by `RunId` (uuid7). Event-triggered Runs have `parent_run_id` set to the producer's Run.
- **State** — User-typed value surviving across hops within a Run; opaque bytes on the wire.
- **Subscription** — Power-user value type binding an Agent to a Kafka topic. Most users use `worker.wire(...)` / `worker.subscribe(...)` kwargs instead. See §11.B.7.
- **Tool** — A standalone Kafka service callable by an LLM. First-class — never owned by an Agent. Three module-level construction paths: `@tool` (Python in-codebase), `Tool.external(...)` (cross-language stub), `Tool.from_agent(...)` (sub-agent). Attached via `tools=[...]`. Orchestration-only. See §10.1.
- **Trigger** — The origin of a Handler invocation, exposed as `ctx.trigger`. Kinds: `request`, `event`, `resume`, `tool_call`, `fan_child`. Lets agent code branch on "how did I get here."
- **Wire** — As in `worker.wire(source=..., target=..., payload=...)`. Deployment-time choreography binding — attaches a Subscription to an Agent. The canonical (and only) form for choreography wiring. See §11.B.6.
- **Worker** — A process hosting Handlers + Extensions, consuming from Kafka. Builder-style: narrow constructor + methods to attach extensions, deps, agents, and topology.

### 25.2 References

- **Temporal**: workflow + activity model, durable execution. We borrow the lifecycle-event idea; we reject the deterministic-workflow constraint.
- **Restate**: durable invocation. We share the "every state change is durable" property.
- **Inngest / Trigger.dev**: function-step durable execution. Their `step.run` is closest to our `ctx.once`.
- **LangGraph v1 middleware**: their `before_*`/`after_*`/`wrap_*` is what `docs/hooks-design.md` was modeled on. We reject their hierarchy and adopt three orthogonal primitives.
- **OpenAI Agents SDK**: handoffs as a first-class concept. We collapse handoffs into `Call`.
- **Pydantic AI**: `RunContext[Deps]`, tool schema synthesis from Python type hints. We adopt the type-driven schema synthesis; we drop the in-process loop.
- **AutoGen / CrewAI**: multi-agent patterns. We support all their patterns as recipes over `Call`/`Reply`/`Fan`.
- **Confluent Schema Registry**: schema management. Mode B in §6.3.
- **OTel semantic conventions for messaging**: we follow `messaging.system="kafka"` and emit `messaging.destination`, `messaging.message_id`, etc., alongside `calfkit.*` attributes.

### 25.3 File references in current calfkit being replaced

For grounding of the discard list (§21):

| File | Lines | What lives here today | Status in v1 |
|---|---|---|---|
| `calfkit/nodes/base.py` | 42 | `BaseNodeDef` class | Removed; `@handler` decorator replaces |
| `calfkit/nodes/base.py` | 85-93 | `gate()` decorator + `_evaluate_gates` | Removed; gates become Extension recipe |
| `calfkit/nodes/base.py` | 127-146 | `run()` abstract method | Removed; user writes plain async functions |
| `calfkit/nodes/base.py` | 161-162 | `_emitter_headers` (2-key dict) | Replaced by comprehensive header set (§6.2) |
| `calfkit/nodes/base.py` | 164-245 | `_publish_action()` Action dispatch | Replaced by runtime's Action interpreter (§3.5) |
| `calfkit/nodes/base.py` | 247-278 | `handler()` FastStream entry | Replaced by aiokafka dispatcher |
| `calfkit/nodes/agent.py` | 27-228 | `BaseAgentNodeDef` god-class | Removed; `agent_loop` recipe |
| `calfkit/nodes/agent.py` | 50 | `_pending_batches` dict | Removed; compacted topic aggregator (§9) |
| `calfkit/nodes/agent.py` | 61-72 | `_parallel_state_aggregation` | Removed; runtime-owned |
| `calfkit/nodes/agent.py` | 74-221 | `run()` god-method | Decomposed into agent_loop recipe + tool dispatch |
| `calfkit/nodes/agent.py` | 116-120 | The `RuntimeError("This indicates lost PendingToolBatch state")` | Eliminated by design |
| `calfkit/nodes/tool.py` | 26-105 | `ToolNodeDef` | Unified `Tool` class; `@tool` (module-level, topic auto-derived from `fn.__name__`) / `Tool.external` (explicit `topic=`) / `Tool.from_agent` |
| `calfkit/nodes/tool.py` | 99-105 | `agent_tool` decorator | Replaced by module-level `@tool` (renamed and refined; the structural shape — standalone Kafka service, not agent-owned — is preserved from 0.x) |
| `calfkit/models/actions.py` | 19-119 | `Reply`, `Delegate`, `Call`, `TailCall`, `ReturnCall`, `Sequential`, `Emit`, `Parallel`, `Silent` | New Action algebra (§3) |
| `calfkit/models/envelope.py` | 9-17 | `Envelope(BaseModel)` with inline `SessionRunContext` | New Envelope with opaque state bytes (§6.1) |
| `calfkit/models/state.py` | 18-101 | `State` god-model | User-defined StateT (§7) |
| `calfkit/models/state.py` | 127-141 | `PendingToolBatch` | Replaced by aggregator (§9) |
| `calfkit/models/session_context.py` | 33-39 | `CallFrame` dataclass | Survives as internal Frame model |
| `calfkit/models/session_context.py` | 45-70 | `WorkflowState` | Becomes part of Envelope structure |
| `calfkit/models/session_context.py` | 73-78 | `Deps(BaseModel)` | Survives, slimmed |
| `calfkit/models/session_context.py` | 81-114 | `BaseSessionRunContext` / `SessionRunContext` | Becomes `Context[Deps]` (single generic param) |
| `calfkit/_protocol.py` | 21-25 | `HDR_EMITTER`, `HDR_EMITTER_KIND` | Survives; extended (§6.2) |
| `calfkit/_protocol.py` | 28-39 | `decode_header_str` | Survives as internal codec helper |
| `calfkit/client/base.py` | 50-112 | `BaseClient`, `connect` | Slimmed; `Client.connect` keeps the API surface |
| `calfkit/client/base.py` | 124-182 | `_invoke` | `Client.invoke` / `Client.execute` |
| `calfkit/client/client.py` | 19-218 | `Client.invoke_node` / `execute_node` | `Client.invoke` / `Client.execute` with cleaner signatures (no `user_prompt` coupling) |
| `calfkit/client/reply_dispatcher.py` | 16-60 | `_ReplyDispatcher` | Survives, slimmed; same shape |
| `calfkit/client/middleware.py` | 9-23 | `ContextInjectionMiddleware` | Removed; aiokafka context handled directly |
| `calfkit/client/node_result.py` | 11-40 | `NodeResult` | `RunResult` (different name, similar shape) |
| `calfkit/worker/worker.py` | 12-61 | `Worker(client, nodes=[...])` | `Worker(bootstrap_servers=..., handlers=[...])` standalone |
| `calfkit/worker/worker_config.py` | 7-15 | `WorkerConfig` | Survives, expanded |
| `docs/hooks-design.md` | — | Two-layer middleware proposal | Superseded by §12 (this doc) |

---

*End of design document.*
