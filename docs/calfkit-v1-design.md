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

- **The user-facing primary surface** is an Agent SDK in the modern Pydantic-AI / OpenAI-Agents / LangGraph idiom: declarative `Agent(name=..., model=..., system_prompt=..., tools=[...], output_type=...)` classes, registered with a Worker in one line (`worker.add(agent)`). This is what 80%+ of users author. They never write a Handler, never see an Action, never touch a topic name unless they want to.
- **The runtime primitive layer** — five concepts: **Handler, Action, Run, State, Worker** — is what the Agent SDK compiles down to. It is the surface for tool authors, advanced orchestration (custom nodes that aren't agents), Extension authors, and the SDK itself. Always documented, always accessible.

The unique value of calfkit is the *combination*: you author your agent at the Pydantic-AI tier of ergonomics, and the SDK deploys it as a distributed, Kafka-native, multi-language-tool-capable, durable, replayable service — with no wiring code from the user. **Both orchestration and choreography are first-class.** Agents may be invoked directly (request/reply over Kafka), wired into hierarchies (handoff, sub-agents, parallel fan-out — see §11.A), *and* subscribed to event streams from other agents or arbitrary Kafka topics (linear pipelines, pub/sub fan-out, schema-typed event reactions — see §11.B). The wiring is declarative and can live either inside an `Agent(...)` declaration or outside it via `worker.wire(...)`, so the same agent can be reused across topologies. If a developer can get the same result by stuffing Pydantic AI inside a Temporal activity, calfkit has no reason to exist. The Agent SDK is the answer to that question. See §4 for the two-layer surface and a head-to-head ergonomics comparison against "Temporal + Pydantic AI."

The runtime layer ships an Action algebra as the single side-effect channel for all Kafka traffic. Durability comes from Kafka alone: an at-least-once log plus two compacted topics (runs-state, fan-out aggregator) replace the in-process `_pending_batches` dict at `calfkit/nodes/agent.py:50`, which is the canonical bug class this version is designed to eliminate. Tools become first-class Kafka topics (cross-language by construction), the agent loop stops being a `BaseAgentNodeDef.run()` god-method, and the FastStream coupling is replaced by direct `aiokafka` so the runtime can own rebalance, header typing, and the fan-out aggregator. Extensions get exactly three primitives (`around_invoke`, `around_publish`, `on_event`) — the named-sugar hierarchy proposed in `docs/hooks-design.md` is rejected because it lies about cross-process boundaries. The doc below specifies the wire format, the lifecycle events, the testing harness, and a five-milestone path from empty repo to feature parity.

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

1. **The agent loop is one god-method.** `BaseAgentNodeDef.run()` at `calfkit/nodes/agent.py:74-221` is ~150 lines mixing tool-registry resolution, parallel aggregation, model invocation, branch on `DeferredToolRequests`, and `Call`/`TailCall`/`ReturnCall` selection. Every cross-cutting concern (retry, tracing, audit, tool filtering) wants to interpose somewhere inside that method, but it has no seams.

2. **Durability is half-Kafka, half-Python-dict.** `BaseAgentNodeDef._pending_batches: dict[str, PendingToolBatch]` at `calfkit/nodes/agent.py:50` is the durability story for parallel tool fan-out. It is lost on process restart, lost on consumer-group rebalance, and not replicated across worker processes. The agent itself catches this at `agent.py:117-120` with a `RuntimeError` whose message literally says *"This indicates lost PendingToolBatch state."* Any non-trivial production deployment will eventually hit this.

3. **Extension is structurally impossible.** Today the only extension points are `gates` (bool predicates at `nodes/base.py:85-93`) and overriding `run()` itself. Cross-cutting concerns (retry, OTel, audit, rate limiting, tool filtering, header propagation) cannot be expressed without forking the SDK. The `docs/hooks-design.md` two-layer middleware proposal acknowledges the gap but introduces named-sugar methods (`before_agent_run`/`after_agent_run`) that fire in *different Kafka handler invocations* — i.e., different Python processes — without that fact being visible in the API. That design lies about distributed reality.

A refactor that fixes (1) breaks public surface. A refactor that fixes (2) requires a new wire field, a new compacted topic, and runtime-owned aggregator code none of which the current `BaseNodeDef` knows about. A refactor that fixes (3) introduces a competing middleware API that conflicts with `gates`. Three concurrent breaking refactors is a 1.0 rewrite.

### 1.2 What problem the SDK solves and for whom

**Problem:** Engineers building production AI agent systems want both (a) the Pydantic-AI / OpenAI-Agents tier of authoring ergonomics for the agent itself — declarative class, system prompt, typed tools, typed output — and (b) event-driven decoupling for deployment (independent scaling, replayability, cross-team integration, polyglot tools, durability). The dominant agent frameworks (LangGraph, Pydantic AI, OpenAI Agents, AutoGen, CrewAI) deliver (a) but are in-process Python — they fail at (b). The durable-execution platforms (Temporal, Restate, Inngest) deliver (b) but offer no agent-specific authoring surface — users stitch together a Pydantic AI agent inside a Temporal Activity, write an HTTP gateway for cross-language tools, and maintain that glue themselves. Calfkit's reason to exist is to deliver both (a) and (b) in a single SDK with no manual stitching.

The instant you need *"the email-classifier team owns the classify-tool, the trading team owns the trading-agent, both deploy independently"*, the in-process frameworks force you into RPC-shaped duct tape. The instant you need *"replay this agent run from yesterday for debugging"*, they force you onto Temporal or roll-your-own. Calfkit collapses both forcing functions: the agent author writes Pydantic-AI-shaped code; the platform delivers Kafka-native distribution. See §4.10 for the head-to-head against "Temporal + Pydantic AI."

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

**The DX target, separately:** the *authoring* surface of calfkit (the Agent SDK, §4) targets the same ergonomics as Pydantic AI, OpenAI Agents SDK, and (parts of) LangGraph. A user authoring `Agent(name=..., model=..., tools=[...], output_type=...)` should not be able to distinguish calfkit from Pydantic AI on first inspection — that is intentional. Calfkit's differentiator is what happens *after* `worker.add(agent)`: the agent runs distributed over Kafka, its tools may live in other languages, its runs are durable, its fan-outs survive restart. The in-process SDKs cannot offer that combination; the durable-execution platforms (Temporal, Restate) cannot offer Pydantic-AI-class authoring ergonomics on top of the same runtime. Calfkit's value is precisely this intersection. §4.10 walks through the head-to-head ergonomics comparison.

### 1.4 Goals (explicit)

- **G0.** **The user-facing primary authoring surface is the Agent SDK** (§4) — a declarative `Agent(name=..., model=..., system_prompt=..., tools=[...], output_type=...)` API at the same DX tier as Pydantic AI and the OpenAI Agents SDK. A new user authoring a hello-world distributed agent should write `Agent(...) ; worker.add(agent) ; worker.run()` and nothing else. The runtime primitives (G1) are the layer *below*; users only see them when they want to.
- **G1.** Five-concept runtime model — Handler, Action, Run, State, Worker — is the underlying primitive layer. Every recipe (the Agent SDK itself, agent loop, handoff, fan-out, HITL) is expressible in those five. The Agent SDK compiles down to them.
- **G2.** Durability comes from Kafka log + compacted topics. No proprietary cluster, no Postgres dependency.
- **G3.** Parallel fan-out is durable across restart and rebalance. The 0.x `_pending_batches` hole is closed.
- **G4.** Tools are Kafka topics. A Go tool can serve a Python agent without an FFI layer.
- **G5.** Extensions ship with a three-primitive API (`around_invoke`, `around_publish`, `on_event`). No named-sugar hierarchy.
- **G6.** No graph DSL. Topology is emergent from topic wiring.
- **G7.** No `dict[str, Any]` on the wire. Discriminated unions or typed payloads only.
- **G8.** Async-only. No sync handler support.
- **G9.** Multi-tenancy via a tenant header by default; topic-per-tenant is opt-in.
- **G10.** HITL via `Interrupt`/`Resume` actions backed by the runs-state compacted topic.
- **G11.** Testing has three honest layers: pure handler unit, in-memory dispatcher, real Kafka.
- **G12.** OTel-native observability by default.

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

**A note before diving in.** The five concepts in this section are the *runtime* primitives. They are what the SDK executes, what Extensions hook into, what the wire format encodes. They are NOT the primary authoring surface for agent developers — that is the Agent SDK described in §4, which is a recipe layer over these five. Read this section to understand what the SDK does; read §4 to understand what users write. Tool authors, Extension authors, and users building non-agent orchestration nodes write at this level directly.

### 2.1 Handler

A **Handler** is an `async` function from `(Context, Message) -> Action`. It is the atomic unit of *runtime* user code in calfkit — meaning, it is what the SDK dispatcher invokes on each Kafka message. Most agent authors do not write Handlers directly; they write `Agent(...)` declarations (§4) and the SDK synthesizes the Handler. But Handlers are first-class, fully documented, and used by tool authors, custom-orchestration users, and SDK internals.

```python
async def my_handler(ctx: Context[Deps, State], msg: InputMsg) -> Action:
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
- **Opaque bytes on the wire.** The runtime sees `state: bytes` on the Envelope. Serialization is the Handler's concern (the `Context[Deps, State]` generic is parameterized on the user's state type; deserialization happens at the SDK boundary, not in the user's Handler body).
- **Versioned.** A `state_schema_version: str` field on the Envelope lets a Handler reject incompatible states or migrate them.
- **Has explicit scopes.** See [Section 7](#7-state-model) for run-scoped vs frame-scoped vs long-term.

The critical motivation for opaque-bytes: today, `State` extends `BaseModel` with no `exclude_unset` discipline, but the *parts of it the agent loop manipulates* (`tool_calls`, `tool_results`, `message_history`) carry the `CompactBaseModel`-style footguns documented in user memory: optional fields without `= None` get excluded then fail re-validation; `.append()` doesn't mark a field as set. Opaque bytes pushes the serialization choice down to the user, so the SDK never has to reason about its shape.

### 2.5 Worker

A **Worker** is the process that hosts handlers, consumes from their topics, and runs the SDK runtime.

```python
worker = Worker(
    bootstrap_servers="kafka:9092",
    handlers=[order_classifier, fraud_checker, decision_agent],
    extensions=[OTelExtension(), RetryExtension(max=3)],
)
await worker.run()
```

- **Hosts ≥1 Handler.** Each Handler maps to one Kafka consumer subscription.
- **Owns the runtime.** Deserializing envelopes, dispatching to Handlers, interpreting Actions, running the Extension chain, owning the fan-out aggregator, publishing on behalf of the Handler.
- **Scales horizontally** via Kafka consumer groups. Adding a Worker process to the same group increases parallelism on the same topics.
- **Owns the rebalance listener** (a key reason for direct aiokafka). On partition revocation, the Worker checkpoints any pending aggregator state to the compacted topic before releasing the partition.
- **Owns lifecycle hooks** (graceful shutdown, signal handling, draining).

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
| `Done`       | `Done()`                                                 | No                     | n/a                    | Terminate this hop. No publish. Marks the Run as complete if there's no parent frame. Equivalent to today's `Silent` but with explicit terminal semantics. |
| `Fail`       | `Fail(error: ErrorPayload, retry: RetryHint = AUTO)`     | DLQ publish (or retry topic) | `(run_id, frame_id, attempt)` | Explicit failure. Differs from raising an exception: gives the runtime a typed error and retry hint. |

### 3.2 First-class vs recipe

| First-class                                       | Recipe                                                                                  |
|---------------------------------------------------|-----------------------------------------------------------------------------------------|
| `Emit`, `Call`, `Reply`, `Fan`, `Interrupt`, `Done`, `Fail`, `Continue` | "Agent loop" = Handler that returns `Call` to tool topic, re-enters on `ReturnCall`-style |
|                                                   | "Handoff" = `Call` to another agent's topic; `Reply` from that agent pops back          |
|                                                   | "Supervisor-worker" = `Fan` of `Call`s, aggregator collects `Reply`s                    |
|                                                   | "Sequential pipeline (orchestration)" = Handler chain via `Call`/`Reply` with no branching |
|                                                   | "Linear choreography" = `Emit` from agent A; agent B `subscribes_to` A's event topic (see §11.B) |
|                                                   | "Pub/sub fan-out" = `Emit` from a single producer; N agents independently `subscribes_to` the same topic |
|                                                   | "TailCall" (in 0.x) = `Call` + `Done` semantically; absorbed                            |
|                                                   | "Sequential" (in 0.x) = a list of `Emit`s with state threading; not first-class         |

The 0.x `Sequential`, `Delegate`, `Parallel`, `TailCall` types (at `calfkit/models/actions.py:73-108`) collapse into the simpler set above. `TailCall` in particular is `Call` with a hint to swap the current frame rather than push — implemented as an Action variant or, if we want to remove it entirely, the user just emits `Call`+`Done` and the runtime collapses the frame.

**Recommended decision:** keep `TailCall` as a first-class Action because the partial-state-aggregation pattern (today at `agent.py:174-178`) benefits from explicit frame-replacement semantics. The doc above counts seven Actions; promoting `TailCall` makes eight. Going forward I treat `Done` as the strict terminator and `TailCall` as an Action variant for the "this hop = a re-dispatch of myself" pattern.

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

This is the most important section in the doc for the SDK's value proposition. Calfkit is a **two-layer SDK**. The primary user-facing surface is the Agent SDK — a declarative class API that looks and feels like Pydantic AI or the OpenAI Agents SDK. Under it, a runtime layer of Handler / Action / Run / State / Worker (specified in §2 and §3) does the actual Kafka work. The two layers are not optional alternatives — they are stacked. The Agent SDK *compiles* into Handlers and Actions. A user who never wants to know that exists, doesn't have to. A user who wants escape hatches, can drop one level and write a Handler directly. Tool authors and Extension authors naturally work at the runtime tier.

### 4.1 The two layers — what they are, and what each is for

| Layer                  | Surface                                                        | Who writes it                                                                                  | Concepts they touch                                                                                  |
|------------------------|----------------------------------------------------------------|------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| **Layer A: Agent SDK** | Declarative classes: `Agent`, `tool`, `output_type`, `handoff`, `Sub`, `worker.add(agent)` | Agent developers (the 80%+ case). The "I want to ship an agent over Kafka" audience.            | `Agent`, `tool`, `output_type`, `output_validator`, `system_prompt`, `instructions`, `handoff_to`, `Sub` (sub-agent), `Interrupt` (HITL), `Deps`, `Worker.add` |
| **Layer B: Runtime**   | `@handler` + Action algebra: `@handler`, `@tool`, `Emit`/`Call`/`Reply`/`Fan`/`Interrupt`/`Done`/`Fail`/`TailCall`, `Extension` | Tool authors. Authors of custom orchestration nodes that aren't agents. Extension authors. SDK internals. | All five from §2 plus the Action algebra from §3.                                                    |

The defining property: **every Layer A construct is a recipe over Layer B**. There is nothing in the Agent SDK that requires a wire-format change, a new Action variant, or a runtime concept that doesn't already exist for Layer B. The Agent SDK is purely an authoring-time compiler that materializes a `BaseAgentRecipe`-driven Handler at registration time. If a user inspects what `worker.add(agent)` produced, they see a normal Handler registered against a normal topic, with the agent's behavior implemented via the standard `Call`/`Reply` agent loop, just as they could have written by hand.

### 4.2 Hello world — the user-facing primary surface

This is what 80%+ of users write. Every line is calfkit; no Pydantic AI under the hood (the Agent SDK is calfkit-native, with type-driven schema synthesis and structured output ported from the Pydantic AI conventions but owned by calfkit).

```python
# weather_agent.py
from calfkit import Agent, Worker, tool
from pydantic import BaseModel

class WeatherReport(BaseModel):
    location: str
    summary: str
    temperature_c: float

@tool
async def get_weather(location: str) -> dict:
    """Fetch current weather for a location."""
    ...

@tool
async def get_forecast(location: str, days: int = 1) -> list[dict]:
    """Fetch a weather forecast for the next N days."""
    ...

weather_agent = Agent(
    name="weather-bot",
    model="openai:gpt-5.4",
    system_prompt="You help users find current weather and short-term forecasts.",
    tools=[get_weather, get_forecast],
    output_type=WeatherReport,
)

if __name__ == "__main__":
    worker = Worker(bootstrap_servers="kafka:9092")
    worker.add(weather_agent)
    worker.run_blocking()
```

That's the whole file. No `@handler`. No `Action`. No topic strings hand-written by the user. No wiring code. The user authored an agent at the Pydantic-AI tier and deployed it as a Kafka-native service.

What `worker.add(weather_agent)` does, mechanically (§4.7 expands):
1. Derives the agent's input topic from `name`: `agent.weather-bot.in` (override via `Agent(topic=...)`).
2. Derives each tool's topic from the tool's name: `tool.get_weather`, `tool.get_forecast`.
3. Synthesizes the agent's input/output Pydantic models for the Envelope payload schema.
4. Generates a Handler from the agent recipe (system prompt + tools + output_type + model = an agent loop). The Handler returns `Call` to tool topics on each LLM tool selection, `Reply` with `WeatherReport` on final output.
5. Registers a tool-Handler for each `@tool` (each tool is itself a Handler subscribed to its tool topic).
6. Wires the agent's `Call`-receipt back into itself for the next-turn re-entry.

The user sees none of this unless they ask (`worker.inspect()` prints the materialized topology). They can override any of it via `Agent(...)` parameters or by escaping to Layer B (§4.6).

### 4.3 Agent with cross-language tools — the calfkit moat

This is calfkit's structural differentiator vs Pydantic AI / OpenAI Agents / LangGraph. A Python agent can use a tool implemented in Go, Rust, Java, or TypeScript — without an FFI layer, without an HTTP gateway, without any change to the agent's authoring code.

```python
# trading_agent.py
from calfkit import Agent, Worker, tool, external_tool
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

# Python tools — implementation lives here
@tool
async def fetch_quote(symbol: str) -> dict:
    """Get the latest market quote for a symbol."""
    ...

@tool
async def fetch_portfolio(user_id: str) -> dict:
    """Get the user's current portfolio."""
    ...

# Cross-language tool — DECLARED here, IMPLEMENTED in a Go service.
# The Go service subscribes to topic "tool.execute_trade" using the
# calfkit-go SDK and publishes Replies in the same Envelope format.
execute_trade = external_tool(
    name="execute_trade",
    description="Submit a buy or sell order; returns confirmation id and fill price.",
    input=BuyOrSellOrder,   # Pydantic model used to synthesize the JSON schema for the LLM
    output=TradeConfirmation,
    # topic is derived: "tool.execute_trade"
)

trading_agent = Agent(
    name="trader",
    model="openai:gpt-5.4",
    system_prompt="You execute trades on behalf of users. Always confirm risk before placing orders.",
    tools=[fetch_quote, fetch_portfolio, execute_trade],
    output_type=TradeResult,
)

if __name__ == "__main__":
    worker = Worker(bootstrap_servers="kafka:9092")
    worker.add(trading_agent)
    worker.run_blocking()
```

From the Python agent's perspective, `execute_trade` is just a tool. The LLM picks it; the Agent SDK turns it into a `Call` to `tool.execute_trade`; some other process — possibly written in Go, possibly running in another data center, possibly owned by a different team — receives that Kafka message and Replies. None of the existing in-process agent SDKs can do this without a transport layer the user has to build by hand. Calfkit makes it the default.

The Go side, sketched in calfkit-go (eventual; v1.x roadmap):

```go
// execute_trade.go — Go service implementing the tool
package main

import "github.com/calfkit/calfkit-go"

func main() {
    w := calfkit.NewWorker("kafka:9092")
    w.AddTool("execute_trade", executeTrade)
    w.Run()
}

func executeTrade(ctx calfkit.Context, req BuyOrSellOrder) (TradeConfirmation, error) {
    // ... place the order via the exchange API ...
    return TradeConfirmation{ConfirmationId: id, ...}, nil
}
```

Same wire format; same Envelope; same headers. The Python agent doesn't know or care it's a Go tool.

### 4.4 Multi-agent: handoff, sub-agents, parallel sub-agents

This is where the Agent SDK absorbs the patterns that LangGraph / OpenAI Agents express via graph nodes or `Handoff` types. All of them compile to `Call`/`Reply`/`Fan` Actions under the hood.

**Handoff (OpenAI Agents-style):** an agent decides which other agent to route to next.

```python
billing_agent = Agent(
    name="billing-agent",
    model="openai:gpt-5.4",
    system_prompt="You handle billing questions.",
    output_type=BillingAnswer,
    tools=[lookup_invoice, issue_refund],
)

tech_agent = Agent(
    name="tech-agent",
    model="openai:gpt-5.4",
    system_prompt="You handle technical support questions.",
    output_type=TechAnswer,
    tools=[run_diagnostics, fetch_logs],
)

triage_agent = Agent(
    name="triage",
    model="openai:gpt-5.4",
    system_prompt="Route user requests to the right specialist.",
    handoffs=[billing_agent, tech_agent],
    output_type=str,  # if no handoff, give a direct answer
)

worker.add(triage_agent)
worker.add(billing_agent)
worker.add(tech_agent)
```

`handoffs=[...]` is a recipe over `Call`. The triage agent's compiled Handler, when the LLM picks a handoff target, returns `Call(topic="agent.billing-agent.in", payload=...)`. The called agent's `Reply` flows back to the original client via the frame stack. No new wire concepts.

**Sub-agents (Pydantic AI / LangGraph-style):** an agent invokes another agent as a synchronous-feeling helper inside its own reasoning. The sub-agent's output is returned to the parent's LLM as a tool result.

```python
research_agent = Agent(
    name="researcher",
    model="openai:gpt-5.4",
    system_prompt="You research a topic deeply.",
    output_type=ResearchReport,
    tools=[web_search, read_url],
)

writer_agent = Agent(
    name="writer",
    model="openai:gpt-5.4",
    system_prompt="You write an article based on the research provided.",
    output_type=Article,
    tools=[
        Sub(research_agent, as_tool="research_topic"),  # research_agent appears as a tool
    ],
)

worker.add(research_agent)
worker.add(writer_agent)
```

`Sub(research_agent, as_tool="research_topic")` is sugar that wraps the sub-agent as a tool from the parent's perspective. Under the hood, `writer_agent`'s LLM sees a tool named `research_topic` with input/output schemas synthesized from the sub-agent's input/output types. When the LLM picks that tool, the writer agent emits `Call(topic="agent.researcher.in", payload=...)` — i.e., a normal agent-to-agent Call. The sub-agent's `Reply` (a `ResearchReport`) is returned to the writer's LLM as a tool result, exactly as if it had been a regular tool. The two agents may run on different Worker processes, scale independently, even be written in different languages (if the sub-agent is `external`).

**Parallel sub-agents (LangGraph fan-out style):** invoke multiple sub-agents concurrently and aggregate.

```python
aggregator_agent = Agent(
    name="aggregator",
    model="openai:gpt-5.4",
    system_prompt="Synthesize answers from multiple specialists.",
    output_type=ConsensusAnswer,
    parallel_tools=[
        Sub(specialist_a, as_tool="ask_specialist_a"),
        Sub(specialist_b, as_tool="ask_specialist_b"),
        Sub(specialist_c, as_tool="ask_specialist_c"),
    ],
    # When the LLM picks 2+ parallel_tools in one turn, the SDK emits Fan instead of Call.
)
```

`parallel_tools` is the agent-level signal that "if the LLM selects multiple of these in a single turn, dispatch them in parallel." The Agent recipe inspects the LLM's tool-selection output: 1 tool selected → `Call`; multiple selected → `Fan`. The fan-out aggregator (§9) collects the Replies and re-enters the agent with all results visible to the LLM's next turn. The user never touches the aggregator topic, never thinks about `fan_id`, never writes the partial-failure handler unless they want to.

### 4.4.B Choreography — agents that subscribe to event streams

Everything in §4.4 is **orchestration**: an agent explicitly invokes another agent (handoff, sub-agent, parallel sub-agents). The caller knows the callee exists. The caller initiates.

**Choreography is the opposite shape.** An agent publishes events. Other agents subscribe to those events and react. The producer does not know the consumers exist. The wiring is declared at deployment time, not in the producer's code. This is the canonical Kafka pattern, and calfkit treats it as first-class — the full §11.B describes the model, this subsection shows the Layer A surface.

**Linear choreography (B always reads A's output stream).** A classifier emits `Classified` events; an enricher reacts to each one.

```python
from calfkit import Agent, Worker, Subscription
from pydantic import BaseModel

class RawInput(BaseModel):
    text: str

class Classified(BaseModel):
    text: str
    label: str
    confidence: float

class Enriched(BaseModel):
    text: str
    label: str
    geo: dict

classifier = Agent(
    name="classifier",
    model="openai:gpt-5.4",
    system_prompt="Classify the input. Emit a Classified event.",
    output_type=Classified,
    publishes=Classified,           # NEW — after final Reply, also Emit to agent.classifier.events
)

enricher = Agent(
    name="enricher",
    model="openai:gpt-5.4",
    system_prompt="Enrich the classified record with geo data.",
    output_type=Enriched,
    subscribes_to=[                 # NEW — reactive entrypoints, separate from agent.enricher.in
        Subscription(source=classifier, payload=Classified),
    ],
)

if __name__ == "__main__":
    worker = Worker(bootstrap_servers="kafka:9092")
    worker.add(classifier)
    worker.add(enricher)
    worker.run_blocking()
```

The classifier does not import `enricher`, never names its topic, and is unchanged whether 0, 1, or N consumers subscribe to its events. The enricher is unchanged whether the classifier exists yet or not (the topic auto-creates). This is choreography.

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
    system_prompt="Identify the most relevant new articles and emit them.",
    output_type=NewsArticle,
    publishes=NewsArticle,
)

summarizer = Agent(
    name="summarizer",
    model="openai:gpt-5.4",
    system_prompt="Produce a 1-paragraph summary of the article.",
    output_type=Summary,
    subscribes_to=[Subscription(source=watcher, payload=NewsArticle)],
)

sentiment = Agent(
    name="sentiment",
    model="openai:gpt-5.4",
    system_prompt="Score the sentiment of the article.",
    output_type=Sentiment,
    subscribes_to=[Subscription(source=watcher, payload=NewsArticle)],
)

alerter = Agent(
    name="alerter",
    model="openai:gpt-5.4",
    system_prompt="Send an alert for urgent articles.",
    output_type=AlertSent,
    subscribes_to=[
        Subscription(
            source=watcher,
            payload=NewsArticle,
            filter=lambda ev: ev.urgency >= 4,   # predicate; only urgent articles
        ),
    ],
)
```

Three agents independently subscribe to `agent.news-watcher.events`. Each runs in its own consumer group (`{group_prefix}.agent.<consumer-name>.sub.<source>`), so each receives every message. Filters run *before* the LLM is invoked — non-matching events are acknowledged and skipped without LLM cost.

**Subscribing to arbitrary Kafka topics (not just other agents' streams).** Calfkit doesn't require the producer to be a calfkit agent. Subscribe to any Kafka topic whose payload deserializes into a Pydantic model:

```python
audit_agent = Agent(
    name="audit",
    model="openai:gpt-5.4",
    system_prompt="Summarize completed transactions for the audit log.",
    output_type=AuditEntry,
    subscribes_to=[
        Subscription(
            topic="events.transactions.completed",    # raw Kafka topic string
            payload=TransactionCompleted,             # Pydantic model the SDK uses to decode
        ),
    ],
)
```

This is how calfkit integrates with non-calfkit producers — a transactions service in Java that publishes Avro to `events.transactions.completed` can drive a calfkit agent without any calfkit-side wiring (Avro mode requires Schema Registry config; see §6.3).

**Deployment-time wiring (no producer / consumer coupling at authoring time).** Reusable agents shouldn't know which producer their events come from. The Subscription can be declared at the worker level instead:

```python
# In a shared library — the agents are reusable units
classifier = Agent(name="classifier", ..., publishes=Classified)
enricher   = Agent(name="enricher",   ...)           # no subscribes_to
geo_tagger = Agent(name="geo-tagger", ...)           # no subscribes_to

# In a deployment-specific main — wire the topology here
worker = Worker(bootstrap_servers="kafka:9092")
worker.add(classifier)
worker.add(enricher)
worker.add(geo_tagger)

# Wire the choreography topology outside the agent definitions
worker.wire(source=classifier, target=enricher,   payload=Classified)
worker.wire(source=classifier, target=geo_tagger, payload=Classified)

worker.run_blocking()
```

`worker.wire(...)` is sugar for "attach this Subscription to that agent at registration time." It's how a team builds reusable agent libraries and composes them per deployment. Either the in-Agent `subscribes_to=[...]` form *or* the `worker.wire(...)` form may be used; the underlying mechanism is identical.

**Mixed orchestration + choreography (the realistic case).** An agent may have request input AND event subscriptions AND handoffs AND tools — all at once. This is normal.

```python
support_agent = Agent(
    name="support-bot",
    model="openai:gpt-5.4",
    system_prompt="Handle support inquiries. For billing issues, hand off to billing.",
    tools=[lookup_user, fetch_history],            # tools (orchestration: agent calls them)
    handoffs=[billing_agent],                       # handoff (orchestration: agent picks)
    subscribes_to=[                                 # choreography: agent reacts to events
        Subscription(topic="events.customer.churn-risk", payload=ChurnRisk),
    ],
    output_type=SupportResponse,
    publishes=SupportResponse,                      # this agent also emits an event stream
)
```

This agent can be invoked directly (`client.invoke("agent.support-bot.in", req)`), reacts to churn-risk events from elsewhere in the company, can hand off to billing mid-conversation, uses tools to answer, and emits a `SupportResponse` event for any downstream consumer (analytics, summarization, archival) — none of which it needs to know about.

**What about consumer semantics in the LLM loop?** When an event arrives, the agent's LLM is invoked with the event as its input message, the agent's normal `system_prompt`, and the tools the agent has. The event payload becomes the user-role content. From the LLM's perspective, an event-triggered run is identical to a request-triggered run — same system prompt, same tools, same output type. What differs is purely external: an event-triggered run has no caller waiting for `Reply`, so the final answer is published to the agent's event stream (if `publishes=...` is set) and the run terminates without a `Reply` publish. See §11.B for the full semantics and the alternatives considered.

### 4.5 Human-in-the-loop on the Agent SDK

```python
class ApprovalRequired(BaseModel):
    proposed_action: str
    risk_score: float

class HumanDecision(BaseModel):
    approved: bool
    notes: str = ""

approval_agent = Agent(
    name="approver",
    model="openai:gpt-5.4",
    system_prompt="Decide whether the proposed action needs human approval.",
    output_type=str,
    tools=[lookup_policy],
    interrupts={
        "human_approval": Interrupt(
            when=lambda output: isinstance(output, ApprovalRequired),
            resume_with=HumanDecision,
        ),
    },
)

# Client-side
handle = await client.invoke("agent.approver.in", req)
state = await handle.wait_for_interrupt()  # blocks until interrupted (or completed)
if state.kind == "interrupted":
    decision = await ui.show_approval_modal(state.payload)  # user input
    await client.resume(state.run_id, HumanDecision(approved=True))
result = await handle.result()
```

`interrupts={...}` is a recipe over the `Interrupt` Action. The agent recipe inspects the LLM output each turn; if it matches the `when` predicate, it emits `Interrupt`, persists state to the runs-state topic, and exits. On `client.resume(...)`, the agent re-enters with the human's input visible in `ctx` and continues. The user does not write Handler code; the recipe handles it.

### 4.6 Escape hatch: dropping to Layer B (the runtime API)

A handful of advanced cases need to step outside the Agent recipe — custom orchestration nodes that aren't agents (a router, a saga coordinator, a translator between two agents with incompatible schemas), tool authors implementing tools directly, or users who want full control over an unusual flow. For these, write a Handler.

```python
from calfkit import handler, Context, Reply, Call, Done

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
worker.add(csv_ingester)
worker.add(fraud_detector_agent)  # An Agent
worker.add(normal_processor_agent)  # An Agent
```

Layer A and Layer B coexist in the same Worker. `worker.add(...)` accepts both an `Agent` instance and a `@handler`-decorated function. Most users will use Layer A everywhere; advanced users will mix the two.

This composability is the reason the runtime layer remains first-class. It is not deprecated, not hidden, not labeled "internal." It is the canonical low-level surface, and the Agent SDK is layered on top of it.

### 4.7 `worker.add(agent)` — the full lifecycle, demystified

What happens when a user calls `worker.add(agent)`? This subsection unpacks the compilation step, because the magic only feels good if you can see through it on demand.

**Step-by-step, with concrete artifacts:**

```python
weather_agent = Agent(
    name="weather-bot",
    model="openai:gpt-5.4",
    system_prompt="...",
    tools=[get_weather, get_forecast],
    output_type=WeatherReport,
)
worker.add(weather_agent)
```

1. **Topic derivation.**
   - Agent *request* input topic: `f"agent.{agent.name}.in"` → `"agent.weather-bot.in"`. This is where direct `client.invoke(...)` requests land. Override via `Agent(topic=...)`.
   - Agent *event* output topic (only if `publishes=EventType` is set): `f"agent.{agent.name}.events"`. This is the choreography output stream — consumers `subscribes_to` this topic. Override via `Agent(events_topic=...)`.
   - Agent *event* subscription topics (one per Subscription in `subscribes_to=[...]`): derived from the Subscription's `source` agent (its events topic) or from a user-supplied `topic="..."` string. Each subscription is a separate Kafka consumer subscription under the agent's identity.
   - Tool input topics: `f"tool.{tool.name}"` → `"tool.get_weather"`, `"tool.get_forecast"`. Override per-tool via `@tool(topic=...)`.
   - Tool reply topic: by default, replies route back via the frame stack (no separate "tool reply topic" — the runtime tracks reply destinations on the Envelope).

2. **Schema synthesis.**
   - The agent's input model: the Agent SDK inspects the agent's signature (or the user-supplied `input_type=...`) and registers the Pydantic model for inbound envelope decoding.
   - The agent's output model: `output_type=WeatherReport` is registered for outbound `Reply` envelopes.
   - Each tool's JSON schema (for the LLM): synthesized from the tool function's signature — parameter types become input properties, the docstring becomes the description. The output type becomes the tool-result schema.

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
           #   1. Calls the LLM with system_prompt, history, tool schemas, user msg.
           #   2. If LLM selected a tool: returns Call(topic=tool_topic, payload=tool_args).
           #   3. If LLM produced a final answer: returns Reply(output_type(answer)).
           #   4. On Reply re-entry (tool result back): appends to history, loops back to (1).
       )
   ```
   This Handler is registered with the runtime exactly as if the user had written it. From the runtime's perspective, there is no difference between a "synthesized agent handler" and a hand-written `@handler`.

4. **Tool Handler registration.** Each `@tool` registers a runtime `@handler` for its tool topic. A tool is a Handler whose input is the tool args and output is the tool result. The runtime decodes/encodes, dispatches the tool function, and Replies.

5. **Consumer subscriptions wired.** The Worker now subscribes to:
   - `agent.weather-bot.in` (consumer group: `{group_prefix}.agent.weather-bot`) — the request entrypoint.
   - One additional consumer subscription per `Subscription` in `subscribes_to=[...]` (consumer group: `{group_prefix}.agent.weather-bot.sub.<source-or-topic>`) — each event subscription is its own subscription so consumer groups are distinct (this is what gives independent fan-out: agent B and agent C subscribing to agent A's events each see every message).
   - `tool.get_weather` (consumer group: `{group_prefix}.tool.get_weather`)
   - `tool.get_forecast` (consumer group: `{group_prefix}.tool.get_forecast`)
   - The shared runtime topics: `calf.fanout-agg`, `calf.runs-state` (if needed).

6. **Inspection.** Users can introspect the materialized topology:
   ```python
   worker.inspect()
   # Prints:
   #   Agent "weather-bot":
   #     Input topic: agent.weather-bot.in
   #     Output type: WeatherReport
   #     Tools (2):
   #       get_weather -> tool.get_weather (input: {"location": "str"}, output: dict)
   #       get_forecast -> tool.get_forecast (input: {"location": "str", "days": "int"}, output: list[dict])
   #     Extensions applied: [OTelExtension, RetryExtension(max=3)]
   ```

7. **Overrides.** Every default is overridable. Users who want fine control can specify:
   ```python
   weather_agent = Agent(
       name="weather-bot",
       topic="custom.weather.in",          # override topic
       group_id="custom-weather-group",     # override consumer group
       reply_topic="custom.weather.out",    # override reply destination
       model="openai:gpt-5.4",
       system_prompt="...",
       tools=[get_weather, get_forecast],
       output_type=WeatherReport,
       max_turns=10,                        # cap on LLM iterations
       output_validators=[validate_no_pii], # post-output checks (Pydantic-AI style)
       deps_type=WeatherDeps,               # Pydantic-AI style typed deps
       on_tool_error="continue",            # or "fail" or a callable
   )
   ```

The agent-level options that map cleanly to Pydantic AI conventions are listed in §4.9. The point is that *defaults* deliver Pydantic-AI-class ergonomics for the simple case, and *escape hatches* are uniformly available.

### 4.8 Real-time fan-out via Agent SDK — the user never sees `Fan`

```python
enrichment_agent = Agent(
    name="enricher",
    model="openai:gpt-5.4",
    system_prompt="Enrich the user record with profile, risk, and history data.",
    output_type=EnrichedRecord,
    parallel_tools=[fetch_profile, fetch_risk, fetch_history],
)
worker.add(enrichment_agent)
```

If the LLM picks all three tools in one turn (or the system_prompt instructs it to), the Agent recipe emits `Fan` automatically. The aggregator (§9) collects the Replies, re-enters the agent with all three tool results visible in the next-turn LLM call. The user never wrote `Fan`. They never touched a `fan_id`. They never had to reason about the compacted aggregator topic. The Layer B power is preserved (an Extension author can intercept the `FanDispatched` event for telemetry); the Layer A user is shielded.

### 4.9 Surface map — Pydantic AI → calfkit Agent SDK

Calfkit's Agent SDK adopts the Pydantic AI conventions where they fit. The intent is that a Pydantic AI user can re-author their agent in calfkit without learning a new mental model — only learning what changes when the agent runs distributed.

| Pydantic AI                          | Calfkit Agent SDK                                | Notes                                                                                              |
|--------------------------------------|--------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `Agent(model=..., system_prompt=...)`| `Agent(model=..., system_prompt=...)`            | Identical surface.                                                                                |
| `@agent.tool`                        | `@tool` (module-level) + `Agent(tools=[...])`    | Tools are module-level so they can be shared across agents and exposed as topics.                  |
| `RunContext[Deps]`                   | `Context[Deps, State]`                           | Same idea; `State` is the per-Run user-typed state surviving across hops.                          |
| `output_type=MyModel`                | `output_type=MyModel`                            | Identical; the recipe synthesizes the output schema.                                              |
| `output_validators=[fn]`             | `output_validators=[fn]`                         | Run after LLM produces output; can request a retry by raising `ModelRetry`.                        |
| `agent.system_prompt(dynamic_fn)`    | `Agent(system_prompt=dynamic_fn)`                | Function form for dynamic prompt construction; receives `Context`.                                |
| `Agent.run(...)` (in-process call)   | `client.invoke("agent.<name>.in", req)` (Kafka)  | The big difference: calfkit dispatches over Kafka. The in-process equivalent for tests is `worker.invoke(...)` (§17.2). |
| Per-call `deps=`                     | `worker.add(agent, deps=...)` (Worker-scoped)    | Per-call deps are an anti-pattern under at-least-once redelivery. Deps live at Worker scope.       |
| Streaming output (`agent.run_stream`)| Not in v1.0                                      | Streaming tokens is out of scope for v1.0; see §1.5.                                              |
| Tool-call retry via `ModelRetry`     | `Agent(on_tool_error="retry")` + `RetryExtension`| Calfkit makes retry an explicit policy at agent level + an SDK-wide Extension.                     |
| Multi-agent (Pydantic AI `delegate`) | `Sub(other_agent, as_tool="...")`                | Sub-agent runs as a separate Kafka-deployed agent; calls become `Call`s to its topic.              |

A user familiar with Pydantic AI should read this table and feel that calfkit's Agent SDK is "Pydantic AI for distributed deployments" — not a different framework.

### 4.10 Head-to-head — "Temporal + Pydantic AI" vs Calfkit

This is the comparison the rest of the doc was missing. If a developer can get the same outcome by running Pydantic AI inside a Temporal activity, calfkit has no reason to exist. Below is what each looks like, and why calfkit's surface is materially smaller for the *same* set of features.

**Goal:** ship an agent with three tools, one of which is implemented in Go, that produces durable, replayable runs, scales horizontally, and supports human-in-the-loop approval.

**Approach 1 — Temporal + Pydantic AI.**

```python
# workflow.py (Python — Temporal workflow)
from temporalio import workflow
from temporalio.common import RetryPolicy
from datetime import timedelta

@workflow.defn
class TradingWorkflow:
    @workflow.run
    async def run(self, req: TradeRequest) -> TradeResult:
        # Pydantic AI agent must run as an activity (workflow code must be deterministic).
        agent_output = await workflow.execute_activity(
            run_pydantic_ai_agent,
            req,
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        if agent_output.requires_approval:
            # HITL: signal-based wait
            approval = await workflow.wait_condition(lambda: self._approval is not None)
            agent_output = await workflow.execute_activity(
                continue_agent_with_approval, agent_output, self._approval,
                start_to_close_timeout=timedelta(minutes=5),
            )

        return agent_output.result

    @workflow.signal
    def submit_approval(self, decision: HumanDecision):
        self._approval = decision


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


# worker.py (Python — Temporal worker)
from temporalio.client import Client
from temporalio.worker import Worker

async def main():
    client = await Client.connect("temporal:7233", namespace="trading")
    worker = Worker(
        client,
        task_queue="trading-workflows",
        workflows=[TradingWorkflow],
        activities=[run_pydantic_ai_agent, continue_agent_with_approval],
    )
    await worker.run()

# Plus: Temporal cluster (3-5 nodes), Temporal UI, Temporal namespace setup,
# Postgres or Cassandra backing for Temporal, plus the Go service deployed
# separately with its own HTTP server, plus a Kafka or HTTP-mesh between the
# Python service and the Go service.
```

**Count:**
- Python LOC (workflow + activities + worker + agent): **~80 lines** of orchestration code, **plus** the tool implementations.
- Go LOC: **~30 lines** of HTTP server boilerplate, **plus** the tool implementation.
- Concepts the developer must learn: Temporal Workflow, Activity, signal, query, retry policy, task queue, Temporal client, Pydantic AI Agent, RunContext, HTTP gateway, Go HTTP server, request/response JSON marshaling.
- Infrastructure components: Temporal cluster (3-5 nodes), Temporal UI, Postgres/Cassandra for Temporal, Pydantic AI runtime, Go HTTP service, HTTP load balancer or service mesh.
- "Wiring code" the developer writes: the HTTP gateway, the JSON marshaling, the Temporal signal plumbing for HITL, the activity timeouts, the Workflow → Activity dispatch.

**Approach 2 — Calfkit.**

```python
# trading_agent.py (Python — calfkit)
from calfkit import Agent, Worker, tool, external_tool, Interrupt
from pydantic import BaseModel

class TradeRequest(BaseModel):
    user_id: str
    natural_language: str

class TradeResult(BaseModel):
    confirmation_id: str
    fill_price: float

class HumanDecision(BaseModel):
    approved: bool

@tool
async def fetch_quote(symbol: str) -> dict:
    """Get the latest market quote."""
    ...

@tool
async def fetch_portfolio(user_id: str) -> dict:
    """Get the user's portfolio."""
    ...

execute_trade = external_tool(
    name="execute_trade",
    description="Submit a buy or sell order.",
    input=BuyOrSellOrder,
    output=TradeConfirmation,
)

trading_agent = Agent(
    name="trader",
    model="openai:gpt-5.4",
    system_prompt="You execute trades on behalf of users.",
    tools=[fetch_quote, fetch_portfolio, execute_trade],
    output_type=TradeResult,
    interrupts={
        "approval": Interrupt(
            when=lambda o: o.requires_approval,
            resume_with=HumanDecision,
        ),
    },
)

if __name__ == "__main__":
    worker = Worker(bootstrap_servers="kafka:9092")
    worker.add(trading_agent)
    worker.run_blocking()
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
- Python LOC: **~35 lines**, including the Agent declaration, tools, and worker bootstrap.
- Go LOC: **~12 lines** (no HTTP server, no JSON marshaling — calfkit-go handles transport and serialization).
- Concepts the developer must learn: Agent, tool, external_tool, Worker, Interrupt. That's it for a hello-world distributed agent. (Five concepts at Layer A; the Layer B concepts are not required for this example.)
- Infrastructure components: Kafka (likely already deployed). No Temporal cluster. No HTTP service mesh. No Postgres for the SDK.
- "Wiring code" the developer writes: zero.

**Where calfkit wins this comparison:**
- ~50% less Python code, ~60% less Go code, with no manual transport.
- No new cluster to operate (Kafka is already present in most environments where this scenario is realistic).
- Cross-language tools are *the wire format*, not an HTTP gateway. Schema synthesis is automatic on both sides.
- HITL is one config block on the Agent, not a Workflow signal + Activity dispatch round-trip.
- Replay/durability is implicit in Kafka. The runs-state topic is the audit log; you don't query Temporal's history.

**Where Temporal still wins:**
- **Strong determinism.** Temporal workflows are deterministic by construction; calfkit does not impose this constraint. For workflows where you need to *guarantee* "this exact sequence of decisions, given the same inputs," Temporal is structurally stronger.
- **Mature operational tooling.** Temporal UI for time-travel debugging is excellent. Calfkit's debugging story leans on Kafka topic inspection + OTel; less polished today.
- **Maturity.** Temporal has been in production at scale for years. Calfkit v1.0 will not have that track record on day one.

**The thesis:** calfkit's reason to exist is the *combination* of (a) Pydantic-AI-tier authoring ergonomics, (b) Kafka-native distributed deployment, (c) cross-language tools as a wire-format primitive, (d) durability that piggybacks on the broker the team already runs. No other system in this space offers that combination. The Agent SDK is the surface that makes (a) true; the runtime layer (§2, §3) is what makes (b)(c)(d) true; the two layers exist together by design.

### 4.11 Layer B reference examples — what the runtime surface looks like

These are illustrative sketches at Layer B (the runtime primitive layer). They are NOT what most users write — for normal agent authoring, see §4.2-§4.5. These exist so that (a) Extension authors and tool authors can see the surface they target, (b) advanced users with non-agent orchestration can see the primitive shape, (c) the Agent SDK's compilation target is visible to anyone curious about what `worker.add(agent)` materializes.

**A handoff at Layer B** — what the Agent SDK's `handoffs=[...]` synthesizes:

```python
from calfkit import handler, Context, Reply, Call

@handler(topic="agent.triage.in", reply=str)
async def triage(ctx: Context, req: UserReq) -> Reply[str] | Call:
    intent = await classify(req.text)
    if intent == "billing":
        return Call(topic="agent.billing.in", payload=BillingReq(text=req.text))
    elif intent == "tech":
        return Call(topic="agent.tech.in", payload=TechReq(text=req.text))
    return Reply("I'm not sure how to help with that.")
```

A handoff is `Call`. The called agent's `Reply` flows back through the call stack. The Agent SDK generates an equivalent Handler when given `handoffs=[billing_agent, tech_agent]`.

**A parallel fan-out at Layer B** — what `parallel_tools=[...]` synthesizes when the LLM selects multiple in one turn:

```python
@handler(topic="enrich.in", reply=EnrichResp)
async def enrich(ctx: Context, req: EnrichReq) -> Fan | Reply[EnrichResp]:
    if ctx.fan_results is None:
        return Fan(
            calls=[
                Call(topic="lookup.profile.in", payload=ProfileReq(req.user_id), key=b"profile"),
                Call(topic="lookup.risk.in",    payload=RiskReq(req.user_id),    key=b"risk"),
                Call(topic="lookup.history.in", payload=HistoryReq(req.user_id), key=b"history"),
            ],
            aggregator=AggregatorSpec(timeout=timedelta(seconds=30), on_partial="error"),
        )
    return Reply(EnrichResp(
        profile=ctx.fan_results[b"profile"],
        risk=ctx.fan_results[b"risk"],
        history=ctx.fan_results[b"history"],
    ))
```

`ctx.fan_results` is `None` on first entry, populated dict on second (the aggregator re-entry). The user never touches the aggregator state directly.

**HITL at Layer B** — what `Agent(interrupts={...})` synthesizes:

```python
@handler(topic="approval.agent.in", reply=ApproveResp)
async def approval_agent(ctx, req: ApproveReq) -> Reply[ApproveResp] | Interrupt:
    if not ctx.resume_payload:
        return Interrupt(
            reason=f"awaiting approval of: {req.proposed_action}",
            resume_topic="approval.agent.resume",
        )
    decision: HumanDecision = ctx.resume_payload
    return Reply(ApproveResp(approved=decision.approved, notes=decision.notes))
```

`ctx.resume_payload` is `None` on first entry, populated on resume. The Handler re-runs from the top on resume; `ctx.once()` (§16) is the escape hatch for non-idempotent side effects that must not duplicate across the resume.

**A custom Extension** is documented in §12 alongside the Extension API. **In-memory dispatcher tests** are documented in §17.2 alongside the testing strategy. Both apply equally to Agent SDK-authored code and Layer B handlers.

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

This layer has two sub-tiers (see §4 for the full discussion):

- **Layer 4A — Agent SDK (the default for most users).** Declarative `Agent(...)` classes, `@tool` decorators, `external_tool(...)` stubs, registered via `worker.add(agent)`. Users at this tier never write a Handler, never see an Action, never name a topic by hand.
- **Layer 4B — Runtime primitives (escape hatch + tool/Extension authors).** Handlers that take `(Context, Msg) -> Action`, Extensions, custom orchestration nodes that aren't agents, raw tool authors. Same surface that the Agent SDK compiles down to.

Both sub-tiers share the same contract with Layer 3:
- Provides Deps via Worker config.
- Does *not* call `broker.publish`. Does *not* serialize/deserialize Envelopes. Does *not* touch `correlation_id` directly.

**Layer 3 (SDK Runtime)**
- Pulls a `ConsumerRecord` from Layer 2; deserializes into `Envelope`; constructs `Context` for the user.
- Resolves the message's *invocation kind* (request, event, tool-reply, resume) from `x-calf-action-kind` and the Handler's binding (an agent's request input topic produces a request-shaped run; an event subscription produces an event-shaped run).
- Calls user Handler via the `around_invoke` Extension chain. For event-triggered Agent runs, the runtime sets `ctx.trigger.kind = "event"` so observers can distinguish.
- Interprets returned Action via the `around_publish` Extension chain.
- Writes to runs-state on `Interrupt`/`RunStarted`/`RunCompleted`.
- Owns the fan-out aggregator entirely. User Handlers never touch aggregator topics.
- **If the Agent declared `publishes=EventType`**: after a Run terminates via `Reply`, the runtime additionally `Emit`s the payload to `agent.<name>.events`. For event-triggered Runs (no waiting caller), this is the sole final-publish; for request-triggered Runs, both the `Reply` to the caller and the `Emit` to the events topic happen, deduped by `(run_id, action_id)`.
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

| Visible at Layer A (Agent SDK) | Visible at Layer B (Runtime) | Internal |
|---------------------------------|------------------------------|----------|
| `Agent`, `tool`, `external_tool`, `Sub`, `handoffs`, `parallel_tools`, `interrupts`, `output_validators`, `publishes`, `subscribes_to=[Subscription(...)]` | `Handler`, `Action`, `Run`, `State`, `Worker` | `Envelope` shape (we may evolve) |
| `worker.add(agent)`, `worker.wire(source=..., target=..., payload=...)`, `worker.inspect()`, `worker.run()` | `@handler` decorator, Action algebra return types | Frame stack, aggregator topic names |
| `Context` API (`ctx.state`, `ctx.deps`, `ctx.run_id`, `ctx.fan_results`, `ctx.resume_payload`, `ctx.trigger`, `ctx.once()`) — shared across tiers | Same `Context` API | Idempotency table |
| Agent-level errors and validators (`ModelRetry`, `output_validators`) | `Extension` base class with three primitives | Header decoding |
| `client.invoke("agent.<name>.in", req)` / `client.publish_event(topic, payload)` / `client.resume` | `LifecycleEvent` discriminated union | Rebalance listener |
| `InMemoryWorker.invoke(agent_name, req)` / `InMemoryWorker.publish_event(topic, payload)` for testing | `InMemoryWorker` accepting raw handlers | Consumer-group naming convention |
| Kafka header *names* (a stable list of reserved `x-calf-*`) | Kafka header names | aiokafka |

---

## 6. Wire format

### 6.1 Envelope shape

```python
class Envelope(BaseModel):
    """Wire format. Stable across calfkit versions starting at v1.0."""
    schema_version: Literal["1.0"]
    action_kind: Literal["call", "reply", "emit", "tailcall", "fan_child"]
    # ^ The Action that produced this envelope. "fan_child" = a single Call within a Fan.
    # ^ "emit" is the choreography envelope — fire-and-forget; no caller awaits a Reply.
    #   Whether a subscribing consumer treats it as "a new request" or "a tap to observe"
    #   is the *subscriber's* concern (subscription configuration), not the publisher's.
    #   Producers do not need to know that subscribers exist.

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

**Call:**

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

**Reply (with state propagated back):**

```json
{
  "schema_version": "1.0",
  "action_kind": "reply",
  "run_id": "01933a2c-...",
  "correlation_id": "01933a2c-...",
  "frame_id": "01933a2d-...",
  "parent_frame_id": null,
  "state_schema": "myapp.OrderState",
  "state": "eyJ1c2VyX2lkIjoidTEiLCJjbGFzc2lmaWNhdGlvbiI6InNwYW0ifQ==",
  "payload_schema": "myapp.ClassifyResp",
  "payload": "eyJsYWJlbCI6InNwYW0ifQ==",
  "call_stack": []
}
```

**Event (`Emit` from an Agent with `publishes=NewsArticle`):**

```json
{
  "schema_version": "1.0",
  "action_kind": "emit",
  "run_id": "01933a2c-...",
  "correlation_id": "01933a2c-...",
  "frame_id": "01933a31-...",
  "parent_frame_id": null,
  "state_schema": null,
  "state": "",
  "payload_schema": "myapp.events.NewsArticle",
  "payload": "eyJpZCI6IjEyMyIsImhlYWRsaW5lIjoiLi4uIn0=",
  "call_stack": []
}
```

Note `call_stack: []` — an event has no caller, so there is no frame to pop on completion. The headers carry `x-calf-event-type=myapp.events.NewsArticle` and `x-calf-producer-agent=news-watcher` for cheap broker-level inspection. Subscribers receive this envelope and create their own Run when reacting; the original `run_id` is propagated as `parent_run_id` on the new Run (see §11.B for the full lineage model).

**Fan child (one of N children of a Fan):**

```json
{
  "schema_version": "1.0",
  "action_kind": "fan_child",
  "run_id": "01933a2c-...",
  "correlation_id": "01933a2e-...",
  "frame_id": "01933a2f-...",
  "fan_id": "01933a30-...",
  "fan_index": 0,
  "fan_total": 3,
  "fan_aggregator_topic": "calf.fanout-agg.enrich",
  "payload_schema": "myapp.ProfileReq",
  "payload": "..."
}
```

The aggregator topic is in the envelope so the reply target knows where to send its Reply (the aggregator subscribes to that topic, indexed by fan_id).

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

Justification for (C): the partitioner sends the Fan's child publishes with key=`run_id`, and the aggregator subscription uses key=`fan_id`. We need `fan_id`-keyed delivery for aggregation, so the aggregator topic is partitioned by `fan_id`. The dispatching Worker subscribes to all partitions of `calf.fanout-agg` it doesn't yet own (lazy). Actually, the cleaner design is:

- The aggregator topic is its own topic, with its own partitioning (key=`fan_id`).
- Every Worker process subscribes to it, in a single consumer group `calf-fanout-agg-<cluster-id>`.
- Each Worker handles whichever partitions it gets assigned by Kafka — the dispatching Worker is *not* guaranteed to be the one that aggregates. That's fine because the aggregator is stateless beyond what's in the topic — the message stream is the state.

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

### 10.1 Tools are Kafka topics

```python
# The natural form — what 90% of tool authors write
@tool
async def fetch_quote(symbol: str) -> dict:
    """Fetch the latest market quote for a symbol."""
    ...
```

The decorator derives everything from the function:
- Topic: `tool.fetch_quote` (from the function name; override via `@tool(topic=...)`).
- LLM-facing name and description: function name + docstring.
- Input JSON schema: synthesized from parameter types (Pydantic-AI-style).
- Output JSON schema: synthesized from the return type.

For explicit control, the long form is available:

```python
@tool(topic="tool.fetch_quote", description="Fetch live quote", input=QuoteReq, output=QuoteResp)
async def fetch_quote(ctx: Context, req: QuoteReq) -> Reply[QuoteResp]:
    ...
```

A Tool is a Handler with two extra things:
1. **A JSON schema** for the LLM (auto-synthesized from parameter types + docstring + name).
2. **Discoverability** — the SDK can register the tool with a tool catalog (an SDK-provided component or external service like a tool registry topic).

Tools are normal Handlers under the hood. They live in a Worker, subscribed to their input topic, and they `Reply` with their output. The short and long forms produce identical runtime behavior; the long form simply exposes the knobs.

**Tools are orchestration-only.** A tool exists to be called by an agent (`Call` → `Reply`). Tools are not subscribers — they do not have `subscribes_to=`. If you want a long-running reactive component (e.g., "every time a transaction is completed, run this function"), use an Agent with `subscribes_to=[...]` and `react=` (§11.B.7), not a tool. The distinction: tools satisfy the LLM's tool-call request; choreography subscribers react to events the LLM never asked about.

### 10.2 Cross-language tools

```python
# Declare a tool that lives in another service/language
execute_trade = external_tool(
    name="execute_trade",
    description="Submit a buy or sell order; returns confirmation id.",
    input=BuyOrSellOrder,    # Pydantic model — schema synthesized for the LLM
    output=TradeConfirmation,
    # topic is derived: "tool.execute_trade" (override via topic=...)
)
```

There's no Python body. The Pydantic models on both sides drive schema synthesis; the wire format is shared across calfkit's language SDKs. A Go service subscribed to `tool.execute_trade` with the calfkit-go SDK (eventual; v1.x roadmap) publishes Replies in the same Envelope format. The Python agent never knows or cares it was a Go tool. For users who'd rather not import Pydantic models from a shared contracts package, raw JSON schemas are also accepted: `external_tool(name=..., input_schema={...}, output_schema={...})`.

This is the **moat** vs in-process SDKs (LangGraph, Pydantic AI, OpenAI Agents). They cannot do this without an FFI layer or an HTTP gateway. See §4.3 for a worked example and §4.10 for the head-to-head against "Temporal + Pydantic AI" where this cross-language story is concrete.

### 10.3 Tool catalog and discovery

For v1.0: tools register at Worker startup; the catalog is a per-Worker registry. Agents pass tool references explicitly.

For v1.1+: a topic-based catalog where tools publish their schema to `calf.tools.catalog` on startup; agents subscribe and discover. This is a clear evolution path, not a v1.0 requirement.

### 10.4 LLM-facing schema synthesis

```python
# From this:
@tool(topic="tool.fetch_quote")
async def fetch_quote(ctx: Context, req: QuoteReq) -> Reply[QuoteResp]:
    """Fetch the latest market quote for a symbol."""

# Synthesize this for the LLM (OpenAI function-calling format):
{
  "name": "fetch_quote",
  "description": "Fetch the latest market quote for a symbol.",
  "parameters": <JSON Schema of QuoteReq>
}
```

`ctx` is dropped — invisible to the model, populated by the SDK.

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
| **Producer knows consumer?** | Yes — caller names the callee's topic   | No — producer emits; subscribers wire themselves    |
| **Wiring location**      | Inside the caller's `Agent(handoffs=[...], tools=[Sub(...)], parallel_tools=[...])` | Inside the consumer's `Agent(subscribes_to=[...])` or at `worker.wire(...)` |
| **Reply expected?**      | Yes — `Reply` flows back through the call stack | No — `Emit` is fire-and-forget; consumers may emit their own events downstream |
| **Coupling**             | Strong — caller breaks if callee is renamed / removed | Loose — producer doesn't know consumers; topic is the contract |
| **Run lineage**          | Single Run across all hops (sub-Runs are explicit opt-in) | Each consumer reaction starts a new Run with `parent_run_id` set |
| **Failure containment**  | A failed callee fails the parent's frame; bubbles to caller | A failed consumer fails only that consumer's Run; producer is unaffected |
| **Adding a consumer**    | Requires editing the caller / Worker     | Add a new agent with `subscribes_to=[...]`; no producer change |
| **Canonical Action**     | `Call` / `Reply` / `Fan`                 | `Emit` + Subscription                                |
| **When to use**          | "I need an answer from B to continue."   | "Whenever X happens, run B (and C and D) independently." |

All multi-agent patterns — both flavors — are recipes over the Action algebra. None require new wire-level Actions. At the Agent SDK tier (§4), they are declarative one-liners; at the runtime tier, they compile to `Emit`/`Call`/`Reply`/`Fan` Actions plus Subscription bindings.

---

## 11.A Orchestration patterns

In all of these, the caller explicitly names the callee. Replies flow back through the call stack.

### 11.A.1 Handoff (OpenAI Agents-style)

```python
# Layer A — what users write
triage_agent = Agent(
    name="triage",
    model="openai:gpt-5.4",
    system_prompt="Route to the right specialist.",
    handoffs=[billing_agent, tech_agent],
    output_type=str,  # default: direct answer if no handoff
)

# Layer B — what this compiles to (illustrative)
@handler(topic="agent.triage.in", reply=str)
async def _triage_synth(ctx, req) -> Reply[str] | Call:
    if needs_billing(req):
        return Call(topic="agent.billing-agent.in", payload=req)
    return Reply("...")
```

A handoff is `Call`. The called agent's `Reply` flows back through the stack to the original publisher. No special "Handoff" type needed.

### 11.A.2 Supervisor-Worker

```python
# Layer A
supervisor_agent = Agent(
    name="supervisor",
    model="openai:gpt-5.4",
    parallel_tools=[Sub(worker_a, as_tool="ask_a"), Sub(worker_b, as_tool="ask_b")],
    output_type=Summary,
)

# Layer B — what this compiles to (illustrative)
@handler(topic="agent.supervisor.in", reply=Summary)
async def _supervisor_synth(ctx, req) -> Fan | Reply[Summary]:
    if ctx.fan_results is None:
        return Fan(calls=[Call("agent.worker-a.in", req, key=b"a"),
                          Call("agent.worker-b.in", req, key=b"b")])
    return Reply(combine(ctx.fan_results[b"a"], ctx.fan_results[b"b"]))
```

Supervisor = an Agent (or Handler) that fans out and aggregates. The aggregator (§9) does the heavy lifting.

### 11.A.3 Parallel sub-agents

Same shape as supervisor — `Fan` over Calls to sub-agent topics. Sub-agents may recursively fan out. Expressed as `parallel_tools=[...]` in the Agent SDK.

### 11.A.4 Pipeline (sequential orchestration)

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

Each stage `Call`s the next. The `Reply` from stage3 unwinds through stage2, stage1, back to the client. For one-way pipelines that don't need replies, use `Emit` instead of `Call`.

Pure-data pipelines are the canonical Layer B case: there's no LLM, no tool catalog, no system_prompt — `@handler` is the natural surface, not `Agent`. For the same shape under choreography semantics — where each stage emits an event and the next stage subscribes — see §11.B.1.

### 11.A.5 Hybrid: hierarchical with handoffs and fan-outs

Combine the above. A triage Agent handoffs to a workflow Agent that uses `parallel_tools` to a set of validator sub-agents whose Replies flow back through the chain. No new primitives required; just declarative Agent composition (or, equivalently, topic wiring at Layer B).

---

## 11.B Choreography patterns

In choreography, producers don't know who consumes their events. Consumers declare subscriptions, optionally with filters and schema typing. The wiring lives outside the producer.

This is the canonical Kafka pattern. Calfkit exposes it through three Layer A constructs and one Layer B equivalent:

| Construct                                  | Layer | Purpose                                                                                   |
|--------------------------------------------|-------|-------------------------------------------------------------------------------------------|
| `Agent(publishes=EventType)`               | A     | Declares the agent's *output event stream*. After a Run terminates, the runtime emits the final payload to `agent.<name>.events`. |
| `Agent(subscribes_to=[Subscription(...)])` | A     | Declares the agent's *reactive entrypoints*. Each Subscription is an independent Kafka subscription that triggers a new Run. |
| `worker.wire(source=..., target=..., payload=...)` | A     | Deployment-time choreography wiring — attaches a Subscription to a target agent without modifying its declaration. |
| `Emit(topic=..., payload=...)` + `@handler(topic=...)` | B     | The bare-bones Layer B equivalent — produce events with `Emit`, consume by registering a handler on the topic. |

### 11.B.1 Linear choreography — B always consumes A's output stream

The simplest pattern. Identical reach to a sequential pipeline, but the wiring is at the consumer side. Adding agent C as another consumer of A doesn't touch A.

```python
classifier = Agent(
    name="classifier",
    model="openai:gpt-5.4",
    system_prompt="Classify the input.",
    output_type=Classified,
    publishes=Classified,
)

enricher = Agent(
    name="enricher",
    model="openai:gpt-5.4",
    system_prompt="Enrich the classified record with external data.",
    output_type=Enriched,
    subscribes_to=[Subscription(source=classifier, payload=Classified)],
    publishes=Enriched,            # forms a chain — anyone else can subscribe to enricher
)

scorer = Agent(
    name="scorer",
    model="openai:gpt-5.4",
    system_prompt="Score the enriched record's risk.",
    output_type=Scored,
    subscribes_to=[Subscription(source=enricher, payload=Enriched)],
)
```

Three agents, three independent Runs per input. Each agent owns its own Run lifecycle, retry policy, observability. The producer side is mute — `classifier` doesn't know `enricher` exists.

**Layer B equivalent** (illustrative; this is what the Agent SDK compiles to under the hood):

```python
# Producer side — emit on terminal Reply
@handler(topic="agent.classifier.in", reply=Classified)
async def _classifier_synth(ctx, req) -> Reply[Classified]:
    out = await classifier_recipe.run(ctx, req)
    # The Reply also produces an Emit to agent.classifier.events because publishes=Classified.
    return Reply(out)

# Consumer side — subscribed handler
@handler(topic="agent.classifier.events", reply=None)
async def _enricher_event_handler(ctx, event: Classified) -> Emit:
    out = await enricher_recipe.run(ctx, event)
    return Emit(topic="agent.enricher.events", payload=out)
```

Note the Layer B form makes the trade-off visible: the consumer's handler returns `Emit` instead of `Reply` because there is no caller awaiting. Layer A hides this — the user just sets `publishes=`.

### 11.B.2 Pub/sub fan-out — N independent consumers of one producer

One producer, N reactive consumers, each with their own consumer group (so each sees every event).

```python
watcher = Agent(name="news-watcher", ..., publishes=NewsArticle)

summarizer = Agent(name="summarizer", ...,
    subscribes_to=[Subscription(source=watcher, payload=NewsArticle)])

sentiment = Agent(name="sentiment", ...,
    subscribes_to=[Subscription(source=watcher, payload=NewsArticle)])

archiver = Agent(name="archiver", ...,
    subscribes_to=[Subscription(source=watcher, payload=NewsArticle)])
```

Three independent consumer groups on `agent.news-watcher.events`. Each agent independently scales (`num_partitions × num_consumers`); a slow `summarizer` does not back-pressure `sentiment` or `archiver`. Adding a fourth consumer is a new file, a new Worker entry, zero producer changes.

### 11.B.3 Filtered subscription

Filters run *before* the LLM is invoked. Non-matching events are acknowledged and skipped without LLM cost. The filter must be pure (no I/O — the runtime calls it synchronously inside the dispatch path).

```python
alerter = Agent(
    name="alerter",
    model="openai:gpt-5.4",
    system_prompt="Compose and send an alert for an urgent article.",
    output_type=AlertSent,
    subscribes_to=[
        Subscription(
            source=watcher,
            payload=NewsArticle,
            filter=lambda ev: ev.urgency >= 4,
        ),
    ],
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

audit_agent = Agent(
    name="audit",
    subscribes_to=[
        Subscription(
            topic="events.transactions.completed",
            payload=TransactionCompleted,
        ),
    ],
    output_type=AuditEntry,
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

alerter = Agent(
    name="alerter",
    subscribes_to=[Subscription(payload=NewsArticle)],   # no topic needed
)
```

Reasonable convention for the implicit form: `event.<module>.<TypeName>`. We don't recommend this for v1.0 because cross-package import cycles get awkward; explicit `topic=` or `source=` is clearer. The schema-only form is documented but not the default.

### 11.B.6 Deployment-time wiring (`worker.wire(...)`)

Agents that should be reusable across deployments shouldn't hard-code their subscriptions. Use `worker.wire(...)` to attach Subscriptions at the deployment site:

```python
# shared_agents.py — library, no deployment specifics
classifier = Agent(name="classifier", ..., publishes=Classified)
enricher   = Agent(name="enricher",   ...)
archiver   = Agent(name="archiver",   ...)

# main.py — deployment composition
worker = Worker(...)
worker.add(classifier)
worker.add(enricher)
worker.add(archiver)

worker.wire(source=classifier, target=enricher, payload=Classified)
worker.wire(source=classifier, target=archiver, payload=Classified)
worker.wire(source=enricher,   target=archiver, payload=Enriched, filter=lambda e: e.flagged)

worker.run_blocking()
```

`worker.wire(...)` is **the canonical form for production deployments** — it puts choreography topology in one inspectable place rather than scattered across N agent files. The in-agent `subscribes_to=[...]` form is fine for hello-world and tightly-coupled agents, but the `worker.wire` form scales.

`worker.inspect()` prints the wired topology so users can confirm the deployment looks how they expect.

### 11.B.7 The `Subscription` declaration in full

```python
@dataclass
class Subscription:
    # Source — exactly one of:
    source: Agent | None = None             # subscribe to this agent's events topic
    topic: str | None = None                # subscribe to this raw Kafka topic
    # (Implicit: the topic = source.events_topic if source is set.)

    payload: type[BaseModel]                # Pydantic model for decoding the envelope payload

    # Optional reaction shape (default: each event starts a new Agent Run):
    react: Callable[[Context, Payload], Awaitable[Action]] | None = None
    # ^ If set, the SDK calls this function instead of running the Agent's LLM loop.
    #   Return value is a normal Action: Call/Emit/Done/Fail/etc. Use Done() to skip the event.
    #   This is the escape hatch for "react to events without invoking the LLM."

    # Optional pure filter (predicate; runs before react/LLM):
    filter: Callable[[Payload], bool] | None = None

    # Optional Subscription overrides:
    group_id: str | None = None             # override consumer group naming
    start_from: Literal["latest", "earliest"] = "latest"
    # ^ At Subscription registration, where to start reading. Default "latest" (most agents
    #   only care about new events). "earliest" replays history — useful for archivers and
    #   backfill agents.
    max_concurrency: int | None = None      # per-Subscription concurrency cap

    # Optional cross-tenant escape:
    cross_tenant: bool = False              # if True, drop the default tenant filter
                                            # (see §11.B.9)
```

This is the full Subscription shape. Common cases use only `source` + `payload`; advanced cases use the rest.

### 11.B.8 Consumer-loop semantics — what does the LLM see?

When an event arrives on a subscription, three things can happen, in order of evaluation:

1. **Filter (if any) is evaluated.** Pure synchronous predicate. If False → ack, skip, emit `EventSkipped` lifecycle event. No LLM call.
2. **`react=` function (if any) is called.** Async function, returns an Action. The user can do anything — call the LLM via `ctx.llm(...)`, return `Done()` to ack-and-skip, return `Call(topic=...)` to invoke a tool or another agent, return `Emit(topic=...)` to publish a derived event. The LLM is NOT invoked unless the react function calls it.
3. **Default (no react): the agent's LLM is invoked with the event as the user-message.** The agent's `system_prompt`, `tools`, and `output_type` are used identically to a request-triggered run. The event payload becomes the LLM's user-role content. The final answer is published to `agent.<name>.events` (if `publishes=` set), and the Run terminates without a `Reply`.

**Three alternatives I considered and rejected for v1:**

- **"Each Subscription has its own system prompt."** Rejected — bloats the Subscription model and incentivizes users to write subscription-specific prompts that diverge from the agent's primary purpose. If you need a fundamentally different prompt per source, that's a different agent.
- **"The event is treated as a tool result."** Rejected — a tool result is semantically a response to a Call the agent made. An incoming event was not solicited; pretending it's a tool result confuses the LLM and breaks the message-history shape.
- **"Subscriptions only do raw reactions; never invoke the LLM."** Rejected — that pushes "agent reacts to an event by running its agent loop" (the common case) into user code, which defeats the Agent SDK's reason to exist. Default to LLM invocation; offer `react=` for the escape hatch.

The chosen model: **events default to invoking the LLM like a user message; `react=` is the escape hatch.** This is what users expect from an "Agent that subscribes to X" — they get an agent reacting to X.

`ctx.trigger` exposes the origin to user code that needs it: `ctx.trigger.kind in {"request", "event", "resume"}`; for events, `ctx.trigger.source_topic`, `ctx.trigger.source_agent`, `ctx.trigger.event_type` are populated. Extensions and `on_event` observers can use this for cardinality-safe OTel attributes.

### 11.B.9 Run lineage across event boundaries

When an event from one agent triggers another agent's Run, the two Runs are *distinct* — each has its own RunId, its own runs-state record, its own lifecycle. They are linked via `parent_run_id`:

- The producer's Envelope has `run_id = X`.
- When the consumer's Worker dispatches the event, it mints a new `RunId = Y` for the consumer's Run, and sets `parent_run_id = X` on the new Run.
- Observability tooling (OTel, runs-state UI) can roll up child Runs under the parent.

**Why distinct Runs, not a continuation of the producer's Run:**
- The producer's Run terminates when it `Reply`s (or `Emit`s, in choreography). The consumer reacts asynchronously — possibly much later, possibly never. There is no single end-to-end Run that "completes" only after every downstream consumer is done.
- Fault isolation: a failed consumer must not retroactively fail the producer's Run.
- Fan-out semantics: with N consumers, "the Run" is ambiguous. Distinct Runs with parent linkage scales to N consumers cleanly.

This is the **opposite** of orchestration's `Call` semantics, where the same RunId carries through every hop. The Run-lineage choice is what makes orchestration vs choreography structurally distinct.

### 11.B.10 Multi-tenancy and cross-tenant subscriptions

By default, an event subscription is **tenant-scoped**: a calfkit consumer only reacts to events that carry the same `x-calf-tenant-id` as the consumer's Worker (the Worker has a default tenant ID; or, in multi-tenant deployments, the consumer is registered per-tenant). Cross-tenant events are filtered at the dispatch layer before the filter/react/LLM stages.

This is the safe default. It prevents accidental cross-tenant data flow when a developer builds a Subscription against an internal topic and forgets that the topic carries multi-tenant traffic.

**Opting in to cross-tenant subscriptions:** `Subscription(..., cross_tenant=True)`. The agent receives events from all tenants and is responsible for handling that — typically by sharding by `ctx.trigger.tenant_id` or by being explicitly tenant-agnostic (an audit agent that logs every tenant's events).

When the producer and consumer are in different tenants but the same calfkit deployment (rare), this is the only escape hatch.

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
# Orchestration: caller → triage → billing/tech
billing_agent = Agent(name="billing", ..., output_type=BillingAnswer,
                      publishes=BillingAnswer)   # also emits an event stream
tech_agent    = Agent(name="tech",    ..., output_type=TechAnswer,
                      publishes=TechAnswer)
triage_agent  = Agent(name="triage",  ...,
                      handoffs=[billing_agent, tech_agent])

# Choreography: analytics and archive react to every answer
analytics = Agent(
    name="analytics",
    system_prompt="Score the quality of the support answer.",
    output_type=QualityScore,
    subscribes_to=[
        Subscription(source=billing_agent, payload=BillingAnswer),
        Subscription(source=tech_agent,    payload=TechAnswer),
    ],
)

archive = Agent(
    name="archive",
    react=lambda ctx, ev: Emit(topic="archive.s3.dump", payload=ev),  # no LLM call
    subscribes_to=[
        Subscription(source=billing_agent, payload=BillingAnswer),
        Subscription(source=tech_agent,    payload=TechAnswer),
    ],
)
```

The customer-facing path is orchestration (triage handoff to billing/tech is synchronous request/reply over Kafka). The post-conversation analytics and archival pipelines are choreography — they react to every answer without the triage agent knowing they exist. Adding a fourth observer (a fraud-detection agent that runs on billing answers only) is one new file with no changes to the support path.

This is the realistic mixed shape calfkit is designed for. Pure orchestration is a subset (no `publishes`, no `subscribes_to`). Pure choreography is a subset (no `handoffs`, no `tools` with `Sub(...)`).

---

## 12. Extension / hook system

This section supersedes `docs/hooks-design.md`.

### 12.1 Why three primitives, not a hierarchy

The previous proposal in `docs/hooks-design.md` (sections 5.2 and below) introduced `NodeMiddleware` + `AgentMiddleware` with named-sugar methods: `before_handler`, `after_handler`, `before_model`, `after_model`, `on_tool_dispatch`, `on_tool_return`, `before_agent_run`, `after_agent_run`. That design has three structural problems:

1. **It lies about cross-process boundaries.** `before_agent_run` and `after_agent_run` fire in *different Handler invocations*. In calfkit, that means different Kafka messages, possibly different Worker processes, definitely different Python interpreter instances. A user writing one class with both methods naturally assumes Python state survives between them — `self.start_time = time.time()` in `before_agent_run`, `elapsed = time.time() - self.start_time` in `after_agent_run` — but the *second invocation* of the middleware class is a fresh instance in a fresh process. The named-sugar API doesn't make this visible.

2. **It conflates two distinct boundaries** (`on_tool_dispatch` in-process vs `on_tool_return` cross-process) under one mental model.

3. **It has registration-order subtleties.** `before_*` runs in forward order, `after_*` in reverse order, `on_error` in reverse order. Multiple rules to remember.

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
worker = Worker(
    handlers=[h1, h2, h3],
    extensions=[
        OTelExtension(),         # outermost
        RetryExtension(max=3),
        RateLimitExtension(qps=100),
        AuditExtension(topic="audit.events"),  # innermost
    ],
)

# Or per-handler:
worker.attach(handler=h1, extensions=[H1OnlyExtension()])
```

Worker-level extensions wrap every Handler. Per-Handler extensions wrap only that Handler, inside the Worker-level chain. (Order: worker-outer → worker-inner → handler-outer → handler-inner → user code, then back out.)

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

**Audit extension:**

```python
class AuditExtension(Extension):
    def __init__(self, topic: str): self.topic = topic

    async def on_event(self, event):
        # Emit one audit record per event, idempotent at the audit consumer
        await self._publish_audit(self.topic, asdict(event))
```

**Rate limiting:** see §4.6.

**Tool filtering** (the LangGraph `wrap_tool_call` equivalent):

```python
class ToolFilter(Extension):
    def __init__(self, allow: set[str]): self.allow = allow

    async def around_publish(self, req, call_next):
        if req.action_kind == "call" and req.target_topic.startswith("tool."):
            tool_name = req.target_topic.removeprefix("tool.")
            if tool_name not in self.allow:
                raise ToolNotAllowed(tool_name)
        return await call_next(req)
```

### 12.9 Testing extensions

```python
async def test_rate_limit_blocks_overage():
    wrk = InMemoryWorker(handlers=[my_handler], extensions=[RateLimit({"my.in": 1.0})])
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
async def trade(ctx: Context[TradingDeps, MyState], req: TradeReq) -> Reply[str]:
    quote = await ctx.deps.coinbase.get_quote(req.symbol)
    ...
```

The Handler is parameterized on `Context[DepsT, StateT]`. The Worker is configured with the actual deps:

```python
worker = Worker(
    handlers=[trade],
    deps=TradingDeps(
        db=session_factory(),
        coinbase=CoinbaseClient(key=os.getenv("CB_KEY")),
        feature_flags=load_flags(),
    ),
)
```

### 13.2 Lifecycle

- Deps are **instantiated at Worker startup** (synchronous or async via `Worker.deps_factory`).
- Deps are **scoped to the Worker process**, not per-Handler-invocation. (Per-invocation deps are an anti-pattern — they don't survive replays.)
- For per-invocation context (current Run's tenant, current user_id), use the State, not Deps.

```python
worker = Worker(
    handlers=[...],
    deps_factory=lambda: TradingDeps(db=await create_pool()),
)
```

### 13.3 Testing deps swap

```python
async def test_trade_with_mock_coinbase():
    wrk = InMemoryWorker(handlers=[trade], deps=TradingDeps(
        db=mock_db(),
        coinbase=MockCoinbase(),
        feature_flags={},
    ))
    result = await wrk.invoke("trade.in", TradeReq(...))
    assert result.output == "..."
```

Deps swap is trivial because the Handler signature is generic on `DepsT`. The Worker just passes whichever Deps it was constructed with.

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

Test a Handler in pure isolation: no Kafka, no broker, no Extensions, no runtime.

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

### 17.2 Layer 2 — In-memory dispatcher

`InMemoryWorker` simulates the full Worker runtime without a broker. Topics become in-process queues; the Extension chain runs; the aggregator works; the idempotency table works (in-memory backing).

```python
async def test_multi_agent_pipeline_in_memory():
    wrk = InMemoryWorker(
        handlers=[triage, billing, tech],
        extensions=[OTelExtension(), RetryExtension(max=2)],
    )
    result = await wrk.execute("triage.in", UserReq(text="my bill is wrong"))
    assert result.output.startswith("I'll help you")
    assert wrk.published("billing.in", count=1)
    assert wrk.published("tech.in", count=0)
```

**What it tests:** multi-handler integration. Extension chain composition. Fan-out aggregation. Idempotency. Run lifecycle events. Most assertions about wiring.

**Choreography testing pattern** — assert that a published event triggers a subscribed Agent:

```python
async def test_pubsub_fanout_in_memory():
    wrk = InMemoryWorker(handlers=[summarizer, sentiment, archiver])
    article = NewsArticle(id="1", headline="Test", body="...", urgency=4)
    await wrk.publish_event("agent.news-watcher.events", article)
    await wrk.drain()  # let all triggered runs complete
    assert wrk.runs_for_agent("summarizer") == 1
    assert wrk.runs_for_agent("sentiment") == 1
    assert wrk.runs_for_agent("archiver") == 1
    # Each consumer saw the event independently (pub/sub fan-out).
```

**What it doesn't:** real Kafka behavior — partition rebalances, broker downtime, consumer-group dynamics, real-network latency, real serialization (it uses Python objects directly, optionally with a flag to force round-trip).

**Critical design choice:** `InMemoryWorker` IS the same runtime class as `Worker`. The difference is the transport adapter — `KafkaTransport` vs `MemoryTransport`. This means the in-memory path is a real subset of production behavior, not a parallel implementation.

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

- **L1** for "did I write this Handler correctly?" — sub-millisecond.
- **L2** for "do my Handlers, Extensions, and Actions compose?" — single-digit milliseconds.
- **L3** for "does this survive Kafka shenanigans?" — testcontainers, multi-second.

CI runs all three. Pre-commit runs L1+L2.

---

## 18. Deployment / runner

### 18.1 What a calfkit Worker process looks like

```python
# worker_main.py
from calfkit import Worker
from myapp.handlers import triage, billing, tech
from myapp.extensions import OTelExtension, RetryExtension

async def main():
    wrk = Worker(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"],
        handlers=[triage, billing, tech],
        extensions=[
            OTelExtension(endpoint=os.environ["OTEL_ENDPOINT"]),
            RetryExtension(max=3),
        ],
        deps_factory=create_deps,
        group_id_prefix="myapp-prod",
        # Optional config:
        partition_strategy="run_id",
        dedup="on",
        runs_state_topic="calf.runs-state",
        fanout_topic="calf.fanout-agg",
        dlq_topic_pattern="calf.dlq.{source}",
    )
    await wrk.run()  # blocks until SIGTERM

if __name__ == "__main__":
    asyncio.run(main())
```

### 18.2 Configuration

Three layers, in precedence order:
1. **Programmatic** (`Worker(...)` kwargs)
2. **Environment variables** (`CALFKIT_KAFKA_BOOTSTRAP`, `CALFKIT_OTEL_ENDPOINT`, etc.)
3. **Config file** (`calfkit.yaml`, opt-in)

The SDK ships a `WorkerConfig` Pydantic model that's the source-of-truth schema; env vars and config file map to it. Programmatic args override.

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

Health check endpoint optional via `Worker(health_check_port=8080)`. By default, the Worker has no HTTP surface — it's pure Kafka.

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
- `agent_tool` → `@tool`
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
| `ToolNodeDef` | `calfkit/nodes/tool.py:26` | `@tool` decorator |
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
| `ToolContext(RunContext[Deps])` from vendored pydantic_ai | `models/tool_context.py:8` | `Context[Deps, State]` SDK-owned, no vendored dependency |
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
| **Primary authoring surface** | `Agent(name, model, system_prompt, tools, output_type, ...)` — Pydantic-AI-class DX. Handler/Action is the runtime layer below. | Graph DSL (`add_node`/`add_edge`) | `@workflow.defn` + `execute_activity` (must be deterministic) | Service handlers (`@restate.service`) | `Agent(...)` (in-process) | `Agent(...)` (in-process) | `step.run(...)` |
| **Execution shape**      | Distributed, one Node per Kafka hop | In-process graph | Workflow + Activity (distributed via SDK) | Distributed handlers (durable invocation) | In-process agent | In-process agent | Function steps (cloud or self-host) |
| **Durable execution**    | Yes (Kafka log + compacted topics) | Optional Postgres checkpointer | Yes (Temporal cluster) | Yes (Restate runtime) | No | No | Yes (Inngest store) |
| **Required runtime infra** | Kafka only | None (or Postgres) | Temporal cluster | Restate runtime | None | None | Inngest service (or self-host) |
| **Graph DSL**            | No (topology emergent)       | Yes (`add_node` / `add_edge`)| No (workflow code is plain) | No (handler code is plain) | No (orchestrator code) | No | No (`step.run` etc.) |
| **Hook surface**         | 3 primitives (`around_invoke`, `around_publish`, `on_event`) | `before_*`/`after_*`/`wrap_*` middleware | Interceptors (4 axes) | Middleware | `RunHooks`/`AgentHooks` | `@tool(prepare=...)`, `system_prompt` | Middleware |
| **Tool model**           | First-class Kafka topic; cross-language native | In-process Python function | Activity (cross-language via SDK) | Service handler | In-process Python | In-process Python | In-process |
| **Cross-language tools** | Yes, by construction         | No                            | Yes via SDKs                   | Yes via SDKs                     | No                         | No                         | TS-focused; some other-lang   |
| **Multi-tenancy**        | Header default, topic opt-in | App-level                    | App-level + namespaces          | App-level                        | App-level                  | App-level                  | App-level                     |
| **Orchestration**        | Yes (`handoffs`, `Sub`, `parallel_tools`) | Yes (`add_edge` / `add_node`) | Yes (`execute_activity`, child workflows) | Yes (handler calls)        | Yes (handoffs)             | Yes (delegate)             | Yes (`step.invoke`)            |
| **Choreography (pub/sub agents)** | **First-class** (`publishes=`, `subscribes_to=`, `worker.wire`) — see §11.B | App-level (no native pub/sub of agent outputs) | App-level (signals are point-to-point, not topic-broadcast) | App-level                        | App-level                  | App-level                  | App-level (events feature, but no agent-output streams) |
| **Type safety**          | Pydantic generics (`Context[Deps, State]`) | TypedDicts | Per-SDK | Per-SDK | Pydantic | Pydantic | TS                |
| **Determinism constraint** | None (any Python in handlers) | None | Strict (workflows must be deterministic) | None | None | None | Steps must be idempotent |
| **HITL**                 | Native (`Interrupt`/`Resume`)| Some support (interrupts, breakpoints) | Native (Signal, query) | Native (awakeable) | Limited                   | Limited                   | Native (`step.waitForEvent`) |
| **Deployment**           | Worker = Kafka consumer process | Run anywhere                  | Worker + Temporal cluster      | Worker + Restate runtime         | Inline                     | Inline                     | Inngest functions             |
| **Open source runtime**  | Yes (Kafka is OSS)           | Yes (lib only)                | Yes                            | Yes                              | Yes (lib only)             | Yes (lib only)             | Open client, server is closed (or partial) |

**Where Calfkit wins:** Pydantic-AI-class authoring DX *combined with* durable, multi-language, zero-extra-cluster deployment (Kafka is likely already present), *combined with* first-class choreography (agents subscribe to event streams from other agents or arbitrary Kafka topics — see §11.B). No other system in this matrix offers that triple. The in-process SDKs (Pydantic AI, OpenAI Agents, LangGraph) cannot do durable + multi-language. The durable-execution platforms (Temporal, Restate, Inngest) treat agent flows as orchestration only — they have no native concept of "subscribe an agent to a topic the way Kafka subscribers do." Calfkit is the only SDK in the space whose primary abstraction is "an agent is a Kafka consumer," which makes choreography the natural shape, not a bolted-on capability.

**Where Calfkit loses:** in-process simplicity for non-distributed agents (LangGraph, Pydantic AI), strong determinism (Temporal), TS-native (Inngest/Trigger), operational maturity / time-travel debugging UI (Temporal).

The pitch: "Author your agent in Pydantic AI's idiom. Deploy it over Kafka as a distributed service with cross-language tools, durable runs, and zero wiring code. If you have Kafka, this is the only SDK in this space that gives you both ergonomics tiers in one package." See §4.10 for the head-to-head against "Temporal + Pydantic AI."

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

- `Interrupt` Action.
- Runs-state checkpoint on Interrupt.
- `client.resume(run_id, payload)` and the resume topic.
- Worker recovery path: on startup, no special action; resume is just a normal publish.
- L2 test for round-trip Interrupt/Resume.
- L3 test for crash-during-Interrupt.

**Risk:** medium. Less critical than M4 but novel.

### Milestone M6 — Tool model and cross-language scaffolding (1 week)

- `@tool` decorator.
- `external_tool` declaration.
- LLM schema synthesis from `input` Pydantic model.
- Tool registry per Worker.
- (Defer cross-language SDKs; just document the wire format and provide a Python-only smoke test.)

**Risk:** low.

### Milestone M7 — Agent SDK (the primary user-facing surface) (2.5 weeks) ★ CRITICAL FOR DX

This is the milestone that delivers Layer A (§4). It is the user-facing primary surface; without it, the SDK is positioned as a low-level Kafka-with-Actions library, not as an agent SDK. Treat its ergonomics as load-bearing as the M4 aggregator.

- `Agent` class: `name`, `model`, `system_prompt` (static or dynamic callable), `tools`, `output_type`, `output_validators`, `handoffs`, `parallel_tools`, `interrupts`, `deps_type`, `max_turns`, `on_tool_error`, `publishes`, `subscribes_to`.
- `@tool` decorator: synthesizes JSON schema for LLM, registers tool topic.
- `external_tool(...)` declaration: cross-language tool stub.
- `Sub(other_agent, as_tool=...)`: agent-as-tool composition.
- `Subscription(...)` declaration: choreography binding (source/topic, payload, filter, react, group_id, start_from, cross_tenant). See §11.B.7.
- `worker.add(agent)`: the compilation step. Materializes the synthesized Handler, registers tool Handlers, derives topics, wires subscriptions. `worker.inspect()` for visibility.
- `worker.wire(source=..., target=..., payload=..., filter=...)`: deployment-time choreography binding (§11.B.6).
- `ctx.trigger`: per-invocation origin info (`kind` ∈ {request, event, resume, tool_call, fan_child}; `source_topic`, `source_agent`, `event_type` populated for events).
- Internal Agent Loop recipe: drives the LLM turn loop, emits `Call` on tool selection, emits `Fan` on multi-tool-selection (when `parallel_tools` covers them), emits `Reply` on final output, emits `Interrupt` on configured triggers, handles `output_validators` retry via `ModelRetry`. For event-triggered Runs, the loop terminates with `Emit` to `agent.<name>.events` instead of `Reply` when no caller awaits.
- Choreography lifecycle events: `EventReceived`, `EventSkipped` (filter false), `EventDispatched`, `EventPublished` — observable via `on_event`.
- Worked examples: hello-world (§4.2), trading agent with cross-language tool (§4.3), handoffs (§4.4), sub-agents (§4.4), parallel sub-agents (§4.4), HITL (§4.5), linear choreography (§11.B.1), pub/sub fan-out (§11.B.2), filtered subscription (§11.B.3), subscribing to arbitrary Kafka topic (§11.B.4), deployment-time wiring (§11.B.6), mixed orchestration+choreography (§11.B.12).
- L2 test: agent with stubbed LLM + stubbed tools + InMemoryWorker, end-to-end via `worker.invoke(...)`; choreography subscription via `InMemoryWorker.publish_event(...)`.
- L3 test: agent across two Worker processes (the agent on one, its tools on another), end-to-end via real Kafka; choreography pipeline of 3 agents with `publishes=`/`subscribes_to=` end-to-end.

**Risk:** medium-high. The compilation step (Agent → synthesized Handler) must be testable, inspectable, and overridable at every level. The ergonomics must feel like Pydantic AI for the simple cases; if `Agent(name=..., model=..., tools=[...])` doesn't deliver a working Kafka-deployed agent in under 30 minutes for a new user, the milestone has failed even if all the wire-format work is correct.

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

4. **Should the SDK ship a default `LLMExtension` that intercepts `agent_loop` model calls for centralized rate-limit/retry/cost, or is that always user-supplied?**
   *Lean:* ship a default that handles cost-tracking only; rate-limit and retry are user-tunable Extensions.

5. **How does `Worker.deps_factory` interact with hot-reload / config changes?**
   *Lean:* not in v1.0. Workers are deploy-stop-redeploy. Hot deps reload is a v1.x concern.

6. **What happens to a Reply when the parent topic no longer has a consumer?** (e.g., parent Handler was removed in a deploy)
   *Lean:* DLQ to `calf.dlq.orphan-replies` after a configurable retention. Emit `OrphanReply` event.

7. **Multi-Handler-on-same-topic — is that supported or rejected?**
   *Lean:* one Handler per topic per Worker. Multiple Workers (different consumer groups) on the same topic is the broadcast pattern. Multiple Handlers in *one* Worker on the *same* topic is rejected (registration error).

8. **Should `Fan.calls` allow heterogeneous payload types or only one type?**
   *Lean:* heterogeneous. The aggregator doesn't care; the user reconstructs by key. The type checker will complain unless `ctx.fan_results` is typed `dict[bytes, Any]`.

9. **What's the right answer for "long-running tool" — tool takes 10 minutes, parent's `Call.timeout` expires?**
   *Lean:* the `Call.timeout` is the parent's hint; the tool is not killed. The tool's Reply will arrive late and be dropped by dedup (frame already closed). The parent receives a `Fail` for the call. This is the at-least-once cost; explicit doc.

10. **Should the runs-state topic carry the *full* state or just a pointer/version?**
    *Lean:* full state on `Interrupt` (we need it for resume); just metadata on `RunStarted`/`Completed` (we don't need to checkpoint mid-run by default).

11. **Should tool topics be auto-created or must they pre-exist?**
    *Lean:* auto-create by default (with `Worker(auto_create_topics=True)`), with a flag to disable for prod environments where topic creation is gated.

12. **What's the upgrade story when the Envelope schema_version bumps mid-deploy?**
    *Lean:* a single Worker can read multiple schema versions (forward-compat decoder for known prior versions; unknown future versions DLQ). Schema version bumps are coordinated across the cluster; the SDK ships a one-version-back decoder.

13. **(Surfaced while writing this doc.) Cost tracking belongs on `Run`, but Runs can have sub-Runs (an agent that handoffs to another agent on the same physical Run vs invokes a sub-agent as a new logical Run). How do we model this for rollup?**
    *Lean:* a Handoff (via `Call` within the same Run) is the same RunId — cost rolls up naturally. A sub-Run (a Handler explicitly creates a new Run via `client.invoke` from within the Handler, with `parent_run_id` set) is a separate RunId — `parent_run_id` enables roll-up at query time. The default is same-Run handoffs; sub-Runs are an explicit user choice.

14. **(Surfaced while writing this doc.) The `agent_loop` recipe's internal state (message_history, tool_calls, tool_results) — does it live in user `State` or in a recipe-owned sub-field?**
    *Lean:* recipe-owned sub-field. The user's State has an optional `agent_loop: AgentLoopState` slot; the recipe reads/writes it. Users who don't use `agent_loop` don't pay the State complexity.

15. **(Surfaced.) For the Schema Registry mode, do we require Confluent Schema Registry or accept Apicurio / Karapace / others?**
    *Lean:* Confluent Schema Registry first (most common). Add Apicurio/Karapace as adapters in v1.1.

16. **(Choreography.) When an Agent declares `publishes=EventType` AND has a caller awaiting a `Reply`, do we emit the event topic + reply concurrently (single-flush) or sequentially (reply first, then event, with the event becoming visible *after* the caller observes the reply)?**
    *Lean:* sequentially, reply first, then event — semantically clearer ("the request was completed before the event was broadcast") and observers don't race to see the event before the original caller has been answered. Cost: one extra small publish latency on the request path. Open to revisiting under load testing.

17. **(Choreography.) Should `subscribes_to=` accept multiple Subscriptions whose payloads are the same type but different sources?**
    *Lean:* yes (no constraint). The dispatch routes by topic; the handler receives the payload + `ctx.trigger.source_topic` to disambiguate. The LLM is invoked with the payload; if the agent needs source-specific behavior, the user can branch on `ctx.trigger`.

18. **(Choreography.) How do we detect orphaned subscriptions when a producer renames its events topic (or an Agent is removed from the deployment)?**
    *Lean:* `worker.inspect()` in CI prints all subscriptions and their resolved topics. Auto-detection in production is hard (events that never arrive look identical to events that just haven't happened). A `--strict-wiring` flag could fail Worker startup if a Subscription's source-agent is not registered in the same Worker; useful for tightly-coupled deployments, optional for loose ones.

19. **(Choreography.) Should the runs-state topic for an event-triggered Run carry the parent producer's `run_id` (lineage) as part of the value, or just as the `parent_run_id` field?**
    *Lean:* `parent_run_id` field is enough. Don't denormalize producer state into the consumer's record — that's coupling. The OTel span hierarchy + runs-state index by `parent_run_id` is the join path.

20. **(Choreography.) Schema-typed subscriptions (`Subscription(payload=NewsArticle)` with no `topic=` or `source=`) require the SDK to resolve a topic from a Pydantic type. Is the convention `event.<module>.<TypeName>`, or do we require an explicit `class Config: calf_topic = ...`?**
    *Lean:* require explicit `class Config: calf_topic = ...` in v1.0. Implicit type→topic conventions cross-package have produced footguns in every framework that adopted them; the explicit form is two extra lines for safety. Revisit for v1.1 if user demand is strong.

21. **(Choreography.) For a Worker hosting N Agents with overlapping subscriptions to the same topic, do we share a single Kafka consumer subscription per topic and multiplex, or keep N separate consumers (one per Agent)?**
    *Lean:* N separate consumers. Each Agent's subscription has its own consumer group so each receives every message — that's the whole point of pub/sub fan-out. Multiplexing inside the Worker would conflict with that. Cost: more consumer connections. Acceptable.

---

## 25. Appendix

### 25.1 Glossary

- **Action** — A typed value returned by a Handler that describes a side effect (publish, checkpoint, terminate) to be performed by the runtime.
- **Aggregator** — Runtime component that collects Fan-children Replies and re-enters the parent Handler with all results.
- **Choreography** — Multi-agent pattern where producers publish events and consumers independently subscribe. Producers don't know consumers exist. See §11.B. Contrast with Orchestration.
- **Context** — The per-invocation object passed to a Handler, exposing `run_id`, `state`, `deps`, `fan_results`, `resume_payload`, `trigger`, etc.
- **Deps** — User-provided dependency object (DB conn, HTTP client, etc.) supplied at Worker startup, scoped to the Worker process.
- **Envelope** — The wire-format message carrying state bytes, payload bytes, frame stack, IDs.
- **Event** — A wire-level envelope with `action_kind="emit"` and empty `call_stack`. Produced by `Emit`; consumed by Subscriptions. The choreography unit.
- **Extension** — User-provided cross-cutting concern, implementing one or more of `around_invoke`, `around_publish`, `on_event`.
- **Fan** — An Action that dispatches N parallel Calls; the aggregator re-enters the parent with all Replies.
- **Frame** — One (caller, callee) pair within a Run; tracked in the Envelope's call_stack.
- **Handler** — User function from `(Context, Msg) -> Action`. The atomic unit of user code.
- **HITL** — Human-in-the-loop. Achieved via `Interrupt` + `Resume`.
- **Hop** — One trip from publisher to consumer over Kafka.
- **LifecycleEvent** — Typed observable event (RunStarted, ToolCallStarted, EventReceived, EventSkipped, etc.) consumed via `on_event`.
- **Orchestration** — Multi-agent pattern where a caller explicitly invokes a callee via `Call`/`Reply`/`Fan`. See §11.A. Contrast with Choreography.
- **Run** — The durable agent-execution aggregate, keyed by `RunId` (uuid7). Event-triggered Runs have `parent_run_id` set to the producer's Run.
- **State** — User-typed value surviving across hops within a Run; opaque bytes on the wire.
- **Subscription** — Declaration that binds an Agent (or Handler) to a Kafka topic as a reactive consumer. Carries `source` or `topic`, `payload` type, optional `filter`, `react`, `group_id`. See §11.B.7.
- **Tool** — A Handler designated as callable by an LLM, with a synthesized JSON schema. Orchestration-only (called by an Agent, not subscribed).
- **Trigger** — The origin of a Handler invocation, exposed as `ctx.trigger`. Kinds: `request`, `event`, `resume`, `tool_call`, `fan_child`. Lets agent code branch on "how did I get here."
- **Wire** — As in `worker.wire(source=..., target=..., payload=...)`. Deployment-time choreography binding — attaches a Subscription to an Agent outside its declaration. See §11.B.6.
- **Worker** — A process hosting Handlers + Extensions, consuming from Kafka.

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
| `calfkit/nodes/tool.py` | 26-105 | `ToolNodeDef` | `@tool` decorator |
| `calfkit/nodes/tool.py` | 99-105 | `agent_tool` decorator | Renamed to `@tool` |
| `calfkit/models/actions.py` | 19-119 | `Reply`, `Delegate`, `Call`, `TailCall`, `ReturnCall`, `Sequential`, `Emit`, `Parallel`, `Silent` | New Action algebra (§3) |
| `calfkit/models/envelope.py` | 9-17 | `Envelope(BaseModel)` with inline `SessionRunContext` | New Envelope with opaque state bytes (§6.1) |
| `calfkit/models/state.py` | 18-101 | `State` god-model | User-defined StateT (§7) |
| `calfkit/models/state.py` | 127-141 | `PendingToolBatch` | Replaced by aggregator (§9) |
| `calfkit/models/session_context.py` | 33-39 | `CallFrame` dataclass | Survives as internal Frame model |
| `calfkit/models/session_context.py` | 45-70 | `WorkflowState` | Becomes part of Envelope structure |
| `calfkit/models/session_context.py` | 73-78 | `Deps(BaseModel)` | Survives, slimmed |
| `calfkit/models/session_context.py` | 81-114 | `BaseSessionRunContext` / `SessionRunContext` | Becomes `Context[DepsT, StateT]` |
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
