# Calfkit 1.0 Design — DX Review

**Reviewer:** python-sdk-dx-reviewer (independent voice)
**Doc reviewed:** docs/calfkit-v1-design.md (3,448 lines, 25 sections)
**Date:** 2026-05-18

---

## Update — 2026-05-18: §3.2 retracted

**§3.2 ("`@tool` is module-level instead of `@agent.tool`") is withdrawn.** The user overrode that recommendation. The right call for calfkit specifically is module-level `@tool` — never `@agent.tool` — because calfkit's tools are distributed Kafka services on their own topics, not in-process Python objects bound to an Agent's `tool_registry` dict. The Pydantic-AI-faithfulness frame I was applying did not weight the underlying mechanism correctly: same syntax with different mechanism is worse than honest divergent syntax. The corrected position is now reflected in §10 of the design doc and in the prefatory remarks here:

- Tools are first-class Kafka services in calfkit; never agent-owned. The decorator surface reflects that reality.
- Cascading corrections that flow from this (all integrated into the design doc):
  - **§3.5 HITL** (`requires_approval`): moved onto the tool itself via `@tool(requires_approval=True, resume_with=...)`. The gate is a property of the side effect being gated, not of the agent calling the tool.
  - **§3.7 sub-agents**: `agent.as_tool()` instance method dropped. Replaced by `Tool.from_agent(agent, name=...)` factory classmethod paralleling `Tool.external(...)`. An agent is already a Kafka service on its request topic; the factory wraps the reference in a `Tool` value at the call site without implying ownership.
  - **§3.12 `external_tool`**: unchanged — `Tool.external(...)` was always correct.
  - **§4 from-scratch sketches**: rewritten to use module-level `@tool`, `Tool.from_agent(...)`, and `Tool.external(...)`. All examples now show tools authored independently of the agent and attached via `tools=[...]`.

The principle I should have applied: **for any DX recommendation that ports a Pydantic AI shape into calfkit, check whether the underlying mechanism is the same.** If yes, port the shape (e.g., `instructions=` naming, `output_type` behavior, `@agent.output_validator`, `RunContext[Deps]` → `Context[Deps]` — all of these describe behavior *inside* an agent's reasoning loop and are mechanically identical across runtimes). If no, diverge and defend the divergence on architectural grounds. The remaining ~19 of 20 critique items in this document continue to apply unchanged — they were all reasoned at the appropriate granularity. Only the `@agent.tool` recommendation in §3.2 (and its cascades into §3.5 and §3.7) was wrong.

---

## 1. Top-line verdict

The two-layer thesis (Agent SDK over a runtime primitive layer) is the right framing and the §1.3 / §4.10 competitive positioning is sound — there is a real, defensible gap between in-process agent SDKs (Pydantic AI, OpenAI Agents) and durable-execution platforms (Temporal, Restate), and calfkit lives in it. The runtime layer (§3 Action algebra, §9 fan-out aggregator, §12 three-primitive Extension API) is well-reasoned distributed-systems engineering and I have very few objections at that tier.

The Agent SDK (Layer A) is where the design fails its own DX brief. It is back-derived from Layer B rather than designed from a Pydantic-AI migrant's first-touch experience, and that shows in (a) a kitchen-sink `Agent(...)` constructor that bolts choreography and HITL onto the same surface as identity and behavior, (b) module-level `@tool` instead of `@agent.tool` — the wrong call for Pydantic AI faithfulness, (c) a dict-of-named-`Interrupt` HITL recipe that lies about the LLM control flow, (d) `Context[Deps, State]` two-parameter generic where Pydantic AI normalized on `RunContext[Deps]` after deliberate consideration, (e) `system_prompt=` instead of `instructions=` (which Pydantic AI itself has now migrated to), and (f) a `Worker(...)` god-constructor that mixes transport, deps, extensions, and topology under one kwarg blob.

**The two changes that matter most.** First: rip `subscribes_to`, `publishes`, `handoffs`, `parallel_tools`, `interrupts`, `output_validators`, `on_tool_error`, `topic`, `group_id`, `reply_topic`, `max_turns`, and `events_topic` out of the `Agent(...)` constructor; keep only `model`, `instructions`, `output_type`, `deps_type`, `tools`, `name`. Everything else moves to decorators on the agent instance (Pydantic AI) or to a separate composition layer on `Worker` (FastAPI / Starlette pattern). Second: stop conflating "the agent declaration" with "the deployment topology." The agent declaration is portable; the topology lives on the Worker. The architect's instinct that "the same agent can be reused across topologies" (§4.4.B) is correct — but the design then violates it by allowing `subscribes_to=` and `publishes=` to live on the `Agent`. Force the choreography wiring entirely onto `worker.wire(...)`.

The doc is also too long to be the SDK's design source. Sections §1, §4, §5, §11, §17, §18, and §22 contain the load-bearing surface decisions. Sections §6–§10, §13–§16, §19 are runtime correctness arguments and can move to a separate "Runtime spec" document. Without the split, the Agent SDK looks like an afterthought sitting on top of a 3,000-line Kafka spec, which is exactly the framing the user said they wanted to avoid.

---

## 2. What works well

These are the load-bearing decisions I would not reverse.

- **Two-layer split is correct.** The §4.1 framing — Layer A is the user-facing primary surface, Layer B is the runtime — is the right packaging. Most agent SDKs that try to expose a single surface end up either Pydantic-AI-clone (no escape hatch) or Kafka-glue (no agent ergonomics). Calfkit's two-layer split has real precedent (Starlette under FastAPI; httpx Transport under httpx Client) and is well-suited here.
- **Action algebra (§3).** Closed set of seven Actions with a documented idempotency contract per Action is exactly the discipline this needs. The 0.x god-method critique in §1.1 is fair and the algebra is the right answer.
- **Compacted-topic-backed fan-out aggregator (§9).** The walked-through example in §9.8 is concrete enough to defend. Picking option C (per-Worker, partition-affine) over options A and B is well-justified. The "don't commit the aggregator's consumer offset until the parent re-entry has been published" invariant in §9.6 is the right invariant.
- **Three-primitive Extension API (§12).** Rejecting the `before_*` / `after_*` named-sugar hierarchy because it lies about cross-process boundaries is correct, important, and well-argued. The `match`-statement dispatch on `LifecycleEvent` is a stronger pattern than method-name dispatch for exactly the reasons §12.4 cites.
- **Opaque state bytes on the wire (§6.1 + §7.2).** Pushing serialization down to the user so the SDK never owns the user's state shape directly fixes the `CompactBaseModel` footgun class permanently. This is the right call for a distributed SDK.
- **Distinguishing `run_id` from `correlation_id` (§2.3, §8.4).** The 0.x conflation is a real bug class; separating them is correct.
- **Per-source DLQ (`calf.dlq.<source-topic>`) (§15.4).** Operationally correct. Shared DLQ would lose the "what failed for tool X" inspection path.
- **OTel-native (§14).** Closed-set cardinality discipline (§14.3) is the right policy.
- **Three-layer testing strategy (§17).** The "InMemoryWorker IS the same runtime class as Worker, with a different transport" point in §17.2 is excellent — it's exactly the Starlette TestClient model.
- **Choreography as first-class (§11.B).** The structural argument for distinct Runs with `parent_run_id` lineage (§11.B.9) is sound; pubsub semantics demand it. The §11.A vs §11.B framing as deliberate parallel patterns with documented trade-offs is the right teaching shape.
- **Direct aiokafka over FastStream (§20).** The five reasons in §20.1 are all real. The rebalance-listener point alone justifies it.

The skeleton is good. The skin is where the issues are.

---

## 3. Where the design goes wrong

### 3.1 [Critical] `Agent(...)` is a kitchen-sink constructor

**Location:** §4.2 hello-world; §4.4 multi-agent; §4.4.B choreography; §4.5 HITL; §4.7 step 7 ("Overrides"); §23 Milestone M7.

**Observation.** The `Agent(...)` constructor accumulates parameters across four orthogonal semantic concerns:
- **Identity:** `name`, `topic`, `group_id`, `reply_topic`, `events_topic`.
- **Model behavior:** `model`, `system_prompt`, `instructions`, `output_type`, `output_validators`, `deps_type`, `max_turns`, `on_tool_error`.
- **Tool registry:** `tools=[...]`, `parallel_tools=[...]`.
- **Multi-agent topology:** `handoffs=[...]`, plus `Sub(...)` instances inside `tools=`.
- **Choreography topology:** `publishes=...`, `subscribes_to=[Subscription(...)]`.
- **HITL:** `interrupts={...}`.

That is **17+ named parameters spanning six semantic concerns** in one constructor. The §4.7 "Overrides" example explicitly shows a 9-parameter call. The §4.4.B "mixed orchestration + choreography" example in §4.4.B shows seven kwargs in one call. This is the kitchen-sink anti-pattern Pydantic AI, OpenAI Agents, httpx, Stripe-python, and FastAPI all explicitly avoid.

**Why it matters.** Three concrete DX failures:

1. **IDE autocomplete becomes useless.** When the user types `Agent(` and tab-completes, they see 17 parameters with no semantic grouping. The 80%-case fields (`model`, `instructions`, `tools`, `output_type`) are buried among advanced fields (`cross_tenant`, `reply_topic`, `max_turns`).
2. **The constructor signature lies about what an Agent *is*.** An Agent that has `subscribes_to=` is fundamentally a different deployment shape than an Agent that doesn't, but the type system treats them identically. The reader of `Agent(name="foo", subscribes_to=[...])` cannot tell from the type whether this object owns a request topic, an event subscription, or both.
3. **Reuse across topologies is structurally blocked.** The doc claims (§4.4.B) "the same agent can be reused across topologies" — but if the topology is in the `Agent(...)` declaration, it isn't. You'd have to redefine the Agent per deployment.

**Side-by-side with Pydantic AI.** Pydantic AI's actual constructor (verified just now from `pydantic.dev/docs/ai/api/pydantic-ai/agent/`):
```python
Agent(
    model: Model | KnownModelName | str | None = None,
    output_type: OutputSpec[OutputDataT] = str,
    instructions: AgentInstructions[AgentDepsT] = None,
    system_prompt: str | Sequence[str] = (),
    deps_type: type[AgentDepsT] = NoneType,
    name: str | None = None,
    tools: Sequence[Tool[AgentDepsT] | ToolFuncEither[...]] = (),
    # ... a handful of advanced opts (retries, end_strategy, model_settings)
)
```

Eight to ten parameters; identity (name) + behavior (model, instructions, output_type, deps_type, tools) and nothing else cross-cutting. Tools are wired via `@agent.tool` decorator after construction. **There is no `handoffs=`, `subscribes_to=`, `publishes=`, `interrupts=`, or `parallel_tools=` in Pydantic AI's constructor** — those are either separate methods, separate types, or absent (Pydantic AI doesn't do choreography).

**OpenAI Agents SDK constructor** (verified from `openai.github.io/openai-agents-python`):
```python
Agent(
    name: str,
    instructions: str | Callable[...] | None = None,
    handoffs: list[Agent[Any] | Handoff[...]] = [],
    model: str | Model | None = None,
    output_type: type[Any] | AgentOutputSchemaBase | None = None,
    tools: list[Tool] = [],
    # ... advanced: guardrails, hooks, tool_use_behavior, mcp_servers
)
```

OpenAI Agents has `handoffs=` in the constructor — but **only that one topology concept**, and `handoffs` is a separate list from `tools` (deliberately not mixed). It does not have `publishes=`, `subscribes_to=`, `interrupts=`, or `parallel_tools=`.

**Recommendation.** Constructor keeps only:
```python
Agent(
    name: str,
    *,
    model: str | Model,
    instructions: str | Callable[[Context], str | Awaitable[str]] | None = None,
    output_type: type[Any] = str,
    deps_type: type[Deps] = NoneType,
    tools: Sequence[Tool] = (),
)
```
Everything else moves out:
- `handoffs` → method: `agent.handoffs_to(billing_agent, tech_agent)` (or instance attribute populated by separate `Handoff(...)` declarations).
- `subscribes_to`, `publishes` → entirely off the Agent; live exclusively on `worker.wire(...)`. (Strong recommendation: see §3.3 below.)
- `interrupts` → `@agent.interrupt(when=...)` decorator returning `Interrupt(...)`; see §3.5.
- `parallel_tools` → not a constructor kwarg; either inferred from `tools=[parallel(t1, t2, t3)]` grouping or controlled by the LLM (which is what `parallel_tool_calls=True` in OpenAI's API already does).
- `output_validators` → `@agent.output_validator` decorator (Pydantic AI's existing pattern).
- `topic`, `group_id`, `reply_topic`, `events_topic` → `agent.deploy_with(topic=..., group_id=...)` or registered at the Worker via `worker.add(agent, topic_overrides={...})`.
- `max_turns`, `on_tool_error` → keep, but on `agent.runtime_config(...)` or move to `Worker` defaults.

**Severity: Critical.** Get this wrong and every reader's first-touch experience is "this looks like Pydantic AI but is actually a different framework with a Pydantic AI veneer," which is exactly the Layer A faithfulness violation the design must avoid.

---

### 3.2 [Critical] `@tool` is module-level instead of `@agent.tool`

**Location:** §4.2 hello-world (`@tool async def get_weather`); §4.9 surface map row "`@agent.tool` → `@tool` (module-level)".

**Observation.** The design rationalizes module-level `@tool` in §4.9 with: "Tools are module-level so they can be shared across agents and exposed as topics." This is a legitimate-sounding argument that I think is wrong.

**Why it matters.**

1. **It diverges from Pydantic AI.** Pydantic AI uses `@agent.tool` and `@agent.tool_plain` *as instance decorators on the Agent*. A Pydantic AI migrant who sees `@tool` at module level will read calfkit as "a different framework." This is the §3.1 critique compounded.
2. **The "shared tools" argument is weak.** Pydantic AI users *do* share tools across agents — by passing the tool function reference via `tools=[fn]` constructor arg, *or* by defining the same `@agent.tool` against multiple agents. Both work. Module-level `@tool` solves a problem nobody had in Pydantic AI.
3. **The Kafka-topic story is orthogonal.** Whether a tool lives on a Kafka topic is a deployment fact, not an authoring fact. The tool function's body is identical; only the wiring differs. You can keep `@agent.tool` and *still* make each tool a Kafka topic at deploy time. Pydantic AI's pattern doesn't preclude calfkit's wire-format choice.
4. **Module-level decorators have a known footgun.** They register tools at import time, which means import order matters, circular imports cause silent failures, and tool registration becomes a global side effect.

**Side-by-side.**

```python
# Pydantic AI (current API)
agent = Agent("openai:gpt-5.4", instructions="...")

@agent.tool
async def get_weather(ctx: RunContext[Deps], location: str) -> dict:
    ...

# OpenAI Agents
@function_tool  # module-level, but adds to a Tool instance, not to a global registry
def get_weather(location: str) -> dict:
    ...
agent = Agent(name="...", tools=[get_weather])

# Calfkit proposal (§4.2)
@tool                  # module-level, registers a global Kafka topic
async def get_weather(location: str) -> dict:
    ...
weather_agent = Agent(name="...", tools=[get_weather])
```

The Pydantic AI form is the strongest precedent for "I am authoring an agent and want a tool." The OpenAI Agents form is precedent for "I am authoring a portable tool and want to attach it to an agent." Calfkit's form pretends to be the latter but its registration model (tool → Kafka topic at decorator time) is closer to the former.

**Recommendation.** Adopt Pydantic AI's `@agent.tool` and `@agent.tool_plain` as the primary form. Keep `@tool` (module-level) as the *secondary* form for "I'm publishing a portable, cross-language tool" — this is the case where module-level makes sense because the tool truly is standalone (no parent agent). Document this as a two-form API:

```python
# Form 1: authoring a tool inside the agent (Pydantic-AI form)
agent = Agent(...)

@agent.tool
async def get_weather(ctx, location: str) -> dict:
    ...

# Form 2: portable tool intended to be published as a standalone Kafka topic
@calfkit.tool(topic="tool.get_weather")
async def get_weather(location: str) -> dict:
    ...
```

Either form synthesizes a Handler; the topic naming convention is identical; the LLM schema synthesis is identical. The split simply maps the syntax to the user's actual intent.

**Severity: Critical.** Layer A faithfulness violation. First decorator a user types after the constructor.

---

### 3.3 [Critical] `Agent(subscribes_to=..., publishes=...)` violates the agent-is-portable principle the doc itself states

**Location:** §4.4.B; §4.7 step 1 ("Topic derivation"); §11.B.

**Observation.** Section 4.4.B introduces `subscribes_to=` and `publishes=` as in-`Agent` declarations. Section 11.B.6 then *acknowledges* the problem: "Agents that should be reusable across deployments shouldn't hard-code their subscriptions. Use `worker.wire(...)` to attach Subscriptions at the deployment site." And then §11.B.6 explicitly says: "`worker.wire(...)` is **the canonical form for production deployments**."

So the design has *two ways to declare choreography wiring*, knows the in-Agent form is wrong for production, and ships both anyway.

**Why it matters.**

1. **Two-way-to-do-it is a Python-community anti-pattern.** PEP 20 ("There should be one — and preferably only one — obvious way to do it"). When the doc says "either form may be used; the underlying mechanism is identical," it produces a learning tax: users see both forms in examples, ask "which should I use," get a fuzzy answer, and one of them becomes a cargo-culted default. The doc itself elsewhere prefers `worker.wire(...)` (§11.B.6) — pick that one and *remove* the other.
2. **The in-Agent form is structurally incoherent with reuse.** An agent with `subscribes_to=[Subscription(source=watcher, ...)]` is no longer a portable unit — it requires the `watcher` reference at import time, creating cross-package import coupling. The §4.4.B example with deployment-time wiring (line 700+) is the *only* form that supports a real shared-library workflow.
3. **It bloats the constructor (§3.1) for a feature with an existing better home.**

**Recommendation.** Choreography wiring lives *exclusively* on `worker.wire(...)`. Remove `subscribes_to=` and `publishes=` from `Agent(...)` entirely. The Agent declares only its *output type* (which is `output_type=`, already present) and its *input type* (inferred from signature or input_type=); whether that output is published as an event is a deployment decision:

```python
# Authoring (in a library)
classifier = Agent(
    name="classifier",
    model="openai:gpt-5.4",
    instructions="Classify the input.",
    output_type=Classified,
)

# Deployment-time wiring (in main.py)
worker.add(classifier)
worker.publish(classifier, as_event=Classified)            # the classifier emits Classified
worker.wire(source=classifier, target=enricher, payload=Classified)
worker.subscribe(audit_agent, topic="events.txns.completed", payload=TransactionCompleted)
```

This is FastAPI's model: the route function is portable; the app wiring (`@app.get(...)`, middleware, dependency overrides) lives at the app level. Routes don't carry app config.

**Severity: Critical.** Architectural mistake that contradicts the design's own stated reuse goal.

---

### 3.4 [Critical] `system_prompt=` is the wrong primary name; Pydantic AI now uses `instructions=`

**Location:** §4.2 hello-world; §4.4 multi-agent examples; §4.9 surface map; §4.10 head-to-head.

**Observation.** Every example in the design uses `system_prompt=`. Pydantic AI's current public API uses `instructions=` as the primary, with `system_prompt=` retained as a legacy variant. Verified just now in the current Pydantic AI docs: the constructor accepts both, but `instructions` is the documented modern form, and `instructions` (unlike `system_prompt`) doesn't get included in run-state replays — it's a current-run-only prompt (per Pydantic AI's docs).

OpenAI Agents SDK also uses `instructions=`, not `system_prompt=`.

**Why it matters.** A Pydantic AI migrant reads `Agent(model=..., system_prompt=...)` and assumes this is the legacy form. They then look for `instructions=` (which Pydantic AI documents as the modern form) and find it absent. First impression: "calfkit shipped a year late."

**Recommendation.** Adopt `instructions=` as the primary kwarg in *every* example. Either reject `system_prompt=` entirely or accept it as a deprecated alias with a `DeprecationWarning`. Update all 14 examples in the design doc that use `system_prompt=`.

**Severity: Critical** because it appears in literally every hello-world and is the most visible word in the SDK after `Agent`.

---

### 3.5 [Critical] HITL `interrupts={...}` dict-of-named-rules is a recipe-not-a-primitive mistake

**Location:** §4.5; §4.10 trading_agent example; Milestone M5 in §23.

**Observation.** The proposed Layer A HITL surface:

```python
approval_agent = Agent(
    ...,
    interrupts={
        "human_approval": Interrupt(
            when=lambda output: isinstance(output, ApprovalRequired),
            resume_with=HumanDecision,
        ),
    },
)
```

This conflates three things into one dict-of-named-rules: (a) the *condition* under which the agent pauses (`when=`), (b) the *type* of the resume payload (`resume_with=`), (c) a *name* (the dict key) that is otherwise unused in the example.

**Why it matters.**

1. **The `when=` predicate inspects the LLM's final output and decides to pause based on output type.** That's not what `Interrupt` is in the Action algebra (§3.1) — Action `Interrupt` is what the *runtime* emits when a Handler returns it. The Agent SDK proposal here is "every LLM turn, the recipe checks each `interrupts[name].when` against the output, and if any match, emit Action `Interrupt`." That's a *recipe*, but the syntax pretends it's a declarative type.
2. **Pydantic AI's HITL story (the `DeferredToolRequests` / `DeferredToolResults` pattern) is built differently and cleaner.** In Pydantic AI, an agent's HITL points are *tools* with `requires_approval=True`. When the LLM picks the tool, the agent suspends, returns a `DeferredToolRequests` value to the caller, the caller collects human input, the caller `agent.run(..., deferred_tool_results=...)` to resume. The HITL boundary is *the tool*, not a synthetic output-type check.
3. **The dict-of-named-rules has no use case in the §4.5 example.** The name "human_approval" is never referenced. If it isn't referenced, it shouldn't exist (one-of-N use case: passing `name=` to `client.resume(name=...)` to disambiguate multiple interrupts, but §4.5 doesn't show that). Single-interrupt is by far the common case.
4. **The lambda `when=` predicate is a footgun.** A lambda gets serialized... no, it doesn't, it gets evaluated in-process each turn. But if the agent is replayed (the at-least-once retry path), the lambda is re-evaluated against fresh LLM output that may or may not match. If two replays of the same Run produce slightly different LLM outputs (non-determinism), one replay interrupts and one doesn't. The dedup story §3.4 catches some of this, but `when=` being an opaque predicate makes the failure mode invisible.

**Recommendation.** Adopt Pydantic AI's tool-approval pattern as the primary HITL form, and add a small `@agent.interrupt(...)` decorator for the "interrupt on output type" case.

```python
# Form A: tool-based HITL (Pydantic-AI-style, the common case)
@agent.tool(requires_approval=True)
async def execute_trade(ctx, order: BuyOrSellOrder) -> TradeConfirmation:
    ...
# When the LLM picks this tool, the agent emits Interrupt(reason=..., resume_payload=HumanDecision)
# The caller collects approval, then client.resume(run_id, HumanDecision(approved=True)).

# Form B: output-type-based HITL (less common)
@agent.interrupt_when(output_type=ApprovalRequired, resume_with=HumanDecision)
async def needs_review(output: ApprovalRequired) -> bool:
    return output.risk_score > 0.5
```

Form A handles 80% of HITL ("the LLM picks an action that needs a human"). Form B handles the rare "the LLM output itself is the request for approval." Both compile to the same Action `Interrupt`. Neither uses a dict-of-named-rules.

**Severity: Critical.** HITL is in every demo of every agent SDK and gets quoted in blog posts. The named-dict shape will become the meme. Make it match Pydantic AI.

---

### 3.6 [Important] `Context[Deps, State]` two-parameter generic is wrong; Pydantic AI deliberately chose one parameter

**Location:** §4.9 surface map ("`RunContext[Deps]` → `Context[Deps, State]`"); §2.4 State section.

**Observation.** Pydantic AI uses `RunContext[Deps]` — one type parameter. OpenAI Agents uses `RunContextWrapper[T]` — one type parameter. Calfkit proposes `Context[Deps, State]` — two type parameters, claiming "State is the per-Run user-typed state surviving across hops."

**Why it matters.**

1. **It diverges from both reference SDKs for a feature that Pydantic AI considered and rejected.** Pydantic AI does have run-scoped state — they keep it inside `deps`. A user who needs durable state across LLM turns puts it inside their `deps` object.
2. **Two generic parameters scale badly.** A function `async def my_tool(ctx: Context[MyDeps, MyState], req: Req) -> Reply[Resp]` has three type params before the user even writes their tool. Generics-fatigue is a real DX cost.
3. **The §7 State model is solid; the SDK doesn't need to parameterize Context on State to deliver it.** Make `ctx.state` typed via inference from the Handler's signature, not via an explicit type parameter on `Context`. Or expose `ctx.state` as `Any` at the Context level and force the user to validate with `state = MyState.model_validate(ctx.state)` in the handler body (explicit, ugly, but honest).

**Side-by-side.**

```python
# Pydantic AI
async def my_tool(ctx: RunContext[MyDeps], location: str) -> dict:
    ...

# OpenAI Agents
async def my_tool(ctx: RunContextWrapper[MyContext], location: str) -> dict:
    ...

# Calfkit proposal
async def my_tool(ctx: Context[MyDeps, MyState], req: ToolReq) -> ToolResp:
    ...

# Calfkit recommended
async def my_tool(ctx: Context[MyDeps], req: ToolReq) -> ToolResp:
    state: MyState = ctx.state  # inferred or validated explicitly
    ...
```

**Recommendation.** Drop the State type parameter. `Context[Deps]` is the single-parameter form. State is exposed as `ctx.state` typed `Any` (or, optionally, the SDK infers the State type from a class attribute on the agent / handler). Keep `Context[Deps]` faithful to Pydantic AI.

**Severity: Important.** Not a blocker but a faithfulness violation that compounds with §3.4.

---

### 3.7 [Important] `Sub(agent, as_tool="...")` is sugar over a Pydantic-AI-shaped pattern that already exists

**Location:** §4.4 sub-agents ("`Sub(research_agent, as_tool='research_topic')`").

**Observation.** The design introduces a new helper type `Sub(...)` to wrap an agent as a tool. Pydantic AI handles this natively: an Agent has an `as_tool(name=...)` method (or you wrap an Agent's `.run()` in a `@agent.tool` definition). OpenAI Agents has `agent.as_tool(name=..., description=...)`.

**Why it matters.**

1. **A new type for a common pattern bloats the import surface.** Users now import `Sub`, `Subscription`, `Agent`, `Worker`, `tool`, `external_tool`, `Interrupt`, `Context`, `handler`, `Reply`, `Call`, `Fan`, `Emit`, `Done`, `Fail`, `Extension` from `calfkit`. That's 16+ names before the user types their first agent.
2. **The name `Sub` is opaque.** A user reading `tools=[Sub(research_agent, as_tool="research_topic")]` has to look up what `Sub` does. `research_agent.as_tool(name="research_topic")` is self-documenting.

**Recommendation.** Mirror Pydantic AI: agents expose `agent.as_tool(name=..., description=...)`. Drop the `Sub` import.

```python
writer_agent = Agent(
    name="writer",
    model="openai:gpt-5.4",
    instructions="You write articles.",
    output_type=Article,
    tools=[
        research_agent.as_tool(name="research_topic"),
    ],
)
```

The `parallel_tools=[...]` problem from §3.1 also gets solved here: `parallel_tools` was needed because `Sub(...)` wasn't tools-list-compatible. Once sub-agents are just tools (via `as_tool()`), they compose with regular tools naturally.

**Severity: Important.**

---

### 3.8 [Important] `Worker(...)` is a kitchen-sink constructor too

**Location:** §2.5 Worker; §18.1 deployment example.

**Observation.** The §18.1 Worker constructor accepts:
```python
Worker(
    bootstrap_servers=...,
    handlers=[...],
    extensions=[...],
    deps_factory=...,
    group_id_prefix=...,
    partition_strategy=...,
    dedup=...,
    runs_state_topic=...,
    fanout_topic=...,
    dlq_topic_pattern=...,
)
```

Ten kwargs spanning transport (`bootstrap_servers`), composition (`handlers`, `extensions`), deps (`deps_factory`), naming (`group_id_prefix`), partitioning (`partition_strategy`), behavior (`dedup`), and topic configuration (`runs_state_topic`, `fanout_topic`, `dlq_topic_pattern`). And §4.2 / §4.4 / §4.4.B also use `worker.add(...)`, `worker.wire(...)`, `worker.inspect()`, `worker.run()`, `worker.run_blocking()` as methods — but `handlers=[...]` is *also* a constructor kwarg, creating two ways to add a handler.

**Why it matters.**

- Same kitchen-sink critique as §3.1, applied to Worker.
- The "two ways to add a handler" (constructor kwarg vs `worker.add(...)` method) is a §3.3 instance again.
- The trio of `runs_state_topic`, `fanout_topic`, `dlq_topic_pattern` as constructor kwargs is **bizarre** — these are SDK-internal topics that 99% of users shouldn't think about. Hiding them in a `Worker.runtime_config(...)` or a `RuntimeOpts` object lets the default constructor stay clean.

**Recommendation.** Builder-style or split:

```python
# Option A: builder-style (Starlette / FastAPI app pattern)
worker = (
    Worker(bootstrap_servers="kafka:9092", group_id_prefix="myapp-prod")
    .use(OTelExtension(), RetryExtension(max=3))
    .deps_from(create_deps)
    .add(triage_agent)
    .add(billing_agent)
    .wire(source=billing_agent, target=audit_agent, payload=BillingAnswer)
)
await worker.run()

# Option B: split (Worker vs WorkerRuntime)
worker = Worker(bootstrap_servers="kafka:9092")
worker.add(triage_agent)
worker.add(billing_agent)
worker.use(OTelExtension())
worker.runtime(
    runs_state_topic="custom.runs",
    dedup=False,
)
await worker.run()
```

Either form keeps the constructor narrow (transport identity + the absolute basics) and pushes everything else to methods.

**Severity: Important.**

---

### 3.9 [Important] The `instructions` callable signature is under-specified vs Pydantic AI

**Location:** §4.7 step 7 ("`Agent(system_prompt=dynamic_fn)`"); §4.9 surface map ("`agent.system_prompt(dynamic_fn)`").

**Observation.** The design says `system_prompt` accepts "static or dynamic callable" and that the callable "receives `Context`." But:

- Pydantic AI's `instructions` callable signature is `(ctx: RunContext[Deps]) -> str | Awaitable[str]`.
- OpenAI Agents' `instructions` callable signature is `(ctx: RunContextWrapper[T], agent: Agent[T]) -> str | Awaitable[str]` — *two* arguments.
- Calfkit's signature is not stated.

This is a footgun-prone unspecified surface that two reference SDKs disagree on. The design must pick one and document it.

**Recommendation.** Pick Pydantic AI's: `(ctx: Context[Deps]) -> str | Awaitable[str]`. Single argument, optional async. Document explicitly. Reject OpenAI Agents' two-argument form because it leaks the Agent identity into the prompt callable — and the prompt callable is the most common spot where users want to use deps, not agent metadata.

**Severity: Important.**

---

### 3.10 [Important] `output_type=str` and `output_type=MyModel` look identical but behave very differently — make this honest

**Location:** §4.2 (`output_type=WeatherReport`); §4.4 triage (`output_type=str`); §4.10 (`output_type=TradeResult`).

**Observation.** `output_type` accepts both `str` (no validation, raw LLM string) and `MyPydanticModel` (structured output via tool-use enforcement on the LLM). These are two fundamentally different LLM behaviors:

- `output_type=str` lets the LLM output free text.
- `output_type=MyModel` forces the LLM to emit a structured tool call matching `MyModel`'s schema (OpenAI's structured outputs, Anthropic's tool-use).

The same kwarg with the same name produces wildly different LLM API calls.

**Why it matters.** Users will confuse the two and wonder why their agent suddenly stops responding when they switch from `output_type=str` to `output_type=MyModel` (because the model rejects free-text and demands a structured output). This is a Pydantic AI inheritance — Pydantic AI has the same surface — so calfkit "diverging" here would also break the migration story. But the doc should call this out explicitly.

**Recommendation.** Document the behavior change explicitly. Add `output_type` to the §4.9 surface map with a note: "When `output_type` is a Pydantic model, the LLM is forced to emit structured output. When `output_type` is `str` (default), the LLM emits free text. Discriminated unions (`output_type=Foo | Bar`) are supported per Pydantic AI's pattern." If there's behavioral divergence from Pydantic AI's structured-output handling (there usually is — discriminated unions, retries via `ModelRetry`, validators), spell it out.

**Severity: Important.**

---

### 3.11 [Important] `client.invoke("agent.weather-bot.in", req)` makes users type a topic string

**Location:** §4.2 ("the in-process equivalent for tests is `worker.invoke(...)`"); §4.5 client-side HITL example; §22 comparison table.

**Observation.** Examples consistently show the client passing a topic string:
```python
handle = await client.invoke("agent.approver.in", req)
```

This forces users to know calfkit's topic naming convention (`agent.<name>.in`). It also means refactoring an agent's name silently breaks every caller.

**Why it matters.**

1. **It's not Pydantic AI's pattern.** Pydantic AI: `result = await agent.run(input)` — the agent reference IS the entry point.
2. **It's not OpenAI Agents' pattern.** `Runner.run(agent, "input")` — the agent is the argument, not its topic name.
3. **The topic naming convention leaks into user code.** If calfkit later wants to rename the convention from `agent.<name>.in` to `agents/<name>/inbox`, every caller breaks.

**Recommendation.** The client accepts an agent reference, not a topic:

```python
# Preferred (Pydantic-AI-shaped)
handle = await client.invoke(approver_agent, req)
# OR (OpenAI-Agents-shaped)
handle = await client.run(approver_agent, req)

# Topic-string form is the escape hatch
handle = await client.invoke_topic("custom.topic.name", req, reply_type=Resp)
```

The client uses the agent's `name` (or topic, if overridden) under the hood. The user never types `agent.weather-bot.in`.

If the rebuttal is "you have to know the agent reference at the client site, but in distributed deployments the agent isn't imported on the client side" — fine, but then the client should accept either an Agent or a `ClientAgentRef(name="approver", input_type=ReqType, output_type=RespType)` proxy. The bare topic-string form should not be the primary surface.

**Severity: Important.**

---

### 3.12 [Important] `external_tool(...)` reinvents what should be a parameter on `@tool`

**Location:** §4.3 trading agent; §10.2 cross-language tools.

**Observation.** The design has two parallel forms:

```python
@tool
async def fetch_quote(symbol: str) -> dict:
    ...

execute_trade = external_tool(
    name="execute_trade",
    description="...",
    input=BuyOrSellOrder,
    output=TradeConfirmation,
)
```

The first registers a Python-implemented tool; the second registers a cross-language tool stub. They have different shapes (decorator vs function-returning-Tool), different parameter names (the decorator pulls from the function signature, `external_tool` requires explicit `input=` and `output=`), and different mental models.

**Why it matters.**

- Two names for one concept. A tool is a Kafka topic with a JSON schema; the location of the implementation is a *runtime fact*, not an *authoring concept*.
- The user's mental model needs to absorb the distinction "Python-local vs external" when in practice it could be inferred or made uniform.

**Recommendation.** One decorator. The presence of a function body distinguishes local from external:

```python
# Local Python tool (body present)
@tool
async def fetch_quote(symbol: str) -> dict:
    return await coinbase.quote(symbol)

# External tool (declared via Tool() / Tool.external() — same primary type)
execute_trade = Tool(
    name="execute_trade",
    input=BuyOrSellOrder,
    output=TradeConfirmation,
    description="Submit a buy or sell order.",
)
# OR a class-level form (still single concept)
class ExecuteTrade(Tool):
    name = "execute_trade"
    input = BuyOrSellOrder
    output = TradeConfirmation
```

Even if the implementation needs two factory paths internally, the surface name should be unified. Pydantic AI's `Tool(fn, takes_ctx=False)` and the OpenAI Agents `Tool` base class are precedents for "tools are a single concept with multiple construction paths."

**Severity: Important.**

---

### 3.13 [Important] No `Agent.run()` / `Agent.run_sync()` / `Agent.run_stream()` parity story for tests / local dev

**Location:** §4.9 surface map ("`Agent.run(...)` → `client.invoke(...)`"); §17.2 InMemoryWorker.

**Observation.** Pydantic AI and OpenAI Agents both let you call `agent.run(input)` in-process for local development, REPL exploration, and unit tests. The §4.9 surface map says the calfkit equivalent is `client.invoke("agent.<name>.in", req)` over Kafka, or `worker.invoke(...)` for tests.

**Why it matters.**

1. **The REPL story is dead.** A user opens a Python REPL, types `agent = Agent(...)`, then `agent.run("hello")` — nothing happens, because calfkit requires a Worker, which requires Kafka or `InMemoryWorker`. The minimum viable "let me poke at this in a REPL" path is several lines of setup.
2. **Tests are heavier than they need to be.** Even with `InMemoryWorker`, the test setup is `wrk = InMemoryWorker(handlers=[my_agent]); result = await wrk.invoke("my-agent.in", req)`. Compare Pydantic AI: `result = await agent.run("hello")`. One line vs four.
3. **Pydantic AI's `run_sync()` is the most-used method in their docs.** A migrant who can't find an equivalent will think calfkit is "Pydantic AI but worse."

**Recommendation.** Add `agent.run(input)` / `agent.run_sync(input)` as the in-process invocation surface. Under the hood, the first call creates a singleton `InMemoryWorker` with the agent registered, runs through it, returns the result. Subsequent calls reuse the worker.

```python
# REPL / unit test
result = await agent.run("classify this please")
assert result.output.label == "spam"

# Production (Kafka)
worker = Worker(bootstrap_servers="kafka:9092")
worker.add(agent)
await worker.run()
# Caller: handle = await client.invoke(agent, req)
```

This preserves the Pydantic-AI top-level idiom. The single-Worker singleton is hidden plumbing.

**Severity: Important.**

---

### 3.14 [Important] `Action` types are exposed at Layer A in the §4.6 escape hatch but inconsistently named with `Sub`, `Subscription`

**Location:** §4.6 escape hatch import (`from calfkit import handler, Context, Reply, Call, Done`); §11.B.7 Subscription.

**Observation.** Action types like `Reply`, `Call`, `Done`, `Fan`, `Emit`, `Interrupt`, `Fail`, `TailCall`, `Continue` are PascalCase verbs. The choreography types `Sub` (sub-agent) and `Subscription` (event subscription) are also PascalCase but are *not* Actions — they're declarative wiring types. Visually similar imports, mechanically different concepts.

**Why it matters.** Discoverability via autocomplete becomes harder: typing `Sub` in autocomplete shows both `Sub` (sub-agent) and `Subscription`. The user has to read docstrings to tell them apart.

**Recommendation.** Distinct namespaces or naming.
- Actions go under `calfkit.actions`: `from calfkit.actions import Reply, Call, Fan, Emit, Done`.
- Declarative wiring types go under `calfkit`: `from calfkit import Agent, Worker, Subscription`.
- Drop `Sub` entirely; use `agent.as_tool(...)` (see §3.7).

This is the namespace pattern Pydantic AI uses (`pydantic_ai.messages` for wire-level types, `pydantic_ai` for primary surface).

**Severity: Important.**

---

### 3.15 [Important] The `Subscription(...)` dataclass collects 7+ fields with overlap and unclear precedence

**Location:** §11.B.7.

**Observation.** `Subscription(source, topic, payload, react, filter, group_id, start_from, max_concurrency, cross_tenant)` — 9 fields. The `source` and `topic` are mutually exclusive ("exactly one of"). `react=` overrides the default Agent-LLM-loop behavior, but only if set. `filter=` runs before `react=` and before the LLM. There's also `cross_tenant` (a flag) and `start_from` (a Literal).

**Why it matters.** Same kitchen-sink critique. Plus, several fields are deployment-level (`group_id`, `start_from`, `max_concurrency`, `cross_tenant`) and shouldn't be on the same type as fields that are wiring-level (`source`, `topic`, `payload`, `react`, `filter`).

**Recommendation.** Split:

```python
@dataclass
class Source:
    """What this subscription consumes."""
    payload: type[BaseModel]
    source: Agent | None = None         # one of source or topic
    topic: str | None = None

@dataclass
class Reaction:
    """How this subscription reacts."""
    react: Callable[[Context, Any], Awaitable[Action]] | None = None
    filter: Callable[[Any], bool] | None = None

@dataclass
class ConsumerOpts:
    """Deployment-level consumer options."""
    group_id: str | None = None
    start_from: Literal["latest", "earliest"] = "latest"
    max_concurrency: int | None = None
    cross_tenant: bool = False

# Usage
worker.wire(
    source=Source(source=watcher, payload=NewsArticle),
    target=alerter,
    reaction=Reaction(filter=lambda ev: ev.urgency >= 4),
    consumer_opts=ConsumerOpts(start_from="earliest"),
)
```

Or simpler: `worker.wire(source=watcher, target=alerter, payload=NewsArticle, filter=..., react=..., start_from=..., group_id=...)` as keyword args on the wire method; that already exists. The point: don't materialize a `Subscription` dataclass with nine fields.

**Severity: Important.**

---

### 3.16 [Suggestion] `worker.run_blocking()` and `worker.run()` is two-way-to-do-it

**Location:** §4.2 hello-world (`worker.run_blocking()`); §18.1 (`await wrk.run()`).

**Observation.** Two methods. `run_blocking()` calls `asyncio.run(self.run())` internally, presumably. `run()` is the async version.

**Why it matters.** Modern Python has `asyncio.run(main())` as the established idiom for "block on this async function." httpx/FastAPI/Starlette don't ship a `run_blocking` variant — they let the user write `asyncio.run(...)`. Adding `run_blocking()` is a small DX win for "hello world" but a small cost for everyone else who has to wonder which to use.

**Recommendation.** Two options, in order of preference:
1. Drop `run_blocking()`. Hello-world becomes `asyncio.run(worker.run())` or, with anyio, `anyio.run(worker.run)`. Three more characters of boilerplate.
2. Keep both, but rename to `worker.run()` (async) and `worker.serve()` (blocking, mirroring Uvicorn's `uvicorn.run()` blocking entry point).

**Severity: Suggestion.**

---

### 3.17 [Suggestion] No `__repr__` discipline for Agent, Worker, Subscription

**Location:** Not addressed in the doc.

**Observation.** The design doesn't say what `repr(agent)` returns. For a Pydantic-AI-quality SDK, this matters — the REPL story rests on `__repr__` being legible.

**Recommendation.** Specify:
- `repr(agent)` → `Agent(name='weather-bot', model='openai:gpt-5.4', tools=2, output_type=WeatherReport)`
- `repr(worker)` → `Worker(bootstrap_servers='kafka:9092', agents=3, handlers=5, extensions=2)`
- `repr(subscription)` → `Subscription(source='news-watcher', payload=NewsArticle, filter=<lambda>)`

**Severity: Suggestion.**

---

### 3.18 [Suggestion] `model="openai:gpt-5.4"` literal-string form deserves `Literal[...]` typing

**Location:** Throughout examples.

**Observation.** Pydantic AI types `model` as `Model | KnownModelName | str | None` where `KnownModelName` is a giant `Literal[...]` union of every known model. This gives autocomplete and `mypy --strict` cleanliness while leaving the `str` escape hatch.

**Recommendation.** Adopt Pydantic AI's pattern. The `KnownModelName` literal is maintained upstream; we can re-export.

**Severity: Suggestion.**

---

### 3.19 [Nit] `Done()` vs `Reply()` semantic — `Done` produces no Reply; how does the caller know?

**Location:** §3.1 Action algebra table; §3.5 runtime interpretation.

**Observation.** `Done()` is "terminate this hop. No publish." If the calling client is waiting on a Reply, `Done()` from the called Handler leaves the client hanging. The doc says (§3.5) `Done` "Marks the Run as complete if there's no parent frame" but if there *is* a parent frame, the parent never re-enters.

**Why it matters.** Footgun. A user who returns `Done()` from a Handler that was reached via `Call` will see hung clients. The runtime should error at registration if a Handler can return `Done` *and* be reachable via `Call`.

**Recommendation.** Either error at registration (Handler `H` is called via `Call`; returns `Done`; reject), or make `Done` automatically synthesize an empty Reply when there's a frame to pop. Document explicitly.

**Severity: Nit.**

---

### 3.20 [Nit] §17.2 `wrk.execute(...)` vs `wrk.invoke(...)` vs `wrk.publish_event(...)` vs `wrk.published(...)` — naming sprawl on InMemoryWorker

**Location:** §17.2.

**Observation.** Four methods with `execute`, `invoke`, `publish_event`, `published`. The first two are basically synonyms (look at §5.2 row "InMemoryWorker"). The third is choreography; the fourth is an assertion helper.

**Recommendation.** Standardize on one verb pair:
- `wrk.invoke(agent_or_topic, req)` — request/reply.
- `wrk.emit(topic, payload)` — fire choreography event.
- `wrk.assert_published(topic, count=...)` — assertion helper, clear it's a test util.

Drop `execute`. Don't ship `published()` as a no-prefix method that can be confused with state.

**Severity: Nit.**

---

## 4. The from-scratch redesign

What I'd propose if I were designing Layer A from a Pydantic AI migrant's perspective, with calfkit-correct Kafka semantics under the hood.

### 4.1 Hello-world agent

```python
from calfkit import Agent, Worker
from pydantic import BaseModel

class WeatherReport(BaseModel):
    location: str
    summary: str
    temperature_c: float

weather_agent = Agent(
    name="weather-bot",
    model="openai:gpt-5.4",
    instructions="You help users find current weather and short-term forecasts.",
    output_type=WeatherReport,
)

@weather_agent.tool
async def get_weather(ctx: Context, location: str) -> dict:
    """Fetch current weather for a location."""
    ...

@weather_agent.tool
async def get_forecast(ctx: Context, location: str, days: int = 1) -> list[dict]:
    """Fetch a weather forecast for the next N days."""
    ...

# Local-dev / REPL: no Worker needed
result = await weather_agent.run("What's the weather in Berlin?")
print(result.output)

# Production
if __name__ == "__main__":
    worker = Worker(bootstrap_servers="kafka:9092")
    worker.add(weather_agent)
    asyncio.run(worker.run())
```

Five concepts at first touch: `Agent`, `Worker`, `@agent.tool`, `agent.run()`, `worker.add()`. Five imports. No `tool` at module level. No `Sub`, `Subscription`, `external_tool`, `Interrupt`, `Reply`, `Call`, `Fan` until the user reaches the feature.

### 4.2 Agent with cross-language tools

```python
from calfkit import Agent, Tool, Worker

execute_trade = Tool.external(
    name="execute_trade",
    description="Submit a buy or sell order.",
    input=BuyOrSellOrder,
    output=TradeConfirmation,
)

trading_agent = Agent(
    name="trader",
    model="openai:gpt-5.4",
    instructions="You execute trades on behalf of users.",
    output_type=TradeResult,
    tools=[execute_trade],  # external tools attach via tools=
)

@trading_agent.tool
async def fetch_quote(ctx, symbol: str) -> dict:
    """Get the latest market quote."""
    ...

@trading_agent.tool
async def fetch_portfolio(ctx, user_id: str) -> dict:
    """Get the user's portfolio."""
    ...

worker = Worker(bootstrap_servers="kafka:9092")
worker.add(trading_agent)
```

One import (`Tool`) added vs hello-world. `external_tool` function call replaced by `Tool.external(...)` classmethod. Python tools use the decorator; external tools use the class-method factory. Tools are a single concept, two construction paths.

### 4.3 Multi-agent: handoff and sub-agents

```python
billing_agent = Agent(
    name="billing-agent",
    model="openai:gpt-5.4",
    instructions="Handle billing questions.",
    output_type=BillingAnswer,
)
tech_agent = Agent(
    name="tech-agent",
    model="openai:gpt-5.4",
    instructions="Handle technical support questions.",
    output_type=TechAnswer,
)

triage_agent = Agent(
    name="triage",
    model="openai:gpt-5.4",
    instructions="Route user requests to the right specialist.",
    output_type=str,
)
triage_agent.handoffs_to(billing_agent, tech_agent)  # method, not constructor

# Sub-agents (research → write) — sub-agents are tools via .as_tool()
writer_agent = Agent(
    name="writer",
    model="openai:gpt-5.4",
    instructions="Write an article based on the research provided.",
    output_type=Article,
    tools=[research_agent.as_tool(name="research_topic")],
)

worker = Worker(bootstrap_servers="kafka:9092")
worker.add(billing_agent, tech_agent, triage_agent, research_agent, writer_agent)
```

`Agent.handoffs_to(...)` is a method on the agent instance, mirroring how Pydantic AI users wire handoffs after construction. Sub-agents are tools via `.as_tool(name=...)` — no separate `Sub(...)` type. `worker.add(*agents)` supports varargs.

### 4.4 Choreography (pub/sub subscription)

```python
# In a shared library — agents are portable
classifier = Agent(
    name="classifier",
    model="openai:gpt-5.4",
    instructions="Classify the input.",
    output_type=Classified,
)
enricher = Agent(
    name="enricher",
    model="openai:gpt-5.4",
    instructions="Enrich the classified record with external data.",
    output_type=Enriched,
)

# In main.py — deployment topology lives on the Worker, not on the Agent
worker = Worker(bootstrap_servers="kafka:9092")
worker.add(classifier, enricher)

worker.publish(classifier, as_event=Classified)              # classifier emits its output as an event
worker.wire(source=classifier, target=enricher, payload=Classified)
worker.wire(
    source=classifier,
    target=alerter,
    payload=Classified,
    filter=lambda ev: ev.urgency >= 4,
    start_from="earliest",
)

# Subscribing to a non-calfkit topic
worker.subscribe(
    audit_agent,
    topic="events.transactions.completed",
    payload=TransactionCompleted,
)

asyncio.run(worker.run())
```

All wiring lives on `worker`. Agents are pure declarations. Filter, react, start_from become kwargs on `worker.wire(...)` — no `Subscription` dataclass to construct manually.

The §11.B "Subscription as a dataclass to construct" form is preserved for the rare power-user case, but the common path is method calls on `worker`.

### 4.5 Human-in-the-loop

```python
@trading_agent.tool(requires_approval=True)
async def execute_trade(ctx, order: BuyOrSellOrder) -> TradeConfirmation:
    """Place the order via the exchange."""
    ...

# Client-side
handle = await client.invoke(trading_agent, req)
state = await handle.wait_for_interrupt()
if state.is_interrupted:
    decision = await ui.show_approval_modal(state.pending_tool_call)
    await client.resume(state.run_id, decision)
result = await handle.result()
```

HITL is *tool-level approval* (Pydantic AI's pattern). No `interrupts={...}` dict-of-named-rules. No `when=lambda` predicate over LLM output type. The agent emits `Interrupt` when the LLM picks a `requires_approval=True` tool; the caller resumes with the user's approval payload.

For the rare "interrupt on output type" case, an `@agent.interrupt_when` decorator:

```python
@approval_agent.interrupt_when(
    on=lambda output: isinstance(output, ApprovalRequired),
    resume_with=HumanDecision,
)
async def needs_review(output, ctx):
    # Optional: side-effect at interrupt time (notify someone, etc.)
    await ctx.deps.notifier.alert(output)
```

### 4.6 Extension / middleware

```python
from calfkit import Worker, Extension
from calfkit.extensions import OTelExtension, RetryExtension

class MyAuditExtension(Extension):
    async def around_invoke(self, req, call_next):
        # ...
        return await call_next(req)

    async def on_event(self, event):
        match event:
            case RunCompleted(run_id=rid, output=out):
                await self.audit_log(rid, out)
            case _:
                pass

worker = (
    Worker(bootstrap_servers="kafka:9092")
    .use(OTelExtension(), RetryExtension(max=3), MyAuditExtension())
    .add(triage_agent, billing_agent, tech_agent)
)
```

Builder-style. `worker.use(*extensions)` attaches extensions in onion order. Same three primitives (`around_invoke`, `around_publish`, `on_event`) as the current §12 design — that part is good and stays.

### 4.7 Worker deployment

```python
worker = Worker(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"],
    group_id_prefix="myapp-prod",
)
worker.add(triage_agent, billing_agent, tech_agent)
worker.use(OTelExtension(endpoint=os.environ["OTEL_ENDPOINT"]), RetryExtension(max=3))
worker.deps_factory(create_deps)
worker.runtime(
    runs_state_topic="calf.runs-state",
    fanout_topic="calf.fanout-agg",
    dlq_topic_pattern="calf.dlq.{source}",
    dedup="on",
    partition_strategy="run_id",
)
asyncio.run(worker.run())
```

Worker constructor is 2 kwargs. Methods add agents, extensions, deps, runtime config. The runtime config (the trio that 99% of users don't touch) lives in a dedicated `worker.runtime(...)` method that's discoverable but out of the way.

---

## 5. Specific dimensions

### 5.1 Imports and module organization

**Concern:** the design implies a large flat import surface (`from calfkit import Agent, Worker, tool, external_tool, Interrupt, Subscription, Sub, Context, handler, Reply, Call, Fan, Emit, Done, Fail, Extension, ...`). Verified against §4.2-§4.6.

**Recommendation.** Three namespaces:
- `calfkit` — primary surface for Layer A: `Agent`, `Worker`, `Tool`, `Context`, `Client`, `Subscription`.
- `calfkit.actions` — Action types for Layer B: `Reply`, `Call`, `Fan`, `Emit`, `Done`, `Fail`, `Interrupt`, `TailCall`, `Continue`.
- `calfkit.extensions` — built-in Extensions: `OTelExtension`, `RetryExtension`, `RateLimitExtension`, `AuditExtension`.

Pydantic AI does similar (`pydantic_ai`, `pydantic_ai.messages`, `pydantic_ai.tools`). FastAPI does similar (`fastapi`, `fastapi.middleware`, `fastapi.testclient`).

### 5.2 Naming

| Calfkit current | Recommended | Reason |
|---|---|---|
| `system_prompt=` | `instructions=` | Pydantic AI moved to `instructions`; legacy `system_prompt` is back-compat only |
| `Context[Deps, State]` | `Context[Deps]` | Pydantic AI / OpenAI Agents both use one generic param |
| `Sub(agent, as_tool="...")` | `agent.as_tool(name="...")` | Pydantic AI / OpenAI Agents pattern; no new type |
| `external_tool(...)` | `Tool.external(...)` | Same concept; classmethod form unifies with `@tool` |
| `Subscription(...)` | Drop as primary; use `worker.wire(...)` kwargs | Constructor-driven declarative wiring is overkill |
| `worker.run_blocking()` | `asyncio.run(worker.run())` | Modern Python idiom |
| `client.invoke("agent.x.in", req)` | `client.invoke(agent_x, req)` | Agent ref, not topic string |
| `Done()` | Keep but error on Call-reachable handlers | Footgun otherwise |
| `Fail` | Keep | Good |
| `Worker(handlers=[...])` | `worker.add(...)` only | One way to add a handler |

### 5.3 Type safety / generics

**Pydantic AI**: `Agent[AgentDepsT, OutputDataT]` — two type params, but they're inferred from the constructor's `deps_type=` and `output_type=` so users rarely write them.

**Calfkit recommended**: `Agent[DepsT, OutputT]` mirroring Pydantic AI. `Context[DepsT]` for the per-invocation context. Tool callables typed `Callable[[Context[DepsT], ...], Awaitable[OutputT]]`. `@tool` decorator preserves the signature.

**py.typed marker** — the design doesn't mention it. Critical for IDE/mypy support. Add `py.typed` to the package.

### 5.4 Async vs sync

The design commits to async-only (G8 in §1.4). I agree. Pydantic AI's `run_sync()` is a thin wrapper over `asyncio.run(self.run(...))` and users can write that themselves. The cost of maintaining sync parity in a Kafka SDK is high (every transport layer needs sync paths) and the audience is async-native anyway.

**Recommendation.** Keep async-only. Document `asyncio.run(...)` for blocking entry points.

### 5.5 Configuration

The §18.2 three-layer precedence (programmatic > env > config file) is correct. The implementation should use Pydantic-Settings (`pydantic_settings.BaseSettings`) — verified upstream library — for the env-var layer, which gives type-safe parsing for free.

**Recommendation.** Ship `WorkerSettings(BaseSettings)` as the source-of-truth schema. Env var prefix `CALFKIT_` (already in §18.2). Config file path overridable via `CALFKIT_CONFIG_FILE`.

### 5.6 Error handling

§15 is solid. One specific recommendation: the SDK needs a base exception hierarchy.

```python
calfkit.errors.CalfkitError              # base
    .ConfigError                          # bad worker config
    .TransportError                       # Kafka unavailable
    .EnvelopeDecodeError                  # bad envelope
    .HandlerError                         # base for handler-side errors
        .RetryableError                   # explicit "retry me"
        .ToolError                        # tool failure
        .ToolTimeoutError
        .ToolNotAllowedError              # used by ToolFilter Extension
        .ModelRetry                       # output validation retry signal
    .RunError                             # run-level
        .RunFailedError
        .RunInterruptedError
        .RunNotFoundError
    .ChoreographyError                    # subscription-level
        .OrphanedSubscriptionError
```

§15 mentions `RetryableError`, `RateLimitExceeded`, `CircuitOpen`, `ToolNotAllowed`, `ModelRetry`, `FanTimeoutError` — these should all live in `calfkit.errors` with a clear hierarchy.

**Recommendation.** Document the exception hierarchy as part of §15. Use `from None` and `raise ... from ...` chaining where appropriate.

### 5.7 Tool authoring

**Pydantic AI form:**
```python
agent = Agent("openai:gpt-5.4")

@agent.tool
async def get_weather(ctx: RunContext[Deps], location: str) -> dict:
    """Fetch current weather."""
    ...

@agent.tool_plain
def get_time() -> str:
    """Return the current time."""
    ...
```

**OpenAI Agents form:**
```python
@function_tool
def get_weather(location: str) -> dict:
    """Fetch current weather."""
    ...

agent = Agent(name="...", tools=[get_weather])
```

**Calfkit current form (§4.2):**
```python
@tool
async def get_weather(location: str) -> dict:
    """Fetch current weather for a location."""
    ...

weather_agent = Agent(name=..., tools=[get_weather, ...])
```

**Calfkit recommended form:**
```python
# Form 1 (primary): attached tool, Pydantic-AI-style
agent = Agent(name="weather-bot", ...)

@agent.tool
async def get_weather(ctx: Context, location: str) -> dict:
    """Fetch current weather."""
    ...

# Form 2 (secondary): portable tool, intended as a published Kafka topic
@calfkit.tool(topic="tool.get_weather")
async def get_weather(location: str) -> dict:
    """Fetch current weather."""
    ...
weather_agent = Agent(name=..., tools=[get_weather])

# Form 3 (external): cross-language tool
execute_trade = Tool.external(name="execute_trade", input=BuyOrSellOrder, output=TradeConfirmation)
```

Three forms, three intents. The current design has only the second form and is missing the first (which is the Pydantic AI default) and the third (which it calls `external_tool`, a separate function).

### 5.8 Extension/middleware API

§12 is good. Three primitives (`around_invoke`, `around_publish`, `on_event`). The `match`-on-`LifecycleEvent` dispatch is correct.

**One refinement:** the `req` object inside `around_invoke` / `around_publish` should be immutable; mutations should be via `req.with_(...)` (like httpx's `Request`) to avoid action-at-a-distance bugs.

### 5.9 Streaming

§1.5 punts streaming to post-v1. That is correct for v1.0 scope. But: the design should *plan* for it — the `Reply` Action and the wire format should leave room for `Stream` as a future variant. Document explicitly in §6.4 that "adding a `stream` action_kind is a non-breaking envelope change."

### 5.10 Logging

The design says §14 OTel-native by default, which covers structured logs via OTel logging bridge. But: there should be a `calfkit.logging` module that configures the SDK's loggers (`calfkit.runtime`, `calfkit.transport`, `calfkit.handler.<name>`, `calfkit.extension.<name>`) with reasonable defaults. The Anthropic SDK and Stripe-python both expose this — the user can `logging.getLogger("calfkit").setLevel(logging.DEBUG)`.

**Recommendation.** Document the logger names and their semantics in §14.

### 5.11 Testing ergonomics

§17 is solid. Two additions:

1. **`Agent.run(input)`** for in-process REPL/test use (§3.13 above). Lowers L1 test friction to one line.
2. **Snapshot testing for envelopes**. The L2 in-memory dispatcher should support `wrk.recorded_envelopes()` returning the list of envelopes emitted during a test, so users can snapshot-test the wire format. (`pytest-recording` or `syrupy` integration.)

### 5.12 Context object (ctx)

The design lists in §5.2: `ctx.state`, `ctx.deps`, `ctx.run_id`, `ctx.fan_results`, `ctx.resume_payload`, `ctx.trigger`, `ctx.once()`. That's seven attributes, two of which are method calls. Also implied: `ctx.tenant_id`, `ctx.llm(...)` (§11.B.8 mentions `ctx.llm(...)` for `react=`), `ctx.usage`, `ctx.frame_state`.

**Concern:** the Context object accumulates attributes the same way the constructors do. Pydantic AI's `RunContext` has six attrs: `deps`, `model`, `usage`, `prompt`, `messages`, `tracer`. That's a tight, well-defined surface.

**Recommendation.** Document `Context` as a single concrete API in §13 (currently scattered). Limit to ~8 attributes; group cleanly by purpose:
- Identity: `run_id`, `frame_id`, `tenant_id`
- Data: `state`, `deps`, `prompt`, `usage`
- Lifecycle: `trigger`, `fan_results`, `resume_payload`
- Helpers: `once()`, `llm()`

### 5.13 Subscription/wiring discoverability

`worker.inspect()` (§4.7 step 6) is the right primitive — make it strong:

```python
worker.inspect(format="text")     # human-readable
worker.inspect(format="dict")     # machine-readable
worker.inspect(format="dot")      # graphviz topology for docs

# Output (text):
# Worker (bootstrap=kafka:9092, group_prefix=myapp-prod)
#   Agents (3):
#     weather-bot
#       input:   agent.weather-bot.in
#       output:  WeatherReport
#       tools:   get_weather, get_forecast
#     news-watcher
#       input:   agent.news-watcher.in
#       events:  agent.news-watcher.events -> NewsArticle
#       output:  NewsArticle
#   Subscriptions (4):
#     summarizer <- news-watcher.events (payload=NewsArticle, group=myapp-prod.summarizer)
#     sentiment  <- news-watcher.events (payload=NewsArticle, group=myapp-prod.sentiment)
#     ...
#   Extensions (2):
#     OTelExtension
#     RetryExtension(max=3)
```

A `worker.inspect(format="dot")` output that can be rendered into a topology diagram would be a meaningful DX moment.

### 5.14 HITL

See §3.5. Primary form is `@agent.tool(requires_approval=True)`. Secondary form is `@agent.interrupt_when(...)`. Both compile to Action `Interrupt`. The dict-of-named-rules form should not ship.

---

## 6. Trade-offs the architect made that I'd reverse

Five concrete reversals, in priority order.

### 6.1 Reverse: in-Agent choreography wiring (`subscribes_to=`, `publishes=`)

**Architect chose:** dual-form choreography wiring — `Agent(subscribes_to=[...], publishes=...)` for tight coupling, `worker.wire(...)` for deployment-time.

**I'd choose:** `worker.wire(...)` only. Remove `subscribes_to` and `publishes` from Agent.

**DX argument:** the doc itself (§11.B.6) acknowledges in-Agent wiring is wrong for reuse. Two-way-to-do-it produces cargo culting. Pick the form that scales to production (deployment-time wiring) and remove the form that doesn't.

### 6.2 Reverse: module-level `@tool` as the only form

**Architect chose:** all tools registered via module-level `@tool` (calfkit/calfkit-go etc. share this style).

**I'd choose:** `@agent.tool` (Pydantic-AI form) as primary; `@calfkit.tool(topic=...)` as secondary for explicitly-portable cross-language tools; `Tool.external(...)` as factory for declarative external tools.

**DX argument:** Pydantic AI migrants see `@tool` at module level and parse calfkit as "different framework with Pydantic AI veneer." Layer A faithfulness violation. The two-form split maps to the user's intent ("attached" vs "portable").

### 6.3 Reverse: `interrupts={...}` dict-of-named-rules HITL

**Architect chose:** HITL via dict of `Interrupt(when=lambda output: ..., resume_with=...)` on the Agent constructor.

**I'd choose:** `@agent.tool(requires_approval=True)` (Pydantic-AI-style) as primary; `@agent.interrupt_when(on=..., resume_with=...)` as the rare "interrupt on output type" form.

**DX argument:** the dict-of-named-rules conflates name, condition, and resume payload type without using the name. The `when=` predicate is a lambda over LLM output — a footgun under at-least-once redelivery. Pydantic AI's tool-approval pattern is cleaner, well-precedented, and matches the actual mental model of "the LLM picked an action that needs approval."

### 6.4 Reverse: `Context[Deps, State]` two-parameter generic

**Architect chose:** `Context[Deps, State]` with two type parameters.

**I'd choose:** `Context[Deps]`, with state typed via the handler's signature or accessed as `Any` and validated by the user.

**DX argument:** both Pydantic AI (`RunContext[Deps]`) and OpenAI Agents (`RunContextWrapper[T]`) deliberately use one parameter. The State concept is real and important — it just doesn't need to be a type parameter on Context to be expressed.

### 6.5 Reverse: `system_prompt=` as primary kwarg

**Architect chose:** `system_prompt=` (and a `system_prompt=callable` dynamic form).

**I'd choose:** `instructions=` (matching Pydantic AI's current public API), with `system_prompt=` as a back-compat alias emitting `DeprecationWarning`.

**DX argument:** Pydantic AI itself moved to `instructions=`. OpenAI Agents uses `instructions=`. Calfkit using `system_prompt=` looks one major-version behind on day one of release.

### 6.6 Reverse: `Worker(handlers=[...], extensions=[...])` kitchen-sink

**Architect chose:** Worker constructor takes ~10 kwargs including handlers, extensions, deps_factory, runtime opts.

**I'd choose:** builder-style — `Worker(bootstrap_servers=..., group_id_prefix=...).use(...).add(...).deps_factory(...).runtime(...)`. Constructor takes 2-3 kwargs (transport identity); methods add the rest.

**DX argument:** Worker has the same kitchen-sink problem as Agent. Builder-style matches Starlette, FastAPI, and httpx patterns and gives discoverability via instance methods.

### 6.7 Reverse: `Sub(...)` as a new type

**Architect chose:** `Sub(other_agent, as_tool="...")` as a helper type for sub-agent composition.

**I'd choose:** `other_agent.as_tool(name=...)` as a method on the Agent instance.

**DX argument:** Pydantic AI's `Agent.as_tool()` method is the precedent. One less import. Self-documenting at the call site.

### 6.8 Reverse: client `invoke("agent.x.in", req)` topic-string surface

**Architect chose:** `client.invoke("agent.weather-bot.in", req)` — caller passes a topic string.

**I'd choose:** `client.invoke(weather_agent, req)` — caller passes an agent reference (or a `ClientAgentRef(name=..., output_type=...)` proxy if the agent isn't imported on the client side).

**DX argument:** Pydantic AI / OpenAI Agents both pass the agent object. Topic-string leaks naming convention into user code; refactor-hostile.

---

## 7. Trade-offs I'd keep

Decisions I would not reverse, with rationale.

- **Async-only (G8).** Correct call. Avoids dual-codepath maintenance.
- **No graph DSL (G6).** Correct. Pydantic AI also doesn't have one. Graphs are emergent.
- **No `dict[str, Any]` on the wire (G7).** Correct. Discriminated unions force clarity.
- **At-least-once + idempotency framing (G3, §16).** Correct. Exactly-once is a tarpit.
- **Per-source DLQ (§15.4).** Correct. Operator-friendly.
- **Action algebra closed set (§3).** Correct. The eight Actions cover every recipe.
- **Compacted-topic-backed fan-out aggregator (§9).** Correct and well-engineered.
- **Three-primitive Extension API (§12).** Correct rejection of the named-sugar hierarchy.
- **Opaque state bytes (§6.1, §7.2).** Correct. Sidesteps the `CompactBaseModel` footguns.
- **`run_id` distinct from `correlation_id` (§8.4).** Correct. Fixes a real 0.x bug class.
- **OTel-native (§14).** Correct.
- **Header-based multi-tenancy default (§19.1).** Correct.
- **Three-layer testing strategy (§17), with the InMemoryWorker = same-class-different-transport (§17.2) insight.** Correct.
- **`worker.inspect()` for topology introspection (§4.7, §5.13).** Correct. Make it stronger.
- **Choreography distinct Runs with `parent_run_id` lineage (§11.B.9).** Correct.
- **Direct aiokafka over FastStream (§20).** Correct.

---

## 8. Risk assessment — where users bounce off

Four user personas, in priority order, with the specific point at which I expect each to bounce.

### Persona 1: Pydantic AI migrant (the calfkit target audience)

**The walk:** reads the calfkit README. Sees "Pydantic-AI-class authoring DX + Kafka-native distribution." Pulls the SDK. Opens the hello-world.

**Bounce points (current design):**
1. `from calfkit import Agent, Worker, tool` (vs Pydantic AI's `from pydantic_ai import Agent`). Three imports for hello-world. **Mild.**
2. `system_prompt=...` instead of `instructions=...`. The user just learned `instructions` last month. **Moderate.**
3. `@tool` at module level instead of `@agent.tool`. **High** — this is the first decorator the user types and it doesn't match Pydantic AI.
4. `Agent.run(input)` is not directly available; user has to set up a Worker (or `InMemoryWorker`) to test locally. **High** — the REPL story is dead.
5. `Sub(other_agent, as_tool="...")` for sub-agents instead of `other_agent.as_tool()`. **Moderate.**

**Bounce rate (gut):** 60%+ of Pydantic AI migrants who open the hello-world will stop within the first 10 minutes if these issues aren't fixed. The whole pitch is "Pydantic-AI-class DX"; the hello-world is the proof. If the proof fails, no one reads §11.B.

**Bounce points after my recommended changes:** §1-§5 reads like Pydantic AI with a Worker added at the end. Migrants either continue or bounce on the Kafka concept itself (which is the legitimate filter — those users were never the audience).

### Persona 2: OpenAI Agents user

**The walk:** has been using OpenAI Agents SDK. Wants durability and cross-language tools. Calfkit positions on both.

**Bounce points (current design):**
1. `handoffs=[billing_agent, tech_agent]` is in the Agent constructor — matches OpenAI Agents shape. **Good.**
2. `Runner.run(agent, input)` shape isn't available; user has to learn `client.invoke("agent.<name>.in", req)`. **Moderate.**
3. `parallel_tools=[...]` vs OpenAI's tool-use behavior. Calfkit's surface is novel (and not better; see §3.1). **Moderate.**
4. Extensions API (§12) is different from OpenAI's `RunHooks` / `AgentHooks` — but better. The `match`-on-`LifecycleEvent` pattern is a credible upgrade once the user gets there. **Mild.**

**Bounce rate (gut):** 30%. Easier transition than from Pydantic AI because OpenAI Agents users are already accustomed to a heavier wiring surface.

### Persona 3: Distributed systems engineer coming from Temporal

**The walk:** runs Temporal in prod. Heard calfkit doesn't need a cluster. Skeptical but curious.

**Bounce points (current design):**
1. Discovers it's "just Kafka + library." Doesn't need Temporal. **Good** — calfkit's pitch resonates.
2. Reads §3 Action algebra. Likes it — closed set, idempotency contract per Action. **Good.**
3. Reads §9 fan-out aggregator. Critical evaluation: "is this actually durable across rebalance?" Walks through §9.6 and §9.8. **Good — the walked example is concrete enough.**
4. Reads §15 error handling. Notes no exactly-once. Compares mentally to Temporal's guarantee. **Acceptable, but flag.**
5. Hits §11.B choreography. **Surprised** — Temporal doesn't have this shape. **Positive surprise.**

**Bounce rate (gut):** 20%. This persona is patient with surface friction because the runtime guarantees matter more than the constructor signature. Calfkit is selling them on §3, §9, §12, §16 — the runtime spec — and those sections are strong.

### Persona 4: Framework-skeptical scripter ("I just want to ship an agent")

**The walk:** has heard the LangChain pain stories. Just wants to ship a thing. Looks at the hello-world.

**Bounce points (current design):**
1. 25 lines of import + agent + worker for hello-world (§4.2). **High** — they expected 10.
2. `worker.run_blocking()` works. **Good.**
3. Tests require `InMemoryWorker` setup. **Moderate.**
4. No `agent.run(input)` in-process. **High.**
5. Has to learn what "Kafka" means to deploy. **This is the legitimate filter; calfkit is not for them.**

**Bounce rate (gut):** 50%+ bounce on hello-world friction; the survivors bounce 70%+ when they realize Kafka is required. Calfkit will never be Pydantic AI for these users.

**Recommendation:** the README's first hello-world should be `await agent.run("hello")` (in-process, no Kafka). The Kafka deployment is the *second* example, framed as "now deploy this." This persona bounces less if they don't see Kafka until they need it.

---

## 9. Appendix: Pydantic AI / OpenAI Agents shapes for cross-reference

Verified current (May 2026) API shapes for the two reference SDKs calfkit Layer A targets:

### Pydantic AI

Constructor:
```python
Agent(
    model: Model | KnownModelName | str | None = None,
    output_type: OutputSpec[OutputDataT] = str,
    instructions: AgentInstructions[AgentDepsT] = None,
    system_prompt: str | Sequence[str] = (),    # legacy alias
    deps_type: type[AgentDepsT] = NoneType,
    name: str | None = None,
    tools: Sequence[Tool[AgentDepsT] | ToolFuncEither[...]] = (),
    # advanced opts elided
)
```

Tool decorator:
```python
@agent.tool
async def my_tool(ctx: RunContext[Deps], param: str) -> Output:
    ...

@agent.tool_plain
def my_tool_no_ctx(param: str) -> Output:
    ...
```

Run methods:
```python
result = await agent.run(input)
result = agent.run_sync(input)
async for chunk in agent.run_stream(input): ...
```

Context:
```python
class RunContext(Generic[AgentDepsT]):
    deps: AgentDepsT
    model: Model
    usage: Usage
    prompt: str
    messages: list[ModelMessage]
    tracer: Tracer
```

### OpenAI Agents SDK

Constructor:
```python
Agent(
    name: str,
    instructions: str | Callable[[RunContextWrapper[T], Agent[T]], MaybeAwaitable[str]] | None = None,
    handoffs: list[Agent[Any] | Handoff[T, Any]] = [],
    model: str | Model | None = None,
    output_type: type[Any] | AgentOutputSchemaBase | None = None,
    tools: list[Tool] = [],
    hooks: AgentHooks[T] | None = None,
    tool_use_behavior: Literal["run_llm_again", "stop_on_first_tool"] | StopAtTools | ToolsToFinalOutputFunction = "run_llm_again",
    # advanced opts elided
)
```

Tool form (module-level):
```python
@function_tool
def my_tool(param: str) -> Output:
    ...
agent = Agent(tools=[my_tool], ...)
```

Run methods:
```python
result = await Runner.run(agent, input)
result = Runner.run_sync(agent, input)
```

Sub-agent as tool:
```python
sub_tool = sub_agent.as_tool(tool_name="research", tool_description="...")
agent = Agent(tools=[sub_tool], ...)
```

### Key calfkit-target takeaways

- **`instructions=`** is now the primary kwarg for the system prompt in both Pydantic AI and OpenAI Agents. Calfkit should follow.
- **`handoffs=[...]`** is acceptable as a constructor kwarg (OpenAI Agents has it) but should be a *separate list* from `tools=[...]` — not mixed via `Sub(...)`.
- **Sub-agent as tool** is `agent.as_tool(name=...)` — an instance method. Both Pydantic AI and OpenAI Agents have this.
- **`Runner.run(agent, input)` vs `agent.run(input)`** is a real split between the two SDKs. Pydantic AI puts `.run()` on the agent; OpenAI Agents puts it on a separate `Runner`. Calfkit can do either, but should pick one and stick to it. Recommend `agent.run(input)` for parity with Pydantic AI (the primary migration target) — and route under the hood to the singleton in-process Worker. The Kafka deployment is `worker.run()` (different verb, different concept, no collision).
- **One generic parameter on Context** (`RunContext[Deps]`, `RunContextWrapper[T]`) is universal. Calfkit's `Context[Deps, State]` is a divergence to revert.
- **Tool decorators** differ: Pydantic AI puts `@agent.tool` on the instance; OpenAI Agents has module-level `@function_tool`. Calfkit can ship both — `@agent.tool` for the Pydantic AI shape, `@calfkit.tool` for the OpenAI Agents shape — provided they have visibly different semantics. The current design ships only the module-level form, which is the weaker of the two for the Pydantic-AI-migrant audience.

---

*End of review.*
