# Header-Based Route Dispatch — Design & Implementation Spec

**Status:** Proposed (design converged; spec rounds 1–2 reviewed & revised — v3)
**Builds on:** `calfkit/_registry.py` (`RegistryMixin` / `@handler`) — a shipped, tested
**primitive** that is **not yet wired into any node**. Integrating it into `BaseNodeDef`
is part of *this* work (see §8.0), not a precondition.
**Scope of v1:** `NodeDef` and `Agent` nodes.

> **v2 changelog** (round-1 review): payload is an explicit optional body field (not
> `input_args`); `RegistryMixin`→`BaseNodeDef` wiring made an explicit step; no-match
> severity keyed on `current_frame.callback_topic`; `route` on `Call` only; pass sentinel
> `Next`; `Agent` loop clarified as a valid implicit `*`; `HandlerInfo` reshape + test
> migration; guard purity a documented contract.
>
> **v3 changelog** (round-2 review — design confirmed converged; remaining were
> implementability tightenings): the **body→`CallFrame.payload` plumbing** is now fully
> specified (`invoke_frame` gains a `payload` param; client threads `body` through
> `_publish_call`); the **dispatch data structures** (`RouteEntry`, `call_run`, fallback
> synthesis) are defined in §7.1; **`accepts_payload := schema is not None`** is the single
> source of truth; the **`run()` fallback threads `input_args`**; **`handler_names()`** post-
> reshape is defined; the producer **usage example** uses real signatures; **`Next` exports
> from `calfkit.models`** beside `Silent`; the **body-schema skip logs at the
> `callback_topic`-aware level**; three error messages are specified to *teach*.

---

## 1. Summary

Today a node has one entrypoint body: `run(self, ctx)`. This feature lets a node declare
**multiple route-matched handlers** and dispatches an inbound message to them by a Kafka
header (`x-calf-route`), running them as a **Chain of Responsibility** from most-specific
route to most-general. It is **content-based routing** (EIP "Message Router"), with a
topic-pattern model closest to RabbitMQ topic exchanges / MQTT topic filters.

Two routing layers coexist and do not fight:
- **Topic → node** (existing): the Kafka topic addresses the node.
- **Route header → handler** (new): `x-calf-route` selects the handler(s) *within* the node.

Opt-in and backwards-compatible: a node with no `@handler` routes, or a message with no
route header, runs `run()` exactly as today.

---

## 2. Goals / Non-goals

**Goals**
- Declarative route-pattern handlers via `@handler("order.created")`.
- Hierarchical "narrow-as-you-subclass" dispatch: subclasses add more-specific routes that
  intercept first and may short-circuit or pass.
- FastAPI-style optional typed body injection: `(self, ctx, payload: Schema)`.
- A single-author state model with no silent data loss under the Kafka workflow-state machine.
- Explicit producer-side API to set routes and bodies (client ingress **and** peer `Call`).

**Non-goals (v1)**
- Routing on `ToolNodeDef` / `ConsumerNodeDef` (bespoke dispatch; left untouched — routing
  simply isn't wired into them; no class-definition guard added).
- "Around"/wrapping middleware (post-handler hooks). The chain is pre/guard-only.
- Full glob/regex grammar. Only exact + single trailing `*`.
- Pattern indexing (trie). Linear match over a node's handlers is fine at expected N.

---

## 3. Terminology

| Term | Meaning |
|---|---|
| **Route key** | The concrete, wildcard-free `.`-delimited string on a message, e.g. `order.created.line`. Set by the **producer**. |
| **Route pattern** | The string in `@handler(...)`: an exact key or a single trailing-`*` prefix, e.g. `order.*`. Held by the **consumer**; also the registry's unique/collision key. |
| **Body / payload** | An optional, producer-supplied object validated against a handler's `schema=` and injected as `payload`. Distinct from run-args (`input_args`). |
| **Match chain** | The patterns of a node that match a given route key, ordered specific → general. For trailing-prefix patterns this is a nested chain with no ties. |
| **Guard / pass-through** | A matched handler that returns `Next` (declines) instead of a control-flow result. Must not mutate `ctx.state`. |
| **Terminal handler** | The matched handler that returns a control-flow result (`Call`/`ReturnCall`/`TailCall`/`Silent`) and ends the chain; sole author of published `state`. |
| **Fallback** | The implicit `*`: the node's overridden `run()` (for `NodeDef` a user body; for `Agent` the framework loop), or an explicit `@handler("*")`. |

---

## 4. Locked decisions (with rationale)

1. **`@handler` positional = route pattern**, and the registry's unique/collision key. A
   separate optional `name=` (defaulting to the method `__name__`) is the human/debug
   identity, so wildcard strings don't pollute introspection.
2. **G2 grammar:** exact, or a single trailing `*`. `order.*` matches segments strictly
   **below** `order` — **not** bare `order`. The lone `*` is the universal catch-all (and
   *does* match bare `order`). Validated at decoration time.
3. **Chain of Responsibility, specific → general, short-circuit.** First non-`Next` result
   wins, is published, and stops the chain.
4. **Pure route-or-pass guards (single state author).** A pass-through handler is a
   *decision only* — by contract it must not mutate `ctx.state`. Exactly one terminal
   handler authors the published state. Rationale: `_publish_action` builds the publish
   envelope from the **action's** `state`, so shared "mutate-and-pass" would silently drop a
   guard's mutation under reassignment and complicate redelivery idempotency. **Enforcement:
   documented contract only (no runtime check) for v1** — guards are pure decisions; the
   residual risk is accepted (see §13).
5. **Dedicated `Next` sentinel** advances the chain. `Silent` keeps its existing terminal
   meaning (and "end of event-stream" warning). The two are never conflated. The name `Next`
   also documents the guard's no-state-change contract.
6. **Explicit `schema=`** drives body validation (one `model_validate` call; no annotation
   inference / `get_type_hints`). The `payload:` annotation is for the developer's
   type-checker only. `schema` must be a Pydantic `BaseModel` subclass — validated at
   decoration time (today `HandlerInfo.schema` is `Any`/opaque; this narrows it).
7. **Body validation failure → log + skip to the next matching handler** (treated as an
   implicit `Next`, not a hard error). `schema` thus acts as a *secondary filter* beyond the
   route pattern.
8. **Route header is ingress-only, never auto-propagated.** Set only by (a) client ingress
   and (b) an explicit peer `Call(..., route=...)`. `ReturnCall`/`TailCall`/implicit
   publishes carry no route, so returns dispatch via the `*`/`run()` fallback. This prevents
   a routeless tool `Call` from vanishing under "no-match → skip". Structurally safe today:
   outbound headers are built fresh from `_emitter_headers()` and never echo inbound, so
   leakage can't happen by accident — `route` is only added at the explicit `Call` publish.
9. **No-match severity by *callback presence*, not callstack depth.** `WARNING` when
   `current_frame.callback_topic is not None` (a caller awaits a return → stuck workflow),
   `DEBUG` when it is `None` (fire-and-forget). (The callstack is never empty at dispatch —
   the caller pushes a frame before publishing — so depth is the wrong signal.)
10. **Migration / fallback (uniform rule):** the implicit `*` fallback is the node's
    **overridden `run()`**, detected by identity (`type(node).run is not BaseNodeDef.run`).
    - `NodeDef`: a user-authored `run()` is the `*` fallback (or none → unmatched routes skip).
    - `Agent`: the framework loop (`BaseAgentNodeDef.run`) *is* an overridden `run()`, so it
      is the implicit `*` — routed handlers intercept first, then fall through to the loop.
      This is intended, not a special case.
    - Defining **both** an overridden `run()` **and** an explicit `@handler("*")` is a
      `RegistryConfigError` (ambiguous catch-all). On `Agent` this means an explicit
      `@handler("*")` is always an error — the loop is already your `*`.
11. **Scope v1 = `NodeDef`/`Agent`.** `ToolNodeDef`/`ConsumerNodeDef` are not wired for
    routing and are otherwise unchanged (no class-definition guard).

---

## 5. Developer-facing API

### 5.1 Declaring routes

```python
class OrderNode(NodeDef):
    @handler("order.created.line", schema=OrderLine)        # exact route + typed body
    async def on_line(self, ctx, payload: OrderLine) -> NodeResult[State]:
        ...                                                  # terminal: returns a control-flow result

    @handler("order.*")                                      # prefix guard, no body
    async def on_any_order(self, ctx) -> NodeResult[State] | Next:
        if not self._mine(ctx):
            return Next()                                    # decline → next handler
        return ReturnCall(state=ctx.state)

    async def run(self, ctx) -> NodeResult[State]:           # implicit "*" fallback
        ...
```

- Handler signature is `(self, ctx)` **or** `(self, ctx, payload)`. Arity is introspected on
  the **unbound function** at collection (threshold `len(params) > 2`, mirroring
  `_run_accepts_input`; use the registry's existing `__func__` unwrap), separate from `run`.
- **The `schema`/param pairing is enforced at `@handler` decoration / collection, *before*
  any `RouteEntry` is built**, so the dispatcher's invariant holds (§7.1): a
  `(self, ctx, payload)` handler **requires** `schema=`; a `(self, ctx)` handler must **not**
  have `schema=`. Mismatch → `RegistryConfigError` at class definition. Consequently
  `accepts_payload` is defined as **`schema is not None`** (single source of truth), and the
  arity check merely asserts the signature agrees.
- Return type is `NodeResult[State] | Next`. Only `Next()` continues the chain.

### 5.2 The `Next` sentinel

```python
@dataclass
class Next:
    """Routing control: this handler declines (no state change, no publish); advance the
    chain to the next, more-general matching handler. NOT a publish action — never reaches
    _publish_action and is intentionally absent from the NodeResult union."""
```

- Lives in `calfkit/models/actions.py` beside `Silent`, and is **exported from
  `calfkit.models`** alongside `Silent`/`Call`/`ReturnCall` (their established import home) —
  *not* lifted to top-level `calfkit` alone, so a handler body imports its return vocabulary
  from one place. (Lifting the whole control-flow vocabulary to top-level is a separate,
  out-of-scope decision.)
- **Not** added to the `NodeResult` union (which feeds `_publish_action`). Routed bodies are
  typed `NodeResult[State] | Next`; the dispatcher consumes `Next` and only ever hands a
  `NodeResult` to `_publish_action`.

### 5.3 Producer-side route + body API

Producers set **concrete route keys** (no wildcards, validated at call time) and an optional
**body**. The producer surface has two positional shapes but a shared `route=`/`body=` keyword
tail (real signatures):

```python
# Client ingress (invoke_node is (user_prompt, topic, *, ...)):
await client.invoke_node(
    "process this order", topic="orders",
    route="order.created.line", body=OrderLine(...),
)
# Peer call (Call is (target_topic, state, *input_args, ...)):
return Call("orders", state, route="order.created.line", body=OrderLine(...))
```

**Client ingress** — add keyword-only `route: str | None = None` and `body: Any | None = None`
to each overload and impl of `invoke_node`, `emit_to_node`, `execute_node`
(`client/client.py`), threaded through `_invoke`/`_emit` → `_publish_call`
(`client/base.py`), which is the single site that stamps `x-calf-route` **and** constructs the
`CallFrame` — so it sets `payload=body` directly there. The client validates `route` is
wildcard-free (error message: *"route keys must be concrete; `order.*` is a pattern for
`@handler`, not a producer route key"*). `route`/`body` apply to `emit_to_node` too; an
unrouted emit is `DEBUG`-dropped on no-match (§9). A `body` with no `route` is logged
(`DEBUG`/`WARNING`) as "will not reach any schema handler", not raised.

**Peer `Call`** — `route`/`body` live on **`Call` only** (not the shared `_Call`, so
`TailCall` cannot accept them). Add a keyword-only `route` and `body` to `Call`:

```python
class Call(_Call[StateT]):
    def __init__(self, target_topic, state, *input_args, route=None, body=None):
        super().__init__(target_topic, state, *input_args)
        self.route = route
        self.body = body
```

`_publish_action` adds the route header and body **only at the `Call`/`list[Call]` publish
sites**:

```python
headers = self._emitter_headers()
if call.route is not None:
    headers = {**headers, HDR_ROUTE: call.route}
# body travels per §5.4
```

`ReturnCall`/`TailCall` neither accept nor carry a route/body.

### 5.4 Where the body travels

The body is an optional field on the **call frame** (`CallFrame.payload: Any | None = None`),
mirroring how `input_args` rides today and kept distinct from run-args. `CallFrame` is
`frozen`, so `payload` is always a **constructor argument**, set at the two frame-construction
sites:

- **Client ingress:** `_publish_call` already builds the `CallFrame` directly — it sets
  `payload=body` there.
- **Peer `Call`:** the frame is built by `WorkflowState.invoke_frame(call, callback_topic)`.
  Give `invoke_frame` an optional `payload` parameter; in `_publish_action`, pass
  `call.body` at the `Call` and `list[Call]` (parallel fan-out) branches, and `None` at the
  `TailCall` branch (so `TailCall` stays body-free). `invoke_frame`'s param stays typed
  against `_Call`; read the body as `getattr(call, "body", None)` since `body` lives on
  `Call` only.

A schema handler validates `current_frame.payload`; producers that send no body leave it
`None` (schema handlers then skip — see §7).

---

## 6. Routing semantics

### 6.1 Grammar (validated at decoration time)

`.`-delimited, non-empty segments. `*` is legal **only** as the entire final segment, or as
the lone universal `*`. Rejected at decoration (`ValueError`; re-checked at collection as
`RegistryConfigError`): empty pattern, empty segment (`a..b`), leading/trailing `.`,
`*` mid-pattern (`a.*.b`), partial-segment `*` (`ord*er`), `**`.

### 6.2 Matching (route key `K` vs pattern `P`)

- `P == "*"` → matches any `K` (including bare `order`).
- `P` exact → matches iff `P == K`.
- `P == "<prefix>.*"` → matches iff `K`'s segments **start with** `prefix`'s segments **and**
  `K` has strictly more segments than `prefix`. Segment-aware (so `order.*` matches
  `order.created` but **not** bare `order` and **not** `orders.created`).

**Worked: what matches the bare key `order`?** the exact `@handler("order")` and the lone
`@handler("*")` — but **not** `@handler("order.*")`.

**Worked: `order.created.line`** matches `order.created.line` (exact), `order.created.*`,
`order.*`, and `*` — a nested chain.

### 6.3 Specificity ordering (specific → general)

Rank by **number of fixed (non-`*`) segments**, descending; at equal fixed-count an exact
pattern precedes a prefix pattern. For any key the matching trailing-prefix patterns are
nested prefixes of the key → a total order with **no ties by construction** (so the matcher
sorts by the specificity key without needing a tie-break).

```
order.created.line   (exact,  fixed=3)   ← 1st
order.created.*      (prefix, fixed=2)   ← 2nd
order.*              (prefix, fixed=1)   ← 3rd
* / run()            (fallback, fixed=0) ← last
```

---

## 7. Dispatch algorithm (in `BaseNodeDef.handler`)

Replaces the hardcoded `self.run(ctx)` call.

```
route = decode_header_str(headers.get(HDR_ROUTE))

ctx = prepare_context(...)
if not evaluate_gates(ctx): return Response(envelope, ...)     # gates: node-level, once, unchanged

if route is None or node has no registered routes:
    output = await call_run(ctx)                               # legacy path, unchanged
else:
    chain = node.routes_for(route)        # specific→general; appends the */run() fallback LAST if one exists
    output = None
    for entry in chain:
        if entry.accepts_payload:                              # entry.schema is not None
            try:
                payload = entry.schema.model_validate(current_frame.payload)
            except ValidationError:
                level = WARNING if current_frame.callback_topic is not None else DEBUG
                log.log(level, "route=%s handler=%s body failed schema; skipping", ...)
                continue                                       # validation failure → next handler
            result = await entry.invoke(ctx, payload)
        else:
            result = await entry.invoke(ctx)                   # the run() fallback entry threads input_args (§7.1)
        if isinstance(result, Next):
            continue
        output = result                                        # first non-Next = terminal; short-circuit
        break
    if output is None:                                         # no match, or an all-Next chain with no run() fallback
        level = WARNING if current_frame.callback_topic is not None else DEBUG
        log.log(level, "no route handler handled route=%s on node=%s; registered=%s", ...)
        return Response(envelope, ...)                         # envelope unchanged, nothing published

body = await _publish_action(output, envelope, correlation_id, broker)   # unchanged; sees only NodeResult
return Response(body, headers=self._emitter_headers())
```

Notes:
- **`output is None` reachability:** when an overridden `run()` is the fallback it is appended
  last and *always* returns a terminal `NodeResult`, so `output is None` is **unreachable** in
  that case. The skip path applies only to (a) no fallback at all (a `NodeDef` with routes but
  no `run()`), or (b) an all-`Next` chain whose fallback is itself a `@handler("*")` guard.
- **Chain atomicity:** an exception from any handler propagates exactly as a `run()` exception
  does today; the whole chain aborts with **no partial publish**.
- **Single state author:** only the terminal handler's `output.state` reaches `_publish_action`.
- **Redelivery:** guards are pure decisions (idempotent); exactly one terminal handler runs →
  redelivery safety equals today's single-`run()` model.
- **Body-schema skip is observable only via the log above** (now `callback_topic`-aware), not
  via `route_table()`/`routes_for()` — those answer "would this handler match by *pattern*",
  but schema validation is per-message (depends on the actual body), so a near-miss skip can't
  be introspected statically. Documented in §13.

### 7.1 Dispatch data structures

```python
@dataclass(frozen=True)
class RouteEntry:
    pattern: str                       # the @handler route, or "*" for the synthesized fallback
    invoke: Callable[..., Awaitable]   # bound dispatch (see below)
    schema: type[BaseModel] | None     # None ⇒ no body
    accepts_payload: bool              # == (schema is not None); the §5.1 pairing rule keeps arity in sync
```

- `routes_for(key)` returns the matched `RouteEntry`s ordered specific→general (§6.3), then —
  **if the node has an overridden `run()`** — appends a synthesized **fallback entry**
  (`pattern="*"`, `schema=None`) whose `invoke` is `call_run` (below). If there is no
  overridden `run()` and no explicit `@handler("*")`, no fallback is appended (→ the chain can
  end with `output is None` → skip).
- A normal entry's `invoke(ctx[, payload])` calls the bound handler method (resolved via the
  registry's `handlers()` bound-method map): `method(ctx)` or `method(ctx, payload)`.
- `call_run(ctx)` **is** today's arity-aware `run` block (`base.py:313-316`):
  `run(ctx, *current_frame.input_args)` when `_run_accepts_input and input_args is not None`,
  else `run(ctx)`. This preserves run-args on the fallback path (a routed message that falls
  through to `run()` keeps its `input_args`, matching the non-routed path). The legacy
  no-route branch (`output = await call_run(ctx)`) uses the same function.

### 8.0 Wire `RegistryMixin` into nodes (prerequisite, part of this work)

`RegistryMixin` is shipped/tested but unused by nodes (`BaseNodeDef(BaseNodeSchema,
LifecycleHookMixin)` does **not** inherit it). Add `RegistryMixin` to `BaseNodeDef`'s bases
and **reconcile the two `__init_subclass__`**: `BaseNodeDef.__init_subclass__` (sets
`_run_accepts_input`) and `RegistryMixin.__init_subclass__` (collects routes) must both run
via cooperative `super().__init_subclass__(**kwargs)`. Verify MRO/dataclass interplay
(`BaseNodeSchema` is a `@dataclass`).

### 8.1 Registry reshape (`_registry.py` / `@handler`)

- Decorator → `@handler(route, *, schema=None, name=None)`. `HandlerInfo` gains `route` (the
  collision key) and keeps `name` (defaults to `func.__name__`); add per-handler
  `accepts_payload` (via `inspect.signature`). `schema` narrowed to `type[BaseModel] | None`,
  validated at decoration.
- **`_handlers` keys on `route`** (was `name`): `_handlers: dict[route -> attr]`, collision/
  uniqueness key on `route`. Introspection is two honest accessors: `routes() -> tuple[str, ...]`
  returns the route patterns (the `_handlers` keys — this **renames** the old `handler_names()`,
  which had become a misnomer once the key was a route), and `route_table() -> dict[route, HandlerInfo]`
  returns the rich view (the human `name` — defaulting to the method name — and `schema` live
  here). No separate `handler_names()`. `name` resolution moves **inside** `_decorate` (where
  `func` is available); the current early `if not name: raise` guard is repurposed to validate
  the `route` positional.
- Grammar validation (§6.1) at decoration + collection.
- Add `routes_for(key) -> list[RouteEntry]` (§7.1) and `route_table()` introspection.
- `schema` narrowed to `type[BaseModel] | None` and validated at decoration (today it is `Any`
  and opaque).
- **Test migration** (`tests/test_registry.py`), mechanical but enumerate it: (a) collision/
  ordering/`handler_names()` assertions move from name-keyed to route-keyed (existing single-
  segment literals like `"alpha"`/`"dup"` pass the grammar, so the matcher won't reject them);
  (b) the `schema=object()`/`schema=<sentinel>` tests must use a real `BaseModel` subclass now
  that `schema` is narrowed + validated; (c) `HandlerInfo(...)` construction/equality tests
  must be rebuilt for the new field set (`route`, `name`, `schema`, `accepts_payload`).

### 8.2 Other files

| File | Change |
|---|---|
| `calfkit/_protocol.py` | Add `HDR_ROUTE = "x-calf-route"`. |
| `calfkit/models/actions.py` | Add `Next` dataclass. Add keyword-only `route`/`body` to **`Call`** (override `__init__`); leave `_Call`/`TailCall`/`ReturnCall` route-free. |
| `calfkit/models/session_context.py` | Add `CallFrame.payload: Any \| None = None`. Add a `payload` parameter to `WorkflowState.invoke_frame(call, callback_topic, payload=None)` and pass it into the constructed `CallFrame` (frozen → constructor arg). |
| `calfkit/nodes/base.py` | `handler()` gains the §7 dispatch (incl. `call_run` for legacy + fallback). `__init_subclass__`: per-handler payload arity; detect `cls.run is not BaseNodeDef.run` as the implicit `*`; raise `RegistryConfigError` on overridden-`run()` + explicit `@handler("*")` (message: *"`@handler('*')` conflicts with the node's `run()` catch-all; use a more specific route, or override `run()`"*). `_publish_action`: at the `Call` and `list[Call]` branches, add `HDR_ROUTE` from `call.route` and pass `getattr(call, "body", None)` into `invoke_frame(..., payload=...)`; the `TailCall` branch passes no route/body. |
| `calfkit/client/client.py` | Add keyword-only `route`/`body` to `invoke_node`/`emit_to_node`/`execute_node` (overloads + impls); validate `route` wildcard-free with the teaching message (§5.3). |
| `calfkit/client/base.py` | Thread `body` through `_invoke`/`_emit` → `_publish_call`; `_publish_call` stamps `x-calf-route` and sets `payload=body` on the `CallFrame` it constructs. |
| `calfkit/exceptions.py` | Reuse `RegistryConfigError`; messages cover grammar, route collision, run/`*` ambiguity, payload/arity mismatch — each phrased to teach. |
| `calfkit/models/__init__.py` | Export `Next` beside `Silent`. (Top-level `calfkit` export only if lifting the whole control-flow vocabulary — out of scope.) |

`RegistryMixin` stays generic; routing concepts (grammar, matching, ordering, `Next`,
dispatch) layer on top — matching/ordering can live in `calfkit/nodes/_routing.py`.

---

## 9. Migration & backwards-compatibility

- No `@handler` routes → behaves exactly as today (`run()` only).
- No `x-calf-route` header → runs `run()` (the implicit `*`).
- Adding routes is additive: `run()` becomes the `*` fallback; more-specific routes intercept.
- Tool `Call`s carry no route → reach `ToolNodeDef` unchanged.

---

## 10. Edge cases & failure modes

| Case | Behavior |
|---|---|
| No route header | Run `run()` (implicit `*`). |
| Route matches nothing, no fallback | Skip; `WARNING` if `current_frame.callback_topic` set, else `DEBUG`. Envelope unchanged. |
| All matched handlers `Next` (only when fallback is itself a `@handler("*")` guard) | Same as no-match skip. With a `run()` fallback this is unreachable (run is always terminal). |
| Body fails `schema` | Log + skip to next handler. Level is `callback_topic`-aware (`WARNING` if a caller waits, else `DEBUG`) — a schema near-miss on a waiting workflow is loud. |
| `schema` set but frame `payload` is `None` | `model_validate(None)` raises (always, even for all-optional models) → skip. (Exactly one `model_validate` call; no special-case `if payload is None`.) |
| Handler raises | Propagate; whole chain aborts, no partial publish (as `run()` today). |
| Terminal returns `Silent` | Existing terminal `Silent` warning; nothing published. |
| Malformed inbound route (empty segment / trailing dot) | Route into the fallback chain: if a `*`/`run()` fallback exists, it handles; if not, the standard no-match skip (callback-aware level) applies. Either way log `WARNING`; never partial-match. |
| Duplicate route pattern in a class | `RegistryConfigError` at class definition (collision on route). |
| Overridden `run()` + `@handler("*")` | `RegistryConfigError` (ambiguous catch-all; on Agent the loop is always the `*`). |
| `(self, ctx, payload)` without `schema=`, or `schema=` without a payload param | `RegistryConfigError` at class definition. |
| Producer route contains a wildcard | `ValueError` at call/construction. |

---

## 11. Testing strategy (TDD)

Add `tests/test_route_dispatch.py` + matcher unit tests; migrate `tests/test_registry.py`
(§8.1).

- **Matcher (pure):** exact; `order.*` below-but-not-bare `order`; segment-safety
  (`order.*` ∤ `orders.created`); `*` universal incl. bare; specificity & no-ties; grammar
  rejection (`a.*.b`, `**`, `ord*er`, empty/trailing).
- **Dispatch:** specific-first; `Next` advances; first control-flow result short-circuits;
  state comes only from the terminal; no-match skip with correct level (`callback_topic`
  set vs `None`); exception aborts chain with no publish; `Silent` terminal path; all-`Next`
  with a `@handler("*")` guard fallback.
- **Body:** valid body injected & typed; invalid body → skip to next; `None` payload skip;
  `(self,ctx)` vs `(self,ctx,payload)` arity; payload-without-schema and schema-without-param
  raise at class def.
- **Migration / fallback:** no-route → `run()`; no-routes node unchanged; `run()` as `*`;
  `run()`+`@handler("*")` raises; **Agent** loop as `*` (routed handler intercepts, then
  falls through to the loop); explicit `@handler("*")` on Agent raises.
- **Header/body lifecycle:** client `route=`/`body=` stamp header + frame payload; peer
  `Call(route=, body=)` stamps; `ReturnCall`/`TailCall` carry neither; tool `Call` unaffected.
- **Producer validation:** wildcard route to a producer raises.

`make fix && make check` (ruff, ruff-format, mypy strict) green before PR.

---

## 12. Open items / future

- **Sentinel naming — decided: `Next`** (round-2 DX). Chosen over `Next` because the semantic
  is directional ("decline, hand off to the next handler" — the Express/Koa/ASP.NET `next`
  gesture) and reads at maximal distance from `Silent` (advance vs. stop). The no-mutation
  contract lives in the sentinel's docstring.
- Tool/Consumer routing (explicitly deferred).
- Around/wrapping middleware (post-handler phase) — out of scope.
- Trie/index for large route tables (only if N grows).
- Annotation-inferred body typing (deferred; `schema=` is the v1 source of truth).
- Runtime enforcement of guard purity (deferred; documented contract for v1 — §13).

---

## 13. Risk register

| Risk | Mitigation |
|---|---|
| Guard mutating `ctx.state` (breaks single-author) | **Documented contract only for v1** (decision #4). Residual risk accepted: a buggy in-place mutation in a guard would ride the terminal's publish. Revisit a read-only state view if it bites. |
| Route header leaking across hops | Ingress-only; `route` only added at the explicit `Call` publish; outbound headers built fresh; covered by tests. |
| Silent mis-routing from a typo'd pattern | Decoration-time grammar validation; `route_table()` introspection. |
| Handler skipped by body-schema mismatch, invisibly | Skip log is `callback_topic`-aware (§10). Note: only the log reveals it — `routes_for()` answers *pattern* match, not per-message *body* validity. |
| Stuck workflow on no-match with a waiting caller | `callback_topic`-aware `WARNING` log (detection, not mitigation — a no-match still doesn't reply). |
| Redelivery re-running side effects | Pure guards + single terminal handler = parity with today. |
| Breaking shipped `tests/test_registry.py` during the registry reshape | Migrate tests in lockstep with the `HandlerInfo` route-key change (§8.1). |
```
