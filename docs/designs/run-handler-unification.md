# Run/Handler Unification — Design & Implementation Spec (ADR)

**Status:** Proposed — v3 (review-converged; **MCP-removal gate CLEARED**, rebased onto `main` 0.7.0 — ready for Stage 1 TDD)
**Date:** 2026-06-08 (re-verified against `main` after PRs #197/#198 removed MCP)
**Branch:** `feat/run-handler-unification` (worktree off `main`)
**Builds on:** `calfkit/_registry.py` (`RegistryMixin` / `@handler`) and the shipped
header-route-dispatch feature (PR #195).
**Amends:** `docs/designs/header-route-dispatch-spec.md` decisions **#10** (run() fallback
detected by identity) and **#11** + §3/§5.4/§7.1 (the `input_args` vs `payload` split).
**Prerequisite — MCP deleted first (separate PR):** the entire `calfkit/mcp/**` + `tests/mcp/**`
subsystem is being removed as a standalone hard-break PR (its own session). **This branch rebases
onto that mcp-free `main` before implementation.** Consequently there is **no `McpBridge`** (so no
import-time pairing failure), **no `worker.py`↔mcp coupling**, and **no `tests/mcp/**`** — the v2
`worker.py`-decouple section and the mcp-coupling note **no longer apply and are dropped** (§6.14 /
§10.1). Post-rebase, `agent.py`'s tools API is already mcp-free (`McpServer` gone from `ToolLike`),
so our `Call`-site changes (§6.4) layer cleanly on top. The only node types with an extra-positional
`run` then are `ToolNodeDef` (migrated) and a test node (`_TerminalNode`, fixed) — see §7.

> **v3 changelog (round-2 convergence):** Round 2 verified all round-1 fixes landed and found the
> remaining non-converged area was *only* the `worker.py`↔mcp seam (in-scope provisioning tests
> broke; the "`import calfkit` is safe" claim was false). **Resolution: MCP is deleted wholesale in
> a separate prerequisite PR** (user decision), eliminating that seam entirely — so §6.14/§10.1 are
> dropped and the spec converges on rebase. Also fixed two mcp-independent cosmetics the review
> flagged: restored the `produced action=` debug log in the §6.3g sketch; reconciled the deferred-
> F2-lint size estimate. **The dispatch/registry/typing/tool-payload core is review-converged** (no
> open critical/major issues) per two independent rounds.
>
> **v2 changelog (post-review):** **Critical fix** — the original 3-stage rollout was unbuildable
> (decorating base `run` as `@handler('*')` makes the un-migrated `ToolNodeDef`/`McpBridge` fail
> the pairing check at *import*, and a routeless body is only *delivered* under the unified
> dispatch); **Stages 1+2 are collapsed into one atomic stage** (§9). Added: `_routing.py`/
> `_dispatch_routed` `str | None` typing widening (§6.13a) — `make check` blocker the v1 spec
> missed; full test-migration list incl. ~11 direct `run(ctx, str)` sites + 4 named tests (§7);
> `worker.py` mcp decouple (§6.14); ruff F401 `Sequence` cleanups (§7). Decisions: **F2 = pass**,
> recorded as a **known temporary shortcoming** to be fixed by the concurrent error-propagation
> work (§5 F2, §13); **F1b unconsumed-body log made callback-aware** (WARNING when a caller
> awaits) instead of flat DEBUG (§6.3g).

---

## 1. Context & problem

Today a node has **two** ways to handle an inbound message, with **two** calling conventions:

| Path | Argument source | Call shape | Dispatched by |
|---|---|---|---|
| `@handler(route, schema=)` | `CallFrame.payload` (schema-validated) | `m(ctx)` / `m(ctx, payload)` | registry chain (`_dispatch_routed`) |
| `run()` | `CallFrame.input_args` (positional splat) | `run(ctx)` / `run(ctx, *input_args)` | hardcoded special case |

`run()` is **not** a registry entry. It is special-cased *by identity* (`type(node).run is not
BaseNodeDef.run`) in two places in `calfkit/nodes/base.py`:

- `handler()` has an `else:` branch that calls `_call_run(ctx, frame.input_args)` directly when
  there is no route header / no registered routes.
- `_dispatch_routed()` appends `run()` as an implicit `*` fallback via the same identity check.

This produces the asymmetry we want to remove: handlers flow through the registry; `run()` does
not. The `input_args` channel exists **solely** to carry one value — a tool node's
`tool_call_id` — and nothing else in non-vendored code uses it semantically.

### 1.1 The constraint that bounds the change

This is **not** an elimination of the per-invocation channel — it is a **channel swap**
(`input_args` → `payload`). Proof: in parallel tool dispatch the agent fans out one `Call` per
tool call, each carrying `ctx.state.model_copy(deep=True)` — a *full, identical* copy of state
holding **all N** pending tool calls (`agent.py`, parallel branch). Every parallel tool
invocation therefore sees the same state and **cannot** recover "which `tool_call_id` is mine"
from `ctx.state` alone. It must be told per-invocation. We move *how* it is told from a
positional run-arg to a schema-validated body; we do not remove the telling.

The batch/aggregation machinery (`PendingToolBatch`, `_require_frame_id_for_write`,
`_parallel_state_aggregation`) keys purely on `frame_id` + `state` and never reads `input_args`,
so it is indifferent to the swap.

---

## 2. Decision

1. **`BaseNodeDef.run` becomes a *declining* `@handler('*')`** that returns `Next()` by default.
   It is no longer `@abstractmethod`. Every node inherits the `'*' → run` registry entry; an
   override resolves to the most-derived `run()` at access time (the registry resolves handlers
   by attribute name). The dispatch-time identity checks are deleted.
2. **One dispatch path.** `handler()` always calls `_dispatch_routed()`. The `else`/`_call_run`
   branch and the `_dispatch_routed` identity fallback are deleted.
3. **`input_args` is removed entirely.** The one real consumer (`ToolNodeDef`'s `tool_call_id`)
   migrates to a schema-validated `payload` (`ToolCallRef`). `Call(topic, state, *input_args)`,
   `CallFrame.input_args`, and the client's `run_args` parameter are removed.
4. **Tool dispatch sends a routeless body** — `Call(tool_topic, state, body=ToolCallRef(...))`
   with no route. The `'*'` handler matches the absent route key, so the **"body requires route"
   producer guard is removed** (its "lands unread" justification is void under unification — §5 F1).

### Why this shape

- **Zero-ceremony preserved.** A user node `class N(NodeDef): async def run(self, ctx)` keeps
  working with **no decorator** — it inherits the `'*'` marker from the base and resolves to its
  own `run()`. This is the single most-used SDK extension point; it must not grow ceremony.
- **Spec "routes-only → skip" preserved.** A node with only `@handler('order.*')` and no `run`
  override falls through to `'*' → `base `run` → `Next()` → chain ends → skip. The base
  *declining* (rather than raising) is what preserves this without an identity check.
- **The pairing rule does the tool's work for free.** `_validate_routes` already enforces
  "payload param ⇔ `schema=`". Once `run` is a registry handler, the tool's
  `run(self, ctx, payload)` is *required* by that same rule to carry `schema=ToolCallRef` — no
  bespoke logic.

---

## 3. Core mechanism (how the registry makes `run` "just a handler")

`RegistryMixin.__init_subclass__` (`calfkit/_registry.py`) walks the MRO and collects
`@handler`-marked functions keyed by **attribute name**, resolving to bound methods at access
time. Consequences for a `@handler('*')` on `BaseNodeDef.run`:

- The marker lives on `BaseNodeDef.run`'s function object; the MRO walk finds it for **every**
  subclass → `_handlers = {'*': 'run', ...}` everywhere.
- `get_handler('*')` → `getattr(self, 'run')` → the **most-derived** `run()`. An override needs
  **no** re-decoration to be picked up (registry docstring: "an override — even one that does not
  re-apply `@handler` — is reflected").
- A subclass that needs a **different schema** (the tool) re-decorates
  `@handler('*', schema=ToolCallRef)`; the most-derived marker wins in collection. Both markers
  map to attribute `'run'`, so the registry's per-route collision check (which only fires when a
  route is owned by **two different attributes**) does **not** trip.
- `'*'` is a valid route pattern (`validate_route_pattern('*')` passes), and `match_chain` orders
  it **last** (least specific), so real routes always intercept before the `run` fallback.

`match_chain(None, {'*': 'run', ...})` returns `['*']` (only `'*'` matches a `None`/absent route
key), so a header-less message dispatches straight to `run`.

---

## 4. Invariants preserved (with proofs)

| Invariant | Today | After | Why it holds |
|---|---|---|---|
| Per-invocation tool id channel | `input_args` | `payload` (`ToolCallRef`) | §1.1 — channel swapped, not removed; parallel fan-out carries `body` per `Call` (already wired at the `list[Call]` publish branch). |
| Routes-only node → unmatched route skips | fallback absent → skip | base `run` returns `Next()` → skip | §2/§3 |
| Zero-ceremony `run(self, ctx)` | identity fallback | inherited `'*'` marker | §3 |
| Agent loop is the implicit `*` | identity fallback (loop overrides run) | loop is `'*' → run` terminal | loop overrides `run`; resolves as `'*'` |
| Single state author | one terminal handler | unchanged | chain semantics untouched |
| Tool return path needs no id | id rides back inside `state.tool_results` | unchanged | `ReturnCall` carries only `state` |

---

## 5. Forks resolved

- **F1 — routeless body to the catch-all (DECIDED: relax the producer guard).** A tool `Call`
  carries `body=ToolCallRef(...)` with **no route**. Because `'*'` matches an absent route key
  (`route_matches('*', None) is True`), the unified dispatch hands `frame.payload` to the tool's
  `@handler('*', schema=ToolCallRef)` `run` without any `x-calf-route` header. The old
  "`body=` requires `route=`" guard was justified by "a routeless body lands unread" — which is
  **only true pre-unification** (today `route is None` skips dispatch entirely). Under this change
  that justification is void, so we **remove** the guard (both raise sites). No synthetic route
  key; no `TOOL_INVOKE_ROUTE` constant. Residual: a body sent to a node whose `'*'/run` has **no**
  schema is silently ignored — the guard only ever *weakly* prevented this (a routed body to a
  no-schema handler is unread too). To keep this observable, §6.3(g) adds a **callback-aware**
  consumer-side log (WARNING when a caller awaits, DEBUG otherwise) when a non-None `payload` goes
  unconsumed — not a flat DEBUG (review fix).
- **F2 — loudness on a do-nothing node (DECIDED: pass, documented as a temporary shortcoming).**
  No class-definition lint in this work. **Known trade-off accepted for now:** with the declining
  base `run`, a forgotten/typo'd `run` (or a node with neither a `run` override nor an `@handler`)
  silently *no-ops* instead of erroring; in a fire-and-forget / choreography flow the safety-net
  no-match log is `DEBUG`-level (effectively invisible). The adversarial design review flagged
  this as the main DX hazard, and a clean structural fix exists (a `ClassVar` marker on the
  framework base classes + a ~3-line validation lint, no dispatch-time identity check). It is
  **deferred deliberately**: the **concurrent calfkit error-propagation work** is the right home
  for surfacing silent node-level no-ops loudly and consistently, so we avoid a one-off lint here.
  Tracked in §13. Revisit if that work slips.

---

## 6. Touch points — every file & change

Grouped by area. Line anchors are approximate (`≈`); the authoritative targets are the named
symbols. "B/A" = before/after sketch.

### 6.1 NEW — `calfkit/models/tool_dispatch.py`

Create the tool-dispatch payload model (no route constant — F1b sends `body=` with no route).

```python
from pydantic import BaseModel, ConfigDict

class ToolCallRef(BaseModel):
    """The per-invocation body handed to a tool node: which tool call it must service.

    Replaces the former positional `input_args=(tool_call_id,)` channel. Carried as the
    `Call(..., body=ToolCallRef(...))` payload (no route) and validated by the tool node's
    `@handler('*', schema=ToolCallRef)` before `run` is entered.
    """
    model_config = ConfigDict(extra="forbid")
    tool_call_id: str
```

> Placement (DECIDED): a dedicated module keeps the tool-dispatch contract out of the generic
> `actions.py`/`payload.py`. Export `ToolCallRef` from `models/__init__` (§6.2).

### 6.2 `calfkit/models/__init__.py`

- **(a)** Export `ToolCallRef` beside the existing model exports. `Next` is already exported
  (no change).

### 6.3 `calfkit/nodes/base.py` — the core

This file holds the bulk of the change.

**(a) Imports.** Add `handler` to the `calfkit._registry` import (currently
`from calfkit._registry import RegistryMixin`). `Next` is already imported from `calfkit.models`.
Drop `abstractmethod` from the `abc` import **iff** unused elsewhere (it is only used on `run`).

**(b) Delete `_run_accepts_input` plumbing.**
- Remove the class attribute `_run_accepts_input: bool` (≈64).
- Remove its assignment in `__init_subclass__`: `cls._run_accepts_input = _accepts_extra_param(cls.run)` (≈79).
- **Keep** `_accepts_extra_param` (≈37) — still used by the `_validate_routes` pairing check (≈99).

**(c) `_validate_routes` (≈83).** Remove the run-vs-`@handler('*')` conflict guard; keep the
pairing loop (it now also validates `run`).

B/A:
```python
# BEFORE
run_overridden = cls.run is not BaseNodeDef.run
if "*" in cls._handlers and run_overridden:
    raise RegistryConfigError("@handler('*') conflicts with the node's run() catch-all ...")
for route, info in cls._handler_info.items():
    handler_fn = getattr(cls, cls._handlers[route])
    accepts_payload = _accepts_extra_param(handler_fn)
    ...pairing checks...

# AFTER
for route, info in cls._handler_info.items():           # '*' -> run now included; pairing covers run
    handler_fn = getattr(cls, cls._handlers[route])
    accepts_payload = _accepts_extra_param(handler_fn)
    ...pairing checks unchanged...
# (No F2 loudness lint — see §5.)
```
The pairing rule now *requires* `ToolNodeDef.run(self, ctx, payload)` to declare
`@handler('*', schema=ToolCallRef)` (payload param ⇒ schema), and *forbids* a schema on the
base/agent `run(self, ctx)` — exactly the constraint we want, enforced for free.

**(d) `run()` (≈190–208): make it a declining `@handler('*')`.**

B/A:
```python
# BEFORE
@abstractmethod
async def run(self, ctx: SessionRunContext, *args: Any, **kwargs: Any) -> NodeResult[State]:
    ...docstring...
    raise NotImplementedError()

# AFTER
@handler("*")
async def run(self, ctx: SessionRunContext) -> NodeResult[State] | Next:
    """Default catch-all handler (route '*', least-specific). The base implementation
    DECLINES (returns Next) so a node with only specific @handler routes skips an
    unmatched message instead of erroring. Override to give the node default behavior;
    overrides resolve as the '*' handler with no re-decoration needed."""
    return Next()
```
Notes: the `*args, **kwargs` (which existed to absorb `input_args`) are gone. The return type
widens to `NodeResult[State] | Next`.

**(e) Delete `_call_run` (≈340–344).** The `'*' → run` entry is now invoked by the normal
handler-invoke code inside `_dispatch_routed` (`method(ctx)` when no schema; `method(ctx, payload)`
when schema). No splat helper needed.

**(f) `_dispatch_routed` (≈346).**
- Remove the `input_args` parameter from the signature and from its caller (§6.3.g).
- **Widen the `route` parameter type to `str | None`** (currently `route: str` at ≈349). Under the
  unified path `handler()` passes `decode_header_str(...)` (a `str | None`) straight in, and a
  `None` route is now the *normal* no-header case (routed to `'*'`). Without this, `make check`
  (mypy strict) fails. See §6.13a for the matching `_routing.py` widening — the review found the v1
  claim "no change to `_routing.py`" to be wrong.
- Fix the malformed-route log so an **absent** route (`None`, the normal no-header case) is not
  flagged; only a *present-but-malformed* key is.
  ```python
  # BEFORE:  if not is_concrete_route_key(route):
  # AFTER:   if route is not None and not is_concrete_route_key(route):
  ```
  > Note: `match_chain(None, {'*': 'run'})` correctly returns `['*']` only because
  > `route_matches` checks `pattern == "*"` *before* `is_concrete_route_key(key)`. That ordering is
  > load-bearing; §6.13a adds an explicit `None`-key test so a future reorder can't silently break
  > all no-route + tool dispatch.
- Delete the identity fallback tail:
  ```python
  # BEFORE (≈400–402)
  if cls.run is not BaseNodeDef.run:
      return await self._call_run(ctx, input_args)
  return None
  # AFTER — the loop already includes '*' -> run; just fall through:
  return None
  ```
  (The `for r in match_chain(route, cls._handlers)` loop now naturally reaches `'*' → run` last;
  if it returns `Next`/`None`, the loop continues and the method returns `None` → skip.)

**(g) `handler()` (≈406): collapse to one path.**

B/A (the **gate short-circuit is preserved** — only the inner post-gate dispatch collapses; the
review flagged that the v1 sketch dropped it):
```python
# BEFORE (post-gate block)
if not await self._evaluate_gates(ctx, correlation_id):
    body = envelope                                  # gate-rejected: unchanged, no dispatch
else:
    frame = envelope.internal_workflow_state.current_frame
    route = decode_header_str(headers.get(HDR_ROUTE))
    if route is not None and type(self)._handlers:
        output = await self._dispatch_routed(ctx, route, frame.payload, frame.input_args, ...)
        if output is None:
            ...no-match log...; return Response(envelope, headers=self._emitter_headers())
    else:
        output = await self._call_run(ctx, frame.input_args)
    body = await self._publish_action(output, envelope, correlation_id, broker)
return Response(body, headers=self._emitter_headers())

# AFTER
if not await self._evaluate_gates(ctx, correlation_id):
    body = envelope                                  # gate branch UNCHANGED — preserved
else:
    frame = envelope.internal_workflow_state.current_frame
    route = decode_header_str(headers.get(HDR_ROUTE))
    output = await self._dispatch_routed(ctx, route, frame.payload,   # single path; no input_args
                                         awaiting_reply=frame.callback_topic is not None,
                                         correlation_id=correlation_id)
    if output is None:                               # all handlers declined (incl. base run's Next)
        level = _stuck_level(frame.callback_topic is not None)
        logger.log(level, "[%s] no handler matched route=%s on node=%s; registered=%s",
                   correlation_id[:8], route, self.node_id, tuple(type(self)._handlers))
        # F1b residual — callback-aware unconsumed-body signal (not flat DEBUG):
        if frame.payload is not None:
            logger.log(level, "[%s] ...and a non-None body was dropped (no schema handler consumed it)",
                       correlation_id[:8])
        return Response(envelope, headers=self._emitter_headers())
    logger.debug("[%s] node=%s produced action=%s", correlation_id[:8], self.node_id, type(output).__name__)
    body = await self._publish_action(output, envelope, correlation_id, broker)
return Response(body, headers=self._emitter_headers())
```
`type(self)._handlers` is now always non-empty (`'*'` always present), so the old guard is moot.
The unconsumed-body log reuses `_stuck_level(awaiting_reply)` so a dropped body on a *waiting*
workflow is a `WARNING`, not an invisible `DEBUG` (F1b observability, per the design review).

### 6.4 `calfkit/nodes/agent.py` — tool Call construction

Three `Call[State](topic, state, tool_call_id)` sites move the id from a positional into `body=`
(no route — F1b). (The agent's own `run` keeps its inherited `'*'` — no change to the loop.)

- **(a) Import:** `from calfkit.models.tool_dispatch import ToolCallRef`.
- **(b) Sequential resume** (`agent.py:272–276` post-rebase):
  ```python
  # BEFORE: Call[State](tool_topic, ctx.state, target_tool_call.tool_call_id)
  # AFTER:
  Call[State](tool_topic, ctx.state, body=ToolCallRef(tool_call_id=target_tool_call.tool_call_id))
  ```
- **(c) New single/sequential dispatch** (`agent.py:443–447`): same transform.
- **(d) Parallel fan-out** (`agent.py:451–455`): same transform inside the comprehension; the
  `list[Call]` publish branch in `base.py` already forwards `payload=call.body`, so per-invocation
  bodies arrive correctly with no `_publish_action` change.

> The agent constructs `ToolCallRef` from a value it already holds (`tc.tool_call_id`), so body
> validation can never fail in practice (the schema is internal, not LLM-shaped).

### 6.5 `calfkit/nodes/tool.py` — tool run becomes a payload handler

- **(a) Imports:** `from calfkit._registry import handler`;
  `from calfkit.models.tool_dispatch import ToolCallRef`.
- **(b) `ToolNodeDef.run` (≈83):**
  ```python
  # BEFORE
  async def run(self, ctx: SessionRunContext, tool_call_id: str) -> NodeResult[State]:
      ...
  # AFTER
  @handler("*", schema=ToolCallRef)
  async def run(self, ctx: SessionRunContext, payload: ToolCallRef) -> NodeResult[State]:
      tool_call_id = payload.tool_call_id
      ...   # body unchanged from here
  ```
  Only the signature + first line change; the rest of the method (`get_tool_call`, error
  handling, `ReturnCall`) is untouched. The `@handler('*', schema=...)` re-decoration overrides
  the base's no-schema `'*'` marker for this class and satisfies the pairing rule.

### 6.6 `calfkit/models/actions.py` — remove `input_args`

- **(a) `_Call`:** remove the `input_args` field (≈38) and the `*input_args` capture in
  `__init__` (≈40–67) → `def __init__(self, target_topic, state)`; drop `self.input_args = ...`.
- **(b) `Call.__init__` (≈85):**
  ```python
  # BEFORE: def __init__(self, target_topic, state, *input_args, route=None, body=None):
  #             super().__init__(target_topic, state, *input_args)
  # AFTER:  def __init__(self, target_topic, state, *, route=None, body=None):
  #             super().__init__(target_topic, state)
  ```
  (`route`/`body` keyword-only validation logic unchanged.) `TailCall`/`ReturnCall` already take
  no positional args beyond state — unaffected.
- **(c) `Delegate.input_args` (≈26):** remove the field. (`Delegate` is never constructed in
  non-vendored code; the field is dead.)
- **(d) Docstrings:** drop the `*input_args` paragraphs from `_Call.__init__`/`Call` docstrings.
- **(e) Remove the body-requires-route guard (F1b):** delete the
  `if body is not None and route is None: raise ValueError("Call body= requires route=; ...")`
  block in `Call.__init__` (≈91–94). A routeless body is now valid — read by the target's `'*'`
  schema handler. (Keep the wildcard-route validation; only the body/route coupling goes.)

### 6.7 `calfkit/models/session_context.py` — drop frame `input_args`

- **(a) `CallFrame.input_args` (≈40):** remove the field. (`payload` stays.)
- **(b) `WorkflowState.invoke_frame` (≈69–78):** drop `input_args=call.input_args` from the
  `CallFrame(...)` construction; keep `payload=payload`.

### 6.8 `calfkit/client/base.py` — remove `run_args`

The client already supports `route`/`body` and `_publish_call` already sets `payload=body` — so
this is pure deletion of the `run_args` plumbing.

- **(a) `_invoke` (≈210):** remove `run_args` param (≈217) and the `run_args=run_args` arg to
  `_publish_call` (≈244). Drop the `run_args:` docstring line (≈230).
- **(b) `_publish_call` (≈257):** remove the `run_args` param (≈265) and the
  `input_args=run_args` line in the `CallFrame(...)` push (≈297).
- **(c) `_emit` (≈316):** remove `run_args` param (≈322) and its pass-through (≈354); drop the
  docstring line (≈341).
- **(d) Remove the body-requires-route guard (F1b):** delete the
  `if body is not None and route is None: raise ValueError("body= requires route=; ...")` block
  in `_publish_call` (≈284–285). (Keep the wildcard-route validation above it.)

### 6.9 `calfkit/client/client.py` — remove `run_args` from public API

`run_args` appears across overloads + impls + docstrings + pass-throughs for three methods.
Remove all (the `route`/`body` params already present become the supported input-shaping channel).

- **(a) `invoke_node`:** overload params (≈97, ≈116), impl param (≈135), docstring (≈169),
  pass-through `run_args=run_args` (≈197).
- **(b) `emit_to_node`:** impl param (≈215), docstring (≈247), pass-through (≈278).
- **(c) `execute_node`:** overload params (≈299, ≈319), impl param (≈339), docstring (≈371),
  pass-through (≈404).

### 6.10 `README.md`

- Remove `run_args` from the `emit_to_node` input-shaping list (≈312). If a replacement is
  warranted, point users to `route=`/`body=`.

### 6.11 `calfkit/nodes/consumer.py` — verify inert (likely no change)

`ConsumerNodeDef` overrides `handler()` wholesale and its `run()` raises `AssertionError`
("should never be invoked"). After the change it inherits `'*' → run`, but `handler()` never
dispatches, so `run` stays unreachable. The override is `(self, ctx)` with no payload → satisfies
the pairing rule against the inherited no-schema `'*'`. **No code change**; add a regression test
(§7) asserting the consumer never invokes `run`.

### 6.12 `calfkit/nodes/node.py` — verify no change

`NodeDef` is a pass-through (`class NodeDef(Generic[...], BaseNodeDef): pass`). It inherits the
declining `'*'` and the zero-ceremony override behavior. **No change.**

### 6.13 `calfkit/_registry.py` — verify no change

The collision check keys on **attribute**, so base `@handler('*')` + a subclass re-decorating
`run` with `@handler('*', schema=...)` (both attr `'run'`) do not collide. A user who adds a
*separate* `@handler('*')` method (different attr) collides — a reasonable error ("`'*'`
registered by both `'run'` and `'<their>'`"). **No change required**; optionally tailor the
collision message to mention `run`.

### 6.13a `calfkit/_routing.py` — widen route-key typing to `str | None` (review fix)

The v1 spec claimed "no change to `_routing.py`" — **wrong**; `make check` (mypy strict) fails
without this. Under the unified dispatch a `None` route key flows into these helpers:

- `is_concrete_route_key(key: str)` (≈33) → `key: str | None`. (Runtime already safe: `bool(None)`
  short-circuits to `False`.)
- `route_matches(pattern: str, key: str)` (≈41) → `key: str | None`. (Runtime already safe:
  `pattern == "*"` returns `True` before any `is_concrete_route_key(key)` call.)
- `match_chain(key: str, patterns)` (≈70) → `key: str | None`.

The runtime behavior is correct **only because** `route_matches` tests `"*"` before touching the
key — keep that ordering; add a focused unit test for `route_matches('*', None) is True` and
`match_chain(None, {...}) == ['*']` (§7) so a refactor can't regress it silently.

### 6.14 `calfkit/worker/worker.py` — DROPPED (subsumed by the MCP-removal prerequisite)

v2 scoped a partial `worker.py`↔mcp decouple here. Round 2 showed it broke in-scope provisioning
tests and rested on a false "`import calfkit` is safe" claim. **Resolved by deleting MCP wholesale
in the prerequisite PR** (see header): after rebasing onto mcp-free `main`, `worker.py` has no
`McpBridge`/`McpServer` references at all, so there is **nothing to do here** for this feature. No
run-unification change touches `worker.py`.

---

## 7. Test touch points

Migrate (per the blast-radius inventory + the completeness review). `tests/mcp/**` no longer
exists (deleted in Stage 0), so there is no mcp test fallout here. The v1 list was incomplete; the
**bold** rows are review additions and several would fail `make check` / collection if missed.

| File | Change |
|---|---|
| `tests/test_node_registry_wiring.py` (≈16,28) | Delete/replace the `_run_accepts_input is False` assertion (concept removed). Replace with: a no-route node registers `{'*': 'run'}`; a routed node registers its routes + `'*'`. |
| **`tests/test_tool_errors.py` — ~11 direct `tool_node.run(ctx, "<str>")` calls** (≈112,144,174,525,818,948,979,1185,1376,1419 + the 665–720 dispatch-assert band) | **Every direct call must become `run(ctx, ToolCallRef(tool_call_id=...))`** (the migrated `run` does `payload.tool_call_id`; a bare str → `AttributeError` + mypy error). Dispatch-`Call` asserts move from `call.input_args[0]` to `call.body == ToolCallRef(...)` / `call.route is None`. |
| **`tests/test_lifecycle_resource_injection.py` (≈140)** | `await tool_node.run(ctx, tool_call_id)` → `run(ctx, ToolCallRef(tool_call_id=...))`. |
| **`tests/test_routed_dispatch.py` (≈91, ≈404)** | `test_call_rejects_body_without_route_at_construction` and `test_client_rejects_body_without_route` — **delete/invert**: a routeless body is now valid (§6.6e/§6.8d removed the guards). |
| **`tests/test_routed_dispatch.py` (≈266, ≈466)** | `test_explicit_star_handler_with_overridden_run_raises` / `..._on_agent_raises` — these relied on the **deleted** conflict guard; they still raise but now via the **registry collision check** (`'*'` owned by both `run` and the user method). Keep them, but update intent/comments (now exercise the collision path, not the run-vs-`*` guard). |
| **`tests/test_routed_dispatch.py` (≈392,413,430)** | `run_args=None` calls to `_publish_call` → remove (param deleted, else `TypeError`). |
| **`tests/test_fire_and_forget.py` (≈56)** | `_TerminalNode.run(self, ctx, *args, **kwargs)` now fails the pairing check at import (extra params, no schema). Drop the `*args, **kwargs` → `run(self, ctx)`. |
| `tests/test_fire_and_forget.py` (≈249–266) | Remove the `run_args=("a", 1)` pass-through test (param removed). |
| `tests/conftest.py` (≈268–296) | Remove `make_input_args_factory` and the `CallFrame(input_args=...)` fixture wiring (`CallFrame` no longer has `input_args`). Re-point any round-trip fixtures to `payload=`. |
| **ruff F401 cleanups** (`make fix` handles, but expect them): orphaned `Sequence` import in `nodes/base.py`, `models/actions.py`, `models/session_context.py`, `client/base.py`, `client/client.py`, `tests/conftest.py` once `input_args`/`run_args` are gone. |
| **NEW** `tests/test_run_unification.py` | See below. |

**New coverage (TDD targets):**
- Base `run` declines: a `NodeDef` with only `@handler('order.*')` skips an unmatched/no-route
  message (envelope unchanged), at the `callback_topic`-aware log level.
- Override `run` is the `'*'` terminal: a `NodeDef` with `run(self, ctx)` (no decorator) handles a
  no-route message; with routes present, a specific route intercepts first and `run` is the
  fallback.
- Tool payload dispatch: agent emits `Call(body=ToolCallRef(...))` (no route); tool
  `run(ctx, payload)` receives the validated `ToolCallRef` via the `'*'` handler and looks up the
  call.
- Routeless body reaches the catch-all (F1b): a `Call(body=Model(...))` with no `route` is
  consumed by a `@handler('*', schema=Model)` `run`; and a routeless `body=` no longer raises at
  `Call` construction / client emit.
- Parallel fan-out: N parallel `Call`s each carry their own `ToolCallRef`; each tool invocation
  services the correct id.
- Pairing enforcement: a node declaring `run(self, ctx, payload)` **without**
  `@handler('*', schema=...)` raises `RegistryConfigError` at class definition; a schema on a
  `(self, ctx)` run raises.
- Consumer inertness: `ConsumerNodeDef.run` is never invoked (handler path owns dispatch).
- Agent loop as `'*'`: a routed handler on an Agent intercepts, then falls through to the loop.
- Routing `None`-key contract (§6.13a): `route_matches('*', None) is True`,
  `is_concrete_route_key(None) is False`, `match_chain(None, {'*': 'run', 'order.*': ...}) == ['*']`
  — guards the short-circuit ordering against a silent regression.
- `body=ToolCallRef(...)` survives a real broker round-trip: assert the receiving handler validates
  `frame.payload` (a `dict` post-deserialization) into a `ToolCallRef` — existing route-dispatch
  tests only ever used `dict` bodies, so the model→dict→`model_validate` path is currently unproven.

`make fix && make check` (ruff, ruff-format, mypy strict) green before PR. Use
`/test-driven-development` and `/pytest-coverage`.

---

## 8. Docs touch points

- **`docs/designs/header-route-dispatch-spec.md`** — add an "Amended by" banner pointing here;
  annotate decisions #10–11 / §3 / §5.4 / §7.1 that `run` is now an explicit declining
  `@handler('*')` and that `input_args` is removed (the `call_run`/fallback-synthesis described in
  §7.1 no longer exists).
- **`docs/designs/fire-and-forget-emit.md`** (≈102) — drop the `run_args` reference.
- **`docs/designs/hooks-design.md`** — `input_args` references **and** now-false "abstract `run()`"
  / `run(ctx, *input_args)` claims (≈46, 96, 1060); already a superseded doc, but note the removal
  so a future reader isn't misled.
- **`CHANGELOG.md`** — entry enumerating the breaking changes in §10.
- **This file** — the authoritative spec.

---

## 9. Staged rollout (each stage stays green)

> **Why not 3 independently-green stages (v1's plan):** the review proved it's unbuildable.
> (1) The pairing check runs at **import time**, so the moment base `run` is `@handler('*')` an
> un-migrated `ToolNodeDef` (extra positional, no schema) fails class-definition → package won't
> import. (2) A routeless `body=` is only *delivered* to `run` under the unified dispatch, so the
> tool's payload migration can't precede or lag the dispatch change. The dispatch unification, the
> tool migration, and the guard removal are therefore **one atomic change**. `input_args`
> *plumbing* removal is the only part that cleanly separates. (The v2 `worker.py` decouple is gone
> entirely — Stage 0 deletes mcp.)

**Stage 0 — MCP removal (PREREQUISITE; separate PR, separate session).**
Delete `calfkit/mcp/**` + `tests/mcp/**` and strip every framework/CLI/test reference. **This
branch rebases onto the resulting mcp-free `main` before Stage 1.** Not specced here (it's its own
hard-break PR); listed so the sequencing is explicit. After this, the only extra-positional `run`
nodes are `ToolNodeDef` (migrated below) and the `_TerminalNode` test node (fixed in §7).

**Stage 1 — Unification + tool migration (one atomic, breaking commit; on mcp-free main).**
- base `run` → declining `@handler('*')`; `handler()` → single `_dispatch_routed` path; fix the
  `route is None` log; **widen `_dispatch_routed`/`_routing.py` route typing to `str | None`**
  (§6.13a); delete both identity checks, the `_validate_routes` conflict guard, `_call_run`, and
  `_run_accepts_input` (keep `_accepts_extra_param`).
- Add `tool_dispatch.py` (`ToolCallRef`); `ToolNodeDef.run` → `@handler('*', schema=ToolCallRef)`;
  the 3 agent `Call` sites → `body=ToolCallRef(...)` (no route); **remove the body-requires-route
  guard** (both raise sites — §6.6e/§6.8d); add the callback-aware unconsumed-body log (§6.3g).
- Migrate **all** affected tests in §7 (the ~11 direct `run(ctx, str)` sites, `_TerminalNode`,
  the 4 named `test_routed_dispatch.py` tests, `run_args` tests, the `_run_accepts_input` test).
- At the end of Stage 1, `input_args`/`run_args` still *exist* on `Call`/`_Call`/`CallFrame`/the
  client but are **unused by any node**. `make check` green (no `tests/mcp/**` exists post-Stage 0).

**Stage 2 — Tear down the now-dead `input_args` plumbing (pure deletion).**
Remove `*input_args`/`input_args` from `actions.py` (`_Call`/`Call`/`Delegate`) and
`session_context.py` (`CallFrame` + `invoke_frame`); remove the client `run_args` param
(`client/base.py`, `client/client.py`); remove `make_input_args_factory` (`conftest.py`); the ruff
F401 `Sequence` cleanups; README + docs. Green because Stage 1 already removed every reader.

---

## 10. Breaking changes & migration

| Break | Was | Now | Migration |
|---|---|---|---|
| `Call(topic, state, arg)` positional | `arg` → `input_args` | positional removed | `Call(topic, state, body=Model(...))` |
| Client `run_args=` | forwarded to `run()` | removed | use `route=`/`body=` (already public) |
| Custom node `run(self, ctx, x)` via run-args | worked via `input_args` | raises at class def (payload param ⇒ needs `schema=`) | declare `@handler('*', schema=Model)` and accept `payload: Model` |
| `CallFrame.input_args` | wire field | removed | internal; use `payload` |
| Node with no `run`/routes | runtime `NotImplementedError` | silent skip (no-match `WARNING` when a caller awaits, `DEBUG` otherwise) — **known temp shortcoming, §13** | give it a `run` or a route |
| `Call(body=..., route=None)` | `ValueError` (body requires route) | allowed — read by the `'*'` handler | none (relaxation, not a break) |

Pre-1.0; hard breaks are acceptable per project policy. Add a CHANGELOG entry enumerating the
above.

### 10.1 mcp coupling — resolved by deleting MCP first (Stage 0 prerequisite)

Round 2 found the worker↔mcp seam was the only non-converged area: `McpBridge.run(ctx, tool_call_id)`
fails the pairing check at **import time**, and because `calfkit/__init__.py` → `calfkit.worker`
→ `calfkit.mcp._bridge`, **`import calfkit` itself crashes** post-change (the v2 "`import calfkit`
is safe" claim was *false* — corrected). A partial `worker.py` decouple also broke in-scope
provisioning tests. **Decision (yours): delete the entire MCP subsystem in a separate hard-break
PR** (`calfkit/mcp/**` + `tests/mcp/**` + all framework/CLI/test references), then **rebase this
branch onto the mcp-free `main`.** That removes `McpBridge` (no pairing failure), the worker
coupling, and the broken tests at the root — so this feature touches no mcp code at all and §6.14
is dropped.

---

## 11. Rejected alternatives

- **Minimal collapse (keep `input_args`, just merge the two dispatch branches).** Removes the
  literal `else`-branch duplication but leaves `run` identity-detected and the dual
  `input_args`/`payload` channels. Rejected: doesn't achieve the stated goal (run as a registry
  handler).
- **Abolish `run()` entirely (pure `@handler` bags; `'*'` opt-in).** Cleanest concept, but every
  existing `run()` override breaks and the zero-ceremony node disappears. Rejected as too
  aggressive.
- **Decorate base `run` as `'*'` but keep it raising.** Breaks the spec's "routes-only → skip"
  (unmatched routes would raise). Rejected; the declining `Next()` base is what preserves skip.
- **Synthetic `"invoke"` route key (keep "body requires route") instead of relaxing the guard.**
  Conservative, but keeps a guard whose justification ("a routeless body lands unread") is *void*
  under unification, and adds a constant + a `route=` arg on every tool `Call`. Rejected in favor
  of F1b (relax the guard; the `'*'` handler reads the routeless body).

---

## 12. Resolved decisions

1. **F1 — routeless body to the catch-all:** relax the body-requires-route guard; tool `Call`s
   carry `body=ToolCallRef(...)` with no route (the `'*'` handler reads it). No route key. (§5 F1)
2. **F2 — loudness lint:** **pass for now, documented as a known temporary shortcoming** (§13) to
   be addressed by the concurrent error-propagation work, not a one-off lint here. (§5 F2)
3. **`ToolCallRef` placement:** dedicated `calfkit/models/tool_dispatch.py`. (§6.1)
4. **mcp:** deleted wholesale in a **separate prerequisite hard-break PR**; this branch rebases
   onto mcp-free `main`. No mcp code is touched by this feature (§10.1; §6.14 dropped). (your call)
5. **Staging:** Stage 0 = MCP removal (prerequisite, separate PR); Stage 1 = the atomic
   unification + tool migration (the original 3-stage plan was unbuildable — §9); Stage 2 =
   `input_args` plumbing teardown (the only cleanly separable part).

---

## 13. Risk register & known temporary shortcomings

| Item | Status / mitigation |
|---|---|
| **Silent no-op on forgotten/typo'd `run`** (declining base; no-match log is `DEBUG` in fire-and-forget) | **Known temporary shortcoming (accepted).** A structural fix exists (a `ClassVar` marker on the framework base classes + a ~3-line validation lint, no dispatch-time identity check) but is **deferred to the concurrent calfkit error-propagation work**, the right home for surfacing silent node-level no-ops uniformly. Revisit if that work slips. |
| **Routeless body dropped at a no-schema `'*'/run`** (F1b) | Mitigated to **callback-aware** logging (WARNING when a caller awaits) — §6.3g. Not a hard guard; the producer-side guard it replaced was itself only a weak proxy. |
| **Tool dispatch whose body fails `ToolCallRef` validation is declined, not replied to** (PR-review C1) — routing tool dispatch through schema validation means a malformed body skips `run()`, so no `ReturnCall` is published and an awaiting agent relies on its reply-TTL. | **Not reachable via supported APIs** — the agent always constructs a valid `ToolCallRef`; only a hand-crafted `Call` or `ToolCallRef`-shape drift across a rolling deploy could trigger it. It is also a variant of the pre-existing `Silent()`-on-unknown-id non-reply in `ToolNodeDef.run`. The decline is **logged loud** (callback-aware WARNING: "body failed ToolCallRef validation"), pinned by `test_malformed_toolcallref_body_to_tool_declines_and_logs_loud`. A proper "a tool always replies, even on body-validation failure" guarantee belongs with the **concurrent error-propagation work** (alongside the F2 lint) — likely by making a reply-owing catch-all schema rejection produce a `FailedToolCall` reply rather than a silent skip. |
| **Routing `None`-contract depends on short-circuit ordering** in `route_matches` | Pinned by an explicit unit test (§6.13a, §7) so a reorder can't silently break all no-route + tool dispatch. |
| **Agent loop as a stateful `'*'` reachable as a CoR fallback** — an intercepting route that mutates `ctx.state` then declines (`Next`) could feed a torn write into the loop | Already governed by the route-dispatch **"pure guards / single state author"** contract (header-route-dispatch-spec decision #4 / risk register). No *new* mitigation; documented-contract risk, unchanged by this work. |
| **`body=ToolCallRef` model round-trip unproven by existing tests** (route-dispatch tests only used `dict` bodies) | New test asserts the `model → dict → model_validate` path over the broker (§7). |
| **Rebase dependency on the MCP-removal PR** | This branch must rebase onto mcp-free `main` before Stage 1; until then the line anchors in §6 are confirmed against the current tree and re-verified at rebase. |
