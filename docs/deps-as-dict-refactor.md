# Deps-as-dict refactor

| | |
|---|---|
| Status | Implemented |
| Closes | #144 (expose `deps` on `NodeResult` so `@consumer` can read inbound deps) |
| Breaking | Yes — `feat!` (public `Deps` removed; `deps` wire body shape changed) |

## Problem

`deps` is the documented channel for user-provided agent dependencies, but its shape
was inconsistent across the three places developers touch it:

- **Client write** — a plain dict: `client.invoke_node(deps={"discord": wire})`.
- **Server read** — the framework wrapped that dict in a `Deps` model
  (`{correlation_id: str, provided_deps: dict}`) so `correlation_id` could ride
  alongside it on the wire. Tools therefore read `ctx.deps.provided_deps["discord"]`
  and `ctx.deps.correlation_id`.
- **pydantic-ai loop** — the agent passed `deps=ctx.deps.provided_deps` (a *dict*) into
  the vendored loop, so pydantic-ai's in-process `ctx.deps` was a dict while calfkit's
  `@agent_tool` `ctx.deps` was the *model*. Same word, two shapes.

And `@consumer` functions, which receive a `NodeResult`, had **no** access to `deps` at
all (#144) — even though every publish already carries it forward
(`_publish_action` in `calfkit/nodes/base.py`). Consumers were forced to abuse
`State.metadata` plus the convention-private `Client._invoke`.

`correlation_id` is also conceptually a run/trace identifier, not a dependency — it was
the odd field nested inside `Deps`, while its siblings `emitter_node_id`,
`emitter_node_kind`, and `frame_id` already live as top-level context attributes.

## Solution

Make `deps` a bare `dict[str, Any]` everywhere, and expose `correlation_id` as a
top-level context attribute. One mental model — *deps is a dict, from input to consumer*:

```python
client.invoke_node(deps={"k": v})   # input  (unchanged)
ctx.deps["k"]                       # tools / node run()
result.deps["k"]                    # consumers / client     ← issue #144
ctx.correlation_id                  # tools / nodes / consumer gates
result.correlation_id               # consumers / client     (already existed)
```

### Before / after

| Surface | Before | After |
|---|---|---|
| `SessionRunContext.deps` | `Deps` model | `dict[str, Any]` |
| run id (context) | `ctx.deps.correlation_id` | `ctx.correlation_id` |
| run id (tool fn) | `ctx.deps.correlation_id` | `ctx.correlation_id` (`run_id` is the underlying pydantic-ai field) |
| user deps (tool fn) | `ctx.deps.provided_deps["k"]` | `ctx.deps["k"]` |
| consumer / client | *(deps absent)* | `result.deps["k"]` |
| `Deps` model | public in `calfkit.models` | removed |

## Why `correlation_id` is transport-sourced, not a wire field

`correlation_id` is **already** the Kafka message's native attribute: every publish sets
`broker.publish(..., correlation_id=..., key=correlation_id.encode())`, the server reads
it via FastStream's `Context()`, and FastStream auto-propagates it across hops. So it is
modeled exactly like `emitter_node_id`/`emitter_node_kind`/`frame_id`: a `PrivateAttr`
on `BaseSessionRunContext`, stamped from the transport in `BaseNodeDef.prepare_context`
(server), the consumer handler, and the client's reply dispatcher — never read from the
envelope body.

This was a deliberate reversal of an earlier draft that made `correlation_id` a serialized
required field. A body field would have (a) duplicated an identifier the transport already
owns (desync risk), (b) made it spoofable via the model constructor, and (c) raised
`ValidationError` at any publish-site constructor that forgot it → a silent consumer hang.
The transport-sourced approach removes `correlation_id` from the wire body entirely, so the
four `_publish_action` publish sites need **no** change and there is no new hang path.

The `correlation_id` accessor is typed `str` (with an `assert`) on both
`BaseSessionRunContext` and `ToolContext`: a handler always stamps it before user code runs,
so unlike `emitter_node_id` (legitimately absent for non-calfkit producers) it is always
present. `ToolContext.correlation_id` aliases the inherited pydantic-ai `run_id`, exposed
under calfkit's own vocabulary (matching `NodeResult.correlation_id`, `Client.execute_node`).

## Conscious decisions

- **`BaseSessionRunContext` stays generic** (`Generic[StateT, DepsT]`, bound to
  `dict[str, Any]`) for parity with pydantic-ai's `RunContext[AgentDepsT]`, even though
  the only binding today is a dict. Calfkit deps must be JSON-serializable (they cross
  Kafka), so a richer typed-deps object is not viable the way pydantic-ai does it in-process.
- **`deps` is surfaced without a defensive copy** — `result.deps` / `ctx.deps` is the same
  dict instance carried on the envelope, mirroring the existing no-copy decision for
  `state`. The old `Deps` was frozen; the bare dict is mutable. `prepare_context` deep-copies
  the inbound context per hop, so tool mutations cannot leak downstream, but callers should
  treat `result.deps` / `ctx.deps` as read-only (documented on `NodeResult`).

## Migration

- `ctx.deps.provided_deps["k"]` → `ctx.deps["k"]`
- `ctx.deps.correlation_id` → `ctx.correlation_id`
- Delete `from calfkit.models import Deps` (the type is gone; the client always accepted a
  plain dict, so there was no legitimate user-construction use case).
- **Wire compat:** the envelope `context.deps` body shape changed (model → dict) and
  `correlation_id` left the body. No cross-version envelope compatibility — deploy a uniform
  calfkit version (framework-internal envelope; 0.x).

## How it subsumes #144

Once `ctx.deps` is a dict, the #144 ask becomes trivial and fully symmetric: `NodeResult`
gains a `deps: dict[str, Any]` field (populated from `envelope.context.deps`), so a
`@consumer` reads `result.deps["discord"]` — the exact same shape a tool reads as
`ctx.deps["discord"]`. The `State.metadata` workaround can be deleted in dependent projects.
