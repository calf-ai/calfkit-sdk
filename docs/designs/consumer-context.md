# ConsumerContext — a node-side context for consumer sinks

**Status:** Proposed
**Related:** [Calfkit 1.0](calfkit-v1-design.md), [Run/handler unification](run-handler-unification.md)

## Summary

Give a `@consumer` sink a dedicated, node-side context — `ConsumerContext` — as the
single argument to its function, instead of the client-facing `NodeResult`. This
moves the node/worker-only `resources` field off `NodeResult` (which is shared with
the client and where `resources` is always empty), and gives consumer authors the
same `ctx.deps` / `ctx.resources` / `ctx.correlation_id` vocabulary that tool
authors already use via `ToolContext`.

`NodeResult` returns to being a **pure projection of the wire envelope + transport**,
used only by the client (`Client.execute_node` / `InvocationHandle.result`).

## 1. Motivation

`NodeResult` is the client-facing reply type. The lifecycle-resources work bolted a
`resources` field onto it so consumers could read node resources — but:

- `resources` is **node-local execution state**, not wire data. Every other
  `NodeResult` field (`output`, `state`, `correlation_id`, `emitter_*`, `deps`) is a
  projection of the envelope/transport and is meaningful on both the client and
  consumer paths. `resources` is the only field that is *injected* and is always
  `{}` on the client path (`InvocationHandle.result` never sets it). It makes a
  context-free DTO carry a context-dependent field.
- Reusing `ToolContext` for the consumer is the wrong fix (evaluated and rejected):
  `ToolContext` is a `pydantic_ai.RunContext` subclass that **lacks** the consumer's
  core (`output`, full `state`) and **carries ~15 tool/LLM-run fields**
  (`tool_call_id`, `tool_name`, `usage`, `retries`, `model`, `run_step`, …) that are
  meaningless in a sink. Adding `output`/`state` to it would pollute the *tool*
  authoring surface; the name literally says "Tool".

What users actually learn from `ToolContext` is the **vocabulary**
(`ctx.deps`, `ctx.resources`, `ctx.correlation_id`), not the class. A purpose-built
`ConsumerContext` keeps that vocabulary, drops the tool baggage, and keeps `resources`
off the client-facing type. §8 proposes unifying both on a shared base later.

## 2. The `ConsumerContext` type

**File:** `calfkit/models/consumer_context.py` (mirrors `tool_context.py`,
`node_result.py`). Exported as a public name; **not** re-exported from
`calfkit.models.__init__` only if it collides — it does not, so add it to `__all__`.

```python
@dataclass(frozen=True)
class ConsumerContext(Generic[OutputT]):
    """Node-side context handed to a @consumer sink, once per inbound envelope.

    Speaks the same vocabulary as ToolContext (ctx.deps / ctx.resources /
    ctx.correlation_id) but is shaped for a terminal sink: it carries the
    projected `output` and full session `state`, and none of the tool/LLM-run
    machinery. Treat it, its `state`, and its `deps` as read-only.
    """

    output: OutputT | None        # deserialized final output; None on intermediate hops
    state: State                  # full session state (tool_calls/results, overrides, …)
    correlation_id: str           # transport correlation id
    emitter_node_id: str | None = None     # x-calf-emitter
    emitter_node_kind: str | None = None    # x-calf-emitter-kind
    deps: Mapping[str, Any] = field(default_factory=dict)
    resources: Mapping[str, Any] = field(default_factory=dict)  # node/worker lifecycle bag

    __hash__ = None  # holds a mutable State; mirror NodeResult

    # convenience read-throughs (identical to NodeResult)
    @property
    def output_parts(self) -> list[ContentPart]: return self.state.final_output_parts
    @property
    def message_history(self) -> list[ModelMessage]: return self.state.message_history
    @property
    def metadata(self) -> Any: return self.state.metadata
```

Field rationale: this is exactly `NodeResult`'s fields **plus** `resources` —
i.e. `ConsumerContext ≈ NodeResult + resources` — which is precisely the
node-local concern we are moving off the client type. `frame_id` is omitted (a sink
is frameless; always `None`).

## 3. Construction

The output projection (extract from `state.final_output_parts`, honoring
`output_type`/`type_adapter`, with the strict/empty rule) is **shared** with
`NodeResult`. Factor the current `NodeResult.from_context` body into a small helper
and call it from both:

```python
# calfkit/models/node_result.py  (or a neutral _projection module)
def project_output(state, output_type=_UNSET, *, strict, type_adapter=None) -> Any:
    if not state.final_output_parts and not strict:
        return None
    return _extract_output(state.final_output_parts, output_type, type_adapter=type_adapter)
```

`ConsumerContext` builds from the consumer's `SessionRunContext` (already stamped by
the handler), defaulting to **`strict=False`** (consumer semantics — intermediate
hops yield `output=None` instead of raising):

```python
@classmethod
def from_run_context(cls, ctx, output_type=_UNSET, *, type_adapter=None) -> "ConsumerContext[Any]":
    return cls(
        output=project_output(ctx.state, output_type, strict=False, type_adapter=type_adapter),
        state=ctx.state,
        correlation_id=ctx.correlation_id,
        emitter_node_id=ctx.emitter_node_id,
        emitter_node_kind=ctx.emitter_node_kind,
        deps=ctx.deps,
        resources=ctx.resources,   # <- sourced from the stamped node context, not the wire
    )
```

Note `resources` comes from `ctx.resources` (stamped by `prepare_context` /
`_effective_resources`), so the consumer needs no separate resources plumbing.

## 4. `ConsumerFn` and the decorator

```python
ConsumerFn = Callable[[ConsumerContext[OutputT]], None | Awaitable[None]]
```

Authoring (single context arg, tool-consistent vocabulary):

```python
@consumer(subscribe_topics="weather.out", output_type=WeatherReport)
async def sink(ctx: ConsumerContext[WeatherReport]):
    if ctx.output is None:
        return  # intermediate hop
    await ctx.resources["db"].save(ctx.output)   # same ctx.* as a tool
```

Decorator/constructor params are unchanged from today (`subscribe_topics`,
`output_type`, `node_id`, `gates`) — keep the public name `output_type`
(not `agent_output_type`).

## 5. `ConsumerNode.run` and the no-copy override

`ConsumerNode` rides `BaseNodeDef.handler` (per the frameless-handler change) and
implements `run(self, ctx)`:

```python
async def run(self, ctx: SessionRunContext) -> None:
    try:
        cctx = ConsumerContext.from_run_context(ctx, self._output_type, type_adapter=self._type_adapter)
    except (DeserializationError, ValidationError):
        logger.exception("consumer=%s projection failed; skipping", self.node_id); return
    except Exception:
        logger.exception("consumer=%s unexpected projection error; skipping", self.node_id); return
    try:
        ret = self._func(cctx)
        if inspect.isgenerator(ret) or inspect.isasyncgen(ret):
            raise TypeError("consume_fn returned a generator object; body never ran")
        if inspect.isawaitable(ret):
            await ret
    except asyncio.CancelledError:
        raise                       # never swallow cooperative cancellation
    except Exception:
        logger.exception("consumer=%s consume_fn raised", self.node_id)   # swallow + continue (no re_raise)
```

**No-copy override.** `BaseNodeDef.prepare_context` does `model_copy(deep=True)`,
which (a) breaks the documented `result.state is envelope.context.state` identity and
(b) deep-copies the full `State` (message history included) on *every* stream
envelope. A sink never mutates-and-publishes, so `ConsumerNode` overrides
`prepare_context` to stamp the inbound context **in place** (skip the deep copy),
matching the original handler's behavior and the no-copy tests.

`_type_adapter` is pre-built in `__init__` (as today) so schema-gen errors surface at
construction and the adapter is reused per envelope.

## 6. `NodeResult` cleanup

- Remove the `resources` field from `NodeResult`.
- Remove the `resources` parameter from `NodeResult.from_context` / `from_envelope`.
- Client path is unaffected (it never populated `resources`).

## 7. Migration & test impact

- New: `calfkit/models/consumer_context.py`; `ConsumerContext` added to
  `calfkit.models.__all__`.
- `node_result.py`: drop `resources`; extract `project_output`.
- `consumer.py` (and `consumer_v2.py` → folds into the real `consumer.py`): switch
  `ConsumerFn` to `ConsumerContext`, build it in `run`, add the `prepare_context`
  override.
- Tests: `test_lifecycle_resource_fields.py` / `test_lifecycle_resource_injection.py`
  resources-on-`NodeResult` assertions move to `ConsumerContext`; `test_consumer.py`
  sinks take `ctx: ConsumerContext` and read `ctx.output` (was `result.output`).
  This is a **breaking** change to the consumer authoring signature and the
  `NodeResult.resources` field — acceptable pre-1.0.

## 8. Future direction — a shared `NodeContext` base (unification note)

After this lands there are three context-ish types: `SessionRunContext` (internal
node context), `ToolContext` (tool authoring), and `ConsumerContext` (consumer
authoring). `ToolContext` and `ConsumerContext` share a vocabulary — `deps`,
`resources`, `correlation_id`, `emitter_node_id`/`kind` — but are built
independently (`ToolContext` extends `pydantic_ai.RunContext`; `ConsumerContext` is
standalone).

**Proposal:** extract a calfkit-owned `NodeContext` base that owns the shared
user-facing vocabulary, and have both authoring contexts extend it:

- `NodeContext`: `deps`, `resources`, `correlation_id`, `emitter_node_id`/`kind`
  (and likely `state` / `message_history`).
- `ToolContext(NodeContext)`: adds `tool_call_id`, `tool_name`. The
  `pydantic_ai.RunContext` surface becomes an internal adapter the agent loop
  passes to tool execution, not the public authoring surface.
- `ConsumerContext(NodeContext)`: adds `output`.

**Benefit:** one user-facing context vocabulary *by construction* — users learn
`ctx.deps` / `ctx.resources` / `ctx.correlation_id` once and it holds everywhere.

**Cost / risk:** `ToolContext` today *is* a `pydantic_ai.RunContext` (the agent loop
hands it to pydantic-ai's tool executor as the `RunContext`). Re-basing it onto a
calfkit `NodeContext` while still satisfying that interface means either multiple
inheritance or wrapping/adapting the pydantic-ai context — a deliberate, larger
refactor that touches the tool execution path. It is a natural fit for the
[Calfkit 1.0](calfkit-v1-design.md) rewrite rather than an incremental change, and is
intentionally **out of scope** for the `ConsumerContext` work above.

## 9. Open questions

- Should `ConsumerContext` be `@dataclass(frozen=True)` (like `NodeResult`) or a
  pydantic model? Frozen dataclass is consistent with `NodeResult` and avoids
  validation cost on a hot per-envelope path — recommended.
- Keep the convenience read-throughs (`output_parts`/`message_history`/`metadata`)
  or have authors read `ctx.state.*`? Mirroring `NodeResult` keeps parity; cheap to
  keep.
