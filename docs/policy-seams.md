# How to guard and transform node invocations

This guide shows you how to run logic **around** a node's body — on the way in
and over its output — without editing the body. You use two policy seams:
`before_node` (runs before the body) and `after_node` (runs over the result). It
assumes you already have a caller-capable node: a `StatelessAgent`, a tool node, or a
`NodeDef` subclass. (Observer nodes built with `@consumer` have no seams — they
handle every event in their own body.)

To recover from a failure or a failed tool call, see
[How to handle errors and faults](error-handling.md).

## Register a handler

Register a handler two ways: pass it to the node's constructor (`StatelessAgent` or a
`NodeDef` subclass) — a single callable or a list — or attach it as a decorator on
the node instance. The decorator works on any node and is the only form for tool
nodes (which you build with `@agent_tool`). Both feed the same chain (constructor
entries first); it runs in registration order, and the **first handler to return a
non-`None` value wins**. Handlers may be sync or async.

```python
from calfkit.nodes import StatelessAgent

agent = StatelessAgent("planner", subscribe_topics="planner.in", model_client=model)

# Decorator form — repeatable; the handler stays a plain function:
@agent.before_node
def add_locale_hint(ctx):
    ...

# Constructor form — a callable or a list; these run before any decorated handlers:
agent = StatelessAgent(
    "planner", subscribe_topics="planner.in", model_client=model,
    after_node=[redact, audit],
)
```

Every seam handler has the same three moves: **return `None`** to let the flow
continue, **return a value** to take over with your own result, or **raise** to
stop the flow. What each move means depends on the seam — the sections below show
each.

## Transform the input before the body runs

`before_node` receives a [`SeamContext`](api.md#seamcontext). Its `state` is the
sanctioned input-transform channel — mutate it in place and return `None`, and the
body runs with your change. For an agent, setting `state.temp_instructions` injects
a one-shot instruction for this run:

```python
@agent.before_node
def add_locale_hint(ctx):
    locale = ctx.deps.get("locale", "en-US")
    ctx.state.temp_instructions = f"Answer in the user's locale ({locale})."
    # return None → the agent runs with the instruction applied
```

## Short-circuit the body

Return a value from `before_node` to **short-circuit** the body: it is skipped, your
value becomes the node's output, and it still passes through `after_node` and is
replied/published as usual. Use it for a cache hit or a canned response.

```python
@agent.before_node
def serve_cached(ctx):
    return cache.get(ctx.correlation_id)  # None → run the agent; a value → return it as the output
```

## Block an invocation

To **block** an invocation so the body never runs, `raise` — a returned value
short-circuits the body, but only a raised exception stops the flow. Raise
`NodeFaultError` to send the caller a clean, typed fault:

```python
from calfkit import NodeFaultError

@agent.before_node
def require_entitlement(ctx):
    if not ctx.deps.get("entitled"):
        raise NodeFaultError("billing.not_entitled", message="No active plan.")
    # return None → entitled callers proceed to the body
```

The caller receives the fault on the result rail; the body and `after_node` are
skipped. Any other exception also blocks the invocation but surfaces as a generic
`calf.exception` fault — prefer `NodeFaultError` so callers get a code they can
branch on. See [How to handle errors and faults](error-handling.md) for minting,
the reserved `calf.*` namespace, and the caller-side `ErrorReport`.

> A seam **cannot** silently drop a reply-owing message. To ignore messages
> addressed to another node on a shared topic without replying at all, that's a
> routing decision (the node's route handler declines), not a seam.

## Validate or reshape the output

`after_node` receives `(ctx, output)` — the produced output. Return `None` to
accept it, return a value to **replace** it, or `raise` to **reject** it and fault
instead of publishing something bad:

```python
@agent.after_node
def redact(ctx, output):
    if contains_pii(output):
        return scrub(output)   # replace the output
    return None                # accept as-is
```

`after_node` takes output values only: returning a framework *action* object (the
kind a node body returns to dispatch a call, not a plain output) raises
`SeamContractError`. To stack behavior, register several handlers — they run in
order and the first non-`None` return wins, so place specific handlers before
general ones.

See also: the [Policy seams reference](api.md#policy-seams) for the full
signature and return-value table, and [How to handle errors and
faults](error-handling.md) for the error seams (an agent's `on_tool_error`, the
general `on_callee_error`, and `on_node_error`) and minting faults.
