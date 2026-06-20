# How to guard and transform node invocations

This guide shows you how to run logic **around** a node's body — on the way in
and over its output — without editing the body. You use two policy seams:
`before_node` (runs before the body) and `after_node` (runs over the result). It
assumes you already have a caller-capable node: an `Agent`, a tool node, or a
`NodeDef` subclass. (Observer nodes built with `@consumer` have no seams — they
handle every event in their own body.)

To recover from a failure or a failed tool call, see
[How to handle errors and faults](error-handling.md).

## Register a handler

Pass a handler to the constructor — a single callable or a list — or attach it as
a decorator on the node instance. Both append to the same chain (constructor
entries first), the chain runs in registration order, and the **first handler to
return a non-`None` value wins**. Handlers may be sync or async.

```python
from calfkit.nodes import Agent

agent = Agent("planner", subscribe_topics="planner.in", model_client=model)

# Decorator form — repeatable; the handler stays a plain function:
@agent.before_node
def add_locale_hint(ctx):
    ...

# Constructor form — a callable or a list; these run before any decorated handlers:
agent = Agent(
    "planner", subscribe_topics="planner.in", model_client=model,
    after_node=[redact, audit],
)
```

Every seam handler has exactly three moves, and they mean the same thing
everywhere: **return `None`** to let the flow continue, **return a value** to
*substitute*, or **raise** to *stop*. What each does depends on the seam.

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

## Answer directly without running the body

Return a value from `before_node` to **short-circuit**: the body is skipped, your
value becomes the node's output, and it is still passed through `after_node` and
replied/published as usual. Use it for a cache hit or a canned response.

```python
@agent.before_node
def serve_cached(ctx):
    return cache.get(ctx.correlation_id)  # None → run the agent; a value → return it as the output
```

## Block an invocation

To **reject** an invocation so the body never runs, `raise`. Raising — not a
returned value — is how you stop a flow that would otherwise proceed: a returned
value is a *substitution*, an exception is a *stop*. Raise `NodeFaultError` to send
the caller a clean, typed fault:

```python
from calfkit import NodeFaultError

@agent.before_node
def require_entitlement(ctx):
    if not ctx.deps.get("entitled"):
        raise NodeFaultError("billing.not_entitled", message="No active plan.")
    # return None → entitled callers proceed to the body
```

The caller receives the fault on the result rail; the body and `after_node` are
skipped. Any other exception also stops the flow but surfaces as a generic
`calf.unhandled` fault — prefer `NodeFaultError` so callers get a code they can
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

`after_node` is values-only: returning an *action* (a `Call`/`ReturnCall`/…)
raises `SeamContractError`. To stack behavior, register several handlers — they
run in order and the first non-`None` return wins, so place specific handlers
before general ones.

See also: the [Policy seams reference](api.md#policy-seams) for the full
signature and return-value table, and [How to handle errors and
faults](error-handling.md) for the error seams (`on_node_error` /
`on_callee_error`) and minting faults.
