# How to handle errors and faults

This guide shows you how to recover from failures and signal your own, using the
two error seams and the `NodeFaultError` / `ErrorReport` types. It assumes a
caller-capable node (an `Agent`, a tool node, or a `NodeDef` subclass). For
guarding input and reshaping output, see [How to guard and transform node
invocations](policy-seams.md).

**How failures work here, briefly.** calfkit has no dead-letter queue. A failure
— your node's, or a node it called — becomes a typed [`ErrorReport`](api.md#errorreport)
that travels the result rail and **escalates up the call chain until a node
handles it**. You intercept a fault in one of two seams — `on_callee_error` (a node
you called failed) or `on_node_error` (your own body failed) — or you observe it by
tapping a `publish_topic` with a [consumer](consumer-nodes.md).

Two things to know up front: a report's `retryable` flag is **advisory** (the
framework never retries for you), and a typed client-side `except NodeFaultError`
surface is planned but not yet available — so handle faults in-node today. For the
design rationale, see the [fault rail & policy seams
spec](designs/fault-rail-and-policy-seams-spec.md).

Seam handlers follow the same three moves as every seam: **return `None`** to let
the failure continue (escalate), **return a value** to recover, or **raise** to
replace one fault with another. Register them like any other seam (see [Register a
handler](policy-seams.md#register-a-handler)).

## Recover when a tool (or callee) fails

When a node your node called fails — most often a tool an agent invoked —
`on_callee_error` fires once per failed call, before the failure escalates. It
receives `(ctx, fault)`: `fault` is the callee's `ErrorReport`, and
`ctx.failing_call` is the failed call. Return a value to **substitute a result**
for that call (the agent continues as if the call had returned it); return `None`
to let it escalate.

```python
from calfkit import FaultTypes

@agent.on_callee_error
def fall_back_on_weather(ctx, fault):
    if fault.find("weather.upstream_timeout"):
        return {"summary": "weather unavailable", "temp_c": None}  # substitute the tool result
    return None  # any other tool failure escalates
```

## Recover from your node's own failure

When your node's body raises, `on_node_error` fires. It receives `(ctx, fault)` —
the synthesized `ErrorReport` — and `ctx.exception` holds the live exception.
Return a value to **recover** (it becomes the node's output); return `None` to let
the original fault escalate.

```python
@agent.on_node_error
def degrade_gracefully(ctx, fault):
    if fault.find(FaultTypes.MODEL_CONTEXT_WINDOW_EXCEEDED):
        return "That conversation got too long for me to process — try trimming it."
    return None  # everything else escalates unchanged
```

## Inspect a fault

Match a report with `.find(error_type)`, which searches the whole fault (a report
plus every nested cause) and returns the first match or `None`. **Use `.find()`,
never a bare `==`** — faults compose (a fan-out wraps its failed branches in a
group), so a top-level equality check would silently stop matching the day a node
fans out. To walk every fault yourself, use `.walk()`.

```python
@agent.on_node_error
def classify(ctx, fault):
    if fault.find(FaultTypes.FANOUT_ABORTED):
        ...  # a parallel batch could not complete
    for report in fault.walk():
        log.warning("fault %s: %s", report.error_type, report.message)
    return None
```

## Mint a deliberate fault

To signal a typed failure from your own code — a node body or any seam — `raise
NodeFaultError` with a dotted `error_type` callers can branch on:

```python
from calfkit import NodeFaultError

raise NodeFaultError(
    "billing.quota_exceeded",
    message="Monthly quota reached.",
    retryable=False,          # advisory only — the framework does not retry on it
    details={"limit": 1000},  # must be JSON-serializable
)
```

A mint converts to a fault verbatim and **bypasses `on_node_error`** (it is a
deliberate signal, not an accident to recover from). Two rules: `error_type` must
be non-empty, and the `calf.` prefix is **framework-reserved** — you cannot mint a
`calf.*` `error_type` or use a `calf.*` `details` key (so callers can trust those
codes are the framework's). The framework's own codes are constants on
[`FaultTypes`](api.md#faulttypes).

See also: the [Errors & faults reference](api.md#errors--faults) for the full
`ErrorReport` fields and `FaultTypes` catalogue, and [How to guard and transform
node invocations](policy-seams.md) for `before_node` / `after_node`.
