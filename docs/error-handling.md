# How to handle errors and faults

This guide shows you how to recover from failures and signal your own, using the
error seams and the `NodeFaultError` / `ErrorReport` types. It assumes a
caller-capable node (a `StatelessAgent`, a tool node, or a `NodeDef` subclass). For
guarding input and reshaping output, see [How to guard and transform node
invocations](policy-seams.md).

**How failures work here, briefly.** calfkit has no dead-letter queue. A failure
— your node's, or a node it called — becomes a typed [`ErrorReport`](api.md#errorreport)
that travels the result rail and **escalates up the call chain until a node
handles it**. You intercept a fault in an error seam — an agent's `on_tool_error`
(a tool it called failed — the promoted surface, with the failing tool first-class),
the general `on_callee_error` (any callee failed), or `on_node_error` (your own body
failed) — or you observe it by tapping a `publish_topic` with a
[consumer](consumer-nodes.md).

Two things to know up front: a report's `retryable` flag is **advisory** (the
framework never retries for you), and a fault surfaces in **two** places — in-node
via the error seams (below), and at the **client**, where a run's fault raises
`NodeFaultError` from `result()` / `execute()` (see [Client-side
errors](#client-side-errors)). For the design rationale, see the [fault rail &
policy seams spec](designs/fault-rail-and-policy-seams-spec.md).

Seam handlers follow the same three moves as every seam: **return `None`** to let
the failure continue (escalate), **return a value** to recover, or **raise** to
replace one fault with another. Register them like any other seam (see [Register a
handler](policy-seams.md#register-a-handler)).

## Recover when a tool an agent called fails

When a tool an agent invoked fails, register **`on_tool_error`** — the promoted
agent surface, with the failing tool first-class. It fires once per failed call,
before the failure escalates, and receives `(tool_call, ctx, report)`: `tool_call`
is the failing [`ToolCall`](api.md#toolcall) (`.tool_name` / `.args`), `report` is
the tool's `ErrorReport`, and `ctx` is the seam context. Branch on
`tool_call.tool_name`; return a value to **substitute a successful result**, return
`retry_text_part(msg)` to **show the model an error** it can react to, or return
`None` to let the failure escalate.

```python
from calfkit import retry_text_part

@agent.on_tool_error
def handle_tool_failure(tool_call, ctx, report):
    if tool_call.tool_name == "get_price":
        return last_known_price(ctx)                                 # substitute a fallback → the model sees a success
    if tool_call.tool_name == "web_search":
        return retry_text_part(f"search failed: {report.message}")   # a model-visible error — the model can retry
    return None                                                      # any other tool failure escalates
```

Pass a single handler or a list to the `on_tool_error=` constructor argument, or
use the `@agent.on_tool_error` decorator (repeatable). Self-healing is opt-in and
visible: with no `on_tool_error` handler, a tool failure escalates and the model
sees nothing (the default).

### Show the model every tool failure

For the common case — "let the model see and react to whatever broke" — register
the zero-argument **`surface_to_model()`** prebuilt. It converts *every* tool
failure into a model-visible error result (the top exception line, e.g.
`RateLimitError: rate limit exceeded`), so the model can retry or adapt:

```python
from calfkit import surface_to_model

agent = StatelessAgent(..., on_tool_error=[surface_to_model()])
```

`surface_to_model()` is unbounded — it converts every matching fault, so the loop
is bounded only by the agent's turn limit (a per-tool budget is a future addition).
For per-tool policy, write your own handler and branch on `tool_call.tool_name`.

### The general mechanism: `on_callee_error`

`on_tool_error` is sugar over **`on_callee_error`**, the general seam every
caller-capable node has (not just agents). Use `on_callee_error(ctx, fault)`
directly when the caller is not an agent, or when you don't need the failing tool's
identity — `ctx.failing_call` still identifies the failed call:

```python
from calfkit import FaultTypes

@node.on_callee_error
def fall_back_on_weather(ctx, fault):
    if fault.find("weather.upstream_timeout"):
        return {"summary": "weather unavailable", "temp_c": None}  # substitute the callee result
    return None  # any other callee failure escalates
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

## Client-side errors

A [client](client-features.md) that calls a node and awaits its reply sees the run's
fault as a raised exception — the same `ErrorReport`, now on the caller's side. The
typed set over run outcomes:

| Condition | Type |
| --- | --- |
| The run (or a node it called) faulted | **`NodeFaultError`** — `e.report` is the verbatim `ErrorReport`; branch with `.find(...)` |
| A successful reply fails a **structured** `output_type` | **`DeserializationError`** (`output_type=str` never raises this — it coerces) |
| This client's `result(timeout=)` / `execute(timeout=)` elapsed | **`ClientTimeoutError`** — the run is unaffected |
| The client was closed (`aclose()`) with the run in flight | **`ClientClosedError`** — the run is unaffected |

```python
from calfkit import NodeFaultError, ClientTimeoutError, FaultTypes

try:
    result = await client.agent("billing").execute("refund order 1234", timeout=30)
except NodeFaultError as e:
    if e.report.find(FaultTypes.MODEL_CONTEXT_WINDOW_EXCEEDED):
        ...                       # branch on the slotted report (find(), never ==)
except ClientTimeoutError:
    ...                           # this client gave up; the run still runs
```

`ClientTimeoutError` / `ClientClosedError` are **run-survives** signals — never a bare
`asyncio.TimeoutError` / `CancelledError`. A `send()` registers no handle, so its
fault is not raised to the caller; observe it on the `events()` firehose (a fault with
no pending handle is also ERROR-logged, never silently dropped).

A fault that grows too large to publish is **degraded, not dropped**: the framework
re-publishes it with the run state elided (the `ErrorReport` you receive is unaffected),
so an oversized failing turn still surfaces as a `NodeFaultError` rather than silence. See
[Oversized faults degrade, never silently drop](api.md#oversized-faults-degrade-never-silently-drop).

See also: the [Errors & faults reference](api.md#errors--faults) for the full
`ErrorReport` fields and `FaultTypes` catalogue, and [How to guard and transform
node invocations](policy-seams.md) for `before_node` / `after_node`.
