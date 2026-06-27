# Client Caller-Side Developer Surface — Design Spec

**Status:** design spec of the *settled* design. It describes the **end-product** — the
developer surface AND the internal implementation it should have — on its own engineering
merits, **independent of calfkit's current code or any migration**. Current code and the size
of any migration have NO bearing on this design; the migration / code-change plan belongs in a
separate implementation plan, not here. Anything not yet decided is named in §9 rather than
specified — do not infer scope beyond what is written. The full surface signatures are written
out in §2–§3; they are the contract.

**Types the design uses (chosen on merit, not for migration convenience):**
- `InvocationResult` — a rich result (full session `State` + `message_history`), chosen so multi-turn continuation and session visibility are first-class.
- `NodeFaultError` / `ErrorReport` / `FaultTypes` — the typed fault value consumers branch on.
- `DeserializationError` — the projection-mismatch error.
- `State`, `ModelMessage`, `ModelSettings`, `ToolBinding` / `ToolProvider` — the typed call knobs.

Scope is the **caller boundary**: code outside the system (web handlers, CLIs, cron, workers)
that dispatches work into calfkit and observes results. Not the authoring surface, not the runtime.

---

## 1. Overview

A root **`Client`** connects to the broker and mints a typed **`AgentGateway`** per deployed
agent. Each gateway speaks the verb triad **`send` / `start` / `execute`**. Replies and progress
are observed two ways: per-run (**`InvocationHandle.stream()` / `.result()`**) or cross-run
(**`client.events()`**, a firehose). One identity noun throughout: **`correlation_id`**.

```python
client = Client.connect("localhost:9092")        # SYNC — lazy; no I/O until first dispatch (§2.7)
result = await client.agent("summarizer", output_type=Summary).execute("Summarize the repo")
print(result.output)            # Summary(...) — typed, validated
print(result.message_history)   # full conversation — feed into the next turn
```

The conceptual load of the common path: **kind (method) · identity (`name`) · expectation
(`output_type`) · verb.** The design uses a rich result (full session state + message history) so
multi-turn continuation and session visibility are first-class, and a closed, typed error model.

---

## 2. The developer surface

### 2.1 `Client` and `connect`

```python
class Client:
    @classmethod
    def connect(                       # SYNC — lazy; no I/O until the first dispatch/events() (§2.7)
        cls,
        server_urls: str | Iterable[str] = ...,
        *,
        inbox_topic: str | None = None,
        deps_factory: Callable[[], dict[str, Any]] | None = None,
        firehose_buffer_size: int = DEFAULT_FIREHOSE_BUFFER_SIZE,
        **broker_kwargs: Any,          # forwarded to the broker, incl. security= (§2.7)
    ) -> Client: ...

    async def aclose(self) -> None: ...
    async def __aenter__(self) -> Client: ...
    async def __aexit__(self, *exc) -> None: ...   # calls aclose()
```

- **`inbox_topic`** — the named topic this client receives its runs' events + terminal replies
  on (and routes its dispatches' callbacks to). `None` (default) → an ephemeral per-client inbox
  (nothing to provision). Set it → a durable, named inbox another process can consume (see §6). On a
  shared inbox, `correlation_id` is a **global** demux key, so caller-supplied ids must be unique
  within it.
- **`deps_factory`** — seeds ambient `deps` merged *under* each call's per-call `deps`.
- **`firehose_buffer_size`** — a **second-class tuning knob**: the bound (in events) of each
  `events()` observer's in-memory buffer (§5.4). The firehose is **best-effort** — a reader that falls
  behind drops its oldest buffered events (signaled), it never blocks the hub. Defaults to a named
  constant `DEFAULT_FIREHOSE_BUFFER_SIZE` (low thousands — sized for coarse whole-turn events; §5.4).
  Set once here at the client level; there is no per-call `events(buffer_size=…)` override in v1 (a
  cheap, non-breaking future add — §9.1). Worst-case firehose memory ≈ `firehose_buffer_size ×
  event_size × open observers`.
- There is **no `reply_ttl`** — abandoned runs garbage-collect on their own (the hub holds weak refs,
  §5.2); per-call patience is `result(timeout=…)` (§4.3).
- The client is an async context manager (one long-lived client per app). `aclose()` is graceful
  (§5.8).

### 2.2 `AgentGateway` and the verb triad

```python
# Four overloads — a dedicated no-output_type form per address so the str default
# is actually bound (a parameter default `= str` does NOT bind a TypeVar; OutT would
# fall back to its `Any` default and the str typing would be silently lost).
@overload
def agent(self, name: str) -> AgentGateway[str]: ...
@overload
def agent(self, name: str, *, output_type: type[OutT]) -> AgentGateway[OutT]: ...
@overload
def agent(self, *, topic: str) -> AgentGateway[str]: ...
@overload
def agent(self, *, topic: str, output_type: type[OutT]) -> AgentGateway[OutT]: ...
```

A gateway is minted for **one** destination, addressed **exactly one** of two ways:
- by **`name`** — a deployed agent. The name **derives** the agent's **Private input topic**
  (`{node_kind}.{name}.private.input`, ADR-0017) — the deterministic name→topic mapping; the caller
  never handles a raw topic. The normal case.
- by **`topic=`** — the escape hatch, for a topic that is *not* a node's derived Private input
  topic (e.g. a shared-ingress work topic several agents subscribe to).

"Both" and "neither" are rejected (overloads + a runtime guard). `output_type` binds **once at
mint** (the per-deployment type); it **defaults to `str`** (extract the text reply). A
structured-output agent requires `output_type=Model` — omitting it makes a structured (`DataPart`)
reply a loud `DeserializationError` (§2.5), never a silent `Any`.

(The agent's **Private input topic** is the *call destination*; the client's `inbox_topic` (§6) is
the separate *reply destination* — two different topics. "Inbox" in this spec always means the
client's reply channel.)

The triad — resolve contract = "durably accepted," not "completed":

```python
class AgentGateway(Generic[OutT]):
    async def send(
        self, prompt: str, *,
        correlation_id: str | None = None,
        message_history: list[ModelMessage] | None = None,
        deps: dict[str, Any] | None = None,
        model_settings: ModelSettings | None = None,
        temp_instructions: str | None = None,
        tool_overrides: Sequence[ToolBinding | ToolProvider] | None = None,
        author: str | None = None,
    ) -> Dispatch: ...                                     # a fire token; .correlation_id, no result()

    async def start(
        self, prompt: str, *,
        correlation_id: str | None = None,
        message_history: list[ModelMessage] | None = None,
        deps: dict[str, Any] | None = None,
        model_settings: ModelSettings | None = None,
        temp_instructions: str | None = None,
        tool_overrides: Sequence[ToolBinding | ToolProvider] | None = None,
        author: str | None = None,
    ) -> InvocationHandle[OutT]: ...

    async def execute(
        self, prompt: str, *,
        timeout: float | None = None,
        correlation_id: str | None = None,
        message_history: list[ModelMessage] | None = None,
        deps: dict[str, Any] | None = None,
        model_settings: ModelSettings | None = None,
        temp_instructions: str | None = None,
        tool_overrides: Sequence[ToolBinding | ToolProvider] | None = None,
        author: str | None = None,
    ) -> InvocationResult[OutT]: ...
```

- **`send`** — dispatch without awaiting; resolves when durably accepted; returns a **`Dispatch`**
  (carrying `.correlation_id`) — deliberately **not** an `InvocationHandle`: it has no
  `result()`/`stream()`, so the *type itself* says the result isn't retrievable by id. Its terminal
  routes to the client's `inbox_topic` (observable via `events()`), but `send()` registers no per-run
  handle. Observe a `send()` result through a running firehose, or use `start()` / `execute()` if you
  need the result.

```python
@dataclass(frozen=True)
class Dispatch:        # what send() returns — a fire token, deliberately NOT a handle
    correlation_id: str
```
- **`start`** — dispatch and return a handle; completion is a separate, explicit step. The handle
  is the **only** way to get this run's per-run result/stream — hold it for the run's lifetime
  (§4.6). There is no reattach-by-`correlation_id`.
- **`execute`** — `start` + `result`; the request/response convenience. `timeout` is client-side
  patience with no default (§4.3).

There is no `handle_for` reattach verb (a deliberate v1 cut — §9.2; the durable-store future home is §9.1).

### 2.3 Per-call knobs

The headline input is a plain **`str`** prompt — there is no `Turn` envelope, because the typed
knobs *are* the structured input. `output_type` is mint-bound (§2.2); the rest ride each call:

- **`correlation_id: str | None`** — the demux key (§5) and firehose correlation key. **The safe
  default is to omit it** — it auto-mints a uuid7, collision-free by construction. Supplying your own
  is an **opt-in** for idempotency / external-system correlation, and with it **you own uniqueness
  within the inbox.** What the framework does and does not police, and *why*:
  - **Guarded (cheap, in-memory):** a duplicate of a run **whose handle is still held** is rejected
    synchronously (`ValueError`, §5.2). The framework does not de-duplicate or attach.
  - **Not guarded, deliberately:** a duplicate of a run whose handle was already **dropped** (the run
    may still be live), or a **cross-client collision on a shared inbox**, is **undefined** — the
    firehose/hub would interleave two runs under one id. Detecting these would require an **unbounded
    "every id ever seen" set**, which would break the memory-bounded-by-held-handles design (§5.2, the
    reason there is no `reply_ttl`). We refuse to trade that bound to police a caller error — so it is
    the caller's responsibility, and auto-mint avoids it entirely. (No bounded "recent ids" half-guard
    either — it neither fully solves the collision nor keeps the model clean.)
- **`message_history: list[ModelMessage] | None`** — prior turns for multi-turn (§2.4); feed
  `result.message_history` back in.
- **`deps: dict[str, Any] | None`** — per-call dependencies merged over the `deps_factory` seed,
  carried to the run's tools. Must be JSON-serializable; a non-serializable value surfaces as a
  serialization error at publish — **no call-site pre-flight** (deliberate: let it bubble downstream).
- **`model_settings: ModelSettings | None`** — per-call model settings (e.g. `{"temperature": 0}`)
  merged over the agent's defaults. JSON-serializable (same serialization-time failure as `deps`).
- **`temp_instructions: str | None`** — system-level instructions injected into this turn only.
- **`tool_overrides: Sequence[ToolBinding | ToolProvider] | None`** — runtime agent-tool overrides
  for this invocation.
- **`author: str | None`** — name of the human author of `prompt`; surfaces as `<user:author>`
  attribution when two or more named humans share a channel.

> (`route` / `body` — node-route targeting and a typed run payload — are **not** on the agent
> surface; they belong to a lower-level node-call surface, out of scope here.)

### 2.4 `InvocationResult` — the rich result

```python
@dataclass(frozen=True)
class InvocationResult(Generic[OutT]):
    output: OutT                     # typed via output_type; NON-optional on the client surface
    state: State                     # full session state (treat read-only)
    correlation_id: str
    output_parts: list[ContentPart]  # the raw reply parts this projected from
    emitter_node_id: str | None      # who produced the terminal (honest after a handoff)
    emitter_node_kind: str | None
    deps: Mapping[str, Any]          # inbound deps, read-only
    resources: Mapping[str, Any]     # the producing node's resources, read-only
    # message_history, metadata are convenience properties reading through `state`
```

**`output` is non-optional on the client surface** (`OutT`, never `OutT | None`). A client `result()`
only ever resolves a *successful terminal*: a fault **raises** `NodeFaultError`, a projection mismatch
**raises** `DeserializationError`, and the client never receives an intermediate hop via `result()` —
so there is no path on which a returned `output` is `None`. The caller writes `result.output.field`
without a `None` dance. (The consumer/runtime path uses a **separate** type — `ConsumerContext`, with
its own `strict=False` intermediate-hop `None` — so `InvocationResult` is **already client-only**.
**Implementation:** tighten the existing `InvocationResult.output` from `OutputT | None` to
`OutT` (`calfkit/models/node_result.py`); `strict=True` always populates it on success, so the tighten
is sound. For the tighten to be *type-sound*, `InvocationResult.from_context` / `from_envelope` must no
longer expose a caller-overridable `strict` (a `strict=False` call would inject `output=None` into a
non-optional field) — hard-code their internal `project_output(..., strict=True)` and keep the `strict`
knob only on the shared `project_output` and `ConsumerContext.from_run_context`. No new type and no
`_ResultBase` split is needed — the earlier "shared with the consumer" framing was a misread.)

**Multi-turn continuation** is first-class: `result.message_history` feeds straight into the next
call's `message_history=`. `state` is a mutable Pydantic model shared with the envelope — **treat
it (and `deps`/`resources`) as read-only.**

### 2.5 Errors (typed; flat hierarchy)

The closed, typed set is over **run outcomes** — the terminal of an *accepted, started* run:

| Condition | Type | Notes |
|---|---|---|
| Downstream / run fault (incl. delivery faults like `calf.delivery.undecodable`) | **`NodeFaultError`** | branch on `e.report.find(FaultTypes.X)` / `e.report.error_type` |
| Successful reply fails `output_type` | **`DeserializationError`** | projection mismatch **only** — a *present-but-wrong* part on a `return` |
| **This client** stopped waiting (`result(timeout=)`) | **`ClientTimeoutError`** | run is unaffected |
| Client closed with the run in-flight | **`ClientClosedError`** | `aclose()` resolved a pending `result()` (§5.8) |

- **`ClientTimeoutError` / `ClientClosedError`** are typed, run-survives signals — **never a bare
  `asyncio.TimeoutError` / `CancelledError`.**
- **No artificial base class** — exceptions are flat.
- **Outcome discriminator runs BEFORE projection**: a fault re-materialises as `NodeFaultError`;
  only a *successful* reply is projected, so a downstream failure is never disguised as a
  `DeserializationError`. The reception mechanism is designed in **§5.9**.

**Dispatch-time failures (NOT run outcomes).** A call that never reaches "durably accepted" fails
straight out of `send`/`start`/`execute`, before any `correlation_id` has a terminal — so these are
deliberately **outside** the closed run-outcome set above:
- **Un-serializable input** (`deps` / `model_settings` carrying a non-JSON value): **no call-site
  pre-flight** — the serialization error bubbles from `publish` (§2.3). Kept simple on purpose; the
  cost is a less-localized error, accepted.
- **Broker / connection failure**: the underlying broker exception **bubbles raw** (not wrapped in a
  calfkit type) — connectivity is the broker's domain, and its error is the honest signal (document,
  don't police). Because `connect()` is lazy (§2.7), this surfaces from the **first**
  `send`/`start`/`execute`/`events()` (which brings the broker up), not from `connect()`.

> **The client error rail.** `NodeFaultError` *reception* — classifying a `FaultMessage` reply and
> raising it — together with `ClientTimeoutError` / `ClientClosedError` is designed in §5.9.
> (Implementation tracked as #250; the migration is the impl plan's concern, not this spec's.)

### 2.6 The dispatch envelope (outbound wire)

`start`/`send` publish ONE call envelope to the resolved Private input topic:
- an `Envelope` carrying the session `State` (built from `prompt` + `message_history`,
  `temp_instructions` injected) and `deps`, plus runtime `overrides` (`tool_overrides` /
  `model_settings`) and a pushed `CallFrame` whose `callback_topic` = the client's `inbox_topic`;
- headers: the emitter id (`x-calf-emitter`), emitter kind (`client`), and `x-calf-kind=call`.

The Kafka `correlation_id` header is the run id. (The client's outbound call is **not** key-keyed;
ordering of a run's *replies* on the inbox is guaranteed by the worker keying every reply publish
by `correlation_id` — §3.3.)

### 2.7 Connection & security

`connect()` is **lazy and synchronous** — it builds the client (config, the *unstarted* broker, the hub
objects) and **registers the hub's reply subscriber** on the inbox; it does **no I/O**, so a connection
failure surfaces from the **first** dispatch / `events()`, not from `connect()` (§2.5). The hub reader is
a **FastStream handler subscriber** (§5.1) — a **groupless *topic* subscription** (`subscribe(inbox)`,
**`group_id=None`**, `auto_offset_reset="latest"`; tail-only, no rebalance/commit, §5.7). Groupless +
*topic-subscribe* (not manual `assign()`) is deliberate: aiokafka's no-group path auto-assigns **all**
partitions at consumer start (`NoGroupCoordinator.assign_all_partitions`, refreshed on repartition), so
there is **no partition enumeration** and a multi-partition inbox is fully covered — manual `assign()`
is the *only* config that would need a partition list. Its handler *is* the hub: per
reply it classifies `x-calf-kind`, demuxes by `correlation_id` into the owning run's channel, and tees
to the firehose outlets (§5.1). Registered at `connect()` (pure bookkeeping, no I/O), it is **started by
the first `broker.start()`** — whether that is the client's first dispatch (the broker comes up before
the publish) **or** a co-located `Worker`'s `app.start()`. Because the subscriber is registered before
any start, there is **no subscribers-before-start conflict** and **no Worker change** — the `Worker`
keeps using the shared broker exactly as today. There is **no eager connect, no separate consumer, and
no `seek_to_end` gate**: the reply subscriber is started before the first publish, so it positions at the
tail (latest) within the reply round-trip — a reply cannot arrive before the consumer's first poll (a
full publish→node→reply round-trip vastly exceeds a local first-poll), so the M4-class "co-located
terminal precedes the reader" race cannot bite for a client's own reply. Running on the **handler path**
keeps FastStream's Envelope decode, the **live decode floor** (`DecodeFloorMiddleware`), context
injection, and security as **single canonical implementations** (not reimplemented). **Auth/security is
configured the broker's way:** pass
a FastStream `security=` object via `**broker_kwargs` (e.g. `security=SASLPlaintext(username=…,
password=…)`), applied to producer, consumer, and any admin client; raw security kwargs are rejected
with an actionable error. Topic existence (the inbox, the agents' input topics) is an operational
contract like every topic in the project — documented, not policed by app-code boot checks.

---

## 3. The streaming surface

Two public read tiers, both first-class.

### 3.1 `handle.stream()` — per-run

```python
class InvocationHandle(Generic[OutT]):
    correlation_id: str
    async def result(self, *, timeout: float | None = None) -> InvocationResult[OutT]: ...
    def stream(self) -> AsyncIterator[RunEvent]: ...
```

`stream()` yields this run's events in order, **terminal-bearing** (the last element is always the
terminal; "quiet" e.g. a pending approval means alive, not finished).

> **v1 reality:** the fabric emits no intermediate events yet (§9.1), so today `stream()` yields
> **exactly one** element — the terminal. The intermediate vocabulary below is the future shape.

### 3.2 `client.events()` — the firehose (cross-run)

```python
def events(self, *, terminal_only: bool = False) -> EventStream: ...      # complete firehose

class EventStream:                       # async context manager AND async iterable
    dropped: int                         # cumulative events this observer dropped (best-effort, §5.4)
    async def __aenter__(self) -> EventStream: ...
    async def __aexit__(self, *exc) -> None: ...
    def __aiter__(self) -> EventStream: ...
    async def __anext__(self) -> RunEvent: ...
```

`events()` reads the client's **one configured inbox** (`connect(inbox_topic=…)`) — there is no
per-call topic override, so the firehose and the hub are **always the same topic** and share **one
upstream consumer** (§5.1/§5.4). It yields **RAW** events across **all** runs on that inbox while open;
the **caller** demuxes by `correlation_id` **and is responsible for its own dedup / terminal handling**
— the firehose has no per-run dedup and (under at-least-once redelivery) may surface a duplicate or a
post-terminal event for a `correlation_id` (§5.5). Knob:
- **`terminal_only=`** — keep only outcomes.

`events()` is the single-reader firehose: while you keep up, you see **every** event on the inbox. But
it is **best-effort, not lossless** — it is a bounded **drop-oldest** outlet (`firehose_buffer_size`,
§2.1/§5.4): a reader that **falls behind** drops its oldest buffered events (signaled by a dropped-count
/ WARNING, never silent) so it can never block the hub. **Drain it promptly; for guaranteed delivery,
hold the run's handle (`start`/`execute`) or run a `@consumer` node** — the firehose is observation, not
a delivery guarantee. To **observe a different topic**, connect a separate client to it
(`Client.connect(inbox_topic=…)`) and call `events()` there — the §6 observer pattern. Consuming a
shared inbox as a **load-balanced worker pool** is deliberately *not* a client method — it is a
`@consumer` node's job (the framework's durable, consumer-group topic-consumption surface); see §9.1.

### 3.3 The `RunEvent` taxonomy — settled *principles*

```python
# Terminal set (settled, v1): a run's stream ends in exactly one of these.
@dataclass(frozen=True)
class RunCompleted: output: Any; correlation_id: str; agent: str | None   # success (raw output)
@dataclass(frozen=True)
class RunFailed:    report: ErrorReport; correlation_id: str   # the fault value; → NodeFaultError(report) (§5.9)
# Intermediate types (AgentMessage, ToolCalled, HandoffOccurred) AND RunRejected (an approval
# rejection — it has no ErrorReport to bind, so its error mapping is settled with the feature)
# are a FUTURE shape, emitted only once approval/gate seams + intermediate emission ship — §9.1.
RunEvent = RunCompleted | RunFailed                 # (v1; widened when those land)
```

- **Closed, `match`-friendly union** — exhaustive and discoverable at the call site.
- **Terminal-bearing** — `result()` maps the terminal to a value or a typed error: **success** →
  projected `InvocationResult`; **fault** → `NodeFaultError` (§2.5). (Approval *rejection* and its
  error mapping land with the deferred approval feature — §9.1 — not v1.) `stream()`
  yields the terminal **raw** as its last element.
- **Whole-turn grain, not token deltas.** Token-level streaming is explicitly not this surface.
- **Honest-naming rule** — ship an event type only once the fabric actually emits it.

**Per-run ordering of the terminal is a fabric property.** Every worker reply publish (returns and
faults to the callback inbox) is keyed by `correlation_id` (`calfkit/nodes/base.py` —
`key=correlation_id.encode()`), so a run's replies are **co-partitioned and offset-ordered**; the
"terminal is last" invariant holds on the **per-run channel** even though the hub reads all
partitions (a single run lives on one partition; cross-partition interleaving only mixes *different*
runs, which we demux anyway). **Note for the deferred intermediate-streaming feature (§9.1):**
this is true *today only for the single terminal return/fault on the callback inbox*. Intermediate
progress events do not route to the inbox yet and the broadcast rail is not `correlation_id`-keyed —
so intermediate streaming requires NEW worker-side routing-to-inbox **and** `correlation_id`-keying
on whatever rail carries it. It is not free.

---

## 4. Read semantics

### 4.1 Tail-only — no replay-from-0
Readers never replay an inbox from offset 0 (a footgun: unbounded re-read + accidental reprocessing).

### 4.2 Per-run delivery is hub-push, not a per-run offset
A per-run reader (`result()`/`stream()`) does **not** open a Kafka cursor or seek an offset. The
**hub** (the client's one consumer, §5.1) reads the inbox once and **pushes** each event into the
owning handle's in-memory channel (§5.2); `result()`/`stream()` read that channel. A terminal that
arrives before the caller reads is **buffered** in the channel (§4.3). There is no "dispatch offset"
on the per-run path — the only real Kafka cursor is the **single upstream inbox reader** (§5.1), whose
raw outlet the firehose (§3.2) exposes; the per-run channels are downstream, in-memory.

### 4.3 `result()` awaiting semantics
`result()` **awaits** (asynchronously parks) until the terminal lands, then resolves to a value or
raises a typed outcome (§2.5) — it does not poll or return early.
- Called before the terminal exists (the common case) → parks and resumes when it arrives.
- Terminal already arrived (fast run) → it is **buffered** in the channel (so long as you retained
  the handle — §5.2), read immediately.
- **No default timeout — deliberate, and not a buried safety knob.** A durable run may legitimately
  pause for a long time (a long agent task, a slow tool, a pending approval), so a default bound would
  spuriously fail legitimate runs — and spurious failures are worse than a hang you can cancel. The
  timeout is an explicit, visible knob, never a hidden default. **For request/reply, pass
  `result(timeout=T)` / `execute(timeout=T)`** to bound the *client's* patience → `ClientTimeoutError`
  (the run is unaffected); without it, `result()`/`execute()` wait indefinitely **by design**.

### 4.4 `result()` / `stream()` coexistence on one handle — "one drain, two faces"
Both read the **same** handle-owned channel, fed by the hub push. The handle's internal driver,
**pumped by the hub (not by reader pull)**, **caches the terminal** and **tees live events to a
stream listener if one is attached** — so a stalled or absent stream reader can never deadlock
`result()`:
- `result()` = await the cached terminal (O(1) if already seen).
- `stream()` = the live listener; yields each event to the terminal.

Contract:
- Supported: `stream()` then `result()` (O(1) cached terminal); `result()` alone (discards
  intermediates); `result()` twice (idempotent, cached); **`result()` then `stream()`** — the cached
  terminal is **replayable**, so `stream()` yields just it (preserving the terminal-bearing invariant);
  intermediates, being consume-once, are not replayed.
- **At most one** live `stream()` listener per channel — a second concurrent `stream()` raises `RuntimeError`.
- Intermediate events are **consume-once** (no replay); the terminal is **replayable** (cached).

### 4.5 `client.events()` + `handle.stream()` coexistence — two outlets of one read
The firehose and a per-run stream are **two in-process outlets of the hub's single upstream read**
(§5.4) — the firehose its raw outlet, the per-run stream the hub's demux push into the handle channel
— so concurrent use never conflicts and a run's events are observable in **both** at once (intended
overlap). The firehose is **not** the union of per-run channels (on a shared inbox it surfaces ids
this client never dispatched), is **raw** (no projection), and (per §5.5) the **caller must do its own
dedup / terminal handling** — only the per-run channel is dedup'd and terminal-bearing. Crucially, the
two outlets are **decoupled in the direction that matters**: a stalled firehose reader **drops** its
own buffered events (best-effort, §5.4), it does **not** stall the hub — so an undrained `events()` can
never delay or deadlock a `result()`. The per-run path stays lossless and prompt regardless of the
firehose; the firehose pays for falling behind with its own dropped events, nobody else's.

### 4.6 Holding the handle (no reattach-by-id)
The `start()` / `execute()` handle is the **only** way to get a per-run `result()`/`stream()`, and
it works **post-completion for as long as you hold it** (it owns the channel; the buffered terminal
stays readable — §5.2). There is **no** reattach-by-`correlation_id`. Consequences:
- A `send()` (no handle) result is observed via the firehose, never retrieved by id.
- A process that drops the handle (or restarts) **cannot** recover that run's result — there is no
  durable store in v1 (§9.1).

> **Blessed pattern for stateless deployments** (web handlers, serverless — the common case where you
> can't hold a handle across a request boundary): do **not** dispatch-and-drop a handle. Instead route
> replies to a **named `inbox_topic`** and consume them with a calfkit **`@consumer` node** — the
> framework's durable, load-balanced (consumer-group) topic-consumption surface, which projects the
> reply and exposes faults via the consumer context. (A single observer client's `events()` also works
> when one tail-reader, no restart-durability, suffices.) That is the v1 answer for cross-process
> result handling; the durable by-id store (§9.1) is the future upgrade. Dropping a handle from
> `start()` mid-flight loses the result (the type system won't stop you — §5.2).

---

## 5. The hub transport (internal implementation)

### 5.1 The hub: one shared reader for per-run demux
Per-run delivery is served by **one** long-lived hub reader per client: a **FastStream handler
subscriber** registered on the inbox — a **groupless topic subscription** (`subscribe(inbox)`,
`group_id=None` → **no consumer group, no offset commits, no rebalance**; aiokafka's no-group path
auto-assigns **all** partitions at start, refreshed on repartition — **no enumeration**),
`auto_offset_reset="latest"` (tail). Its **handler is the hub** —
FastStream decodes each reply to an `Envelope` (the **decode floor runs live** on this path) and invokes
the handler, which **pushes** the event by `correlation_id` into the owning handle's channel (§5.2) and
tees it to the firehose outlets (§5.4). The key is read from the **transport** (message key/header),
never from the body. An event whose `correlation_id` has no registered handle is handled by
`x-calf-kind`: a **return** is dropped
with a WARNING (a lost return is benign — firehose-recoverable), while a **fault** is **ERROR-logged
with the full `ErrorReport`** (the receive-side structured-log floor; glossary *Fault: never silently
dropped*, aligning with fault-rail §11). Either way the firehose also surfaces it (best-effort, subject
to drop — the ERROR-log floor, not the firehose, is the actual never-dropped guarantee) — the expected
shared-inbox / `send()`-reply case, where the caller demuxes by `correlation_id`. (On a busy *shared*
inbox the fault floor ERROR-logs foreign faults too; that volume is the accepted cost of "never
silently dropped" — the hub keeps zero `send()` state, so it cannot tell a foreign fault from its own
`send()`'s fault.) The handler does **non-blocking** pushes (per-run channel + firehose append), so it
returns immediately and FastStream's consume loop keeps draining — it cannot pull-backpressure per run
(§5.4). The subscriber is **registered at `connect()`** and **started by the first `broker.start()`**
(the client's first dispatch, or a co-located `Worker`'s `app.start()`); because the broker starts
before the first publish, the consumer positions at the tail within the reply round-trip, so a reply can
never precede it (the M4-class race cannot bite — §2.7). No eager connect, no `seek_to_end` gate, no
separate consumer.

The subscriber uses `group_id=None` rather than a consumer group deliberately: a single groupless
all-partitions reader has **no rebalance / rejoin surface** and **no committed-offset window** to
skip (§5.7) — and no coordinator join/heartbeat at all. (It still reads **every** partition: the
no-group path auto-assigns them, §2.7 — groupless is not partition-scoped.) The firehose (`events()`) is **not** a separate consumer — it is an in-process outlet the
**handler tees into** (§5.4), so it always sees the **same** reads as the per-run demux (delivery
diverges: the firehose may drop-oldest, the per-run channel does not). That is why `events()` has no
per-call topic override (§3.2/§6). There is **no** consumer-group user at all — the entire client
transport is this one handler subscriber. (Consuming a *foreign* inbox at scale is a `@consumer` node's
job — the framework's durable, load-balanced consumption surface — never a client method; see §9.1.)

(The reply subscriber is brought up by the first `broker.start()` and feeds both outlets — the per-run
demux and the firehose — for the client's whole life; a pure observer brings the broker up via its first
`events()`. `broker.stop()`/`aclose()` tears it down for free. One reader per client, period.)

**Handler robustness.** The handler's pushes are **await-free** (a synchronous channel push + a
`deque.append` tee), which is what makes the firehose-outlet set safe to mutate as `events()` streams
open/close (no iterate-while-await) and keeps the handler non-blocking. Two failure modes to name:
(a) a **malformed-but-decodable terminal** (e.g. a `kind=return` reply with no slot, or a kind↔slot
mismatch) is treated as a **delivery anomaly** — the handler fails that run's channel with a
`calf.delivery.*` fault rather than letting an `AttributeError` escape (never a silent loss); (b) a
genuine **handler-internal bug** is caught and logged by FastStream's consume loop (ack-first, the run
loses its terminal) — that run then relies on `result(timeout=…)` to surface, since there is no
redelivery. The first is defended; the second is an accepted, logged failure mode (a bug is a bug).
(One nuance from the §5.8 floor: the broker-wide decode floor wraps the whole inbox chain and catches
`ValidationError` / `JSONDecodeError` / `UnicodeDecodeError`, so a hub-handler bug that raised one of
*those three* would be (mis)classified as `calf.delivery.undecodable` and surfaced as a `RunFailed` via
the seam — better than (b)'s lost terminal, but still a misclassification. By construction the hub
handler decodes nothing and is await-free classify-and-push only, so it raises none of the three; this
is a stated invariant, not a relied-upon accident.)

### 5.2 Per-run channels — handle-owned, weak-ref routing map, GC-cleaned
A "per-run channel" is an **in-memory** async queue — NOT a Kafka topic per request — **owned by the
`InvocationHandle`**. The hub's routing map holds a **weak** ref to the handle
(`WeakValueDictionary[correlation_id, InvocationHandle]`): the map never keeps a handle alive, and an
entry auto-vanishes the moment its handle is collected. So **the channel lives exactly as long as the
caller holds the handle.** Lifecycle:
- **`start()` ordering is mandatory and race-free:** in a **single synchronous step before any
  `await`**, mint the `correlation_id`, reject a duplicate of a *currently-registered* id
  (`ValueError`), create the handle (with its channel), and register the weak `correlation_id →
  handle`; **then** publish the call. The reply is causally *after* the publish, and the handle is
  held strongly by `start()`'s frame throughout (then by the caller's binding).
- The hub derefs the weak ref and **pushes** the run's events into the handle's channel
  (`result()`/`stream()` read it). If the handle has been dropped, the deref yields `None` → the
  no-handle path (§5.1: a return drops with WARNING, a fault ERROR-floors).
- **Terminal dedup is the channel closing once.** The first terminal closes the channel; a duplicate
  redelivery *while the caller still holds the handle* derefs the live handle and pushes into the
  **closed** channel, which drops it (benign, no warning — the custom channel's post-close `put` is a
  no-op, §5.3). After the handle is dropped, a late duplicate derefs to `None` → the no-handle path
  (§5.1). (The map entry is **not** explicitly removed at the terminal — it vanishes only on handle GC.)

> **The terminal is buffered-and-readable only for a handle the caller *retains*.** A `start()`
> whose handle is immediately discarded (`await agent.start(...)` as a bare statement) behaves like
> `send()` — the handle GCs on return, so a fast terminal **either** derefs to `None` **or** lands in
> the unread, soon-collected channel and is dropped (both benign), and the result is observable only on
> the firehose. This is by design (no delivery guarantee to a dropped handle); bind the handle
> for the run's lifetime (§4.6), or use `send()`/`execute()`.

Consequences: **memory is bounded entirely by the handles the caller holds.** Abandoned runs —
finished *or* never-terminating — garbage-collect on their own (the weak map imposes no retention);
a `send()` registers no handle, so it never creates a per-run channel (its reply just lands on the
inbox for the firehose). There is **no eviction timer and no `reply_ttl`** — per-call patience is
`result(timeout=…)` (§4.3).

> **Implementation note (required, not optional):** the handle↔channel relationship MUST be
> **acyclic** — the channel is a plain queue owned by the handle, the hub-pumped driver state lives on
> the handle, and nothing strong-refs the handle back. This is what makes "abandoned runs self-GC /
> memory bounded by held handles" true: a back-ref cycle would retain a dropped, never-terminating run
> until the next cyclic-GC pass, breaking that bound. Acyclic ⇒ CPython refcounting reclaims a dropped
> handle the instant the caller releases it.
>
> **And the driver is advanced by *synchronous* hub calls — there is NO per-run `asyncio` task.** A
> task bound to a handle method roots the handle in the event loop (event loop → Task → bound method →
> handle) for the run's life, silently defeating the weak map and breaking the self-GC bound — so never
> spawn one. The hub pushes into the channel synchronously; the handle's driver is passive state the
> reader pulls from.

### 5.3 The per-run channel: lossless, sized by the run (handle-owned)
The per-run channel is the **critical path** — it is **lossless and never drops** (distinct from the
firehose outlet, which is bounded best-effort, §5.4). It yields many events, closed at the terminal —
`result()` reads to the terminal, `stream()` yields every event; one channel serves both verbs (§4.4).
It is a **custom lightweight queue with a `_closed` flag**: a `put` after close is a **benign no-op**
(exactly what makes §5.2's terminal-dedup "a duplicate pushes into the closed channel and is dropped"
true), and the terminal is **cached** on the channel so it stays replayable (§4.4) after close. It is
deliberately **not** a stock `asyncio.Queue` — that has no `close()`, and 3.13's `Queue.shutdown()`
*raises* on a post-close `put` rather than dropping. **Sizing:** a single run emits a small, finite
number of (coarse, whole-turn) events, so the channel never grows large in practice — its size is the
run's own event count, not a drop policy, and the synchronous hub `put` into it never blocks. (A
high-volume per-run stream — token deltas — would need a real per-run overflow policy, but that is out
of scope, §9.2.) The firehose outlet is the *only* place a drop policy applies (§5.4).

### 5.4 One reader, two outlets (no separate firehose consumer)
There is **one** physical consumer per client — the hub's handler subscriber (§5.1). The **handler**
tee's each decoded reply, in-process, to two outlets, and **neither blocks the handler** (so it returns
immediately and FastStream's consume loop keeps draining):
- **The per-run demux outlet** (the hub proper): routes each event by `correlation_id` into the owning
  handle's **lossless** channel (sized by the run, §5.3) via a synchronous, non-blocking `put`. A slow
  `result()`/`stream()` reader cannot stall the read — the terminal is buffered and the driver is
  hub-pumped (§4.4). The critical path never drops and never blocks.
- **The firehose outlet(s)**: each open `events()` gets its **own bounded buffer**
  (`firehose_buffer_size`, §2.1), and the tee's push into it is a **non-blocking enqueue-or-drop**: if
  the buffer is full (the reader fell behind), the **oldest** buffered event is dropped to admit the
  new one — **drop-oldest, keep newest N** (Reactor's `onBackpressureLatest` is the N=1 special case),
  so a catching-up reader sees the *latest*, not stale, events. The push therefore **never blocks the
  fetch loop**, so a stalled firehose can **never** stall
  the hub or another run.

This is a deliberate **best-effort observation tier**, the standard pattern for a fast producer feeding
a possibly-slow observer (cf. OpenTelemetry's `BatchSpanProcessor`, Reactor's `onBackpressureLatest`):
the **critical path stays lossless and non-blocking**; only the **observation firehose** is best-effort,
trading a bounded, *signaled* gap for non-blocking + bounded memory. The single non-blocking option that
is also memory-safe — a bounded *blocking* outlet would deadlock the hub; an *unbounded* one would OOM.

> **Drops are signaled, never silent** — via the **`EventStream.dropped`** cumulative counter (§3.2)
> plus a WARNING; not a stream sentinel (the `RunEvent` union stays closed, §3.3). When a firehose outlet sheds events it surfaces a **dropped
> count** (and a WARNING), so a gap is always observable and the `firehose_buffer_size` knob is
> empirically tunable from it. Worst-case firehose memory is bounded: `firehose_buffer_size × event_size
> × open observers`. For sustained throughput beyond a single reader's drain rate, use a **`@consumer`
> node** (its own consumer group, fully decoupled, durable), not a larger buffer — an oversized buffer
> only delays the drop and adds latency (bufferbloat). The design buys **simplicity** (one consumer, no
> fetch amplification) and a **lossless critical path**; the honest cost is a best-effort firehose.

### 5.5 Terminal-once / dedup (scoped to the per-run channel)
At-least-once delivery (§5.7) can deliver a terminal more than once. Dedup lives on **the per-run
channel**, via the close-once mechanism in §5.2: while the caller holds the handle, a duplicate
terminal pushes into the already-closed channel and is dropped; after the handle is dropped, a late
duplicate derefs to `None` → the no-handle path (§5.1: a late duplicate **return** WARNING-drops, a
late duplicate **fault** ERROR-floors the full report — never a silent drop). Scoped to the per-run
channel only:
- **The firehose is NOT dedup'd** — it is the raw outlet of the same single read (§5.4), so it may
  surface duplicate terminals (and, under redelivery, post-terminal events) for a `correlation_id`.
  Firehose consumers dedup **themselves**, and the key differs by kind:
  - **faults → dedup on `report_id`**, *not* `(correlation_id, terminal)`. A run can legitimately emit
    **multiple distinct faults** under one `correlation_id` — e.g. a fan-out abort faults the caller
    once per outstanding slot (up to N `calf.fanout.aborted`, in-node fan-out spec). Those are genuinely
    different faults with different `report_id`s; a `(correlation_id, terminal)` key would wrongly
    collapse them to one. `report_id` is the framework's own mirror-dedup key (glossary *Fault stream*:
    "Mirrors of one logical fault share its report id"), so redelivered copies of *one* fault collapse
    while N distinct faults survive.
  - **returns → `(correlation_id, terminal)` is fine** (one return per run).

### 5.6 Lifecycle
The firehose (`events()`) is an async context manager that registers a raw **outlet** on the shared
upstream reader (§5.4) on enter and unregisters + drains it on exit — it does **not** own a Kafka
consumer (there is one per client — the handler subscriber, brought up by the first `broker.start()`, §5.1/§2.7). Per-run channels are handle-owned
and GC-cleaned (§5.2) — no eviction timer. The client's single reader is closed by `aclose()` (§5.8).
The floor's undecodable-sink entry (§5.8) is registered at `connect()` and removed at `aclose()` — the
**only** two mutation points, so no handler/task races the registry mid-consume; an undecodable arriving
after `aclose()` finds no sink (the floor still synthesizes + ERROR-logs), and any in-flight run was
already resolved with `ClientClosedError`.

### 5.7 Reconnect & delivery scope
A transient broker disconnect (network blip, broker restart) is handled by **aiokafka** — it
auto-reconnects and, because the hub is **groupless** (`group_id=None`, no commits, §5.1), **resumes from
its in-memory read position**, not from a committed offset. So there is **no rebalance/rejoin surface and no
auto-commit window to skip**: an event already fetched is either already pushed into its in-memory
channel (which persists across the in-process reconnect) or re-fetched from the retained position.
At-least-once re-delivery is absorbed by the per-run dedup (§5.5).

**The one real loss path** (named, not silently assumed away): if a disconnect outlives the inbox's
**log retention at the read head**, the aged-out records are gone, and aiokafka's `OffsetOutOfRange` +
`auto_offset_reset=latest` **skips forward to the tail** — those records are silently lost. This is
inherent to tail-reading any Kafka topic (not a groupless-vs-group defect); it bounds how long the
client may be disconnected before a low-retention inbox loses replies.

**Delivery scope:** at-least-once *broker* delivery; **at-most-once *application* delivery on a
process crash** (deliberately). A crash loses the in-memory handles/channels and the hub map; the
design does not recover them. Crash-survival (and by-id result retrieval) is the deferred durable
store (§9.1).

### 5.8 Close, unknown replies, undecodable replies
- **`aclose()` with runs in-flight** resolves every pending `result()` with **`ClientClosedError`**
  (a typed signal, never a bare `CancelledError`) and stops the readers gracefully.
- **Unknown `correlation_id`:** classified by `x-calf-kind` — a **return** drops-with-WARNING (benign
  mis-address / lost reply); a **fault** is **ERROR-logged with the full `ErrorReport`** (never silently
  dropped, fault-rail §11). Both are also visible on the firehose (best-effort, subject to drop — the
  ERROR-log floor is the never-dropped guarantee, not the firehose) (the expected shared-inbox / `send()`-
  reply case — the caller demuxes by `correlation_id`).
- **Undecodable inbox reply** (wire corruption / schema drift): handled by the **broker-wide decode
  floor** (`DecodeFloorMiddleware` — the *same* shim every node subscriber runs), not a client-bespoke
  path. The body failed to decode, but the **transport `correlation_id` survives** (it's a header, not
  the body), so the floor synthesizes a `calf.delivery.undecodable` `ErrorReport` (via
  `ErrorReport.build_safe`, carrying the surviving `correlation_id` + the clamped decode error) and
  **ERROR-logs it with the full report** — the structured-log floor (glossary: *Fault stream*), which
  fires **always, handle or not**. (The floor catches **every** decode failure — a Pydantic
  `ValidationError`, a JSON-parse `JSONDecodeError`, or a `UnicodeDecodeError` — not only schema
  mismatches.) To notify an awaiting run, the floor exposes a generic **topic-keyed undecodable-sink
  seam**: the client registers its `inbox_topic → hub.fail_run` at `connect()`, so when an undecodable
  lands **on the inbox** the floor hands the *same* synthesized report to that sink, which pushes a
  **`RunFailed(report)`** into the matching run's channel (the seam runs *in* the floor, which re-raises
  before the handler — see §5.9), so
  `result()` **raises `NodeFaultError(calf.delivery.undecodable)`** rather than parking — it is a
  finished-but-broken reply, not a slow run, so it must not masquerade as a `ClientTimeoutError`, and
  (per §5.9's discriminator-before-projection) **never** as a `DeserializationError`. If no handle is
  registered (a `send()`, or a foreign id), the ERROR floor is the whole story — an undecodable body
  cannot be surfaced as a typed `RunEvent` on the firehose.

### 5.9 Fault reception — the client error rail

A node fault is wired on the *producer* side (ADR-0003/0006): it rides the per-delivery **reply slot**
as a `FaultMessage` carrying an `ErrorReport`, is stamped `x-calf-kind=fault`, escalates frame-by-frame
up the call stack, and is published to the caller's inbox **keyed by `correlation_id`** — identical to
a return. This section specifies the client's **receive arm** — classifying a `FaultMessage` and raising
`NodeFaultError`. (Implementation tracked as #250; the migration belongs in the impl plan.)

**The reply slot (the fault rail's wire model).** Every reply is `Envelope.reply: ReturnMessage |
FaultMessage | None`, a body union with a `kind` field (`ReturnMessage(kind="return")` carrying content
`parts`; `FaultMessage(kind="fault")` carrying `error: ErrorReport` — `error_type`, `message`,
`retryable`, `details`, `causes`, `frame_chain`). Three pieces of transport metadata ride alongside,
with **distinct** roles — do not conflate them:
- **`x-calf-kind` header** (`return | fault | call`) — the **classifier**. The hub branches on this (it
  survives an undecodable body, where the body `kind` field can't be read); the body `kind` is just the
  post-decode deserialization discriminator, which agrees by construction.
- **`correlation_id` header** — the **demux key**, ties the reply to its run (survives an undecodable
  body, §5.8).
- **`x-calf-error-type` header** — **ops-only** (`_protocol.py`: "lets ops filter faults at the broker
  without deserializing the body"). The receive arm does **not** read it — it reads `error_type` off the
  decoded `report`. It is neither classification nor correlation.

**Reception — the outcome discriminator (at the hub).** When the hub reads a reply for a run, it
classifies on the **`x-calf-kind` header** (not the body field — the header is the framework's
classifier and survives an undecodable body) and pushes the matching terminal into the run's channel:
- `return` → **`RunCompleted`** carrying the reply's `parts`.
- `fault` → **`RunFailed`** carrying the reply's `ErrorReport` (verbatim — not flattened).
- undecodable body → the **broker-wide decode floor's** `calf.delivery.undecodable` `ErrorReport`
  (not a client-bespoke synthesis — the same `DecodeFloorMiddleware` every node runs), surfaced as a
  `RunFailed` routed by the surviving transport `correlation_id` (§5.8). The floor's ERROR log fires
  regardless. **Mechanism on the handler path:** the floor catches the decode failure
  (`ValidationError` / `JSONDecodeError` / `UnicodeDecodeError`) and **re-raises before the handler
  runs**, so rather than coupling to the hub it exposes a **topic-keyed undecodable-sink seam**: the
  client registers `inbox_topic → hub.fail_run`, and the floor — having synthesized the report **once**
  — hands that report to the sink registered for the delivery's topic, then re-raises. The push is thus
  **scoped to the client inbox by the topic key** (a node-hop undecodable on a node topic has no sink,
  so it can never reach the hub even though it carries the run's `correlation_id`), the floor stays
  generic (it knows a topic→sink map, never the hub), and the floor's *synthesis* stays the single
  canonical implementation — only the seam call is added. (A header that says `return`/`fault`
  but whose body won't decode to that shape is a producer contract violation — it floors here, same
  path.)
- **unknown / missing kind → log ERROR and drop** (never resolve a run on an unclassifiable reply).
  This is **not** the framework's "a missing header reads as `call`" norm — that norm is for *ingress*
  (a fresh-call inbox, the Private input topic); the client inbox is a **reply** channel, where a
  missing/`call`/unknown kind is a mis-addressed delivery, not work to run.

The discriminator runs **before projection**, so a fault is never fed to the output projector and can
never surface as a `DeserializationError`.

**`result()` maps the terminal:**
- `RunFailed(report)` → **`raise NodeFaultError(report)`** — the receive arm wraps the `ErrorReport`
  *verbatim* (the already-built `NodeFaultError(report)` constructor; no synthesized
  `message`/`retryable`/`details` — those are mint-only). The caller branches on the slotted report.
- `RunCompleted(parts)` → project to `output_type` → `InvocationResult` (or `DeserializationError`
  only on a *present-but-mismatched* part).

**The firehose** surfaces a fault as a raw `RunFailed(report)` event; the caller maps/branches itself
(the firehose is not error-mapped — §4.5).

**Constraints the receive arm honors** (verified against the fault rail):
- **Branch with `report.find(FaultTypes.X)` / `.walk()`, never `==`** — faults compose into **groups**
  (a fan-out batch's unhandled sibling faults become a `calf.fault_group` carrying `causes`); `walk()`
  is cycle-guarded.
- `error_type` is an **open string code**, not a closed enum — tolerate unknown codes; a *received*
  report may even carry an empty `error_type`.
- The client may receive a **stripped** report (an oversized fault is stripped to identity-only at the
  producer), so make no assumption that `causes`/`details`/`frame_chain` are present.
- `frame_chain` is **topology-only** (frame ids + topics, no user payloads) — a leak-free traceback.
- **`calf.retry` / recoverable errors are NOT faults** — a `ModelRetry` / MCP `isError` is rendered to
  ordinary `return` content (a `TextPart` with a `calf.retry` marker) and honored by the *agent's*
  materializer; it arrives at the client as a normal `return`, so the receive arm does **not** see it.

---

## 6. `inbox_topic`

The named topic the client routes its dispatches' callbacks to **and** reads from — set **once, at
`connect()`**. Default `None` → ephemeral per-client inbox; set → named, durable, shareable so a
separate process can consume it (e.g. a `@consumer` node). It is the client's **single** inbox: the hub and the firehose
(`events()`) both read it through one upstream consumer (§5.1/§5.4), which is exactly why there is
**no per-call `events(inbox_topic=…)` override** — the firehose can never drift off the hub's topic.

**Naming:** `inbox_topic`, not `reply_topic` or `topic`: `topic` is already the **outbound**
destination everywhere else (`agent(topic=…)`); `reply` under-describes a channel that streams
lifecycle events; `inbox` is the accurate, content-neutral inbound noun.

**Observing an arbitrary topic.** A pure *observer* (a client that never dispatches) sets that topic
as its **own** `connect(inbox_topic=…)` and reads it with `events()` — one obvious place to point an
inbox, and the observer's hub/firehose stay in sync by construction. The observer reads **all**
partitions via its single-reader firehose. (Consequence: one client instance observes exactly one
inbox; to watch your own inbox *and* a foreign topic, connect two clients.)

---

## 7. Naming decisions (capsule)

| Concept | Chosen | Rejected (and why) |
|---|---|---|
| Component identity | `name` | `node_id` |
| Inbound channel | `inbox_topic` | `topic` (outbound clash); `reply_topic` (narrow once streaming) |
| Fire token | `Dispatch` (from `send`) | a bare `str` / an `InvocationHandle` (hides the no-result asymmetry) |
| Client-gave-up / closed | `ClientTimeoutError` / `ClientClosedError` | bare `asyncio.TimeoutError` / `CancelledError` |
| Ambient deps | `deps_factory` | `run_context_factory` |

---

## 8. Usage patterns (recipes — decided surface only)

```python
# Request-reply, typed + multi-turn. Pass timeout= to bound client patience for request/reply
# (no default — a durable run may legitimately pause; see §4.3). Omit it only when you mean "wait forever".
r1 = await client.agent("summarizer", output_type=Summary).execute("…", author="ryan", timeout=30)
r2 = await client.agent("summarizer", output_type=Summary).execute("…", message_history=r1.message_history, timeout=30)

# Dispatch without awaiting (reply lands on the inbox; observe via events())
cid = (await client.agent("notifier").send("…")).correlation_id        # send() → Dispatch

# Stream one run (per-run), then get the typed terminal (O(1) cached) — HOLD the handle
h = await client.agent("support").start("…")
async for ev in h.stream(): ...
res = await h.result()

# Handle + bounded client patience
res = await h.result(timeout=30)                                       # ClientTimeoutError if exceeded

# Firehose over this client's own inbox; caller demuxes + dedups
async with client.events(terminal_only=True) as stream:
    async for ev in stream: ...

# Background firehose catches a send()'s reply (subscribe BEFORE you send).
# Best-effort: an actively-drained stream won't drop, but the firehose is not a delivery
# guarantee (§3.2) — if you must not miss the result, use start()/execute() or a @consumer node.
async with client.events(terminal_only=True) as stream:
    cid = (await client.agent("x").send("…")).correlation_id
    async for ev in stream:
        if ev.correlation_id == cid: ...
```

---

## 9. Boundary: future features and out of scope

### 9.1 Future features to consider (noted, not designed)
- **Worker-side intermediate event emission.** A run emitting progress events per `correlation_id`
  (assistant turns, tool calls, handoffs, approvals), which `handle.stream()` / `events()` would
  surface. Requires worker routing-to-inbox + `correlation_id`-keying on that rail (§3.3) and a
  per-run channel overflow policy (§5.3). Not free.
- **Per-observer firehose buffer override (`events(buffer_size=…)`).** A per-call override of the
  client-level `firehose_buffer_size` (§2.1), for a client running one fast and one slow observer. Cut
  from v1 for simplicity (the client-level default covers the real case). **Cheap and non-breaking** —
  the buffer is an in-process per-outlet `deque(maxlen=…)`, so heterogeneous sizes need no separate
  consumer (unlike `inbox_topic`); add it when a concrete need appears.
- **Direct tool invocation (`ToolGateway`).** A `client.tool(name)` gateway invoking a tool node by
  name with the tool's input as keyword arguments. **Deferred from v1 — not a priority.** Shape only;
  undecided (do not infer): the verb set; how framework controls (`output_type`, `timeout`,
  `correlation_id`) bind alongside tool kwargs; how a bare tool name resolves to its hosting node
  (depends on the capability/control plane); reattach/firehose support (hinges on the `correlation_id`
  binding). To be specced in a later pass.
- **A run control plane for `status()` / `cancel()`.** Server-side run-state tracking + durable cancel.
- **A durable result store + by-id retrieval.** A compacted view keyed by `correlation_id` that would
  enable cross-process / post-completion result retrieval (and would be the home for a re-introduced
  `handle_for`). The v1 design is hold-the-handle only (§4.6); this store is the deferred upgrade.
- **A caller-side load-balanced drain (`drain()`).** A consumer-group worker-pool drain of a named
  inbox, *on the client surface*. Cut from v1: the `@consumer` node already covers durable,
  load-balanced inbox consumption (the canonical surface for it), and a caller-side drainer would be
  the *only* consumer-group surface on the client — large transport weight for a node-free convenience.
  Revisit alongside the durable result store if a caller-side drain without authoring a node proves
  needed.

### 9.2 Out of scope (not part of the design)
- **`handle_for` reattach-by-id** — cut from v1 (narrow use case; depends on the durable store, §9.1).
- **`publish()` raw write-side escape hatch** — not supported (arbitrary-topic *observation* is the §6
  observer pattern).
- **`route` / `body` on the agent surface** — node-route targeting + typed run payload belong to a
  lower-level node-call surface, not the agent gateway.
- **Dedicated per-stream consumer** (a Kafka consumer per `stream()`/`result()`) — the transport is the
  hub.
- **True fire-and-forget on the client surface** (`send()` producing no reply at all). `send()` always
  routes its reply to the inbox. The wire-level node concept is unaffected. (Supersedes the ADR-0005
  bar on `send`→own-inbox — see [ADR-0022](../adr/0022-send-replies-to-callers-inbox-via-firehose.md).)
- **Token-level / high-volume per-run streaming** — coarse whole-turn grain only.
- **`reply_ttl` / an eviction timer** — removed; the weak-ref hub map (§5.2) bounds memory by GC, and
  `result(timeout=…)` covers per-call patience, so an eviction TTL is redundant.

### 9.3 Open / to-confirm
- **Settled v1 surface (§2–§8): no open design calls** — the deep-review findings are resolved.
- **Not final, named here rather than specified** (per the line-7 rule): the **tool gateway** (§9.1 —
  undesigned, deferred from v1). (The **fault-reception arm** is fully specified in §5.9 — it is *not* in
  this "named-not-specified" bucket; implementation is tracked as #250.)

---

## 10. References
- The rich result the design uses: `calfkit/models/node_result.py`
- The typed fault value: `calfkit/exceptions.py`, `calfkit/models/error_report.py`
- The reply slot + fault rail (§5.9): `calfkit/models/reply.py`, `docs/designs/fault-rail-and-policy-seams-spec.md`, ADR-0003 / ADR-0006. Client reception tracked as **#250**.
- `send`→inbox decision: [ADR-0022](../adr/0022-send-replies-to-callers-inbox-via-firehose.md); name→topic: ADR-0017; identity term: ADR-0009 / ADR-0020.
- Single-reader-tee transport (§5), `drain()` dropped, best-effort bounded-drop firehose: [ADR-0023](../adr/0023-client-single-inbox-reader-tee.md).
