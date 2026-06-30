# Caller-Side Mesh View — Design Spec

**Status:** design spec of the *settled* design — the end-product surface AND internals, on engineering
merits. Every decision is **locked** and written as contract; §9.3 logs the resolved decisions, §9.2 the
deferred item. Do not infer scope beyond what is written.

Scope is the **caller boundary**: a non-agent process (a gateway, a Discord bridge, a CLI, a dashboard)
that connects to the mesh and wants a *live look at which agents and tools are currently online* —
read-only. Not the authoring surface, not the agent runtime, not agent-to-agent discovery.

**Origin.** [Issue #301](https://github.com/calf-ai/calfkit-sdk/issues/301) — a supported caller-side
read of the live **agent** directory; broadened here to **tools** (same substrate).

**ADRs.** [ADR-0028](../adr/0028-caller-side-mesh-view-over-control-plane.md) (caller-side projected read
surface) · [ADR-0029](../adr/0029-caller-side-mesh-view-naive-open.md) (directory-topic
existence is an operational precondition; graceful-empty tolerance considered + rejected). Vocabulary:
[CONTEXT.md](../../CONTEXT.md) — **Mesh (caller-side access)**, **Agent Info**, **Tool Info**.

---

## 1. Overview

A connected `Client` exposes a zero-I/O **`client.mesh`** accessor — a cached singleton that owns **one
materialized `ControlPlaneView` per kind** (agents, tools) for the client's lifetime. The public surface
is two read methods returning a **name-keyed `Mapping`**:

```python
client = Client.connect("localhost:9092")        # mesh_config=MeshViewConfig(...) optional

agents = await client.mesh.get_agents()          # Mapping[str, AgentInfo], keyed by agent name
agents["billing"]                                # AgentInfo
"billing" in agents                              # idempotent "is it already up?" across hosts
for name, info in agents.items(): ...

tools = await client.mesh.get_tools()            # Mapping[str, ToolInfo]
for name, info in tools.items():
    match info:                                  # closed, match-friendly union
        case ToolNodeInfo(): ...                 # one function tool node
        case ToolboxInfo():  ...                 # one MCP toolbox (many tools)
```

The first `get_*` for a kind opens one Kafka consumer and catches it up; **every call after reads the
live, continuously-updated cached view's snapshot** — O(1), no re-open. The mesh opens **naively**
(`ensure_topic=False`): the directory topic is an operational precondition created by the agents' workers
or ops — the observer never creates it and a genuinely-missing topic fails loud (§6.3). The views are
torn down at `client.aclose()`.

## 2. Motivation

Agents advertise on `calf.agents` (every agent, no opt-out) and tool advertisers on `calf.capabilities`;
agents resolve peers/tools internally via a per-reader Control Plane View. There is **no supported
caller-side way** for a non-agent process to observe the same live directory — the record types
(`AgentCard`, `CapabilityRecord`) and topic names are internal/unexported. calfcord needs a live roster
(which agents are online) to resolve `@mentions`, attribute persona messages, render status, and make
`agent start` idempotent across hosts; a sanctioned reader lets it delete its bespoke presence plane.

## 3. Goals / Non-goals

**Goals**

- A supported, public, caller-side **live** read of which **agents** and **tools** are online — name,
  description, liveness — as a name-keyed `Mapping`, importing no internal records and hard-coding no
  topic names.
- Reflect nodes coming and going (a live view, not a one-shot at connect), with advisory staleness.
- **Efficient repeated reads:** one consumer + one catch-up per kind for the client's lifetime; O(1)
  reads.
- Reuse the control-plane substrate verbatim — **no new wire surface**, no parallel plane, **no changes
  to any dependency**.

**Non-goals**

- Agent-to-agent messaging/handoff; a write/registration surface; an ops fleet roster / `calfkit nodes`
  CLI; a change-event stream (a read returns a current snapshot, not deltas — §9.1); a held live-view
  handle (the surface is `get_*` → `Mapping`, not a `MeshView` object); any change to the worker-side
  view, its boot gate, or the wire records.
- **Graceful-empty cold-start tolerance** — explicitly rejected (ADR-0029); the directory topic is a
  documented operational precondition (§6.3).

## 4. Vocabulary

Defined in [CONTEXT.md](../../CONTEXT.md).

| Term | Meaning |
|---|---|
| **Mesh (caller-side access)** | `client.mesh` — the caller's read-only window onto the live control plane. A cached singleton owning one `ControlPlaneView` per kind, read by `get_agents()` / `get_tools()`. |
| **Control Plane View** | The internal per-reader materialization the Mesh caches (raw records). *Substrate — unchanged.* |
| **Agent Info / Tool Info** | The public DTOs the Mapping holds (`AgentInfo`; `ToolInfo = ToolNodeInfo \| ToolboxInfo`). Decoupled from the wire records. |
| **Advisory staleness** | The reader-side liveness rule (no recent heartbeat ⇒ offline; no TTL). Presence in the Mapping ⇔ online. |

## 5. The developer surface

### 5.1 `client.mesh` — the accessor

```python
class Client:
    @property
    def mesh(self) -> Mesh: ...   # cached singleton; zero I/O until a get_* is awaited

class Mesh:
    async def get_agents(self) -> Mapping[str, AgentInfo]: ...
    async def get_tools(self)  -> Mapping[str, ToolInfo]:  ...
    # aclose() is internal — called by Client.aclose()
```

`client.mesh` returns a **cached `Mesh` singleton** (one per client) and does no I/O. The `Mesh` lazily
opens and caches **one `ControlPlaneView` per kind** on first `get_*`, holds them as instance state, and
tears them down at `client.aclose()` (§6.2). A view's consumer is **independent of the client's inbox /
Hub** (§7.3): a pure-observer client reads the mesh without bringing up its reply path. Opening requires
the client's bootstrap address; a client built directly (no `connect()`, so no `server_urls`) raises a
clear error on first `get_*`.

### 5.2 `get_agents()` / `get_tools()`

Each `await`-ed call **lazily opens + catches up the cached per-kind view** (agents → `calf.agents` /
`AgentCard`; tools → `calf.capabilities` / `CapabilityRecord`) and returns its live snapshot projected to
a **fresh, read-only `Mapping`** keyed by node name:

- **Indexing / membership / iteration:** `m["billing"]` (raises `KeyError` if offline), `"billing" in m`,
  `m.get("billing")` (→ `None` if offline), `len(m)`, `for name, info in m.items()`.
- **Liveness:** presence in the Mapping ⇔ online (advisory staleness, §6.1); `info.last_seen` is the
  freshness. Offline is modeled as absence, never an exception.
- **Health is by behaviour, not a status field** (§6.4): `get_*` raises a typed
  **`MeshUnavailableError`** carrying a **`reason`** (`"establishing"` / `"reader_dead"` / `"open_failed"`)
  when the view is not usable, and returns the `Mapping` otherwise. The `reason` lets a poller route
  (retry vs alert) without string-matching; it never returns a silently-partial roster. (A misconfigured
  client — `server_urls is None` — is a synchronous `ValueError`, not a retriable `MeshUnavailableError`.)

A read is point-in-time: each call re-projects the live view's current snapshot, so repeated calls
reflect nodes coming and going. The Mapping is immutable (a fresh read-only view each call).

### 5.3 The public DTOs

Public, immutable projections of the wire records; they carry **no** `schema_version` / `node_kind` /
worker-instance stamps. **Frozen dataclasses** (matching `RunCompleted` / `RunFailed`), with the
caller-friendly field name **`last_seen`** (not the wire's `last_heartbeat_at`). Callers distinguish the
`ToolInfo` union members by **type** (`match`/`isinstance`), not a discriminator field — the `RunEvent`
precedent.

```python
@dataclass(frozen=True)
class AgentInfo:
    name: str                  # addressable identity (the control-plane wire key)
    description: str | None     # advertised public blurb (may be absent)
    last_seen: datetime         # aware UTC — last heartbeat; the liveness basis

ToolInfo = ToolNodeInfo | ToolboxInfo   # closed match-friendly union (mirrors RunEvent)

@dataclass(frozen=True)
class ToolNodeInfo:            # one function tool node — its single tool, inline
    name: str                  # node name == tool name == capability key (ADR-0013)
    description: str | None
    parameters_schema: dict[str, Any]
    last_seen: datetime

@dataclass(frozen=True)
class ToolboxInfo:            # one MCP toolbox — many tools (no toolbox-level description; the wire carries none)
    name: str
    tools: tuple[ToolSpec, ...]
    last_seen: datetime

@dataclass(frozen=True)
class ToolSpec:                # one tool advertised within a toolbox
    name: str                  # BARE name (e.g. `search`), not the LLM-facing `<toolbox>__search`
    description: str | None
    parameters_schema: dict[str, Any]
```

A function tool node advertises **exactly one** tool, so it maps to a flat `ToolNodeInfo`; only a toolbox
carries a list. `ToolboxInfo` has **no top-level `description`** (the wire `CapabilityRecord` carries
none — descriptions live per `ToolSpec`; documented asymmetry).

### 5.4 Configuration — `MeshViewConfig`

A reader-scoped tuning model — *not* the worker's `ControlPlaneConfig` (which carries the publisher-only
`heartbeat_interval`). **Set once** via `Client.connect(mesh_config=…)`; omitting it uses defaults.

```python
class MeshViewConfig(BaseModel):                 # frozen, extra="forbid"
    stale_after: PositiveFiniteFloat | None = None     # reader staleness override; None → derive from record cadence
    catchup_timeout: PositiveFiniteFloat = 30.0        # the open-time catch-up gate (§6.2)
    reader_tuning: KTableReaderTuning = Field(default_factory=KTableReaderTuning)   # reader cadence
```

No `bootstrap_servers` (the client supplies it), no `heartbeat_interval` (publisher-only), no
`ensure_topic` (the observer opens naively, `ensure_topic=False`, §6.3), no `probe_interval` (no probe).

## 6. Semantics

### 6.1 Liveness — advisory staleness, presence ⇔ online

The Mapping is the Control Plane View's collapsed, stale-filtered read, projected: a node appears **iff**
it has a supported, non-stale instance. Presence **is** "online"; `last_seen` is freshness. No `online`
flag, no TTL. Offline is absence.

### 6.2 Lifecycle — lazy open, cache, deterministic teardown

The `Mesh` owns one `ControlPlaneView` per kind, opened **lazily on first `get_*`** and cached:

1. **Lazy, concurrency-safe, cancel-safe open — per-kind single-flight.** Each kind caches its open
   `asyncio.Task`; concurrent first callers of one kind `await asyncio.shield(task)` on the *same* task,
   and the two kinds open **in parallel** (the lock guards only the cache dict + `_closed`, never an
   `await start()`). The task runs `_open` (§7.1) — the **naive** `ControlPlaneView.open(...,
   ensure_topic=False)` + teardown-guarded `start()` (which **is** the catch-up gate).
   A **failed** open is evicted by the task's own `done` callback (not a waiter's `except`) so it is
   retried next call, never on a waiter's cancellation (§7.2). `shield` keeps a waiter's `wait_for`-style
   cancel from killing the shared open.
2. **Reads are O(1) snapshots:** `get_*` checks health (§6.4) then projects the cached view's current
   `snapshot()` into a fresh read-only `Mapping`.
3. **Teardown** at `client.aclose()`: under the lock, flip `_closed` + drain the cache; then cancel each
   open task and `stop()` the resulting view (best-effort — per-view `stop()` errors are logged, not
   aggregated). An in-flight `get_*` whose shared open task `aclose()` *cancels* surfaces
   `ClientClosedError`; if the open instead *completed* in the race window, `get_*` returns a final
   point-in-time snapshot (the cell is read as a local, never re-indexed) — both are acceptable. A `get_*`
   starting after close raises `ClientClosedError`. `client.aclose()` tears the mesh down before the
   broker, and **still closes the broker even if a view's `stop()` raises**.

### 6.3 Cold start — naive observer; the topic is an operational precondition (ADR-0029)

The directory topic's existence is an **operational precondition**, and the observer client **never
participates in creating it**. The Mesh opens each view **naively — `ensure_topic=False`, always** — and
touches no deployment config: it does not consult `client._provisioning` (which governs the client's own
inbox/data topics, issue #180, *not* the cluster control plane), and **issues no admin create/ensure**.
The control-plane topics (`calf.agents` / `calf.capabilities`) are created by the **agents' workers**
(which `ensure` when provisioning is enabled, else the broker auto-creates on the first advert) or by
**ops** — never by an admin call the observer makes. This is the *document-don't-police-deployment*
posture: a read-only observer must not actively create cluster control-plane topology.

- **Topic present** (an agent/tool has come online, or ops provisioned it): the view opens and reads it.
- **Genuinely missing, on a no-auto-create broker** (which production control planes **must** be — broker
  auto-create gives the wrong `cleanup.policy=delete`): `view.start()` **fails loud**; `get_*` raises
  `MeshUnavailableError(reason="open_failed")` with an actionable message ("could not open the agents
  directory — the topic isn't present yet; it is created when an agent/tool comes online, or provision it
  via ops"; cause attached), **without** classifying the error (no string-matching).
- **Best-effort naive — auto-create broker** (e.g. dev/CI Redpanda `dev-container`): the line we draw is
  that the **client sets nothing on the deployment** (no admin create/ensure, `ensure_topic=False`). What
  the broker does on a plain subscribe is the broker's own policy — its auto-create triggers on the
  consumer's metadata request, so a missing topic yields an **empty** roster there (the *same* behaviour
  the worker's own consumer has, not something the observer asked for). The guarantee is "no explicit
  client-side deployment action," not "provably side-effect-free against any broker config" — the latter is
  the broker's to enforce (no-auto-create, which production control planes must use).

No bespoke caller-side cold-start mechanism, no probe, no supervisor, **no dependency change** — and,
unlike the worker, **no `ensure` either** (the worker is part of the deployment; an observer is not).
Graceful-empty tolerance — serving an empty view on a missing topic — was considered and rejected as the
dominant complexity source for an edge the agents'/ops' topic creation already covers (ADR-0029). A
**transient broker outage** after a view is established does not raise: the underlying ktables reader
serves the frozen last-applied state and resumes; advisory staleness then ages nodes out (the Mapping
empties). Only a **non-retriable reader death** flips the view to failed — surfaced by the next `get_*`
raising `reason="reader_dead"` (§6.4).

### 6.4 Health — a typed raise carrying a `reason`, not a status field

`get_*` returns a `Mapping` **or raises a typed `MeshUnavailableError`** — there is no public
`status`/`failure` field. The error carries a **`reason`** discriminator (mirroring
`NodeFaultError.report.retryable`, the codebase's discriminate-by-field precedent) so a poller routes
without string-matching the message. After ensuring the cached view is open, `get_*` checks it:

- **Usable** (caught up, reader alive): return the projected `Mapping` — the current online set,
  **possibly empty** (no nodes online; the topic exists). Empty is a normal value, not an error.
- **`reason="establishing"`** — `not view.is_caught_up` (catch-up timed out at open / still latching;
  `view.failure is None`, so **no cause**): the cached view **self-heals** — its background reader keeps
  going and latches, so a retry on the *same* cached view succeeds. Retry (with backoff).
- **`reason="reader_dead"`** — `view.failure is not None` (the reader died; the cause is attached). ktables
  only sets this on a **non-retriable** error (e.g. authorization) — a transient broker outage does *not*
  kill the reader (§6.3) — so this is **terminal for the view's lifetime**; the cell is **not** auto-evicted
  (re-opening with the same config would re-fail). A poller should **alert**; recovery requires a **fresh
  `Client`** once the underlying cause is fixed (the same dead view cannot self-recover, and is deliberately
  not re-opened — that is what stops the spin). The `reason` is the signal to stop retrying.
- **`reason="open_failed"`** — the open itself raised (missing topic / unreachable broker; cause attached).
  The failed open is **not cached** (the cell is evicted, §7.2), so a retry **re-opens**. Could be transient
  (broker was momentarily down) or a config fault (topic not provisioned, §6.3) — calfkit cannot tell them
  apart without string-matching, so the cause/message carries the detail; retry with backoff.

It never hands back a silently-partial roster (the wrong-roster footgun), and **`view.is_caught_up` is a
sticky one-time latch** (False→True once, never cleared — it does *not* AND with `failure`), so `not
is_caught_up` means *cold-start only* — once a view has ever caught up, a transient outage keeps it `live`
serving frozen state (§6.3), never re-raising mid-operation. **Load-bearing check order:** `get_*` tests
`failure` **before** `is_caught_up` — a reader that dies *after* catching up has `is_caught_up=True` **and**
`failure` set, so checking `is_caught_up` first would return a stale snapshot from a dead view instead of
raising `reader_dead`; `failure`-first is the invariant that makes the eviction asymmetry (§7.2) safe. A programmatic health/staleness accessor (telling healthy from establishing
without catching the exception) is a deferred future add (§9.1). The `server_urls is None` *programming*
error is a plain **`ValueError`** raised synchronously (not a `MeshUnavailableError`) — you never retry a
misconfigured client.

**Two kinds of "nothing".** An empty `Mapping` (a *value*) means "the directory exists, no nodes online";
a `reason="open_failed"` raise means "the directory isn't there yet" — a developer must not conflate them.

**The canonical poll loop** (the `reason` is the routing key; `MeshUnavailableError` is a deferred-tool /
docstring-shipped recipe so callers don't re-derive it; treat `reason` as **extensible** — keep a `case _:`
defaulting to retry-with-backoff):

```python
backoff = 1.0
while True:
    try:
        agents = await client.mesh.get_agents()      # Mapping[str, AgentInfo]
    except MeshUnavailableError as e:
        match e.reason:
            case "reader_dead":  alert(e); return     # terminal — fix the cause, make a fresh Client
            case "establishing": await asyncio.sleep(0.5)
            case _:              await asyncio.sleep(min(backoff, 30)); backoff *= 2   # open_failed + forward-compat
        continue
    except ClientClosedError:  return                 # shutdown
    # ValueError (client built without connect()) intentionally propagates — it's a bug.
    backoff = 1.0; render(agents); await asyncio.sleep(2.0)
```

**This is deliberately NOT symmetric with the agent side.** The agent-side readers of these same views
(`peers/directory.py`, `nodes/agent.py`) *warn-and-degrade to an empty result* — reader health must never
raise into an agent's run loop. A caller polling `get_*` is the opposite case: it *can* act on an
exception, so a typed raise is the right contract. (The observer also diverges from the worker on
**cold-start**: the worker *ensures* the topic in dev/CI; the observer opens **naively** and never creates
it — §6.3.)

### 6.5 Projection rules (record → DTO)

- **Agents** (`AgentCard` → `AgentInfo`): `name` from the wire key; `description`; `last_seen` from
  `last_heartbeat_at`.
- **Tools** (`CapabilityRecord` → `ToolNodeInfo | ToolboxInfo`): dispatch on `node_kind` (inherited from
  the `ControlPlaneStamp` base) — `"tool"` → `ToolNodeInfo` (single inlined `CapabilityToolDef`);
  `"toolbox"` → `ToolboxInfo` (tool list → `tuple[ToolSpec, …]`, bare names).
- **Tolerant projection (never crash a read):** a record that fails projection → **skip**, uniformly
  absent from the Mapping (mirroring `resolve_capability`'s degrade-don't-crash). The reachable cases are
  an unknown future `node_kind`, and a `"tool"` record whose `tools` is not exactly one element. A
  *malformed `ToolSpec`* (e.g. a missing `parameters_json_schema`) is **not** a projection concern:
  `CapabilityToolDef.parameters_json_schema` is a **required** field, so such a record fails wire-decode in
  ktables and is dropped **before** projection — the projector never sees it. Skips are logged **once**
  (deduped, mirroring `_log_schema_skip`); the dedup state lives on the per-kind cache cell (§7.2), 1:1
  with the long-lived view — **not** a per-call function (which would re-log every read).
- A toolbox advertising **zero** tools projects to `ToolboxInfo(tools=())` (valid, not malformed).
- **`parameters_schema` is deep-copied into the DTO**, not aliased from the cached wire record —
  otherwise a caller mutating `info.parameters_schema` would mutate the live cached record, undercutting
  the "immutable projection" guarantee.

## 7. Internal implementation

### 7.1 The open dance (inlined in the mesh)

`Mesh._open(kind)` opens the view **naively** — `ensure_topic=False`, always — with a **teardown-guarded**
`start()`. The observer touches no deployment config (§6.3): it never creates a control-plane topic, and
it does **not** consult `client._provisioning` (which is about the client's *own* inbox/data topics, not
the cluster control plane):

```python
async def _open(self, kind) -> ControlPlaneView:                 # the server_urls-None ValueError fires earlier (§7.2)
    view = ControlPlaneView.open(bootstrap_servers=self._client.server_urls,
                                 topic=kind.topic, record_type=kind.record_type,
                                 ensure_topic=False,             # naive: the observer never creates topics
                                 catchup_timeout=self._config.catchup_timeout,
                                 stale_after=self._config.stale_after, reader_tuning=self._config.reader_tuning)
    try:
        await view.start()                                       # start() IS the catch-up gate
    except BaseException:                                        # incl. CancelledError → stop the half-built view
        with contextlib.suppress(Exception): await view.stop()  #   (no leaked consumer / reader task)
        raise                                                    # → the task fails → evicted → get_* raises reason="open_failed"
    return view
```

(The worker's view resources run a *similar* dance but with `ensure_topic = self._client._provisioning.enabled`
— it ensures in dev/CI — so they are deliberately **not** unified with this naive observer open. A future
shared helper, if any, is a tracked follow-up; the mesh inlines its own naive open now.)

A `Mesh` holds one kind-scoped `ControlPlaneView[R]` per kind (its own `GroupedKafkaTable` consumer);
record type + topic are bound internally (never exported); the projection + public DTOs sit on top. No
substrate change, no dependency change.

### 7.2 The cache (per-kind single-flight, cancel-safe)

The cache cell is the open **`Task`** per kind. The lock guards only the dict + `_closed` (never an
`await start()`), so kinds open in parallel. Three idioms make it **cancellation-safe** (a caller may
wrap `get_*` in `asyncio.wait_for`, and `aclose()` may cancel an in-flight open):

- **`await asyncio.shield(task)`** — a *waiter's* cancellation is absorbed by the shield (the awaiter's
  `await` is cancelled, the **shared open task keeps running** for other waiters). Without shield, `await
  task` makes the task the awaiter's `_fut_waiter`, so a waiter's cancel would cancel the shared open and
  poison every co-waiter — and double-open afterward.
- **Eviction on the *task's* terminal state**, via `add_done_callback` — evict iff the open finished with
  a real exception (`not cancelled() and exception() is not None`); keep on success; an
  `aclose`-cancelled task is left for `aclose` to drain. (Evicting in a waiter's `except` would drop a
  still-running task on that waiter's cancel.)
- **Teardown-guarded open** in the shared seam (§7.1) — `_open`'s `view.start()` is wrapped so a failed
  *or cancelled* `start()` stops the half-built view (no leaked consumer / reader task).

```python
@dataclass
class _Cell:                                 # the per-kind cache cell — 1:1 with the cached view
    task: asyncio.Task[ControlPlaneView]      # the open task IS the single-flight cell
    project: _Projector                       # kind-bound projector + its OWN deduped skip-log set (§6.5)

class Mesh:
    def __init__(self, client, config):
        self._client, self._config = client, config or MeshViewConfig()
        self._cells: dict[str, _Cell] = {}
        self._lock = asyncio.Lock()           # guards _cells + _closed only — NEVER held across start()
        self._closed = False

    def _make_cell(self, kind) -> _Cell:                          # caller holds self._lock
        cell = _Cell(task=asyncio.create_task(self._open(kind)), project=_projector(kind))
        cell.task.add_done_callback(lambda t, n=kind.name, c=cell: self._evict(n, c, t))
        self._cells[kind.name] = cell
        return cell

    def _evict(self, name, cell, task):                           # done-callback: drop ONLY on the open's OWN failure
        if task.cancelled() or task.exception() is None: return   # aclose-cancel or success → keep
        if self._cells.get(name) is cell: self._cells.pop(name, None)  # identity guard: never drop a retry's newer cell

    async def _cell(self, kind) -> _Cell:
        if not self._client.server_urls:                          # programming error → ValueError, SYNCHRONOUS, pre-open
            raise ValueError("client.mesh requires a connected client (Client.connect(...)).")
        async with self._lock:
            if self._closed: raise ClientClosedError(...)
            cell = self._cells.get(kind.name)
            if cell is None: cell = self._make_cell(kind)
        try:
            await asyncio.shield(cell.task)                       # shield: a waiter's cancel can't kill the shared open
        except asyncio.CancelledError:
            if self._closed: raise ClientClosedError(...) from None   # aclose cancelled us → closed contract
            raise                                                 # the waiter's OWN cancellation propagates
        except Exception as e:                                    # the open task raised (start() failed)
            raise MeshUnavailableError(f"could not open the {kind.label} directory", reason="open_failed") from e
        return cell

    async def get_agents(self) -> Mapping[str, AgentInfo]:
        cell = await self._cell(_AGENTS)
        view = cell.task.result()                                 # the resolved view — NO self._cells re-index (no aclose-race KeyError)
        if view.failure is not None:                              # §6.4 health-by-reason
            raise MeshUnavailableError("agents reader died", reason="reader_dead") from view.failure
        if not view.is_caught_up:
            raise MeshUnavailableError("agents directory still establishing", reason="establishing")
        return cell.project(view.snapshot())                      # projector + dedup live on the cell

    async def aclose(self):
        async with self._lock:
            self._closed = True
            cells = list(self._cells.values()); self._cells.clear()
        for c in cells:                                           # cancel in-flight; stop completed; best-effort (log, don't aggregate-raise)
            c.task.cancel()
            try: view = await c.task
            except (asyncio.CancelledError, Exception): continue  # in-flight (the §7.1 guard tore it down) / failed open
            try: await view.stop()
            except Exception: logger.exception("mesh view stop failed during aclose")
```

This is the canonical async lazy-singleton: `shield(cell.task)` (a waiter's cancel can't kill the shared
open), eviction on the task's **own** non-cancelled failure via the identity-guarded done-callback (so a
failed open is retried and a retry's newer cell is never wrongly dropped), the resolved view read from
`cell.task.result()` (no `_cells` re-index → no `aclose`-race `KeyError`), and the §7.1 teardown-guarded
open (no consumer leak on a failed/cancelled `start()`). `aclose` is best-effort (per-view `stop()`
errors logged, broker still closed). **Residual, upstream:** a cancel landing *inside* aiokafka's
`consumer.start()` — before ktables records `_consumer` — still leaks (a pre-existing ktables/aiokafka
gap, reachable here only via `aclose`; noted, owned upstream). (`_evict` mutates `_cells` outside the
lock; safe because it runs as a discrete event-loop step with no `await` — do not add one.)

### 7.3 No coupling to the inbox / Hub

A Mesh view uses neither the client's FastStream broker, inbox, nor Hub — only the bootstrap address. So
opening a view does not start the client's reply path, and the mesh's consumers and the client's inbox
reader are independent (the client owns both teardowns).

## 8. Testing

- **Offline projection unit** (no broker, no ktables): a dict-backed fake `GroupedTableReader` in
  `ControlPlaneView(...)` drives the projection, liveness filter, and `Mapping` indexing/membership/iteration.
  Cases: agent card; tool node (single inlined tool); toolbox (many, bare names, empty → `()`); unknown
  `node_kind` → skipped; `"tool"` with `tools ≠ 1` → skipped; deduped skip-log; **`parameters_schema`
  deep-copy** (mutating `info.parameters_schema` does not mutate the cached record).
- **Offline health unit** (`_FakeTable(failure=…, is_caught_up=…)`): `failure` set →
  `MeshUnavailableError(reason="reader_dead")`; `is_caught_up=False` →
  `MeshUnavailableError(reason="establishing")`; **self-heal** — flip `is_caught_up=True` → next `get_*`
  returns; caught-up + empty → empty `Mapping` (no raise); an open that raises →
  `MeshUnavailableError(reason="open_failed")` + the cell is evicted (retried); `server_urls is None` →
  `ValueError` synchronously (not `MeshUnavailableError`).
- **Cache/cancel-safety unit** (stub opener): first `get_*` opens once, subsequent reuse; concurrent first
  calls of one kind → exactly one open; the two kinds open concurrently (no cross-serialization); a failed
  open is not retained (retried). **Cancel-safety:** a waiter cancelled (`asyncio.wait_for`) while awaiting
  a shared open does not cancel the open, not poison a co-waiter, and not evict the still-running task (no
  double-open); a cancelled in-flight open leaves no orphaned consumer (stub asserts `stop()` ran);
  `aclose()` cancels in-flight + stops completed (per-view `stop()` error logged, not raised); in-flight
  `get_*` during `aclose()` → `ClientClosedError` **or** a final snapshot; `get_*` after `aclose()` →
  `ClientClosedError`. Lazy-import assertion: `import calfkit.client.mesh` pulls no aiokafka/ktables.
- **Kafka lane** (`@pytest.mark.kafka`): a worker advertises an agent + a function tool node + an MCP
  toolbox; `get_agents()`/`get_tools()` return the right projections; repeated `get_*` reuses one
  consumer (assert a single open). **Missing-topic → `reason="open_failed"`** runs on the **no-auto-create**
  provisioning lane (not the auto-create main lane, where the naive consumer auto-creates the topic and a
  read returns an empty `Mapping` — the §6.3 caveat).

## 9. Boundary

### 9.1 Future features (noted, not designed)

- **Health/staleness accessor** — if a roster UI needs to tell degraded from healthy programmatically
  (vs the log), add a small `client.mesh` accessor; deferred.
- **Change feed** — a read returns a snapshot, not deltas; a push `changes()` is a future add iff ktables
  grows a change hook.
- **More kinds** — the accessor generalizes to future control planes as more `get_*` methods.

### 9.2 Out of scope

Agent-to-agent messaging/handoff; a write/registration surface; an ops fleet roster / CLI; a held
live-view handle; change-event delivery; any change to the worker-side view, boot gate, or wire records.

**Deferred (known limitation).** Security / SASL passthrough to a Mesh view's consumer. ktables takes
only `bootstrap_servers` today (this also limits the worker-side views), so v1 caller-side mesh views
require an observer path **without** broker auth. A follow-up needing a ktables/substrate change.

### 9.3 Resolved design decisions (folded into the body)

1. **Cold start = naive observer, not graceful-empty (and not ensure).** Topic existence is an
   operational precondition; the mesh opens **naively (`ensure_topic=False`, always)**, never creating a
   control-plane topic and never consulting `client._provisioning` — the observer touches no deployment
   config (document-don't-police). On a no-auto-create broker (production must be) a missing topic fails
   loud (`reason="open_failed"`); the agents' workers (ensure-when-provisioning, else broker auto-create on
   first advert) or ops create the topics, never the observer. (Honest caveat: an auto-create dev broker
   creates the topic on the naive consumer's metadata fetch — §6.3.) The graceful-empty tolerance (and its
   ktables `tolerate_missing_topic` prerequisite, bounded open, self-heal complexity) was **considered and
   dropped** (ADR-0029). **No dependency change.**
2. **Public surface = `get_agents()` / `get_tools()` → `Mapping[str, …]`.** No `view_*`, no `MeshView`
   handle (which also removes any use-after-close handle hazard). Both reads share the one cached per-kind
   view.
3. **Cache = per-kind single-flight, cancel-safe** (`asyncio.Task` per kind; `await asyncio.shield(task)`;
   eviction on the task's own failure via `done` callback; teardown-guarded open). Lock guards only the
   dict + `_closed`; kinds open in parallel; `aclose()` cancels in-flight + stops completed (§7.2).
4. **Health by behaviour — a typed raise with a `reason`.** No public `status`; `get_*` raises
   **`MeshUnavailableError(reason=…)`** when the view is not usable — `reason="establishing"` (not caught
   up; self-heals on retry), `"reader_dead"` (non-retriable reader death; terminal, alert/reconnect), or
   `"open_failed"` (open raised; evicted → retry) — else returns the `Mapping`. Never a silently-partial
   roster (§6.4). `server_urls is None` is a synchronous `ValueError` (programming error, not retriable).
   Deliberately divergent from the agent side (which warn-degrades) — named in §6.4.
4b. **Open dance inlined; 3-site extraction deferred.** The mesh inlines the teardown-guarded open (§7.1).
   Extracting a shared `open_client_view` helper was deferred — it is not a clean superset of the worker's
   bootstrap fallback, so extracting for one caller now is premature (Rule of Three); the mesh + 2 worker
   views are a tracked follow-up.
5. **DTOs** frozen dataclasses; `ToolInfo` union branch-by-type; `last_seen`; bare tool names;
   `ToolboxInfo` has no description (§5.3).
6. **`MeshViewConfig`** reader-scoped, set once via `Client.connect(mesh_config=…)` (§5.4).
7. **Names:** facade `Mesh`; new module `calfkit/client/mesh.py`; the existing `client/_mesh.py` (URL
   resolver) is renamed **`_mesh_url.py`** to match its contents (`resolve_mesh_url` / `CALFKIT_MESH_URL`)
   and free the `mesh` vocabulary.
8. **Security/SASL — deferred** (§9.2).

## 10. References

- [ADR-0028 — Caller-side mesh view over the control plane](../adr/0028-caller-side-mesh-view-over-control-plane.md)
- [ADR-0029 — Directory-topic existence is a precondition (graceful-empty rejected)](../adr/0029-caller-side-mesh-view-naive-open.md)
- [Impl plan](caller-side-mesh-view-impl-plan.md)
- [Control-Plane Substrate spec](control-plane-substrate-spec.md) · ADR-0010/0011
- [Client Caller-Side Developer Surface spec](client-caller-surface-spec.md)
- [CONTEXT.md](../../CONTEXT.md) glossary · [Issue #301](https://github.com/calf-ai/calfkit-sdk/issues/301)
