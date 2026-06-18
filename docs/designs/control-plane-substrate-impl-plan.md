# Control-Plane Substrate — Implementation Plan

- **Implements:** [`control-plane-substrate-spec.md`](./control-plane-substrate-spec.md)
- **ADRs:** 0010 (instance-keyed storage, collapsed view, cadence-on-record), 0011 (worker-owned publisher).
- **Method:** strict TDD (`/test-driven-development`), red→green per step; 100% coverage of new code (`/pytest-coverage`).
- **Shape:** one cohesive PR built in the ordered steps below (pure machinery, no concrete plane). Adopters (capability migration, agent discovery) are separate later PRs.
- **Dependency:** `ktables>=0.3.0` (already on `main`).

---

## 1. Principles & constraints

1. **TDD.** Every step writes failing tests first, then the minimal implementation. No implementation code lands without a test that exercised it red.
2. **Layering — keep the node layer ktables-free.** `calfkit/nodes/base.py` may import the **advert decorator + mixin** (pure Python, no ktables), but must **not** import the view or publisher (which touch ktables). Only `view.py` and `publisher.py` import `ktables`; only the worker wiring file imports the publisher. This preserves the existing rule that the agent/node layer never imports ktables (today only `worker.py` and `mcp/mcp_toolbox.py` do).
3. **Use-case-blind machinery.** Neither the publisher nor the worker wiring imports any concrete record type. The view is generic over `R` (bound to `ControlPlaneRecord`) and is constructed by consumers (tests in v1).
4. **No concrete plane.** v1 ships only the machinery; the integration vehicle is a **test-only** `ControlPlaneRecord` subclass + test node type.
5. **Mirror existing idioms.** `@advertises` mirrors `@handler`/`RegistryMixin` (`calfkit/_registry.py`); the worker wiring mirrors `_maybe_register_capability_view` (`calfkit/worker/worker.py`); the publisher lifecycle mirrors `MCPToolboxNode` (`calfkit/mcp/mcp_toolbox.py`).

## 2. Module layout

New package `calfkit/controlplane/`:

| File | Contents | Imports ktables? |
|---|---|---|
| `calfkit/controlplane/__init__.py` | Public exports | no (re-exports only) |
| `calfkit/controlplane/records.py` | `ControlPlaneRecord` base, `ControlPlaneIdentity` envelope | no |
| `calfkit/controlplane/advert.py` | `advertises` decorator, `AdvertInfo`, `AdvertRegistryMixin`, `advert_info` | no |
| `calfkit/controlplane/config.py` | `ControlPlaneConfig`, `STALE_MULTIPLIER` | no |
| `calfkit/controlplane/view.py` | `ControlPlaneView[R]` (+ `GroupedTableReader` Protocol for fakes) | **yes** (`GroupedKafkaTable`) |
| `calfkit/controlplane/publisher.py` | `ControlPlanePublisher` | **yes** (`GroupedKafkaTableWriter`) |

Touched existing files:

| File | Change |
|---|---|
| `calfkit/nodes/base.py` | add `AdvertRegistryMixin` to `BaseNodeDef`'s bases (alongside `LifecycleHookMixin, RegistryMixin`); import from `calfkit.controlplane.advert` (the submodule, **not** the package `__init__`, to avoid pulling ktables into the node layer) |
| `calfkit/worker/worker.py` | add `control_plane: ControlPlaneConfig` ctor param + `self._control_plane`; add `_maybe_register_control_plane()` (called from `register_handlers`); add `_make_control_plane_writer_resource(topic)` |
| `calfkit/__init__.py` (or top-level exports) | export `ControlPlaneRecord`, `ControlPlaneIdentity`, `advertises`, `ControlPlaneView`, `ControlPlaneConfig` (per the existing top-level-export convention) |

## 3. Component designs (precise signatures)

### 3.1 `records.py`

```python
from pydantic import AwareDatetime, BaseModel, ConfigDict

class ControlPlaneRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")    # tolerant reader (additive forward-compat)
    schema_version: int                          # NO default on the base; each subclass sets one
    node_id: str
    worker_id: str
    started_at: AwareDatetime
    last_heartbeat_at: AwareDatetime
    heartbeat_interval: float                    # the WRITER's cadence (seconds) — the C1 field

class ControlPlaneIdentity(BaseModel):
    model_config = ConfigDict(frozen=True)
    node_id: str
    worker_id: str
    started_at: AwareDatetime
    last_heartbeat_at: AwareDatetime
    heartbeat_interval: float
```

- The base intentionally has **no** `schema_version` default, so it is abstract-by-omission (a subclass *must* set one — `schema_version: int = N`). This is load-bearing: `ControlPlaneView` derives its reader version from that default and **rejects a record type without one** at construction (§3.4), so a missing default fails loud at view build, not as a `TypeError` on every read. Concrete records construct as `MyRecord(**identity.model_dump(), <content>)`.
- `ControlPlaneIdentity` is what the publisher stamps and passes to factories; its 5 fields are exactly the base's non-`schema_version` fields, so `**identity.model_dump()` fills them.

### 3.2 `advert.py`

```python
_ADVERT_ATTR = "__calf_advert__"

@dataclass(frozen=True)
class AdvertInfo:
    topic: str
    record: type[ControlPlaneRecord]
    name: str                                    # the method/attr name on the owning class

def advertises(topic: str, *, record: type[ControlPlaneRecord]) -> Callable[[F], F]:
    # eager validation: topic non-empty; record is a ControlPlaneRecord subclass.
    # _decorate: setattr(_unwrap(method), _ADVERT_ATTR, AdvertInfo(topic, record, method.__name__)); return method UNCHANGED.

def advert_info(method) -> AdvertInfo | None: ...  # getattr(_unwrap(method), _ADVERT_ATTR, None)

class AdvertRegistryMixin:
    _adverts: ClassVar[dict[str, AdvertInfo]] = {}   # topic -> info; fresh per subclass
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)          # CHAIN — RegistryMixin also defines this
        # walk reversed(cls.__mro__), collect _ADVERT_ATTR-marked members into {attr: info}
        # (most-derived wins, like _handlers); build {topic: info} with topic-uniqueness:
        #   duplicate topic across two attrs => RegistryConfigError at class definition.
        cls._adverts = ...
    def control_plane_adverts(self) -> dict[str, Callable[[ControlPlaneIdentity], ControlPlaneRecord]]:
        return {topic: getattr(self, info.name) for topic, info in type(self)._adverts.items()}
```

- `_unwrap`, `RegistryConfigError`, and the marker+`__init_subclass__` structure are copied from `_registry.py` (do **not** re-implement subtly differently). Reuse `_registry._unwrap` and `calfkit.exceptions.RegistryConfigError`.
- Coexistence with `@handler`: distinct marker attr (`__calf_advert__` ≠ `__calf_handler__`) and a separate `_adverts` registry; both `__init_subclass__`s chain via `super()`. Verified feasible.
- `getattr(self, info.name)` resolves the **bound** method at access time, so an override-without-redecorate is reflected (same semantics as `RegistryMixin.handlers`).

### 3.3 `config.py`

```python
STALE_MULTIPLIER = 3

@dataclass(frozen=True)
class ControlPlaneConfig:
    heartbeat_interval: float = 30.0     # WRITE side: loop cadence; stamped on every record
    stale_after: float | None = None     # READ side: override/floor; None => STALE_MULTIPLIER * record.heartbeat_interval
    catchup_timeout: float = 30.0        # READ side: view catch-up gate bound
    bootstrap_servers: str | None = None # both sides: split-cluster escape hatch
    def __post_init__(self):
        # validate heartbeat_interval > 0, catchup_timeout > 0, stale_after is None or > 0
```

### 3.4 `view.py`

```python
@runtime_checkable
class GroupedTableReader(Protocol):       # the subset of GroupedKafkaTable the view needs; a dict-backed fake satisfies it
    def groups(self) -> set[str]: ...
    def members(self, group: str) -> dict[str, Any]: ...
    @property
    def status(self) -> "TableStatus": ...
    @property
    def failure(self) -> BaseException | None: ...
    @property
    def is_caught_up(self) -> bool: ...
    async def barrier(self, timeout: float | None = None) -> bool: ...
    async def wait_until_caught_up(self, timeout: float | None = None) -> bool: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...

R = TypeVar("R", bound=ControlPlaneRecord)

class ControlPlaneView(Generic[R]):
    def __init__(self, table: GroupedTableReader, record_type: type[R], *, stale_after: float | None = None):
        # Resolve the reader's supported version from R's schema_version DEFAULT — and reject a
        # record type that has no default. The abstract base ControlPlaneRecord (and any subclass
        # that forgot the default) has `model_fields["schema_version"].default is PydanticUndefined`;
        # comparing that with `>` later would TypeError on EVERY read. Catch it HERE, loudly:
        #   field = record_type.model_fields["schema_version"]
        #   if field.default is PydanticUndefined:
        #       raise RegistryConfigError(f"{record_type.__name__} must set `schema_version: int = N`")
        #   self._reader_version: int = field.default

    @classmethod
    def open(cls, *, bootstrap_servers, topic, record_type, catchup_timeout=30.0,
             ensure_topic=False, stale_after=None) -> "ControlPlaneView[R]":
        # builds GroupedKafkaTable.json(model=record_type, ...) and wraps it. (Caller still awaits .start().)

    # collapsed, liveness-aware reads
    def get(self, node_id: str) -> R | None
    def online_nodes(self) -> set[str]
    def snapshot(self) -> dict[str, R]

    # health passthrough
    @property
    def status(self) -> TableStatus
    @property
    def failure(self) -> BaseException | None
    @property
    def is_caught_up(self) -> bool
    async def barrier(self, timeout=None) -> bool
    async def wait_until_caught_up(self, timeout=None) -> bool
    async def start(self) -> None
    async def stop(self) -> None

    # internal
    def _live_members(self, node_id) -> dict[str, R]   # filter: supported schema + live
```

Collapse / filter algorithm (the C2 + m4 invariants):

```
reader_version = self._reader_version    # R's schema_version default, validated non-undefined in __init__
now = datetime.now(tz=utc)
_live_members(node_id):
    out = {}
    for worker_id, rec in table.members(node_id).items():     # ktables already decoded each to R
        if rec.schema_version > reader_version:    continue   # schema-skip (log once)
        threshold = self._stale_after if self._stale_after is not None else STALE_MULTIPLIER * rec.heartbeat_interval
        if (now - rec.last_heartbeat_at).total_seconds() > threshold:  continue   # stale
        out[worker_id] = rec
    return out
get(node_id):       live = _live_members(node_id); return None if not live else max(live.values(), key=lambda r: r.last_heartbeat_at)
online_nodes():     return {g for g in table.groups() if self._live_members(g)}
snapshot():         return {g: self.get(g) for g in online_nodes()}     # all non-None by construction
```

This guarantees `get(g) is not None ⟺ g ∈ online_nodes()` (filter-then-tie-break, never the reverse). `status`/`failure`/`is_caught_up`/`barrier`/`start`/`stop`/`wait_until_caught_up` delegate to the table.

### 3.5 `publisher.py`

```python
class ControlPlanePublisher:
    def __init__(self, *, worker_id: str, adverts: list[tuple[BaseNodeDef, AdvertInfo]],
                 config: ControlPlaneConfig):
        self._worker_id = worker_id
        self._adverts = adverts                 # (node, AdvertInfo)
        self._config = config
        self._task: asyncio.Task | None = None
        self._shutting_down = False
        self._started_at: AwareDatetime | None = None

    async def start(self, ctx: ServingContext) -> None:        # registered as worker after_startup
        # capture started_at, then publish each advert ONCE. A failure here PROPAGATES, aborting
        # boot (fail-loud, matching MCPToolboxNode._publish_on_startup) — a node that cannot advertise
        # must surface, not start silently degraded. Only after a clean first publish do we spawn
        # the heartbeat loop.
        self._started_at = datetime.now(tz=utc)
        for node, info in self._adverts:
            await self._publish_one(ctx, node, info, self._started_at)
        self._task = asyncio.create_task(self._loop(ctx), name="control-plane-heartbeat")

    async def stop(self, ctx: ServingContext) -> None:         # registered as worker on_shutdown
        # flag FIRST (synchronous, no await above), cancel+await loop, THEN tombstone the declared
        # cross-product. Cancel-before-delete is what makes the tombstone win — after `await task`
        # the loop (incl. any in-flight `set`) is fully resolved/cancelled, so no `set` lands after.
        self._shutting_down = True
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for node, info in self._adverts:
            writer = ctx.resources.get(self._writer_key(info.topic))
            if writer is not None:
                await writer.delete(node.node_id, self._worker_id)   # idempotent; declared cross-product

    async def _loop(self, ctx) -> None:
        # loop ticks are per-advert RESILIENT: a single bad advert is logged and the rest still
        # publish; everything retries next tick (compaction/heartbeat heals). CancelledError propagates.
        while True:
            await asyncio.sleep(self._config.heartbeat_interval)
            now = datetime.now(tz=utc)
            for node, info in self._adverts:
                try:
                    await self._publish_one(ctx, node, info, now)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("control-plane publish failed node=%s topic=%s; retry next tick",
                                     node.node_id, info.topic)

    async def _publish_one(self, ctx, node, info, now) -> None:
        assert self._started_at is not None        # set in start() before any publish (also narrows for mypy)
        writer = ctx.resources[self._writer_key(info.topic)]   # KeyError => wiring bug (fail-loud at start)
        identity = ControlPlaneIdentity(
            node_id=node.node_id, worker_id=self._worker_id, started_at=self._started_at,
            last_heartbeat_at=now, heartbeat_interval=self._config.heartbeat_interval)
        record = getattr(node, info.name)(identity)            # opaque ControlPlaneRecord
        await writer.set(node.node_id, self._worker_id, record)

    @staticmethod
    def _writer_key(topic: str) -> str:
        return f"calfkit.controlplane.writer.{topic}"
```

Notes:
- `_publish_one` pulls content from the factory each call (pull-only). **Startup is fail-loud:** `start` publishes each advert once and lets any failure propagate (aborting boot, like `MCPToolboxNode._publish_on_startup`; on such a failure the lifecycle runs no `on_shutdown`, so a partially-published record simply goes stale — crash-equivalent and accepted). **Loop ticks are resilient:** `_loop` catches per-advert (log-and-continue; re-raise `CancelledError`), mirroring `MCPToolboxNode._heartbeat_loop`.
- **Resurrection safety:** `stop` cancels-and-`await`s the loop *before* any `delete`, so no in-flight `set` can land after a `delete` (after `await task`, the task — including any in-flight `set` — is fully resolved or cancelled; the subsequent `delete` therefore wins). The `_shutting_down` flag is set synchronously first; it is the seam a future `publish_now()` push will check.
- The publisher never imports a concrete record type — `getattr(node, info.name)(identity)` returns an opaque `ControlPlaneRecord`.

### 3.6 Worker wiring (`worker.py`)

```python
# __init__: control_plane: ControlPlaneConfig | None = None
self._control_plane = control_plane if control_plane is not None else ControlPlaneConfig()

def _maybe_register_control_plane(self) -> None:
    if self._control_plane_publisher is not None:   # idempotent: only wire once
        return
    adverts = [(node, info) for node in self._nodes for info in type(node)._adverts.values()]
    if not adverts:
        return
    # distinct topics => distinct keys, registered once; the guard above makes the whole
    # method idempotent, so no per-key dedup is needed.
    for topic in {info.topic for _, info in adverts}:
        self.resource(name=control_plane_writer_key(topic))(self._make_control_plane_writer_resource(topic))
    publisher = ControlPlanePublisher(worker_id=self.id, adverts=adverts, config=self._control_plane)
    self._control_plane_publisher = publisher
    self.after_startup(publisher.start)
    self.on_shutdown(publisher.stop)

def _make_control_plane_writer_resource(self, topic: str):
    async def _resource(ctx) -> AsyncIterator[GroupedKafkaTableWriter]:
        from ktables import GroupedKafkaTableWriter
        bootstrap = self._control_plane.bootstrap_servers or self._derive_bootstrap_servers()
        if not bootstrap:
            raise RuntimeError(f"cannot derive bootstrap servers for control-plane topic {topic!r}; "
                               "set ControlPlaneConfig(bootstrap_servers=...).")
        writer = GroupedKafkaTableWriter.json(
            bootstrap_servers=bootstrap, topic=topic,
            ensure_topic=self._client._provisioning.enabled,
        )
        await writer.start()
        yield writer
        await writer.stop()
    return _resource

# in register_handlers(), after self._maybe_register_capability_view():
self._maybe_register_control_plane()
```

Sequencing (verified against the lifecycle): `register_handlers` runs in `_on_startup` (pre-broker) → registers the writer `@resource`s **and** the publisher's `after_startup`/`on_shutdown` hooks synchronously → the resource phase opens the writers → `after_startup` runs `publisher.start` (writers already in `ctx.resources`) → `on_shutdown` runs `publisher.stop` (writers still alive; resources close only after `after_shutdown`). A node added via `add_nodes()` after `register_handlers` (which latches `_prepared`) does not join a plane — document this boundary.

## 4. Build order (TDD steps)

> Each step: write the listed tests (red) → implement → green → `make fix && make check`.

**Step 0 — package scaffold.** Create `calfkit/controlplane/__init__.py` (empty exports stub). No tests.

**Step 1 — records (`records.py`).**
- Tests (`tests/test_controlplane_records.py`): a test subclass `_Rec(ControlPlaneRecord)` with `schema_version=1` + a content field; construct via `_Rec(**identity.model_dump(), content=...)`; assert all base fields populate. Tolerant reader (extra field ignored). Base alone (no `schema_version`) → `ValidationError`. `ControlPlaneIdentity` is frozen.
- Implement `records.py`.

**Step 2 — advert decorator + mixin (`advert.py`) + attach to `BaseNodeDef`.**
- Tests (`tests/test_controlplane_advert.py`): decorator returns the method unchanged (still directly callable) + stamps `AdvertInfo`; `__init_subclass__` collects `_adverts` per class; MRO override (re-decorate and override-without-redecorate both resolve to the most-derived method); duplicate topic on two methods of one class → `RegistryConfigError`; a class with **both** `@handler` and `@advertises` collects both registries independently (coexistence); a node with no adverts has empty `_adverts`. `control_plane_adverts(self)` returns `{topic: bound method}`.
- Implement `advert.py`; add `AdvertRegistryMixin` to `BaseNodeDef` bases. Run the **full** node/handler suite to confirm no regression from the extra `__init_subclass__`.

**Step 3 — config (`config.py`).**
- Tests (`tests/test_controlplane_config.py`): defaults; `__post_init__` rejects non-positive `heartbeat_interval`/`catchup_timeout` and non-positive `stale_after` (None allowed); frozen.
- Implement `config.py`.

**Step 4 — view (`view.py`).**
- Tests (`tests/test_controlplane_view.py`), all offline against a **dict-backed fake** table satisfying `GroupedTableReader`:
  - `get`/`online_nodes`/`snapshot` over multiple groups/members.
  - **Collapse:** multiple live members → most-recent `last_heartbeat_at` wins.
  - **Staleness from the record's cadence:** a member with old `last_heartbeat_at` relative to *its own* `heartbeat_interval` is excluded; a reader `stale_after` override takes precedence.
  - **Invariant:** `get(g) is not None ⟺ g ∈ online_nodes()` across mixed live/stale members.
  - **Schema-skip:** a member with `schema_version` > reader version is excluded from all reads.
  - **Health passthrough:** `status`/`failure`/`is_caught_up` reflect the fake's values; `barrier`/`start`/`stop` delegate.
  - **Defaultless record type rejected:** constructing `ControlPlaneView` with a record type whose `schema_version` has no default raises a clear `RegistryConfigError` (red-first — guards the `PydanticUndefined` bug), not a later read-time `TypeError`.
- Implement `view.py` (the `.open()` classmethod is covered by the integration step, not unit — it touches ktables).

**Step 5 — publisher (`publisher.py`).**
- Tests (`tests/test_controlplane_publisher.py`), offline against a **fake writer** (records `set`/`delete` calls) and a fake `ServingContext`:
  - `start` captures `started_at`, publishes each advert once with a correct `ControlPlaneIdentity` (incl. `heartbeat_interval` from config), keyed `(node_id, worker_id)`, to the right topic's writer.
  - **Liveness/content split (the §6 contract):** with a content-bearing test record whose `content_updated_at` is a node-tracked field, two ticks without a content change leave `content_updated_at` fixed while `last_heartbeat_at` advances; a content change moves both. (Drive two publishes via `_publish_one` with an advancing `now`, or run `_loop` at a short interval.)
  - **Tombstone:** `stop` deletes the **declared cross-product** `(node_id, worker_id)` on every advert's topic writer; idempotent (delete of an unwritten key tolerated).
  - **Ordering:** `stop` sets `_shutting_down` and cancels+awaits the loop before any `delete` (assert no `set` recorded after the first `delete`).
  - **Startup is fail-loud, loop is resilient:** a factory/`set` failure during `start` **propagates** (aborts boot); during a `_loop` tick the same failure is logged, the loop survives, and other adverts in that tick still publish.
- Implement `publisher.py`.

**Step 6 — worker wiring (`worker.py`).**
- Tests (`tests/test_controlplane_worker_wiring.py`), offline:
  - `_maybe_register_control_plane` registers the publisher + one writer resource per distinct topic **iff** a hosted node has adverts; no adverts → nothing registered (publisher dormant).
  - Idempotent (a second `register_handlers` does not double-register).
  - Two node types on the same topic → one writer resource for that topic.
  - Bootstrap derivation error path raises a clear message when underivable.
- Implement the worker changes (ctor param, `_maybe_register_control_plane`, `_make_control_plane_writer_resource`, call site in `register_handlers`).

**Step 7 — integration (kafka lane, `tests/integration/test_controlplane_substrate_kafka.py`).** Marked `@pytest.mark.kafka`, using the `kafka_bootstrap` fixture (`tests/integration/conftest.py`). A test-only record + test node type:
  - **Round-trip:** a worker hosting an advertising node → a `ControlPlaneView.open(...)` over the topic reflects the node after `barrier()`; `get`/`online_nodes` correct.
  - **Tombstone:** clean worker shutdown → the view drops the node after `barrier()`.
  - **2-worker same-node replica regression (ADR-0010 / CRITICAL-1 proof):** two workers host the same `node_id`; one shuts down cleanly; the node stays online via the survivor.
  - **Frozen-view:** a view pointed at an unreachable broker (or after the table fails) surfaces via `status`/`failure` (CRITICAL-4) — best-effort; keep robust.

**Step 8 — exports, docs, polish.**
- Top-level exports (`calfkit/__init__.py`): `ControlPlaneRecord`, `ControlPlaneIdentity`, `advertises`, `ControlPlaneView`, `ControlPlaneConfig` (follow the existing export style + `__all__`).
- `calfkit/controlplane/__init__.py` exports all public symbols.
- `/pytest-coverage` to 100% on the new modules; `make fix && make check` clean (offline lane) + `make test-kafka` green.
- Flip ADR-0010 / ADR-0011 `status: proposed → accepted` at merge.

## 5. Test plan summary

| Layer | Where | Broker? |
|---|---|---|
| records, advert, config, view, publisher, worker-wiring | `tests/test_controlplane_*.py` | **no** (dict-backed fake table, fake writer, `TestKafkaBroker` where a broker object is needed) |
| substrate round-trip / tombstone / replica regression / frozen-view | `tests/integration/test_controlplane_substrate_kafka.py` | **yes** (`@pytest.mark.kafka`, testcontainers) |

- ktables' own semantics (catch-up gate, tombstone delivery, reader death) are **not** re-tested — covered by the ktables suite. calfkit tests only its policy layers: collapse, staleness-from-cadence, schema-skip, advert collection, publisher ordering/tombstone-set, wiring trigger.
- The dict-backed fake table satisfies `GroupedTableReader` (a `Protocol`), so view unit tests need zero ktables.

## 6. Cross-cutting invariants to enforce (each has a test above, except #5)

1. **Filter-then-tie-break** in the view → `get(g) is not None ⟺ g ∈ online_nodes()` (Step 4).
2. **Staleness derives from `record.heartbeat_interval`**, not reader config (Step 4) — the C1 fix.
3. **Schema-skip lives in the view filter** (Step 4) — the m4 fix.
4. **`content_updated_at` is node-tracked, never `now()` in a factory** — enforced by docstring on `@advertises` + the §6 contract + the Step 5 split test. The substrate cannot police it (use-case-blind); it is tested via the test record.
5. **One topic ⇒ one schema** — a documented deployment-time contract; **no test (not policeable across node types)** — note it in the `advertises` and `ControlPlaneView` docstrings.
6. **Tombstone = declared cross-product, idempotent** (Step 5).
7. **Resurrection-safety:** flag-first, cancel-and-await loop before delete (Step 5).
8. **Wiring sequence:** writer resources + serving hooks registered synchronously in `register_handlers`, before the resource phase (Step 6).

## 7. Risks & edge cases

- **`__init_subclass__` chaining.** Adding `AdvertRegistryMixin` to `BaseNodeDef` means two mixins define `__init_subclass__`; both must call `super().__init_subclass__(**kwargs)`. `RegistryMixin` already does. Mitigation: Step 2 runs the full existing node suite to confirm no handler-collection regression. MRO order of the bases doesn't matter (each collects into its own attr).
- **Bootstrap on the read side.** `ControlPlaneView.open()` needs bootstrap + topic only (no writer config — cadence rides on records). A worker-hosted reader can pass `worker._derive_bootstrap_servers()`; an out-of-process reader passes its own. (v1 has no production reader; this is for adopters/tests.)
- **Clock for staleness.** The view uses `datetime.now(tz=utc)`; unit tests set record timestamps in the past/future to exercise the threshold (no clock injection needed).
- **`started_at` semantics.** Captured once at `publisher.start` (the worker's serving start), shared by all of this worker's records — matches "this instance's boot time."
- **Heterogeneous cadence.** Because the threshold is computed per-member from `record.heartbeat_interval`, two writers on different intervals are each judged correctly.
- **`add_nodes()` after prepare.** Does not join a plane (documented); not a v1 feature.
- **`ensure_topic` provisioning.** Writers and (adopter) views set `ensure_topic=self._client._provisioning.enabled`; in prod (provisioning off) the topic is ops-governed and a missing topic fails loudly — consistent with the capability topic.

## 8. Out of scope (deferred, additive later)

- No concrete plane (capability/agent) — separate adopter PRs.
- No `publish_now()` push — the `_shutting_down` flag + single write path leave the seam ready.
- No `instances(node_id)` / annotated (`include_stale`) reads — additive when an adopter (ops, load-balancing) needs them.
- No per-topic config override of the `@advertises` topic — additive.
- No error-propagation concerns (deadlines, dangling, budgets).

## 9. Definition of done

- All Step 1–7 tests green. `/pytest-coverage` 100% on the offline-testable logic in `calfkit/controlplane/*` and the new worker code; the thin ktables-touching glue (`ControlPlaneView.open()`, the writer `@resource`) runs only in the kafka lane, so tag it `# pragma: no cover -- exercised in the kafka lane` for the offline `/pytest-coverage` pass (matching the existing repo convention).
- `make fix && make check` clean (`make check` = lint + format + type only, no tests); `make test` (offline) and `make test-kafka` (integration) both green.
- Public exports in place; docstrings carry the §6 invariants (one-schema, content-timestamp contract).
- Spec + ADRs unchanged by implementation (or any necessary correction folded back, with a note); ADR-0010/0011 flipped to `accepted` at merge.
- No node-layer ktables import introduced (grep `calfkit/nodes/` for `ktables` stays empty).
