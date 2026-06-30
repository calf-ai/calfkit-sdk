# Caller-Side Mesh View — Implementation Plan

Implements [`docs/designs/caller-side-mesh-view-spec.md`](caller-side-mesh-view-spec.md) and
[ADR-0028](../adr/0028-caller-side-mesh-view-over-control-plane.md) /
[ADR-0029](../adr/0029-caller-side-mesh-view-naive-open.md). The migration / code-change
plan the spec leaves out. Read the spec first; this uses its §-numbers.

**Status:** plan for adversarial review *before* any code. No code written yet. A worktree is step 0 of
execution, created after approval.

**No prerequisites.** The earlier ktables `tolerate_missing_topic` change + `0.4→1.x` bump are **dropped**
— the naive-observer cold-start (ADR-0029: `ensure_topic=False`, the topic is an operational
precondition) needs no dependency change. This is a self-contained calfkit PR on the current
`ktables>=0.4.0`.

---

## 0. Decisions locked

| # | Decision | Choice |
|---|---|---|
| Surface | reads | `client.mesh.get_agents()` / `get_tools()` → `Mapping[str, AgentInfo]` / `Mapping[str, ToolInfo]`; **no `view_*`, no held handle** |
| Accessor | `client.mesh` | cached `Mesh` singleton (one per client); zero-I/O until first `get_*` |
| Lifecycle | who owns the consumer | the **client** — one cached `ControlPlaneView` per kind, lazily opened (**per-kind single-flight, cancel-safe**: `shield` + done-callback eviction + teardown-guarded open), torn down at `client.aclose()` |
| Cold start | missing topic | **naive observer** — `ensure_topic=False`, always; **no** `client._provisioning` consult; the observer never creates a control-plane topic (workers/ops do). A missing topic fails loud (`reason="open_failed"`). No probe, no supervisor, no bounded open, no dependency change |
| Health | view status | **typed raise with `reason`** — `get_*` raises `MeshUnavailableError(reason="establishing"\|"reader_dead"\|"open_failed")` if not usable, else returns the `Mapping`; `server_urls is None` → synchronous `ValueError`; no public `status` field |
| Factoring | the open dance | **inline** the mesh's naive open (it differs from the worker's ensure-bearing open); the 3-site extraction is a tracked follow-up |
| DTOs | public types | frozen dataclasses `AgentInfo`; `ToolInfo = ToolNodeInfo \| ToolboxInfo` (branch by type) + `ToolSpec`; `last_seen`; bare tool names; `ToolboxInfo` no description |
| Config | tuning | `MeshViewConfig(stale_after, catchup_timeout, reader_tuning)`, set once via `Client.connect(mesh_config=…)` |
| Module | naming | new `calfkit/client/mesh.py`; **rename** `client/_mesh.py` → `_mesh_url.py` (matches its `resolve_mesh_url`/`CALFKIT_MESH_URL` contents; frees `mesh`) |
| Security | SASL passthrough | **deferred** (spec §9.2) |

## 1. Scope

**In:** `calfkit/client/mesh.py` (DTOs, `MeshViewConfig`, projections, `Mesh`); `Client.mesh` +
`Client.connect(mesh_config=)` + `Client.aclose()` wiring + `Client.__init__` field init; the
`_mesh.py → _mesh_url.py` rename (all import sites); public exports.

**Out:** the worker-side views / boot gate / wire records (unchanged); security passthrough; a change
feed; any dependency change.

## 2. Reuse map (verified — `calfkit/…` file:line)

| Need | Reuse | Where |
|---|---|---|
| Bootstrap address | `Client.server_urls` (`None` for a directly-built client) | `client/caller.py:199` (`@property`), `:200` (def) |
| `aclose()` to extend | stop the mesh views before the broker | `client/caller.py:262` |
| Materialization | `ControlPlaneView.open(*, bootstrap_servers, topic, record_type, catchup_timeout, ensure_topic, stale_after, reader_tuning)` — **all keyword-only**; `start()` **is** the catch-up gate | `controlplane/view.py:205` (`@classmethod` `:204`) |
| Collapsed reads + health | `snapshot()` `:100` / `get()` `:90` / **`is_caught_up`** `:187` / `failure` `:183` / `stop()` `:199` — health checks `failure` (`:183`) **before** `is_caught_up` (`:187`) (NOT the `"degraded"` string) | `controlplane/view.py` |
| Agents wire | `AGENTS_TOPIC`, `AgentCard(description, last_heartbeat_at)` | `models/agents.py:29,49` |
| Tools wire | `CAPABILITY_TOPIC`; `CapabilityToolDef(name, description, parameters_json_schema)` `:39`; `CapabilityRecord(dispatch_topic, tools, …)` `:49`; `node_kind` on the `ControlPlaneStamp` base | `models/capability.py:31`; `controlplane/records.py:35` |
| Bare-name rule | `_namespace_prefix` (bare here) | `models/capability.py:80` |
| Config field types | `PositiveFiniteFloat`, `KTableReaderTuning` | `tuning.py:29,32` |
| Frozen-dataclass + closed-union precedent | the `RunEvent` family (`RunTerminal` = the 2-member alias) | `client/events.py` |
| Error types | new `MeshUnavailableError(reason=…)` (joins the family); `ClientClosedError` (post-`aclose`); `ValueError` (`server_urls is None`). `NodeFaultError.report.retryable` is the discriminate-by-field precedent | `calfkit/exceptions.py` |
| Offline fake reader | `_FakeTable` (dict-backed; `status=`/`failure=`/`is_caught_up=` knobs) in `ControlPlaneView(_FakeTable(…), Rec)` | `tests/test_controlplane_view.py:37` (`is_caught_up=` @46/68) |
| Kafka lane | `@pytest.mark.kafka` + `kafka_bootstrap` fixture | `tests/integration/conftest.py:137` |
| Exports | `client/__init__.py`, top-level `__init__.py` | — |

## 3. Impl-time decisions resolved here (rationale)

1. **Module:** one new `calfkit/client/mesh.py` — DTOs, `MeshViewConfig`, projections, `Mesh`. **No
   aiokafka/ktables import at module load** (ktables stays lazy inside `ControlPlaneView.open`); asserted
   by a test.
2. **Cache = per-kind single-flight, CANCEL-SAFE** (spec §7.2). A `_Cell` = `(task: asyncio.Task[ControlPlaneView],
   project)`; `Mesh` holds `dict[kind, _Cell]` + an `asyncio.Lock` (guards the dict + `_closed` only,
   **never** held across `start()`). `_cell(kind)`: raise `ValueError` synchronously if `server_urls is
   None`; under the lock create-or-get the cell (`_make_cell` registers the `done` callback closing over
   the **cell** — no `t.cell` back-ref); then **`await asyncio.shield(cell.task)`** outside the lock (a
   waiter's `wait_for` cancel is absorbed by the shield, **not** propagated into the shared open — the
   round-3 CRITICAL). Eviction is the `done` callback `_evict(name, cell, task)`: drop **only** on the
   task's own non-cancelled exception, guarded by `self._cells.get(name) is cell` (never drop a retry's
   newer cell; benign after the `aclose` drain). `get_*` reads the resolved view from
   **`cell.task.result()`** (no `self._cells` re-index → no `aclose`-race `KeyError`). A waiter catching
   `CancelledError` re-raises `ClientClosedError` if `_closed`, else re-raises; a waiter catching the
   open's `Exception` re-raises `MeshUnavailableError(reason="open_failed")`.
3. **`_open(kind)` opens NAIVELY, inlined** (spec §7.1; the seam extraction is deferred — the worker's
   open is not equivalent). `ControlPlaneView.open(bootstrap_servers=client.server_urls, …,
   ensure_topic=False)` — **always `False`**; the observer never creates a topic and never consults
   `client._provisioning` (cold-start = naive, §6.3). **Teardown-guards `start()`** (`try/except
   BaseException: with suppress(Exception): await view.stop(); raise` — no leaked consumer on a failed *or
   cancelled* start). A failed open → the task fails → evicted → `get_*` raises `reason="open_failed"`
   (cause attached, no string-matching).
4. **Health = typed raise with `reason`** (spec §6.4). `get_agents`/`get_tools` are thin typed wrappers over
   one shared `_read(kind)` core (DRY — don't copy the ordering-sensitive health logic twice). After
   `_cell()`, `_read` checks the resolved view **`failure` first, then `is_caught_up`** (LOAD-BEARING:
   `is_caught_up` is a sticky latch that stays `True` after a death, so a dead-but-caught-up view must
   resolve to `reader_dead`, not `establishing`): `view.failure is not None` →
   `MeshUnavailableError(reason="reader_dead")` from the cause (**terminal**: ktables sets `failure` only on
   non-retriable death; the cell is *not* auto-evicted — recovery is a fresh `Client`); `not
   view.is_caught_up` → `reason="establishing"` (self-heals on the **same** cached view); else project
   `view.snapshot()`. No best-effort-partial path.
5. **Projection + dedup on the per-kind cache cell.** `_project_agent` / `_project_tool` are pure over one
   record; the **skip-log dedup set** lives on the `_Cell` (1:1 with the long-lived view). `_project_tool`
   returns `None` for unknown `node_kind` and a `"tool"` record with `tools ≠ 1`; a *malformed `ToolSpec`*
   is **not** a projection concern (a missing **required** `parameters_json_schema` fails wire-decode in
   ktables, dropped before projection). **`parameters_schema` is deep-copied** into the DTO (not aliased),
   keeping the projection truly immutable.
6. **Return a fresh read-only `Mapping`** each call (e.g. `types.MappingProxyType` over the projected
   dict) — immutable, point-in-time.
7. **`aclose()`** (spec §7.2): under the lock set `_closed`, drain the cell dict; then `cancel()` each
   task and `view.stop()` the resolved view — **best-effort** (a per-view `stop()` error is *logged*, not
   aggregated/raised — matching the codebase teardown-swallow pattern; in-flight opens skip via
   `continue`). `Client.aclose()` calls it **only if `self._mesh is not None`** and still stops the broker
   even if mesh teardown raises. `Client.__init__` initializes `self._mesh = None` + `self._mesh_config = None`.
8. **Error taxonomy** — a new typed **`MeshUnavailableError(message, *, reason)`** in `calfkit/exceptions.py`
   (the `reason: Literal["establishing","reader_dead","open_failed"]` mirrors `NodeFaultError.report.retryable`):
   not-usable → `MeshUnavailableError(reason=…)` (cause attached) so a poller routes retry-fast
   (`establishing`/`open_failed`) vs alert/reconnect (`reader_dead`) without string-matching; **unconnected**
   (`server_urls is None`, a *programming* error) → **`ValueError`**, raised **synchronously** before the
   open (never wrapped — you never retry a misconfigured client); **after `aclose()`** (or an in-flight
   `get_*` whose task `aclose` cancels) → `ClientClosedError`.
9. **`_mesh.py → _mesh_url.py` rename** (+ `tests/test_mesh.py → test_mesh_url.py`) in this PR. Update
   **all three** production import sites — `client/caller.py:24`, `client/_broker.py:24`, `cli/_run.py:19`
   (+ the `caller.py:114` comment) — and the test docstrings.

## 4. Commit staging (TDD — ONE PR; each commit red→green→refactor; repo green at every commit)

### Commit 0 — Worktree + rename + skeleton
Branch off `main`. Rename `client/_mesh.py → _mesh_url.py` + `tests/test_mesh.py → test_mesh_url.py`; fix **all three** production import sites (`caller.py:24`, `_broker.py:24`, `cli/_run.py:19`) + the `caller.py:114` comment + the renamed test's **live import (`test_mesh_url.py:13`) and docstrings**. Add empty `client/mesh.py` + `tests/test_mesh.py` (the freed name → the new feature's tests). `make check` green.

### Commit 1 — DTOs + `MeshViewConfig` + `MeshUnavailableError`
Frozen dataclasses + `ToolInfo` alias; `MeshViewConfig` (pydantic frozen, `extra="forbid"`); `MeshUnavailableError(message, *, reason: Literal["establishing","reader_dead","open_failed"])` in `calfkit/exceptions.py` (the client error family) — its **docstring carries a `reason`→action table** (so the implementation-leaning value names self-document, like `NodeFaultError`'s docstring), and a **`__reduce__`** so it survives cross-process serialization with its `reason` intact (the `ClientTimeoutError` precedent).
**Tests:** construction/immutability; `match`-by-type; config rejects `bootstrap_servers`/`heartbeat_interval`/`ensure_topic`/`probe_interval` and NaN/inf/≤0; `MeshUnavailableError` is exported, catchable, carries `reason`, and round-trips cross-process with `reason` preserved.

### Commit 2 — Projections (+ dedup on a per-kind wrapper)
`_project_agent`; `_project_tool` (kind dispatch; unknown kind / `"tool"` with `tools≠1` → `None`; deep-copy `parameters_schema`); a small per-kind projector wrapper holding the deduped skip-log. `last_seen` ← `last_heartbeat_at`.
**Tests:** agent card; tool node (single inlined tool); toolbox (many, bare names, empty→`()`); unknown kind → `None`; `"tool"` with `tools≠1` → `None`; skip-log deduped across repeated projections; **`parameters_schema` deep-copy** (mutating the DTO's dict doesn't touch the cached record).

### Commit 3 — `Mesh` cache + naive open + reads + health
`Mesh(client, config)`: `_cell`/`_make_cell`/`_evict` (cancel-safe single-flight — `shield` + identity-guarded done-callback eviction), `_open` (**inlined naive open**: `ControlPlaneView.open(..., ensure_topic=False)` + **teardown-guarded `start()`**; no `client._provisioning`), `get_agents`/`get_tools` (health-by-`reason` then project), `aclose` (best-effort).
**Tests** (inject a stub opener + `_FakeTable`): first `get_*` opens once, subsequent reuse; concurrent first calls of one kind → one open; two kinds open concurrently (no cross-serialization); failed open not retained (retried); `get_*` projects to a read-only `Mapping`; **`_open` passes `ensure_topic=False`** (the observer never ensures).
- **Health (by `reason`):** `failure` set → `MeshUnavailableError(reason="reader_dead")` **and the cell is retained** (a second `get_*` re-raises `reader_dead` with **no** re-open — assert a single open); `failure`-before-`is_caught_up` order (a `failure`-set + `is_caught_up=True` view → `reader_dead`, not `establishing`); `is_caught_up=False` → `reason="establishing"`; **self-heal** (flip `is_caught_up=True` → next `get_*` returns); caught-up + empty → empty `Mapping`; open raises → `reason="open_failed"` (cell evicted → retried); `server_urls is None` → `ValueError` synchronously.
- **Cancel-safety (the round-3 CRITICAL):** a waiter cancelled (`asyncio.wait_for`) while awaiting a shared open does **not** cancel the open, **not** poison a co-waiter, **not** evict the still-running task (no double-open); a cancelled in-flight open leaves **no** orphaned consumer (stub asserts `stop()` ran); the `done`-callback eviction fires on the task's own failure (a fake AttributeError-free callback); `aclose()` cancels in-flight + stops completed (per-view `stop()` error logged); in-flight `get_*` during `aclose()` → `ClientClosedError` **or** a final snapshot; `get_*` after `aclose()` → `ClientClosedError`.

### Commit 4 — `Client.mesh` + `connect(mesh_config=)` + `aclose` wiring
`Client.mesh` cached-singleton property; `connect(mesh_config=None)` stored; `__init__` inits `_mesh`/`_mesh_config` to `None`; `aclose()` closes the mesh (guarded) then the broker (always).
**Tests:** `client.mesh` is zero-I/O + identity-stable; `get_*` binds the right topic/record/projector and opens **`ensure_topic=False`** (no `_provisioning` consult); `server_urls is None` → `ValueError`; directly-built client (`Client(...)` not `connect`) — `client.mesh` doesn't `AttributeError`; opening a view does not start `client._broker` (inbox ⊥); `aclose()` tears mesh down then broker even if mesh teardown raises; lazy-import assertion.

### Commit 5 — Exports
`client/__init__.py` + top-level `__init__.py`: `Mesh`, `MeshViewConfig`, `AgentInfo`, `ToolInfo`, `ToolNodeInfo`, `ToolboxInfo`, `ToolSpec`. Wire records/topics stay unexported.
**Tests:** `from calfkit import Mesh, AgentInfo, ToolInfo, …` resolves; `AgentCard`/`CapabilityRecord` remain non-exported.

### Commit 6 — Kafka integration lane (`@pytest.mark.kafka`)
`tests/integration/test_mesh_view_kafka.py`. A real `Worker` advertises an `Agent` + a function tool node + an MCP toolbox; a `Client.connect(bootstrap)`:
- `get_agents()` returns the agent online with the right `AgentInfo`; `get_tools()` projects the tool node → `ToolNodeInfo` and the toolbox → `ToolboxInfo` (bare names);
- repeated `get_agents()` reuses one consumer (assert a single open).
- **Missing-topic → `reason="open_failed"`** must run on the **no-auto-create** lane (`integration-topic-provisioning.yml` / `test_topic_provisioning.py`), **NOT** `@pytest.mark.kafka`: the main lane is Redpanda `dev-container` (topic auto-create ON), where the naive consumer's metadata request auto-creates `calf.agents` → `get_*` returns an **empty** `Mapping`, not `open_failed` (the documented auto-create caveat, ADR-0029 / spec §6.3). Keep the worker-advertises-then-read happy path on the main `kafka` lane.

## 5. Verification & quality gates

- `make fix && make check` clean at every commit.
- **100% coverage** on `calfkit/client/mesh.py` (offline lane); `# pragma: no cover` only for real-`open()` lines exercised in the kafka lane.
- `import calfkit.client.mesh` pulls no aiokafka/ktables (lazy-import test); node/agent layers stay ktables-free.
- Full offline suite + kafka lane green. ADR-0028/0029 flip `proposed → accepted` at merge.
- Docs (how-to + `api.md` entry for `client.mesh`) are a follow-up doc pass once the surface lands.

## 6. Risks & watch-items

- **Missing-topic error quality.** The fail-loud `reason="open_failed"` message must be actionable ("the directory topic isn't present yet — it's created when an agent/tool comes online, or provision it via ops") — contextualize the cause, don't string-match it. The observer never creates the topic (naive open). Verify the message in the kafka lane.
- **Cache cancellation-safety (the round-3 CRITICAL).** Waiter `await` must be `asyncio.shield(task)`, eviction must key on the task's own terminal state (done-callback), and the open must be teardown-guarded — else a waiter's `wait_for` cancel kills the shared open / double-opens / leaks a consumer. Cancel/await outside the lock; a `view.stop()` that raises must not skip the others or the broker close. Covered by the Commit 3 cancel-safety tests.
- **Residual cancel leak (upstream).** A cancellation landing *inside* aiokafka's `consumer.start()` — before ktables records `_consumer` — still orphans the consumer (`view.stop()` finds nothing to stop). A pre-existing ktables/aiokafka gap, newly *reachable* here only via `aclose`-cancel; low-probability, owned upstream (the worker never cancels `start()`). Note it; don't paper over it caller-side.
- **Transient outage → stale-but-no-raise.** Post-establishment broker blips keep the cached reader `caught_up` serving frozen state, ageing to empty via staleness (no raise). Documented (spec §6.3/§6.4); gate trust on `last_seen`, not on a read succeeding.
- **Empty Mapping ambiguity.** An empty `get_agents()` means "no agents online" (the topic exists). "Topic missing" is a *raise*, not an empty — so the two are distinguishable, unlike the rejected graceful-empty design.
- **Security-deferred reach.** On a SASL/SSL cluster the consumer can't auth (spec §9.2); kafka lane runs unsecured; document in the follow-up how-to.

## 7. References

- Spec: [`caller-side-mesh-view-spec.md`](caller-side-mesh-view-spec.md) · ADR-0028 · ADR-0029
- Substrate: [`control-plane-substrate-spec.md`](control-plane-substrate-spec.md) · ADR-0010/0011
- Sibling: [`client-caller-surface-spec.md`](client-caller-surface-spec.md) (TDD staging precedent)
- [CONTEXT.md](../../CONTEXT.md) glossary · [Issue #301](https://github.com/calf-ai/calfkit-sdk/issues/301)
