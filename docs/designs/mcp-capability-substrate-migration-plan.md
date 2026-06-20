# MCP Capability Plane → Control-Plane Substrate: Migration Plan

**Status:** IMPLEMENTED + verified (2026-06-19) · **Branch:** `feat/mcp-capability-on-substrate` (off `main` @ `c77e2ec`) · ADR-0012

> Built TDD across the five units below. `make check` clean (lint + format + mypy, 66 source files); offline suite **3141 passed**; full kafka lane **59 passed / 6 pre-existing skips** on real Redpanda (incl. the parity + CRITICAL-3 capability tests and all 9 adapted MCP-roundtrip e2e tests). Decisions D1–D6 are recorded in ADR-0012.

This plan migrates the shipped, hand-rolled MCP capability discoverability
(`calfkit/mcp/mcp_toolbox.py` + `Worker._capability_view_resource`) onto the
generic **control-plane substrate** shipped in PR #256 (`calfkit/controlplane/`).
It is grounded in the **actually shipped** substrate API, not the older
`capability-plane-migration-and-ops-spec.md` / `node-presence-substrate-spec.md`,
which predate the substrate and describe an API that diverged (see §2).

---

## 1. Goal, non-goals, locked decisions

### Goal
Replace the toolbox's per-node ktable writer + heartbeat loop + tombstone and the
worker's flat `KafkaTable` reader with the substrate's `@advertises` factory +
worker-owned `ControlPlanePublisher` + `ControlPlaneView[CapabilityRecord]`,
fixing the three confirmed defects (CRITICAL-1/3/4) and deleting the duplicated
lifecycle machinery. The **agent-side selector API stays a near drop-in**.

### Non-goals (explicitly out of this PR)
- `PresenceRecord` / `calf.presence` plane — separate adopter.
- `calfkit nodes` ops CLI — separate.
- Removing the unused `Worker.name` — separate cleanup.
- A `publish_now()` push path — deferred (see decision D1).

### Locked decisions (confirmed with Ryan, 2026-06-19)
- **D1 — Propagation: pull-only for v1.** `tools/list_changed` updates the cached
  record; it lands at the next heartbeat tick (≤ `heartbeat_interval`, default 30 s).
  No `publish_now()`. Accepted latency regression vs. today's immediate re-publish.
- **D2 — Staleness: hard-filter.** A stale toolbox is invisible (`view.get()` → `None`)
  → `missing_toolbox` → degrade. No advisory "use last-known + warn" path. The old
  `stale_seconds` / `_STALE_LOG_AFTER_SECONDS` logic is removed.
- **D3 — Bundle the de-MCP renames.** It is already a clean wire break, so in the
  same PR: topic `mcp.capabilities` → `calf.capabilities`; fold `MCPDiscoveryConfig`
  into `ControlPlaneConfig` (delete the former); resource key
  `calfkit.mcp.capability_view` → `calfkit.controlplane.capability_view`.
- **D4 — Clean cut, one PR.** Pre-1.0 hard break; no dual-read shim. New worktree
  (this one); comprehensive plan (this doc) before any code.
- **D5 — Remove `strict` entirely.** The MCP-selector `strict` flag is deleted: from
  `MCPToolboxNode.select()`, the `MCPToolbox` handle, `resolve_capability`, and
  `SelectorResult`. Its only behavioral footprint was two raise sites in `agent.py`;
  both go, so an unresolved selection **always** warns + degrades. `MCPToolResolutionError`
  (raised only by those sites) is deleted too. `include` remains as the trust boundary
  (it pins which tool names an agent may see). Technically orthogonal, but folded in
  because D2 changes `strict`'s behavior and the migration already rewrites every file it lives in.
- **D6 — The view owns filtering; CRITICAL-4 is log-only.** The `ControlPlaneView`
  is the single owner of **both** staleness and schema-version filtering (it already
  does both, and logs each). So `SelectorResult` drops `stale_seconds` **and**
  `skipped_newer_schema`, and `resolve_capability` drops its `is_unsupported_schema`
  branch. The agent's CRITICAL-4 fix is **observation only**: `logger.warning` when
  `view.status` is `degraded`/`failed` or `view.failure` is set — it **never raises**
  on view health.

---

## 2. Reconciling the stale specs with what shipped

⚠️ Anyone implementing from `capability-plane-migration-and-ops-spec.md` or
`node-presence-substrate-spec.md` will write the **wrong** code. The shipped
substrate diverged:

| Old spec says | Shipped substrate reality |
|---|---|
| `ControlPlaneRecord` carries `node_id`/`worker_id` in the value | Identity is the **wire key only**, never in the value (`records.py:33-49` docstring is explicit) |
| Read API: `view.instances(node_id, include_stale=False)`, `is_online()` | Shipped view has **`get()` / `online_nodes()` / `snapshot()`** only; it **collapses to one record per node**, no per-instance access, no `include_stale` |
| `PresenceRecord` + `calf.presence` ship here | Neither shipped — substrate is pure machinery |
| Staleness is annotate-or-filter (two surfaces) | `get()` **always hard-filters** stale; one surface |

Net effect: the shipped collapsed `get()` view is actually **closer** to a drop-in,
because today's `resolve_capability` already calls `view.get(toolbox_id)`.

**Action:** these two specs must be marked superseded/reconciled by this plan in §10.

---

## 3. The three defects this migration closes (and how)

| Defect (confirmed live on `main`) | Root cause today | Fix via substrate |
|---|---|---|
| **CRITICAL-1 — replica flap** | Flat key = `toolbox_id`. Two workers hosting the same toolbox clobber each other's heartbeats (LWW); one's clean-shutdown `delete(toolbox_id)` blanks the toolbox cluster-wide while the other is alive. | Instance-keying (`group=node_id`, `member=worker_id`) → single-writer-per-key; one replica's tombstone only removes its own member; the view collapses survivors. |
| **CRITICAL-3 — heartbeat masks content staleness** | Single `published_at`, re-stamped every tick → can't distinguish "alive" from "tools fresh". | Split: `last_heartbeat_at` (worker stamps every tick) vs. node-tracked `content_updated_at` (bumped only on real tool change). |
| **CRITICAL-4 — frozen-view blindness** | Agent reads the table but never checks `status`/`failure`; a degraded/failed reader silently serves a frozen snapshot. | View **exposes** `status`/`failure`/`is_caught_up`; **agent is edited to log a warning when degraded/failed** (D6 — observe only, never raise). The swap alone does not close this — see §6.4. |

---

## 4. Current → target, at a glance

| Concern | Today (`main`) | Target |
|---|---|---|
| Record | `CapabilityRecord(BaseModel)`: `schema_version, toolbox_id, dispatch_topic, tools, published_at` | `CapabilityRecord(ControlPlaneRecord)`: inherits `started_at, last_heartbeat_at, heartbeat_interval, schema_version`; drops `toolbox_id` (now wire key); replaces `published_at` with `content_updated_at` |
| Write | Node owns flat `KafkaTableWriter` + `_heartbeat_loop` + `_tombstone_on_shutdown` | **Delete all of it.** One `@advertises("calf.capabilities", CapabilityRecord)` factory; worker-owned publisher heartbeats + tombstones |
| First list | `list_tools()` in `_publish_on_startup` (`after_startup`) | `list_tools()` in `_mcp_session` **resource setup** (runs before any `after_startup`), caches `_last_tools` + `_tools_changed_at` |
| `tools/list_changed` | re-list + **immediate** `writer.set` | re-list → update cache; propagates at next tick (pull, D1) |
| Read | flat `KafkaTable.json(...)` under `calfkit.mcp.capability_view` | `ControlPlaneView.open(topic="calf.capabilities", record_type=CapabilityRecord, ...)` under `calfkit.controlplane.capability_view` |
| Selector call site | `resolve_capability(view, id)` → `view.get(id)` | **unchanged** — `ControlPlaneView.get()` is a structural drop-in for the flat `.get()` |
| `strict` flag | `select(strict=…)` → 2 raise sites in `agent.py` | **deleted** (D5); unresolved always warns + degrades; `MCPToolResolutionError` deleted |
| Staleness | advisory log (`stale_seconds`) | view hard-filters (D2/D6); `stale_seconds` dropped from `SelectorResult`; agent stale log removed |
| Schema filter | `is_unsupported_schema` in `resolve_capability` | view filters + logs newer-schema (D6); `skipped_newer_schema` dropped from `SelectorResult`; resolver branch removed |
| CRITICAL-4 | not checked | agent **logs** when `view.status`/`view.failure` signal degraded/failed (D6 — never raises) |
| Config | `MCPDiscoveryConfig` (`Worker(mcp_discovery=...)`) | `ControlPlaneConfig` (`Worker(control_plane=...)`); `MCPDiscoveryConfig` deleted |

**Verified facts that make this safe:**
- `record.toolbox_id` (the value field) is **read nowhere** — only `SelectorResult.toolbox_id` is. Dropping it from the value is safe (`grep` across `calfkit/` confirms).
- `record.published_at` is read in **exactly one place** (`resolve_capability`'s `stale_seconds`), which D2 removes.
- Lifecycle phase order guarantees **resources set up before `after_startup`** (`worker/lifecycle.py:103`), so listing tools in the session resource populates the cache before the publisher's `start()` calls the factory.
- `GroupedKafkaTable` / `GroupedKafkaTableWriter` are present in pinned `ktables>=0.3.0` (verified at import).

---

## 5. The clean part vs. the friction (honest scoping)

**Genuinely clean (near drop-in):** the read/resolution path. `resolve_capability`
only calls `view.get(toolbox_id)`; `ControlPlaneView.get()` returns one collapsed
record. `MCPToolbox`, `record_to_bindings`, and the `ToolSelector` protocol shape are
untouched in behavior (`select()` loses only its `strict` param, D5).

**Real friction (must be handled, not hand-waved):**
1. **Pull latency (D1):** tool changes lag ≤ `heartbeat_interval`. Accepted.
2. **Staleness semantics change (D2):** stale toolbox disappears instead of being
   used-with-warning. Accepted; the 90 s default window (`3 × 30 s`) makes flapping unlikely.
3. **CRITICAL-4 needs an agent code edit** (§6.4) — log-only (D6), not auto-fixed by the swap.
4. **`ControlPlaneView` is not a `Mapping`.** Works at runtime (duck-typed `.get`);
   the `Mapping[str, Any]` annotation on `ToolSelector.resolve_tools` /
   `resolve_capability` becomes a lie. Introduce a tiny `CapabilityLookup` protocol
   (`get(node_id) -> CapabilityRecord | None`) and retype both sides.
5. **Factory must read cached state only.** It runs on the shared heartbeat loop;
   any I/O (e.g. `session.list_tools()`) there would block every other advert and
   re-rot CRITICAL-3. Contract: return `self._last_tools` + `self._tools_changed_at`,
   never `now()`. Enforced by a focused test, not by the substrate.

---

## 6. Detailed change list (per file)

### 6.1 `calfkit/models/capability.py`
- **`CapabilityRecord(BaseModel)` → `CapabilityRecord(ControlPlaneRecord)`.**
  - Remove `toolbox_id` (wire key now) and `published_at`.
  - Add `content_updated_at: AwareDatetime`.
  - Keep `dispatch_topic`, `tools`, `schema_version: int = CAPABILITY_SCHEMA_VERSION`.
  - `CAPABILITY_SCHEMA_VERSION = 1` — fresh topic, fresh schema lineage (recommend 1, not 2; old `mcp.capabilities` is abandoned and unreadable by the new codec anyway).
  - Inherits `frozen=True` from the base — fine (the node no longer mutates a cached record; the factory builds fresh each tick).
- **New constants:**
  - `CAPABILITY_TOPIC = "calf.capabilities"` (the `@advertises` topic + the view topic).
  - `CAPABILITY_VIEW_RESOURCE_KEY = "calfkit.controlplane.capability_view"` (re-namespaced, D3).
- **`resolve_capability`** simplifies to "is it here, and does it have the tools I asked for" (the view owns staleness + schema filtering, D6):
  - `view.get(toolbox_id)` → `None` → `missing_toolbox` (now also covers stale & newer-schema, since the view filters and logs both).
  - record → `record_to_bindings` → `invalid_record` on expansion failure; `include` filter → `missing_tools`.
  - **Remove** the `strict` param (D5), the `is_unsupported_schema`/`skipped_newer_schema` branch (D6), and the `stale_seconds` computation (D2/D6; `published_at` is gone).
  - Retype `view` param: `CapabilityLookup` (new protocol) instead of `Any`/`Mapping`.
- **`is_unsupported_schema`:** delete — its only caller (`resolve_capability`) is gone, and the view's `schema_version > reader_version` check (`view.py:116`) is now the single owner. (The module docstring's "skipped with a log by the consumer" line must be updated to point at the view.)
- **`CapabilityLookup` protocol (new):** `def get(self, node_id: str) -> CapabilityRecord | None: ...` — satisfied by `ControlPlaneView[CapabilityRecord]` and by a plain dict (test strategy).

### 6.2 `calfkit/models/tool_dispatch.py`
- `ToolSelector.resolve_tools(view: Mapping[str, Any])` → `view: CapabilityLookup`
  (recommend the protocol for type honesty; structural `Any` is the fallback).
- `SelectorResult`: drop **`strict`** (D5), **`stale_seconds`** (D2/D6), and
  **`skipped_newer_schema`** (D6). Keep `toolbox_id`, `bindings`, `missing_toolbox`,
  `missing_tools`, `invalid_record`. `unresolved` becomes
  `missing_toolbox or bool(missing_tools) or invalid_record`.

### 6.3 `calfkit/mcp/mcp_toolbox.py` (`MCPToolboxNode`)
**Delete:**
- `_capability_writer` resource + `_writer_resource_key`.
- `_publish_on_startup` + its `self.after_startup(...)` registration.
- `_heartbeat_loop`.
- `_tombstone_on_shutdown` + its `self.on_shutdown(...)` registration.
- `_last_record`, `_shutting_down` (no node-side writes remain → no resurrection risk to guard; that is now the worker publisher's job).
- `_discovery` property, `_control_plane_bootstrap`, `_provisioning_enabled` (bootstrap/provisioning now worker-owned for the writer).

**Add / change:**
- State: `self._last_tools: list[CapabilityToolDef] | None = None`, `self._tools_changed_at: AwareDatetime | None = None`.
- `_mcp_session` resource: after `session.initialize()`, `await self._refresh_tools(session)` to populate the cache **before** yielding (fail-loud here = abort boot, same as today).
- `_refresh_tools(session)`: `listing = await session.list_tools()`; set `_last_tools = [...]`; set `_tools_changed_at = now()` (this is a genuine content-change moment — startup or `tools/list_changed` — so `now()` is correct **here**, never in the factory).
- `@advertises(topic=CAPABILITY_TOPIC, record=CapabilityRecord)` factory:
  ```python
  @advertises(topic=CAPABILITY_TOPIC, record=CapabilityRecord)
  def _capability_record(self, stamp: ControlPlaneStamp) -> CapabilityRecord:
      assert self._last_tools is not None and self._tools_changed_at is not None  # session resource populates first
      return CapabilityRecord(
          **stamp.model_dump(),
          dispatch_topic=self.subscribe_topics[0],
          tools=self._last_tools,
          content_updated_at=self._tools_changed_at,
      )
  ```
- `_handle_tools_list_changed`: re-list via `_refresh_tools` (updates cache + bumps `content_updated_at`); **no `writer.set`** (pull, D1).
- `_mcp_message_handler` / `_relist_tasks`: keep the deadlock-avoiding offload + task tracking, but only to **cancel cleanly at session teardown** — drop the `_shutting_down`/tombstone-resurrection logic.
- `resolve_tools(view)` retyped to `CapabilityLookup`; behavior unchanged.
- `select(*, include=None)`: **drop the `strict` param** (D5). The `MCPToolbox` handle
  loses its `strict` field too; `MCPToolbox(name, include=…)` and the `resolve_capability`
  call shed `strict`. `include` (the trust boundary) is unchanged.

### 6.4 `calfkit/nodes/agent.py`
- `_resolve_selector_tools`:
  - **CRITICAL-4 (D6, log-only):** after fetching `view`, `logger.warning(...)` when
    `view.status` is `degraded`/`failed` or `view.failure` is set (today this is totally
    silent). **Never raises** on view health — purely observational.
  - **Delete the `strict` raise path (D5):** the no-view branch (`agent.py:222-227`) now
    always `logger.warning` + degrades (no `MCPToolResolutionError`); the
    `result.strict and result.unresolved` raise (`agent.py:241-242`) is removed — an
    unresolved selection always warns + degrades.
  - **Remove** the `stale_seconds` warning block + `_STALE_LOG_AFTER_SECONDS` (D2/D6;
    the view hard-filters, so a returned record is never stale; `content_updated_at` is a
    "last changed" timestamp, **not** a staleness signal — a stable toolbox legitimately
    never changes, so warning on its age would be a false alarm).
- `_maybe_resolve_selectors`: the override-skip comment ("a strict selector must not be
  able to fail a turn") loses its `strict` rationale; the skip itself stays (overrides
  still pin the exact tool surface).
- `import` updates: drop `MCPToolResolutionError`; `CAPABILITY_VIEW_RESOURCE_KEY` still
  imported from `models.capability` (now the re-namespaced value).

### 6.8 `calfkit/exceptions.py`
- **Delete `MCPToolResolutionError`** — raised only by the two `strict` sites in
  `agent.py` (`grep` confirms no other raiser), so it is dead after D5. Drop it from any
  `__all__`/exports.

### 6.5 `calfkit/worker/worker.py`
- `_capability_view_resource`: swap `KafkaTable.json(...)` → `ControlPlaneView.open(bootstrap_servers, topic=CAPABILITY_TOPIC, record_type=CapabilityRecord, catchup_timeout, ensure_topic, stale_after)`; `await view.start()` (boot gate preserved) / `yield` / `await view.stop()`. Read config from `self._control_plane` (was `self._mcp_discovery`).
- `_maybe_register_capability_view`: trigger unchanged (any node with `_tool_selectors`); only the resource body and key change. The **writer** is already auto-wired by the shipped `_maybe_register_control_plane` (triggered by `@advertises`) — independent trigger, no change needed there.
- `__init__`: remove the `mcp_discovery` param + `self._mcp_discovery`; keep `control_plane: ControlPlaneConfig`.
- Imports/exports: drop `MCPDiscoveryConfig`.

### 6.6 `calfkit/worker/worker_config.py`
- **Delete `MCPDiscoveryConfig`** (D3 fold). All four fields are covered by
  `ControlPlaneConfig` (`heartbeat_interval`, `catchup_timeout`, `stale_after`,
  `bootstrap_servers`); `topic` moves to the `@advertises` decorator constant.

### 6.7 Top-level exports (`calfkit/__init__.py`, `calfkit/mcp/__init__.py`)
- Remove `MCPDiscoveryConfig` from public exports; ensure `ControlPlaneConfig` is
  exported (it is, via the substrate PR). Confirm `MCPToolbox`/`MCPToolboxNode` exports unchanged.

---

## 7. TDD test plan

**Offline (dict-backed fakes, no broker) — write tests first:**
1. `CapabilityRecord` is a `ControlPlaneRecord` subclass; `schema_version` default present (else `ControlPlaneView` ctor raises `RegistryConfigError`); tolerant reader ignores extras; round-trips.
2. `_capability_record` factory: splatting the stamp populates `started_at`/`last_heartbeat_at`/`heartbeat_interval`; returns cached tools + `content_updated_at`.
3. **CRITICAL-3 split:** two factory calls with different `last_heartbeat_at` but unchanged tools → identical `content_updated_at`; after `_refresh_tools` → `content_updated_at` advances. (This is the "never `now()` in the factory" guard.)
4. `_refresh_tools` populates the cache from a fake session; `tools/list_changed` offloads a re-list that updates the cache (no write).
5. `resolve_capability` over a `ControlPlaneView` backed by a dict `GroupedTableReader` fake:
   - live record → bindings; **stale** record → `view.get()` None → `missing_toolbox` (D2); `include` filter → `missing_tools`; empty `dispatch_topic` → `invalid_record`.
6. **CRITICAL-4 (log-only, D6):** agent resolution with a fake view whose `status`/`failure` signal degraded/failed → warning emitted; **assert the turn still proceeds** (no raise).
7. Worker wiring: hosting an `MCPToolboxNode` registers a `calf.capabilities` writer (`_maybe_register_control_plane`); hosting an agent-with-selectors registers the `ControlPlaneView` resource under the new key; node layer stays **ktables-free at import**.
8. `MCPToolboxNode` no longer registers `_capability_writer`/capability `after_startup`/`on_shutdown` (assert only the session resource + the advert remain).
9. **`strict` removed (D5):** unresolved selection (missing toolbox / missing tools) → warns + degrades, **never raises**; no-view → warns + degrades. (`MCPToolResolutionError` no longer importable.) Replaces the old strict-raise tests in `test_tool_selector.py` / `test_mcp_toolbox.py`.

**Kafka lane (real Redpanda, mirrors the substrate's lane):**
1. **Parity:** toolbox worker up → record on `calf.capabilities`; agent worker up → catch-up gate passes → resolves the same tools as pre-migration.
2. **CRITICAL-1 replica regression:** two workers host the same toolbox name; one shuts down cleanly; the survivor's record keeps the toolbox resolvable.
3. **Pull propagation (D1):** change the server's tools → within ≤ `heartbeat_interval` the view reflects the new set.
4. **Tombstone:** clean shutdown removes the `(toolbox, worker)` instance key; sole instance → toolbox offline → agent degrades (warns, no raise).
5. **CRITICAL-4 loud failure:** view over an unreachable broker fails loud at `start()` (already asserted by the substrate lane; re-assert at the capability layer).

ktables' own semantics (catch-up gate, LWW, tombstones, grouped codec) are **not**
re-tested here — only the calfkit policy layers.

---

## 8. Build order (ordered commits, one PR)

1. **Record + constants + resolver + `strict`/filter removal** (§6.1, §6.2): new `CapabilityRecord`, `CapabilityLookup`, simplified `resolve_capability`, slimmed `SelectorResult` (drop `strict`/`stale_seconds`/`skipped_newer_schema`) + offline tests 1, 5.
2. **Toolbox write side** (§6.3) → `@advertises` + `_refresh_tools`; delete writer/heartbeat/tombstone; drop `select(strict=…)` + offline tests 2, 3, 4, 8.
3. **Worker reader + config fold** (§6.5, §6.6, §6.7) + offline test 7.
4. **Agent CRITICAL-4 (log-only) + strict-raise & stale-log removal** (§6.4) + delete `MCPToolResolutionError` (§6.8) + offline tests 6, 9.
5. **Kafka-lane integration** (§7) — tests 1–5.
6. **Docs + ADRs + `make fix && make check`** (§10).

Each step is red→green→refactor. `make check` must stay clean (lint+format+type) at every commit boundary.

---

## 9. Risks & open questions

- **Resolved:** the failed-view policy is log-only (D6); `strict` is removed (D5);
  `stale_seconds` and `skipped_newer_schema` are both dropped (the view owns filtering, D6).
- **Public-API break beyond the wire:** D5 removes the `strict` kwarg from
  `MCPToolboxNode.select()` / `MCPToolbox` and deletes `MCPToolResolutionError`. Pre-1.0,
  acceptable; ensure no example/doc/README still passes `strict=` or catches the exception.
- **Coordination with other live worktrees:** `feat/run-handler-unification` (folds `run()` into `@handler` — MCP already uses `@handler("*")`, low risk) and `test/mcp-roundtrip-e2e` touch MCP; rebase order matters. `calf-sdk-ktables-0.3.0` is moot — `main` already pins `ktables>=0.3.0`.
- **Partition count for `calf.capabilities`:** recommend 1 (total order per key is free at this cardinality).
- **`started_at` semantics:** now the worker's serving-start (shared by all this worker's records), vs. today's per-toolbox connect time. Acceptable; document.

## 10. Docs & ADRs (PR final steps)

- **Update** `docs/designs/mcp-capability-discovery-spec.md` to describe the substrate-backed plane (writer = `@advertises` factory; reader = `ControlPlaneView`; pull-only; hard-filter).
- **Reconcile / supersede** `capability-plane-migration-and-ops-spec.md` and the
  presence-spec API mismatches called out in §2 (point them at this plan + the shipped substrate).
- **ADRs:** confirm ADR-0010 / ADR-0011 flip proposed→accepted (the substrate PR may already track this). Evaluate a short ADR (or amend the migration spec) recording the locked decisions: **pull-only propagation** (D1), **hard-filter staleness** (D2), **`calf.capabilities` rename + `MCPDiscoveryConfig` fold** (D3), **`strict` removal** (D5), **view-owned filtering + log-only CRITICAL-4** (D6).
- **CONTEXT.md:** ensure the capability-plane vocabulary matches (topic name, "content currency" vs "liveness").
- **Prune** stale comments referencing `mcp.capabilities` / `published_at` / `MCPDiscoveryConfig`.
