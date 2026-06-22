# Spec: worker-level ktable reader tuning (`KTableReaderTuning`, `FanoutConfig`)

Status: **proposed** (design converged 2026-06-22). Pre-1.0, additive.

## 1. Summary

Surface ktables' two reader-cadence knobs — `poll_timeout_ms` and `fetch_max_wait_ms`
— to developers at the **worker level**, for both ktables-backed substrates calfkit
opens: the control-plane **capability view** and each fan-out agent's **durable
batch store**.

The restructure introduces one shared value object and one new worker-level config,
and in doing so pays down two existing fan-out smells:

- a new shared `KTableReaderTuning` (the reader-cadence pair, defined + validated once);
- a new worker-level `FanoutConfig` (sibling to `ControlPlaneConfig`) that composes it
  and additionally surfaces the fan-out store's **`catchup_timeout`** (currently a *dead*
  constructor parameter) and **`barrier_timeout`** (currently the hardcoded
  `_BARRIER_TIMEOUT_S = 30.0`).

The developer surface is **worker-only**: node constructors (`Agent`, …) are untouched;
the config is wired into each node's store internally at resource-open time.

## 2. Motivation

ktables' `REPORT.md` (finding #1) shows an idle `barrier()` costs
`≈ max(fetch_max_wait_ms, poll_timeout_ms)` — ~500 ms at the stock 200/500 defaults,
collapsing to ~30 ms when both are lowered, and to ~1 ms on a churning table. calfkit
currently leaves both knobs at ktables' defaults **everywhere** and exposes neither.

The capability view is the high-value target: it is quiet between heartbeats (default
`heartbeat_interval = 30 s`) yet by-name tool / MCP / agent-mesh resolution does reads +
`barrier()` against it, so the ~500 ms idle floor lands directly in discovery latency.
The fan-out store's win is smaller (its `state` table churns during an aggregation → ~1 ms
barriers already), but wiring it *properly* is the occasion to remove the two smells above.

## 3. Goals / non-goals

**Goals**

- A single, validated definition of the reader-cadence knobs, composed by each
  reader-owning subsystem (control plane, fan-out) — no duplication, no cross-subsystem
  config reads.
- Worker-level developer surface for both planes; node constructors unchanged.
- Behavior-preserving at defaults (no config set ⇒ identical to today).
- Fold the dead `catchup_timeout` param and the hardcoded `_BARRIER_TIMEOUT_S` into the
  new `FanoutConfig`.

**Non-goals**

- Per-agent (node-constructor) tuning. Deliberately omitted — config is a
  worker/deployment concern, not a node-definition concern.
- Writer cadence / relaxing `enable_idempotence` (`acks=all`). Both planes hold
  correctness-critical state; durability is not a knob.
- Provisioning knobs (`num_partitions`, `topic_configs`) — governed by `ProvisioningConfig`
  / ops, not here.
- A `**kwargs` passthrough to aiokafka — ktables does not offer one.
- Changing any default value. This change only adds the *ability* to set the knobs.
- Migrating **non-ktables** configs to Pydantic. The Pydantic conversion is scoped to the
  ktables-backed configs (`KTableReaderTuning`, `FanoutConfig`, `ControlPlaneConfig`).
  `ProvisioningConfig` and the other stdlib `__post_init__` config dataclasses are **out of
  scope** and left as-is — a separate concern, not a follow-up this change implies.

## 4. Locked design decisions

| # | Decision |
|---|---|
| D1 | The cadence pair lives in a shared frozen value object `KTableReaderTuning`. |
| D2 | Each reader-owning subsystem **composes its own** instance (`ControlPlaneConfig.reader_tuning`, `FanoutConfig.reader_tuning`) — **not** one worker-global instance. Their workloads are opposite (quiet vs churning), so independent tuning is the correct default. |
| D3 | Knob names mirror ktables 1:1 (`poll_timeout_ms`, `fetch_max_wait_ms`); no calfkit aliases. |
| D4 | Default `None` ⇒ omit the kwarg ⇒ ktables owns the default value (single source of truth; no drift). |
| D5 | `catchup_timeout` stays a **flat** field on each config (a boot-gate, conceptually distinct from steady-state cadence); it is **not** folded into `KTableReaderTuning`. |
| D6 | `FanoutConfig` is a new worker-level arg `Worker(fanout=...)`, sibling to `control_plane=...`. |
| D7 | The two configs are intentionally **not symmetric** — they share only `reader_tuning`. `ControlPlaneConfig` keeps `heartbeat_interval`/`stale_after`/`bootstrap_servers` (heartbeated, splittable presence plane); `FanoutConfig` has `barrier_timeout` (RYOW) and **no** `bootstrap_servers` (fan-out tables ride the client's data cluster, derived internally). |
| D8 | **The three ktables configs are Pydantic `BaseModel`s (frozen, `extra="forbid"`), with declarative constrained types** — not stdlib `@dataclass` + hand-rolled `__post_init__`. This is the better engineering pattern (declarative validation, free error messages), adopted per the project's engineering-over-legacy priority. |
| D9 | **Scope is the ktables-backed configs only:** `KTableReaderTuning`, `FanoutConfig`, and `ControlPlaneConfig` (the control plane and fan-out store are both ktables-backed). `ControlPlaneConfig` is **converted** as part of this change so the two sibling worker configs share one idiom. **Non-ktables configs are explicitly untouched** — `ProvisioningConfig` (Kafka topic provisioning) and the other `__post_init__` validators keep their current form; migrating them is a separate concern, *not* part of this change. |

## 5. Public developer surface (the end state)

```python
from calfkit import Worker, ControlPlaneConfig, FanoutConfig, KTableReaderTuning

worker = Worker(
    client,
    nodes=[...],
    control_plane: ControlPlaneConfig | None = None,   # existing
    fanout:        FanoutConfig       | None = None,    # NEW
)
```

All three are Pydantic `BaseModel`s (frozen). Two shared constrained-type aliases carry the
positivity/finiteness rules declaratively (defined once in `calfkit/tuning.py`):

```python
from typing import Annotated
from pydantic import Field

PositiveTimeoutMs   = Annotated[int,   Field(ge=1, strict=True)]          # reject bool/float/str coercion
PositiveFiniteFloat = Annotated[float, Field(gt=0, allow_inf_nan=False)]  # reject 0 / -n / inf / nan
```

### `KTableReaderTuning` (new, shared)

```python
class KTableReaderTuning(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    poll_timeout_ms:   PositiveTimeoutMs | None = None   # None -> ktables default (200 ms)
    fetch_max_wait_ms: PositiveTimeoutMs | None = None   # None -> ktables default (500 ms)

    def as_kwargs(self) -> dict[str, int]:
        return self.model_dump(exclude_none=True)         # exactly the non-None cadence subset
```

### `ControlPlaneConfig` (existing — converted to BaseModel + one new field)

```python
class ControlPlaneConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    heartbeat_interval: PositiveFiniteFloat = 30.0
    stale_after:        PositiveFiniteFloat | None = None
    catchup_timeout:    PositiveFiniteFloat = 30.0
    bootstrap_servers:  str | None = None
    reader_tuning:      KTableReaderTuning = Field(default_factory=KTableReaderTuning)   # NEW (view cadence)
```

### `FanoutConfig` (new)

```python
class FanoutConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    reader_tuning:   KTableReaderTuning = Field(default_factory=KTableReaderTuning)   # the 2 store readers' cadence
    catchup_timeout: PositiveFiniteFloat | None = None                               # was a DEAD ctor param -> now live
    barrier_timeout: PositiveFiniteFloat = 30.0                                       # was hardcoded _BARRIER_TIMEOUT_S
```

### Worked examples

```python
# 1. Default — everything at ktables stock (200/500); identical to today.
Worker(client, nodes=[agent])

# 2. High-value: fast tool/MCP/mesh discovery (control plane only).
Worker(client, nodes=[agent],
    control_plane=ControlPlaneConfig(
        reader_tuning=KTableReaderTuning(poll_timeout_ms=20, fetch_max_wait_ms=10),
    ),
)

# 3. Both planes, tuned independently (opposite workloads).
Worker(client, nodes=[fanout_agent],
    control_plane=ControlPlaneConfig(
        heartbeat_interval=10.0, stale_after=30.0,
        reader_tuning=KTableReaderTuning(poll_timeout_ms=20, fetch_max_wait_ms=10),   # quiet -> low
    ),
    fanout=FanoutConfig(
        barrier_timeout=10.0,
        reader_tuning=KTableReaderTuning(fetch_max_wait_ms=50),                        # churns -> light touch
    ),
)
```

## 6. Internal data flow

```
developer ── Worker(control_plane=…, fanout=…)
                │  stored as self._control_plane / self._fanout
                ├─► _capability_view_resource ─► ControlPlaneView.open(reader_tuning=cfg.reader_tuning)
                │        └─► GroupedKafkaTable.json(**reader_tuning.as_kwargs())
                └─► (per fan-out agent) BaseAgentNodeDef._fanout_store_resource
                         reads self._worker._fanout
                         └─► KtablesFanoutBatchStore(reader_tuning=…, catchup_timeout=…, barrier_timeout=…)
                                  └─► KafkaTable.json(**reader_tuning.as_kwargs(), …)  ×2 readers
```

The agent reaches its hosting worker via the existing back-reference `node._worker`
(set in `Worker.register_handlers`, `worker.py:168`), the same seam
`_fanout_store_resource` already uses for `_derive_bootstrap_servers()`. The config is
read at **resource-open time** (worker boot), never at node construction — which is why
it can be worker-level rather than a node-ctor arg.

## 7. Module layout & exports

New module **`calfkit/tuning.py`** holds `KTableReaderTuning`, `FanoutConfig`, and the two
shared constrained-type aliases (`PositiveTimeoutMs`, `PositiveFiniteFloat`). The fan-out
machinery itself lives in the *private* `calfkit/nodes/_fanout_store.py`, so its public
config needs a public home. It is a leaf module (only `pydantic` + `typing`), so
`controlplane/config.py` importing `KTableReaderTuning` + `PositiveFiniteFloat` from it
introduces no cycle.

`ControlPlaneConfig` **stays** in `calfkit/controlplane/config.py` (it configures the whole
substrate, not just tuning; it is heavily referenced) but is **converted from a stdlib
`@dataclass` to a Pydantic `BaseModel`** (frozen) and gains
`reader_tuning: KTableReaderTuning`.

Exports added to **`calfkit/__init__.py`** (next to `ControlPlaneConfig`):
`KTableReaderTuning`, `FanoutConfig`.

## 8. File-by-file change map

1. **`calfkit/tuning.py`** *(new)* — the `PositiveTimeoutMs` / `PositiveFiniteFloat` aliases,
   `KTableReaderTuning` (BaseModel + `as_kwargs()`), `FanoutConfig` (BaseModel). `__all__`.
2. **`calfkit/controlplane/config.py`** — **convert `ControlPlaneConfig` from `@dataclass` to
   `BaseModel`** (frozen): drop `__post_init__` + the `math` import, express the existing
   positivity/finiteness rules via `PositiveFiniteFloat`, add the `reader_tuning` field
   (import `KTableReaderTuning`, `PositiveFiniteFloat` from `calfkit.tuning`). Keep
   `STALE_MULTIPLIER` and the docstring (extended).
3. **`calfkit/controlplane/view.py`** (`ControlPlaneView.open`, ~`:202-223`) — add
   `reader_tuning: KTableReaderTuning | None = None`; splat
   `**(reader_tuning.as_kwargs() if reader_tuning else {})` into `GroupedKafkaTable.json`.
4. **`calfkit/worker/worker.py`**
   - `__init__` (~`:61-110`): add `fanout: FanoutConfig | None = None`; store
     `self._fanout = fanout if fanout is not None else FanoutConfig()`.
   - `_capability_view_resource` (~`:203-212`): pass `reader_tuning=cfg.reader_tuning`.
5. **`calfkit/nodes/_fanout_store.py`** (`KtablesFanoutBatchStore`, ~`:255-345`)
   - ctor: add `reader_tuning: KTableReaderTuning | None = None` and
     `barrier_timeout: float = 30.0` (keep the existing `catchup_timeout`, now fed).
     `reader_kwargs = {"ensure_topic": True, **(reader_tuning.as_kwargs() if reader_tuning else {})}`;
     conditionally add `catchup_timeout`. Store `self._barrier_timeout`.
   - delete the `_BARRIER_TIMEOUT_S` class constant; `_await_fresh` uses
     `self._barrier_timeout`.
6. **`calfkit/nodes/agent.py`** (`_fanout_store_resource`, ~`:95-116`) — read
   `fcfg = worker._fanout`; pass `reader_tuning=fcfg.reader_tuning,
   catchup_timeout=fcfg.catchup_timeout, barrier_timeout=fcfg.barrier_timeout`.
7. **`calfkit/__init__.py`** — import + `__all__` for `KTableReaderTuning`, `FanoutConfig`.

The `FanoutBatchStore` Protocol and `tests._fanout_fakes.FakeFanoutBatchStore` are
**untouched** — tuning affects only the *real* store's construction, not the fold/close
logic, so the offline (fake-backed) unit lane is unaffected.

## 9. Validation (declarative, via constrained types)

No `__post_init__`. Constraints are expressed on the field types and enforced by Pydantic
at construction. Behavior **verified empirically against the installed pydantic 2.12.5**:

- `PositiveTimeoutMs = Annotated[int, Field(ge=1, strict=True)]` — accepts `int >= 1`;
  **rejects** `0`, negatives, and (because of `strict=True`) `bool` / `float` / `str`
  coercion. *(Verified: lax `ge=1` would coerce `True -> 1` and `"5" -> 5`; `strict=True`
  rejects them — the right contract for a config knob.)*
- `PositiveFiniteFloat = Annotated[float, Field(gt=0, allow_inf_nan=False)]` — accepts
  finite `> 0` (incl. an `int` like `30`, naturally coerced); **rejects** `0`, negatives,
  `inf`, `nan`. *(Verified: this is an exact replacement for the old
  `math.isfinite(x) and x > 0` guard — note plain `PositiveFloat` would let `inf` through.)*
- `model_config = ConfigDict(frozen=True, extra="forbid")` makes each model
  immutable/hashable (mutation raises `pydantic.ValidationError`) **and rejects unknown
  field names** — so a typo like `ControlPlaneConfig(heatbeat_interval=10)` raises instead of
  silently no-opping (Pydantic's default `extra="ignore"` would swallow it).
- Nested defaults use `Field(default_factory=KTableReaderTuning)`; `reader_tuning`
  validates recursively on construction.

Failure mode: invalid construction raises `pydantic.ValidationError`, which **is a subclass
of `ValueError`** in 2.12.5 *(verified)* — so existing `pytest.raises(ValueError)` assertions
on `ControlPlaneConfig` keep passing; only assertions on the **message text** change.

## 10. Behavior-preservation invariant

With no config supplied, defaults must reproduce today's behavior exactly:

- `ControlPlaneConfig().reader_tuning.as_kwargs() == {}` ⇒ view passes **no** cadence
  kwargs ⇒ ktables defaults (200/500), as today.
- `FanoutConfig().reader_tuning.as_kwargs() == {}` ⇒ store readers pass no cadence kwargs,
  as today.
- `FanoutConfig().catchup_timeout is None` ⇒ not passed ⇒ ktables default (30 s), as today.
- `FanoutConfig().barrier_timeout == 30.0` ⇒ identical to the deleted
  `_BARRIER_TIMEOUT_S = 30.0`.

This invariant is asserted by an explicit test (§11).

## 11. Test plan (TDD)

New `tests/test_tuning.py` (all reject-cases assert `pydantic.ValidationError`):
- `KTableReaderTuning`: `None`/`int >= 1` accepted; `0`/`-1`/`True`/`5.0`/`"5"` rejected
  (strict int); `as_kwargs()` returns only the non-`None` subset and `== {}` at defaults;
  frozen (mutation raises).
- `FanoutConfig`: `catchup_timeout` `None`/finite `>0` (rejects `0`/`-1`/`inf`/`nan`);
  `barrier_timeout` finite `>0`; defaults (`reader_tuning` empty, `catchup_timeout is None`,
  `barrier_timeout == 30.0`); frozen.

Extend existing suites (offline; patch the ktables factories — no broker):
- `tests/test_controlplane_config.py`: `reader_tuning` default + type; the now-`BaseModel`
  reject-cases still satisfy `pytest.raises(ValueError)` (ValidationError ⊂ ValueError), but
  any assertions on the **message text** are updated to Pydantic's format.
- `tests/test_controlplane_view.py` (or a wiring test): `ControlPlaneView.open` splats
  `reader_tuning.as_kwargs()` into a spy `GroupedKafkaTable.json`; default ⇒ no cadence
  kwargs. (This also covers the currently `# pragma: no cover` `open()`.)
- `tests/test_controlplane_worker_wiring.py` / `test_worker_capability_view.py`:
  `_capability_view_resource` forwards `cfg.reader_tuning`; `Worker(fanout=...)` stored,
  defaults to `FanoutConfig()`.
- `tests/test_fanout_store.py`: `KtablesFanoutBatchStore` forwards `reader_tuning.as_kwargs()`
  + `catchup_timeout` to **both** readers (patch `ktables.KafkaTable.json` /
  `KafkaTableWriter.json`); `_await_fresh` calls `reader.barrier(timeout=barrier_timeout)`.
- An agent-resource test: `_fanout_store_resource` reads `worker._fanout` and passes the
  three pieces (patch `KtablesFanoutBatchStore`).
- **Behavior-preservation test** (§10): default configs ⇒ no cadence kwargs reach ktables,
  `barrier_timeout == 30.0`.

Regression (must stay green): the offline fold/close suites over the fake, and the
kafka-lane `test_durable_fanout_e2e.py` + capability-view kafka tests (unchanged at
defaults). Latency itself is **ktables'** benchmark to prove — calfkit does not re-measure
it; at most a kafka-lane smoke that a tuned view/store still starts.

## 12. Docs

- `docs/dev/control-plane-substrate.md`: add `ControlPlaneConfig.reader_tuning` + the
  `max(fmw, poll)` idle-barrier note.
- A config/tuning reference (new or in the control-plane doc): the full surface from §5,
  incl. `FanoutConfig` + `KTableReaderTuning`, with the "tune the quiet plane, leave the
  churning one" guidance.
- `docs/api.md`: add the two new public types.
- Review (do not necessarily edit) the in-node fan-out spec passages that call
  `_BARRIER_TIMEOUT_S` hardcoded and "ops out of scope" — now configurable via
  `FanoutConfig`; note the drift.

## 13. ADR

Add **ADR-0021 — "Worker-level ktable reader tuning via a shared cadence value object."**
Captures: (a) reader cadence is a cross-cutting concern modeled as one `KTableReaderTuning`
composed per-subsystem (not global, not per-node); (b) `FanoutConfig` as a worker-level
sibling to `ControlPlaneConfig`; (c) node constructors stay free of ktable knobs;
(d) surfacing the previously-dead `catchup_timeout` and hardcoded barrier timeout;
(e) the ktables-backed configs are Pydantic `BaseModel`s with declarative constrained
types (engineering-over-legacy), scoped to the ktables configs only — non-ktables configs
(`ProvisioningConfig`, …) are untouched. Status **proposed**, flips to **accepted** on merge
(per repo convention).

## 14. Build order

Two stacked PRs (one logical change; split along the high-value/low-risk seam):

- **PR1 — control plane / view.** `calfkit/tuning.py` (`KTableReaderTuning`) +
  `ControlPlaneConfig.reader_tuning` + `view.open` + `_capability_view_resource` + exports +
  tests + docs + ADR-0021 (describing the whole shape). Independently shippable; delivers
  the high-value win.
- **PR2 — fan-out.** Add `FanoutConfig` to `calfkit/tuning.py` + `Worker(fanout=...)` +
  store ctor (`reader_tuning` / `barrier_timeout`, `catchup_timeout` now fed) + agent
  resource + delete `_BARRIER_TIMEOUT_S` + tests + docs. Pays down the two debt items.

(May also land as a single PR; the two-PR stack keeps review surfaces small and lets the
view ship without waiting on the fan-out wiring.)

## 15. Risks

- **Behavior drift at defaults** — guarded by the §10 invariant + test. None expected.
- **Public surface addition** — additive, non-breaking; pre-1.0.
- **`ControlPlaneConfig` dataclass → BaseModel conversion** — two behavior changes to audit:
  (1) **keyword-only construction** — Pydantic `BaseModel` does not accept positional args,
  so any positional `ControlPlaneConfig(30.0, …)` in code or tests must become keyword
  (the one production site, `worker.py:104`, is `ControlPlaneConfig()` — safe; audit tests).
  (2) **exception text** — reject-cases raise `ValidationError` (⊂ `ValueError`, so
  `pytest.raises(ValueError)` survives; message-text assertions update).
- **Coverage** — `ControlPlaneView.open` was `# pragma: no cover`; the new spy test covers
  the forwarding path (net coverage gain).

