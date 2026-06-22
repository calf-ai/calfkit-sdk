# Spec: parallelize the real-broker (`kafka`) integration lane

Status: **proposed** (design converged 2026-06-22). Test-infrastructure change; no production
code changes.

## 1. Summary

Run the real-broker (`kafka`) integration tests **in parallel against one shared Redpanda
broker** via `pytest-xdist`, and apply a **preset low-latency ktable config** to the
capability-plane suites. Target: cut the lane from **~3m41s serial** toward **well under a
minute**.

The per-test `topic_namespace` fixture — plus the unique, namespaced node names/ids and
namespace-filtered assertions the suite **already** uses to coexist on one session broker —
makes the tests safe to run concurrently on a single broker. No test rewrites are needed.

## 2. Baseline & motivation

Measured (`pytest -m kafka --durations`, 2026-06-22): **65 passed, 6 skipped, 221.65s
(3m41s)**, serial. Slowest single test 14.8s; flat distribution (most 3–8s multi-hop
round-trips) — the profile where parallelism dominates. The broker is already well-tuned
(testcontainers runs Redpanda `--mode dev-container --smp 1 --memory 1G`, session-scoped), so
the time is in **serial execution** + **per-test round-trip / convergence latency**, not
container churn.

Two levers: **(1) parallelism** (dominant) and **(2) per-test ktable convergence latency**
(now tunable since the worker-level reader-tuning shipped in #277).

## 3. Goals / non-goals

**Goals**
- Run the lane in parallel on **one** shared broker; `pytest -m kafka -n auto` works standalone.
- Broker config (image / smp / memory) and worker count are **env/CLI-overridable**.
- Preserve the existing `CALF_TEST_KAFKA_BOOTSTRAP` external-broker path and the graceful
  Docker-absent skip.
- Cut per-test convergence latency with a shared low-latency ktable test config.
- Zero change to what the tests assert.

**Non-goals**
- Rewriting tests or namespacing `CAPABILITY_TOPIC` (not needed — see §5; the suite already
  coexists on the shared `calf.capabilities` topic).
- The offline lane (already ~9s) or any production code.
- CI runner sizing (ops concern; the lane just needs to *support* parallelism).

## 4. Locked design decisions

| # | Decision |
|---|---|
| D1 | `pytest-xdist`; one Redpanda shared across all workers. |
| D2 | Shared-broker lifecycle via the **xdist controller hook** (`pytest_configure_node` injects the bootstrap address into each worker's `workerinput`; `pytest_unconfigure` tears the container down). Race-free, no file-lock / ref-count. The non-xdist path starts its own container (today's behavior). `CALF_TEST_KAFKA_BOOTSTRAP` short-circuits both. |
| D3 | Broker config is **env-overridable**: image, smp, memory (via a thin `RedpandaContainer` subclass overriding `tc_start()`, since upstream hardcodes `--smp 1 --memory 1G`). Worker count via a Makefile var → `-n`. |
| D4 | A **preset low-latency ktable config** fixture (`reader_tuning` poll=20ms/fetch=10ms + `heartbeat_interval=0.1`) replaces the discovery/MCP suites' `heartbeat_interval=5.0` + default-cadence configs. |
| D5 | **No preemptive capability isolation.** The suite already coexists on `calf.capabilities` (unique namespaced node names + namespace-filtered enumeration assertions); parallelize and verify empirically, fixing only what actually flakes. |

## 5. Why the shared `calf.capabilities` topic is already safe (and what to watch)

`calf.capabilities` (`CAPABILITY_TOPIC`) is a fixed, cluster-wide topic — **not** covered by
`topic_namespace`. Under parallelism, every test's tool records land there and every agent's
view reads the whole topic. A test would break **only** if it asserted over the *whole plane*
("exactly N tools total", "snapshot length", "plane empty"). Verified (2026-06-22) that the
suite does **not**:
- `test_discover_mode_kafka` filters discover results to its own namespace
  (`mine = {n for n in pov if n.startswith(topic_namespace)}`), and its docstring explicitly
  notes other suites' tools coexist on the shared broker.
- `test_controlplane_substrate_kafka` uses `f"{topic_namespace}.presence"`, not the global topic.
- Fan-out tests key on namespaced node_ids (`calf.fanout.{topic_namespace}-…`).
- No unfiltered whole-plane count assertions exist.

This is unsurprising: the lane already shares one **session** broker across all serial tests, so
the authors already had to make tests coexist (records linger until compaction). Parallelism
just makes coexistence concurrent. **To watch:** `test_fanout_store_kafka.py:40` has a bare
`node_id="n"` (the real test cases use `f"{topic_namespace}-n"`); confirm it's a helper default
and namespace it if a real construction uses it.

## 6. The shared-broker mechanism (`tests/integration/conftest.py`)

Three-way resolution — external env > xdist-shared > local single — with the container owned by
the xdist controller:

```python
# xdist controller, once per worker node: start ONE Redpanda, hand its address to each worker.
def pytest_configure_node(node):
    if os.getenv("CALF_TEST_KAFKA_BOOTSTRAP"):
        return  # external broker; nothing to start
    node.workerinput["calf_kafka_bootstrap"] = _ensure_shared_redpanda(node.config)  # start-once, cached on config; None if Docker absent

def pytest_unconfigure(config):  # controller: stop the shared container if we started one
    _stop_shared_redpanda(config)

@pytest.fixture(scope="session")
def kafka_bootstrap(request) -> Iterator[str]:
    if (ext := os.getenv("CALF_TEST_KAFKA_BOOTSTRAP")):
        yield ext; return
    wi = getattr(request.config, "workerinput", None)
    if wi is not None and "calf_kafka_bootstrap" in wi:           # xdist worker
        addr = wi["calf_kafka_bootstrap"]
        if addr is None:
            pytest.skip("Docker not available for the shared Redpanda testcontainer")
        yield addr; return
    # not under xdist (-n0 / plain pytest): own a single container (today's path)
    with _redpanda_container() as addr:
        yield addr
```

- `_ensure_shared_redpanda(config)` is idempotent: start the container once, cache it on
  `config`, return the bootstrap (or `None` on `DockerException` so each worker skips cleanly).
- The controller process lives for the whole session, so the container stays up until
  `pytest_unconfigure`.
- `pytest_configure_node` is an xdist-only hook — harmless/uncalled when `-n` is absent, so the
  fixture's last branch preserves today's single-container behavior for plain `pytest`. It is
  declared with `@pytest.hookimpl(optionalhook=True)` so a plain (xdist-less) run doesn't trip
  pytest's unknown-hook validation.
- **The run must be scoped to `tests/integration`** (`make test-kafka` / `pytest tests/integration
  -m kafka …`). `pytest_configure_node` is a *controller*-only hook, and the controller only loads
  conftests on the invocation's path; a bare `pytest -m kafka` from the repo root would load this
  conftest in the *workers* (during collection) but never in the controller, so the hook would
  never fire and every test would skip. The fixture detects the resulting missing `workerinput`
  key (vs a `None` address) and skips with an actionable message.
- **Guard:** `pytest_configure_node` starts the broker only when the marker expression actually
  selects `kafka` (`"kafka" in markexpr and "not kafka" not in markexpr`), so a future *offline*
  parallel run (`pytest tests/ -n auto`, which loads this conftest and deselects kafka) does not
  wastefully spin up Redpanda.

**Measured (M3 Pro, 11 cores):** 221s serial → **~31–36s at `-n auto`** (≈6.2×), stable across
3 runs, 0 leaked containers, with `_DEFAULT_SMP=2` / `_DEFAULT_MEMORY=2G`.

## 7. Env-overridable broker config (`RedpandaContainer` subclass)

Upstream `RedpandaContainer.tc_start()` hardcodes `--mode dev-container --smp 1 --memory 1G`. A
thin subclass overrides just that line to read env values:

```python
class _TunedRedpanda(RedpandaContainer):
    def tc_start(self) -> None:
        smp    = os.getenv("CALF_TEST_REDPANDA_SMP", _DEFAULT_SMP)
        memory = os.getenv("CALF_TEST_REDPANDA_MEMORY", _DEFAULT_MEMORY)
        ...  # the upstream tc_start template with {smp}/{memory} substituted
```

Public knobs (all optional; defaults preserve a working lane):

| Env var | Default | Tunes |
|---|---|---|
| `CALF_TEST_KAFKA_BOOTSTRAP` | unset | reuse an external broker (no container) — existing |
| `CALF_TEST_REDPANDA_IMAGE` | the pinned `…/redpanda:v26.1.10` | image override |
| `CALF_TEST_REDPANDA_SMP` | `2` | Redpanda CPU cores |
| `CALF_TEST_REDPANDA_MEMORY` | `2G` | Redpanda memory |

Worker count is a CLI/Makefile concern (not an env knob): `make test-kafka KAFKA_XDIST=auto`
(default) → `pytest -n $(KAFKA_XDIST)`. Defaults (`smp=2`, `memory=2G`, `-n auto`) are a starting
point — **tuned empirically** during impl against the measured numbers, since parallelism shifts
the bottleneck from serial execution to broker throughput (N workers vs Redpanda's core/memory).

## 8. Preset low-latency ktable config

A shared fixture/factory in the integration conftest, so the cadence lives in one place:

```python
@pytest.fixture
def fast_control_plane():
    def _make(bootstrap: str, **overrides) -> ControlPlaneConfig:
        return ControlPlaneConfig(
            reader_tuning=KTableReaderTuning(poll_timeout_ms=20, fetch_max_wait_ms=10),
            heartbeat_interval=0.1, bootstrap_servers=bootstrap, **overrides,
        )
    return _make
```

Applied to the discovery/MCP suites that currently build
`ControlPlaneConfig(heartbeat_interval=5.0, …)` (e.g. `test_discover_mode_kafka`,
`test_tool_discovery_kafka`, `test_mcp_roundtrip_kafka`). Records then converge in tens of ms
instead of being gated by the ~500ms idle-barrier floor + 5s heartbeat. Behavior is unchanged —
only faster. (Fan-out tests barrier against a churning store, already ~1ms — left as-is.)

## 9. Makefile

`test-kafka` gains `-n $(KAFKA_XDIST)` (default `auto`); the help text documents the env knobs
from §7. The external-broker and `-n0` (serial) invocations remain valid.

## 10. Verification

- `make test-kafka` (parallel) run **3–5×** → all green, no flakes; record wall-clock vs the
  221s baseline.
- `make test-kafka KAFKA_XDIST=0` (serial / non-xdist path) → green.
- `CALF_TEST_KAFKA_BOOTSTRAP=<addr> make test-kafka` → uses the external broker, starts no
  container.
- No Docker → lane skips cleanly (controller-hook → `None` → per-worker skip).
- Tune `smp`/`memory`/`-n` empirically; record the chosen defaults. Audit `node_id="n"`.

## 11. Risks

- **Broker contention** under N workers → mitigated by the smp/memory bump + worker-count
  tuning; the defaults are a starting point, measured during impl.
- **Latent shared-state flakes** → mitigated by multi-run verification; the suite already
  coexists on the shared topics by design (§5).
- **Docker-absent under xdist** → handled by the `None`-sentinel skip path (§6).
- **`tc_start` override drift** vs upstream testcontainers → small, pinned dependency; acceptable,
  and isolated to one method.
- **CI**: the kafka CI job must invoke the parallel form and have a runner with enough cores to
  benefit (ops; out of scope here, but flagged).

## 12. Build order

1. Add `pytest-xdist` to the `integration` dependency group.
2. Env-overridable broker config: the `_TunedRedpanda` subclass + image/smp/memory env knobs.
3. The shared-broker controller-hook fixture (§6); verify `-n auto`, `-n0`, external-broker, and
   no-Docker paths.
4. The `fast_control_plane` fixture; apply to the discovery/MCP suites.
5. Makefile `-n $(KAFKA_XDIST)` + help/docs.
6. Empirical verification (multi-run, measure, tune defaults, fix any straggler).
