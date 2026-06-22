---
status: accepted
---

# Worker-level ktable reader tuning via a shared cadence value object

calfkit opens [ktables](https://github.com/ryan-yuuu/ktables) readers for two substrates — the
control-plane **capability view** (`ControlPlaneView` over a `GroupedKafkaTable`) and each
fan-out agent's **durable batch store** (`KtablesFanoutBatchStore` over two `KafkaTable`s). Both
ran at ktables' default reader cadence (`poll_timeout_ms=200`, `fetch_max_wait_ms=500`) with no
way for a developer to change them. ktables' benchmark shows an idle `barrier()` costs
`≈ max(fetch_max_wait_ms, poll_timeout_ms)` — ~500 ms by default, ~30 ms with both lowered —
which lands directly in tool/MCP/agent-mesh discovery latency against the (quiet) capability view.

This ADR records the decision to surface those two **reader-only** cadence knobs at the
**worker level**, modeled as one shared value object **composed per subsystem**, and to make the
ktables-backed configs Pydantic models.

- `KTableReaderTuning(poll_timeout_ms, fetch_max_wait_ms)` — the cadence pair, defined and
  validated once.
- `ControlPlaneConfig` (existing, `Worker(control_plane=...)`) and the new `FanoutConfig`
  (`Worker(fanout=...)`) each hold their **own** `reader_tuning: KTableReaderTuning`. A node
  constructor never carries a ktable knob (config is a worker/deployment concern).

## Notes

- **Per-subsystem, not one worker-global instance.** The capability view is *quiet* (heartbeats
  every `heartbeat_interval`, so an idle barrier sits on the ~500 ms floor) while the fan-out
  `state` table *churns* during an aggregation (barriers already ~1 ms). Their optimal cadence is
  opposite, so each composes its own tuning rather than sharing one knob.
- **`None` means "ktables' default".** A `None` knob is omitted from the `*.json(...)` factory
  call (`as_kwargs()` = `model_dump(exclude_none=True)`), so ktables stays the single source of
  truth for the default value — no duplicated 200/500 to drift.
- **Asymmetric siblings.** `ControlPlaneConfig` and `FanoutConfig` share only `reader_tuning`.
  The control-plane config keeps `heartbeat_interval`/`stale_after`/`bootstrap_servers` (a
  heartbeated, splittable presence plane); `FanoutConfig` has `barrier_timeout` (read-your-own-
  writes) and no `bootstrap_servers` (fan-out tables ride the client's data cluster). Two configs
  composing one cadence object, not one merged config.
- **Two smells folded in.** `FanoutConfig` gives the fan-out store's previously *dead*
  `catchup_timeout` constructor parameter a real source, and replaces its hardcoded
  `_BARRIER_TIMEOUT_S = 30.0` with `barrier_timeout`.
- **Pydantic, scoped to the ktables configs.** The three ktables-backed configs
  (`KTableReaderTuning`, `FanoutConfig`, `ControlPlaneConfig`) are Pydantic `BaseModel`s
  (`frozen=True, extra="forbid"`) with declarative constrained types
  (`Field(ge=1, strict=True)` for the ms ints, `Field(gt=0, allow_inf_nan=False)` for the floats)
  rather than stdlib dataclasses with hand-rolled `__post_init__`. This is the better-engineering
  default ([engineering-over-legacy]); non-ktables configs (`ProvisioningConfig`, …) are out of
  scope and unchanged.

## Consequences

- **Behavior-preserving at defaults.** With no config supplied, `as_kwargs()` is empty and
  `barrier_timeout` is 30.0 — identical to the prior hardcoded behavior. An explicit
  invariant test pins this.
- **`ControlPlaneConfig` becomes keyword-only.** Converting it from a `@dataclass` to a
  `BaseModel` means positional construction (`ControlPlaneConfig(30.0, …)`) no longer works;
  the keyword form (`ControlPlaneConfig(heartbeat_interval=30.0, …)`) is the idiomatic one. A
  clean pre-1.0 break, consistent with the project's hard-breaks-over-compat stance. Validation
  failures raise `pydantic.ValidationError`, which subclasses `ValueError`, so existing
  type-based `pytest.raises(ValueError)` still holds.
- **New public surface.** `KTableReaderTuning` and `FanoutConfig` are exported from the top-level
  `calfkit` package alongside `ControlPlaneConfig`.
