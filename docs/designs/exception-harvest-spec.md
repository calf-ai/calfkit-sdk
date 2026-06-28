# Structured Exception Harvest Spec

- **Status:** Proposed (design settled with the user, 2026-06-27; **review round 1 folded**). Not yet implemented. Decision recorded in **ADR-0024**.
- **Date:** 2026-06-27
- **Depends on:** the fault rail (`ErrorReport` / `FaultMessage` / the `_fault_from_exception` chokepoint — ADR-0003/0004/0006, shipped PR #247) and the reply-slot wire model (ADR-0006). This feature is **additive** to that machinery — it adds one typed field and enriches one synthesis path; it introduces no new topic, header, or routing. (It does **change the value** carried on the existing `x-calf-error-type` header — the rename, §9.)
- **Relationship to prior specs:** extends `docs/designs/fault-rail-and-policy-seams-spec.md` §4.3/§6.7 (the typed fault value and the arbitrary-exception → catch-all synthesis). It does **not** change how faults travel, escalate, or are received.

---

## 1. Summary

When an arbitrary exception escapes a node's code uncaught (e.g. a provider `ModelHTTPError`, a `KeyError`, a bug), the rail synthesizes a fault from it via `ErrorReport.from_exception` (`calfkit/models/error_report.py:335`). Today that synthesis **flattens** the exception to a single string — `message = str(exc)` plus a class-name breadcrumb in `details["calf.exception_type"]`. All structured state the exception carried (a provider's `status_code` int, a `body` dict, the provider error `code`) and the entire `__cause__` chain are **lost** — recoverable only by string-parsing `message`.

This feature adds a typed, dict-bearing **`exception` slot** to `ErrorReport` and harvests the escaped exception's own attributes into it as JSON-native data, then maps the Python exception **cause chain (`__cause__`-only)** onto the existing recursive `causes` field — so the **full chain** of structured exception data propagates to the caller and deserializes, after the wire, with no producer class required.

It ships **data, not objects**. Exception identity never crosses the wire (the ADR-0006 principle); what crosses is a structured projection a consumer reads off `report.exception` and `report.walk()`.

## 2. Motivation

- **Recover the structured information.** A consumer should read `report.exception.attrs["status_code"]` / `["body"]["code"]` directly, not regex `report.message`. (Empirically, a context-window overflow surfaces today as `ModelHTTPError` with `status_code=400` and `body={... "code": "context_length_exceeded" ...}` — all of which is currently discarded into the message string.)
- **Preserve the cause chain.** A `raise ModelHTTPError(...) from openai.BadRequestError` carries a `__cause__` with its own structured state. The rail drops it entirely today. The chain is the diagnostic spine of the failure.
- **Stay decoupled.** The receiving deployment must not need `openai`, the vendored `pydantic_ai`, or any producer class importable to read a fault. Shipping JSON-native data keeps producer and consumer version-independent.

## 3. Goals / Non-goals

### Goals
1. A typed `ExceptionInfo` wrapper carried in a new `ErrorReport.exception: ExceptionInfo | None` field — a typed envelope around the **dict-typed** `attrs` harvest.
2. Harvest the escaped exception's `vars()` into `attrs` as JSON-native data, **totally** (never raising) and **best-effort leaf-safe** (an unserializable value nested anywhere is coerced where possible; a residually-unserializable attr is dropped-and-tallied, never silently).
3. Map the Python exception cause chain (`__cause__`-only) onto the existing `causes` recursion — each link a lean `ErrorReport` carrying its own `exception` slot.
4. Deserialize after the wire as plain `pydantic` validation — no custom decoder, no class registry, no object rehydration.
5. Rename the catch-all fault type `calf.unhandled` → **`calf.exception`** (§9).
6. Remove the now-redundant `details["calf.exception_type"]` breadcrumb (subsumed by `exception.type`) (§10).

### Non-goals
- **No curated `error_type` classification.** This feature delivers the *slot + generic harvest* only. The fault keeps the catch-all `error_type` (`calf.exception`); it does **not** inspect the exception to mint a specific code. `FaultTypes.MODEL_CONTEXT_WINDOW_EXCEEDED` therefore **stays unproduced** — a consumer reads the structured `attrs`, not a typed code. Curated classification (giving that constant a producer, e.g. at the LiteLLM boundary, #230) is the separate **Option A** successor and rides this same slot later.
- **Only `__cause__` is structured; everything else is string-coerced or dropped.** The chain walk follows the linear `__cause__` lineage only (`__context__` was evaluated and **dropped**, §6.1). An exception held as a *plain attribute* — `self.inner = exc`, `tenacity.RetryError.last_attempt`, or an **`ExceptionGroup`'s `.exceptions`** (a getset descriptor, **not in `vars()`**, so dropped entirely, not even string-coerced) — and the implicit `__context__` chain are **not** chain-walked. A `FallbackExceptionGroup` (all fallback models failed) therefore loses its per-model failures in this version. Structured branching (mapping `.exceptions` into `causes`) is a deferred follow-up (§16).
- **No harvest on the mint path.** A deliberately-minted `NodeFaultError` converts **verbatim** (the §6.5 mint rule, bypassing `from_exception`) and gets **no** `exception` slot — it already carries a complete, chosen report; there is no foreign exception to introspect. Consequence: `raise NodeFaultError(...) from some_exc` does not harvest `some_exc` (the minter owns their `details`).
- **No leak prevention.** Per the settled decision, exceptions are treated as non-leaking. The harvest is unconditional; no allow/deny-list, no redaction, no config knob. (Rationale: an escaped exception's attributes are protocol/runtime metadata; calfkit user payloads live on frame inputs / `message_history`, never as exception attributes.)
- **No object rehydration.** We never reconstruct a live Python exception on the receiver. `ExceptionInfo` *is* the deserialized form.

## 4. Background — the machinery being reused (grounded)

| Piece | Location | Role |
|---|---|---|
| `ErrorReport` | `calfkit/models/error_report.py:130` | The typed fault value. **Gains the `exception` field (§5).** Frozen, `extra="ignore"` BaseModel (`:156`) — additive field is backward-compatible. |
| `ErrorReport.from_exception` | `error_report.py:335` (decorator `:334`) | Arbitrary-exception → fault synthesis (§6.7). **The single producer this feature enriches** (harvest + chain). Public signature unchanged. |
| `ErrorReport.build_safe` | `error_report.py:263` | Total construction + carriage bounds. **Gains an `exception=` param and bounds `exception.attrs` (§7).** The fallback arm (`:316`) stays the secondary backstop. |
| `causes` / `_bound_cause_list` | `error_report.py:180,392` | Recursive cause carriage + depth/total budget (`_MAX_CAUSES_DEPTH=8`, `_MAX_CAUSES_TOTAL=64`) + cycle guard. **Reused** to carry the mapped exception chain; `model_copy` at `:414` preserves the new `exception` field on each bounded node (verified — no `_bound_cause_list` edit needed). Its **field docstring (`:180-182`) must be updated** to add the exception chain as a 4th `causes` source (§9). |
| `walk()` / `find()` | `error_report.py:217,237` | Pre-order, cycle-guarded traversal of `causes`. `walk()` is the consumer's chain-reading API; `find(error_type)` is **not** a chain discriminator here (§11, all links share `calf.exception`). |
| `_bound_details` | `error_report.py:446` | 16 KB serialized bound for a dict, with a dropped-byte count. **Reused** to bound `exception.attrs`. |
| `to_minimal` | `error_report.py:249` | Identity-only strip floor for the 256 KB publish cap. Already omits `exception` by construction — **must NOT be extended to carry it** (§7). |
| `FaultTypes.UNHANDLED` / `EXCEPTION_TYPE` | `error_report.py:55,98` | Catch-all code + class-name breadcrumb. **Renamed / removed (§9, §10).** |
| `_fault_from_exception` + direct call | `calfkit/nodes/base.py:659` (6 sites) **and a 7th direct `from_exception` call at `base.py:1070`** | The synthesis paths; `from_exception` is called **UNGUARDED** (no surrounding `try`) → **`from_exception` must be total** (§8). Unchanged by this feature. |
| `HDR_ERROR_TYPE` | `calfkit/_protocol.py:43`; stamped `base.py:733/767/811/1148` | The `x-calf-error-type` header carrying `error_type` for broker-level filtering. Its **value flips** with the rename (§9). |
| in-process `ctx.exception` | fault-rail spec §6.3/§486 | The **live** exception object exposed to the recovery seam. `report.exception` is the **post-wire data projection of that same exception** — the seam keeps the live object; the wire/client gets the harvested data. |

The single insertion point: **`from_exception`** is the only place that turns a foreign exception into an `ErrorReport` (two call sites: `base.py:669` via `_fault_from_exception`, and `base.py:1070` directly). Enriching it propagates to both uniformly — the chokepoint pattern holds, no call-site edits.

## 5. Wire model — `ExceptionInfo` and the `exception` field

```python
class ExceptionInfo(BaseModel):
    """A structured projection of a raised exception (inspired by OTel's exception.* attrs).

    Carried on a fault synthesized from an uncaught exception. Carries DATA, never
    the exception object — `attrs` is JSON-native (harvested via the sanitize round-trip).
    The receiver needs no producer class to read it.
    """
    model_config = ConfigDict(frozen=True, extra="ignore")

    type: str                                  # type(exc).__name__, e.g. "ModelHTTPError"
    module: str | None = None                  # type(exc).__module__
    attrs: dict[str, Any] = Field(default_factory=dict)   # sanitized vars(exc)


class ErrorReport(BaseModel):
    ...
    exception: ExceptionInfo | None = None     # populated only on the from_exception path
```

- **`type` / `module`** are *inspired by* OpenTelemetry's `exception.type` convention (split here into bare name + module for easy consumer matching; `module`/`attrs` are calfkit extensions, not OTel attributes — this is convention, not conformance). `type` subsumes the deleted `calf.exception_type` breadcrumb.
- **`attrs`** is the dict-typed slot: the exception's own instance attributes, JSON-native after sanitize. `message` is dropped from it (redundant with the enclosing `ErrorReport.message` for the `AgentRunError`/provider families this targets; on the rare exception whose `.message` attr diverges from `str(exc)`, that nuance is lost — acceptable for the temp-fix scope).
- **Additive & backward-compatible.** `ErrorReport`'s `extra="ignore"` + the `None` default mean an old wire message without the field validates to `None`, and an old reader ignores the new key. No schema-version bump.

## 6. Harvest and chain — the algorithm

`from_exception` becomes a thin public seed over a recursive core that threads a shared `_ChainState` (the `id()`-keyed cycle-guard `seen` + the one-shot depth-cap truncation flag `truncated`) and a per-frame `_depth` counter through the whole language chain. `_depth` stays per-frame (by-value) so a future *branching* walk (`ExceptionGroup` members, §16) counts each branch independently rather than sharing one depth:

```python
class _ChainState:                                   # sibling of the existing _CauseBudget
    __slots__ = ("seen", "truncated")
    def __init__(self, root_id):
        self.seen = {root_id}                        # cycle guard, root pre-marked
        self.truncated = False                       # set on a depth-cap drop (breadcrumbed, never silent)

@classmethod
def from_exception(cls, exc, *, node=None, ctx=None, cause=None, frame_chain=None, origin_frame_id=None):
    # public entry — seeds the walk; this exc is the ROOT (gets origin breadcrumbs).
    return cls._from_exception(exc, node=node, ctx=ctx, cause=cause, frame_chain=frame_chain,
                               origin_frame_id=origin_frame_id, _state=_ChainState(id(exc)), _depth=1)

@classmethod
def _from_exception(cls, exc, *, node, ctx, cause, frame_chain, origin_frame_id, _state, _depth):
    exc_info, attrs_dropped = _harvest_exception(exc)                          # TOTAL (see below)
    chain = cls._walk_exception_chain(exc, _state=_state, _depth=_depth)       # list[ErrorReport], len ≤ 1
    return cls.build_safe(
        error_type=FaultTypes.EXCEPTION,                              # renamed catch-all (§9)
        message=safe_exc_message(exc),                                # str(exc); FULLY total — str/repr/__name__ all guarded (calfkit._safe leaf)
        origin_node_id=getattr(node, "node_id", None),
        origin_frame_id=origin_frame_id if origin_frame_id is not None else getattr(ctx, "frame_id", None),
        frame_chain=frame_chain,
        exception=exc_info,
        exception_attrs_dropped=attrs_dropped,                        # ELIDED breadcrumb (§7)
        chain_truncated=_state.truncated,                            # depth-cap-drop breadcrumb (§7)
        causes=([cause] if cause is not None else []) + chain or None,
    )

@classmethod
def _walk_exception_chain(cls, exc, *, _state, _depth):
    # __cause__-ONLY — `__context__` was evaluated and dropped (§6.1). GUARDED: a hostile/raising
    # __cause__ descriptor must not break totality (drop the chain, stay total).
    try:
        nxt = exc.__cause__
    except Exception:
        return []
    if nxt is None or id(nxt) in _state.seen:
        return []                                  # end-of-chain, or a cycle (correct dedup, not a drop)
    if _depth >= _MAX_CAUSES_DEPTH:
        _state.truncated = True                    # a real, non-cycle drop — breadcrumb it, never silent
        return []
    _state.seen.add(id(nxt))
    link = cls._from_exception(nxt, node=None, ctx=None, cause=None, frame_chain=None,
                               origin_frame_id=None, _state=_state, _depth=_depth + 1)
    return [link]
```

```python
def _jsonsafe(v):
    # Recursive, leaf-level: an unserializable value nested anywhere becomes its
    # str()/"<Unserializable …>" placeholder. HARDENED: bytes_mode='base64' (raw HTTP/socket
    # error bodies are often non-utf8) and inf_nan_mode='strings' (nan/inf -> "NaN"/"Infinity",
    # lossless and wire-consistent — the model publish path emits null for inf/nan otherwise).
    # The residual raise is pathologically-deep nesting (serialize recursion limit), handled by
    # the per-attr guard in _harvest_exception.
    return pydantic_core.from_json(
        pydantic_core.to_json(v, serialize_unknown=True, bytes_mode="base64", inf_nan_mode="strings")
    )

def _harvest_exception(exc) -> tuple[ExceptionInfo, int]:
    # TOTAL — runs as an argument to build_safe, OUTSIDE its try/except, on the unguarded
    # chokepoint path (base.py:669/1070). It must never raise on its own. Returns the info plus
    # a count of attrs dropped because they could not be serialized even after hardening.
    try:
        raw = vars(exc)                          # slots-only / hostile __dict__ -> ANY exception
    except Exception:
        raw = {}
    attrs: dict[str, Any] = {}
    dropped = 0
    try:
        items = list(raw.items())                # hostile .items() (dict-subclass) -> no attrs, stay total
    except Exception:
        items = []
    for k, v in items:
        # The WHOLE per-attr body is guarded — a hostile key's __eq__ or str(k), or a deep/odd
        # value, must not escape. Anything that raises is dropped-and-tallied, never silent.
        try:
            if k == "message":                   # redundant with ErrorReport.message
                continue
            key = k if isinstance(k, str) else str(k)   # attrs keys must be str (pydantic) — coerce
            attrs[key] = _jsonsafe(v)
        except Exception:
            dropped += 1
    # Harvest type and module INDEPENDENTLY: a hostile metaclass whose __module__ raises must not
    # discard a readable __name__ (the forensic class hint §9 preserves on the synthesis log).
    try:
        type_name = type(exc).__name__
    except Exception:
        type_name = "<unharvestable>"
    try:
        module = type(exc).__module__
    except Exception:
        module = None
    try:
        return ExceptionInfo(type=type_name, module=module, attrs=attrs), dropped
    except Exception:
        # A non-str type/module RETURNED by a hostile metaclass (no raise) is rejected by
        # ExceptionInfo: drop to the sentinel, but TALLY the harvested attrs so the loss is never silent.
        return ExceptionInfo(type="<unharvestable>"), dropped + len(attrs)
```

Key properties of the recursion:
- **The root and every link are the same operation.** Both are `_from_exception` of one exception → one `ErrorReport` carrying its own `exception` slot. So `_walk_exception_chain` *recurses through `_from_exception`* — every chained exception is serialized into its own report with its own harvest. Mutual recursion: `_from_exception` → `_walk_exception_chain` → `_from_exception` → …
- **Links carry no calfkit origin.** Chain links pass `node=None, ctx=None, frame_chain=None`, so a mid-chain `openai.BadRequestError` correctly has `origin_node_id=None` and an empty `frame_chain` — it happened at no calfkit frame. Only the **root** (the exception that faulted *at the node*) gets the origin/topology.
- **Linear, not branching.** We follow exactly one next link per exception (`__cause__` only), so each node contributes ≤1 chain link; the whole chain is a linear list of depth ≤ `_MAX_CAUSES_DEPTH`. A chain deeper than the cap is truncated and the drop is breadcrumbed (`details["calf.elided"]["chain_truncated"] == True`) — never silent; a cycle terminates via the threaded `_ChainState.seen` (correct dedup, not a drop).
- **`causes` is a flat union; discriminate by content, not position.** On a recovery-then-failure synthesis (`cause=` passed), `causes` holds the recovery-prior report **and** the language-chain links co-mingled in one list (`[cause] + chain`). A consumer must **not** assume `causes[0]` is the recovery-prior — discriminate by `error_type` / `exception`, and traverse with `walk()`. (This is the acknowledged trade for reusing one chain mechanism; ADR-0024 rejected a separate `ExceptionInfo.cause` chain for the single traversal.)
- **Shared budget; chain may be elided first (accepted).** Each link routes through its own `build_safe` (per-node `attrs` bound), and the top-level `_bound_cause_list` then bounds the overall structure against the shared `_MAX_CAUSES_DEPTH`/`_MAX_CAUSES_TOTAL`. Because the chain is appended **after** any recovery `cause`, a large recovery subtree can exhaust the 64-cause budget and the language-chain links are dropped first — **breadcrumbed** under `details["calf.elided"].causes`, never silent. Per the settled decision (option a) this shared-budget degradation is accepted; a dedicated chain sub-budget is deferred to Option A.

### 6.1 `__context__` — evaluated and DROPPED (resolved 2026-06-27)

The initial scoping included implicit `__context__` chaining on a trivial-cost basis, with an explicit escape hatch: drop it if it proved non-trivial. **The plan review found it non-trivial, and it was dropped — the walk is now `__cause__`-only.** Reason: `from_exception` runs *inside* the rail's `except` blocks (`base.py:1686`, `:1712-1721`), and Python auto-sets `__context__` to the exception currently being handled. Walking it would (a) **leak framework internals onto the wire** — when the rail's caught exception is the boundary-seam wrapper, the walk harvests it → `exception.type == "_SeamAccidentError"`, violating the decoupling invariant (§8/§11); and (b) **duplicate the body exception** — in recovery-then-failure the original appears both as the explicit recovery `cause=` and as an implicit `__context__` link. `__cause__`-only loses nothing for the target case (provider errors chain via explicit `raise ... from` = `__cause__`) and excludes only the rail's incidental handling context, which was never wanted. Decision recorded in ADR-0024.

## 7. Carriage bounds — `build_safe` gains `exception=`

`build_safe` is the single total-construction chokepoint, so the new field's bound lives there too:

- Add `exception: ExceptionInfo | None = None`, `exception_attrs_dropped: int = 0`, and `chain_truncated: bool = False` to the signature.
- Bound `exception.attrs` by reusing `_bound_details` (the 16 KB serialized cap). Because `attrs` is already JSON-native (harvested via `_jsonsafe`), the unserializable branch of `_bound_details` (`return {}, None`) is unreachable here — but handle it **defensively** the way the user-`details` path does (empty attrs + an `exception_attrs_unserializable` flag) so the chokepoint stays uniform and never silent. Rebuild the bounded copy via `exception.model_copy(update={"attrs": bounded})` (the model is frozen — `type`/`module` are preserved).
- **Assemble ALL elision keys into `elided` BEFORE the single `if elided: bounded_details = {**bounded_details, FaultTypes.ELIDED: elided}` merge** (`error_report.py:304-305`). The merge **snapshots** `elided`; any key added *after* it never reaches `bounded_details` — a silent drop, re-opening the exact hole the feature closes. New keys, all added pre-merge: `exception_attrs_bytes` (oversize attrs), `exception_attrs_dropped` (the harvest's residual per-attr drops), `chain_truncated` (the `__cause__`-chain depth-cap drop, §6).
- The fallback arm (`error_report.py:316`) sets `exception=None` (it already discards `causes`/`details`/`frame_chain`); totality is preserved.
- **Do NOT add `exception` to `to_minimal`.** The 256 KB strip floor must stay identity-only. Note the aggregate pressure: an 8-deep chain can carry ≈8×16 KB of `attrs`, raising the odds of crossing the 256 KB cap, at which point the strip floor drops the **entire** harvest wholesale (all-or-nothing). Accepted for the temp-fix scope; a graceful aggregate `attrs` budget is deferred to Option A.

## 8. Safety guarantees

| Property | How it's guaranteed |
|---|---|
| **Totality — `from_exception` never raises** | **The harvest itself is total** (it runs *before*/as input to `build_safe`, outside its `try`): `_harvest_exception` guards `vars()`, the `.items()` iteration, and the **entire per-attr body** (the `== "message"` compare, the `str(k)` key coercion, and the serialize) with bare `except`, plus the `__name__` and `__module__` accesses *independently* (a raising `__module__` keeps a readable `__name__`) and the `ExceptionInfo(...)` construction (a non-str type/module the construction rejects is tallied with `dropped`, never silently lost); `_walk_exception_chain` guards the `__cause__` read (a raising descriptor drops the chain, stays total). The `message` helper `safe_exc_message` (the `calfkit._safe` leaf) is FULLY total — `str`, `repr`, *and* the `<unprintable {type(exc).__name__}>` fallback are each guarded, so a hostile metaclass `__name__` cannot break it (stdlib `traceback._some_str` does NOT guard that last access; we must, since this runs unguarded at the chokepoint). `build_safe` is the secondary backstop. The chokepoint (`base.py:669/1070`) calls `from_exception` unguarded, so this is load-bearing. |
| **Leaf-safety (best-effort)** | `_jsonsafe` (`to_json(serialize_unknown=True, bytes_mode='base64', inf_nan_mode='strings')` round-trip) coerces unknown types, non-utf8 bytes, and nan/inf at any depth; a **cyclic-container** attr serializes to a `"..."` marker (handled, not dropped — verified). The only residual raise is pathologically-deep nesting (serialize recursion limit), dropped per-attr and **tallied** (`exception_attrs_dropped`). Good neighbors survive; nothing live is stored. |
| **Bounded depth** | `_walk_exception_chain` caps the live walk at `_MAX_CAUSES_DEPTH` (8); `_bound_cause_list` re-bounds the serialized structure (≤8 deep, ≤64 total). |
| **Cycle-safe** | One `id()`-keyed `seen` set on the threaded `_ChainState`, root pre-marked — a `__cause__` chain can cycle via explicit construction. |
| **Size-bounded** | `exception.attrs` rides the 16 KB `_bound_details` cap; `to_minimal` omits the field for the 256 KB publish floor. |
| **No live objects on the wire** | `_jsonsafe` replaces every non-JSON-native leaf with a `str`/`<Unserializable …>` placeholder. Tracebacks are never touched — `vars(exc)` excludes `__traceback__`. |

## 9. Rename: `calf.unhandled` → `calf.exception` (hard cutover)

`FaultTypes.UNHANDLED = "calf.unhandled"` becomes `FaultTypes.EXCEPTION = "calf.exception"` (constant name and wire value). The catch-all's job is "a fault synthesized from a raised exception," and `calf.exception` names that origin, pairs with the new `exception` slot, and drops the faint "framework dropped it" tone of *unhandled*. A clean pre-1.0 **hard cutover** — no rollout/compat shim; the `x-calf-error-type` header value flips `calf.unhandled`→`calf.exception` and any ops tap / DLT filter keyed on the old value updates in the same cut.

**Blast radius (enumerated — `git grep` confirmed, `_vendor`/worktrees excluded):**

*Code:*
- `error_report.py`: the constant (`:55`), `build_safe` fallback arm (`:326`), `from_exception` (`:362`) + its docstring (`:347-348`), `_coerce_error_type` docstring (`:209-210`), the `FANOUT_ABORTED` comment (`:72`). (The `EXCEPTION_TYPE` comment block `:94-98` — which also mentions `calf.unhandled` at `:95` — is removed wholesale by §10.)
- `calfkit/nodes/base.py`: comment/docstring references at `:662, :824, :1047, :1067, :1325, :1513, :1669`. (The header *value* at `:733/767/811/1148` flips automatically — no literal edit.)

*Docs:*
- `docs/api.md:307` — the public `FaultTypes` reference table row (`UNHANDLED | calf.unhandled`).
- `docs/policy-seams.md:89`.
- `docs/designs/fault-rail-and-policy-seams-spec.md` — line 52 glossary, `:238` naming-scheme list, `:460`/`:486` (slot-scoped / exception mapping), `:507` (recovery-correction note), `:843` scenario. (The §4.2 stamping table `:188-198` carries only the header *name* `x-calf-error-type` — no `calf.unhandled` literal to rename.)
- `docs/designs/fault-rail-fanout-behavior-catalogue.md` — intro item 3 (`:23`), FR-2 (`:41`), FR-6 (`:45`), SE-9 (`:133`), XC-5 (`:258`).
- `docs/designs/fault-rail-fanout-integration-test-plan.md` — `:81, :82, :107, :130, :174, :175, :188, :191, :222, :223`.
- `docs/adr/0003-faults-travel-the-result-rail-and-escalate.md:23` — the `causes`-transform enumeration (add "an exception chain" as a 4th source; mirror in the `causes` field docstring `error_report.py:180-182` and the fault-rail §52 glossary — see §6).
- *(ADR-0024's own references to `calf.unhandled`/`calf.exception_type` are intentional — it records the rename/removal — and stay.)*

*Tests (~57 `"calf.unhandled"` literal lines, ~79 incl. the `FaultTypes.UNHANDLED` symbol, across 13 files):* `test_fault_pipeline.py` (incl. fixtures), `test_fault_wire_model.py` (incl. the rename pin `:476` `FaultTypes.UNHANDLED == "calf.unhandled"`), `test_resolve_callee.py`, `test_reply_slot.py`, `test_staged_pipeline.py`, `test_seam_context.py`, `test_seams.py`, and kafka-lane `test_fault_escalation_kafka.py`, `test_fanout_faults_kafka.py`, `test_tool_discovery_kafka.py`, `test_seam_pipeline_kafka.py`, `test_deferred_reception_kafka.py`, `_fault_tools.py:9/36`.

**Logging (§13 mandate):** after the rename every harvested fault logs `error_type=calf.exception` uniformly, and the class-name that `calf.exception_type` provided is being removed (§10). Add `exception.type` to the synthesis log line (`base.py:678` / `_fault_from_exception`) so the forensic class hint is not demoted.

## 10. Remove `details["calf.exception_type"]`

`exception.type` carries the class name structurally, so the `FaultTypes.EXCEPTION_TYPE` breadcrumb (`error_report.py:98`, set at `:372`) is redundant. Remove the constant and the write. `from_exception` is its only *producer*, but the removal is **not** contained on the consumer side — **7 assertions read it and must migrate to `report.exception.type`:** `test_fault_wire_model.py:519/532/548`, `test_fault_pipeline.py:256`, `test_fault_escalation_kafka.py:88`, `test_tool_discovery_kafka.py:244`, `test_seam_pipeline_kafka.py:231` (+ the `_fault_tools.py:10` docstring and the behavior-catalogue/test-plan FR-2/F-1 references in §9).

## 11. Crossing the wire & deserialization

Safe **by construction** — there is no custom deserializer, and that is the design:

- **Serialize.** `ErrorReport` (with `exception` + the `causes` chain) is serialized by the existing envelope path. Because we store JSON-native data, it is lossless for what we store (incl. `nan`/`inf` → `"NaN"`/`"Infinity"` strings, set at harvest time by `inf_nan_mode='strings'`).
- **Deserialize.** The client hub decodes `Envelope → FaultMessage → ErrorReport`; `pydantic` validates `exception: ExceptionInfo | None` and `causes` recursively. An embedded dict round-trips back to a Python `dict` under `Any` (values JSON-shaped: `dict`/`list`/`str`/`int`/`float`/`bool`/`None`). **No producer class need be importable on the receiver.**
- **Consume — read `exception` for diagnostics; branch on `error_type` (see §11.1).** Every chain link carries the *same* `calf.exception` `error_type`, so `find(error_type)` cannot discriminate links here (it returns the shallowest match); it stays useful for fault-group navigation and becomes useful for chain links again under Option A (curated codes). The top-level `report.exception` is the everyday read; to inspect a *specific* chained exception (forensic / stopgap), walk the tree and match `exception.type`:
  ```python
  except NodeFaultError as e:
      # DIAGNOSTIC read — what failed (first-class):
      e.report.exception.type                       # "ModelHTTPError"
      e.report.exception.attrs["body"]["code"]      # "context_length_exceeded"
      # FORENSIC chain walk — a specific deeper cause (stopgap; keys on a class name, §11.1):
      openai = next((r for r in e.report.walk()
                     if r.exception and r.exception.type == "BadRequestError"), None)
      if openai: openai.exception.attrs["status_code"]   # 400
  ```
- **No rehydration.** `ExceptionInfo` is the deserialized form; we never fabricate Python exception objects from it. It is the post-wire data projection of the same exception the seam sees live as `ctx.exception`.

### 11.1 Consumer contract — what to read vs what to branch on

`exception` is **diagnostic-first**. The intended division of labour for a consumer (the client at `NodeFaultError.report`, an ops tap, a log sink):

- **`error_type` (+ `find()`) is the durable branching contract.** Control flow keys on the stable dotted code — today the framework codes (`calf.delivery.*`, `calf.fanout.*`), tomorrow the Option A provider codes (`calf.model.context_window_exceeded`). `find()` is the composition-safe matcher (it traverses `causes`, so it keeps working when a fan-out wraps faults in a group).
- **`report.exception` is the everyday diagnostic read** — first-class for logging, error UIs, and debugging "what failed." The top-level fault is usually the exception you care about, so `report.exception.type` / `.attrs` answers it without walking.
- **`walk()` + `exception.type` is forensic / a pre-Option-A stopgap, NOT a blessed branching API.** Use it (a) to inspect a *specific exception deeper in the chain*, or (b) to branch programmatically *before* a curated code exists — explicitly **best-effort**, because it keys on a **class name**, not a code. That class name is version-fragile by nature, and in this codebase especially: the LiteLLM migration (#230) replaces the pydantic-ai exception classes wholesale (`ModelHTTPError` → a LiteLLM type), so an `exception.type`-based branch breaks at that migration while a curated `error_type` survives it. Treat `exception.type` as something you *see*, not something you *decide on*.

**Deliberately not added:** a first-class `report.find_exception(type_name)` helper. It would entrench class-name branching — the anti-pattern Option A exists to retire. The right response to recurring class-name branching is to prioritize the curated code (Option A), not to ergonomically bless the stopgap.

## 12. Decisions (settled with the user, 2026-06-27)

1. **Slot placement** — a typed `ExceptionInfo` wrapper in a dedicated `ErrorReport.exception` field (not a raw `details` key).
2. **Chain** — map the language chain onto the existing `causes` recursion; each link a lean `ErrorReport` with its own `exception`; flat union with the recovery-prior, discriminated by content not position.
3. **Leak posture** — none; exceptions treated as non-leaking; unconditional harvest, no knob.
4. **Harvest** — `pydantic_core.from_json(to_json(v, serialize_unknown=True, bytes_mode='base64', inf_nan_mode='strings'))` per attr; the existing dependency, no new one.
5. **Walk rule** — `__cause__`-only (`__context__` evaluated and **dropped** — inside the rail's `except` blocks it captures the framework's handling context, leaking internals and duplicating the cause; §6.1).
6. **`ExceptionGroup` / exception-valued-attribute branching** — deferred (linear `__cause__` only; members/attrs string-coerced or dropped).
7. **Mint path** — `NodeFaultError` mints stay verbatim; no `exception` slot.
8. **Rename** — `calf.unhandled` → `calf.exception`; **hard cutover**, no rollout-safety shim.
9. **Breadcrumb** — remove `calf.exception_type`.
10. **Budget** — option (a): accept the current shared-budget degradation (chain links breadcrumb-elided first under pressure); dedicated sub-budget deferred to Option A.
11. **Consumer contract** — `exception` is diagnostic-first; durable branching is `error_type` + `find()`; `walk()` + `exception.type` is forensic / a pre-Option-A stopgap, not a blessed branching API; no `find_exception` helper (§11.1).
12. **ADR** — recorded as ADR-0024.

## 13. Rejected alternatives

- **Serialize the exception object and rehydrate it on the receiver** (native binary object serialization, or hand-written per-class pydantic schemas). Rejected: native object serialization executes arbitrary code on decode — unacceptable across a shared message bus / trust boundary — and does **not** round-trip the exceptions we care about (verified — `ModelHTTPError`'s default reconstruction recipe replays `self.args`, and rebuilding raises `TypeError`); both approaches also require the producer's class importable on the receiver at a compatible version, re-coupling deployments and reversing the ADR-0006 "exception identity never crosses the wire" principle. We ship data, not objects.
- **A separate `ExceptionInfo.cause: ExceptionInfo` chain** distinct from `causes`. Rejected: faithful but duplicates the bounding + cycle-guard machinery and forces a consumer to traverse two places; reusing `causes` gives one traversal (`walk()`) and the budgets for free. The cost — recovery-prior and chain co-mingle in `causes` — is accepted and documented (§6).
- **Curated `error_type` in this feature** (e.g. detect context-window → mint `calf.model.context_window_exceeded`). Out of scope here — the slot is permanent infra; classification is the Option A successor (#230) that rides the same slot. Bundling them would conflate "carry the data" with "name the failure."

## 14. Build order (TDD)

1. **Model.** `ExceptionInfo` + `ErrorReport.exception`; rename `UNHANDLED`→`EXCEPTION` (+ the §9 code/doc/test edits); remove `EXCEPTION_TYPE` (+ migrate the §10 consumer assertions to `report.exception.type`). Update the `causes` field docstring (`:180-182`), fault-rail §52 glossary, and ADR-0003 (`:23`) to add the exception chain as a 4th `causes` source. *Tests:* additive decode (old message → `None`), frozen, rename references, JSON round-trip of `exception`.
2. **Harvest.** `_jsonsafe` (hardened) + `_harvest_exception` (total) + `build_safe(exception=, exception_attrs_dropped=, chain_truncated=)` bounding (`model_copy`). *Tests:* serializable attrs kept and typed; nested-unserializable leaf coerced (good neighbors survive); **non-utf8 `bytes` → base64 (no raise)**; **`nan`/`inf` → strings**; **cyclic-container attr → `"..."` marker (handled, not dropped)**; **deeply-nested (~2000-deep) attr → `exception_attrs_dropped` (the deterministic residual lever — cyclic does NOT drop)**; **lone-surrogate string attr**; **generator- and `BaseModel`-valued attrs** coerced; empty / slots-only exception → `attrs={}`; **non-`str` `__dict__` key coerced**; `message` dropped; 16 KB `exception_attrs_bytes` elision breadcrumb (and `type`/`module` survive the `model_copy` bound).
3. **Chain.** `_from_exception` / `_walk_exception_chain` recursion (`__cause__`-only). *Tests:* `__cause__` mapped to a link with its own `exception`; end-of-chain (`__cause__ is None`); **multi-hop cycle** (`A.__cause__=B`, `B.__cause__=A`) terminates via the threaded `_ChainState.seen`; cycle onto a **mid-chain (non-root) link** terminates; **>`_MAX_CAUSES_DEPTH` chain → kept links + `details["calf.elided"]["chain_truncated"] == True`** (never silent); links carry `origin_node_id=None`/empty `frame_chain`; recovery-`cause=` + chain co-mingle in `causes` (discriminate by content, not position); **`walk()`+`exception.type` locates a deep link** (NOT `find`).
4. **Totality.** *Tests (the load-bearing C1 set):* a hostile exception whose `__str__`/`__repr__` raises (existing bar `test_fault_wire_model.py:525-548`), **whose metaclass makes `type(exc).__name__` raise** (and a separate **`__module__`-raising** variant — good `__name__`), **with a non-`str` `__dict__` key** (incl. one whose `str()`/`__eq__` itself raises), **whose `__dict__` property or `.items()` raises** (non-`TypeError`), **with a raising `__cause__` descriptor** (at root and mid-chain), **with a hostile exception mid-`__cause__`-chain** — none make `from_exception` raise. (The currently-benign hostile variants pass even with the holes open, so the descriptor- and key-coercion-hostile variants are the ones that actually exercise the round-2 fixes.)
5. **End-to-end.** Real `ModelHTTPError` (offline) → `from_exception` populates `exception.attrs` with `status_code`/`body` and the `__cause__` link; plus a **kafka-lane** real-broker test: a faulting model → client `NodeFaultError` whose `report.exception` and chain survive the wire.
6. **Docs.** Update the `NodeFaultError` docstring example to read the `exception` slot via `walk()`/`exception.type`; write/keep ADR-0024; add `exception.type` to the §13 synthesis log.

## 15. Test plan (coverage targets)

- **Harvest unit:** JSON-native attrs preserved and typed after round-trip; poisoned leaf → placeholder; partially-poisoned dict keeps good keys; non-utf8 bytes / nan / inf / cyclic-container handled without raise or wire-divergence; over-budget attrs elided with breadcrumb (and `type`/`module` survive); deterministic deep-nesting drop tallied; `vars()`-less (slots) exception → empty attrs.
- **Chain unit (`__cause__`-only):** `__cause__` mapped / end-of-chain; multi-hop cycle (to root and to a mid-chain link) terminates via the threaded `_ChainState.seen`; >`_MAX_CAUSES_DEPTH` truncation → `chain_truncated` breadcrumb (never silent); recovery `cause=` and chain co-mingle under one `causes` list (content-not-position); `walk()`+`exception.type` matches a deep link; `find(error_type)` does NOT discriminate links.
- **Non-harvest paths:** a minted `NodeFaultError` and a framework `build_safe` fault (`calf.delivery.rejected`) both leave `exception is None`.
- **Totality (load-bearing):** the §14.4 hostile set never makes `from_exception` raise — the chokepoint has no backstop.
- **Wire:** full `Envelope → FaultMessage → ErrorReport` round-trip preserves `exception` and the chain; embedded dict decodes to a Python `dict`; receiver needs no producer class.
- **Integration (kafka lane):** faulting-model → client receives `NodeFaultError(report)` with `report.exception.type`/`attrs` and the mapped chain intact.

## 16. Future work

- **Option A — curated extractor.** A per-exception-family adapter that *also* assigns a specific `error_type` (giving `MODEL_CONTEXT_WINDOW_EXCEEDED` a producer), ideally at the LiteLLM boundary (#230). It populates the same `exception` slot — this feature is its permanent substrate — and makes `find(error_type)` a useful chain discriminator again. A dedicated chain sub-budget and a graceful aggregate-`attrs` floor (vs today's all-or-nothing strip) also belong here.
- **`ExceptionGroup` / wrapped-exception branching.** Map an `ExceptionGroup`'s `.exceptions` (and exception-valued plain attributes like `RetryError.last_attempt`) into `causes` so a `FallbackExceptionGroup`'s per-model failures become structured siblings rather than being dropped/string-coerced.
- **Client-side typed views.** Optional `error_type` → local exception subclass mapping on the receiver (decoupled, discriminator-keyed) — note this partially reverses the deliberate "no `error_type` → exception registry" stance in `NodeFaultError`, so it is a separate decision.
