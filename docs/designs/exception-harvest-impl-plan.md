# Structured Exception Harvest — Implementation Plan

- **Status:** Ready to implement. Lands the feature in `docs/designs/exception-harvest-spec.md` (reviewed to convergence; spec + plan review rounds folded) in **one PR**. Decision: ADR-0024.
- **Date:** 2026-06-27
- **Branch:** `feat/exception-harvest` (off `main`; worktree per project rule — confirm at kickoff).
- **Scope sentence:** add a typed `ErrorReport.exception` slot harvested from an uncaught exception, map its `__cause__` chain (`__cause__`-only — §6.1 of the spec) onto `causes`, rename the catch-all `calf.unhandled` → `calf.exception`, and remove the redundant `calf.exception_type` breadcrumb — all behind the single synthesis chokepoint `ErrorReport.from_exception`, with no new topic/header/routing.

This plan is the **HOW/WHERE/WHAT-TESTS**. The WHY and the contract live in the spec; every code shape below is the post-review (proven-total, `__cause__`-only) version from spec §5–§7.

---

## 1. Scope of changes

### In scope (this PR)
1. New wire type `ExceptionInfo` + `ErrorReport.exception: ExceptionInfo | None`, exported.
2. The total harvest (`_jsonsafe` hardened, `_harvest_exception` total) + `build_safe` bounding of `exception.attrs`.
3. The chain recursion (`_from_exception` / `_walk_exception_chain`) mapping **`__cause__`-only** onto `causes`, with a `chain_truncated` breadcrumb on depth-cap drop.
4. Rename `FaultTypes.UNHANDLED`/`"calf.unhandled"` → `FaultTypes.EXCEPTION`/`"calf.exception"` (hard cutover) across code, tests, docs.
5. Remove `FaultTypes.EXCEPTION_TYPE` + its write; migrate consumers to `report.exception.type`.
6. `exception.type` added to the synthesis log line; `causes` 4th-meaning doc edits (docstring + glossary + ADR-0003 + `api.md`).
7. Tests: harvest/chain/totality unit (`test_fault_wire_model.py`), rail (`test_fault_pipeline.py`), one **deterministic** kafka-lane end-to-end replaying the real OpenAI `ModelHTTPError` JSON.

### Out of scope (cross-ref spec §3 / §16)
- Curated `error_type` classification (Option A / #230); `MODEL_CONTEXT_WINDOW_EXCEEDED` stays unproduced.
- `__context__` chaining (evaluated and dropped, spec §6.1); `ExceptionGroup` / wrapped-exception branching.
- Harvest on the mint path; leak prevention; client `error_type→exception-subclass` registry; `find_exception()` helper; dedicated chain sub-budget / graceful aggregate floor; **a `live`-LLM test** (the kafka test replays the real JSON deterministically instead).

---

## 2. File-by-file change inventory

| File | Change | Kind |
|---|---|---|
| `calfkit/models/error_report.py` | `ExceptionInfo` model; `ErrorReport.exception` field; `_jsonsafe`; `_harvest_exception`; `from_exception`→seed + `_from_exception` + `_walk_exception_chain` (`__cause__`-only, `_trunc`); `build_safe(exception=, exception_attrs_dropped=, chain_truncated=)` bounding; rename `UNHANDLED`→`EXCEPTION`; remove `EXCEPTION_TYPE` (constant `:98`, comment block `:94-98`, write `:372`); `causes` docstring (`:180-182`) 4th meaning; comment/docstring `calf.unhandled` refs (`:72,:209-210,:347-348`). | edit |
| `calfkit/models/__init__.py` | export `ExceptionInfo` (import `:12`; `__all__` insert at **`:46`**, before `FaultTypes`, alphabetical). | edit |
| `calfkit/__init__.py` | export `ExceptionInfo` (import `:16`; `__all__` `:39`). | edit |
| `calfkit/nodes/base.py` | synthesis log **`:679`** gains `exception.type`; comment/docstring `calf.unhandled` refs (`:662,:824,:1047,:1067,:1325,:1513,:1669` — **`:1669` is prose; reword to "a `calf.exception` fault", not a blind "EXCEPTION"**). Header stamps (`:733/767/811/1148`) flip value automatically — no edit. | edit |
| `docs/api.md` | (1) `:307` `FaultTypes` table row → `EXCEPTION`/`calf.exception`; (2) `:19` import example + add `ExceptionInfo`; (3) `:284-294` `ErrorReport` field table → add an `exception` row + document `ExceptionInfo`; (4) `:290` `causes` row → add the 4th source (the `__cause__` chain). | edit |
| `docs/policy-seams.md` | `:89` rename. | edit |
| `docs/designs/fault-rail-and-policy-seams-spec.md` | line 52 glossary (causes 4th meaning), `:238,:460,:486,:507,:843` rename. | edit |
| `docs/designs/fault-rail-fanout-behavior-catalogue.md` | `:23,:41,:45,:133,:258` (rename + `calf.exception_type`→`exception.type`). | edit |
| `docs/designs/fault-rail-fanout-integration-test-plan.md` | **11** token lines (`:81,82,107,110,130,174,175,188,191,222,223`; `:174` also breadcrumb). | edit |
| `docs/adr/0003-faults-travel-the-result-rail-and-escalate.md` | `:23` causes-transform enumeration (+ exception chain). | edit |
| `tests/test_fault_wire_model.py` | NEW harvest/chain/totality unit tests; migrate `UNHANDLED`/`EXCEPTION_TYPE` asserts (incl. rename pin `:476`). | edit |
| `tests/test_fault_pipeline.py` | NEW `_ChainRaisingNode` rail test (node-own chained raise → `exception` slot + `causes`); migrate `UNHANDLED`/`EXCEPTION_TYPE`. | edit |
| `tests/integration/_fault_tools.py` | **add a structured, `__cause__`-chained fault tool** (the real `ModelHTTPError(status_code=400, body=<exact OpenAI JSON>) from BadRequestError(...)`); migrate `:9/36` + `:10` docstring. | edit |
| every other test with the tokens | grep-driven rename + breadcrumb migration (§4). | edit |
| `tests/integration/test_exception_harvest_kafka.py` | NEW kafka-lane end-to-end (or extend `test_fault_escalation_kafka.py`). | new |

---

## 3. Build order (TDD commits — each ends green)

Dependency order: additive field first; rename next (no behavior dep); harvest (adds `exception.type`); **breadcrumb removal BEFORE the chain** (so the chain commit can take spec §6 verbatim with no `EXCEPTION_TYPE` baggage, and consumers have `exception.type` to migrate to); chain; docs/logging; integration.

### Commit 1 — model + exports (additive, unpopulated)
- Add `ExceptionInfo` above `ErrorReport`; add `exception: ExceptionInfo | None = None` (after `causes`). Export from both `__init__`s (`__all__` insert at `models/__init__.py:46`).
- **Tests** (`test_fault_wire_model.py`): additive decode (old report w/o key → `exception is None`); `extra="ignore"` ignores an unknown sibling key; `ExceptionInfo` frozen; full `Envelope→FaultMessage→ErrorReport` round-trip of a hand-built `exception`; `to_minimal()` omits `exception`.

### Commit 2 — rename `calf.unhandled` → `calf.exception` (mechanical)
- `FaultTypes.UNHANDLED`→`EXCEPTION`, value `"calf.unhandled"`→`"calf.exception"`. Keep `EXCEPTION_TYPE`.
- Grep-driven, **excluding the intentional-retain set**:
  ```
  git grep -lI -e 'calf\.unhandled' -e '\bUNHANDLED\b' \
    -- ':!calfkit/_vendor' ':!.claude' ':!docs/designs/exception-harvest-*' ':!docs/adr/0024-*'
  ```
  Replace in each; update rename pin `test_fault_wire_model.py:476` → `FaultTypes.EXCEPTION == "calf.exception"`; reword `base.py:1669` prose. **NOTE:** the spec/ADR/this-plan keep `calf.unhandled` intentionally — never `git add` then blind-replace them (they're excluded above).
- **GREEN:** offline suite passes with renamed asserts.

### Commit 3 — the harvest (total) + bounds
- Add `_jsonsafe` (hardened) and `_harvest_exception` (total) — spec §6. Extend `build_safe` with `exception=`/`exception_attrs_dropped=`/`chain_truncated=` and the bounding (spec §7 — **all elided keys assembled BEFORE the merge**). Wire `from_exception` to harvest and pass `exception=`/`exception_attrs_dropped=` (chain stays `[]` until commit 5).
- **Keep totality across the retained `EXCEPTION_TYPE` write.** `from_exception` still writes `details[EXCEPTION_TYPE]` until commit 4 — but it MUST source it from the already-total harvest, **not** re-access the metaclass: `report.details[FaultTypes.EXCEPTION_TYPE] = exc_info.type` (NOT `type(exc).__name__[:200]`, whose unguarded access raises on the §5.A hostile-`__name__` metaclass case → commit 3 RED). `exc_info.type` equals `"ValueError"` etc. for the existing `:519/:532/:548` asserts, so they stay green. (The write stays *after* `build_safe`, which strips `calf.*` details keys.)
- **Tests** (`test_fault_wire_model.py`): the §5.B harvest-fidelity battery + the **harvest-totality** subset of §5.A (str/repr, metaclass `__name__`/`__module__`, non-`str` key, `__dict__`/`.items()` raising — the *walk*-totality variants land in commit 5 with `_walk_exception_chain`); plus a `build_safe(chain_truncated=True)` unit test (the param's `elided` branch isn't exercised by `from_exception` until commit 5).

### Commit 4 — remove the `EXCEPTION_TYPE` breadcrumb
- Delete `FaultTypes.EXCEPTION_TYPE` (constant `:98`, comment block `:94-98`) and the write at `from_exception` `:372`.
- Migrate the **7** consumer assertions to `report.exception.type`: `test_fault_wire_model.py:519/532/548`, `test_fault_pipeline.py:256`, `test_fault_escalation_kafka.py:88`, `test_tool_discovery_kafka.py:244`, `test_seam_pipeline_kafka.py:231`; `_fault_tools.py:10` docstring. (`exception.type` is populated by commit 3, so this is now safe.)
- **GREEN:** no `EXCEPTION_TYPE`/`calf.exception_type` outside the retain set.

### Commit 5 — the chain recursion (`__cause__`-only)
- Split `from_exception` into the seed + `_from_exception` (threads `_seen`/`_depth`/`_trunc`) + `_walk_exception_chain` (`__cause__`-only, guarded read, depth-cap → `chain_truncated`) — spec §6, **verbatim** (no `EXCEPTION_TYPE` baggage left after commit 4).
- **Tests** (`test_fault_wire_model.py`): the §5.C battery.

### Commit 6 — logging + docs
- `base.py:679` synthesis log: add `exception.type` (`report.exception.type if report.exception else None` — the guard is *necessary*: the `build_safe` fallback arm leaves `exception=None`).
- `NodeFaultError` docstring example → diagnostic read via `exception`/`walk()`, branch on `error_type` (spec §11.1).
- `causes` 4th meaning: `error_report.py:180-182`, fault-rail spec line 52 glossary, ADR-0003 `:23`, **`api.md:290`**.
- Doc renames + the new public-surface rows in `api.md` (§2).

### Commit 7 — deterministic kafka-lane integration
- Add the structured `__cause__`-chained tool to `_fault_tools.py` replaying the **exact OpenAI JSON**:
  ```python
  _CTX_BODY = {"message": "Input tokens exceed the configured limit of 272000 tokens. "
                          "Your messages resulted in 700007 tokens. Please reduce the length of the messages.",
               "type": "invalid_request_error", "param": "messages", "code": "context_length_exceeded"}
  def ctx_overflow(x: int) -> int:  # harvested at the chokepoint; chained cause
      # openai.BadRequestError.__init__ requires a live httpx.Response (it derefs response.request),
      # so scaffold one — httpx is a transitive dep via openai; ModelHTTPError via calfkit._vendor.pydantic_ai.
      resp = httpx.Response(status_code=400, request=httpx.Request("POST", "http://x"))
      raise ModelHTTPError(status_code=400, model_name="gpt-5-nano", body=_CTX_BODY) \
          from BadRequestError("context_length_exceeded", response=resp, body=_CTX_BODY)
  ```
- `test_exception_harvest_kafka.py` (`@pytest.mark.kafka`, offline model/synthetic raise — **not** `live`): faulting node → **Channel A** (`_fault_tap.next_fault`) asserts `fault.error.exception.type == "ModelHTTPError"`, `attrs["status_code"] == 400`, `attrs["body"]["code"] == "context_length_exceeded"`, and a `walk()` link `exception.type == "BadRequestError"`; **Channel C** the client `result()` raises `NodeFaultError` whose `report.exception` + chain survive the wire. `_fault_tap` exposes `fault.error.exception`/`.causes` already — no tap-helper change.

---

## 4. Reference code (final, proven-total, `__cause__`-only)

```python
class ExceptionInfo(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")
    type: str
    module: str | None = None
    attrs: dict[str, Any] = Field(default_factory=dict)
# ErrorReport gains:  exception: ExceptionInfo | None = None   (after `causes`)

def _jsonsafe(v: Any) -> Any:
    return pydantic_core.from_json(
        pydantic_core.to_json(v, serialize_unknown=True, bytes_mode="base64", inf_nan_mode="strings")
    )

def _harvest_exception(exc: BaseException) -> tuple[ExceptionInfo, int]:
    try:
        raw = vars(exc)
    except Exception:
        raw = {}
    attrs: dict[str, Any] = {}
    dropped = 0
    try:
        items = list(raw.items())
    except Exception:
        items = []
    for k, v in items:
        try:
            if k == "message":
                continue
            key = k if isinstance(k, str) else str(k)
            attrs[key] = _jsonsafe(v)
        except Exception:
            dropped += 1
    try:
        type_name, module = type(exc).__name__, type(exc).__module__
    except Exception:
        type_name, module = "<unharvestable>", None
    try:
        return ExceptionInfo(type=type_name, module=module, attrs=attrs), dropped
    except Exception:
        return ExceptionInfo(type="<unharvestable>"), dropped
```

`from_exception` / `_from_exception` / `_walk_exception_chain` — verbatim from spec §6 (`__cause__`-only; `_seen`/`_depth`/`_trunc` threaded; links pass `node=None, ctx=None`; depth-cap sets `_trunc[0]=True`).

`build_safe` integration — **assemble the complete `elided` dict, then the ONE merge** (do NOT append after `error_report.py:305`):
```python
bounded_exception = None
exc_attrs_bytes = 0
exc_attrs_unser = False
if exception is not None:
    bounded_attrs, dropped_or_none = _bound_details(exception.attrs)   # attrs are jsonsafe'd → None branch unreachable
    if dropped_or_none is None:                                        # defensive, uniform w/ the user-details path
        bounded_attrs, exc_attrs_unser = {}, True
    else:
        exc_attrs_bytes = dropped_or_none
    bounded_exception = exception.model_copy(update={"attrs": bounded_attrs})
# ... existing elided keys (causes/frames/details), THEN add (ALL before the single merge):
if exception_attrs_dropped: elided["exception_attrs_dropped"] = exception_attrs_dropped
if exc_attrs_bytes:         elided["exception_attrs_bytes"] = exc_attrs_bytes
if exc_attrs_unser:         elided["exception_attrs_unserializable"] = True   # never silent (spec §7)
if chain_truncated:         elided["chain_truncated"] = True
# ... THEN the single `if elided: bounded_details = {**bounded_details, FaultTypes.ELIDED: elided}`
# ... return cls(..., exception=bounded_exception)
# fallback arm: returns exception=None by omission (unchanged)
```

---

## 5. Tests that MUST pass (acceptance gate)

**A. Totality (the rail must never re-open the silent-drop hole).** `from_exception` returns an `ErrorReport` — never raises — for: `__str__`/`__repr__` both raise; metaclass makes `type(exc).__name__` raise (and a separate `__module__`-raising variant); non-`str` `__dict__` key whose `str()` raises, and one whose `__eq__` raises; `__dict__`/`.items()` raises (non-`TypeError`); raising `__cause__` descriptor at root and mid-chain. (Empirically verified in review; the hostile battery passed with zero escapes.)

**B. Harvest fidelity.** Real-shaped `ModelHTTPError` → `exception.type`/`attrs["status_code"]`/`attrs["body"]["code"]`, `message` key absent; embedded dict round-trips to a Python `dict`; non-utf8 `bytes`→base64 (base64 even for *valid* utf8 bytes); `nan`/`inf`→`"NaN"`/`"Infinity"`, identical in-process and after `model_dump_json`; **cyclic-container attr → `"..."` marker (handled, NOT dropped)**; **deeply-nested (~2000) attr → `exception_attrs_dropped` (the deterministic drop lever — `pydantic_core`'s serde depth ≈250 is stack-independent, so it fires even inside the rail's deep call stack)**; **lone-surrogate string attr → `exception_attrs_dropped` (it *raises* `PydanticSerializationError`, so it's dropped-and-tallied, NOT coerced)**; generator- and `BaseModel`-valued attrs coerced; nested-unserializable leaf → placeholder, neighbors retained; **a raised-and-caught exception → `__traceback__` absent from `attrs`** (no `TracebackType` value — pins the §8 "tracebacks never touched" guarantee, which fresh-constructed exceptions never exercise).

**C. Chain (`__cause__`-only).** `raise B() from A()` → root `exception.type=="B"`, `causes[0].exception.type=="A"`, `causes[0].origin_node_id is None`, `frame_chain==[]`; end-of-chain (`__cause__ is None`); **multi-hop cycle to root** (`A.__cause__=B`, `B.__cause__=A`) and **cycle onto a mid-chain link** both terminate via threaded `_seen`; **a chain exactly at the kept-depth → end-of-chain, `chain_truncated` absent; one link deeper → `True`** (`nxt is None` is checked *before* the depth cap, so this pins the off-by-one and that a cycle does NOT set the flag); `walk()`+`exception.type` locates a depth-N link; `find("calf.exception")` returns only the root.

**D. Bounds & breadcrumbs.** `exception.attrs` > 16 KB → `details["calf.elided"]["exception_attrs_bytes"] > 0`, slot kept with bounded attrs, **`type`/`module` survive the `model_copy`**; residual per-attr drop → `exception_attrs_dropped >= 1`; `to_minimal()` drops `exception` (and the chain).

**E. Rail integration (`test_fault_pipeline.py`).**
- (1) A `_ChainRaisingNode` body raising a chained exception → published `FaultMessage.error` has `error_type=="calf.exception"`, populated `exception`, the chain in `causes`, and (post commit 4) **no** `details["calf.exception_type"]`.
- (2) **No-framework-leak guard must hit the case that actually bites.** `_SeamAccidentError` lands in `__context__` ONLY on the boundary-seam-accident → recover → recovery-path-`after_node`-raises path (`base.py:1690/1721`), never a plain body raise (whose `__context__` is a user exception). Add that CASE-2 test (`before_node` raises → wrapped; `on_node_error` recovers; `after_node` then raises) and assert `"_SeamAccidentError" not in [r.exception.type for r in report.walk() if r.exception]` **and** no `r.exception.module` starts with `calfkit.nodes.base`. A body-raise assertion here is **vacuous** — it passes even if `__context__`-walking were re-enabled.
- (3) **Unwrap guard.** On an existing boundary-seam-accident test, assert the ROOT `exception.type == "RuntimeError"` (the unwrapped user exc per `base.py:1690`), not `_SeamAccidentError` — else a regression dropping the unwrap goes uncaught (a body raise never routes `_SeamAccidentError` to `caught`).
- (4) Recovery-then-failure `causes` composition asserts **content/`any()`, never length/position**.

**F. Wire / decoupling.** Author the fault wire JSON as a **string literal** in a test module that imports **no** producer class; decode it and assert the structured reads (optionally `assert "openai" not in sys.modules` / decode in a subprocess) — proving no producer class is importable on the receiver.

**G. Kafka lane (`@pytest.mark.kafka`, deterministic).** §3 commit 7: the real-OpenAI-JSON `ModelHTTPError from BadRequestError` rides a real broker; `exception` + chain intact on the tap and at the client `result()`.

**H. Non-harvest paths.** A minted `NodeFaultError` and a framework `build_safe` fault (`calf.delivery.rejected`) both leave `exception is None`.

**I. Regression.** The full pre-existing offline suite + kafka lane pass after the rename/breadcrumb migration.

---

## 6. Edge cases to test explicitly

- **No `__dict__`** (pure `__slots__`) → `attrs={}`, still typed.
- **`message` attr diverging from `str(exc)`** → dropped from `attrs` (documented loss); `str(exc)` in `report.message`.
- **`vars(exc)` containing another exception** (plain attribute, e.g. `RetryError.last_attempt`) → string-coerced, NOT chain-walked (boundary; spec §3).
- **`ExceptionGroup`** → `.exceptions` absent from `attrs` (dropped); its `__cause__`, if any, still walked.
- **`__context__`-only chain** (raised in an `except` with no `raise from`) → **NOT walked** (`__cause__`-only) — pin this explicitly, it's the dropped behavior.
- **Cyclic `__cause__`** (`A.__cause__=B`, `B.__cause__=A`) → terminates via `_seen`.
- **Deep `__cause__` chain (> 8)** → `_MAX_CAUSES_DEPTH` links kept + `chain_truncated` breadcrumb.
- **Recovery `cause=` is a fan-out fault group (large) + a chain** → chain links elided first under the 64-budget, breadcrumbed (accepted, spec §6).
- **Aggregate `attrs` near 256 KB** → `to_minimal` strips the whole harvest (all-or-nothing, accepted) — assert the floored fault still publishes (`test_oversized_fault_kafka.py` shape).
- **`BaseException` (not `Exception`) reaching `from_exception`** — signature accepts `BaseException`; `KeyboardInterrupt`/`SystemExit` don't reach the `except Exception` chokepoint.

---

## 7. Acceptance criteria / verification

1. `make fix && make check` clean on the branch.
2. Offline green: `uv run pytest -m "not kafka and not live"`.
3. Kafka green: `uv run pytest -m kafka`.
4. **After `git add` of all new files**, the acceptance grep returns **zero** hits (the retain set is excluded, so this is a true mechanical zero, not "only intentional refs"):
   ```
   git grep -nI -e 'calf\.unhandled' -e '\bUNHANDLED\b' -e 'calf\.exception_type' -e '\bEXCEPTION_TYPE\b' \
     -- ':!calfkit/_vendor' ':!.claude' ':!docs/designs/exception-harvest-*' ':!docs/adr/0024-*'
   ```
5. 100% coverage on the new/changed `error_report.py` lines via `/pytest-coverage` — **every `except` arm of `_harvest_exception`/`_walk_exception_chain` exercised** (the totality guards are the priority).
6. The §5 acceptance tests A–I all present and passing.
7. Per `CLAUDE.md` PR steps: ADR-0024 status confirmed; docs (spec, api.md, policy-seams, catalogue, test-plan) updated; final `/pr-review-toolkit:review-pr` + `/simplify` pass.

---

## 8. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Totality regression (an unguarded path reintroduced) | §5.A hostile battery is a hard gate; coverage forces every `except` arm. #1 priority — the chokepoint has no backstop. |
| `build_safe` elided keys appended *after* the merge → silent breadcrumb drop | §4 mandates assembling `elided` **before** the single merge; §5.D asserts `exception_attrs_bytes`/`chain_truncated` actually appear. |
| Chain depth-cap drops silently | `chain_truncated` breadcrumb (§4/§6); §5.C pins it. |
| Rename misses a site / corrupts a retain-file | Scoped grep (§4) excludes the retain set; acceptance grep (§7.4) after `git add` must return clean. |
| `api.md` new public surface undocumented | §2 lists the 4 `api.md` edits; §7.7 gate. |
| Recovery-then-failure `causes` asserted positionally | `__cause__`-only removes the `__context__` duplication; §5.E asserts content/`any()` and the no-framework-internal-leak invariant. `test_chains_a_cause`/exact-list asserts survive only on constructed-not-raised exceptions — audit in commit 5. |
| `exception_attrs_dropped` lever non-deterministic | Use ~2000-deep nesting (recursion limit, deterministic); cyclic does NOT drop (→`"..."`). |

---

## 9. Estimated shape

~7 commits, one PR. Net new code is small (one model, two helpers, a recursion split, a `build_safe` arm); the bulk of the diff is the mechanical rename/breadcrumb migration across tests and docs. Engineering risk concentrates in **totality** (§5.A) — everything else is additive or mechanical.
