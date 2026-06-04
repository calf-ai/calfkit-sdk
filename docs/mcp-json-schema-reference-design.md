# Design: Reference JSON Schema for `mcp.json`

**Status:** Implemented (PR #172) · **Component:** `calfkit/mcp` · **Decision:** Option A3 (parser delegates to Pydantic input models). Schema is **wrapped-only**, has **no `$id`**; the `$schema` URL users reference is a docs concern (raw-git, pinned to `main`).

> This is the design record (kept for the rationale and rejected alternatives). It was written pre-implementation and verified across four review passes; the feature is now merged. Where the text below says it "replicates `_parse_server_spec`" or carries "verify during impl" notes, those refer to the design phase — the `_parse_*_spec` helpers have since been **removed**, and that transport-detection behavior now lives in `_validate_server` (`calfkit/mcp/_config.py`). A post-merge PR review also hardened error rendering to avoid leaking `$VAR`-expanded secrets and to surface all validation errors.

---

## 1. Problem & goal

`McpServers.from_file("./mcp.json", schemas=...)` parses a hand-rolled config, but calfkit ships no machine-readable reference for what that file may contain. Users learn the accepted surface only by reading `calfkit/mcp/_config.py`. There is **no official ecosystem schema** for the `mcp.json` *client config* (de-facto convention; clients have diverged — VS Code uses `servers`, others `mcpServers`; Cursor never hosted its `$schema`). A generic ecosystem schema would mislead, because calfkit accepts a **specific subset/variant**:

- **stdio:** `command` (required, non-empty), `args` (list[str]), `env` ({str:str}), `cwd` (str)
- **http:** `type` (`http`/`sse`), `url` (required, non-empty), `headers` ({str:str})
- **calfkit-only:** `$VAR` / `${VAR}` / `$$` expansion across all string values
- accepts wrapped (`{"mcpServers": {...}}`) **or** bare (`{"<name>": {...}}`)
- ignores unknown keys (so existing `disabled`/`autoApprove` don't break the drop-in)

**Goal:** ship a calfkit-specific reference JSON Schema, generated from a single source of truth (no drift from the parser), consumable as editor IntelliSense via a `$schema` line, plus a CLI emitter with CI drift-checking that mirrors `calfkit mcp codegen`.

**Non-goals:** strict validation that rejects unknown keys (breaks drop-in); SchemaStore registration under the generic `mcp.json` name; runtime validation inside `from_file` (the schema is a documentation aid, **not** a hot-path validator — stated so nobody wires `jsonschema` in later); IntelliSense for the bare form (see §3.5).

## 2. Chosen approach (A3)

`ParsedStdioSpec` / `ParsedHttpSpec` are the parser's **normalized output** (frozen dataclasses, `kind` discriminator, tuple `args`) — not the input shape, so generating a schema from them would describe the wrong thing. A3 introduces **input-shaped Pydantic models** mirroring the accepted surface, routes parsing through them, and normalizes the result back into the existing `Parsed*` dataclasses — so `_factory.py` and all downstream consumers stay untouched. The same two models generate the schema → single source of truth.

```
parse_mcp_config(config)
  ├─ _load_config_file (unchanged)                     # str/Path → dict
  ├─ unwrap mcpServers / bare (unchanged)
  ├─ name + dict pre-checks (unchanged)                # "non-empty string", "spec for server", "must be an object"
  └─ per server:
       ├─ expand_env(raw_spec, where=...) (unchanged)  # $VAR; raises on unset
       └─ _validate_server(name, expanded)  ──► StdioServerConfig | HttpServerConfig
            ├─ resolve transport (url-first precedence) or raise "unknown transport"
            ├─ Model.model_validate(...)  → ValidationError translated to McpConfigError
            └─ _to_parsed(cfg)  ──► ParsedStdioSpec | ParsedHttpSpec   (unchanged output type)
```

## 3. Detailed design

### 3.1 Input models (`calfkit/mcp/_config.py`)

Two **real validation models** (used per-server). Module-private by non-export (same convention as the existing `Parsed*`); class names stay clean (no leading underscore) so the schema's `$defs` keys read `StdioServerConfig` / `HttpServerConfig`. There is **no** `McpJsonConfig` and **no** named union alias — both were schema-only and are replaced by a `TypeAdapter` in §3.5.

```python
from pydantic import BaseModel, ConfigDict, Field
from typing import Literal

class StdioServerConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")  # default; explicit for intent (drop-in leniency)
    type: Literal["stdio"] | None = Field(default=None, description="Optional; inferred from `command` when omitted.")
    command: str = Field(min_length=1, description="Executable to spawn. Supports $VAR/${VAR} expansion.")
    args: list[str] = Field(default_factory=list, description="CLI args; each supports $VAR.")
    env: dict[str, str] | None = Field(default=None, description="Subprocess env overlay; values support $VAR.")
    cwd: str | None = Field(default=None, description="Subprocess working directory.")

class HttpServerConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    type: Literal["http", "sse"] | None = Field(default=None, description="`http` (or legacy `sse`); inferred from `url`.")
    url: str = Field(min_length=1, description="Streamable-HTTP endpoint. Supports $VAR.")
    headers: dict[str, str] | None = Field(default=None, description="Request headers; values support $VAR.")
```

- `extra="ignore"` ⇒ unknown keys dropped (parser leniency preserved) **and** no `additionalProperties` emitted (editors permit drop-in extras). Verified against current Pydantic docs + live run.
- `min_length=1` reproduces the "non-empty command/url" rule **and** correctly rejects a `$VAR` that expands to `""` (expansion runs before validation — see §5 test).
- `type` is decorative for validation (transport is resolved before validate); it exists **only to document/autocomplete the key** in the schema. Comment this at the field so nobody thinks it drives transport selection.

### 3.2 Transport resolution + validation — `_validate_server`

Exact replica of `_parse_server_spec` precedence, folded together with validation so the function returns a validated **instance** (avoids mypy-strict friction from returning `type[A | B]`):

```python
def _validate_server(name: str, raw: dict[str, Any]) -> StdioServerConfig | HttpServerConfig:
    t, has_url, has_command = raw.get("type"), "url" in raw, "command" in raw
    if t in ("http", "sse") or (t is None and has_url):
        model: type[StdioServerConfig | HttpServerConfig] = HttpServerConfig
    elif t == "stdio" or (t is None and has_command):
        model = StdioServerConfig
    else:
        raise McpConfigError(
            f"mcp.json: server {name!r} has unknown transport. Provide either 'command' (stdio) "
            f"or 'url' (http); got keys={sorted(raw.keys())}"
        )
    try:
        return model.model_validate(raw)
    except ValidationError as exc:
        _raise_from_validation(name, exc)  # NoReturn
```

Preserves **url-first** precedence on ambiguous specs and keeps the friendly `unknown transport` message out of Pydantic. We deliberately **do not** use a Pydantic smart/discriminated union for validation: (a) smart-union order ≠ url-first; (b) `Field(discriminator="type")` is incompatible with `type` being optional (verified `PydanticUserError`); (c) union errors break the `unknown transport` message and the `loc[0]` substring contract. Verified: a 16-case differential against the current `_parse_server_spec` is a zero-mismatch match (incl. both-keys, contradictory/garbage `type`, empty `command:""`).

### 3.3 Error translation — preserve substring contract, no quality regression

Existing tests match **substrings** (`'command'`, `'args'`, `'env'`, `'cwd'`, `'url'`, `'headers'`). Verified: for every failing-path test, `exc.errors()[0]["loc"][0]` is exactly that field name (`args=[1,2,3]` yields 3 errors but `errors()[0]` is the `('args',)` list-level error). Include `input` so the message is **no worse** than today's hand-written ones:

```python
def _raise_from_validation(name: str, exc: ValidationError) -> NoReturn:
    err = exc.errors()[0]
    field = err["loc"][0] if err.get("loc") else "<unknown>"
    raise McpConfigError(
        f"mcp.json: server {name!r}: invalid {field!r} — {err['msg']} (got {err['input']!r})"
    ) from exc
```

`{field!r}` renders `'command'`, `'args'`, … satisfying every `match=`.

### 3.4 Normalize to existing output (the load-bearing invariant)

```python
def _to_parsed(cfg: StdioServerConfig | HttpServerConfig) -> ParsedMcpServerSpec:
    if isinstance(cfg, StdioServerConfig):
        return ParsedStdioSpec(command=cfg.command, args=tuple(cfg.args), env=cfg.env, cwd=cfg.cwd)
    return ParsedHttpSpec(url=cfg.url, headers=cfg.headers)
```

`Parsed*` unchanged ⇒ `_factory.py` (isinstance + splatted-tuple `*spec.args` + `kind`) untouched. **The type system does not enforce input↔output field congruence** — `_to_parsed` is a hand-maintained copy. Mitigation: keep `_to_parsed` adjacent to both type defs, and the §6 round-trip test is **mandatory** (it is the only enforcement; add a field and forget here → only the test fails).

### 3.5 Schema generation — wrapped-only, via `TypeAdapter`, no sacrificial model

```python
from pydantic import TypeAdapter

_SERVERS = TypeAdapter(dict[str, StdioServerConfig | HttpServerConfig])

def mcp_json_schema() -> dict[str, Any]:
    """Reference JSON Schema (draft 2020-12) for a calfkit mcp.json. Permissive: unknown keys allowed."""
    servers_map = _SERVERS.json_schema(ref_template="#/$defs/{model}")  # {type:object, additionalProperties:{anyOf:[$ref,$ref]}, $defs:{...}}
    defs = servers_map.pop("$defs")
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "calfkit mcp.json",
        "description": "Reference schema for calfkit McpServers.from_file(...). Unknown keys are ignored.",
        "type": "object",
        "required": ["mcpServers"],
        "properties": {"mcpServers": servers_map},
        "$defs": defs,
    }
```

- **Wrapped-only** (no bare-form `anyOf`). Rationale: `type` is optional ⇒ the server union is a plain `anyOf` of two `$ref`s with no discriminator; nesting that under a second wrapped-vs-bare `anyOf` is exactly the "matches none / matches both" editor noise we want to avoid. The bare form stays **fully accepted by the parser at runtime** — it just doesn't get IntelliSense (and a bare-form file can't carry a top-level `"$schema"` key without colliding with a server named `$schema` anyway). One-line `anyOf` to add later if ever needed (YAGNI now).
- **No `$id`.** The permissive doc-schema has no external `$ref`s; omitting `$id` keeps the function tiny and avoids baking a repo URL into the committed artifact (the user-facing `$schema` URL is a docs concern — §3.8 / O1).
- Pure-Pydantic (no `typer`, no MCP spawn) ⇒ lives in `_config.py`, exported from `calfkit.mcp`, usable without the `mcp-codegen` extra.
- *Verify during impl:* `TypeAdapter(...).json_schema()` emits `$defs` keyed by class name + the `additionalProperties: {anyOf:[$ref,$ref]}` map (independently asserted by two reviewers for the model-schema form; confirm for the TypeAdapter form).

### 3.6 CLI: `calfkit mcp schema`

New subcommand in `calfkit/cli/mcp.py`, mirroring `codegen`'s write/`--check`/exit contract (0 ok · 1 drift/missing · 2 io-error):

```
calfkit mcp schema [--output PATH=calfkit/mcp/mcp.schema.json] [--check]
```

- **Deterministic render:** `json.dumps(mcp_json_schema(), indent=2) + "\n"` (no `sort_keys`; Pydantic emits stable order — cross-process determinism verified). A test asserts re-render is byte-identical so `--check` can't flap.
- **Shared tail:** factor `_emit_text(rendered, output, *, check, refresh_hint) -> int` (existence-check + read + write + exit codes + messages) and have `codegen` use it too. Keep the **diff caller-supplied** — `codegen` passes its AST-aware `diff_modules`; `schema` passes a plain `difflib` text diff. Do **not** force `diff_modules` onto JSON.
- Help text states the default `--output` targets a **repo checkout** (a pip-installed user running it would write into read-only site-packages and hit the existing exit-2 path — tolerable, the file already ships).
- The CLI inherits the `mcp-codegen` (typer) extra, same as `codegen`. Library users who only want the schema call `calfkit.mcp.mcp_json_schema()` (no extra needed) — this is the documented headline path; the CLI is a maintainer/CI convenience.

### 3.7 Packaging

Committed artifact at `calfkit/mcp/mcp.schema.json`, **inside the package** (so `pip install` ships it for offline `"$schema": "<local-path>"` use). Verified: hatchling `packages=["calfkit"]` ships `.json` files automatically (a probe wheel was built and inspected) — **no `artifacts` entry needed**, no `.gitignore` exclusion.

### 3.8 Docs

`docs/mcp-overview.md` mcp.json section: add a `"$schema"` example line (raw-git URL pinned to `main` + a note on the in-package local-path alternative), a field-reference table, and a note on `$VAR` + unknown-key leniency. Add a "Reference schema" row to *Functionality at a glance*. The library-function path (`mcp_json_schema()`) is documented as the primary way to obtain the schema programmatically.

## 4. File-by-file change list

| File | Change |
|---|---|
| `calfkit/mcp/_config.py` | Add `StdioServerConfig`, `HttpServerConfig`, `_SERVERS` adapter, `_validate_server`, `_raise_from_validation`, `_to_parsed`, `mcp_json_schema`. Rewire `parse_mcp_config` body; delete `_parse_server_spec`/`_parse_stdio_spec`/`_parse_http_spec`. Keep `expand_env`, `_load_config_file`, unwrap + pre-checks, and the `Parsed*` dataclasses verbatim. |
| `calfkit/mcp/__init__.py` | Export `mcp_json_schema` (only new public symbol). |
| `calfkit/cli/mcp.py` | Add `schema` command + shared `_emit_text` helper; refactor `codegen`'s tail to use it (diff stays caller-supplied). |
| `calfkit/mcp/mcp.schema.json` | New committed generated artifact. |
| `docs/mcp-overview.md` | `$schema` example (raw-git `main` URL + local-path) + field table + feature row. |
| `pyproject.toml` | Fix stale `[project.urls]` `calf-sdk` → `calfkit-sdk` (side fix; O1). |
| CI (`.github/workflows/code-checks.yml`) | Add a `calfkit mcp schema --check` step. |
| `tests/mcp/test_config.py` | **No changes** — regression contract. |
| `tests/mcp/test_schema.py` | New (see §6). |

## 5. Behavior-preservation matrix (every existing `test_config.py` case — all verified ✅)

| Test group | Path under A3 |
|---|---|
| `expand_env_*` (8) | `expand_env` untouched |
| wrapped / bare shape | unwrap untouched |
| stdio/http explicit+inferred, sse→http | `_validate_server` precedence (16-case diff: 0 mismatches) |
| missing transport → `"unknown transport"` | same raise text |
| stdio missing/empty `command`; non-string `args`/`env`/`cwd` | `min_length`/`missing`/type errors; `loc[0]` = field |
| http missing `url`; bad `headers` | `loc[0]` = `url`/`headers` |
| all-optional stdio/http; env subst into env/url/headers; unset→raise | `expand_env` before validate; `_to_parsed` args→tuple |
| empty name / non-dict spec / non-dict top-level | pre-checks unchanged |
| file: missing / invalid JSON / non-object | `_load_config_file` unchanged |
| `Parsed*` frozen | dataclasses unchanged |

Verified: no Pydantic int/bool→str coercion (bad-type inputs rejected as today); no test asserts old phrasing; all 17 `match=` substrings still produced.

## 6. Test plan (`tests/mcp/test_schema.py`)

- **Round-trip field congruence (mandatory):** for representative stdio+http specs, assert `_to_parsed(_validate_server(...))` equals the `Parsed*` produced by `parse_mcp_config` — the sole enforcement of input↔output congruence.
- **Post-expansion empty command:** `{"command": "$EMPTY"}` with `EMPTY=""` raises (`min_length=1` after expansion), substring `'command'`.
- **Schema shape:** `mcp_json_schema()` has `$schema`(2020-12), `title`, `required:["mcpServers"]`, `$defs.{StdioServerConfig,HttpServerConfig}`, server union is `anyOf` of two `$ref`s; **no** `additionalProperties:false` anywhere; **no** `$id`.
- **Permissiveness / structural validity:** example configs (wrapped-stdio, wrapped-http, with-extra-key `disabled`) round-trip through `parse_mcp_config` cleanly; `{}` and a server with neither `command` nor `url` are rejected by the parser. (Assert against the parser — our code — **not** via a `jsonschema` dep.)
- **Determinism:** `json.dumps(mcp_json_schema(), indent=2)+"\n"` is byte-stable across calls.
- **CLI `--check`:** exit 0 when committed file matches, 1 on drift/missing (typer `CliRunner`, per `tests/mcp/test_cli.py`).
- Run full `tests/mcp/` to confirm `test_config.py` passes unchanged.

## 7. Considered & rejected

- **A2 (schema-only models + contract test):** two representations reconciled by a test, not one source of truth. Rejected for A3.
- **Validate whole config via one model:** changes precedence, loses friendly errors + bare form. Rejected.
- **Pydantic discriminated union (`Field(discriminator="type")`):** incompatible with optional/inferred `type` (verified error). Rejected; would require making `type` mandatory (worse paste-in DX).
- **Bare-form `anyOf` in schema:** doubled `anyOf` → editor noise; parser still accepts bare at runtime. Cut.
- **Sacrificial `McpJsonConfig` BaseModel:** validatable-looking but never validated (foot-gun) + `mcpServers` field trips ruff `N815`. Replaced by `TypeAdapter`.
- **`union_mode="left_to_right"` / `$id` / `jsonschema` dep / packaging `artifacts`:** each verified to add nothing here. Cut.
- **Drop `Parsed*`, make Pydantic the output:** larger `_factory.py` blast radius. Rejected.

## 8. Risks & mitigations

| Risk | Mitigation |
|---|---|
| `_to_parsed` input↔output drift (no type-level guard) | mandatory §6 round-trip test; keep converter adjacent to type defs |
| Pydantic error `loc`/message drift across versions | translator keys only on `loc[0]` (stable); `test_config.py` guards; deps pinned |
| `--check` flapping on nondeterministic render | pinned `indent`/newline + byte-stability test |
| `TypeAdapter` schema shape differs from expectation | verify-during-impl note in §3.5; shape test in §6 |
| `$schema` docs URL unresolvable (wrong slug / private fork) | O1; in-package local-path `$schema` always works offline |
| mypy-strict friction on transport-resolve typing | `_validate_server` returns an instance, not `type[A|B]`; spike both spellings before committing |

## 9. Resolved decisions

- **O1 — RESOLVED.** Public repo is `calf-ai/calfkit-sdk` (confirmed). Docs `$schema` example:
  `https://raw.githubusercontent.com/calf-ai/calfkit-sdk/main/calfkit/mcp/mcp.schema.json` (pinned to `main` — stable URL, auto-updating; fine for a permissive doc-schema), with the in-package local-path alternative also documented. Side fix: `pyproject [project.urls]` currently points at the stale `calf-ai/calf-sdk` — correct those to `calfkit-sdk` in the same change.

No open questions remain.

## 10. Implementation sequence (TDD)

**Iron law:** no new production code without a failing test first. New behavior (`mcp_json_schema`, the CLI `schema` command) is strict red→green→refactor. The `parse_mcp_config` rewrite is **refactor-under-contract**: the existing `tests/mcp/test_config.py` (baseline **52 passed**, verified green) must stay green through the rewrite; new edge/congruence tests are added test-first.

Phases are **sequential** (file dependencies: the CLI uses `mcp_json_schema`; the parser reuses its models; the artifact/docs need the CLI). Mechanism verified live: `TypeAdapter(dict[str, Stdio|Http]).json_schema()` yields the expected `$defs`+`anyOf` map with no `additionalProperties:false`; error `loc[0]` is the field name with `input` available.

1. **Core (`_config.py` + `__init__.py` + tests)** — owns `_config.py` end-to-end:
   - RED→GREEN: write `tests/mcp/test_schema.py` (shape, determinism, no `$id`/no `additionalProperties:false`) → add the two models + `_SERVERS` adapter + `mcp_json_schema()` → green.
   - Refactor-under-contract: add congruence + post-expansion-empty-command + empty-object-rejected tests → rewire `parse_mcp_config` via `_validate_server`/`_to_parsed`, delete `_parse_*_spec` → **all** of `test_config.py` + new tests green.
   - Export `mcp_json_schema` from `calfkit/mcp/__init__.py`.
2. **CLI (`cli/mcp.py` + `test_cli.py`)** — RED→GREEN: write `schema --check`/write tests → add `_emit_text` + `schema` command, refactor `codegen`'s tail onto `_emit_text` (diff stays caller-supplied; codegen keeps `diff_modules`) → green.
3. **Artifact + docs + meta** — generate `calfkit/mcp/mcp.schema.json` via `uv run calfkit mcp schema`; update `docs/mcp-overview.md`; fix `pyproject [project.urls]`; add CI `--check` step; full `uv run pytest tests/mcp/` + `ruff check` + `mypy` green.

After each phase: `uv run pytest tests/mcp/`, `uv run ruff check`, `uv run mypy` must pass before the next.

## Change log (post-review)

Adopted: drop bare-form schema (wrapped-only); replace `McpJsonConfig`/union alias with `TypeAdapter` (also fixes the `N815` CI blocker and the never-validated-model foot-gun); drop `union_mode`, `$id`, `jsonschema` dep, packaging `artifacts` hedge; fold resolve+validate into `_validate_server` (mypy-strict); include `err['input']` in error messages; pin render determinism; make round-trip congruence test mandatory; add post-expansion-empty-command test; CLI demoted to maintainer/CI tool with `mcp_json_schema()` as the headline library path. Verified-and-kept: dual representation, per-server precedence, `loc[0]` substring contract, `_emit_text` DRY with caller-supplied diff.
