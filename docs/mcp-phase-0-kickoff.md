# Phase 0 Day-1 Kickoff — MCP Adaptor v1

**Status:** Ready to execute
**Document version:** 1.0
**Last updated:** 2026-05-30
**Companion to:** [`docs/mcp-v1-plan.md`](./mcp-v1-plan.md) §7 (phased implementation)

This is the literal day-1 checklist for an engineer starting Phase 0. Three commits, ~4-6 hours, all on a feature branch. Ends with the foundation laid and the load-bearing assumption empirically validated.

---

## What Phase 0 delivers

1. `mcp>=1.20.0` runtime dep + `mcp-codegen` optional extra (`typer`) added and locked.
2. `calfkit/mcp/` and `calfkit/cli/` package skeletons importable.
3. **The most important deliverable**: a baseline test proving the v1 plan's load-bearing claim (D7) — that `Agent(tools=[BaseToolNodeSchema(...)])` dispatches without client-side validation and that malformed args still become `RetryPromptPart`. If this test fails, Phase 1 cannot start.

No public API changes (`calfkit/__init__.py` is untouched — public exports land in Phase 6).

---

## Commit 1 — Dependencies

### Edit `pyproject.toml`

```diff
 dependencies = [
     "openai>=1.0.0",
     "anthropic>=0.80.0",
     "typing-extensions>=4.15.0",
     "dishka>=1.9.1",
+    # MCP (Model Context Protocol) — used by calfkit/mcp/* adaptor
+    "mcp>=1.20.0,<2",
 ]

 [project.optional-dependencies]
 dev = []
+# Installed via `pip install calfkit[mcp-codegen]`. Off the critical install
+# path because the CLI is only invoked at schema-generation time, not runtime.
+mcp-codegen = ["typer>=0.12"]
```

### Lock and verify

```bash
uv lock
uv sync --group dev

# Smoke-test the SDK imports — pre-validates the three transport entry points Phase 1 will wrap
uv run python -c "import mcp; from mcp import ClientSession; from mcp.client.stdio import stdio_client; from mcp.client.streamable_http import streamablehttp_client; print('mcp', mcp.__version__)"

# Existing tests still pass
uv run pytest tests/ -x --tb=short -q
```

**Pitfall:** if the smoke import fails on `streamablehttp_client`, the SDK is on a pre-1.20 version where the symbol was renamed. Investigate before continuing.

### Commit

```bash
git add pyproject.toml uv.lock
git commit -m "chore(mcp): add mcp>=1.20.0 dep and mcp-codegen optional extra"
```

---

## Commit 2 — Package skeleton

### Create directories and stub files

```bash
mkdir -p calfkit/mcp calfkit/cli tests/mcp
```

**`calfkit/mcp/__init__.py`** (empty surface; public re-exports land in Phase 6):
```python
"""calfkit MCP adaptor — see docs/mcp-v1-plan.md.

Public surface is finalized in Phase 6. During Phase 0–5 the package only
exposes internal modules consumed by tests.
"""
```

**`calfkit/mcp/exceptions.py`**:
```python
"""Exception hierarchy for the calfkit MCP adaptor.

The taxonomy distinguishes:
- McpConfigError — user-visible misconfiguration (parse/construction time)
- McpTransportError — runtime transport failure → FailedToolCall
- McpProtocolError — well-formed MCP error response → FailedToolCall
- McpToolDriftError — reserved for v1.1 strict-mode opt-in

Tool-semantic errors (CallToolResult.is_error=True) do NOT raise here —
they are returned as RetryPromptPart content by _adapt.py so the LLM can
retry. See v1 plan §3 and §9.
"""

from __future__ import annotations


class McpError(Exception):
    """Base class for all MCP adaptor exceptions."""


class McpConfigError(McpError):
    """User-visible misconfiguration. Raised at parse / construction time."""


class McpTransportError(McpError):
    """Transport-layer failure (subprocess crash, HTTP connect refused, timeout)."""


class McpProtocolError(McpError):
    """MCP protocol-layer failure (server-side MCPError, capability mismatch)."""


class McpToolDriftError(McpError):
    """Sanity-check found declared tools that the server does not advertise.

    v1 logs a warning rather than raising (see v1 plan §11 Q7). This class
    is reserved for the v1.1 strict-mode opt-in.
    """
```

**`calfkit/cli/__init__.py`**:
```python
"""calfkit CLI subcommands. Currently only `mcp codegen` (Phase 5)."""
```

**`tests/mcp/__init__.py`** — empty.

### Verify importability

```bash
uv run python -c "
import calfkit.mcp
import calfkit.mcp.exceptions
import calfkit.cli
from calfkit.mcp.exceptions import (
    McpError, McpConfigError, McpTransportError, McpProtocolError, McpToolDriftError,
)
assert issubclass(McpConfigError, McpError)
print('skeleton OK')
"
```

**Pitfall:** `calfkit/__init__.py` must remain unchanged. Public exports land in Phase 6.

### Commit

```bash
git add calfkit/mcp/ calfkit/cli/ tests/mcp/__init__.py
git commit -m "chore(mcp): scaffold calfkit/mcp/ + calfkit/cli/ packages"
```

---

## Commit 3 — BaseToolNodeSchema sanity test (the load-bearing gate)

This is the most important commit. The v1 plan's "zero agent.py changes" claim rests on a single empirical assumption that needs validating before any other MCP code is written.

### Why this test is non-redundant

Existing tests at `test_tool_errors.py:687-723` and `:1118-1157` validate the schema-only path **via `OverridesState.override_agent_tools`** (`agent.py:165` branch). The MCP adaptor uses the `Agent(tools=[...])` path instead (`agent.py:167` branch). Phase 0 must verify that branch accepts bare `BaseToolNodeSchema` instances — if not, agent.py needs a change before Phase 1.

### Create `tests/mcp/conftest.py`

```python
"""Shared fixtures for tests/mcp/. Populated in Phase 1+ with FakeMcpServer."""
```

### Create `tests/mcp/test_baseline_schema_only_dispatch.py`

```python
"""Phase 0 sanity test: the load-bearing claim of the MCP v1 plan.

The v1 plan (docs/mcp-v1-plan.md §D7, §6.1) asserts that the agent loop
already supports schema-only tools registered as ``BaseToolNodeSchema``
instances via the ``Agent(tools=[...])`` path, with two properties the
MCP adaptor depends on:

  1. Dispatches the tool call without attempting client-side argument
     validation (the ``isinstance(tool_node, BaseToolNodeDef)`` gate at
     calfkit/nodes/agent.py:374 — a bare ``BaseToolNodeSchema`` fails
     the check, so the validation block is skipped).

  2. Malformed JSON args from the LLM still become a ``RetryPromptPart``
     (not a hard ``FailedToolCall``), via the ``args_as_dict()`` try/except
     at calfkit/nodes/agent.py:350-369 which runs on *all* dispatch paths.

If this test fails, the v1 plan's "zero agent.py changes" claim does not
hold and Phase 1 must not start.
"""

from __future__ import annotations

import pytest

from calfkit._vendor.pydantic_ai.messages import RetryPromptPart, ToolCallPart
from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.models.actions import Call, TailCall
from calfkit.models.node_schema import BaseToolNodeSchema
from calfkit.models.state import State
from calfkit.nodes import Agent

# Reuse the proven helpers from the override-mode tests rather than duplicating.
from tests.test_tool_errors import _make_ctx, _model_emits_tool_calls


def _make_schema_only_tool(
    *,
    tool_name: str = "mcp_search",
    topic_base: str = "mcp.everything.search",
) -> BaseToolNodeSchema:
    """Construct a BaseToolNodeSchema mirroring what McpToolDef will yield in Phase 1."""
    return BaseToolNodeSchema(
        node_id=f"mcp_{tool_name}",
        subscribe_topics=[f"{topic_base}.input"],
        publish_topic=f"{topic_base}.output",
        tool_schema=ToolDefinition(
            name=tool_name,
            description="Synthetic MCP-style tool with one required string arg.",
            parameters_json_schema={
                "type": "object",
                "properties": {"q": {"type": "string"}},
                "required": ["q"],
                "additionalProperties": False,
            },
        ),
    )


async def test_schema_only_tool_via_tools_kwarg_dispatches_without_validation():
    """Property 1: Agent(tools=[BaseToolNodeSchema(...)]) dispatches without validation."""
    schema_only = _make_schema_only_tool()
    tool_call_id = "tc-mcp-baseline-01"
    call = ToolCallPart(tool_name="mcp_search", args={"q": "hello"}, tool_call_id=tool_call_id)

    agent = Agent(
        "agent_mcp_baseline",
        system_prompt="x",
        subscribe_topics="agent_mcp_baseline.input",
        publish_topic="agent_mcp_baseline.output",
        model_client=_model_emits_tool_calls([call]),
        tools=[schema_only],  # the tools= kwarg path (not OverridesState)
    )
    ctx = _make_ctx(State())
    result = await agent.run(ctx)

    assert tool_call_id not in ctx.state.tool_results
    assert isinstance(result, Call)
    assert result.input_args is not None and result.input_args[0] == tool_call_id
    assert result.target_topic == "mcp.everything.search.input"


async def test_schema_only_tool_malformed_args_become_retry_prompt():
    """Property 2: malformed JSON args still become RetryPromptPart on the schema-only path."""
    schema_only = _make_schema_only_tool()
    bad_call = ToolCallPart(
        tool_name="mcp_search",
        args="not-valid-json",  # str, not dict — args_as_dict() will raise
        tool_call_id="tc-mcp-baseline-malformed",
    )
    agent = Agent(
        "agent_mcp_baseline_malformed",
        system_prompt="x",
        subscribe_topics="agent_mcp_baseline_malformed.input",
        publish_topic="agent_mcp_baseline_malformed.output",
        model_client=_model_emits_tool_calls([bad_call]),
        tools=[schema_only],
    )
    ctx = _make_ctx(State())
    result = await agent.run(ctx)

    assert isinstance(result, TailCall)
    stored = ctx.state.tool_results.get("tc-mcp-baseline-malformed")
    assert isinstance(stored, RetryPromptPart)
    assert "Malformed tool arguments" in str(stored.content)


@pytest.mark.parametrize("args", ["[1,2,3]", "", "{not json"])
async def test_schema_only_tool_handles_various_bad_arg_shapes(args: str):
    """Defense-in-depth across off-spec args payloads."""
    schema_only = _make_schema_only_tool()
    tcid = f"tc-{hash(args)}"
    bad_call = ToolCallPart(tool_name="mcp_search", args=args, tool_call_id=tcid)
    agent = Agent(
        "agent_mcp_baseline_param",
        system_prompt="x",
        subscribe_topics="agent_mcp_baseline_param.input",
        publish_topic="agent_mcp_baseline_param.output",
        model_client=_model_emits_tool_calls([bad_call]),
        tools=[schema_only],
    )
    ctx = _make_ctx(State())
    await agent.run(ctx)
    stored = ctx.state.tool_results.get(tcid)
    assert isinstance(stored, RetryPromptPart), f"args={args!r}: got {type(stored).__name__}"
```

### Run and verify

```bash
# Verify tests/__init__.py exists (needed so tests.test_tool_errors is importable)
ls tests/__init__.py || touch tests/__init__.py

uv run pytest tests/mcp/test_baseline_schema_only_dispatch.py -v --tb=short
uv run pytest tests/ --tb=short -q
make check
```

### Outcomes

- **All 5 cases pass** → v1 plan's D7 holds. **Phase 1 is green-lit.**
- **Property 1 fails** → `Agent(tools=...)` doesn't accept bare `BaseToolNodeSchema`. Likely fix: widen the kwarg's type annotation and the `tools_registry` build at `agent.py:167`. **STOP — surface as architectural escalation.** This is the D13 `ToolLike` Union work being needed earlier than v1 plan §6.1 anticipates.
- **Property 2 fails** → malformed-args safety net doesn't cover the schema-only path. Document the deviation; decide whether v1 ships client-side validation after all.

### Commit

```bash
git add tests/mcp/
git commit -m "test(mcp): add Phase 0 baseline test for BaseToolNodeSchema dispatch"
```

---

## CI / infrastructure prep

**No CI changes needed in Phase 0.** Existing `.github/workflows/test.yml` will auto-discover `tests/mcp/` (testpaths=`tests` in pyproject.toml). mypy strict mode applies to `calfkit/mcp/` and `calfkit/cli/`.

Real-Kafka lane (Docker Compose + `@modelcontextprotocol/server-everything`) is deferred to **Phase 8** per v1 plan §8.1.

---

## Local dev setup (for Phase 1+ engineer)

Reference MCP server for manual testing — verify on your machine:

```bash
# Verify the server starts (Ctrl-C to exit)
npx -y @modelcontextprotocol/server-everything
```

If `npx` is unavailable: `brew install node` (macOS) or use a fresh NodeSource install (Linux).

**Phase 1 should write `tests/mcp/fixtures/echo_mcp_server.py`** (~30 LOC using `mcp.server.FastMCP`) for integration tests, avoiding the Node dependency on contributor machines. Don't write this in Phase 0.

---

## Definition of Done

- [ ] `pyproject.toml`: `mcp>=1.20.0,<2` in `dependencies`; `mcp-codegen = ["typer>=0.12"]` extra
- [ ] `uv lock` succeeds; `uv sync --group dev` clean
- [ ] SDK transport entry points importable (`stdio_client`, `streamablehttp_client`)
- [ ] `calfkit/mcp/__init__.py`, `calfkit/mcp/exceptions.py` (5 classes) exist
- [ ] `calfkit/cli/__init__.py`, `tests/mcp/__init__.py`, `tests/mcp/conftest.py` exist
- [ ] `calfkit/__init__.py` unchanged
- [ ] `tests/mcp/test_baseline_schema_only_dispatch.py` — all 5 test cases pass
- [ ] Existing test suite still green
- [ ] `make check` passes (ruff + ruff-format + mypy strict)
- [ ] Three commits on feature branch; PR opened referencing #158
- [ ] Phase 1 handoff issue/comment created

---

## Pitfalls

1. **Don't forget `uv lock` after `pyproject.toml` edits** — `uv sync` alone errors out on unmatched constraints.
2. **`mcp` may bump `pydantic`** — project pins `pydantic>=2.12.5`; if lock reports downgrade, investigate, don't pass `--upgrade` blindly.
3. **`streamablehttp_client` is the v1.20+ name** — one word, no underscore. IDE autocomplete may suggest the pre-1.20 `streamable_http_client` name.
4. **`tests/__init__.py` must exist** — the sanity test imports `tests.test_tool_errors` helpers. Create as empty file if missing.
5. **`BaseToolNodeSchema` uses `KW_ONLY`** — every field must be passed by keyword; positional raises `TypeError`. `subscribe_topics` must be a non-empty list.
6. **`ToolDefinition` is the vendored type** — import from `calfkit._vendor.pydantic_ai.tools`, not from `pydantic_ai.tools` directly.
7. **`asyncio_mode = "auto"` is set** — don't add `@pytest.mark.asyncio` to async tests; match the established convention in `test_tool_errors.py`.
8. **mypy strict applies to `calfkit/mcp/`** — add no untyped functions in Phase 0 beyond what's specified.
9. **The sanity test failing is an architectural escalation** — don't try to "fix it quietly" by editing `agent.py`. Surface to the team; it may mean D13 `ToolLike` widening needs to land in Phase 0.5.
10. **Don't touch release-please config in Phase 0** — that's Phase 8 work.

---

## Quick reference — driver

```bash
cd /Users/ryan/Projects/calf-sdk
git checkout -b phase-0-mcp-skeleton

# === Commit 1 ===
# Edit pyproject.toml per the diff above
uv lock && uv sync --group dev
uv run python -c "import mcp; from mcp.client.stdio import stdio_client; from mcp.client.streamable_http import streamablehttp_client; print('mcp', mcp.__version__)"
uv run pytest tests/ -x --tb=short -q
git add pyproject.toml uv.lock
git commit -m "chore(mcp): add mcp>=1.20.0 dep and mcp-codegen optional extra"

# === Commit 2 ===
mkdir -p calfkit/mcp calfkit/cli tests/mcp
# Create the four files per the bodies above
uv run python -c "import calfkit.mcp; import calfkit.mcp.exceptions; import calfkit.cli; print('skeleton OK')"
git add calfkit/mcp/ calfkit/cli/ tests/mcp/__init__.py
git commit -m "chore(mcp): scaffold calfkit/mcp/ + calfkit/cli/ packages"

# === Commit 3 ===
# Create tests/mcp/conftest.py + tests/mcp/test_baseline_schema_only_dispatch.py per bodies above
ls tests/__init__.py || touch tests/__init__.py
uv run pytest tests/mcp/test_baseline_schema_only_dispatch.py -v --tb=short
uv run pytest tests/ --tb=short -q
make check
git add tests/mcp/
git commit -m "test(mcp): add Phase 0 baseline test for BaseToolNodeSchema dispatch"

# === PR ===
git push -u origin phase-0-mcp-skeleton
gh pr create --title "Phase 0: MCP adaptor skeleton + BaseToolNodeSchema sanity test" \
  --body "Phase 0 of #158. See docs/mcp-phase-0-kickoff.md."
```
