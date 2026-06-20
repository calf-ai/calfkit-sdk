# Runtime Tool Discoverability Spec

- **Status:** Implemented (2026-06-20) on branch `feat/runtime-tool-discoverability` — built TDD per the §10 build order; review rounds 1 & 2 folded (§13); offline suite + full kafka lane green. Decision recorded in ADR-0013.
- **Date:** 2026-06-19
- **Depends on:** the control-plane substrate (`calfkit/controlplane/`, ADR-0010/0011) and the MCP capability plane migration (ADR-0012). This feature is a second *adopter* of the same substrate, sharing the capability plane with MCP.
- **Relationship to prior specs:** consumes the machinery defined in `docs/designs/control-plane-substrate-spec.md` and reuses the wire model from the (now-migrated) `docs/designs/mcp-capability-discovery-spec.md`. It does **not** introduce a new control plane.

---

## 1. Summary

Today an agent can only use a function tool node by being handed the **live node object** (`tools=[my_tool_node]`), which bakes the tool's JSON schema into the agent's process at construction (`BaseToolNodeDef.tool_bindings`, `calfkit/nodes/tool.py:30`). That couples the agent's deployment to *importing the tool's Python code*, even though the call itself always crosses Kafka.

This feature adds an **identity-only handle** — `Tools(*names)` — that lets an agent reference tool nodes **by name** and discover their schemas **at runtime** from the shared capability control plane (a `ControlPlaneView[CapabilityRecord]` materialized from the compacted `calf.capabilities` topic). It is the exact mirror of how `MCPToolbox` already resolves MCP tools, applied to function tool nodes.

The eager path is **kept** unchanged. `Tools` is purely additive: the same tool node can be consumed eagerly (schema baked in, local arg validation) or by reference (schema discovered, validation deferred to the node + fault rail).

**Naming convention (project-wide).** A `Node` suffix marks a *deployable* host; the bare name is a *reference* to it. So `MCPToolboxNode` (deployable) ↔ `MCPToolbox` (reference), and the planned `ToolNodeDef`→`ToolNode` rename (deployable) ↔ `Tools` (reference). `Tools` is plural because a tool node is single-tool, so referencing several at once is the common case; it has no `Node` suffix because it is not deployable; and it avoids colliding with the vendored `pydantic_ai.Tool`.

---

## 2. Motivation

- **Decouple the agent from tool code.** An agent that references `Tools("add")` needs neither the `add` function nor its module on its deployment path — only the name. The schema travels over the capability plane.
- **Symmetry with MCP.** MCP has *no* eager path: you always hand the agent a handle (`MCPToolbox`) and the schema is discovered. Function tool nodes were the odd one out (always eager). `Tools` makes the two kinds of tools symmetric — both are "callable tools advertised on the capability plane," differing only in cardinality.
- **Reuse, not rebuild.** The control-plane substrate, the worker-owned publisher, the capability view, the per-turn resolution loop, the staleness/schema filtering, and the wire model already shipped for MCP. A tool node becomes one more *advertiser*, and `Tools` becomes one more `ToolSelector`. The net new surface is small.

---

## 3. Goals / Non-goals

### Goals
1. `Tools(*names, names=...)` — a frozen, identity-only handle an agent holds to reference tool nodes by name; resolved per turn against the capability view.
2. Tool nodes advertise their (single, static) tool to the shared `calf.capabilities` plane, **always-on**, via the substrate's `@advertises` mechanism.
3. The discovered binding is **provably equivalent** to the eager binding for the current tool-node surface (§7).
4. De-MCP the resolution vocabulary so it reads correctly for both a multi-tool toolbox and N single-tool nodes (`SelectorResult` plural, cardinality-neutral diagnostics).
5. A **structural** guard against an agent absorbing the wrong kind of advertiser (e.g. `Tools` pulling a whole MCP toolbox), via a worker-stamped `node_kind` discriminator on the control-plane record.
6. Zero new worker wiring: the feature rides the existing write-side (`@advertises` → publisher) and read-side (`_tool_selectors` → view) auto-registration.

### Non-goals
- **No `strict` mode.** Removed from MCP in ADR-0012; not added for `Tools`. Unresolved selections warn and degrade.
- **No open-ended discovery.** An agent discovers exactly the names it declares. There is no "discover whatever tool nodes are online" mode (§14).
- **No new control plane / topic / record type.** Tool nodes share `calf.capabilities` and `CapabilityRecord`.
- **No protocol merge.** Collapsing `ToolProvider` + `ToolSelector` is an orthogonal refactor, out of scope (L9).
- **No opt-in advertising in v1.** Every tool node advertises (L7). Opt-in is designed and recorded as future work (§14.1).
- **No tool-node argument-schema fidelity beyond name/description/JSON-schema** (§7 boundary).

---

## 4. Background — the machinery being reused (grounded)

| Piece | Location | Role |
|---|---|---|
| `ControlPlaneRecord` / `ControlPlaneStamp` | `calfkit/controlplane/records.py:14,33` | Record base: identity-as-key + boot/liveness/cadence stamp + `schema_version`. **This spec adds `node_kind` to the stamp (§6.4).** |
| `@advertises(topic, record=...)` | `calfkit/controlplane/advert.py:52` | Class-body marker: binds a node *type* to one topic + record schema; factory `(self, stamp) -> record`. |
| `ControlPlanePublisher` | `calfkit/controlplane/publisher.py:42` | One per worker; fail-loud first publish, per-advert-resilient heartbeat loop, ordered tombstone. **Stamps `node_kind` (§6.4).** |
| `ControlPlaneView[R]` | `calfkit/controlplane/view.py:64` | Read side: instance-keyed (`node_id × worker_id`) → collapsed `get(node_id)`, staleness + schema-version filtering, health passthrough. **Gains a mixed-`node_kind` warning (§6.4, C1).** |
| `CapabilityRecord` / `CapabilityToolDef` | `calfkit/models/capability.py:48,38` | Wire content: `dispatch_topic`, `tools[name/description/json_schema]`, `content_updated_at` (+ inherited `node_kind`). |
| `resolve_capability` / `record_to_bindings` | `calfkit/models/capability.py:116,79` | Resolution kernel: `view.get(id)` → validator-less `ToolBinding`s. **Gains `expected_kind` (§6.4).** |
| `ToolBinding` / `ToolProvider` / `ToolSelector` / `SelectorResult` | `calfkit/models/tool_dispatch.py:21,58,96,71` | The agent's tool-type machinery. |
| `_node_kind` | `calfkit/nodes/base.py:483` (used in `HDR_EMITTER_KIND`); `mcp_toolbox.py:41` (`"toolbox"`); `tool.py:25` (`"tool"`) | Each node type's kind. Already exists; `node_kind` on the stamp is sourced from it. |
| Worker read-side wiring | `calfkit/worker/worker.py:171-184` | Registers ONE `ControlPlaneView[CapabilityRecord]` resource iff any hosted node has `_tool_selectors`. |
| Worker write-side wiring | `calfkit/worker/worker.py:232-253` | Registers the publisher + one writer per topic iff any hosted node declares an `@advertises`. |
| Agent resolution | `calfkit/nodes/agent.py:195-258` | Per turn: read the view, resolve each selector, merge bindings (collision → existing wins), warn/degrade. |

MCP's flow today: `MCPToolboxNode` declares `@advertises(topic=CAPABILITY_TOPIC, record=CapabilityRecord)` (`mcp_toolbox.py:132`); the `MCPToolbox` handle (`name` + `include`, no `strict`) resolves via `resolve_capability(view, name, include=...)` (`mcp_toolbox.py:246`). `Tools` and the tool node mirror both halves.

---

## 5. Design overview

Four changes, all additive except the one required identity rename:

1. **A `node_kind` discriminator on the control-plane stamp.** Worker-stamped from each node's `_node_kind` (so it costs no per-advertiser code). It gives the over-pull guard (§6.3) a structural basis and lets the view observe heterogeneous-owner collisions (§6.4, C1).
2. **Tool nodes become advertisers.** `BaseToolNodeDef` declares an `@advertises` factory that publishes its single static tool to `calf.capabilities`, keyed by its `node_id`. Always-on.
3. **Tool-node identity becomes the tool name.** Drop the `tool_` `node_id` prefix so `node_id == LLM tool name == capability key`; let `agent_tool(func, *, name=...)` and `create_tool_node(..., *, name=...)` override it.
4. **`Tools` selector + de-MCP'd `SelectorResult`.** A new `ToolSelector` that does N independent capability lookups (one per name, `expected_kind="tool"`) and aggregates; `SelectorResult` becomes a cardinality-neutral, fully-immutable value with plural diagnostics.

End-to-end flow (discovered path):

```
deploy time     tool node `add` (node_id="add", _node_kind="tool", subscribe="tool.add.input")
                  └─ @advertises → worker publisher stamps node_kind="tool" → set("add", worker_id,
                       CapabilityRecord{node_kind:"tool", tools:[add], dispatch_topic:"tool.add.input"})
                                                                                  ↓ calf.capabilities (compacted)
agent worker    ControlPlaneView[CapabilityRecord] (auto-registered: agent has a Tools selector)
                                                                                  ↓
agent turn      Tools("add").resolve_tools(view) → resolve_capability(view,"add",expected_kind="tool")
                  → view.get("add") → node_kind matches → ToolBinding(tool_def=add, dispatch_topic=..., validator=None)
                  → merged into tools_registry → advertised to the LLM
model calls add → Call(dispatch_topic="tool.add.input", body=ToolCallRef) → tool node runs, validates, returns via reply slot
                  (bad args → function_schema raises ValidationError → fault rail → typed fault back to the agent)
```

---

## 6. Detailed design

### 6.1 Tool node as advertiser (always-on)

`BaseToolNodeDef` already extends `BaseNodeDef`, which carries `AdvertRegistryMixin` (`base.py:221`), and already holds `tool_schema: ToolDefinition` (`tool.py:28`) and `subscribe_topics`. The factory mirrors MCP's, reading static data instead of a cached session list. **It does not stamp `node_kind` itself** — that rides on the stamp (§6.4), so it flows in via `stamp.model_dump()`:

```python
# calfkit/nodes/tool.py — on BaseToolNodeDef
@advertises(topic=CAPABILITY_TOPIC, record=CapabilityRecord)
def _capability_advert(self, stamp: ControlPlaneStamp) -> CapabilityRecord:
    """Advertise this node's single tool on the shared capability plane.

    Static-schema advertiser: the tool surface is fixed at construction, so the
    factory reads ``tool_schema`` directly (no session, no cache to prime).
    ``content_updated_at`` is the process boot time (``stamp.started_at``): the
    content never changes, so a stable, non-``now()`` value satisfies the
    substrate's no-``now()``-in-factory contract. ``node_kind`` rides on the
    stamp (the worker stamps it), so it is not set here.
    """
    if not self.subscribe_topics:  # named guard: empty subscribe_topics would IndexError mid-publish
        raise RuntimeError(f"tool node {self.node_id!r} has no subscribe_topics; cannot advertise a dispatch topic")
    return CapabilityRecord(
        **stamp.model_dump(),  # started_at, last_heartbeat_at, heartbeat_interval, node_kind, schema_version
        dispatch_topic=self.subscribe_topics[0],
        tools=[
            CapabilityToolDef(
                name=self.tool_schema.name,
                description=self.tool_schema.description,
                parameters_json_schema=self.tool_schema.parameters_json_schema,
            )
        ],
        content_updated_at=stamp.started_at,
    )
```

Notes:
- **Always-on.** The `@advertises` lives on the base type, so every `ToolNodeDef` advertises (L7). Consequence: a worker hosting any tool node stands up the control-plane writer + publisher at boot (fail-loud) — see §8. The opt-in alternative is fully designed in §14.1.
- **Fail-safe at publish.** The factory reads only static fields; the one non-content read (`subscribe_topics[0]`) is guarded with a named error rather than letting an `IndexError` escape mid-publish.
- **`content_updated_at = stamp.started_at`.** Correct for a static schema; never moves tick-to-tick.
- **Replicas are trivially equivalent.** `CapabilityRecord`'s collapse precondition (equivalent content across replicas) is satisfied by construction for tool nodes: the schema is derived from the same `func`, so two replicas of the same tool node advertise byte-identical content. (Two *different* functions deployed under the same `name` would violate it — but that is the identity-collision case of §8, not a replica case.)
- **Tombstone on clean shutdown** is handled by the shared publisher (`publisher.py:75`). No tool-node-specific lifecycle code.

### 6.2 Identity & naming

The capability key is the node's `node_id` (the view is `get(node_id)`). For `Tools("add")` → `view.get("add")` to resolve, the tool node's `node_id` **must equal the tool name** the user references. Today `create_tool_node` sets `node_id=f"tool_{func.__name__}"` (`tool.py:72`) while the LLM-facing tool name is `func.__name__` — they diverge.

**Change `create_tool_node` / `agent_tool` so the effective tool name drives all three identities:** the `node_id`, the LLM-facing `tool_def.name`, and the topic names. The function name is the default; `name=` overrides it (the disambiguation knob for the cluster-unique-identity contract, §8). `name=` is accepted by **both** entry points; an empty `name=""` is rejected (it must not silently fall back to the function name and quietly violate the identity contract). `agent_tool` is usable three ways — bare (`@agent_tool`), with args (`@agent_tool(name=...)`), or as a direct call (`agent_tool(fn, name=...)`) — via the standard optional-argument decorator pattern (`func=None` → return the builder; else build now).

```python
@classmethod
def create_tool_node(cls, func, subscribe_topics, publish_topic, *, name=None) -> Self:
    if name is not None and not name:
        raise ValueError("name must be non-empty when given")
    effective = name or func.__name__
    tool = Tool(func, name=effective)          # pydantic_ai Tool name override (tools.py:294,375)
    return cls(
        node_id=effective,                     # capability key == LLM tool name (== node.name property)
        tool_schema=tool.tool_def,             # tool_def.name == effective
        subscribe_topics=subscribe_topics,
        publish_topic=publish_topic,
        _tool=tool,
    )

def agent_tool(func=None, *, name=None) -> ToolNodeDef:   # bare / with-args / call (overloaded)
    if name is not None and not name:
        raise ValueError("name must be non-empty when given")

    def _build(fn):
        effective = name or fn.__name__        # computed once; topics + identity share one source of truth
        return ToolNodeDef.create_tool_node(
            func=fn, name=effective,
            subscribe_topics=f"tool.{effective}.input",
            publish_topic=f"tool.{effective}.output",
        )

    return _build if func is None else _build(func)       # (name=…) → return decorator; else build now
```

`pydantic_ai.Tool` accepts a `name` override (verified `tools.py:294`, `self.name = name or function.__name__` at `tools.py:375`), so `tool_def.name`, `node_id`, and the topics stay in lockstep. The `tool_` prefix is removed (cosmetic — one construction site, nothing parses it). `node.name`/`node.id` are read-only aliases of `node_id` (`base.py:1748-1753`), so the capability key is equivalently the node's `name`.

> **Breaking change.** Dropping the `tool_` prefix changes the `node_id`, which appears on the wire in envelope headers (`HDR_EMITTER`), fault-rail origin fields, and logs. Pre-1.0, this is a clean break (§9).

### 6.3 The `Tools` handle

A frozen, deployment-free, identity-only handle. No connection params, no schema, no `strict`, no `include` (each name *is* an explicit selection). It dedupes names (so `Tools("add", "add") == Tools("add")`), rejects the mixed positional+keyword call, and binds only `node_kind == "tool"` records (§6.4) so it can never silently absorb a multi-tool toolbox.

```python
# calfkit/nodes/tool.py (co-located with the tool node it references; exported from `calfkit`)
@dataclass(frozen=True)
class Tools:
    """Identity-only handle to one or more function tool nodes, resolved per agent
    turn against the capability view. The call-side counterpart to a deployed tool
    node (mirrors ``MCPToolbox``): constructible anywhere with just the tool names —
    no schema, no import of the tool's code. Each name is a tool node's identity (its
    ``node_id``, which equals the LLM-facing tool name). Frozen value semantics:
    equal handles compare and hash equal; names are order-preserving-deduped.

    NOTE — deliberate divergence from ``MCPToolbox``'s shape: ``MCPToolbox`` is a
    plain frozen dataclass (declared fields + ``__post_init__``). ``Tools`` needs a
    custom ``__init__`` to accept varargs (``Tools("add", "subtract")``), so it keeps
    ``@dataclass(frozen=True)`` only for ``__eq__``/``__hash__``/``__repr__`` over
    ``names``. The varargs ergonomics (L1) require this.
    """

    names: tuple[str, ...]

    def __init__(self, *positional: str, names: Sequence[str] | None = None) -> None:
        if positional and names is not None:
            raise ValueError("pass tool names positionally or via names=, not both")
        collected = tuple(positional) if positional else tuple(names or ())
        collected = tuple(dict.fromkeys(collected))  # order-preserving dedupe
        if not collected:
            raise ValueError("Tools requires at least one tool name")
        if any(not n for n in collected):
            raise ValueError("Tools names must be non-empty")
        object.__setattr__(self, "names", collected)

    def resolve_tools(self, view: CapabilityLookup) -> SelectorResult:
        bindings: list[ToolBinding] = []
        missing: list[str] = []
        invalid: list[str] = []
        wrong_kind: list[str] = []
        for name in self.names:
            r = resolve_capability(view, name, expected_kind="tool")  # structural over-pull guard
            bindings.extend(r.bindings)
            missing.extend(r.missing_targets)
            invalid.extend(r.invalid_targets)
            wrong_kind.extend(r.wrong_kind_targets)
        return SelectorResult(
            bindings=tuple(bindings),
            missing_targets=tuple(missing),
            invalid_targets=tuple(invalid),
            wrong_kind_targets=tuple(wrong_kind),
        )
```

Both `Tools("add", "subtract")` (varargs, the common case) and `Tools(names=[...])` (pre-built list) work; mixing them raises. `Tools` satisfies the existing `ToolSelector` protocol (`tool_dispatch.py:96`), so it flows through `split_tool_declarations` into `agent._tool_selectors` with **no change to the agent's `tools=` union type**.

### 6.4 Wire model, resolution kernel & de-MCP'd `SelectorResult`

**`node_kind` on the stamp (the canonical home).** The kind discriminator is worker-stamped node metadata, exactly like `started_at`/`heartbeat_interval`, so it belongs on `ControlPlaneStamp`, not on `CapabilityRecord`. This makes it (a) generic — every control-plane record carries it, so the generic `ControlPlaneView` can use it; (b) free — it flows into every advert via `stamp.model_dump()`, so no advert factory changes; (c) identity-safe — it is a *category*, not identity, so it sits in the value alongside the other stamp metadata without violating "identity is the key, not the value."

```python
# calfkit/controlplane/records.py
class ControlPlaneStamp(BaseModel):
    model_config = ConfigDict(frozen=True)
    started_at: AwareDatetime
    last_heartbeat_at: AwareDatetime
    heartbeat_interval: float
    node_kind: str            # NEW: worker-stamped node kind ("tool"/"toolbox"/"agent"/...), sourced from node._node_kind.
                              # Required, like the sibling stamp fields (fails loud if ever unstamped). Adding it is a
                              # BREAKING wire change: a record written before this field existed will not decode, so the
                              # calf.capabilities topic must be recreated on upgrade. Pre-1.0, no deployments to preserve,
                              # so no compat default (a default would mask a missing stamp and conflate legacy with "node").

# calfkit/controlplane/publisher.py — _publish_one
stamp = ControlPlaneStamp(
    started_at=self._started_at,
    last_heartbeat_at=now,
    heartbeat_interval=self._config.heartbeat_interval,
    node_kind=node._node_kind,   # the worker knows each hosted node's kind
)
```

`CapabilityRecord` inherits `node_kind` automatically (no field added here):

```python
class CapabilityRecord(ControlPlaneRecord):   # ControlPlaneRecord(ControlPlaneStamp) → has node_kind
    schema_version: int = CAPABILITY_SCHEMA_VERSION
    dispatch_topic: str
    tools: list[CapabilityToolDef]
    content_updated_at: AwareDatetime
```

> `node_kind` (the *node* category: tool/toolbox) is distinct from `ToolDefinition.kind` (the *tool-call* category: function/unapproved). They are unrelated; adding `node_kind` has **no effect on the §7 fidelity parity**, which is purely about `ToolDefinition` fields.

**Wire compatibility — breaking change (accepted; pre-1.0, no deployments).** `node_kind` is **required**, like the sibling stamp fields. A `CapabilityRecord` written before this field existed will fail to decode (`model_validate_json` raises; ktables' poison-tolerance then *skips* it — `kafka_table.py`), so the compacted `calf.capabilities` topic must be **recreated/drained on upgrade**. This is a deliberate clean break rather than a compat default: a defaulted `node_kind="node"` would mask a publisher that forgot to stamp the field and would conflate "legacy/unknown" with the real base-node kind — and with no live deployments to preserve, the shim earns nothing (and a shim runs against the project's hard-breaks-over-compat stance). **No `CAPABILITY_SCHEMA_VERSION` bump** — the topic is recreated with the new shape, so `schema_version` stays `1`. `node_kind` is typed `str` (not the `NodeKind` Literal) so a future *additive* kind value from a newer writer still decodes on an older reader.

**C1 — heterogeneous-owner collision (observable, documented).** Tool nodes and MCP toolboxes share the `calf.capabilities` `node_id` keyspace. A `node_id` collision between *different owners* (a tool node and a toolbox, or two unrelated tool nodes) would land them in one group as different `worker_id` members, and the collapsed `get()` (max-by-heartbeat, `view.py:90`) would flap the agent's surface between two owners tick-to-tick. This is **a facet of the existing global `node_id`-uniqueness contract** — `node_id` already must be unique cluster-wide because it drives return topics and consumer groups (`worker.py:299,309`); a collision is an already-broken deployment, and the flap is one more symptom. So it is documented, not policed (§8). For observability, the generic view logs (dedup'd, like its schema-skip warning) when one group holds members of differing `node_kind`:

```python
# calfkit/controlplane/view.py — inside _live_members, after collecting live members
kinds = {r.node_kind for r in live.values()}
if len(kinds) > 1:
    self._log_mixed_kind_once(node_id, kinds)   # dedup'd warning: duplicate node_id across owner kinds
```

**This warning is partial — cross-kind only.** It fires only when one `node_id` group holds members of *differing* `node_kind` (e.g. a tool node and a toolbox sharing a name). A **same-kind** collision — two *different* tool functions deployed under one `name`, both `node_kind="tool"` (the §6.1 replica-violation / §8 identity-collision case) — yields a single-element kind set and stays **silent**; it is covered only by the documented global `node_id`-uniqueness contract (§8) and its other symptoms (shared return topic / consumer group). Detecting same-kind/different-content collisions would require content comparison, which the collapsed view deliberately does not do. The dedup key is `(node_id, frozenset(kinds))` (mirroring `_log_schema_skip`'s `(node, worker, version)`), so a changed collision set re-warns and the warning self-clears once resolved.

**`SelectorResult` — cardinality-neutral, fully immutable.** `bindings` becomes a `tuple` so the whole result is a true frozen value (a `list` in a frozen dataclass is only shallow-frozen and unhashable). `toolbox_id`/`missing_toolbox`/`invalid_record` are replaced by plural, generic fields; `wrong_kind_targets` is added for the over-pull guard:

```python
@dataclass(frozen=True)
class SelectorResult:
    """Outcome of resolving one ToolSelector against the capability view.

    Cardinality-neutral: a selector may resolve one target (a tool node), one
    target with many tools (an MCP toolbox), or many targets (a `Tools` handle).
    The view owns staleness + schema-version filtering, so the unresolved cases
    are: an absent target, an include-pinned tool missing from a present record
    (MCP `include` only), a present-but-unexpandable record, or a present record
    of the wrong node kind (e.g. `Tools` resolving a toolbox).
    """

    bindings: tuple[ToolBinding, ...] = ()
    missing_targets: tuple[str, ...] = ()      # requested identities with no live record
    missing_tools: tuple[str, ...] = ()        # MCP `include`-pinned tool names absent from a present record (MCP-only)
    invalid_targets: tuple[str, ...] = ()      # identities whose record was present but failed binding expansion
    wrong_kind_targets: tuple[str, ...] = ()   # identities present but of the wrong node_kind for this selector

    @property
    def unresolved(self) -> bool:
        return bool(self.missing_targets or self.missing_tools or self.invalid_targets or self.wrong_kind_targets)
```

**`resolve_capability` — single-target kernel, `expected_kind` guard.** Param renamed `toolbox_id` → `target_id`; gains `expected_kind`. **The narrow `except ValidationError` is deliberate** (a non-validation error is a logic bug and must propagate, fail-loud) — keep that comment through the de-MCP cleanup, don't widen it to `except Exception`:

```python
def resolve_capability(view, target_id, *, include=None, expected_kind=None) -> SelectorResult:
    record = view.get(target_id)
    if record is None:
        return SelectorResult(missing_targets=(target_id,))
    if expected_kind is not None and record.node_kind != expected_kind:
        return SelectorResult(wrong_kind_targets=(target_id,))
    try:
        bindings = record_to_bindings(record)
    except ValidationError:
        # tolerant-reader bad-data path ONLY (e.g. empty dispatch_topic fails ToolBinding min_length);
        # a non-ValidationError here is a logic bug — let it propagate (fail loud).
        return SelectorResult(invalid_targets=(target_id,))
    missing_tools: tuple[str, ...] = ()
    if include is not None:
        wanted = set(include)
        bindings = [b for b in bindings if b.name in wanted]
        missing_tools = tuple(n for n in include if n not in {b.name for b in bindings})
    return SelectorResult(bindings=tuple(bindings), missing_tools=missing_tools)
```

- `MCPToolbox.resolve_tools` → `resolve_capability(view, self.name, include=self.include, expected_kind="toolbox")` (symmetric protection: an MCP handle won't bind a tool-node record either).
- `Tools.resolve_tools` → loop with `expected_kind="tool"`; never uses `include`; `missing_tools` always empty.

The full de-MCP cleanup (L11) also generalizes the MCP-flavored docstrings in `capability.py` (module docstring, `record_to_bindings` "the toolbox's MCP server is the argument validator", `CapabilityLookup.get` param) so the kernel reads correctly for both adopters.

### 6.5 Agent-side resolution

`Tools` rides the existing path (`agent.py:195-258`): it lands in `_tool_selectors` via `split_tool_declarations`; per turn `_resolve_selector_tools` reads `CAPABILITY_VIEW_RESOURCE_KEY`, calls `selector.resolve_tools(view)`, and merges `result.bindings` into `tools_registry`.

Required edits (these are **attribute-level**, not "pure log-text" — the old `SelectorResult` fields cease to exist, so the code won't compile until updated):
- **Unresolved warning** now reads the plural fields (`missing_targets`, `missing_tools`, `invalid_targets`, `wrong_kind_targets`).
- **Collision log — provenance via the selector, not the result.** The old log read `result.toolbox_id` to name the source; that field is gone (a `Tools` result aggregates N targets). Instead log the `selector` object, which is in scope in the merge loop and is *better* provenance:

```python
for selector in self._tool_selectors:          # selector in scope
    result = selector.resolve_tools(view)
    ...
    for binding in result.bindings:
        if binding.name in tools_registry:
            logger.error("agent=%s discovered tool %r from selector %r collides with an existing tool; existing wins",
                         self.name, binding.name, selector)
            continue
        tools_registry[binding.name] = binding
```

(For an `MCPToolbox` the selector repr carries `name="github"`; for `Tools` it carries the name list — strictly more informative than the old single `toolbox_id`.)

- **Degrade (unchanged policy):** missing view, degraded/failed view, or `result.unresolved` → warn and continue. No raise.

**Validation semantics (the eager/discovered divergence, by design):**
- *Eager:* `ToolBinding.validator = validate_call_args` (`tool.py:41`) → the agent validates LLM args **before** dispatch; a schema mismatch is corrected locally with no Kafka hop.
- *Discovered:* `validator=None` → the agent dispatches unvalidated. Two distinct outcomes at the tool node:
  - a **schema-mismatch** (`function_schema` raises `ValidationError`, `tool.py:110`) escapes to the chokepoint → `on_node_error` → fault rail → typed fault back to the agent;
  - an in-body **`ModelRetry`** is *not* a fault — it is rendered at origin to a `calf.retry`-marked reply (`tool.py:116-121`), a model-visible recoverable, identical on both eager and discovered paths (it's body behavior, not arg validation).

So the discovered path's *arg-validation* failure rides the rail (one Kafka hop later than the eager path's local correction); in-body retries behave identically on both paths.

**`add_tools` timing caveat (carried over from MCP).** `Agent.add_tools` (`agent.py:518`) added *after* the worker's `register_handlers` has snapshotted `_tool_selectors` will **not** get a capability view (the read-side gate already ran), so its `Tools`/`MCPToolbox` selectors silently degrade to no-discovery. Declare discovery selectors at agent construction. (Pre-existing MCP constraint; `Tools` inherits it.)

### 6.6 Worker wiring (zero new code)

- **Write side:** `_maybe_register_control_plane` (`worker.py:245`) collects `type(node)._adverts`. With `@advertises` on `BaseToolNodeDef`, every hosted tool node contributes the capability advert automatically — no new branch.
- **Read side:** `_maybe_register_capability_view` (`worker.py:180`) registers the view iff any hosted node has `_tool_selectors`. A `Tools` selector trips the same gate. One worker-level `ControlPlaneView[CapabilityRecord]`, shared with MCP.

---

## 7. Wire model reuse & the fidelity boundary

`Tools` reuses `CapabilityRecord` / `CapabilityToolDef` for tool content unchanged. For the **current** tool-node surface, the discovered `ToolBinding` is **field-for-field identical** to the eager one. `record_to_bindings` builds `ToolDefinition(name, description, parameters_json_schema)` and lets the other six fields default (`capability.py:87`); a bare `Tool(func).tool_def` carries the same defaults (verified empirically):

| `ToolDefinition` field | discovered (defaulted) | eager `Tool(func).tool_def` | match |
|---|---|---|---|
| `strict` | `None` | `None` | ✓ |
| `sequential` | `False` | `False` | ✓ |
| `kind` | `'function'` | `'function'` (`requires_approval=False`) | ✓ |
| `metadata` | `None` | `None` | ✓ |
| `timeout` | `None` | `None` | ✓ |
| `outer_typed_dict_key` | `None` | `None` | ✓ |

> **Boundary to track.** `CapabilityToolDef` carries only name/description/JSON-schema. `Tool.__init__` *accepts* `strict`, `sequential`, `requires_approval`, `metadata`, `timeout` (`tools.py:300`), but `create_tool_node` exposes none today — which is *why* dropping them is lossless. **If the tool-node surface ever grows to expose those knobs, `CapabilityToolDef` must grow matching fields, or discovery silently drops them** (most consequentially `requires_approval` → `kind='unapproved'`, the human-in-the-loop gate). Such additions are additive (tolerant reader). A unit test asserts eager/discovered `ToolDefinition` parity so a future knob that breaks it fails loudly (§11).

---

## 8. Operational contract

- **Identities are cluster-wide unique (existing contract).** `node_id` (= `name` = capability key) must be unique across **all** nodes cluster-wide — this was already required (it drives return topics and consumer groups, `worker.py:299,309`). Dropping the `tool_` prefix means a tool node `node_id` can now collide with an **agent** (or any node) `node_id` — e.g. an agent named `add` and a tool named `add` — where `tool_add` previously could not. A collision is a broken deployment with several symptoms; on the capability plane it manifests as a flapping tool surface (§6.4 C1), now observable via the mixed-`node_kind` warning. **Documented, not policed** (framework rule). `name=` on `agent_tool`/`create_tool_node` is the disambiguation knob.
- **Always-on advertising ⇒ control-plane boot dependency.** Any worker hosting a tool node stands up the `calf.capabilities` writer + publisher; the first publish is fail-loud (`publisher.py:62`). The topic must exist (provisioning on, dev/CI) or be ops-provisioned (prod), and brokers must be reachable, or the worker won't boot — even if no agent ever discovers that tool. Accepted (L7); §14.1 is the escape hatch.
- **Topic & compaction.** `calf.capabilities`, `cleanup.policy=compact`, delegated to ktables `ensure_topic` (dev/CI) or ops (prod) — same as MCP. Documented, not policed.
- **Catch-up gating.** The view's `@resource` awaits `view.start()` during resource setup (before serving), so an agent never resolves against a half-built view. `Tools` inherits this.
- **Staleness.** A crashed tool node's record goes stale after `3 × heartbeat_interval` and is hidden by the view; a clean shutdown tombstones immediately.

---

## 9. Backwards compatibility / breaking changes

1. **`node_id` of tool nodes changes** (`tool_add` → `add`). On the wire (headers, fault origin) and logs. Pre-1.0 hard break.
2. **`SelectorResult` redesign** — `toolbox_id` removed; `missing_toolbox`→`missing_targets`; `invalid_record`→`invalid_targets`; add `wrong_kind_targets`; `bindings` → `tuple`. Touches the shared MCP path. Internal types; no user-facing wire impact.
3. **`resolve_capability(toolbox_id=...)` → `target_id`** + new `expected_kind`. Public-ish kernel; pre-1.0 rename.
4. **`ControlPlaneStamp` gains `node_kind`** (substrate touch: `records.py`, `publisher.py`, `view.py`), **required** like the sibling stamp fields. This is a **breaking wire change**: records written before this field existed won't decode (ktables skips them), so the `calf.capabilities` topic must be recreated on upgrade. Accepted — pre-1.0, no live deployments; a compat default is explicitly rejected (it would mask a missing stamp and conflicts with the project's hard-breaks stance). No `CAPABILITY_SCHEMA_VERSION` bump (fresh topic, new shape; `schema_version` stays `1`).
5. **`agent_tool` / `create_tool_node` gain `name=`** (additive); `agent_tool` drops the id prefix (breaking per #1).

No wire-format break to `CapabilityRecord`/`CapabilityToolDef` content (MCP and tool nodes interoperate immediately).

**Concrete migration checklist (from the blast-radius review — nothing parses the prefix, so it is mechanical):**
- Production: `tool.py:72` (the prefix), `tool.py:130-136`/`61-77` (`name=`), `agent.py` resolution logs (SelectorResult fields), `mcp_toolbox.py:246` (`expected_kind`), substrate touch (`records.py`, `publisher.py`, `view.py`).
- Node-id test assertions (8): `tests/test_run_loader.py:25,33,41,91,110,138`, `tests/test_run_serve.py:48,122` (all fed by `tests/provisioning_cli_nodes.py`).
- `SelectorResult`-field assertions (~14): `tests/test_tool_selector.py` (many), `tests/test_mcp_toolbox.py:95` (`toolbox_id` equality — needs rewrite).
- **`node_kind` required-field fallout (new).** Because `node_kind` is required, **every** test that constructs a `ControlPlaneStamp`/`ControlPlaneRecord`/`CapabilityRecord` must pass it, and the field-set assertions break: `tests/test_controlplane_records.py:49,53` (add `node_kind`); the `_stamp`/`make_stamp`/`make_record` helpers in `tests/test_controlplane_records.py`, `tests/test_controlplane_advert.py`, `tests/test_mcp_toolbox_publisher.py`, `tests/test_capability_models.py`, `tests/test_tool_selector.py`, `tests/test_mcp_toolbox.py`, `tests/test_controlplane_view.py`, `tests/test_controlplane_worker_wiring.py`, `tests/test_controlplane_publisher.py`. Resolution fixtures must set the kind the selector expects (`"tool"` for `Tools` tests, `"toolbox"` for MCP tests) or they resolve as `wrong_kind`. Add a publisher test asserting the stamp carries `node._node_kind`.
- **Export:** add `Tools` to `calfkit/__init__.py.__all__` (today it exports `ToolNodeDef`, `agent_tool`, no `Tools`).
- Docs (L11 de-MCP sweep): `capability.py` docstrings + `docs/adr/0012-*`, `docs/designs/{mcp-capability-substrate-migration-plan,capability-plane-migration-and-ops-spec,control-plane-substrate-spec,node-presence-substrate-spec,mcp-capability-discovery-spec}.md`.
- Not affected (verified false positives): `tool_call_id`/`tool_name`/`tool_calls` metadata; `ConsumerNode` ids; `examples/deprecated/` (already-dead import path).

---

## 10. Build order (TDD)

1. **`node_kind` on the substrate** — add required `node_kind: str` to `ControlPlaneStamp`, stamp it in the publisher from `node._node_kind`, add the view's mixed-kind warning. *Tests:* the publisher stamps `node_kind` from `node._node_kind` (`MCPToolboxNode`→`"toolbox"`, tool node→`"tool"`); the view warns once per `(node_id, frozenset(kinds))` on a cross-kind group (and does NOT warn on a same-kind group).
2. **De-MCP `SelectorResult` + `resolve_capability`** — plural/immutable fields, `expected_kind`, `bindings` tuple, kept fail-loud comment. Update `MCPToolbox.resolve_tools` (`expected_kind="toolbox"`) and agent logs (selector-provenance). *Tests:* MCP resolution adapted; new `missing_targets`/`invalid_targets`/`wrong_kind_targets` cases.
3. **Drop the `tool_` prefix + `name=`** on `agent_tool`/`create_tool_node` (reject empty). *Tests:* `node_id == effective`, topics derive from it, `tool_def.name == effective`, override on both entry points, empty rejected.
4. **Tool node advertises** (`@advertises` on `BaseToolNodeDef`, guarded `subscribe_topics[0]`). *Tests:* factory builds a valid record from `tool_schema`+stamp; `content_updated_at == started_at`; worker auto-registers publisher/writer.
5. **`Tools` selector** — varargs + `names=`, dedupe, reject-mixed, reject-empty, `expected_kind="tool"`. *Tests (dict view):* single/multi name; `names=` form; dedupe; mixed-call raises; missing → `missing_targets`; toolbox key → `wrong_kind_targets`; satisfies `ToolSelector`; lands in `_tool_selectors`.
6. **Eager/discovered parity test** (§7).
7. **Kafka-lane roundtrip** (mirror `test_mcp_roundtrip_kafka.py`): tool node in one worker, agent with `tools=[Tools("add")]` in another; discover → dispatch `add(2,3)` → `5` returns. Plus a bad-args case asserting the fault-rail path.

---

## 11. Testing strategy

- **Unit** — `CapabilityLookup` is a `Protocol` satisfied by a `dict` (`capability.py:104`), so all resolution logic is broker-free.
- **Parity** — eager vs discovered `ToolDefinition` equality (§7 guard).
- **Scenarios** (from review) — duplicate names dedupe; `Tools(...)`/`Tools(names=...)` and the rejected mixed call; rejected empty `name=`; `Tools` + eager same tool (eager wins, validator preserved); `Tools` against `view is None` / degraded view; `wrong_kind_targets` (Tools → toolbox key); `expected_kind` for both selectors; the publisher stamps `node_kind` from `_node_kind`; cross-kind view warning fires / same-kind does not; `name=` collision (two nodes same `name`); `add_tools` post-registration degradation; replica content-equivalence; eager/discovered `ToolDefinition` parity.
- **Worker wiring** — tool node trips write-side; `Tools` agent trips read-side.
- **Kafka lane** — full roundtrip + fault path (`kafka` marker, in CI).
- **Coverage** — 100% on new code (`/pytest-coverage`).

---

## 12. Locked decisions

| # | Decision | Rationale |
|---|---|---|
| L1 | Plural `Tools(*names, names=...)`, dedupe, reject mixed call. | Tool nodes are single-tool; a plural handle restores MCP-like conciseness and is one type. |
| L2 | Identity = the tool name (no `tool_` prefix); `node_id == tool name == capability key`. `name=` (non-empty) on `agent_tool` + `create_tool_node`. | One name everywhere; required for `Tools(name)` to resolve. |
| L3 | No `strict`; warn-and-degrade. | Mirrors MCP (ADR-0012 D5). |
| L4 | Keep both eager and discovered paths. | Eager = zero-infra + local validation; discovered = decoupled + fault-rail. Two handles, one node. |
| L5 | `Tools` and `MCPToolbox` separate types, shared `resolve_capability` kernel; each binds only its own `node_kind` (`expected_kind`). | Distinct mental models; structural over-pull guard. |
| L6 | Share `calf.capabilities` / `CapabilityRecord`; no new topic/record/view. | `CapabilityRecord` covers all needs (§7); one view aggregates both. |
| L7 | Always-on advertising in v1. | Simplest; consistent with MCP. Cost: boot dependency (§8). Opt-in is the designed escape hatch (§14.1). |
| L8 | `content_updated_at = stamp.started_at` for tool nodes. | Static schema ⇒ stable, non-`now()`. |
| L9 | Keep the trio (`ToolBinding`/`ToolProvider`/`ToolSelector`); `Tools` is a third `ToolSelector`. No protocol merge. | Slots in cleanly; merge is a separate optional refactor (§14.2). |
| L10 | Handle name `Tools`; deployable `ToolNode` (planned rename of `ToolNodeDef`). | Project convention: `Node` suffix = deployable, bare = reference. Rules out `ToolNodes` (Node suffix). Avoids vendored `Tool` collision. |
| L11 | Full de-MCP vocabulary cleanup (`SelectorResult` fields, `resolve_capability(target_id=)`, MCP-flavored docstrings — including docstring *bodies*, e.g. `capability.py:84` "the toolbox's MCP server is the argument validator"). | Makes the shared plane read correctly for both adopters. |
| L12 | `Tools` in `calfkit/nodes/tool.py`, exported from `calfkit`; no `.ref()` convenience. | Co-located; `.ref()` low-value (`Tools` takes names directly). |
| L13 | Required `node_kind: str` discriminator on `ControlPlaneStamp` (worker-stamped from `_node_kind`); breaking wire change (recreate `calf.capabilities`), no compat default. | Canonical home for worker-stamped node metadata; powers the over-pull guard (L5) and C1 observability; free via the stamp. Required (not defaulted) matches the sibling stamp fields and fails loud on a missing stamp; pre-1.0 hard break, no deployments to preserve. |
| L14 | C1 (heterogeneous-owner collision): document + tie to the existing global `node_id`-uniqueness contract, **plus** a dedup'd **cross-kind** mixed-`node_kind` view warning for observability (same-kind collisions stay silent — covered by the contract). | A facet of an already-required contract; namespacing the key would break L2/L6. |

---

## 13. Review status

- **Round 1 (2026-06-19): complete, folded in.** Four lenses (correctness/runtime, architecture, gaps/blast-radius, type-design), all source-grounded. No CRITICAL architecture breaks. Verified: `Tool(func, name=)` works; §7 parity holds empirically; frozen-dataclass-custom-`__init__` is sound; `@advertises` MRO collection + topic-uniqueness OK; worker triggers fire. Folded: C1→L14 (`node_kind` + mixed-kind warning), structural over-pull guard (`expected_kind`/`wrong_kind_targets`), name dedupe, `SelectorResult.bindings`→tuple, collision-log-via-selector, reject mixed/empty, validation-taxonomy precision, `add_tools` caveat, identity-collision note, replica-equivalence note, `subscribe_topics[0]` guard, fail-loud comment retention, blast-radius checklist (§9), test scenarios (§11).
- **Round 2 (2026-06-19): complete, folded in.** Three lenses (wire/schema-evolution, regression/C1-completeness, consistency/gaps), source-grounded; all converged on one CRITICAL — adding `node_kind` to an existing wire model means pre-change records lacking it fail `model_validate_json`, and ktables' poison-tolerance skips them (orphaning the node from the view). Verified empirically. **Resolution (Ryan): accept the breaking change** — `node_kind` is **required** (no compat default); the `calf.capabilities` topic is recreated on upgrade (pre-1.0, no deployments; a default shim was explicitly rejected as it would mask a missing stamp and conflicts with the hard-breaks stance). Also folded: C1 partial-coverage honesty (cross-kind only; same-kind silent) + dedup key `(node_id, frozenset(kinds))`; blast-radius extended to every stamp/record construction site + field-set assertions + the `calfkit` export; §10/§11 wording. No redesign — the `node_kind`-on-stamp architecture is sound. All four round-1 fixes re-verified with no regression.
- **Round 3: pending.** A light confirm pass over the round-2 edits (the default, the `wrong_kind_targets` transient semantics, the blast-radius additions) to declare convergence before implementation.

---

## 14. Future extensions (explicitly deferred)

### 14.1 Opt-in advertising (designed; deferred per L7)
When the always-on boot dependency (§8) becomes a problem, opt-in is cheap because the substrate models advertising at the **type** level. Express it as a type distinction, not an instance flag:

```
BaseToolNodeDef           # eager machinery only — NO advert
 └─ ToolNodeDef           # run handler + create_tool_node — NO advert (eager-only default)
     └─ DiscoverableToolNodeDef   # adds the @advertises factory — advertises

def agent_tool(func, *, name=None, discoverable=False):
    cls = DiscoverableToolNodeDef if discoverable else ToolNodeDef
    ...
```
A plain `ToolNodeDef` has empty `_adverts` → contributes nothing → eager-only deployments stay zero-infra. No substrate/worker change. Rejected alternative: an instance-flag gate in the worker (injects instance-level opt-out into a type-level substrate, applying globally for one use case).

### 14.2 Other deferred items
- **Extended `CapabilityToolDef` fidelity** — carry `requires_approval`/`strict`/`sequential`/`timeout`/`metadata` if the tool-node surface grows (§7). Additive.
- **Open-ended discovery** — an agent that discovers tool nodes it did not pre-name (needs a new view-registration trigger, e.g. a `discover=True` agent flag). Out of scope (§3).
- **Protocol merge** — collapse `ToolProvider` + `ToolSelector` (L9).
- **Multi-tool tool nodes** — `CapabilityRecord.tools` is already a list; a future multi-tool node would advertise N entries and `Tools` would need an `include` filter, converging with `MCPToolbox`. (The `expected_kind="tool"` guard already admits it — it filters by kind, not count.)
