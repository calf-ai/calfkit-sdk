# MCP-over-Kafka RPC Discovery — Future Design

**Status:** Roadmap (post-v1)
**Document version:** 1.0
**Last updated:** 2026-05-30
**Related to:** [`docs/mcp-adaptor-design.md`](./mcp-adaptor-design.md) (v1, codegen-based)

---

## 1. Summary

A future calfkit revision (post-v1) will add **runtime tool discovery** by treating every MCP server method as a Kafka request/reply topic on the bridge worker. This eliminates the codegen step for users who prefer dynamic discovery and generalizes naturally to additional MCP methods (`resources/*`, `prompts/*`) beyond v1's `tools/*` scope.

This document is a brief sketch — full design will be drafted when this entry is committed for implementation.

---

## 2. Motivation

v1 ships with codegen-generated schema declarations:

```python
from gmail_schemas import Gmail
gmail = mcp("npx -y @mcp/server-gmail", tools=Gmail.ALL)
```

This is the right v1 choice (see [archived catalog sub-plan](./mcp-catalog-discovery-plan.md) for the alternatives considered), but it imposes a build step and asks users to manage generated artifacts. For dev/experimentation, this is friction. For users who prefer the dynamic-discovery model that pydantic-ai / OpenAI Agents / Google ADK offer, codegen is a step backward.

A future revision adds a runtime-discovery alternative behind the same `mcp(...)` API.

---

## 3. The pattern

Every MCP server method becomes a Kafka request/reply topic on the bridge worker, using calfkit's existing `Client.execute_node` request/reply primitive.

| MCP method | Kafka topic pair (input → output) | Added in |
|---|---|---|
| `tools/call` for tool `<t>` | `mcp.<server>.<t>.input` / `.output` | v1 (already present) |
| `tools/list` | `mcp.<server>.tools_list.input` / `.output` | future v1.x |
| `resources/list` | `mcp.<server>.resources_list.input` / `.output` | v2 |
| `resources/read` | `mcp.<server>.resources_read.input` / `.output` | v2 |
| `prompts/list` | `mcp.<server>.prompts_list.input` / `.output` | v2 |
| `prompts/get` | `mcp.<server>.prompts_get.input` / `.output` | v2 |

The bridge becomes a generic MCP-over-Kafka proxy. Adding a new MCP method is "add one node class to the bridge, define one topic name." No new infrastructure, no protocol surface change.

---

## 4. User-facing change

The `mcp(...)` API stays identical. The only difference is the `tools=` kwarg becoming optional:

```python
# v1 — codegen path (current)
from gmail_schemas import Gmail
gmail = mcp("npx -y @mcp/server-gmail", tools=Gmail.ALL)

# v1.x — RPC discovery (this design)
gmail = mcp("npx -y @mcp/server-gmail")  # tools omitted → discover at startup
```

If `tools=` is provided, use it (skip discovery — the current v1 behaviour). If omitted, the agent worker calls `client.execute_node(target_topic="mcp.<server>.tools_list.input")` during `_on_startup` to fetch the catalog.

No breaking change. Codegen users see no difference.

---

## 5. Implementation sketch

### 5.1 New node: `McpToolsListNode`

Lives in `calfkit/mcp/_bridge.py`. Subscribes to `mcp.<server>.tools_list.input`. On request, returns the bridge's in-memory tool catalog (already populated at bridge startup via `session.list_tools()`).

```python
class McpToolsListNode(BaseNodeDef):
    def __init__(self, server: McpServer):
        self._server = server
        super().__init__(
            node_id=f"mcp_{server._name}_tools_list",
            subscribe_topics=[f"mcp.{server._name}.tools_list.input"],
            publish_topic=f"mcp.{server._name}.tools_list.output",
        )

    async def run(self, ctx: SessionRunContext) -> NodeResult[State]:
        ctx.state.final_output_parts = [DataPart(data={
            "schema_version": 1,
            "server": self._server._name,
            "mcp_server_info": self._server._initialize_result.server_info.model_dump(),
            "tools": [m.to_dict() for m in self._server._discovered],
        })]
        return ReturnCall[State](state=ctx.state)
```

### 5.2 Agent-worker startup change

```python
async def _on_startup(self) -> None:
    # ... existing bridge-mode handling ...
    for server in self._collect_agent_only_servers():
        if server._tools is not None:
            continue  # codegen path: user provided tools= upfront
        # RPC path: fetch tools/list from the bridge
        result = await self._client.execute_node(
            input="",
            target_topic=f"mcp.{server._name}.tools_list.input",
            timeout=self._tools_list_timeout_s,
        )
        server._apply_catalog(result.output)
    self._register_handlers_now()
```

### 5.3 Trade-offs vs codegen (acknowledged)

- **Bridge availability at agent boot**: required. Bridge down → agent boot fails until orchestrator retry. (v1 codegen has no such coupling.)
- **No PR-reviewable schema diffs**: catalog evolution is invisible until runtime failure or bridge republish. (v1 codegen makes evolution visible.)
- **Live freshness**: every agent boot sees the current MCP server state. No staleness.

These trade-offs are reasonable for dev/experimentation; users for whom they matter operationally should use codegen.

---

## 6. v2 generalization — full MCP-over-Kafka proxy

Same pattern extended to other MCP methods. Each becomes a `McpXxxNode` subscribing to `mcp.<server>.xxx.input`. The bridge becomes a complete MCP-over-Kafka transport proxy:

- Any calfkit component can speak MCP via Kafka topics
- External services that speak calfkit's envelope format can also use MCP via the bridge
- New MCP protocol versions are absorbed by adding new node types

This is the longer-term architectural direction. v1.x (this design) ships `tools/list` to validate the pattern; v2 generalises.

---

## 7. Open questions for the implementation phase

- Should the bridge cache the `tools/list` response, or call MCP every time? (Probably cache with TTL — MCP server catalog changes rarely.)
- How does the agent worker behave when both codegen schemas and RPC-discovered schemas conflict? (Codegen wins; log warning on drift.)
- Should the bridge emit a notification when `tools/list_changed` arrives from the MCP server, allowing agent workers to refetch? (v2.)
- Should the `tools/list` node enforce any auth, or trust intra-Kafka topology? (Match calfkit's existing per-tool topic auth model.)
