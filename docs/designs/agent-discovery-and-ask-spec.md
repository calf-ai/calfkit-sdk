# Agent Discovery & Ask (Caller-Side)

- **Status:** Draft (design)
- **Date:** 2026-06-16
- **Builds on:** [Node Presence Control Plane — Substrate & Shared Primitive](./node-presence-substrate-spec.md), [Capability-Plane Migration & Ops CLI](./capability-plane-migration-and-ops-spec.md)
- **Scope (Phase 2):** an opt-in agent registry (`calf.agents`), agent discovery (static peers + dynamic `find_agents`), the **caller-side** request/reply ("Ask") adapter, and caller-side trust (opt-in advertise + scoped consume).
- **Related:** prior agent-discovery assessment; operator ACL plane (#231).

---

## 1. Summary

Build agent-to-agent **discovery** and the **Ask** (request/reply) messaging mode on top of the presence substrate. A discoverable agent advertises an `AgentCard` to `calf.agents`; a calling agent finds peers (statically wired or dynamically queried) and asks one a question, receiving its answer as a normal tool result.

The central design finding (validated against the source): **the callee needs no new handler.** An agent invoked over the standard call path processes the request through its existing `run` and replies through the existing reply slot. The only genuinely new code is a **caller-side adapter** that makes a peer-agent call look like a tool to the calling agent's loop.

## 2. Goals / Non-goals

**Goals**
- `AgentCard` + `calf.agents` (opt-in discoverable agents).
- Discovery: explicit `peers=[...]` (static) and a `find_agents` tool (dynamic, default-closed), selecting live peers via the presence/agents views.
- A caller-side peer-as-tool adapter: seed a sub-conversation, dispatch to the peer, map its reply into `tool_results`.
- Caller-side trust: a node advertises as discoverable only by opt-in; a caller only asks peers on its include-list.

**Non-goals (explicitly deferred)**
- **No callee-side handler** — peers are invoked via the existing path (§4).
- **No error handling** — deadlines, timeouts, dangling-call recovery, retries, and recursion/depth budgets are inherited from the SDK-wide error-propagation work, **not** specced here. (A true per-call deadline is impossible in the suspend-to-Kafka re-entry model without a timer subsystem; `reply_ttl` is a client-only in-process timer and does not apply to a re-entry-driven agent caller.)
- **No Enlist mode** — multiple agents sharing one live conversation needs concurrent-history merge, which is unsolved/research. Out of scope.
- **No callee-side admission / operator ACL** — that is #231 (operator-authored, needs caller-identity-on-the-wire). The adapter only *reserves a seam* for it.

## 3. `AgentCard` and the agents topic

```python
class AgentCard(ControlPlaneRecord):     # topic: calf.agents  (opt-in discoverable agents)
    schema_version: int = AGENT_CARD_SCHEMA_VERSION
    ask_topic: str                        # the agent's dispatch topic for Ask requests
    skills: list[Skill]                   # A2A-style: name, description, tags
    revision: str | None = None           # deploy revision (rolling-deploy visibility)
    # inherits identity + liveness from ControlPlaneRecord
    # deliberately NO system prompt, NO full tool list (don't leak internals)
```

- Published via the worker's `ControlPlanePublisher` (same heartbeat/tombstone path) **only when the agent opts in** (`discoverable=True`). Presence (`calf.presence`) stays default-on; being a *messageable peer* is the opt-in.
- Borrows the A2A AgentCard's skills/tags shape (not its HTTP transport). `ask_topic` is the agent's normal dispatch topic — an Ask is just an invocation (§4).

## 4. The callee needs no new handler

A source-grounded trace of how an agent is invoked today (`client.py`, `nodes/base.py`, `nodes/agent.py`):

- A client invokes an agent by seeding a **conversation `State`** (not a payload) and publishing a `Call` to the agent's topic with a `callback_topic`:
  `State(...)` + `stage_message(ModelRequest.user_text_prompt(...))` (`client.py:73-74`), wrapped with a `CallFrame(callback_topic=...)` (`base.py:346-349`).
- The agent's entry handler is the inherited `@handler("*")` with **no schema/payload**; `run(self, ctx)` reads input purely from `ctx.state` (`agent.py:174, 289-298`).
- It returns `ReturnCall(value=parts)` → `_publish_action` mints `ReturnMessage(in_reply_to=frame.frame_id, tag=frame.tag, parts=…)` to `frame.callback_topic` with `x-calf-kind=return` (`base.py:362-387`), projected to typed output by `project_output` (`node_result.py:257-302`).

So a peer agent invoked over the standard path — a seeded sub-conversation `State` sent to its `ask_topic` with `callback_topic` = the caller's return slot — processes it through its existing `run` and replies through the existing slot. **It's a client invocation minus the client.** No `peer.ask` handler, no new schema.

> Why the assessment thought a handler was needed: dispatching a `Call` with a `ToolCallRef` *payload* at an agent silently drops the payload, because the agent's `'*'` handler declares no schema/payload param (`base.py:749-750`). The fix is not to teach the agent to read a payload — it's to invoke it the State way. (Contrast: a tool node *does* re-decorate `@handler("*", schema=ToolCallRef)` and read the payload — `tool.py:84-85`.)

Callee-side trust, when it lands (#231), is a **guard** on this standard handler (checking `x-calf-emitter`), still not a separate handler.

## 5. The caller-side adapter (the new code)

To make a peer call look like a tool to the *calling* agent's loop, add a peer-as-tool provider (likely the body behind the v1-design `Tool.from_agent(...)` / `peers=[...]` surface — verify whether that surface is built or a stub during implementation):

1. **On tool-call:** seed a fresh sub-conversation `State` from the tool args, and emit a `Call` to the peer's `ask_topic` with:
   - `callback_topic` = the calling node's return slot, and
   - `tag` = the caller's `tool_call_id` (the `tag` field already exists for exactly this correlation — `reply.py:20-23`, `session_context.py:52-57`).
2. **On the peer's reply:** translate the inbound `ReturnMessage.parts` into a `ToolReturn` keyed by `tag`, fold it into `state.tool_results`, and resume the caller's loop as if a tool returned.

The wrinkle that makes this genuinely new: a normal tool returns the **same** `State` carrying `tool_results[tag]` (`tool.py:165`), whereas a peer agent returns its **own** reply parts on a **fresh** sub-State — so without this translation the caller's fold finds nothing at `tool_results[tag]`. That translation is the adapter.

Also needed (small): a **node-side State-seeding helper**. Today State-seeding lives only in `Client._build_state_and_overrides` (`client.py:73-74`); nodes need a thin affordance to build a sub-conversation (reachable now via `State()` + `stage_message`, just unsurfaced).

> Note: the peer's `run` treats the seeded user turn as user-role input (POV projection re-roles it). That is inherent to reusing the standard path and is exactly why the trust boundary (§6) matters — a peer must treat an ask as untrusted input.

## 6. Discovery & trust

### 6.1 Discovery read surface

- **Static peer wiring (safe default):** an agent declares `peers=[support_agent, ...]` — an explicit include-list resolved against the `calf.agents` view. The include-list *is* the trust boundary; semantically identical to today's MCP tool selectors.
- **Dynamic discovery (default-closed):** a `find_agents(query)` tool exposed to the LLM, returning discoverable agents matching skills/tags, filtered to **live** (presence staleness) ∩ **allowed** (scope). Off unless explicitly enabled and scoped.

Both select **live** instances via `ControlPlaneView` (using the substrate's staleness filter) so a caller prefers an online peer. (What happens if the chosen peer dies mid-ask is the error-propagation layer's concern, not this spec's.)

### 6.2 Trust posture (caller-side in v1)

- **Advertise opt-in:** an agent is in `calf.agents` only with `discoverable=True`.
- **Consume scoped:** the caller's `peers=[...]` / `find_agents` scope bounds who it will ever ask. Caller-side self-restriction is the v1 trust mechanism.
- **Callee-side admission + operator ACL — deferred to #231.** Rejecting inbound asks from a verified emitter needs caller-identity-on-the-wire that isn't solid yet. The adapter reserves the seam (consult an injected policy view if present); enforcement is caller-side only in v1.

## 7. Phasing

**Phase 2**, after the substrate + capability migration (Phase 1):
1. `AgentCard` + `calf.agents` + opt-in publishing via `ControlPlanePublisher`.
2. Static `peers=[...]` resolution against the agents view.
3. Caller-side peer-as-tool adapter + node-side State-seed helper.
4. Dynamic `find_agents` tool (default-closed) + caller-side scoping.

**Future (out of scope):** Enlist mode; callee-side admission / operator ACL (#231); per-call deadlines and failure propagation (inherited error-propagation work).

## 8. Testing

- **Unit:** `AgentCard` schema; the `ReturnMessage → ToolReturn` translation in isolation (a peer reply folds into `tool_results[tag]` and resumes the loop); static `peers` resolution against a fake agents view (live-filtered).
- **Integration (Redpanda lane):** agent A asks peer B over the standard path; B's answer arrives as A's tool result; a non-discoverable agent does not appear in `calf.agents`; a stale peer is excluded from selection.
- **Explicitly not tested here:** timeout/dangling/recursion behaviour (owned by the error-propagation layer).

## 9. Open questions

- Whether `Tool.from_agent` / `peers=[...]` already exists on `main` or is a stub — determines how much of §5 is new.
- The `find_agents` result shape and how scope is expressed (skill/tag filter vs explicit allow-list).
- `Skill` model fields (align with A2A: name, description, tags).
