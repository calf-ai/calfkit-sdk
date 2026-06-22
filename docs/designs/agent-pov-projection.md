# Agent-POV Message-History Projection — Design Document

**Status:** Roadmap — finalized & converged design (v4.2), ready for implementation
**Document version:** 4.2
**Last updated:** 2026-06-05
**Closes:** [#154](https://github.com/calf-ai/calfkit-sdk/issues/154)
**Related to:** [`docs/designs/calfkit-v1-design.md`](./calfkit-v1-design.md) §7.2 (state ownership), §11 (multi-agent patterns)

> **Evolution.** v1 invented a parallel `SharedTranscript` threaded through `deps` (wrong — duplicated `message_history`; `deps` round-trips `ModelMessage` to dicts). v2 collapsed it onto `message_history`. v3 proposed an opt-in flag + a model wrapper for stamping. v4 finalized the design: **always-on** behavior with **no flag** (projection auto-detects multi-participant histories), `name`-stamping via a single `State` helper (covers streaming), `name` stripped from all model input, and text+structured output surfaced together. **v4.1** applied the §7 fix (preamble reads the last `ModelResponse`, not `new_messages()[-1]`, using the vendored `TextPart`), pinned the surface separator / empty-surface check / custom-output-tool-name limitation, switched self-view to `replace()`, and corrected citations. **v4.2** (this version) closes the final review round: the deferred-results re-entry invariant is made explicit + tested (§6.2), structured args are rendered canonically via `args_as_dict()` to avoid provider-dependent whitespace (§5.5), and the cross-feed/handoff footgun (§5.1), `correlation_id`-per-post caveat (§9), and `name`-in-public-history note (§11) are documented. Three independent opus reviews verified v4.1/v4.2 against source (the §7 fix reproduced empirically) and returned **converged — implementation-ready**. Every claim is grounded in source citations.

---

## 1. Summary

When multiple agents share a conversational surface (a Discord channel, a group chat), each agent must be invoked with the conversation projected from **its own** point of view: messages it authored stay assistant turns; everyone else's become attributed, surface-only user turns. The transcript already exists as `State.message_history` — one list that travels on the wire and accumulates per turn. The feature adds:

1. **Always-on identity stamping** — every `ModelResponse` is tagged with the producing agent's id via `ModelResponse.name`, in one `State` helper at the two history-append sites.
2. **Always-on projection** — at the model-input boundary, `message_history` is projected to the running agent's POV. It **auto-detects** whether the history is multi-participant: if the running agent is the sole author (no agent other than the viewer, ≤1 named human) it passes through transparently (byte-identical to today); otherwise — including a single *other* agent's history (e.g. a handed-off conversation) — it re-roles other agents to attributed user turns. `name` is always stripped from the model input.

There is **no opt-in flag and no new public type/wire field** — it is recipe-layer behavior consistent with v1 §11 (multi-agent patterns are recipes) and §7.2 (message history is an agent-loop concern).

---

## 2. Motivation

### 2.1 The gap

`message_history` is a flat `list[ModelMessage]` with **no author populated** and **no POV awareness** (`state.py:37`; first message built with no `name`, `client.py:123-124`; history passed straight to the model, `agent.py:315-316`). For one agent this is fine (one "assistant," POV baked into roles). On a shared channel the same history must read differently per agent: agent A's turns are A's `ModelResponse`s but must appear to agent B as attributed user turns. A bare `ModelResponse` says only "the assistant spoke" — it loses *which* assistant.

### 2.2 Why provider-level `name` can't carry attribution (verified)

Setting `name=` and letting the provider attribute speakers does **not** work cross-provider:

| Model client | API | Honors participant `name` on the wire? | Evidence |
|---|---|---|---|
| `OpenAIModelClient` | Chat Completions | ✅ (and **sent verbatim**) | `models/openai.py:864,903-904`, `:1029-1041`, `:1164-1165` |
| `OpenAIResponsesModelClient` | Responses | ❌ dropped | `_map_messages` never reads `part.name` (`models/openai.py:1717-2075`) |
| `AnthropicModelClient` | Anthropic Messages | ❌ dropped | `_map_user_prompt` ignores `name`; `BetaMessageParam` is role/content only (`models/anthropic.py:714-741`) |

Two of three drop it. Model-facing attribution must be **content-level**: an `<author>` prefix in the `UserPromptPart`. `name` is dropped only at the **provider-mapping boundary**, so it round-trips fine in `State.message_history` over Kafka — we use it for *internal* identity (storage) and a *projected* content prefix for *model-facing* attribution. (Chat Completions *does* send `name` verbatim with no sanitization, `:903-904`, and `node_id` has no charset validation — another reason §5.5 strips `name` from all model input.)

### 2.3 What apps reimplement today

Per invocation: track author per message, classify self vs other, re-role others, attribute portably, surface-only the others, identify another agent's structured answer, match tool results to owners. pydantic-ai's mapping does none of this (1:1 message→param, `models/anthropic.py:708-897`). Centralizing removes duplication and the subtle per-app bugs.

---

## 3. The core model: `message_history` *is* the transcript

### 3.1 One canonical, author-tagged history on the wire

There is exactly one transcript: `State.message_history` — carried inside `SessionRunContext.state` in the `Envelope` (`models/envelope.py`), which the node base publishes/republishes on every Action (`nodes/base.py:201-228`), appended to and republished every turn. We add one always-on discipline: **every `ModelResponse` carries its author in `name`**:

| Message in canonical history | Identity slot | Set by |
|---|---|---|
| An agent's turn → `ModelResponse(name=<agent_id>)` | `ModelResponse.name` (`messages.py:1310`) | the stamping helper (§4) |
| A human / external post → `ModelRequest(parts=[UserPromptPart(name=<name?>)])` | `UserPromptPart.name` (`messages.py:755`) — **optional** | the channel producer (optional, §8) |
| A tool exchange an agent generated | the owning `ModelResponse.name`, resolved by `tool_call_id` (§5.3) | the agent loop |

`ModelResponse.name` is documented as *"an optional name for the participant in a multi-agent conversation"* (`messages.py:1310-1313`). pydantic-ai never sets it (only telemetry uses the agent name, `agent/__init__.py:688-699`), so we stamp it. This adds **no** schema/wire field — `name` is a pre-existing optional field.

### 3.2 Content is clean; the prefix is a projection artifact

Stored content is **never** prefixed. The `<author>` prefix and the `name`-strip are synthesized by projection at read time. The canonical history stays free of presentation artifacts and free of any provider-facing `name`, so re-projecting for any viewer is always clean.

### 3.3 Accumulation (verified)

1. A producer posts to the channel topic; the new message rides in `State` (staged as `uncommitted_message`, `client.py:124` → committed into `message_history` at `agent.py:311-312`).
2. An agent consumes the envelope, **projects** `message_history` to its POV (§5), runs the model on the projection.
3. The agent's new `ModelResponse`(s) are appended to the **canonical** `message_history`, `name`-stamped by the §4 helper at the existing append sites (`agent.py:331,487`).
4. `ReturnCall(state)` republishes the canonical history, one turn longer (`nodes/base.py:217-228`).
5. The next agent repeats from (2).

Projection transforms the **model input only**; what is stored and republished is always the canonical author-tagged history.

---

## 4. Identity stamping (always-on, one helper)

Stamping is unconditional and lives in one place: a `State` helper that stamps un-named `ModelResponse`s before extending the history, routed through both existing append sites.

```python
# calfkit/models/state.py  (new method on CoreMessageState)
def extend_with_responses(self, messages: list[ModelMessage], author: str) -> None:
    """Append run-produced messages, stamping author identity on un-named responses."""
    for m in messages:
        if isinstance(m, ModelResponse) and m.name is None:
            m.name = author
    self.message_history.extend(messages)
```

```python
# calfkit/nodes/agent.py:331 and :487  (changed)
ctx.state.extend_with_responses(result.new_messages(), self.name)   # was: message_history.extend(result.new_messages())
```

**Why the helper, not a model wrapper.** Both the non-streaming and streaming paths converge on `_finish_handling` (`_agent_graph.py:500` and `:486` → `:555`), and the fully-assembled response is what `result.new_messages()` returns — for streaming via `streamed_response.get()` (`:484`), for non-streaming via `model.request()` (`:497`). Stamping at the `new_messages()` append therefore covers **both paths** with no vendored edits and **no fragile `StreamedResponse` proxy** (a `.get()`-only stream wrapper does not work against the ABC's iteration-driven accumulation, `models/__init__.py:916-1022`). The `if m.name is None` guard makes it idempotent and lets any inner-wrapper/provider-set name win.

**The in-place `.name` mutation is safe** (verified): the objects from `result.new_messages()` are the newly-generated tail, not aliased into the canonical history the projection was built from (projection emits fresh objects, §5). After `run()` returns, its internal `GraphAgentState` is discarded, and the next re-entry builds a fresh run from a shallow copy of canonical (`agent/__init__.py:627`). So stamping `.name` post-run corrupts nothing pydantic-ai still uses, and survives `_clean_message_history` (which `replace()`s, preserving `name`).

---

## 5. The projection

`project(history, viewer)` is pure: it returns a new `list[ModelMessage]`, constructs **new** message/part objects (never mutates the canonical history, so re-projection for the next viewer is always clean — the vendored cleaner uses `replace()` for the same reason, `_agent_graph.py:1428`), and **strips `name` from every message it emits** (§5.5).

### 5.1 Multi-participant detection (the no-flag mechanism)

Projection runs for every agent, but engages prefixing/re-roling only when a role is genuinely ambiguous:

```python
agent_names = { m.name for m in history if isinstance(m, ModelResponse) and m.name }
human_names = { p.name for m in history if isinstance(m, ModelRequest)
                        for p in m.parts if isinstance(p, UserPromptPart) and p.name }

if (agent_names - {viewer}) or len(human_names) >= 2:
    # viewer-aware: an agent OTHER than the viewer authored a turn (incl. a single one —
    # a handed-off conversation), or ≥2 named humans → re-role + prefix (§5.2–5.4)
else:
    # only the viewer's own turns, ≤1 named human → TRANSPARENT pass-through:
    #   strip name from every message; keep roles; no prefixes → model input identical to today
```

This is correct across the cases (verified by analysis + tests): **only the viewer's own turns** + any unnamed humans → transparent; **any agent other than the viewer authored a turn → projected** — including a *single* other agent (a handed-off or cross-fed conversation; the handoff first-hop) and the multi-agent channel cases (two agents ± an unnamed human, etc.); one agent + ≥2 named humans → projected. Unnamed humans remain indistinguishable (to us and the model) — naming them is how a user opts into distinguishing them. A new agent that hasn't spoken yet owns nothing but correctly sees the other author(s) → projected. (The earlier gate counted distinct authors `>= 2`, which **missed a single other-agent's history**; the viewer-aware `agent_names - {viewer}` fixes it.)

**Two consequences to state honestly:**
- **"Transparent" means the model *input* is byte-identical to today**, not that the stored history is. Stamping is always-on, so the stored canonical `message_history` now carries `name` on every `ModelResponse` (an additive optional field; no calfkit consumer reads it, and it never reaches a provider — §5.5). It is backward-compatible but *not* a zero-diff on the wire.
- **The transparent→projected transition (when a 2nd agent first speaks) rewrites the prompt prefix**: the same human messages retroactively gain `<user>` prefixes on that turn, which **invalidates provider prompt caches** (Anthropic/OpenAI prefix caching) for the history once. This is the inherent cost of keeping single-agent chats prefix-free; the alternative (always prefixing) would change every 1:1 conversation. Accepted as a one-time, per-channel cost. Implementation should `logger.debug` when an agent first transitions transparent→multi-participant, so the engage point is greppable (the no-flag model has no other signal).
- **Cross-feed / single-other-agent (now fixed — viewer-aware gate):** the detection scans the *whole* history and compares authors to the viewer, so feeding **agent A's** `result.message_history` to a **different** agent B — or handing a conversation off to B — re-roles A's turns as attributed `<A>` user turns **even when A is the only author**. (The original design left this as a "document, don't fix" footgun under the author-count `>= 2` gate, where B saw A's turns as its own; the `agent_names - {viewer}` gate fixes it, which the handoff feature requires.) The README `message_history=result.message_history` **same-agent multi-turn** pattern is unaffected (viewer == sole author → transparent/identical).

### 5.2 Per-message re-roling (multi-participant mode)

For viewer `V`:

**Owner == V (self — full fidelity, `name` stripped):**
- `ModelResponse` → emit `replace(m, name=None)` (a **new** object via `dataclasses.replace`, never an in-place `m.name = None` — that would corrupt the canonical message for the next viewer and for storage). Parts are unchanged (V's `TextPart`/`ThinkingPart`/`ToolCallPart` kept); `replace` is shallow, which is safe because parts are never mutated in place.
- tool-return `ModelRequest` → emit unchanged (V sees its own tool results).

**Owner != V (other — surface-only, attributed):**
- `ModelResponse` → compute its **surface** (§5.4); if non-empty, emit `ModelRequest(parts=[UserPromptPart(content=<prefix> + surface)])`. Drop reasoning/tool-call-only responses.
- tool-return `ModelRequest` → drop (another agent's internal tool result).
- human `ModelRequest(UserPromptPart)` → emit `ModelRequest(parts=[UserPromptPart(content=<prefix> + text)])`.

### 5.3 Ownership resolution

- `ModelResponse` → owner is its `name`.
- `ModelRequest` with a `UserPromptPart` → a human (owner = "the user").
- `ModelRequest` with only `ToolReturnPart`/`RetryPromptPart` → owner resolved **by `tool_call_id`**: map `tool_call_id → owning ModelResponse.name` from the `ToolCallPart`s, attribute each `ToolReturnPart` accordingly (`tool_call_id` on both parts, `messages.py:1200`/`:821`). Exact and interleave-safe — never positional. (A `ToolReturnPart` with no resolvable owner can only arise if the SDK user truncated history between a call and its return — their responsibility, §16; the projection itself never orphans, since for self it keeps both and for others it drops both.)
- An un-`name`d `ModelResponse` (only possible from histories created before this feature shipped, since stamping is now always-on) → treated as other → `<unknown>`. This *can* surface mid-conversation in a mixed history (a legacy un-stamped response alongside ≥2 stamped agents, which still triggers projection). The outcome is benign — a harmless `<unknown>` prefix, never a self/other privacy error — but it is not, as an earlier draft claimed, confined to single-agent histories. It is a one-time upgrade artifact, documented (§16) not engineered.

### 5.4 Attribution prefixes

- **Other agents:** `<author>` (the `ModelResponse.name`), e.g. `<scheduler>`.
- **Humans:** `<user>` when the `UserPromptPart` has no `name`; `<user:name>` when it does.
- **Self:** no prefix; `name` stripped.

The format lives in one place; pluggability is not exposed in v1 (add when a second format is demanded).

### 5.5 Surface extraction — all three output modes + the `name` strip (verified)

`name` is stripped from **every** message the projection emits (self responses and any constructed request), so it never reaches a provider — closing the OpenAI Chat 400 path (§2.2) uniformly in both transparent and multi-participant modes.

pydantic-ai produces structured output in three modes (resolved per-model from `default_structured_output_mode`, default `'tool'` for all three calfkit providers, `profiles/__init__.py:42`):

| Mode | Answer in `message_history` |
|---|---|
| `tool` (default) | `ModelResponse` → `ToolCallPart(tool_name='final_result', args=<value>)` for a single output; a 2+-output **union** renames each output tool `final_result_<TypeName>` (`multiple=True`, `_output.py:889,909-914`). `DeferredToolRequests` is stripped to a flag, not suffixed (`_output.py:242-244`). |
| `native` | `ModelResponse` → `TextPart(<JSON>)` (no output tool; `_agent_graph.py:786-794`) |
| `prompted` | `ModelResponse` → `TextPart(<JSON, maybe fenced>)` (no output tool) |

**Surface of an other-agent turn = its `TextPart` text(s) + the rendered output-tool args.** Because a tool-mode final response *can* contain a `TextPart` preamble **and** the output-tool call (`final_result`, or `final_result_<Type>` for a union), both are surfaced. The surface is assembled by joining the non-empty components — the concatenated preamble `TextPart` text(s), then the structured value — with a single `"\n"` separator (so a turn with both reads as `<author>\nOn it.\n{"flights":3}` after the prefix). `native`/`prompted` answers are already `TextPart`s, surfaced by the text rule. Ordinary (function) tool calls — anything outside the `final_result*` namespace — and their returns are dropped as internal.

Two implementation rules verified in review:
- **Render structured args canonically, and test emptiness on `ToolCallPart.args` directly.** Render via `args_as_dict()` then a canonical compact encode (`json.dumps(tc.args_as_dict(), separators=(",", ":"), sort_keys=True)`) — **not** bare `args_as_json_str()`, which passes a JSON *string* through verbatim: OpenAI stores tool args as a string (`models/openai.py:738`) while Anthropic stores a dict (`models/anthropic.py:556`), so `args_as_json_str()` would emit provider-dependent whitespace (`'{"flights": 3}'` or even pretty-printed) and make the surface — and any golden test — provider-coupled and flaky. For the emptiness test, branch on `if tc.args:` (truthy) — `args_as_json_str()` returns `'{}'` for both `args=None` and `args={}`, so a pure hand-off (`final_result(args=None)`, no preamble) would otherwise surface as `<author> {}` instead of being omitted. Do **not** use `ToolCallPart.has_content()` (it returns `False` for `{"x": 0}`, `messages.py:1256`, and would drop a valid answer).
- **Match the output tool by its auto-generated `final_result*` namespace.** pydantic-ai names the output tool `final_result` for a single output and `final_result_<TypeName>` for each member of a 2+-output union (`multiple=True`); both are surfaced (helper `_is_output_tool`). A user who customizes the name via `ToolOutput(Model, name="answer")` produces a `tool_name` outside that namespace, which this rule treats as an ordinary (dropped) tool call — so a *custom* output-tool name is **not** surfaced cross-agent. Documented limitation (§16); the common (auto-named) path is surfaced.

An empty surface (pure hand-off — no preamble, no structured args, no text) → the turn is omitted, never an attribution-only prefix.

### 5.6 Normalization — delegated to pydantic-ai (verified)

`_clean_message_history` runs on the history we pass to the loop (`_agent_graph.py:213`) and **already merges consecutive `ModelRequest`s** (concatenates parts, sorts tool-returns first, `:1391-1413`). Since several projected "other" turns in a row are consecutive `ModelRequest`s, that case is handled for free — calfkit does not merge. A leading `ModelResponse` (an agent that spoke first with no prior user turn) is **not** normalized — same as pydantic-ai, it's the SDK user's responsibility not to construct such a channel (§16).

### 5.7 Worked example

Canonical channel history (author in `name`, content clean):

```
ModelRequest:  UserPromptPart("what's friday & book a flight?")            # human, no name
ModelResponse: name="scheduler"  ThinkingPart(…), ToolCallPart(get_calendar)
ModelRequest:  ToolReturnPart(get_calendar → "2pm sync")                   # owner=scheduler (by tool_call_id)
ModelResponse: name="scheduler"  TextPart("You have a 2pm sync.")
ModelResponse: name="researcher" TextPart("On it."), ToolCallPart(final_result, args={"flights":3})
ModelRequest:  ToolReturnPart(final_result → "Final result processed.")    # owner=researcher
```

`agent_names = {scheduler, researcher}` → multi-participant. `project(history, viewer="scheduler")`:

```
ModelRequest:  UserPromptPart("<user> what's friday & book a flight?")
ModelResponse: ThinkingPart(…), ToolCallPart(get_calendar)        # self — kept, name stripped
ModelRequest:  ToolReturnPart(get_calendar → "2pm sync")          # self — kept
ModelResponse: TextPart("You have a 2pm sync.")                   # self — name stripped
ModelRequest:  UserPromptPart('<researcher>\nOn it.\n{"flights":3}')   # other — preamble + structured, "\n"-joined, compact JSON
```

A single-agent history (only `scheduler`) projects transparently: same messages, `name`s stripped, no prefixes — the **model input** is identical to today (the stored canonical history additionally carries `name` on responses; see §5.1).

---

## 6. Agent-loop integration

### 6.1 Project at the model-input boundary; keep canonical for storage

```python
# calfkit/nodes/agent.py:315  (changed)
result = await self._agent_loop.run(
    message_history=project(ctx.state.message_history, viewer=self.name),   # ← project the INPUT
    ...
)
```

`project()` returns a new list; canonical `ctx.state.message_history` is untouched. The loop copies its input internally (`agent/__init__.py:627`), so the model runs on the projection while canonical is unaffected. `result.new_messages()` is the generated tail (`run.py:381-395`); the §4 helper appends it to canonical — adding only the agent's own new, stamped responses (verified: `new_messages()` on a projected input never includes a leading `ModelRequest` or projected/prefixed content, because the new-message index is reset to the cleaned-projected length at `_agent_graph.py:213-216`).

### 6.2 Why dispatch logic stays correct on canonical (verified)

`latest_tool_calls()` (`state.py:42-49`) walks canonical to find pending tool calls and is safe **without modification** because a completed turn never leaves a dangling tool call: a tool-mode turn ends with a `ModelRequest` carrying the `final_result` return (`_agent_graph.py:809-814`, appended explicitly *"to allow this message history to be used in a future run without dangling tool calls"*), and a text-mode turn ends with a tool-call-free `ModelResponse`. So after others' completed turns, `latest_tool_calls()` returns `[]`; the only pending calls it sees are the agent's own in-flight ones. Tool dispatch, `deferred_tool_results` matching (the agent's own tool calls are kept full-fidelity in the projection, so their `tool_call_id`s are present), and parallel-batch handling all operate on canonical, unchanged.

**Load-bearing invariant (must be tested):** when the agent re-enters mid-tool-round-trip with `deferred_tool_results`, `_handle_deferred_tool_results` raises `UserError` unless the last `ModelResponse` in the *projected* input still carries the in-flight `ToolCallPart`s, and it matches results to calls by `tool_call_id` (`_agent_graph.py:301-308`). The self-view (§5.2) preserves the viewer's own `ModelResponse` verbatim (ids + parts), so this holds — but any future self-view optimization that drops the viewer's tool-call-only turns would silently break every tool round-trip. **Projection MUST preserve the viewer's in-flight `ToolCallPart`s (id + name) verbatim.** §14's integration test must include a tool round-trip *inside an engaged (multi-participant) projection*, asserting no `UserError` and matching ids — not only the gated/transparent case.

---

## 7. Client-facing final output (text + structure)

When an agent's final response carries **both** a text preamble and a structured output, the client-facing `final_output_parts` surfaces **both**, not just one:

```python
# calfkit/nodes/agent.py:487-492  (changed)
# NOTE two correctness traps, both verified in review:
#  (a) In tool mode (the default), pydantic-ai appends a trailing ModelRequest
#      (the `final_result` ToolReturn 'Final result processed.', _agent_graph.py:809-814)
#      AFTER the final response — so `new_messages()[-1]` is that ModelRequest, NOT the
#      ModelResponse with the preamble. Reverse-scan for the last ModelResponse instead.
#  (b) The response's parts are the VENDORED `TextPart` (field `.content`), NOT calfkit's
#      `payload.TextPart` (field `.text`). Use the vendored type to read, calfkit's to build.
from calfkit._vendor.pydantic_ai.messages import ModelResponse, TextPart as _RespTextPart

final_resp = next((m for m in reversed(result.new_messages()) if isinstance(m, ModelResponse)), None)
preamble = "".join(p.content for p in final_resp.parts if isinstance(p, _RespTextPart)) if final_resp else ""

parts: list[ContentPart] = []
if isinstance(result.output, str):
    parts = [TextPart(text=result.output)]            # calfkit payload TextPart
else:
    if preamble:
        parts.append(TextPart(text=preamble))         # separate part — no separator needed here
    parts.append(DataPart(data=result.output))
ctx.state.final_output_parts = parts
```

So a caller can read both the prose and the structured value — consistent with the projection surface rule (§5.5). The two parts stay distinct (`[TextPart, DataPart]`), so no separator is needed here; deserialization is unaffected because `client/deserialize.py` selects by part *type* (it prefers `DataPart`, falls back to `TextPart`), not by position — a structured caller still gets the `DataPart`, a `str` caller still gets the single `TextPart`. (`output_parts` ordering changes for structured-with-preamble callers, but no code reads it positionally.)

---

## 8. Optional human authoring

Humans never *require* a name (default prefix `<user>`), but a producer may supply one for multi-human channels. `user_text_prompt` already accepts `name=` (`messages.py:1041`); we surface it on the client:

```python
# calfkit/client/client.py  (changed)
async def invoke_node(self, user_prompt, topic, *, author: str | None = None, ...):
    ...
    state.stage_message(ModelRequest.user_text_prompt(user_prompt, name=author))   # was: no name
```

`author=None` → `<user>`; `author="Alice"` → `<user:Alice>` once ≥2 named humans appear (§5.1). The kwarg must be added to **all four** `invoke_node`/`execute_node` typed overloads, **both** impls, **and** threaded through the `execute_node`→`invoke_node` forward call (`client.py:236-248`) — seven edit sites; missing the forward silently drops `execute_node(author=...)`.

---

## 9. The shared-channel topology (the A2A surface)

The projection serves the **ambient channel / group chat** pattern — a §11 shape not yet covered (orchestration and linear/fan-out choreography exist; a topic agents both read and write does not).

- **One Kafka topic is the channel.** Every participant agent subscribes (own consumer group) — the README gate-stack pattern (`discord.thread.123`). Each agent gates (§9.1) and, if it acts, projects → runs → appends its stamped turn → republishes to the channel (§3.3).

**`correlation_id` is per-post, not per-conversation.** Each human post is a separate `invoke_node` call, which mints a fresh `correlation_id` (`client.py:118-119`); transcript continuity is carried entirely by `message_history`, not by a shared id. So you can trace an individual turn by `correlation_id` but **not** a whole channel conversation. Conversation-level correlation (and the turn budget, §9.1) belongs to the future channel-recipe design, not this feature.

### 9.1 Turn-taking & loop prevention
- **Address-gating (default):** act only when addressed (@mention / slash-target) — README gate stack.
- **Ignore-self:** never respond to your own emission; `ctx.emitter_node_id` (from `x-calf-emitter`, `session_context.py:111-119`) drops self-emitted events in a gate.
- **Turn budget:** consecutive-agent-turn cap needs **channel-scoped durable state** (compacted-state topic / DB the app owns); owned by the future channel-recipe design.

### 9.2 Channel isolation invariant (verified)
Intermediate tool exchanges never hit the channel: tool `Call`s target the tool's **private** topic (`agent.py:463`) with the agent's own return topic as callback (`nodes/base.py:200`). An agent completes its tool loop privately and republishes only its completed, atomic turn — load-bearing for §6.2.

---

## 10. Privacy

Canonical history is full-fidelity (every agent's reasoning/tool calls, author-tagged); `project()` strips other agents' internals at read time (the "filter on projection" posture). The filter is **structural** (owner + part type, §5.2/5.5), with no content heuristic to drift; a security property test (§14) asserts no non-viewer-owned `ThinkingPart`/`ToolCallPart`/`ToolReturnPart` ever appears in a projection.

**Honest residual risk.** Full-fidelity bytes physically sit in the canonical history on the channel topic, readable independent of `project()` — by consumer nodes (full `State`, `README:322-326`), `earliest` replay, DLQs. `project()` protects the model-prompt surface, not transport. This is a prompt-shaping control, not a confidentiality boundary. The hardened model (agents emit surface-only, keep full history privately) is additive — `project()` already keys on owner — and recorded as the migration if the posture must harden.

---

## 11. Public API surface

The feature is **behavioral** — there is almost nothing new to import:

- **No flag, no new type, no re-export.** `ModelMessage` is already in `execute_node`'s public signature from `_vendor` (`client.py:10,41`); users pass values they already hold. calfkit's `payload.TextPart` is untouched.
- **`project()` is internal** (`calfkit/nodes/_projection.py`) — it is always called with `viewer=self.name` from the loop, so there is no public bare-`str` `viewer` to mistype. It can be promoted to a public utility later, when the channel-recipe design provides a second, vetted consumer.
- **The only new public surface** is the optional `author: str | None` kwarg on `Client.invoke_node`/`execute_node` (§8).
- **One observable, additive change:** `ModelResponse.name` is now populated (with the producing agent's id) in the already-public `result.message_history` (`node_result.py:112`) and in the canonical history tool functions see via `ctx.messages` (`nodes/tool.py:104`). It is internal identity — additive, backward-compatible, never sent to a provider (§5.5). Document it where consumer/tool authors look (the `node_result.message_history` and `ToolContext.messages` docstrings).

---

## 12. Provider portability matrix (verified)

| Provider | `name` honored on wire | Strict alternation | First message must be `user` |
|---|---|---|---|
| OpenAI Chat Completions | yes (sent verbatim) | tolerant | tolerant |
| OpenAI Responses | dropped | tolerant | tolerant |
| Anthropic Messages | dropped | **required** | **required** |

Model-facing attribution is always the content prefix; `name` is stripped from all model input (§5.5), so nothing provider-specific leaks. `_clean_message_history` covers the common alternation case (§5.6); a leading `ModelResponse` is the user's responsibility (§16).

---

## 13. Edge cases

1. **Single-agent / no-other-participant** → transparent pass-through (names stripped, no prefixes); model input identical to today. (§5.1)
2. **Two agents, unnamed human** → projected (the rule triggers on ≥2 agent names regardless of human naming). (§5.1)
3. **Structured output, all 3 modes** → surfaced via `TextPart`s + `final_result` args; text preamble + structure both surfaced. (§5.5)
4. **Tool result by `tool_call_id`** → exact owner, interleave-safe; truncation orphans are the user's responsibility. (§5.3, §16)
5. **Empty surface** (hand-off) → turn omitted. (§5.5)
6. **Multimodal / file output of another agent** → a `FilePart` is response-side and not valid `UserPromptPart` content; surfacing it needs conversion to `BinaryContent` — **deferred TODO**, out of scope (§16).
7. **Leading `ModelResponse`** → not normalized (user's burden, §5.6).
8. **Pre-feature un-stamped history** → `<unknown>` only in a multi-agent history; single-agent passes through. One-time upgrade artifact, documented not engineered. (§5.3)

---

## 14. Testing strategy (TDD)

1. **Pure-function golden tests** (`project`): single-agent transparent (incl. names-stripped), 2-agents-1-unnamed-human, 2-named-humans-1-agent, agent-to-agent, self-then-other, tool-mode structured surface (`final_result` + preamble), native/prompted surface, `<user>`/`<user:name>`, tool-return ownership across an interleaved boundary, empty-surface omission, `<unknown>`.
2. **Detection tests:** the per-role rule for every row of §5.1's table.
3. **Security property test:** no non-viewer-owned internal part ever appears in `project(history, viewer)`. Gates the feature.
4. **Purity + name-strip:** `project()` doesn't mutate input; no emitted message carries `name`.
5. **Stamping helper:** `extend_with_responses` stamps un-named responses, leaves named ones, leaves `ModelRequest`s; round-trips through `State` (de)serialization.
6. **Client final output:** structured-with-preamble → `[TextPart, DataPart]`. **Must run in `tool` mode (the default)**, where the final `ModelResponse` is followed by a trailing `final_result` tool-return `ModelRequest` — the regression test must assert the preamble *survives* (the v4 bug dropped it by reading `new_messages()[-1]`); a `str`/native-mode fixture would pass even with the bug.
7. **Provider-validity:** projected histories through each vendored mapping — no orphaned tool_use/tool_result; non-`assistant` first message for Anthropic when humans lead.
8. **Integration (`InMemoryWorker` group chat):** two agents on one topic, gated; each sees the other as attributed surface-only `ModelRequest`s and itself full-fidelity; canonical wire history stays prefix-free and author-tagged; `latest_tool_calls()` never picks up another agent's calls.

---

## 15. Implementation plan

Tests first (§14). Each phase independently testable.

1. **Projection core** — `calfkit/nodes/_projection.py`: `project()`, per-role detection, ownership (incl. `tool_call_id` map), re-roling, surface extraction (3 modes + `final_result` + preamble), prefixing, `name`-strip, purity. *New file + tests.*
2. **Stamping helper** — `State.extend_with_responses(...)`; route both append sites through it. *`state.py`, `agent.py:331,487` + tests.*
3. **Loop integration** — project the run input at `agent.py:315`; surface text+structure in `final_output_parts` at `agent.py:487-492`. *`agent.py` + integration test.*
4. **Optional author** — `author=` on `invoke_node`/`execute_node`. *`client.py` + test.*
5. **Docs** — README "multi-agent channel" section; link from `ROADMAP.md`.
6. **Pre-PR** — `make fix` then `make check`.

---

## 16. Open questions / deferred

- **Multimodal surfacing** — another agent's file/image output (`FilePart` → `BinaryContent`/multimodal `UserContent`) is a documented TODO, out of scope for v1.
- **Custom output-tool name** — the structured-answer surface matches the auto-generated `final_result*` namespace, i.e. `final_result` (single output) and `final_result_<Type>` (multi-output union); both are surfaced (§5.5). Only an agent that *overrides* the name via `ToolOutput(Model, name=...)` falls outside the namespace and will not have its structured answer surfaced cross-agent. Supportable later by resolving the name from the agent's output schema; out of scope for v1.
- **Consecutive-self / leading-`ModelResponse`** — not normalized; the SDK user must not construct a channel whose history starts with an agent turn (same as pydantic-ai).
- **Turn-budget durability** — the future channel-recipe design.
- **Promoting `project()` to public** — when the channel recipe provides a vetted second consumer.
- **Privacy hardening** — surface-only-channel migration if confidentiality becomes a requirement (§10).

---

## 17. Out of scope (application responsibility)

- Sourcing/persisting the transcript beyond the on-wire `message_history`.
- Truncation strategy (and keeping tool call/return pairs together when truncating — §5.3).
- The channel's turn-taking policy and loop-budget thresholds (§9.1 gives guidance; values are the app's).
