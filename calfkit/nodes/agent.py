import logging
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from typing import Any, ClassVar, Generic, cast

import pydantic_core
from pydantic import ValidationError

from calfkit._protocol import NodeKind
from calfkit._types import AgentOutputT
from calfkit._vendor.pydantic_ai import Agent as InternalAgentLoop
from calfkit._vendor.pydantic_ai import DeferredToolRequests
from calfkit._vendor.pydantic_ai.messages import ModelRequest, RetryPromptPart, ToolCallPart, ToolReturn, UserPromptPart
from calfkit._vendor.pydantic_ai.output import OutputSpec
from calfkit._vendor.pydantic_ai.settings import ModelSettings
from calfkit._vendor.pydantic_ai.tools import DeferredToolResults, ToolDefinition
from calfkit._vendor.pydantic_ai.toolsets.external import ExternalToolset
from calfkit.controlplane import ControlPlaneStamp, advertises
from calfkit.exceptions import DeserializationError, safe_exc_message
from calfkit.models import Call, DataPart, NodeResult, ReturnCall, State, TailCall, TextPart
from calfkit.models.agents import AGENTS_TOPIC, AGENTS_VIEW_RESOURCE_KEY, AgentCard, derive_input_topic
from calfkit.models.capability import CAPABILITY_VIEW_RESOURCE_KEY, SelectorResult
from calfkit.models.node_result import _extract_text, extract_lenient
from calfkit.models.payload import RETRY_MARKER, ContentPart, FilePart, is_retry
from calfkit.models.seam_context import SeamContext
from calfkit.models.session_context import SessionRunContext
from calfkit.models.tool_dispatch import ToolBinding, ToolCallRef, ToolProvider, ToolSelector, split_tool_declarations
from calfkit.nodes._fanout_store import FANOUT_STORE_KEY, FanoutBatchStore, KtablesFanoutBatchStore
from calfkit.nodes._projection import project, structured_output_preamble
from calfkit.nodes.base import BaseNodeDef, _SeamArg, _SlotFailed, _SlotResolved
from calfkit.nodes.tool import BaseToolNodeDef, Tools
from calfkit.peers import Handoff, Messaging
from calfkit.peers.directory import render_peer_directory, resolve_live_peers
from calfkit.peers.handoff import _HANDOFF_NO_PEERS_NOTE, HandoffRequest, _build_handoff_request
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.worker.lifecycle import ResourceSetupContext

logger = logging.getLogger(__name__)

_MESSAGE_AGENT_TOOL = "message_agent"


def _serialize_message_reply(parts: list[ContentPart] | None) -> str:
    """Serialize ALL parts of a peer's reply into one string (message_agent fold, §5.2): Text verbatim,
    Data JSON-encoded, File as a placeholder; newline-joined; empty -> "(no content)". extract_lenient
    would drop a peer's text preamble before its structured data."""
    if not parts:
        return "(no content)"
    rendered: list[str] = []
    for p in parts:
        if isinstance(p, TextPart):
            rendered.append(p.text)
        elif isinstance(p, DataPart):
            rendered.append(pydantic_core.to_json(p.data).decode())
        elif isinstance(p, FilePart):
            rendered.append(f"[file: {p.media_type} {p.uri or '<inline>'}]")
        else:
            rendered.append(pydantic_core.to_json(p).decode())
    return "\n".join(rendered)


class BaseAgentNodeDef(
    Generic[AgentOutputT],
    BaseNodeDef,
):
    _node_kind: ClassVar[NodeKind] = "agent"

    def __init__(
        self,
        name: str,
        *,
        system_prompt: str = "You are a helpful AI assistant.",
        description: str | None = None,
        subscribe_topics: str | list[str],
        publish_topic: str | None = None,
        before_node: _SeamArg = None,
        after_node: _SeamArg = None,
        on_node_error: _SeamArg = None,
        on_callee_error: _SeamArg = None,
        tools: Sequence[ToolProvider | ToolBinding | ToolSelector] | None = None,
        model_client: PydanticModelClient,
        final_output_type: OutputSpec[AgentOutputT] = str,  # type: ignore[assignment]
        sequential_only_mode: bool = False,
        model_settings: ModelSettings | dict[str, Any] | None = None,
        peers: Sequence[Messaging | Handoff] | None = None,
    ):
        self.final_output_type = final_output_type
        self.system_prompt = system_prompt
        # Public directory blurb (distinct from system_prompt) advertised on the
        # AgentCard — the only card content beyond identity + liveness (ADR-0015).
        self._description = description
        self.tools: list[ToolBinding] = []
        self._tool_selectors: list[ToolSelector] = []
        self._eager_tool_nodes: list[BaseToolNodeDef] = []
        self._add_tools(tools)  # enforce the tool-surface contract, then commit (raises before agent-loop build)
        self.sequential_only_mode = sequential_only_mode
        # peers= (ADR-0015/0019): capability handles for agent-to-agent reach (Messaging — consult; Handoff
        # — transfer control), validated + stored BEFORE the fan-out store @resource gate below (decision
        # 1(b): `_needs_durable_batch` reads `self._messaging_handles`, so a `sequential_only_mode` messaging
        # agent gets the store for its lone `message_agent`; a Handoff-only agent needs none). The own-name
        # reject lives here — a handle can't see the enclosing agent's name (M2); `peers=` type-validates
        # each element is a `Messaging`/`Handoff` handle (M4: no cross-absorption with `tools=`).
        peer_handles = tuple(peers or ())
        for peer in peer_handles:
            if not isinstance(peer, (Messaging, Handoff)):
                raise TypeError(f"peers= elements must be Messaging or Handoff handles, got {type(peer).__name__}: {peer!r}")
            if name in peer.names:
                raise ValueError(f"agent {name!r} cannot name itself in a peers= handle — remove its own name")
        # §5.1 discover-exclusivity, PER CAPABILITY: a `discover=True` handle is the exclusive author of its
        # OWN capability's scope — no named handle OF THE SAME KIND may accompany it (mirrors the shipped
        # `Tools(discover=True)` rule). Independent ACROSS capabilities: a discover Messaging coexists with a
        # named Handoff, and vice versa.
        messaging = [h for h in peer_handles if isinstance(h, Messaging)]
        if any(h.discover for h in messaging) and len(messaging) > 1:
            raise ValueError("Messaging(discover=True) is the exclusive author of the messaging scope — no other Messaging handle may accompany it")
        handoff = [h for h in peer_handles if isinstance(h, Handoff)]
        if any(h.discover for h in handoff) and len(handoff) > 1:
            raise ValueError("Handoff(discover=True) is the exclusive author of the handoff scope — no other Handoff handle may accompany it")
        self._peers: tuple[Messaging | Handoff, ...] = peer_handles
        # Reserve the built-in tool name against the construction-time tool surface (§5.2) — MESSAGING-only:
        # the built-in is injected into the ExternalToolset OUTSIDE tools_registry, so the intra-registry
        # collision guard would not see it. Only a `Messaging` handle injects `message_agent`, so a
        # Handoff-only agent reserves nothing. Checked against eager/static tools (self.tools, set by
        # _add_tools above) and named `Tools` selectors (self._tool_selectors). (A discover-resolved tool
        # node of this name is a deferred follow-up, not reserved here.)
        if self._messaging_handles:
            reserved = _MESSAGE_AGENT_TOOL in {b.name for b in self.tools} or any(
                isinstance(sel, Tools) and _MESSAGE_AGENT_TOOL in sel.names for sel in self._tool_selectors
            )
            if reserved:
                raise ValueError(
                    f"tool name {_MESSAGE_AGENT_TOOL!r} is reserved for the built-in messaging tool (a Messaging handle is set); rename the user tool"
                )

        if not isinstance(subscribe_topics, (list, tuple)):
            subscribe_topics = [subscribe_topics]

        super().__init__(
            node_id=name,
            subscribe_topics=subscribe_topics,
            publish_topic=publish_topic,
            before_node=before_node,
            after_node=after_node,
            on_node_error=on_node_error,
            on_callee_error=on_callee_error,
        )

        self._agent_loop: InternalAgentLoop[dict[str, Any], AgentOutputT | DeferredToolRequests] = InternalAgentLoop(
            model_client,
            name=self.name,
            output_type=[final_output_type, DeferredToolRequests],
            deps_type=dict,
            instructions=system_prompt,
            model_settings=cast(ModelSettings | None, model_settings),
        )

        if self._needs_durable_batch:
            # An agent that needs the durable batch machinery owns its store as a node @resource (opened
            # by the worker lifecycle before serving; mirrors the worker's Capability View resource).
            # `_needs_durable_batch` (decision 1(b)) is true for a true fan-out agent OR a messaging agent
            # (a `peers` handle) — so a `sequential_only_mode` messaging agent still gets the store for its
            # lone `message_agent`'s degenerate batch, while its tool routing stays one-at-a-time. The
            # @resource never runs under the synchronous TestKafkaBroker — offline, the test harness
            # injects a fake into the bag instead (tests/providers.py::prepare_worker).
            self.resource(name=FANOUT_STORE_KEY)(self._fanout_store_resource)

    @advertises(topic=AGENTS_TOPIC, record=AgentCard)
    def _agent_card_advert(self, stamp: ControlPlaneStamp) -> AgentCard:
        """Advertise this agent on the shared ``calf.agents`` plane (always-on, no opt-out — L7).

        Static-content advertiser: the directory ``description`` is fixed at construction, so
        the factory reads ``self._description`` directly — no session, nothing that can fail at
        publish time. Identity (the agent's name) is the wire key, never in the value;
        ``node_kind`` ("agent") rides on the worker stamp. The card carries no input topic (the
        caller derives ``agent.{name}.private.input``, ADR-0017), no system prompt, no tool list
        — the minimal card. Splatting the bare stamp (never a record) honours the
        ``@advertises`` duplicate-kwarg contract.
        """
        return AgentCard(**stamp.model_dump(), description=self._description)

    async def _fanout_store_resource(self, ctx: ResourceSetupContext["BaseAgentNodeDef[AgentOutputT]"]) -> AsyncIterator[FanoutBatchStore]:
        """Open this fan-out agent's durable batch store for the worker's lifetime.

        Mirrors the worker's Capability View resource: each ktables reader's ``start()`` is the
        catch-up gate (replays the compacted state/basestate topics to their start-time offsets),
        and the topics self-provision compacted via ktables' ``ensure_topic``. The resulting store
        lands in this node's own ``ctx.resources`` under :data:`FANOUT_STORE_KEY`.
        """
        worker = self._worker
        if worker is None:
            raise RuntimeError(f"fan-out agent {self.node_id!r} has no hosting worker; cannot open its durable store")
        bootstrap = worker._derive_bootstrap_servers()
        if not bootstrap:
            raise RuntimeError(
                f"cannot derive Kafka bootstrap servers for fan-out agent {self.node_id!r}'s durable store (client built without connect()?)."
            )
        fcfg = worker._fanout
        store = KtablesFanoutBatchStore(
            bootstrap_servers=bootstrap,
            node_id=self.node_id,
            reader_tuning=fcfg.reader_tuning,
            catchup_timeout=fcfg.catchup_timeout,
            barrier_timeout=fcfg.barrier_timeout,
        )
        await store.start()
        try:
            yield store
        finally:
            await store.stop()

    @property
    def _is_fanout_capable(self) -> bool:
        """A non-sequential agent folds durable fan-out batches in-node; a
        ``sequential_only_mode`` agent issues only single calls and never fans out."""
        return not self.sequential_only_mode

    @property
    def _seam_output_type(self) -> Any:
        """The agent's declared output type, so its OUTPUT-position seam substitutes (``before_node``
        short-circuit, ``on_node_error`` recovery, ``after_node`` replacement) are validated against it
        at coercion (scenario 44 / §6.3) — a structured-output agent's contract is machine-projected by
        its callers, so a type-breaking substitute must fail at the seam, not as a ``DeserializationError``
        downstream. ``on_callee_error`` substitutes are EXEMPT (slot position — they materialize via
        ``_resolve_slot``, never ``_coerce_output``). An exotic ``OutputSpec`` ``TypeAdapter`` cannot
        schematize degrades to a lenient skip (in the base ``_coerce_output``/``_output_view``)."""
        return self.final_output_type

    def _resolve_slot(self, ctx: SeamContext[State], outcome: _SlotResolved | _SlotFailed) -> None:
        """The agent's slot materialization (spec §6.9) — the SDK's single per-type codec. After the
        base records the ``CalleeResult``, a RESOLVED slot is materialized into the agent's private
        ``tool_results`` bookkeeping (I4), keyed by the echoed ``tag`` (the ``tool_call_id``), so the
        unchanged ``DeferredToolResults`` consumer feeds it to the next model turn:

        - a ``calf.retry``-marked part → ``RetryPromptPart`` (Anthropic ``is_error=True`` fidelity).
          The wire carries the RAW message; the agent HYDRATES the richer retry — ``tool_name`` from
          its own ``tool_calls`` (it issued the call) and the fix-and-retry suffix via the provider's
          ``model_response()`` (so it is rendered exactly once, not doubled). ``content`` is extracted
          STR-only (spec §6.9 ``_text(p)``), since ``RetryPromptPart.content`` is ``list[ErrorDetails]
          | str`` — never the lenient ``DataPart.data`` (an arbitrary non-str).
        - a plain return / handled substitute → ``ToolReturn``. The value's wire-safety is already
          enforced upstream (a handled ``on_callee_error`` substitute is eager-``to_json``-checked in
          ``_resolve_callee``/``_coerce_to_parts`` and becomes ``calf.slot.materialization_failed``
          before it can reach here; a plain return's parts came off the wire and are definitionally
          serializable). The ``to_json`` below is defensive belt-and-suspenders (spec §6.9-aligned),
          not the primary guard.

        A ``_SlotFailed`` is NOT materialized — the unhandled fault escalates at closure and never
        reaches the model (``ErrorReport`` stays off ``tool_results``)."""
        super()._resolve_slot(ctx, outcome)
        if not isinstance(outcome, _SlotResolved):
            return  # _SlotFailed: the fault escalates; it never reaches the model
        tag = outcome.tag
        if tag is None:  # impossible from a calfkit producer (the agent sets the tool_call_id as the tag)
            logger.error(
                "[%s] agent=%s resolved slot has no tag; cannot materialize into the model conversation", ctx.correlation_id[:8], self.node_id
            )
            return
        if is_retry(outcome.parts):
            call = ctx.state.tool_calls.get(tag)
            retry = RetryPromptPart(
                content=self._retry_content(outcome.parts), tool_name=call.tool_name if call is not None else None, tool_call_id=tag
            )
            ctx.state.add_tool_result(tag, retry)
            return
        # message_agent folds the WHOLE peer reply (all parts serialized) — a peer authors its own
        # multi-part sub-conversation answer, and the generic single-part extract_lenient would drop a
        # text preamble before its structured data (§5.2). Discriminated by the registered tool call's
        # name (None-guarded — a tag without a registered call falls back to the generic codec).
        call = ctx.state.tool_calls.get(tag)
        if call is not None and call.tool_name == _MESSAGE_AGENT_TOOL:
            tool_return = ToolReturn(return_value=_serialize_message_reply(outcome.parts), metadata={"tool_call_id": tag})
        else:
            tool_return = ToolReturn(return_value=extract_lenient(outcome.parts), metadata={"tool_call_id": tag})
        # defensive belt-and-suspenders (I4); wire-safety already enforced upstream in _resolve_callee/_coerce_to_parts
        pydantic_core.to_json(tool_return)
        ctx.state.add_tool_result(tag, tool_return)

    @staticmethod
    def _retry_content(parts: list[ContentPart] | None) -> str:
        """STR-only extraction for ``RetryPromptPart.content`` (spec §6.9 ``_text(p)``).

        ``RetryPromptPart.content`` is ``list[ErrorDetails] | str``, so the retry branch must NOT use
        ``extract_lenient`` (which returns ``DataPart.data`` FIRST — an arbitrary non-str). The branch
        fires on ``is_retry`` (which keys on the ``calf.retry`` MARKER, scanning any part), so the text
        must come from the MARKED ``TextPart`` — not merely the first ``TextPart`` — or an unmarked
        preamble preceding the marked text would be returned instead of the actual retry message.
        Fall back to the first ``TextPart.text`` (``_extract_text``) when no marked ``TextPart`` is
        present, then to a defensive ``str()`` of the lenient value — a hypothetical future producer
        marking a non-``TextPart`` part (``is_retry`` reads only the open ``metadata`` slot, so it is
        total over the vocabulary) degrades there rather than crashing. The contract that ``content``
        is always a ``str`` holds in every case."""
        for part in parts or []:
            if isinstance(part, TextPart) and (part.metadata or {}).get(RETRY_MARKER):
                return part.text
        try:
            return _extract_text(parts or [])
        except DeserializationError:
            return str(extract_lenient(parts))

    @property
    def _messaging_handles(self) -> list[Messaging]:
        return [h for h in self._peers if isinstance(h, Messaging)]

    @property
    def _handoff_handles(self) -> list[Handoff]:
        return [h for h in self._peers if isinstance(h, Handoff)]

    def _message_agent_tool_def(self, ctx: SessionRunContext) -> ToolDefinition | None:
        """The runtime-rendered ``message_agent`` external tool def (§5.2), or ``None`` when the agent
        carries no ``Messaging`` handle. The description renders the live, Messaging-scoped, self-excluded
        peer directory fresh each turn (so it self-heals); the tool is still produced with a "none
        reachable" body when no peer is live, so the model keeps the capability."""
        handles = self._messaging_handles
        if not handles:
            return None
        view = ctx.resources.get(AGENTS_VIEW_RESOURCE_KEY)
        directory = render_peer_directory(resolve_live_peers(view, handles, self_name=self.name))
        return ToolDefinition(
            name=_MESSAGE_AGENT_TOOL,
            description=(
                "Send a message to another agent and get its reply — a consultation: the peer answers on "
                "a fresh conversation (it sees only your message) and you keep control of this one.\n"
                f"Available agents (name — description):\n{directory}"
            ),
            parameters_json_schema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "The agent to message (a name from the list above)."},
                    "message": {"type": "string", "description": "Your message or question for that agent."},
                },
                "required": ["name", "message"],
                "additionalProperties": False,
            },
        )

    def _handoff_output_override(self, ctx: SessionRunContext) -> tuple[OutputSpec[Any] | None, str | None]:
        """The per-run handoff ``output_type`` override + the empty-set ephemeral instruction (§5.3), or
        ``(None, None)`` when the agent carries no ``Handoff`` handle (the run is left unchanged).

        With >=1 live in-scope peer: ``([final_output_type, <HandoffRequest subclass>, DeferredToolRequests],
        None)`` — the override REPLACES the construction-time type, so it carries the FULL list (dropping
        ``DeferredToolRequests`` would break tool dispatch). With a ``Handoff`` handle but NO live peer:
        ``(None, <no-peers note>)`` — the member is OMITTED (an empty ``Literal`` is unbuildable) and the note
        conveys the dormant capability via the request-level ``instructions`` (self-heals when a peer comes
        online). The live directory is the SAME ``resolve_live_peers`` render messaging uses, Handoff-scoped."""
        handles = self._handoff_handles
        if not handles:
            return None, None
        live = tuple(resolve_live_peers(ctx.resources.get(AGENTS_VIEW_RESOURCE_KEY), handles, self_name=self.name))
        if not live:
            return None, _HANDOFF_NO_PEERS_NOTE
        return [self.final_output_type, _build_handoff_request(live), DeferredToolRequests], None

    def _dispatch_handoff(self, handoff: HandoffRequest, ctx: SessionRunContext) -> TailCall[State]:
        """Route a model-produced ``HandoffRequest`` (§5.3/§5.4) — A's output is ALREADY persisted (the
        final-output branch ran ``extend_with_responses``); this only routes, thin + drop-in.

        The target is re-checked LIVE (the render->dispatch staleness race is the only case calfkit handles):
        - **live** -> null ``state.overrides`` (C2: the agent reads the state channel) + ``TailCall`` to the
          peer's input topic with ``clear_overrides=True`` (nulls the frame channel — ``prepare_context``
          re-applies ``frame.overrides`` onto ``state.overrides`` at B's start, so BOTH must be nulled). The
          frame retargets preserving frame_id/tag/callback_topic + caller_node_id, so B inherits A's ORIGINAL
          caller + full conversation and A drops out; B uses its own tools/model.
        - **stale** (gone between render and dispatch) -> do NOT relinquish: append a FEEDBACK TURN (a
          user-role ``ModelRequest``) so the history tail is a ``ModelRequest`` and the re-entered model is
          forced to re-decide against a fresh ``Literal``, then ``TailCall`` to SELF (no ``clear_overrides`` —
          keep A's surface). Without the feedback turn a ``ModelResponse``-tail history + empty instructions
          hits pydantic-ai's ``UserPromptNode`` no-model-call shortcut and the stale handoff output would be
          returned as A's answer in native/prompted mode. Mirrors the all-invalid self-retry *pattern* (a
          different branch); unbounded in v1 (#251)."""
        reachable = {n for n, _ in resolve_live_peers(ctx.resources.get(AGENTS_VIEW_RESOURCE_KEY), self._handoff_handles, self_name=self.name)}
        if handoff.name in reachable:
            logger.debug("[%s] handoff: relinquishing control to %r node=%s", ctx.correlation_id[:8], handoff.name, self.name)
            ctx.state.overrides = None
            return TailCall[State](target_topic=derive_input_topic(handoff.name), state=ctx.state, clear_overrides=True)
        # The stale self-retry is the entry to an unbounded loop (#251); WARN so an operator can see a
        # stale-handoff ring (the only signal until #251 lands a bound).
        logger.warning(
            "[%s] handoff target %r went offline between render and dispatch; self-retrying (unbounded in v1, #251) node=%s",
            ctx.correlation_id[:8],
            handoff.name,
            self.name,
        )
        feedback = (
            f"The agent {handoff.name!r} you tried to hand off to is no longer available. "
            "Choose another agent that is online, or answer the user directly."
        )
        ctx.state.message_history.append(ModelRequest(parts=[UserPromptPart(content=feedback)]))
        return TailCall[State](target_topic=self._return_topic, state=ctx.state)

    def _message_agent_target_error(self, name: str, ctx: SessionRunContext) -> str | None:
        """The model-visible reason a ``message_agent`` target is invalid, or ``None`` when it is OK to
        dispatch: self (§5.1 M2), a messaging cycle (the target already an ancestor caller, §8/ADR-0016),
        or offline/out-of-scope (re-reading the live view at dispatch). Identity-matched against
        ``ctx.ancestor_callers`` so a public-entry ring is caught while a legitimate diamond is allowed."""
        if name == self.name:
            return f"You cannot message yourself ({name!r})."
        if (name, self._node_kind) in ctx.ancestor_callers:
            return f"Cannot message {name!r}: it is already awaiting your reply (this would create a messaging cycle)."
        reachable = {n for n, _ in resolve_live_peers(ctx.resources.get(AGENTS_VIEW_RESOURCE_KEY), self._messaging_handles, self_name=self.name)}
        if name not in reachable:
            return f"Agent {name!r} is not currently reachable — it is offline or not in your messaging scope. Choose from the agents listed in the tool description."  # noqa: E501
        return None

    def _message_agent_retry(self, tool_call: ToolCallPart, ctx: SessionRunContext, content: str) -> None:
        """Land a model-visible ``RetryPromptPart`` for a bad ``message_agent`` call (so the slot completes
        and never dispatches), keyed by ``tool_call_id`` like any tool result."""
        ctx.state.add_tool_result(
            tool_call.tool_call_id, RetryPromptPart(content=content, tool_name=tool_call.tool_name, tool_call_id=tool_call.tool_call_id)
        )

    def _validate_message_agent(self, tool_call: ToolCallPart, ctx: SessionRunContext) -> None:
        """Validate a message_agent call's target; RetryPromptPart on failure, else leave it pending
        (no tools_registry binding) so the dispatch builds the peer Call. View re-read at dispatch."""
        try:
            args = tool_call.args_as_dict()
        except Exception as e:  # noqa: BLE001
            self._message_agent_retry(tool_call, ctx, f"Malformed message_agent arguments: {type(e).__name__}: {safe_exc_message(e)}")
            return
        name, message = args.get("name"), args.get("message")
        if not isinstance(name, str) or not name.strip() or not isinstance(message, str) or not message.strip():
            self._message_agent_retry(tool_call, ctx, "message_agent requires non-empty string 'name' and 'message' arguments.")
            return
        error = self._message_agent_target_error(name, ctx)
        if error is not None:
            self._message_agent_retry(tool_call, ctx, error)

    def _message_agent_call(self, tool_call: ToolCallPart) -> Call[State]:
        """Build the peer Call: a fresh seeded sub-state (message staged as a user turn) to the peer's
        derived input topic, isolate_state=True so the caller snapshots/restores (C1)."""
        args = tool_call.args_as_dict()
        seed = State()
        seed.stage_message(ModelRequest.user_text_prompt(args["message"]))
        return Call[State](derive_input_topic(args["name"]), seed, tag=tool_call.tool_call_id, isolate_state=True)

    def _maybe_resolve_selectors(self, ctx: SessionRunContext, tools_registry: dict[str, ToolBinding]) -> None:
        """Selector resolution gate: per-run overrides pin the EXACT tool
        surface for the turn, so selectors are skipped entirely when
        ``override_agent_tools`` is present (overrides are the exact surface;
        discovery must not widen a turn the caller scoped away from it)."""
        if not self._tool_selectors:
            return
        if ctx.state.overrides is not None and ctx.state.overrides.override_agent_tools is not None:
            logger.debug(
                "agent=%s per-run tool overrides active; skipping selector resolution for this turn",
                self.name,
            )
            return
        self._resolve_selector_tools(ctx.resources, tools_registry)

    def _resolve_selector_tools(self, resources: Mapping[str, Any], tools_registry: dict[str, ToolBinding]) -> None:
        """Per-turn tool-selector resolution against the capability view.

        Merges AFTER static tools with collision = error-log + existing wins (a
        discovered tool must never silently shadow a locally configured one). The
        :class:`ControlPlaneView` owns staleness + schema-version filtering, so an
        unresolved selection (missing target/tools, wrong kind, or a poisoned record)
        only warns and degrades — there is no strict fail path.
        """
        view = resources.get(CAPABILITY_VIEW_RESOURCE_KEY)
        if view is None:
            logger.warning(
                "agent=%s has tool selectors but no Capability View resource; running without discovered tools",
                self.name,
            )
            return
        # CRITICAL-4 (log-only): a degraded/failed reader serves a possibly-frozen view.
        # Surface it so the staleness is observable — but never block the turn.
        status = getattr(view, "status", None)
        failure = getattr(view, "failure", None)
        if failure is not None or status in ("degraded", "failed"):
            logger.warning(
                "agent=%s resolving tools against a %s Capability View (failure=%r); tools may be stale",
                self.name,
                status,
                failure,
            )
        for selector in self._tool_selectors:
            result: SelectorResult = selector.resolve_tools(view)
            if isinstance(selector, Tools) and selector.discover:
                # Discover names nothing, so a healthy view with zero tool nodes resolves silently
                # (a legitimate empty cluster, not a misconfiguration). A DEBUG count aids the
                # "why does my agent have no tools?" case without crying wolf.
                logger.debug("agent=%s discover mode resolved %d tool node(s)", self.name, len(result.bindings))
            if result.unresolved:
                logger.warning(
                    "agent=%s tool selection partially unresolved by %r (missing_targets=%s "
                    "missing_tools=%s invalid_targets=%s wrong_kind_targets=%s); running degraded",
                    self.name,
                    selector,
                    result.missing_targets,
                    result.missing_tools,
                    result.invalid_targets,
                    result.wrong_kind_targets,
                )
            for binding in result.bindings:
                if binding.name in tools_registry:
                    logger.error(
                        "agent=%s discovered tool %r from selector %r collides with an existing tool; existing wins",
                        self.name,
                        binding.name,
                        selector,
                    )
                    continue
                tools_registry[binding.name] = binding

    async def run(self, ctx: SessionRunContext) -> NodeResult[State]:
        tools_registry = dict[str, ToolBinding]()
        if ctx.state.overrides is not None and ctx.state.overrides.override_agent_tools is not None:
            # Override tools arrive over the wire as ToolBindings whose
            # validator was stripped at serialization, so they dispatch
            # unvalidated (the documented schema-only carve-out).
            tools_registry = {binding.name: binding for binding in ctx.state.overrides.override_agent_tools}
        elif self.tools:
            tools_registry = {binding.name: binding for binding in self.tools}

        self._maybe_resolve_selectors(ctx, tools_registry)

        # ``latest_tool_calls()`` walks ``message_history`` in reverse on each call;
        # cache once for all pre-model uses. The post-model use after
        # ``result.new_messages()`` is extended into history must re-call to see
        # the model's new tool calls.
        latest_tool_calls = ctx.state.latest_tool_calls()

        logger.debug(
            "[%s] agent run entered node=%s pending_tool_calls=%d history_len=%d",
            ctx.correlation_id[:8],
            self.name,
            len(latest_tool_calls),
            len(ctx.state.message_history),
        )

        # Parallel fan-out aggregation lives in the durable in-node fold (BaseNodeDef._aggregate):
        # sibling replies are folded in the handler and never reach run(); run() is re-entered
        # (with all tool results materialized) only at the batch's durable close. There is no
        # in-process park here — incomplete batches park in the handler, not run().
        #
        # A faulting tool no longer lands a FailedToolCall in tool_results — its exception escalates via
        # the rail (the handler's stage-1 on_callee_error / the closing batch's fault group, §6.9), so
        # everything materialized into tool_results is a ToolReturn / RetryPromptPart. The agent's old
        # FailedToolCall scan → ToolExecutionError is gone (the carriage switch, 4.4); nothing
        # un-materializable reaches the DeferredToolResults consumer below.

        tool_results = None

        if len(latest_tool_calls) > 0:
            if not ctx.state.all_call_ids_complete(*[tc.tool_call_id for tc in latest_tool_calls]):
                if self.sequential_only_mode:
                    target_tool_call = next(tc for tc in latest_tool_calls if tc.tool_call_id not in ctx.state.tool_results)
                    logger.debug(
                        "[%s] routing pending tool call=%s tool=%s node=%s",
                        ctx.correlation_id[:8],
                        target_tool_call.tool_call_id,
                        target_tool_call.tool_name,
                        self.name,
                    )
                    if target_tool_call.tool_name == _MESSAGE_AGENT_TOOL:
                        return self._message_agent_call(target_tool_call)
                    return Call[State](
                        tools_registry[target_tool_call.tool_name].dispatch_topic,
                        ctx.state,
                        body=ToolCallRef.from_tool_call_part(target_tool_call),
                        # tag=tool_call_id so the tool's reply is self-describing (§4.2): the framework
                        # echoes it on reply.tag and the agent materializes the result at that slot.
                        tag=target_tool_call.tool_call_id,
                    )
                else:
                    remaining = [tc for tc in latest_tool_calls if tc.tool_call_id not in ctx.state.tool_results]
                    raise RuntimeError(
                        f"[{ctx.correlation_id[:8]}] Parallel mode reached incomplete tool calls in run(). "
                        f"node={self.name} frame_id={ctx.frame_id} remaining_ids={[tc.tool_call_id for tc in remaining]}. "
                        f"The durable in-node fold should re-enter run() only on a complete batch — this indicates "
                        f"the fan-out did not fully fold before re-entry (e.g. a lost batch from rebalance/restart)."
                    )

            tool_results = DeferredToolResults(calls={tc.tool_call_id: ctx.state.get_tool_result(tc.tool_call_id) for tc in latest_tool_calls})

        if ctx.state.uncommitted_message is not None:
            ctx.state.commit_message_to_history()

        run_model_settings = cast(ModelSettings | None, ctx.state.overrides.model_settings) if ctx.state.overrides is not None else None
        # Run the model on the agent's POV projection of the canonical history
        # (docs/designs/agent-pov-projection.md §6.1). ``project()`` returns a fresh list;
        # the canonical ``ctx.state.message_history`` is left untouched for storage,
        # republishing, and dispatch logic (which keys on canonical, §6.2).
        external_defs = [binding.tool_def for binding in tools_registry.values()]
        # Inject the built-in message_agent tool as a REAL ExternalToolset member (L1): a name absent from
        # the toolset is classified `unknown` and auto-retried INSIDE the model run (pydantic-ai
        # ModelRetry), never reaching calfkit dispatch. `_message_agent_tool_def` returns None when the
        # agent carries no Messaging handle, so a non-messaging agent's toolset is unchanged.
        message_agent_def = self._message_agent_tool_def(ctx)
        if message_agent_def is not None:
            external_defs.append(message_agent_def)
        # Handoff (§5.3): override the per-run output_type to add the HandoffRequest union member built over
        # the live in-scope directory (so the model MAY transfer control); an empty live set omits the member
        # and instead injects an ephemeral "no agents online" note into the request-level instructions
        # (a None-FILTERED list — a bare [None, note] raises TypeError). `(None, None)` for a non-handoff
        # agent leaves both output_type (construction-time default) and instructions unchanged.
        handoff_output_type, handoff_note = self._handoff_output_override(ctx)
        instructions: str | list[str] | None
        if handoff_note is None:
            instructions = ctx.state.temp_instructions
        else:
            instructions = [s for s in (ctx.state.temp_instructions, handoff_note) if s]
        result = await self._agent_loop.run(
            message_history=project(ctx.state.message_history, viewer=self.name),
            instructions=instructions,
            # `run`'s overloads accept either a strict `None` or a non-`None` OutputSpec; a `None`-able value
            # matches neither, but the impl signature accepts `None` (-> construction-time default, no
            # rebuild). Cast to bridge the overload — `None` here is the no-override / member-omitted path.
            output_type=cast("OutputSpec[Any]", handoff_output_type),
            toolsets=[ExternalToolset(external_defs)],
            deps=ctx.deps,
            deferred_tool_results=tool_results,
            model_settings=run_model_settings,
        )
        if isinstance(result.output, DeferredToolRequests):
            logger.debug(
                "[%s] model returned DeferredToolRequests tool_count=%d node=%s",
                ctx.correlation_id[:8],
                len(result.output.calls),
                self.name,
            )
            messages = result.new_messages()
            # stamp author identity onto the agent's own responses (§4, §6.1)
            ctx.state.extend_with_responses(messages, self.name)
            latest_tool_calls = ctx.state.latest_tool_calls()

            for tool_call in result.output.calls:
                ctx.state.add_tool_call(tool_call)

                # message_agent forks BEFORE the tools_registry lookup (it is never a registry binding):
                # validate the target (RetryPromptPart on self/cycle/offline/malformed), else leave it
                # pending so the dispatch path below builds the peer Call. add_tool_call above is still
                # load-bearing for the completion check (KeyError on an unregistered id).
                if tool_call.tool_name == _MESSAGE_AGENT_TOOL:
                    self._validate_message_agent(tool_call, ctx)
                    continue

                binding = tools_registry.get(tool_call.tool_name)
                if binding is None:
                    logger.error("tool=%s does not exist.", tool_call.tool_name)
                    ctx.state.add_tool_result(
                        tool_call.tool_call_id,
                        RetryPromptPart(
                            content=f"There is no tool named {tool_call.tool_name}, it does not exist. Please ensure you are only calling tools you are provided.",  # noqa: E501
                            tool_name=tool_call.tool_name,
                            tool_call_id=tool_call.tool_call_id,
                        ),
                    )
                    continue

                # Parse args from the LLM's emission. Applies to ALL dispatch
                # paths so that malformed-JSON args from override (schema-only)
                # tools are also surfaced as an LLM-visible RetryPromptPart at the
                # agent, instead of dispatching unparseable args across the wire.
                #
                # ``args_as_dict()`` can raise more than just ValueError /
                # AssertionError: ``pydantic_core.from_json`` raises TypeError
                # when ``args`` is a non-string/non-bytes value (e.g. an int or
                # list emitted by an off-spec provider). Catch broadly so any
                # parse failure surfaces as an LLM-retryable RetryPromptPart
                # rather than escaping ``run()`` and hanging the caller.
                try:
                    args = tool_call.args_as_dict()
                except Exception as e:
                    content = f"Malformed tool arguments: {type(e).__name__}: {safe_exc_message(e)}"
                    logger.warning(
                        "[%s] tool=%s args parse failed at dispatch: %s",
                        ctx.correlation_id[:8],
                        tool_call.tool_name,
                        content,
                        exc_info=True,
                    )
                    ctx.state.add_tool_result(
                        tool_call.tool_call_id,
                        RetryPromptPart(
                            content=content,
                            tool_name=tool_call.tool_name,
                            tool_call_id=tool_call.tool_call_id,
                        ),
                    )
                    continue

                # Validate against the schema if we have a runtime validator.
                # Validator-less bindings (e.g. wire-deserialized overrides)
                # skip this step and dispatch unvalidated; this is the
                # documented carve-out.
                if binding.validator is not None:
                    try:
                        binding.validator(args)
                    except ValidationError as e:
                        validation_errors = e.errors(include_url=False, include_context=False)
                        logger.warning(
                            "[%s] tool=%s arg validation failed at dispatch: %s",
                            ctx.correlation_id[:8],
                            tool_call.tool_name,
                            validation_errors,
                        )
                        ctx.state.add_tool_result(
                            tool_call.tool_call_id,
                            RetryPromptPart(
                                content=validation_errors,
                                tool_name=tool_call.tool_name,
                                tool_call_id=tool_call.tool_call_id,
                            ),
                        )
                        continue
                    except Exception as e:
                        # A user-authored Pydantic ``field_validator`` raised
                        # something other than ``ValidationError`` (e.g.
                        # ``RuntimeError``, ``TypeError``, a custom exception).
                        # Surface as ``RetryPromptPart`` so the LLM can retry
                        # rather than letting the exception escape ``run()`` and
                        # silently hang the caller.
                        validator_content = f"Tool argument validator raised {type(e).__name__}: {safe_exc_message(e)}"
                        logger.warning(
                            "[%s] tool=%s arg validator raised %s; surfacing as RetryPromptPart",
                            ctx.correlation_id[:8],
                            tool_call.tool_name,
                            type(e).__name__,
                            exc_info=True,
                        )
                        ctx.state.add_tool_result(
                            tool_call.tool_call_id,
                            RetryPromptPart(
                                content=validator_content,
                                tool_name=tool_call.tool_name,
                                tool_call_id=tool_call.tool_call_id,
                            ),
                        )
                        continue

            if ctx.state.all_call_ids_complete(*[tc.tool_call_id for tc in latest_tool_calls]):
                # TODO: maybe consider a node retry return type that doesn't require round trip to itself.
                # Tailcall to itself is a roundtrip.
                logger.debug("[%s] all tool calls invalid, TailCall retry node=%s", ctx.correlation_id[:8], self.name)
                return TailCall[State](target_topic=self._return_topic, state=ctx.state)

            pending_tool_calls = [tc for tc in latest_tool_calls if tc.tool_call_id not in ctx.state.tool_results]

            if self.sequential_only_mode or len(pending_tool_calls) == 1:
                target_tool_call = pending_tool_calls[0]
                logger.debug(
                    "[%s] routing new tool call=%s tool=%s node=%s",
                    ctx.correlation_id[:8],
                    target_tool_call.tool_call_id,
                    target_tool_call.tool_name,
                    self.name,
                )
                if target_tool_call.tool_name == _MESSAGE_AGENT_TOOL:
                    return self._message_agent_call(target_tool_call)
                return Call[State](
                    tools_registry[target_tool_call.tool_name].dispatch_topic,
                    ctx.state,
                    body=ToolCallRef.from_tool_call_part(target_tool_call),
                    # tag=tool_call_id so the tool's reply is self-describing (§4.2): the framework
                    # echoes it on reply.tag and the agent materializes the result at that slot.
                    tag=target_tool_call.tool_call_id,
                )
            else:
                # Parallel fan-out: each sibling Call carries its tool_call_id as ``tag`` so the callee
                # echoes it on its reply (reply.tag); stage-1 resolves the slot and the durable fold
                # records the outcome under it. The handler's _handle_fanout_open opens the durable
                # batch from this list — replacing the old in-process _pending_batches registration.
                parallel_tool_calls = [
                    # Per-kind sibling construction (§5.4 / L13): a message_agent gets a fresh-seeded
                    # isolate_state Call to the peer's derived topic (no ToolCallRef body); a tool gets a
                    # deep-copied caller state + its ToolCallRef. They open one durable batch and fold by tag.
                    self._message_agent_call(tc)
                    if tc.tool_name == _MESSAGE_AGENT_TOOL
                    else Call[State](
                        tools_registry[tc.tool_name].dispatch_topic,
                        ctx.state.model_copy(deep=True),
                        body=ToolCallRef.from_tool_call_part(tc),
                        tag=tc.tool_call_id,
                    )
                    for tc in pending_tool_calls
                ]
                return parallel_tool_calls

        else:
            new_messages = result.new_messages()
            # stamp author identity onto the agent's own responses (§4, §6.1)
            ctx.state.extend_with_responses(new_messages, self.name)
            # Handoff (§5.3/§5.4): A produced a HandoffRequest as its turn output (already persisted above —
            # do NOT extend again). Transfer control to the live peer via a TailCall, or self-retry on the
            # render->dispatch staleness race. Invalid/self/hallucinated names never reach here (the per-turn
            # Literal + pydantic-ai auto-retry handle them). Discriminated by isinstance (mode-agnostic).
            # `_dispatch_handoff` logs its own disposition; this branch's log below is the ReturnCall path only.
            if isinstance(result.output, HandoffRequest):
                return self._dispatch_handoff(result.output, ctx)
            logger.debug("[%s] final output reached, ReturnCall node=%s", ctx.correlation_id[:8], self.name)
            if isinstance(result.output, str):
                parts: list[ContentPart] = [TextPart(text=result.output)]
            else:
                # Surface a text preamble alongside the structured value when present (§7).
                # ``structured_output_preamble`` reads the run's last ModelResponse and
                # returns text ONLY in tool mode (where it is a genuine preamble distinct
                # from the ``final_result`` call); in native/prompted mode the response's
                # TextPart IS the JSON answer, so it returns "" to avoid duplicating it
                # alongside the DataPart.
                parts = []
                preamble = structured_output_preamble(new_messages)
                if preamble:
                    parts.append(TextPart(text=preamble))
                parts.append(DataPart(data=result.output))
            # Output rides the reply slot (ReturnCall.value -> reply.parts at the
            # chokepoint), not the retired State.final_output_parts side-channel (§4.5).
            return ReturnCall[State](state=ctx.state, value=parts)

    def _add_tools(self, raw_tools: Sequence[ToolProvider | ToolBinding | ToolSelector] | None) -> None:
        """Validate the prospective tool surface against the contract, then commit.

        Shared by ``__init__`` and :meth:`add_tools`. The tool-surface contract (spec §15.3) is
        checked over the RAW entries — types intact before ``split_tool_declarations`` flattens a
        tool node into a bare :class:`ToolBinding`:

          1. **No duplicate tool names** across eager bindings + named ``Tools``.
          2. **``Tools(discover=True)`` owns the tool-node surface** — no eager tool node and no
             named ``Tools(...)`` may accompany it (an ``MCPToolbox``, a different node kind, may).

        Validate-before-commit: a raised ``ValueError`` leaves the existing surface unchanged.
        """
        bindings, selectors = split_tool_declarations(raw_tools)
        # The eager tool nodes, kept TYPED — read off the raw ``BaseToolNodeDef`` entries (which the
        # split flattens into bindings). The discover-exclusivity check needs to know which
        # references are tool nodes, and that type is gone from ``self.tools`` after the split.
        new_nodes = [t for t in (raw_tools or ()) if isinstance(t, BaseToolNodeDef)]
        eager_nodes = self._eager_tool_nodes + new_nodes
        selectors_all = self._tool_selectors + selectors
        named = [s for s in selectors_all if isinstance(s, Tools) and not s.discover]
        discover = any(isinstance(s, Tools) and s.discover for s in selectors_all)

        # (2) discover owns the tool-node surface
        if discover and (eager_nodes or named):
            raise ValueError(
                "Tools(discover=True) owns the agent's tool-node surface: no eager tool node "
                "or named Tools(...) may accompany it (an MCPToolbox may)."
            )
        # (1) no duplicate tool names across the statically-named sources
        static_names = [b.name for b in self.tools + bindings] + [n for s in named for n in s.names]
        dupes = sorted({n for n in static_names if static_names.count(n) > 1})
        if dupes:
            raise ValueError(f"duplicate tool name(s) in tools=: {dupes}; each tool may be referenced once")

        self.tools += bindings  # commit only after both checks pass
        self._tool_selectors += selectors
        self._eager_tool_nodes = eager_nodes

    def add_tools(self, *tools: ToolProvider | ToolBinding | ToolSelector) -> None:
        """Add tools after construction (enforces the §15.3 tool-surface contract).

        Note: a ToolSelector added AFTER the hosting worker's
        ``register_handlers()`` has run will not get a Capability View
        resource (view registration snapshots hosted nodes once, like topic
        provisioning) — it degrades per the unresolved-selector policy.
        Declare selectors before registering, or at construction.
        """
        self._add_tools(tools)

    def instructions(self, func: Callable[..., str | None]) -> Callable[..., str | None]:
        """Decorator to define dynamic instruction functions that can build instructions at runtime."""
        return self._agent_loop.instructions(func)


Agent = BaseAgentNodeDef
