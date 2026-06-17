import logging
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from typing import Any, ClassVar, Generic, cast

from pydantic import ValidationError

from calfkit._protocol import NodeKind
from calfkit._types import AgentOutputT
from calfkit._vendor.pydantic_ai import Agent as InternalAgentLoop
from calfkit._vendor.pydantic_ai import DeferredToolRequests
from calfkit._vendor.pydantic_ai.messages import RetryPromptPart
from calfkit._vendor.pydantic_ai.output import OutputSpec
from calfkit._vendor.pydantic_ai.settings import ModelSettings
from calfkit._vendor.pydantic_ai.tools import DeferredToolResults
from calfkit._vendor.pydantic_ai.toolsets.external import ExternalToolset
from calfkit.exceptions import MCPToolResolutionError, ToolExecutionError, safe_exc_message
from calfkit.models import Call, DataPart, NodeResult, ReturnCall, State, TailCall, TextPart
from calfkit.models.capability import CAPABILITY_VIEW_RESOURCE_KEY, SelectorResult
from calfkit.models.payload import ContentPart
from calfkit.models.session_context import SessionRunContext
from calfkit.models.state import FailedToolCall
from calfkit.models.tool_dispatch import ToolBinding, ToolCallRef, ToolProvider, ToolSelector, split_tool_declarations
from calfkit.nodes._fanout_store import FANOUT_STORE_KEY, FanoutBatchStore, KtablesFanoutBatchStore
from calfkit.nodes._projection import project, structured_output_preamble
from calfkit.nodes.base import BaseNodeDef, _SeamArg
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.worker.lifecycle import ResourceSetupContext

logger = logging.getLogger(__name__)


class BaseAgentNodeDef(
    Generic[AgentOutputT],
    BaseNodeDef,
):
    _node_kind: ClassVar[NodeKind] = "agent"

    def __init__(
        self,
        node_id: str,
        *,
        system_prompt: str = "You are a helpful AI assistant.",
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
    ):
        self.final_output_type = final_output_type
        self.system_prompt = system_prompt
        self.tools, self._tool_selectors = split_tool_declarations(tools)
        self.sequential_only_mode = sequential_only_mode

        if not isinstance(subscribe_topics, (list, tuple)):
            subscribe_topics = [subscribe_topics]

        super().__init__(
            node_id=node_id,
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

        if not self.sequential_only_mode:
            # A true fan-out agent owns its durable batch store as a node @resource (opened by the
            # worker lifecycle before serving; mirrors the worker's Capability View resource). The
            # @resource never runs under the synchronous TestKafkaBroker — offline, the test harness
            # injects a fake into the bag instead (tests/providers.py::prepare_worker).
            self.resource(name=FANOUT_STORE_KEY)(self._fanout_store_resource)

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
        store = KtablesFanoutBatchStore(bootstrap_servers=bootstrap, node_id=self.node_id)
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

    def _maybe_resolve_selectors(self, ctx: SessionRunContext, tools_registry: dict[str, ToolBinding]) -> None:
        """Selector resolution gate: per-run overrides pin the EXACT tool
        surface for the turn, so MCP selectors are skipped entirely when
        ``override_agent_tools`` is present (a strict selector must not be
        able to fail a turn the caller scoped away from MCP)."""
        if not self._tool_selectors:
            return
        if ctx.state.overrides is not None and ctx.state.overrides.override_agent_tools is not None:
            logger.debug(
                "agent=%s per-run tool overrides active; skipping MCP selector resolution for this turn",
                self.name,
            )
            return
        self._resolve_selector_tools(ctx.resources, tools_registry)

    _STALE_LOG_AFTER_SECONDS = 90.0  # 3x the default heartbeat interval

    def _resolve_selector_tools(self, resources: Mapping[str, Any], tools_registry: dict[str, ToolBinding]) -> None:
        """Per-turn MCP selector resolution against the Capability View (spec §8.4).

        Merges AFTER static tools with collision = error-log + static wins (a
        remote server must never silently shadow a locally defined tool).
        Unresolved selections warn and degrade; ``strict=True`` raises before
        the model runs. Staleness and newer-schema records are log-only (v1).
        """
        view = resources.get(CAPABILITY_VIEW_RESOURCE_KEY)
        if view is None:
            if any(getattr(sel, "strict", False) for sel in self._tool_selectors):
                raise MCPToolResolutionError(
                    f"agent {self.name!r} declares strict MCP tool selectors but no Capability View "
                    f"resource ({CAPABILITY_VIEW_RESOURCE_KEY!r}) is available — is this agent hosted "
                    "by a Worker with MCP discovery active?"
                )
            logger.warning(
                "agent=%s has MCP tool selectors but no Capability View resource; running without MCP tools",
                self.name,
            )
            return
        for selector in self._tool_selectors:
            result: SelectorResult = selector.resolve_tools(view)
            if result.unresolved:
                detail = (
                    f"toolbox={result.toolbox_id!r} missing_toolbox={result.missing_toolbox} "
                    f"missing_tools={result.missing_tools} newer_schema={result.skipped_newer_schema} "
                    f"invalid_record={result.invalid_record}"
                )
                if result.strict:
                    raise MCPToolResolutionError(f"agent {self.name!r}: strict MCP selection unresolved: {detail}")
                logger.warning("agent=%s MCP selection partially unresolved (%s); running degraded", self.name, detail)
            if result.stale_seconds is not None and result.stale_seconds > self._STALE_LOG_AFTER_SECONDS:
                logger.warning(
                    "agent=%s toolbox=%s capability record is %.0fs stale (heartbeats missing?); using last-known tools",
                    self.name,
                    result.toolbox_id,
                    result.stale_seconds,
                )
            for binding in result.bindings:
                if binding.name in tools_registry:
                    logger.error(
                        "agent=%s MCP tool %r from toolbox=%s collides with a locally configured tool; static wins",
                        self.name,
                        binding.name,
                        result.toolbox_id,
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

        # Collect all FailedToolCall results in this turn so operators see every
        # failure in a parallel batch; raise on the first after logging all.
        # Also defend against corrupt marker dicts: a payload with the calfkit
        # marker tag that fails ``FailedToolCall`` validation (e.g. schema drift
        # during rolling deploy, a required field added without default, a
        # tampered wire payload) round-trips through the union's ``| Any`` arm
        # as a plain ``dict``. Synthesize a typed marker so the silent-failure
        # path closes — operators still see the raise and the corrupt-keys
        # context.
        failed_tool_calls: list[FailedToolCall] = []
        for tc in latest_tool_calls:
            result = ctx.state.tool_results.get(tc.tool_call_id)
            if isinstance(result, FailedToolCall):
                failed_tool_calls.append(result)
            elif isinstance(result, dict) and result.get("marker_kind") == "calfkit-tool-error":
                logger.error(
                    "[%s] corrupt FailedToolCall marker detected node=%s tool_call_id=%s raw_keys=%s; "
                    "likely schema drift or version skew across the Kafka boundary",
                    ctx.correlation_id[:8],
                    self.name,
                    tc.tool_call_id,
                    sorted(result.keys()),
                )
                # Synthesize a typed marker with sentinel fields known to pass
                # FailedToolCall's validators. tc.tool_call_id is the LLM-emitted
                # correlation key; fall back to "<missing>" if it's empty so
                # ``min_length=1`` doesn't itself raise.
                raw_tool_name = result.get("tool_name")
                failed_tool_calls.append(
                    FailedToolCall(
                        tool_name=raw_tool_name if isinstance(raw_tool_name, str) and raw_tool_name else "<unknown>",
                        tool_call_id=tc.tool_call_id or "<missing>",
                        exc_type="CorruptFailedToolCallMarker",
                        exc_message=f"Marker dict failed validation as FailedToolCall (likely schema drift); raw_keys={sorted(result.keys())}",
                    )
                )
        if failed_tool_calls:
            for failure in failed_tool_calls:
                logger.error(
                    "[%s] tool execution error detected node=%s tool=%s tool_call_id=%s exc_type=%s exc_message=%s",
                    ctx.correlation_id[:8],
                    self.name,
                    failure.tool_name,
                    failure.tool_call_id,
                    failure.exc_type,
                    failure.exc_message,
                )
            first = failed_tool_calls[0]
            raise ToolExecutionError(
                tool_name=first.tool_name,
                tool_call_id=first.tool_call_id,
                exc_type=first.exc_type,
                exc_message=first.exc_message,
            )

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
                    return Call[State](
                        tools_registry[target_tool_call.tool_name].dispatch_topic,
                        ctx.state,
                        body=ToolCallRef.from_tool_call_part(target_tool_call),
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
        result = await self._agent_loop.run(
            message_history=project(ctx.state.message_history, viewer=self.name),
            instructions=ctx.state.temp_instructions,
            toolsets=[ExternalToolset([binding.tool_def for binding in tools_registry.values()])],
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
                # tools are also surfaced as RetryPromptPart instead of escaping
                # to the worker's hard FailedToolCall path.
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
                return Call[State](
                    tools_registry[target_tool_call.tool_name].dispatch_topic,
                    ctx.state,
                    body=ToolCallRef.from_tool_call_part(target_tool_call),
                )
            else:
                # Parallel fan-out: each sibling Call carries its tool_call_id as ``tag`` so the
                # callee echoes it on its reply, letting the durable fold read the result from
                # ``state.tool_results[tag]``. The handler's _handle_fanout_open opens the durable
                # batch from this list — replacing the old in-process _pending_batches registration.
                parallel_tool_calls = [
                    Call[State](
                        tools_registry[tc.tool_name].dispatch_topic,
                        ctx.state.model_copy(deep=True),
                        body=ToolCallRef.from_tool_call_part(tc),
                        tag=tc.tool_call_id,
                    )
                    for tc in pending_tool_calls
                ]
                return parallel_tool_calls

        else:
            logger.debug("[%s] final output reached, ReturnCall node=%s", ctx.correlation_id[:8], self.name)
            new_messages = result.new_messages()
            # stamp author identity onto the agent's own responses (§4, §6.1)
            ctx.state.extend_with_responses(new_messages, self.name)
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

    def add_tools(self, *tools: ToolProvider | ToolBinding | ToolSelector) -> None:
        """Add tools after construction.

        Note: a ToolSelector added AFTER the hosting worker's
        ``register_handlers()`` has run will not get a Capability View
        resource (view registration snapshots hosted nodes once, like topic
        provisioning) — it degrades per the unresolved-selector policy.
        Declare selectors before registering, or at construction.
        """
        bindings, selectors = split_tool_declarations(tools)
        self.tools.extend(bindings)
        self._tool_selectors.extend(selectors)

    def instructions(self, func: Callable[..., str | None]) -> Callable[..., str | None]:
        """Decorator to define dynamic instruction functions that can build instructions at runtime."""
        return self._agent_loop.instructions(func)


Agent = BaseAgentNodeDef
