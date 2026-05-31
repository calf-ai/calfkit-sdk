import logging
from collections.abc import Callable, Iterable
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
from calfkit.exceptions import ToolExecutionError
from calfkit.mcp._server import McpServer
from calfkit.models import Call, DataPart, NodeResult, ReturnCall, State, TailCall, TextPart
from calfkit.models.actions import Silent
from calfkit.models.node_schema import BaseToolNodeSchema
from calfkit.models.session_context import SessionRunContext
from calfkit.models.state import FailedToolCall, PendingToolBatch
from calfkit.nodes.base import BaseNodeDef, GateFunction
from calfkit.nodes.tool import BaseToolNodeDef, ToolNodeDef, _safe_exc_message
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient

# Public alias for the heterogeneous tool entries that ``Agent(tools=...)``
# accepts. Native tool nodes pass through; ``McpServer`` instances are
# flattened via their ``__iter__`` at construction time. Type-checkers see
# the widened union; runtime sees the expanded list.
ToolLike = ToolNodeDef | McpServer


def _flatten_tools(entries: Iterable[ToolLike] | None) -> list[BaseToolNodeSchema]:
    """Expand ``McpServer`` entries into their underlying tool schemas.

    Native ``ToolNodeDef`` / ``BaseToolNodeSchema`` entries are appended
    as-is. ``McpServer`` instances yield one ``BaseToolNodeSchema`` per
    filtered tool via ``__iter__``. This is what makes
    ``Agent(tools=[mcp_server])`` work end-to-end — without flattening,
    the registry build at :meth:`BaseAgentNodeDef.run` would attempt
    ``mcp_server.tool_schema.name`` and raise ``AttributeError``.
    """
    if not entries:
        return []
    flattened: list[BaseToolNodeSchema] = []
    for entry in entries:
        if isinstance(entry, McpServer):
            flattened.extend(entry)
        else:
            flattened.append(entry)
    return flattened


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
        gates: list[GateFunction] | None = None,
        tools: list[ToolLike] | None = None,
        model_client: PydanticModelClient,
        final_output_type: OutputSpec[AgentOutputT] = str,  # type: ignore[assignment]
        sequential_only_mode: bool = False,
        model_settings: ModelSettings | dict[str, Any] | None = None,
    ):
        self.final_output_type = final_output_type
        self.system_prompt = system_prompt
        self.tools: list[BaseToolNodeSchema] = _flatten_tools(tools)
        self.sequential_only_mode = sequential_only_mode
        self._pending_batches: dict[str, PendingToolBatch] = dict()

        if not isinstance(subscribe_topics, (list, tuple)):
            subscribe_topics = [subscribe_topics]

        super().__init__(node_id=node_id, subscribe_topics=subscribe_topics, publish_topic=publish_topic, gates=gates)

        self._agent_loop: InternalAgentLoop[dict[str, Any], AgentOutputT | DeferredToolRequests] = InternalAgentLoop(
            model_client,
            name=self.name,
            output_type=[final_output_type, DeferredToolRequests],
            deps_type=dict,
            instructions=system_prompt,
            model_settings=cast(ModelSettings | None, model_settings),
        )

    @staticmethod
    def _require_frame_id_for_write(ctx: SessionRunContext) -> str:
        """Resolve the per-invocation frame_id when WRITING a new parallel batch.

        ``ctx.frame_id`` is populated by ``BaseNodeDef.prepare_context`` from
        ``envelope.internal_workflow_state.current_frame.frame_id``. It is the
        only correct key for ``_pending_batches`` because parallel invocations
        of the same agent share a single ``correlation_id`` — see
        :meth:`_parallel_state_aggregation` for the collision scenario this
        defends against. A ``None`` value at write time means the agent was
        invoked outside the framework's prepare-context path (e.g. a test
        driving ``run()`` directly without seeding ``_frame_id``); raising is
        the right call because silently bucketing every batch under ``None``
        would re-introduce the exact bug this keying is designed to prevent.

        Read-side lookups tolerate ``None`` (treated as "no batch present")
        because the only way a read can find a ``None``-keyed entry is if a
        write was previously allowed to use ``None`` — which this guard
        prevents at the source.
        """
        frame_id = ctx.frame_id
        if frame_id is None:
            raise RuntimeError(
                "ctx.frame_id is None — parallel tool-batch dispatch requires a "
                "frame_id, normally populated by BaseNodeDef.prepare_context from "
                "the inbound envelope's current_frame. If you are driving run() "
                "directly in a test that fans out a parallel batch, set "
                "ctx._frame_id before invoking."
            )
        return frame_id

    def _parallel_state_aggregation(self, ctx: SessionRunContext) -> None:
        # Keyed on ``ctx.frame_id`` (per-invocation), NOT ``ctx.deps.correlation_id``:
        # a supervisor that fans out two ``Call``s to the same agent topic shares one
        # ``correlation_id`` across both invocations. Each invocation publishes onto
        # its own fresh ``CallFrame`` (UUID7 ``frame_id``), so the per-invocation
        # frame is the only key that keeps concurrent batches from clobbering each
        # other in this dict.
        frame_id = ctx.frame_id
        if frame_id is None:
            # ``BaseNodeDef.prepare_context`` unconditionally mirrors
            # ``current_frame.frame_id`` (which has a ``default_factory``) onto
            # ``ctx._frame_id``, so a None here means ``run()`` was driven outside
            # the framework handler — either a unit test or a subclass that
            # bypasses ``prepare_context``. Warn loudly so a future regression in
            # the handler path surfaces immediately instead of silently skipping
            # aggregation and potentially advancing past a real batch.
            logger.warning(
                "[%s] _parallel_state_aggregation: ctx.frame_id is None on node=%s; "
                "skipping aggregation. prepare_context is the only legitimate "
                "population path for _frame_id — a None value here indicates run() "
                "was driven outside the framework handler.",
                ctx.deps.correlation_id[:8],
                self.name,
            )
            return
        batch = self._pending_batches.get(frame_id)
        if batch is not None:
            for tool_call_id in batch.expected_tool_call_ids:
                if tool_call_id not in batch.collected_results and tool_call_id in ctx.state.tool_results:
                    batch.collected_results[tool_call_id] = ctx.state.tool_results[tool_call_id]

            if batch.is_complete:
                for tool_call_id, tool_call_result in batch.collected_results.items():
                    batch.base_state.add_tool_result(tool_call_id, tool_call_result)
                ctx.state = batch.base_state
                del self._pending_batches[frame_id]
        else:
            # Stray-reply / lost-batch detection: more than one tool_result for the
            # current ``latest_tool_calls`` is present, but no batch is registered
            # for this frame_id. The agent would have written a batch on dispatch
            # (single-tool dispatch in parallel mode is the one legitimate
            # no-batch path, hence the ``> 1`` floor to suppress that false
            # positive). Likely causes: lost batch from partition rebalance or
            # process restart, stray/duplicate delivery, or a routing error.
            # Replies will not be aggregated; if any tool_call_ids remain
            # incomplete the existing guard at the bottom of ``run()`` raises
            # ``RuntimeError`` — this warning fires in the all-arrived-together
            # race that would otherwise silently advance.
            completed_latest = [tc for tc in ctx.state.latest_tool_calls() if tc.tool_call_id in ctx.state.tool_results]
            if len(completed_latest) > 1:
                logger.warning(
                    "[%s] no PendingToolBatch for frame_id=%s on node=%s but %d completed "
                    "tool replies present (tool_call_ids=%s); replies will NOT be "
                    "aggregated. Likely a lost batch (partition rebalance or process "
                    "restart), stray/duplicate delivery, or a routing error.",
                    ctx.deps.correlation_id[:8],
                    frame_id,
                    self.name,
                    len(completed_latest),
                    [tc.tool_call_id for tc in completed_latest],
                )

    async def run(self, ctx: SessionRunContext) -> NodeResult[State]:
        tools_registry = dict[str, BaseToolNodeSchema]()
        if ctx.state.overrides is not None and ctx.state.overrides.override_agent_tools is not None:
            tools_registry = {tool.tool_schema.name: tool for tool in ctx.state.overrides.override_agent_tools}
        elif self.tools:
            tools_registry = {tool.tool_schema.name: tool for tool in self.tools}

        # ``latest_tool_calls()`` walks ``message_history`` in reverse on each call;
        # cache once for all pre-model uses. The post-model use after
        # ``result.new_messages()`` is extended into history must re-call to see
        # the model's new tool calls.
        latest_tool_calls = ctx.state.latest_tool_calls()

        logger.debug(
            "[%s] agent run entered node=%s pending_tool_calls=%d history_len=%d",
            ctx.deps.correlation_id[:8],
            self.name,
            len(latest_tool_calls),
            len(ctx.state.message_history),
        )

        if not self.sequential_only_mode:
            self._parallel_state_aggregation(ctx)
            # Defense-in-depth: ``_parallel_state_aggregation`` already warns
            # and returns early on a None ``frame_id``, but a future refactor
            # that calls this branch independently must not silently mask the
            # missing key. See ``_require_frame_id_for_write`` for why write-side
            # rejects None.
            if ctx.frame_id is None:
                logger.warning(
                    "[%s] run(): ctx.frame_id is None on node=%s; cannot look up parallel batch — treating as no batch.",
                    ctx.deps.correlation_id[:8],
                    self.name,
                )
                batch = None
            else:
                batch = self._pending_batches.get(ctx.frame_id)
            if batch and not batch.is_complete:
                return Silent()

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
                    ctx.deps.correlation_id[:8],
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
                    ctx.deps.correlation_id[:8],
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
                        ctx.deps.correlation_id[:8],
                        target_tool_call.tool_call_id,
                        target_tool_call.tool_name,
                        self.name,
                    )
                    return Call[State](
                        tools_registry[target_tool_call.tool_name].subscribe_topics[0],
                        ctx.state,
                        target_tool_call.tool_call_id,
                    )
                else:
                    remaining = [tc for tc in latest_tool_calls if tc.tool_call_id not in ctx.state.tool_results]
                    raise RuntimeError(
                        f"[{ctx.deps.correlation_id[:8]}] Parallel mode reached incomplete tool calls outside aggregation gate. "
                        f"node={self.name} frame_id={ctx.frame_id} remaining_ids={[tc.tool_call_id for tc in remaining]}. "
                        f"This indicates lost PendingToolBatch state (e.g. partition rebalance or process restart)."
                    )

            tool_results = DeferredToolResults(calls={tc.tool_call_id: ctx.state.get_tool_result(tc.tool_call_id) for tc in latest_tool_calls})

        if ctx.state.uncommitted_message is not None:
            ctx.state.commit_message_to_history()

        run_model_settings = cast(ModelSettings | None, ctx.state.overrides.model_settings) if ctx.state.overrides is not None else None
        result = await self._agent_loop.run(
            message_history=ctx.state.message_history,
            instructions=ctx.state.temp_instructions,
            toolsets=[ExternalToolset([tool.tool_schema for tool in tools_registry.values()])],
            deps=ctx.deps.provided_deps,  # None valid when AgentDepsT=NoneType
            deferred_tool_results=tool_results,
            model_settings=run_model_settings,
        )
        if isinstance(result.output, DeferredToolRequests):
            logger.debug(
                "[%s] model returned DeferredToolRequests tool_count=%d node=%s",
                ctx.deps.correlation_id[:8],
                len(result.output.calls),
                self.name,
            )
            messages = result.new_messages()
            ctx.state.message_history.extend(messages)
            latest_tool_calls = ctx.state.latest_tool_calls()

            for tool_call in result.output.calls:
                ctx.state.add_tool_call(tool_call)

                tool_node = tools_registry.get(tool_call.tool_name)
                if tool_node is None:
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

                if tool_node.subscribe_topics is None:
                    logger.error(
                        "tool=%s is unreachable. No subscribe topics were provided for the tool node.",
                        tool_call.tool_name,
                    )
                    ctx.state.add_tool_result(
                        tool_call.tool_call_id,
                        RetryPromptPart(
                            content=f"This tool ({tool_call.tool_name}) is not callable and will not run. Please do not call this tool.",  # noqa: E501
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
                    content = f"Malformed tool arguments: {type(e).__name__}: {_safe_exc_message(e)}"
                    logger.warning(
                        "[%s] tool=%s args parse failed at dispatch: %s",
                        ctx.deps.correlation_id[:8],
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
                # Override-mode schemas (BaseToolNodeSchema-only) skip this step
                # and dispatch unvalidated; this is the documented carve-out.
                if isinstance(tool_node, BaseToolNodeDef):
                    try:
                        tool_node.validate_call_args(args)
                    except ValidationError as e:
                        validation_errors = e.errors(include_url=False, include_context=False)
                        logger.warning(
                            "[%s] tool=%s arg validation failed at dispatch: %s",
                            ctx.deps.correlation_id[:8],
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
                        validator_content = f"Tool argument validator raised {type(e).__name__}: {_safe_exc_message(e)}"
                        logger.warning(
                            "[%s] tool=%s arg validator raised %s; surfacing as RetryPromptPart",
                            ctx.deps.correlation_id[:8],
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
                logger.debug("[%s] all tool calls invalid, TailCall retry node=%s", ctx.deps.correlation_id[:8], self.name)
                return TailCall[State](target_topic=self._return_topic, state=ctx.state)

            pending_tool_calls = [tc for tc in latest_tool_calls if tc.tool_call_id not in ctx.state.tool_results]

            if self.sequential_only_mode or len(pending_tool_calls) == 1:
                target_tool_call = pending_tool_calls[0]
                logger.debug(
                    "[%s] routing new tool call=%s tool=%s node=%s",
                    ctx.deps.correlation_id[:8],
                    target_tool_call.tool_call_id,
                    target_tool_call.tool_name,
                    self.name,
                )
                return Call[State](
                    tools_registry[target_tool_call.tool_name].subscribe_topics[0],
                    ctx.state,
                    target_tool_call.tool_call_id,
                )
            else:
                launch_tool_call_ids = [tc.tool_call_id for tc in pending_tool_calls]
                parallel_tool_calls = [
                    Call[State](
                        tools_registry[tc.tool_name].subscribe_topics[0],
                        ctx.state.model_copy(deep=True),
                        tc.tool_call_id,
                    )
                    for tc in pending_tool_calls
                ]

                self._pending_batches[self._require_frame_id_for_write(ctx)] = PendingToolBatch(
                    expected_tool_call_ids=frozenset(launch_tool_call_ids),
                    base_state=ctx.state.model_copy(deep=True),
                )

                return parallel_tool_calls

        else:
            logger.debug("[%s] final output reached, ReturnCall node=%s", ctx.deps.correlation_id[:8], self.name)
            ctx.state.message_history.extend(result.new_messages())
            if isinstance(result.output, str):
                ctx.state.final_output_parts = [TextPart(text=result.output)]
            else:
                ctx.state.final_output_parts = [DataPart(data=result.output)]
            return ReturnCall[State](state=ctx.state)

    def add_tools(self, *tools: ToolLike) -> None:
        self.tools.extend(_flatten_tools(tools))

    def instructions(self, func: Callable[..., str | None]) -> Callable[..., str | None]:
        """Decorator to define dynamic instruction functions that can build instructions at runtime."""
        return self._agent_loop.instructions(func)


Agent = BaseAgentNodeDef
