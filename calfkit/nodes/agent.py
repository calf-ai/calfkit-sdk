import logging
import time
import warnings
from collections.abc import Callable
from typing import Annotated, Any, ClassVar, Generic, cast

from faststream import Context, Response
from faststream.kafka.annotations import KafkaBroker as BrokerAnnotation

from calfkit._protocol import (
    HDR_DEGRADED_MERGE,
    HDR_FANOUT_ID,
    HDR_FRAME_ID,
    NodeKind,
    decode_header_str,
)
from calfkit._types import AgentOutputT
from calfkit._vendor.pydantic_ai import Agent as InternalAgentLoop
from calfkit._vendor.pydantic_ai import DeferredToolRequests
from calfkit._vendor.pydantic_ai.messages import RetryPromptPart
from calfkit._vendor.pydantic_ai.output import OutputSpec
from calfkit._vendor.pydantic_ai.settings import ModelSettings
from calfkit._vendor.pydantic_ai.tools import DeferredToolResults
from calfkit._vendor.pydantic_ai.toolsets.external import ExternalToolset
from calfkit.models import Call, DataPart, NodeResult, ReturnCall, State, TailCall, TextPart
from calfkit.models.envelope import Envelope
from calfkit.models.node_schema import BaseToolNodeSchema
from calfkit.models.session_context import SessionRunContext
from calfkit.nodes.aggregator import FanOutAggregator
from calfkit.nodes.aggregator._in_memory_store import _InFlightBatch
from calfkit.nodes.aggregator.aggregator import MergeErrorPolicy
from calfkit.nodes.aggregator.errors import AggregatorMergeError
from calfkit.nodes.aggregator.state import AggregatedReturn, ToolCallId
from calfkit.nodes.base import BaseNodeDef, GateFunction, _KafkaSubscription
from calfkit.nodes.tool import ToolNodeDef
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient

logger = logging.getLogger(__name__)

NoneType = type(None)


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
        tools: list[ToolNodeDef] | None = None,
        model_client: PydanticModelClient,
        final_output_type: OutputSpec[AgentOutputT] = str,  # type: ignore[assignment]
        sequential_only_mode: bool = False,
        model_settings: ModelSettings | dict[str, Any] | None = None,
        aggregator: FanOutAggregator | None = None,
    ):
        self.final_output_type = final_output_type
        self.system_prompt = system_prompt
        self.tools = tools or list()
        if sequential_only_mode:
            warnings.warn(
                "sequential_only_mode=True is deprecated and will be removed in a future "
                "release. The durable FanOutAggregator handles parallel fan-out correctly "
                "across worker restarts and partition rebalances; sequential routing is no "
                "longer necessary as a correctness workaround. If you have a use case that "
                "requires strictly sequential tool routing, file an issue.",
                DeprecationWarning,
                stacklevel=2,
            )
        self.sequential_only_mode = sequential_only_mode
        # The aggregator owns the durable join-state for parallel tool fan-out.
        # An instance is always present so the Worker can wire it up at startup;
        # subclasses (or callers) override behaviour by passing their own.
        self.aggregator: FanOutAggregator = aggregator if aggregator is not None else FanOutAggregator()

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

    @property
    def fanout_state_topic(self) -> str:
        """Well-known topic name for this agent's compacted state topic."""
        return f"{self.node_id}.fanout-state"

    @property
    def fanout_returns_topic(self) -> str:
        """Well-known topic name for this agent's fan-out returns topic."""
        return f"{self.node_id}.fanout-returns"

    def kafka_subscriptions(self) -> list[_KafkaSubscription]:
        """Return the agent's main subscription plus the aggregator's returns subscription.

        The aggregator subscription is always included (using well-known
        topic names derived from ``node_id``) so :meth:`Worker.register_handlers`
        registers it regardless of whether
        :meth:`FanOutAggregator.setup` has run yet. The rebalance listener
        is included only after ``setup()`` has populated it (during
        ``Worker.run()`` in production); in tests that bypass
        ``Worker.run()`` and rely on ``TestKafkaBroker``, the listener is
        absent — which is correct because the test harness doesn't simulate
        partition rebalances.
        """
        subscriptions = list(super().kafka_subscriptions())
        subscriptions.append(
            _KafkaSubscription(
                topics=[self.fanout_returns_topic],
                handler=self._aggregator_handler,
                listener=self.aggregator._rebalance_listener,
                # Force serial processing: a single asyncio loop processes
                # returns for this agent's partitions, so read-modify-write
                # on the state store is linearizable.
                max_workers=1,
            )
        )
        return subscriptions

    async def _ensure_aggregator_ready(self, broker: BrokerAnnotation) -> None:
        """Lazily run :meth:`FanOutAggregator.setup` if it hasn't been called yet.

        The Worker pre-runs setup in production; this fallback covers test
        paths that bypass ``Worker.run()`` (``TestKafkaBroker`` flows) so the
        first parallel fan-out can construct the state store on demand.
        Idempotent: subsequent calls are no-ops.

        Test paths don't have a :class:`KafkaConfig` threaded through, so
        we synthesise a placeholder here. The transient
        :class:`AIOKafkaConsumer` constructed with it is never invoked
        under ``TestKafkaBroker`` (which simulates Kafka in memory), so
        the placeholder values never reach a real broker.
        """
        if self.aggregator._state_store is None:
            # Local import to avoid a circular at module-load time
            # (calfkit.client imports calfkit.nodes via partitioner).
            from calfkit.client.kafka_config import KafkaConfig

            await self.aggregator.setup(
                broker,
                node_id=self.node_id,
                main_topic=self.subscribe_topics[0],
                kafka_config=KafkaConfig(bootstrap_servers="localhost:9092", client_kwargs={}),
            )

    async def run(self, ctx: SessionRunContext) -> NodeResult[State]:
        tools_registry = dict[str, BaseToolNodeSchema]()
        if ctx.state.overrides is not None and ctx.state.overrides.override_agent_tools is not None:
            tools_registry = {tool.tool_schema.name: tool for tool in ctx.state.overrides.override_agent_tools}
        elif self.tools:
            tools_registry = {tool.tool_schema.name: tool for tool in self.tools}

        logger.debug(
            "[%s] agent run entered node=%s pending_tool_calls=%d history_len=%d",
            ctx.deps.correlation_id[:8],
            self.name,
            len(ctx.state.latest_tool_calls()),
            len(ctx.state.message_history),
        )

        latest_tool_calls = ctx.state.latest_tool_calls()
        tool_results = None

        if len(latest_tool_calls) > 0:
            if not ctx.state.all_call_ids_complete(*[tc.tool_call_id for tc in latest_tool_calls]):
                # In sequential mode, route the next pending tool one at a time.
                # In parallel mode, we shouldn't be here — the aggregator has
                # already merged everything by the time we re-enter run().
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
                elif tool_node.subscribe_topics is None:
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

            if ctx.state.all_call_ids_complete(*[tc.tool_call_id for tc in latest_tool_calls]):
                # All tool calls were invalid; retry by tail-calling the agent.
                logger.debug("[%s] all tool calls invalid, TailCall retry node=%s", ctx.deps.correlation_id[:8], self.name)
                return TailCall[State](target_topic=self.subscribe_topics[0], state=ctx.state)

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
                # Parallel fan-out: emit list[Call]. The base class's
                # _publish_action recognises the agent has an aggregator
                # attached and (a) writes the initial FanOutState record
                # to the compacted state topic and (b) routes each Call's
                # callback to {node_id}.fanout-returns so the aggregator
                # collects the returns durably.
                parallel_tool_calls = [
                    Call[State](
                        tools_registry[tc.tool_name].subscribe_topics[0],
                        ctx.state.model_copy(deep=True),
                        tc.tool_call_id,
                    )
                    for tc in pending_tool_calls
                ]
                return parallel_tool_calls

        else:
            logger.debug("[%s] final output reached, ReturnCall node=%s", ctx.deps.correlation_id[:8], self.name)
            ctx.state.message_history.extend(result.new_messages())
            if isinstance(result.output, str):
                ctx.state.final_output_parts = [TextPart(text=result.output)]
            else:
                ctx.state.final_output_parts = [DataPart(data=result.output)]
            return ReturnCall[State](state=ctx.state)

    async def _publish_action(
        self,
        output: NodeResult[State],
        envelope: Envelope,
        correlation_id: str,
        broker: BrokerAnnotation,
        inbound_headers: dict[str, Any] | None = None,
        inbound_partition: int | None = None,
    ) -> Envelope:
        """Override the parallel-fan-out branch to write the durable batch
        record before publishing tool Calls, and route each Call's callback
        to the aggregator's returns topic.

        All other branches (single Call, ReturnCall, TailCall, Silent) fall
        through to :meth:`BaseNodeDef._publish_action` unchanged.
        """
        if isinstance(output, list) and output and all(isinstance(item, Call) for item in output):
            await self._ensure_aggregator_ready(broker)
            await self._publish_parallel_with_aggregator(
                output,
                envelope,
                correlation_id,
                broker,
                partition=inbound_partition,
            )
            return envelope

        return await super()._publish_action(
            output,
            envelope,
            correlation_id,
            broker,
            inbound_headers=inbound_headers,
            inbound_partition=inbound_partition,
        )

    async def _publish_parallel_with_aggregator(
        self,
        calls: list[Call[State]],
        envelope: Envelope,
        correlation_id: str,
        broker: BrokerAnnotation,
        *,
        partition: int | None = None,
    ) -> None:
        """Publish a parallel tool fan-out with durable aggregator semantics.

        Steps:

        1. Derive ``fan_out_id`` deterministically from the inbound
           ``CallFrame.frame_id`` so redelivered inbounds produce the same
           id (idempotent dispatch).
        2. Idempotency check against the durable state: if the batch is
           already complete (tombstoned within the recently-completed TTL),
           skip the fan-out entirely. If it's still in-flight with the
           same ``expected_tool_call_ids``, preserve the merged ``received``
           state and re-publish the Calls; the tool layer is responsible
           for being idempotent on ``tool_call_id``.
        3. On first-time dispatch, build the initial :class:`_InFlightBatch`
           and write it durably to the state topic before publishing any
           Call — the state record is the recovery anchor if the worker
           crashes after publishing but before all returns arrive.
        4. Publish each tool ``Call`` to its tool topic with callback
           routed to ``{node_id}.fanout-returns`` (the aggregator's
           subscriber) and headers stamped with :data:`HDR_FANOUT_ID`
           (the deterministic batch id) and :data:`HDR_FRAME_ID` (the new
           per-tool-call frame id).
        """
        assert self.aggregator._state_store is not None, "setup() must have been called"
        assert self.aggregator._returns_topic is not None
        state_store = self.aggregator._state_store
        returns_topic = self.aggregator._returns_topic

        # 1. Deterministic fan_out_id from the inbound agent frame.
        fan_out_id = envelope.internal_workflow_state.current_frame.frame_id
        expected_tool_call_ids = frozenset({call.input_args[0] for call in calls if call.input_args is not None})
        key = (correlation_id, fan_out_id)

        # 2. Idempotent dispatch — handle inbound redelivery cleanly.
        if state_store.was_recently_completed(key):
            logger.info(
                "[%s] redelivered inbound for already-completed batch key=%s; skipping fan-out",
                correlation_id[:8],
                key,
            )
            return

        existing = state_store.get(key)
        if existing is not None and existing.expected_tool_call_ids == expected_tool_call_ids:
            # In-flight redelivery: keep durable state untouched. Tool Calls
            # are re-published below; tool dedup is the tool's responsibility.
            logger.info(
                "[%s] redelivered inbound for in-flight batch key=%s; preserving received=%d/%d",
                correlation_id[:8],
                key,
                len(existing.received),
                len(expected_tool_call_ids),
            )
        else:
            if existing is not None:
                # The agent loop produced a different set of tool calls on
                # re-entry. This is a real upstream bug; surface it loudly
                # and overwrite the stale state so the new dispatch can
                # proceed.
                logger.error(
                    "[%s] fan-out key=%s redispatch with different expected_tool_call_ids "
                    "(prev=%s new=%s); overwriting",
                    correlation_id[:8],
                    key,
                    sorted(existing.expected_tool_call_ids),
                    sorted(expected_tool_call_ids),
                )
            # 3. First-time dispatch — build and durably persist the initial batch.
            now_ms = int(time.time() * 1000)
            initial_batch = _InFlightBatch(
                correlation_id=correlation_id,
                fan_out_id=fan_out_id,
                expected_tool_call_ids=expected_tool_call_ids,
                base_state=calls[0].state.model_copy(deep=True),
                received={},
                started_at_ms=now_ms,
                last_updated_ms=now_ms,
                agent_topic=self.subscribe_topics[0],
            )
            await state_store.put(key, initial_batch, partition=partition)

        logger.debug(
            "[%s] fan-out dispatch persisted key=%s expected=%d node=%s",
            correlation_id[:8],
            key,
            len(expected_tool_call_ids),
            self.name,
        )

        # 4. Publish each tool Call with callback → fanout-returns.
        for call in calls:
            wf_copy = envelope.internal_workflow_state.model_copy(deep=True)
            wf_copy.invoke_frame(call, returns_topic)
            publish_envelope = Envelope(
                context=SessionRunContext(state=call.state, deps=envelope.context.deps),
                internal_workflow_state=wf_copy,
            )
            outbound_headers = {
                **self._emitter_headers(),
                HDR_FANOUT_ID: fan_out_id,
                HDR_FRAME_ID: wf_copy.current_frame.frame_id,
            }
            await broker.publish(
                publish_envelope,
                topic=wf_copy.current_frame.target_topic,
                correlation_id=correlation_id,
                key=correlation_id.encode(),
                headers=outbound_headers,
            )

    async def _aggregator_handler(
        self,
        envelope: Envelope,
        correlation_id: Annotated[str, Context()],
        headers: Annotated[dict[str, Any], Context("message.headers")],
        broker: BrokerAnnotation,
        partition: Annotated[int, Context("message.partition")] = 0,
    ) -> Response:
        """FastStream handler for tool returns arriving on ``{node_id}.fanout-returns``.

        Maintains the durable fan-out aggregator state and, when the batch
        completes, publishes the merged :class:`AggregatedReturn` back to the
        agent's main topic so the agent re-enters with the full set of tool
        results merged into its state.

        ``partition`` is the inbound message's partition, threaded through
        to the state-store write so the durable publish lands on the
        partition this worker owns (co-partitioned via
        :class:`FanOutAggregatorPartitioner`).
        """
        # Lazy setup so test paths that bypass Worker.run() still work.
        await self._ensure_aggregator_ready(broker)

        fan_out_id = decode_header_str(headers.get(HDR_FANOUT_ID))
        if fan_out_id is None:
            logger.warning(
                "[%s] fanout-returns msg without %s header; dropping",
                correlation_id[:8],
                HDR_FANOUT_ID,
            )
            return Response(envelope, headers=self._emitter_headers())

        key = (correlation_id, fan_out_id)
        state_store = self.aggregator._state_store
        # _ensure_aggregator_ready guarantees state_store is set; narrow for mypy.
        assert state_store is not None

        # 1. Reject late returns for already-completed batches.
        if state_store.was_recently_completed(key):
            logger.info(
                "[%s] late return after completion key=%s; dropping",
                correlation_id[:8],
                key,
            )
            return Response(envelope, headers=self._emitter_headers())

        # 2. Look up the in-flight batch in the cache.
        batch = state_store.get(key)
        if batch is None:
            logger.warning(
                "[%s] orphan return key=%s (no active batch in cache); dropping",
                correlation_id[:8],
                key,
            )
            return Response(envelope, headers=self._emitter_headers())

        # 3. Determine which tool_call_id is newly returned (diff vs batch.received).
        incoming_results = envelope.context.state.tool_results
        new_ids = {tcid for tcid in incoming_results if tcid in batch.expected_tool_call_ids and tcid not in batch.received}
        if not new_ids:
            logger.debug(
                "[%s] duplicate/empty return key=%s; idempotent drop",
                correlation_id[:8],
                key,
            )
            return Response(envelope, headers=self._emitter_headers())

        # 4. Build the merged batch as a new instance. Mutating the cached
        #    batch before the durable publish would let a publish failure
        #    produce a phantom-merged cache state that no record reflects —
        #    FastStream's redelivery would then see the new tcid as
        #    "already merged" via the dedup check and skip it forever.
        updated_received: dict[ToolCallId, Any] = {
            **batch.received,
            **{ToolCallId(tcid): incoming_results[tcid] for tcid in new_ids},
        }
        updated_batch = batch.with_received(updated_received, last_updated_ms=int(time.time() * 1000))

        # 5. Durable write first — state_store.put publishes then updates
        #    cache (on raise the cache stays pinned to the old batch).
        await state_store.put(key, updated_batch, partition=partition)
        batch = updated_batch

        # 6. on_partial hook.
        view = self.aggregator._batch_view(batch)
        await self.aggregator.on_partial(view, frozenset({ToolCallId(tcid) for tcid in new_ids}))

        # 7. Completion check + merge + publish aggregated result.
        if await self.aggregator.should_complete(view):
            merged: AggregatedReturn
            try:
                merged = await self.aggregator.merge(view)
            except Exception as exc:
                fallback = await self._handle_merge_error(view, key, exc)
                if fallback is None:
                    # ABORT policy: re-raise so the broker delivers again
                    # (and the late-return path eventually drops it).
                    raise AggregatorMergeError(f"merge() raised for key={key}") from exc
                merged = fallback

            # Construct an envelope mirroring a normal ReturnCall back to
            # the agent's main topic. The workflow_state already has the
            # agent's frame on top (the tool frames were unwound when each
            # tool returned), so the agent re-enters cleanly.
            publish_envelope = Envelope(
                context=SessionRunContext(state=merged.state, deps=envelope.context.deps),
                internal_workflow_state=envelope.internal_workflow_state,
            )
            outbound_headers = self._emitter_headers()
            if merged.degraded:
                outbound_headers[HDR_DEGRADED_MERGE] = "1"
            await broker.publish(
                publish_envelope,
                topic=batch.agent_topic,
                correlation_id=correlation_id,
                key=correlation_id.encode(),
                headers=outbound_headers,
            )

            # Tombstone the batch so late returns are recognised.
            await state_store.tombstone(key, partition=partition)

        return Response(envelope, headers=self._emitter_headers())

    async def _handle_merge_error(
        self,
        view: Any,
        key: tuple[str, str],
        exc: Exception,
    ) -> AggregatedReturn | None:
        """Apply the configured :class:`MergeErrorPolicy`. Returns the merged
        result on success, or ``None`` to signal the caller to re-raise."""
        policy = self.aggregator.merge_error_policy
        if policy == MergeErrorPolicy.RETRY:
            try:
                return await self.aggregator.merge(view)
            except Exception:
                logger.exception("[%s] merge() raised on retry key=%s", key[0][:8], key)
                return None
        if policy == MergeErrorPolicy.DROP:
            # Fall back to the default merge so the batch still completes,
            # but mark the result as degraded so the published envelope
            # gets HDR_DEGRADED_MERGE — operators can detect that the
            # user's custom merge silently failed.
            logger.exception(
                "[%s] merge() raised; DROP policy → default merge key=%s "
                "(downstream envelope will carry %s=1)",
                key[0][:8],
                key,
                HDR_DEGRADED_MERGE,
            )
            default = await FanOutAggregator.merge(self.aggregator, view)
            return AggregatedReturn(state=default.state, degraded=True)
        # ABORT
        logger.exception("[%s] merge() raised; ABORT policy key=%s", key[0][:8], key)
        return None

    def add_tools(self, *tools: ToolNodeDef) -> None:
        self.tools.extend(tools)

    def instructions(self, func: Callable[..., str | None]) -> Callable[..., str | None]:
        """Decorator to define dynamic instruction functions that can build instructions at runtime."""
        return self._agent_loop.instructions(func)


Agent = BaseAgentNodeDef
