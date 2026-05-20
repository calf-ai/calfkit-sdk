import dataclasses
import hashlib
import logging
import time
import warnings
from collections.abc import Callable
from typing import Annotated, Any, ClassVar, Generic, cast

from faststream import AckPolicy, Context, Response
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
from calfkit.nodes.aggregator.errors import AggregatorMergeError, AggregatorStateStoreError
from calfkit.nodes.aggregator.state import AggregatedReturn, AggregatorBatch
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

        The agent's main subscription is force-set to
        :data:`AckPolicy.NACK_ON_ERROR` (overriding
        :class:`BaseNodeDef`'s default): the dispatch path's durable
        state-store write and per-Call publish loop must rewind the
        inbound offset on raise — otherwise an inbound is
        committed-then-lost and the user-facing request hangs forever.
        Only agents have this invariant; other node kinds keep their
        defaults.

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
        base_subscriptions = list(super().kafka_subscriptions())
        # Match on the handler reference rather than equality on
        # ``topics``: the real invariant is "the agent's main inbound
        # handler must rewind on raise so a state-store write or per-Call
        # publish failure doesn't commit-then-lose the inbound." A subclass
        # that overrides ``kafka_subscriptions()`` to mutate the topic list
        # but keeps ``self.handler`` would silently lose NACK_ON_ERROR
        # under the previous ``sub.topics == list(self.subscribe_topics)``
        # check. The handler reference is the durable identity.
        #
        # Bound methods in CPython are recreated on every attribute access
        # (``self.handler is self.handler`` is False), so we must compare
        # via ``==`` — which on bound methods is defined as
        # ``__self__ is __self__ and __func__ is __func__``. That is
        # semantic identity (same instance, same function), not value
        # equality in any deeper sense.
        subscriptions = [
            dataclasses.replace(sub, ack_policy=AckPolicy.NACK_ON_ERROR) if sub.handler == self.handler else sub for sub in base_subscriptions
        ]
        # The rebalance listener is set on the aggregator's runtime by
        # setup(); it's None in test paths that haven't run setup yet
        # (TestKafkaBroker), and the subscription's listener field
        # tolerates None.
        runtime = self.aggregator._runtime
        listener = runtime.rebalance_listener if runtime is not None else None
        subscriptions.append(
            _KafkaSubscription(
                topics=[self.fanout_returns_topic],
                handler=self._aggregator_handler,
                listener=listener,
                # Force serial processing: a single asyncio loop processes
                # returns for this agent's partitions, so read-modify-write
                # on the state store is linearizable.
                max_workers=1,
                # FastStream's default ACK_FIRST commits the offset BEFORE
                # the handler runs, which would silently swallow handler
                # raises (publish failures, transient state-store errors,
                # ABORT merge raises). NACK_ON_ERROR rewinds the consumer
                # offset on exception so the message is redelivered within
                # the same consumer session.
                ack_policy=AckPolicy.NACK_ON_ERROR,
            )
        )
        return subscriptions

    def _ensure_aggregator_ready(self) -> None:
        """Assert that :meth:`FanOutAggregator.setup` has run.

        Called from the two hot paths that depend on the aggregator's
        runtime (the parallel-fan-out dispatcher and the fanout-returns
        handler). Pure validation — no test-fixture synthesis in
        production code.

        Worker.run() handles setup automatically at startup. Tests that
        bypass Worker.run() must set up explicitly via
        :func:`calfkit.nodes.aggregator.testing.setup_for_tests`.
        """
        if self.aggregator._runtime is None:
            raise AggregatorStateStoreError(
                f"FanOutAggregator.setup() has not been called for agent "
                f"{self.node_id!r}. Worker.run() handles this in "
                f"production; tests bypassing Worker.run() must call "
                f"calfkit.nodes.aggregator.testing.setup_for_tests("
                f"agent, broker)."
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
                # Parallel fan-out: emit list[Call]. The agent's
                # ``_publish_action`` override intercepts this branch and
                # delegates to ``_publish_parallel_with_aggregator``, which
                # (a) writes the initial FanOutState record to the
                # compacted state topic and (b) routes each Call's callback
                # to ``{node_id}.fanout-returns`` so the aggregator
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
            self._ensure_aggregator_ready()
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
        runtime = self.aggregator.runtime
        state_store = runtime.state_store
        returns_topic = runtime.returns_topic

        fan_out_id = envelope.internal_workflow_state.current_frame.frame_id
        expected_tool_call_ids = frozenset({call.input_args[0] for call in calls if call.input_args is not None})
        key = (correlation_id, fan_out_id)

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
            drifted = existing is not None
            if drifted:
                # The agent loop produced a different set of tool calls on
                # re-entry. Surface loudly and overwrite the stale state.
                # The fresh batch is born degraded so the eventual completion
                # publish carries HDR_DEGRADED_MERGE — downstream observability
                # gets a quantifiable signal for the silent data loss. Storing
                # the flag ON the durable batch (not in a process-local set)
                # is what lets it survive worker restart and NACK redelivery.
                logger.error(
                    "[%s] fan-out key=%s redispatch with different expected_tool_call_ids "
                    "(prev=%s new=%s); overwriting (discarded_received_count=%d)",
                    correlation_id[:8],
                    key,
                    sorted(existing.expected_tool_call_ids),  # type: ignore[union-attr]
                    sorted(expected_tool_call_ids),
                    len(existing.received),  # type: ignore[union-attr]
                )
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
                degraded=drifted,
            )
            await state_store.put(key, initial_batch, partition=partition)

        logger.debug(
            "[%s] fan-out dispatch persisted key=%s expected=%d node=%s",
            correlation_id[:8],
            key,
            len(expected_tool_call_ids),
            self.name,
        )

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
        # Validate setup ran; Worker.run() handles this in production
        # and tests must call setup_for_tests() explicitly.
        self._ensure_aggregator_ready()

        fan_out_id = decode_header_str(headers.get(HDR_FANOUT_ID))
        # Reject both ``None`` (header absent) and the empty string (header
        # present but blank). ``decode_header_str(b"")`` returns ``""``, and
        # ``""`` would have slipped past a bare ``is None`` check and become
        # a state-store key tuple second element of ``""`` — silently
        # producing a key collision class for every empty-id return.
        if not fan_out_id:
            # A fanout-returns message without HDR_FANOUT_ID can't be tied
            # to any in-flight batch. The previous WARN-and-ack swallowed
            # the protocol violation; under NACK_ON_ERROR a raise rewinds
            # the offset so the operator sees the corruption (a stuck
            # partition) instead of silent data loss. Include the inbound
            # frame_id in the context so an operator can correlate against
            # the producer's logs.
            inbound_frame_id = envelope.internal_workflow_state.current_frame.frame_id
            raise AggregatorStateStoreError(
                f"fanout-returns message missing or empty {HDR_FANOUT_ID} header "
                f"(correlation_id={correlation_id}, inbound_frame_id={inbound_frame_id}); "
                f"refusing to silently drop — the producer or upstream forwarder is "
                f"violating the fan-out protocol",
                state_topic=self.aggregator.runtime.state_topic,
            )

        # Late returns / orphan returns / duplicate returns all drop silently;
        # only new tool_call_ids advance the batch. The cache write is
        # deferred until the durable publish acks so a publish failure can be
        # redelivered (ack_policy=NACK_ON_ERROR on the subscription rewinds
        # the offset on raise) without a phantom-merged state.
        key = (correlation_id, fan_out_id)
        state_store = self.aggregator.runtime.state_store

        if state_store.was_recently_completed(key):
            logger.info(
                "[%s] late return after completion key=%s; dropping",
                correlation_id[:8],
                key,
            )
            return Response(envelope, headers=self._emitter_headers())

        batch = state_store.get(key)
        if batch is None:
            # An empty cache slot for a fanout-returns message has two
            # legitimate causes:
            #   (a) partition rebalance race — this worker received a
            #       message for a partition it doesn't yet own (or has
            #       just lost). The new owner has the durable state;
            #       acking here is correct, otherwise we'd loop on a
            #       message we can't act on.
            #   (b) partition owned but cache empty — the state record
            #       genuinely doesn't exist (lost write, manual deletion,
            #       protocol bug). Silently acking here was the previous
            #       behaviour and meant a real durability violation
            #       disappeared into a WARN log. Raise instead so
            #       NACK_ON_ERROR redelivers and the operator sees a
            #       stuck partition rather than a vanished batch.
            owned = state_store.owned_partitions
            if owned and partition not in owned:
                logger.info(
                    "[%s] orphan return key=%s on partition=%d not in owned_partitions=%s; rebalance race, dropping",
                    correlation_id[:8],
                    key,
                    partition,
                    sorted(owned),
                )
                return Response(envelope, headers=self._emitter_headers())
            raise AggregatorStateStoreError(
                f"orphan return: no in-flight batch for key={key} on owned "
                f"partition={partition} (correlation_id={correlation_id}, "
                f"fan_out_id={fan_out_id}). The state record is missing; the durable "
                f"log may have been truncated, the producer may not have written it, "
                f"or partition co-partitioning is broken.",
                state_topic=self.aggregator.runtime.state_topic,
            )

        incoming_results = envelope.context.state.tool_results
        new_ids = {tcid for tcid in incoming_results if tcid in batch.expected_tool_call_ids and tcid not in batch.received}

        # One deep copy of base_state amortised across every override call
        # in this handler invocation (up to two of {on_partial,
        # should_complete} plus merge). The merge contract documented on
        # ``FanOutAggregator.merge`` states observers don't mutate and
        # merge mutates last, which is what makes sharing the copy safe.
        # Without this, a worst-case handler invocation deep-copied the
        # full agent State three separate times (one per ``_batch_view``
        # call site) — measurable overhead for any State carrying real
        # message_history.
        shared_base_state = batch.base_state.model_copy(deep=True)

        if new_ids:
            updated_received: dict[str, Any] = {
                **batch.received,
                **{tcid: incoming_results[tcid] for tcid in new_ids},
            }
            updated_batch = batch.with_received(updated_received, last_updated_ms=int(time.time() * 1000))
            await state_store.put(key, updated_batch, partition=partition)
            batch = updated_batch
            # The shared copy was taken from the pre-update batch's
            # base_state, but ``with_received`` carries base_state through
            # by reference so the copy is still a faithful snapshot of the
            # post-update batch's base_state.
            view = self.aggregator._batch_view(batch, base_state_copy=shared_base_state)
            await self.aggregator.on_partial(view, frozenset(new_ids))
            is_complete = await self.aggregator.should_complete(view)
        else:
            # No new tcids — but the batch may already be complete and
            # waiting on a redelivered completion. That happens when the
            # previous attempt's agent-topic publish or tombstone failed
            # after state_store.put succeeded: the durable record has all
            # results, but the merge hasn't been delivered downstream.
            # Fall through to re-attempt the completion path so the
            # aggregated return reaches the agent at-least-once. If the
            # batch isn't complete yet, drop as a normal duplicate.
            view = self.aggregator._batch_view(batch, base_state_copy=shared_base_state)
            is_complete = await self.aggregator.should_complete(view)
            if not is_complete:
                logger.debug(
                    "[%s] duplicate/empty return key=%s; idempotent drop",
                    correlation_id[:8],
                    key,
                )
                return Response(envelope, headers=self._emitter_headers())
            logger.info(
                "[%s] re-attempting completion for already-merged batch key=%s (redelivery after a failed downstream publish or tombstone)",
                correlation_id[:8],
                key,
            )

        if is_complete:
            merged: AggregatedReturn
            try:
                merged = await self.aggregator.merge(view)
            except Exception as exc:
                # Pass the durable ``batch`` (frozen ``_InFlightBatch``) to
                # the error handler so the FALLBACK_TO_DEFAULT path can
                # rebuild a fresh view from the immutable ``base_state``.
                # The current ``view`` may have been partially mutated by
                # the failed user merge before it raised; without an
                # untainted source the fallback would re-merge over a
                # polluted base.
                fallback = await self._handle_merge_error(view, key, exc, batch)
                if fallback is None:
                    # ABORT policy: re-raise so FastStream's
                    # ack_policy=NACK_ON_ERROR rewinds the consumer offset
                    # and the message is redelivered. Each redelivery
                    # re-attempts the merge until it succeeds (operator
                    # fix) or the consumer is paused.
                    raise AggregatorMergeError(
                        f"merge() raised for key={key}",
                        correlation_id=key[0],
                        fan_out_id=key[1],
                        state_topic=self.aggregator.runtime.state_topic,
                    ) from exc
                merged = fallback

            # The agent's frame is on top of the call stack (tool frames
            # were unwound when each tool returned). Re-stamp its identity
            # with a derived frame_id that satisfies two distinct
            # invariants:
            #
            # (a) **Distinctness from the previous frame.** A subsequent
            #     parallel fan-out within the same invocation derives its
            #     ``fan_out_id`` from the inbound's frame_id; if we reused
            #     the previous frame_id the new dispatch would collide
            #     with the just-tombstoned key, hit
            #     ``was_recently_completed`` and be silently skipped
            #     (agent hangs forever).
            #
            # (b) **Determinism across handler retries.** NACK_ON_ERROR
            #     redelivery means the same merged batch may re-enter the
            #     completion publish path multiple times (e.g., a
            #     transient broker failure between the agent-topic
            #     publish and the tombstone). Each redelivery must produce
            #     the SAME re-entry frame_id so downstream consumers
            #     dedup correctly — a fresh ``uuid7()`` would emit a
            #     distinct re-entry envelope on every retry and any
            #     downstream stateful sink would treat them as distinct
            #     work items.
            #
            # SHA-256 of ``"re-entry:" + previous_frame_id`` satisfies
            # both: a hash differs structurally from the input (no
            # collision in case (a)) and is pure-functional on the
            # input (case (b)). The "re-entry:" prefix prevents accidental
            # collision against any other frame_id derivation that might
            # hash a frame_id directly.
            re_entry_state = envelope.internal_workflow_state.model_copy(deep=True)
            previous_frame = re_entry_state.unwind_frame()
            new_frame_id = hashlib.sha256(f"re-entry:{previous_frame.frame_id}".encode()).hexdigest()[:32]
            new_frame = dataclasses.replace(previous_frame, frame_id=new_frame_id)
            re_entry_state.call_stack.push(new_frame)
            # Structured log for chain reconstruction: operators can grep
            # by parent_frame to trace the full agent-invocation lineage
            # across re-entries.
            logger.info(
                "[%s] agent re-entry frame=%s parent_frame=%s",
                correlation_id[:8],
                new_frame_id,
                previous_frame.frame_id,
            )

            publish_envelope = Envelope(
                context=SessionRunContext(state=merged.state, deps=envelope.context.deps),
                internal_workflow_state=re_entry_state,
            )
            outbound_headers = {
                **self._emitter_headers(),
                HDR_FRAME_ID: new_frame.frame_id,
            }
            # Stamp HDR_DEGRADED_MERGE if either the merge itself produced a
            # degraded result (FALLBACK_TO_DEFAULT path) OR the dispatch path
            # flagged the batch as degraded (drifted-redispatch overwrite that
            # discarded prior received results). The dispatch-time flag lives
            # ON the durable batch record so it survives worker restart.
            if merged.degraded or batch.degraded:
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
        view: AggregatorBatch,
        key: tuple[str, str],
        exc: Exception,
        batch: _InFlightBatch,
    ) -> AggregatedReturn | None:
        """Apply the configured :class:`MergeErrorPolicy`. Returns the merged
        result on success, or ``None`` to signal the caller to re-raise.

        ``batch`` is the durable, frozen :class:`_InFlightBatch` from the
        state-store cache — the source of truth for the un-mutated
        ``base_state``. The FALLBACK_TO_DEFAULT path uses it to rebuild a
        clean view because the failed user merge may have partially mutated
        the passed-in ``view.base_state`` before raising.
        """
        policy = self.aggregator.merge_error_policy
        state_topic = self.aggregator.runtime.state_topic
        if policy == MergeErrorPolicy.RETRY:
            try:
                retried = await self.aggregator.merge(view)
            except Exception:
                # The caller's `raise AggregatorMergeError(...) from exc` chains
                # the FIRST exception, not the retry exception. Log the retry
                # traceback here so the retry's cause isn't invisible.
                logger.exception(
                    "[%s] merge() raised on retry key=%s state_topic=%s",
                    key[0][:8],
                    key,
                    state_topic,
                )
                return None
            # Retry success is the only signal operators get that the
            # underlying merge needed a second attempt; without this WARN
            # a flapping downstream (LLM/DB) is invisible until it crosses
            # the threshold into permanent failure. WARN (not INFO) so
            # standard log shipping surfaces it to on-call.
            logger.warning(
                "[%s] merge() succeeded on retry key=%s state_topic=%s",
                key[0][:8],
                key,
                state_topic,
            )
            return retried
        if policy == MergeErrorPolicy.FALLBACK_TO_DEFAULT:
            # Fall back to the default merge so the batch still completes,
            # but mark the result as degraded so the published envelope
            # gets HDR_DEGRADED_MERGE — operators can detect that the
            # user's custom merge silently failed. The exception is
            # logged here because FALLBACK_TO_DEFAULT doesn't re-raise;
            # this is the only operator-visible signal.
            logger.exception(
                "[%s] merge() raised; FALLBACK_TO_DEFAULT policy -> attempting default merge key=%s state_topic=%s",
                key[0][:8],
                key,
                state_topic,
            )
            # Rebuild a clean view from the durable ``batch.base_state``
            # before invoking the default merge. The user's merge MAY have
            # mutated ``view.base_state`` (e.g., appended to
            # ``message_history``) before raising; the default merge does
            # ``state = batch.base_state.model_copy(deep=True)`` which
            # would otherwise faithfully clone the polluted state and
            # silently emit a degraded result that secretly carries the
            # user's partial mutation. Using ``_InFlightBatch.base_state``
            # (frozen, untouched by user code) is the only way to guarantee
            # the fallback sees pristine input.
            clean_base_state = batch.base_state.model_copy(deep=True)
            fresh_view = self.aggregator._batch_view(batch, base_state_copy=clean_base_state)
            # The default merge can itself raise (e.g., a broken
            # ``State.add_tool_result`` or a malformed received result).
            # Without this guard the inner exception bypasses the policy
            # entirely: the user opted into FALLBACK_TO_DEFAULT expecting
            # "log and continue," but would get a raw exception with no
            # AggregatorMergeError wrapping and no degraded-merge stamp.
            # Treat a double-failure as ABORT so the operator sees a
            # structured AggregatorMergeError via NACK redelivery.
            try:
                default = await FanOutAggregator.merge(self.aggregator, fresh_view)
            except Exception:
                logger.exception(
                    "[%s] FALLBACK_TO_DEFAULT: default merge ALSO raised key=%s state_topic=%s; treating as ABORT",
                    key[0][:8],
                    key,
                    state_topic,
                )
                return None
            return AggregatedReturn(state=default.state, degraded=True)
        # ABORT: the caller's `raise AggregatorMergeError(...) from exc`
        # propagates the exception with the chained traceback; FastStream
        # will log it once at the framework boundary. Logging here would
        # double-log the same stack.
        return None

    def add_tools(self, *tools: ToolNodeDef) -> None:
        self.tools.extend(tools)

    def instructions(self, func: Callable[..., str | None]) -> Callable[..., str | None]:
        """Decorator to define dynamic instruction functions that can build instructions at runtime."""
        return self._agent_loop.instructions(func)


Agent = BaseAgentNodeDef
