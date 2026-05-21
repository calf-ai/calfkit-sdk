"""Regression tests for issue #141: tool returns must not leak to co-tenant nodes.

Two ``Agent`` nodes that share a Kafka subscribe topic (the "ambient channel"
multi-agent pattern) must each receive only their *own* tool return envelope
and only their *own* ``TailCall`` self-retry. Before the fix,
``BaseNodeDef._publish_action`` wrote ``self.subscribe_topics[0]`` into the
call frame as the tool-return rendezvous, and ``BaseAgentNodeDef.run`` used
the same idiom for ``TailCall`` self-retry targets — both fanned out to every
co-tenant, contaminating peers' state and producing duplicate output.

Test layout mirrors the project's established patterns:
- TestKafkaBroker for in-memory routing (per ``tests/test_consumer.py``,
  ``tests/test_headers.py``).
- ``@broker.subscriber`` observers for envelope-level inspection (per
  ``tests/test_headers.py::test_gate_reject_auto_publish_carries_emitter_header``).
- ``consumer()`` sinks with ``final_output_parts`` gates for counting final
  hops (per ``tests/test_consumer.py``).
- ``wait_for_condition`` from ``tests/utils.py`` for deterministic settling.
"""

from typing import Annotated, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from faststream import Context
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai import DeferredToolRequests
from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    ToolCallPart,
    ToolReturnPart,
)
from calfkit._vendor.pydantic_ai.messages import TextPart as ModelTextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client, NodeResult
from calfkit.models.actions import TailCall
from calfkit.models.envelope import Envelope
from calfkit.models.session_context import (
    CallFrame,
    CallFrameStack,
    Deps,
    SessionRunContext,
    WorkflowState,
)
from calfkit.models.state import State
from calfkit.nodes import Agent, ConsumerNodeDef, ToolNodeDef, agent_tool, consumer
from calfkit.worker import Worker
from tests.providers import get_users_name, prepare_worker
from tests.utils import wait_for_condition

SHARED_INPUT = "co_tenant_chan.in"


# ---------------------------------------------------------------------------
# FunctionModel stubs
# ---------------------------------------------------------------------------


def _call_one_tool_then_finalize() -> FunctionModel:
    """Call the first available tool on turn 1; emit final text after the result."""

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        if isinstance(last, ModelRequest) and any(isinstance(p, ToolReturnPart) for p in last.parts):
            return ModelResponse(parts=[ModelTextPart("ok")])
        if info.function_tools:
            return ModelResponse(parts=[ToolCallPart(info.function_tools[0].name)])
        return ModelResponse(parts=[ModelTextPart("no tools available")])

    return FunctionModel(_fn)


def _call_all_tools_then_finalize(tool_names: list[str]) -> FunctionModel:
    """Parallel fan-out stub: call every tool in *tool_names* in one round, then
    finalize. Mirrors ``tests/test_headers.py::_function_model_calls_all``."""

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        if isinstance(last, ModelRequest) and any(isinstance(p, ToolReturnPart) for p in last.parts):
            return ModelResponse(parts=[ModelTextPart("done")])
        return ModelResponse(parts=[ToolCallPart(name) for name in tool_names])

    return FunctionModel(_fn)


# ---------------------------------------------------------------------------
# End-to-end leak test (tool Call round-trip)
# ---------------------------------------------------------------------------


async def test_tool_return_does_not_leak_between_co_tenant_agents(container):
    """Two agents share ``co_tenant_chan.in`` + one tool. After one publish,
    each agent must emit exactly one final ReturnCall — without the fix, each
    agent also processes the *peer's* tool-return envelope and emits a duplicate.
    """
    model = _call_one_tool_then_finalize()
    shared_tool = agent_tool(get_users_name)

    worker = container.get(Worker)
    alpha = Agent(
        "alpha_agent",
        system_prompt="x",
        subscribe_topics=SHARED_INPUT,
        publish_topic="alpha_agent.out",
        model_client=model,
        tools=[shared_tool],
    )
    bravo = Agent(
        "bravo_agent",
        system_prompt="x",
        subscribe_topics=SHARED_INPUT,
        publish_topic="bravo_agent.out",
        model_client=model,
        tools=[shared_tool],
    )

    alpha_finals: list[NodeResult] = []
    bravo_finals: list[NodeResult] = []

    @consumer(
        subscribe_topics="alpha_agent.out",
        gates=[lambda ctx: bool(ctx.state.final_output_parts)],
        node_id="alpha_final_sink",
    )
    def alpha_sink(result: NodeResult) -> None:
        alpha_finals.append(result)

    @consumer(
        subscribe_topics="bravo_agent.out",
        gates=[lambda ctx: bool(ctx.state.final_output_parts)],
        node_id="bravo_final_sink",
    )
    def bravo_sink(result: NodeResult) -> None:
        bravo_finals.append(result)

    # ``shared_tool`` is added to the worker once even though both agents
    # reference it in their ``tools=`` lists — agents' tool registries are
    # built locally from the node objects, and ``Worker.add_nodes`` does not
    # dedup (a second add would register a redundant subscriber under the
    # same ``group_id``).
    worker.add_nodes(alpha, bravo, shared_tool, alpha_sink, bravo_sink)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        await client.invoke_node("hi", SHARED_INPUT)
        await wait_for_condition(
            lambda: len(alpha_finals) >= 1 and len(bravo_finals) >= 1,
            timeout=5.0,
        )
    # TestKafkaBroker dispatches publishes synchronously through its
    # in-memory ``FakeProducer``, so any duplicate final the buggy code would
    # have produced has already landed in ``alpha_finals``/``bravo_finals``
    # by the time ``wait_for_condition`` resolves — no explicit drain needed.

    assert len(alpha_finals) == 1, f"alpha emitted {len(alpha_finals)} finals (expected 1); peer-tool-return leak"
    assert len(bravo_finals) == 1, f"bravo emitted {len(bravo_finals)} finals (expected 1); peer-tool-return leak"


async def test_parallel_fanout_does_not_leak_merged_completion_between_co_tenant_agents(container):
    """Two agents share ``co_tenant_chan.in`` and each runs a parallel fan-out
    over its own tool set. After one publish, each agent's aggregator merges
    its own tool returns and re-enters that agent exactly once — yielding
    exactly one final per agent.

    Without ``agent.py:466`` setting ``agent_topic=self._return_topic``, the
    aggregator's merged-completion publish would target the shared
    ``subscribe_topics[0]`` and both co-tenant agents would receive (and
    re-enter on) each other's completions. This regression test pins the
    aggregator-path co-tenant isolation invariant.

    Sibling to :func:`test_tool_return_does_not_leak_between_co_tenant_agents`
    (which exercises the non-aggregator single-tool path via
    ``BaseNodeDef._publish_action``); this test exercises the durable
    aggregator path via ``BaseAgentNodeDef._publish_parallel_with_aggregator``
    + ``_aggregator_handler``'s merged-completion publish.
    """
    from calfkit.nodes.aggregator.testing import setup_for_tests

    # Each agent gets its own tool set — parallel fan-out targets must be
    # distinct so the aggregator's per-agent batch keys (correlation_id,
    # fan_out_id) don't entangle. The leak this test pins is the *merged
    # completion* publish target (``agent_topic``), not the per-call dispatch.
    @agent_tool
    def alpha_fanout_a() -> str:
        return "alpha-a"

    @agent_tool
    def alpha_fanout_b() -> str:
        return "alpha-b"

    @agent_tool
    def bravo_fanout_a() -> str:
        return "bravo-a"

    @agent_tool
    def bravo_fanout_b() -> str:
        return "bravo-b"

    worker = container.get(Worker)
    alpha = Agent(
        "alpha_fanout_agent",
        system_prompt="x",
        subscribe_topics=SHARED_INPUT,
        publish_topic="alpha_fanout_agent.out",
        model_client=_call_all_tools_then_finalize(["alpha_fanout_a", "alpha_fanout_b"]),
        tools=[alpha_fanout_a, alpha_fanout_b],
    )
    bravo = Agent(
        "bravo_fanout_agent",
        system_prompt="x",
        subscribe_topics=SHARED_INPUT,
        publish_topic="bravo_fanout_agent.out",
        model_client=_call_all_tools_then_finalize(["bravo_fanout_a", "bravo_fanout_b"]),
        tools=[bravo_fanout_a, bravo_fanout_b],
    )

    alpha_finals: list[NodeResult] = []
    bravo_finals: list[NodeResult] = []

    @consumer(
        subscribe_topics="alpha_fanout_agent.out",
        gates=[lambda ctx: bool(ctx.state.final_output_parts)],
        node_id="alpha_fanout_final_sink",
    )
    def alpha_sink(result: NodeResult) -> None:
        alpha_finals.append(result)

    @consumer(
        subscribe_topics="bravo_fanout_agent.out",
        gates=[lambda ctx: bool(ctx.state.final_output_parts)],
        node_id="bravo_fanout_final_sink",
    )
    def bravo_sink(result: NodeResult) -> None:
        bravo_finals.append(result)

    worker.add_nodes(
        alpha,
        bravo,
        alpha_fanout_a,
        alpha_fanout_b,
        bravo_fanout_a,
        bravo_fanout_b,
        alpha_sink,
        bravo_sink,
    )
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        # Both agents go through the durable aggregator for parallel fan-out;
        # setup_for_tests wires the in-test broker into each aggregator so
        # _publish_parallel_with_aggregator + _aggregator_handler can run.
        await setup_for_tests(alpha, broker)
        await setup_for_tests(bravo, broker)
        await client.invoke_node("hi", SHARED_INPUT)
        await wait_for_condition(
            lambda: len(alpha_finals) >= 1 and len(bravo_finals) >= 1,
            timeout=5.0,
        )
    # TestKafkaBroker dispatches publishes synchronously through its
    # in-memory ``FakeProducer``, so any duplicate final the buggy code
    # would have produced (peer's merged completion re-entering on the
    # shared subscribe topic) has already landed by the time
    # ``wait_for_condition`` resolves — no explicit drain needed.

    assert len(alpha_finals) == 1, (
        f"alpha emitted {len(alpha_finals)} finals (expected 1); aggregator merged-completion leak via shared subscribe_topics[0]"
    )
    assert len(bravo_finals) == 1, (
        f"bravo emitted {len(bravo_finals)} finals (expected 1); aggregator merged-completion leak via shared subscribe_topics[0]"
    )


# ---------------------------------------------------------------------------
# Contract tests: each routing action writes the right callback / target
# ---------------------------------------------------------------------------


async def test_single_call_writes_private_return_topic_as_callback(container):
    """A single tool ``Call`` envelope arriving at the tool's input topic must
    carry ``frame.callback_topic == agent._return_topic``. Pins the routing
    decision so a future refactor cannot silently swap it back to a public
    subscribe topic.
    """
    captured: list[tuple[str, str]] = []  # (target, callback)

    @agent_tool
    def probe_tool() -> str:
        return "ok"

    worker = container.get(Worker)
    agent = Agent(
        "single_call_callback_agent",
        system_prompt="x",
        subscribe_topics="single_call_callback.input",
        model_client=_call_one_tool_then_finalize(),
        tools=[probe_tool],
    )
    worker.add_nodes(agent, probe_tool)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    @broker.subscriber(probe_tool.subscribe_topics[0], group_id="single_call_callback_observer")
    async def _observe(envelope: Envelope, headers: Annotated[dict[str, Any], Context("message.headers")]) -> None:
        frame = envelope.internal_workflow_state.current_frame
        captured.append((frame.target_topic, frame.callback_topic))

    prepare_worker(container)

    async with TestKafkaBroker(broker):
        await client.invoke_node("hi", agent.subscribe_topics[0])
        await wait_for_condition(lambda: len(captured) >= 1, timeout=5.0)

    assert captured, "tool input topic never received the Call envelope"
    target, callback = captured[0]
    assert target == probe_tool.subscribe_topics[0]
    assert callback == agent._return_topic, (
        f"single Call wrote callback_topic={callback!r}; expected node-private _return_topic={agent._return_topic!r} (issue #141)"
    )
    assert callback != agent.subscribe_topics[0], "callback_topic must not be the public subscribe topic — would leak to co-tenants"


async def test_parallel_call_writes_private_return_topic_as_callback(container):
    """Every cloned ``Call`` in a parallel fan-out must carry a per-node-unique
    ``frame.callback_topic`` (NOT the shared ``subscribe_topics[0]``) so
    co-tenant agents don't leak each other's tool returns.

    For ``Agent`` instances, parallel fan-out goes through the durable
    aggregator (``BaseAgentNodeDef._publish_parallel_with_aggregator``), which
    sets the callback to ``agent.fanout_returns_topic`` (``{node_id}.fanout-returns``).
    This is per-node-unique by aggregator design and satisfies the same
    co-tenant-isolation invariant as ``_return_topic``: only this agent
    instance's ``FanOutAggregator`` is subscribed to ``fanout-returns``.

    Non-Agent ``BaseNodeDef`` subclasses with parallel fan-out continue to use
    ``_return_topic`` via ``BaseNodeDef._publish_action``'s parallel branch
    (PR #142); that path is exercised by the single-Call contract test above
    (which also goes through ``_publish_action`` but with a singular ``Call``).
    """
    from calfkit.nodes.aggregator.testing import setup_for_tests

    captured_a: list[str] = []
    captured_b: list[str] = []
    captured_c: list[str] = []

    @agent_tool
    def fanout_tool_a() -> str:
        return "a"

    @agent_tool
    def fanout_tool_b() -> str:
        return "b"

    @agent_tool
    def fanout_tool_c() -> str:
        return "c"

    worker = container.get(Worker)
    agent = Agent(
        "parallel_callback_agent",
        system_prompt="x",
        subscribe_topics="parallel_callback.input",
        model_client=_call_all_tools_then_finalize(["fanout_tool_a", "fanout_tool_b", "fanout_tool_c"]),
        tools=[fanout_tool_a, fanout_tool_b, fanout_tool_c],
    )
    worker.add_nodes(agent, fanout_tool_a, fanout_tool_b, fanout_tool_c)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    @broker.subscriber(fanout_tool_a.subscribe_topics[0], group_id="parallel_callback_obs_a")
    async def _obs_a(envelope: Envelope) -> None:
        captured_a.append(envelope.internal_workflow_state.current_frame.callback_topic)

    @broker.subscriber(fanout_tool_b.subscribe_topics[0], group_id="parallel_callback_obs_b")
    async def _obs_b(envelope: Envelope) -> None:
        captured_b.append(envelope.internal_workflow_state.current_frame.callback_topic)

    @broker.subscriber(fanout_tool_c.subscribe_topics[0], group_id="parallel_callback_obs_c")
    async def _obs_c(envelope: Envelope) -> None:
        captured_c.append(envelope.internal_workflow_state.current_frame.callback_topic)

    prepare_worker(container)

    async with TestKafkaBroker(broker):
        # Parallel fan-out on an Agent goes through the durable aggregator;
        # setup_for_tests wires the in-test broker into aggregator.setup so
        # _publish_parallel_with_aggregator can dispatch.
        await setup_for_tests(agent, broker)
        await client.invoke_node("hi", agent.subscribe_topics[0])
        await wait_for_condition(
            lambda: len(captured_a) >= 1 and len(captured_b) >= 1 and len(captured_c) >= 1,
            timeout=5.0,
        )

    all_callbacks = captured_a + captured_b + captured_c
    assert all_callbacks, "no fan-out Calls captured at tool input topics"
    for callback in all_callbacks:
        assert callback == agent.fanout_returns_topic, (
            f"parallel fan-out Call wrote callback_topic={callback!r}; "
            f"expected per-node aggregator returns topic={agent.fanout_returns_topic!r} "
            f"(issue #141: Agents route parallel fan-out through the aggregator, "
            f"which uses {{node_id}}.fanout-returns — per-node-unique like _return_topic, "
            f"different name)"
        )
        assert callback != agent.subscribe_topics[0], "callback_topic must not be the public subscribe topic — would leak to co-tenants"


async def test_tailcall_self_retry_targets_private_return_topic():
    """``BaseAgentNodeDef.run`` returns a ``TailCall`` targeting the node's
    private ``_return_topic`` when all tool calls in the model output are
    unreachable (the ``all_call_ids_complete`` retry branch). Pins that
    constant so a refactor cannot silently swap it back to
    ``self.subscribe_topics[0]``.

    The internal ``_agent_loop`` is mocked to fabricate a
    ``DeferredToolRequests`` whose tool name is not in the agent's
    ``tools_registry`` — the natural trigger for the all-invalid branch.
    pydantic_ai's loop only emits ``DeferredToolRequests`` for tools in the
    toolset, so without mocking we can't construct the registry/toolset
    mismatch this branch defends against.
    """
    agent = Agent(
        "tailcall_target_agent",
        system_prompt="x",
        subscribe_topics="tailcall_target.input",
        model_client=_call_one_tool_then_finalize(),  # never called — _agent_loop is mocked
    )

    fabricated_call = ToolCallPart("unreachable_tool", tool_call_id="c1")
    fabricated_output = DeferredToolRequests(calls=[fabricated_call])
    # ``spec_set`` locks the fabricated result to the exact attributes
    # ``BaseAgentNodeDef.run`` reads today. If a future refactor reaches for
    # ``result.usage``, ``result.all_messages``, etc., the bare MagicMock
    # would silently return a child mock and this test would keep "passing"
    # while the contract drifts — ``spec_set`` raises ``AttributeError``
    # instead, forcing the test to be updated in lockstep.
    fabricated_result = MagicMock(spec_set=["output", "new_messages"])
    fabricated_result.output = fabricated_output
    fabricated_result.new_messages.return_value = [ModelResponse(parts=[fabricated_call])]
    agent._agent_loop.run = AsyncMock(return_value=fabricated_result)

    state = State()
    state.stage_message(ModelRequest.user_text_prompt("hi"))
    ctx = SessionRunContext(state=state, deps=Deps(correlation_id="cid", provided_deps={}))

    result = await agent.run(ctx)

    assert isinstance(result, TailCall), f"expected TailCall (all-invalid retry); got {type(result).__name__}"
    assert result.target_topic == agent._return_topic, (
        f"TailCall.target_topic={result.target_topic!r}; "
        f"expected node-private _return_topic={agent._return_topic!r} "
        f"(issue #141, BaseAgentNodeDef.run all-invalid retry)"
    )
    assert result.target_topic != agent.subscribe_topics[0], (
        "TailCall target must not be the public subscribe topic — would leak self-retries to co-tenants"
    )

    # Also exercise the downstream ``_publish_action`` TailCall branch end-
    # to-end. The agent.run pin above proves ``target_topic`` is set
    # correctly; this proves the broker is actually invoked with that topic.
    # A regression that swapped the publish target inside _publish_action
    # would otherwise be caught only by the higher-cost E2E leak test.
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic="tailcall_target.input", callback_topic="client.reply"))
    envelope = Envelope(
        context=SessionRunContext(state=result.state, deps=Deps(correlation_id="cid", provided_deps={})),
        internal_workflow_state=WorkflowState(call_stack=stack),
    )
    broker = MagicMock()
    broker.publish = AsyncMock()

    await agent._publish_action(result, envelope, "cid", broker)

    assert broker.publish.call_count == 1, f"expected one TailCall publish; got {broker.publish.call_count}"
    published_topic = broker.publish.call_args.kwargs["topic"]
    assert published_topic == agent._return_topic, (
        f"_publish_action published TailCall to topic={published_topic!r}; expected node-private _return_topic={agent._return_topic!r}"
    )
    # Callback inheritance: the new frame must carry the popped frame's
    # callback so the eventual final ReturnCall still finds the client.
    current_frame = envelope.internal_workflow_state.current_frame
    assert current_frame.target_topic == agent._return_topic
    assert current_frame.callback_topic == "client.reply", f"TailCall did not inherit parent callback_topic; got {current_frame.callback_topic!r}"


# ---------------------------------------------------------------------------
# Worker.register_handlers: dedup of _return_topic in subscribe_topics
# ---------------------------------------------------------------------------


def test_worker_register_handlers_dedupes_explicit_return_topic(container):
    """A user who manually subscribes to ``f'{node_id}.private.return'`` must
    register with exactly one entry per unique topic — the framework should
    not register the private return topic twice just because the user already
    listed it.

    Spies on the underlying ``KafkaBroker.subscriber`` factory rather than
    relying on an end-to-end "still works" assertion. Both ``aiokafka`` (via
    ``set(topics)`` in ``AIOKafkaConsumer._validate_topics``) and FastStream's
    ``TestKafkaBroker`` (via ``_find_handler``'s per-publish ``group_id``
    dedup) silently tolerate duplicate topic args, so an E2E test would pass
    even if ``dict.fromkeys`` were removed. The dedup at
    ``Worker.register_handlers`` is therefore not load-bearing for delivery,
    but it keeps registration logs, AsyncAPI specs, and observability tooling
    free of duplicate-topic noise — pinning that intent here.
    """
    worker = container.get(Worker)
    # The user manually listed the framework-private return topic in
    # ``subscribe_topics`` (e.g. because they wired things up before this fix
    # existed and shipped a forward-compatible workaround).
    agent = Agent(
        "dedup_agent",
        system_prompt="x",
        subscribe_topics=["dedup_agent.private.return", "dedup_chan.in"],
        publish_topic="dedup_agent.out",
        model_client=_call_one_tool_then_finalize(),
        tools=[agent_tool(get_users_name)],
    )
    worker.add_nodes(agent, *agent.tools)
    assert agent.subscribe_topics[0] == agent._return_topic

    broker = container.get(KafkaBroker)
    with patch.object(broker, "subscriber", wraps=broker.subscriber) as spy:
        worker.register_handlers()

    # Agents register two subscriptions under the same group_id: the main
    # public subscription (which carries the user's subscribe_topics + the
    # auto-added _return_topic, deduped) AND the aggregator's fanout-returns
    # subscription. The dedup invariant applies to the MAIN subscription —
    # identify it by the user's public input topic ("dedup_chan.in"); the
    # aggregator subscription only carries "dedup_agent.fanout-returns".
    main_sub_calls = [c for c in spy.call_args_list if c.kwargs.get("group_id") == "dedup_agent" and "dedup_chan.in" in c.args]
    assert len(main_sub_calls) == 1, f"expected one main subscriber registration for dedup_agent; got {len(main_sub_calls)}"
    assert main_sub_calls[0].args == ("dedup_agent.private.return", "dedup_chan.in"), (
        f"worker registered main subscriber with topics={main_sub_calls[0].args}; "
        f"expected deduped 2-tuple ('dedup_agent.private.return', 'dedup_chan.in'). "
        f"Without dict.fromkeys, the private return topic would appear twice in registration "
        f"logs / AsyncAPI / observability — silently tolerated by Kafka clients but noisy."
    )

    # The aggregator's fanout-returns subscription must own its own topic set
    # WITHOUT _return_topic. Adding _return_topic here would split _return_topic
    # delivery across two consumer groups (main handler and aggregator handler)
    # — main handler would never receive the re-entry envelope reliably.
    # The composite resolution in Worker.register_handlers gates this with
    # ``sub.handler == node.handler``; a regression that flipped that gate
    # would dual-subscribe the aggregator handler to _return_topic and break
    # re-entry routing non-deterministically.
    aggregator_sub_calls = [c for c in spy.call_args_list if c.kwargs.get("group_id") == "dedup_agent" and "dedup_agent.fanout-returns" in c.args]
    assert len(aggregator_sub_calls) == 1, f"expected one aggregator subscriber registration; got {len(aggregator_sub_calls)}"
    assert "dedup_agent.private.return" not in aggregator_sub_calls[0].args, (
        f"aggregator's fanout-returns subscription must NOT subscribe to _return_topic; "
        f"got args={aggregator_sub_calls[0].args}. Splitting _return_topic across two "
        f"consumer groups in the same group_id would non-deterministically route "
        f"re-entry envelopes to either handler."
    )

    # Agents register exactly two subscriptions under group_id="dedup_agent":
    # (1) the main handler subscription (carries user subscribe_topics +
    #     deduped _return_topic), and
    # (2) the aggregator's fanout-returns subscription (carries only
    #     {node_id}.fanout-returns).
    # Anything else (e.g., an accidental third registration from a refactor)
    # would change consumer-group rebalancing semantics and must be caught
    # here.
    all_dedup_agent_subs = [c for c in spy.call_args_list if c.kwargs.get("group_id") == "dedup_agent"]
    assert len(all_dedup_agent_subs) == 2, (
        f"expected exactly 2 subscriber registrations under group_id='dedup_agent' "
        f"(main + aggregator fanout-returns); got {len(all_dedup_agent_subs)}: "
        f"{[c.args for c in all_dedup_agent_subs]}"
    )


# ---------------------------------------------------------------------------
# Negative-path: BaseNodeSchema.__post_init__ guard on empty subscribe_topics
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "node_factory",
    [
        pytest.param(
            lambda: Agent(
                "empty_agent",
                system_prompt="x",
                subscribe_topics=[],
                model_client=_call_one_tool_then_finalize(),
            ),
            id="Agent",
        ),
        pytest.param(
            lambda: ConsumerNodeDef(
                node_id="empty_consumer",
                subscribe_topics=[],
                consume_fn=lambda r: None,
            ),
            id="ConsumerNodeDef",
        ),
        pytest.param(
            lambda: ToolNodeDef(
                node_id="empty_tool",
                tool_schema=MagicMock(),
                subscribe_topics=[],
                publish_topic="out",
                _tool=MagicMock(),
            ),
            id="ToolNodeDef",
        ),
    ],
)
def test_empty_subscribe_topics_raises_value_error(node_factory):
    """The ``BaseNodeSchema.__post_init__`` guard must reject empty
    ``subscribe_topics`` for every node kind. ``Agent`` and
    ``ConsumerNodeDef`` reach the guard via ``BaseNodeDef.__init__``'s
    ``super().__init__(...)`` chain; ``ToolNodeDef`` reaches it via the
    dataclass-auto-generated ``__init__`` (which bypasses
    ``BaseNodeDef.__init__`` entirely). A regression that moved the guard
    back into ``BaseNodeDef.__init__`` would silently re-open the
    ``ToolNodeDef`` bypass.
    """
    with pytest.raises(ValueError, match="requires at least one subscribe_topic"):
        node_factory()
