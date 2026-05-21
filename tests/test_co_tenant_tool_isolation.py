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
    """Every cloned ``Call`` in a parallel fan-out must carry
    ``frame.callback_topic == agent._return_topic`` — a regression that drops
    ``_return_topic`` from only the parallel-fan-out branch of
    ``BaseNodeDef._publish_action`` would not be caught by the single-``Call``
    contract test above.
    """
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
        await client.invoke_node("hi", agent.subscribe_topics[0])
        await wait_for_condition(
            lambda: len(captured_a) >= 1 and len(captured_b) >= 1 and len(captured_c) >= 1,
            timeout=5.0,
        )

    all_callbacks = captured_a + captured_b + captured_c
    assert all_callbacks, "no fan-out Calls captured at tool input topics"
    for callback in all_callbacks:
        assert callback == agent._return_topic, (
            f"parallel fan-out Call wrote callback_topic={callback!r}; "
            f"expected node-private _return_topic={agent._return_topic!r} "
            f"(issue #141, _publish_action parallel-fan-out branch)"
        )


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

    dedup_calls = [c for c in spy.call_args_list if c.kwargs.get("group_id") == "dedup_agent"]
    assert len(dedup_calls) == 1, f"expected one subscriber registration for dedup_agent; got {len(dedup_calls)}"
    assert dedup_calls[0].args == ("dedup_agent.private.return", "dedup_chan.in"), (
        f"worker registered subscriber with topics={dedup_calls[0].args}; "
        f"expected deduped 2-tuple ('dedup_agent.private.return', 'dedup_chan.in'). "
        f"Without dict.fromkeys, the private return topic would appear twice in registration "
        f"logs / AsyncAPI / observability — silently tolerated by Kafka clients but noisy."
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
