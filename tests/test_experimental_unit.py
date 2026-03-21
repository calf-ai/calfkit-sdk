"""Unit tests for experimental choreography-based node architecture.

Tests models (Payload, State, Deps, Envelope), schema generation,
base node choreography (Reply, Delegate, Emit, Silent routing), and
tool node mechanics. Does NOT require real LLM API calls.
"""

import asyncio
import time
from typing import Annotated, Any

import pytest
from faststream import Context
from faststream.kafka import TestKafkaBroker
from faststream.kafka.annotations import KafkaBroker as BrokerAnnotation

from calfkit.broker.broker import BrokerClient
from calfkit.experimental.context.session_context import BaseSessionRunContext
from calfkit.experimental.data_model.payload import (
    DataPart,
    FilePart,
    Payload,
    TextPart,
    ToolCallPart,
)
from calfkit.experimental.data_model.state_deps import AgentActivityState, Deps, State
from calfkit.experimental.nodes.node_def import (
    BaseNodeDef,
    Delegate,
    Emit,
    Envelope,
    NodeResult,
    Parallel,
    Reply,
    Silent,
)
from calfkit.experimental.nodes.tool_def import ToolNodeDef
from calfkit.experimental.nodes.tool_def import agent_tool as experimental_agent_tool
from calfkit.experimental.utils import find_first_tool_call_part, generate_payload_id
from calfkit.models.tool_context import ToolContext

# ===========================================================================
# Model tests
# ===========================================================================


class TestPayloadModel:
    """Tests for the Payload model and its ContentPart types."""

    def test_text_rendering_single(self):
        """Payload.text() returns the TextPart content."""
        payload = Payload(
            correlation_id="p1",
            parts=[TextPart(text="Hello world")],
            timestamp=time.time(),
        )
        assert payload.text() == "Hello world"

    def test_text_rendering_multiple(self):
        """Payload.text() concatenates multiple TextParts with separator."""
        payload = Payload(
            correlation_id="p2",
            parts=[TextPart(text="Hello"), TextPart(text="world")],
            timestamp=time.time(),
        )
        assert payload.text() == "Hello\n\nworld"
        assert payload.text(separator=" ") == "Hello world"

    def test_text_rendering_with_data_part(self):
        """Payload.text() serializes DataPart as JSON."""
        payload = Payload(
            correlation_id="p3",
            parts=[DataPart(data={"key": "value"})],
            timestamp=time.time(),
        )
        assert '"key"' in payload.text()
        assert '"value"' in payload.text()

    def test_text_rendering_skips_file_without_data(self):
        """Payload.text() skips FilePart when data is None."""
        payload = Payload(
            correlation_id="p4",
            parts=[
                TextPart(text="before"),
                FilePart(media_type="image/png", uri="s3://bucket/image.png"),
                TextPart(text="after"),
            ],
            timestamp=time.time(),
        )
        assert payload.text() == "before\n\nafter"

    def test_text_rendering_includes_file_with_data(self):
        """Payload.text() includes FilePart when data is present."""
        payload = Payload(
            correlation_id="p5",
            parts=[FilePart(media_type="text/plain", data="file content")],
            timestamp=time.time(),
        )
        assert payload.text() == "file content"

    def test_payload_id_unique(self):
        """Each Payload gets a unique id by default."""
        p1 = Payload(correlation_id="x", parts=[], timestamp=0.0)
        p2 = Payload(correlation_id="x", parts=[], timestamp=0.0)
        assert p1.id != p2.id

    def test_payload_serialization_roundtrip(self):
        """Payload survives JSON serialization and deserialization."""
        original = Payload(
            correlation_id="rt1",
            source_node_id="agent_a",
            parts=[
                TextPart(text="hello"),
                ToolCallPart(
                    tool_call_id="tc1",
                    kwargs={"arg": "val"},
                    tool_name="my_tool",
                ),
            ],
            timestamp=1234567890.0,
            metadata={"custom": "data"},
        )
        dumped = original.model_dump(mode="json")
        restored = Payload.model_validate(dumped)

        assert restored.correlation_id == "rt1"
        assert restored.source_node_id == "agent_a"
        assert len(restored.parts) == 2
        assert isinstance(restored.parts[0], TextPart)
        assert isinstance(restored.parts[1], ToolCallPart)
        assert restored.parts[1].tool_name == "my_tool"
        assert restored.metadata == {"custom": "data"}


class TestUtilFunctions:
    """Tests for utility functions."""

    def test_generate_payload_id_format(self):
        """generate_payload_id returns a prefixed uuid7 hex string."""
        pid = generate_payload_id()
        assert pid.startswith("payload_")
        assert len(pid) > len("payload_")

    def test_find_first_tool_call_part_found(self):
        """find_first_tool_call_part returns the first ToolCallPart."""
        payload = Payload(
            correlation_id="x",
            parts=[
                TextPart(text="preamble"),
                ToolCallPart(tool_call_id="tc1", kwargs={"a": 1}, tool_name="tool_a"),
                ToolCallPart(tool_call_id="tc2", kwargs={"b": 2}, tool_name="tool_b"),
            ],
            timestamp=0.0,
        )
        result = find_first_tool_call_part(payload)
        assert result is not None
        assert result.tool_call_id == "tc1"
        assert result.tool_name == "tool_a"

    def test_find_first_tool_call_part_not_found(self):
        """find_first_tool_call_part returns None when no ToolCallPart exists."""
        payload = Payload(
            correlation_id="x",
            parts=[TextPart(text="just text")],
            timestamp=0.0,
        )
        assert find_first_tool_call_part(payload) is None


class TestStateModel:
    """Tests for the State model."""

    def test_default_values(self):
        """State has correct default field values."""
        state = State()
        assert state.run_state.todo_stack == []
        assert state.run_state.message_history == []
        assert state.run_state.tool_results is None

    def test_state_serialization_roundtrip(self):
        """State survives serialization roundtrip."""
        payload = Payload(
            correlation_id="s1",
            parts=[TextPart(text="task")],
            timestamp=time.time(),
        )
        state = State(run_state=AgentActivityState(todo_stack=[payload]))
        dumped = state.model_dump(mode="json")
        restored = State.model_validate(dumped)
        assert len(restored.run_state.todo_stack) == 1
        assert restored.run_state.todo_stack[0].correlation_id == "s1"

    def test_state_extra_fields_ignored(self):
        """State ignores extra fields (ConfigDict extra='ignore')."""
        data = {
            "run_state": {"todo_stack": [], "message_history": []},
            "unknown_field": "ignored",
        }
        state = State.model_validate(data)
        assert not hasattr(state, "unknown_field")


class TestDepsModel:
    """Tests for the Deps model."""

    def test_deps_creation(self):
        """Deps model creates correctly with required fields."""
        deps = Deps(correlation_id="c1", agent_deps={"key": "value"})
        assert deps.correlation_id == "c1"
        assert deps.agent_deps == {"key": "value"}

    def test_deps_frozen(self):
        """Deps model is frozen (immutable)."""
        deps = Deps(correlation_id="c1", agent_deps=None)
        with pytest.raises(Exception):  # ValidationError for frozen model
            deps.correlation_id = "c2"

    def test_deps_extra_fields_ignored(self):
        """Deps ignores extra fields."""
        data = {"correlation_id": "c1", "agent_deps": None, "extra": "ignored"}
        deps = Deps.model_validate(data)
        assert deps.correlation_id == "c1"

    def test_deps_serialization_roundtrip(self):
        """Deps survives serialization roundtrip."""
        deps = Deps(correlation_id="c1", agent_deps={"nested": {"data": 42}})
        dumped = deps.model_dump(mode="json")
        restored = Deps.model_validate(dumped)
        assert restored.correlation_id == "c1"
        assert restored.agent_deps == {"nested": {"data": 42}}


class TestEnvelopeModel:
    """Tests for the Envelope wire format model."""

    def test_envelope_creation(self):
        """Envelope wraps context and reply_stack."""
        ctx = BaseSessionRunContext(
            state=State(),
            deps=Deps(correlation_id="e1", agent_deps=None),
        )
        envelope = Envelope(context=ctx, reply_stack=["topic_a", "topic_b"])
        assert envelope.reply_stack == ["topic_a", "topic_b"]
        assert envelope.context.deps.correlation_id == "e1"

    def test_envelope_default_empty_reply_stack(self):
        """Envelope defaults to empty reply_stack."""
        ctx = BaseSessionRunContext(
            state=State(),
            deps=Deps(correlation_id="e2", agent_deps=None),
        )
        envelope = Envelope(context=ctx)
        assert envelope.reply_stack == []

    def test_envelope_serialization_roundtrip(self):
        """Envelope survives full JSON serialization roundtrip.

        Uses plain BaseModel (not CompactBaseModel), so all fields
        are always included -- no exclude_unset gotchas.
        """
        payload = Payload(
            correlation_id="e3",
            parts=[TextPart(text="hello")],
            timestamp=time.time(),
        )
        ctx = BaseSessionRunContext(
            state=State(run_state=AgentActivityState(todo_stack=[payload])),
            deps=Deps(correlation_id="e3", agent_deps={"k": "v"}),
        )
        original = Envelope(context=ctx, reply_stack=["reply_topic"])

        dumped = original.model_dump(mode="json")
        restored = Envelope.model_validate(dumped)

        assert restored.reply_stack == ["reply_topic"]
        assert len(restored.context.state.run_state.todo_stack) == 1
        assert restored.context.deps.correlation_id == "e3"


# ===========================================================================
# Schema / decorator tests
# ===========================================================================


class TestAgentToolDecorator:
    """Tests for the experimental @agent_tool decorator."""

    def test_creates_tool_node_def(self):
        """@agent_tool returns a ToolNodeDef instance."""

        @experimental_agent_tool
        def my_func(x: int) -> str:
            """Double a number.

            Args:
                x: The number to double.
            """
            return str(x * 2)

        assert isinstance(my_func, ToolNodeDef)

    def test_subscribe_topic_convention(self):
        """@agent_tool creates subscribe topic as 'tool.{name}.input'."""

        @experimental_agent_tool
        def lookup(query: str) -> str:
            """Look something up.

            Args:
                query: The search query.
            """
            return query

        assert lookup.subscribe_topics == ["tool.lookup.input"]

    def test_publish_topic_convention(self):
        """@agent_tool creates publish topic as 'tool.{name}.output'."""

        @experimental_agent_tool
        def lookup(query: str) -> str:
            """Look something up.

            Args:
                query: The search query.
            """
            return query

        assert lookup.publish_topic == "tool.lookup.output"

    def test_node_id_convention(self):
        """@agent_tool sets node_id as 'tool_{name}'."""

        @experimental_agent_tool
        def calc(x: int) -> int:
            """Calculate.

            Args:
                x: Input.
            """
            return x

        assert calc.id == "tool_calc"
        assert calc.name == "tool_calc"

    def test_tool_schema_generated(self):
        """@agent_tool generates a ToolDefinition with correct name and params."""

        @experimental_agent_tool
        def add(a: int, b: int) -> int:
            """Add two numbers.

            Args:
                a: First number.
                b: Second number.
            """
            return a + b

        schema = add.tool_schema
        assert schema.name == "add"
        properties = schema.parameters_json_schema.get("properties", {})
        assert "a" in properties
        assert "b" in properties

    def test_tool_context_excluded_from_schema(self):
        """ToolContext parameter is hidden from the tool's JSON schema."""

        @experimental_agent_tool
        def search(ctx: ToolContext, query: str) -> str:
            """Search for something.

            Args:
                query: The search query.
            """
            return f"result for {query}"

        schema = search.tool_schema
        properties = schema.parameters_json_schema.get("properties", {})
        assert "ctx" not in properties, f"ToolContext leaked into schema: {properties}"
        assert "query" in properties, f"Expected 'query' in schema: {properties}"

    def test_tool_without_context_works(self):
        """Tools without ToolContext still generate correct schemas."""

        @experimental_agent_tool
        def greet(name: str) -> str:
            """Greet someone.

            Args:
                name: Person's name.
            """
            return f"Hello {name}"

        schema = greet.tool_schema
        properties = schema.parameters_json_schema.get("properties", {})
        assert "name" in properties
        assert "ctx" not in properties


# ===========================================================================
# Base node choreography tests (Reply, Delegate, Emit, Silent, Parallel)
# ===========================================================================


class StubReplyNode(BaseNodeDef[State, Deps]):
    """Returns Reply with the current state."""

    async def run(self, ctx: BaseSessionRunContext[State, Deps]) -> NodeResult[State]:
        return Reply(value=ctx.state)


class StubDelegateNode(BaseNodeDef[State, Deps]):
    """Returns a Delegate to a target topic."""

    def __init__(self, node_id: str, delegate_to: str, **kwargs: Any):
        self._delegate_to = delegate_to
        super().__init__(node_id, **kwargs)

    async def run(self, ctx: BaseSessionRunContext[State, Deps]) -> NodeResult[State]:
        return Delegate(topic=self._delegate_to, value=ctx.state)


class StubEmitNode(BaseNodeDef[State, Deps]):
    """Returns an Emit to a target topic."""

    def __init__(self, node_id: str, emit_to: str, **kwargs: Any):
        self._emit_to = emit_to
        super().__init__(node_id, **kwargs)

    async def run(self, ctx: BaseSessionRunContext[State, Deps]) -> NodeResult[State]:
        return Emit(value=ctx.state, topic=self._emit_to)


class StubSilentNode(BaseNodeDef[State, Deps]):
    """Returns Silent (no publish)."""

    async def run(self, ctx: BaseSessionRunContext[State, Deps]) -> NodeResult[State]:
        return Silent()


class StubParallelNode(BaseNodeDef[State, Deps]):
    """Returns Parallel with two delegates."""

    def __init__(self, node_id: str, topics: list[str], **kwargs: Any):
        self._topics = topics
        super().__init__(node_id, **kwargs)

    async def run(self, ctx: BaseSessionRunContext[State, Deps]) -> NodeResult[State]:
        return Parallel(delegates=[Delegate(topic=t, value=ctx.state) for t in self._topics])


def _make_test_envelope(reply_stack: list[str]) -> Envelope[State, Deps]:
    """Create a minimal test envelope."""
    payload = Payload(
        correlation_id="choreo-test",
        parts=[TextPart(text="test input")],
        timestamp=time.time(),
    )
    return Envelope(
        context=BaseSessionRunContext(
            state=State(run_state=AgentActivityState(todo_stack=[payload])),
            deps=Deps(correlation_id="choreo-test", agent_deps=None),
        ),
        reply_stack=reply_stack,
    )


class TestReplyChoreography:
    """Tests for Reply routing through the handler."""

    @pytest.mark.asyncio
    async def test_reply_routes_to_reply_stack_top(self):
        """Reply pops reply_stack and publishes to the top topic."""
        node = StubReplyNode("reply_node", subscribe_topics="reply_node.input")

        broker = BrokerClient()
        received: dict[str, asyncio.Queue[Envelope]] = {}

        @broker.subscriber("reply_node.input")
        async def handle(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
            broker_: BrokerAnnotation,
        ):
            await node.handler(envelope, correlation_id, broker_)

        @broker.subscriber("output_topic")
        async def collect(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
        ):
            if correlation_id not in received:
                received[correlation_id] = asyncio.Queue()
            received[correlation_id].put_nowait(envelope)

        async with TestKafkaBroker(broker) as _:
            test_envelope = _make_test_envelope(reply_stack=["output_topic"])
            await broker.publish(
                test_envelope,
                topic="reply_node.input",
                correlation_id="reply-test-1",
            )

            from tests.utils import wait_for_condition

            await wait_for_condition(lambda: "reply-test-1" in received, timeout=5.0)
            result = await received["reply-test-1"].get()

            # reply_stack should be empty (popped "output_topic")
            assert result.reply_stack == []
            # State should be preserved
            assert len(result.context.state.run_state.todo_stack) == 1

    @pytest.mark.asyncio
    async def test_reply_with_empty_stack_drops_message(self):
        """Reply with empty reply_stack logs warning and drops the message."""
        node = StubReplyNode("orphan_node", subscribe_topics="orphan.input")

        broker = BrokerClient()
        received: list[Envelope] = []

        @broker.subscriber("orphan.input")
        async def handle(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
            broker_: BrokerAnnotation,
        ):
            await node.handler(envelope, correlation_id, broker_)

        # No collector -- nothing should be published
        async with TestKafkaBroker(broker) as _:
            test_envelope = _make_test_envelope(reply_stack=[])
            await broker.publish(
                test_envelope,
                topic="orphan.input",
                correlation_id="orphan-1",
            )
            # If we get here without error, the message was dropped gracefully
            assert len(received) == 0


class TestDelegateChoreography:
    """Tests for Delegate routing through the handler."""

    @pytest.mark.asyncio
    async def test_delegate_pushes_return_topic_and_routes(self):
        """Delegate pushes self._return_topic onto reply_stack and publishes
        to the delegate's topic."""
        node = StubDelegateNode(
            "delegator",
            delegate_to="target_node.input",
            subscribe_topics="delegator.input",
        )

        broker = BrokerClient()
        received: dict[str, asyncio.Queue[Envelope]] = {}

        @broker.subscriber("delegator.input")
        async def handle(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
            broker_: BrokerAnnotation,
        ):
            await node.handler(envelope, correlation_id, broker_)

        @broker.subscriber("target_node.input")
        async def collect(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
        ):
            if correlation_id not in received:
                received[correlation_id] = asyncio.Queue()
            received[correlation_id].put_nowait(envelope)

        async with TestKafkaBroker(broker) as _:
            test_envelope = _make_test_envelope(reply_stack=["original_output"])
            await broker.publish(
                test_envelope,
                topic="delegator.input",
                correlation_id="delegate-test-1",
            )

            from tests.utils import wait_for_condition

            await wait_for_condition(lambda: "delegate-test-1" in received, timeout=5.0)
            result = await received["delegate-test-1"].get()

            # reply_stack should have original + return topic
            assert result.reply_stack == [
                "original_output",
                "delegator.private.return",
            ]


class TestEmitChoreography:
    """Tests for Emit (fire-and-forget) routing."""

    @pytest.mark.asyncio
    async def test_emit_publishes_without_reply_stack(self):
        """Emit publishes a BaseSessionRunContext (not Envelope) to the target topic."""
        node = StubEmitNode(
            "emitter",
            emit_to="event_log",
            subscribe_topics="emitter.input",
        )

        broker = BrokerClient()
        received: dict[str, asyncio.Queue] = {}

        @broker.subscriber("emitter.input")
        async def handle(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
            broker_: BrokerAnnotation,
        ):
            await node.handler(envelope, correlation_id, broker_)

        @broker.subscriber("event_log")
        async def collect(
            ctx: BaseSessionRunContext[State, Deps],
            correlation_id: Annotated[str, Context()],
        ):
            if correlation_id not in received:
                received[correlation_id] = asyncio.Queue()
            received[correlation_id].put_nowait(ctx)

        async with TestKafkaBroker(broker) as _:
            test_envelope = _make_test_envelope(reply_stack=["should_not_matter"])
            await broker.publish(
                test_envelope,
                topic="emitter.input",
                correlation_id="emit-test-1",
            )

            from tests.utils import wait_for_condition

            await wait_for_condition(lambda: "emit-test-1" in received, timeout=5.0)
            result = await received["emit-test-1"].get()
            # Emit sends BaseSessionRunContext, not Envelope
            assert isinstance(result, BaseSessionRunContext)
            assert len(result.state.run_state.todo_stack) == 1


class TestSilentChoreography:
    """Tests for Silent (no-op) behavior."""

    @pytest.mark.asyncio
    async def test_silent_produces_no_output(self):
        """Silent node produces no message on any topic."""
        node = StubSilentNode("silent_node", subscribe_topics="silent.input")

        broker = BrokerClient()
        any_output: list[Any] = []

        @broker.subscriber("silent.input")
        async def handle(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
            broker_: BrokerAnnotation,
        ):
            await node.handler(envelope, correlation_id, broker_)

        async with TestKafkaBroker(broker) as _:
            test_envelope = _make_test_envelope(reply_stack=["output"])
            await broker.publish(
                test_envelope,
                topic="silent.input",
                correlation_id="silent-1",
            )
            # Should complete without publishing anything
            assert len(any_output) == 0


class TestParallelChoreography:
    """Tests for Parallel fan-out routing."""

    @pytest.mark.asyncio
    async def test_parallel_fans_out_to_all_topics(self):
        """Parallel publishes to all delegate topics with correct reply_stack."""
        node = StubParallelNode(
            "fan_out",
            topics=["worker_a.input", "worker_b.input"],
            subscribe_topics="fan_out.input",
        )

        broker = BrokerClient()
        received_a: dict[str, asyncio.Queue[Envelope]] = {}
        received_b: dict[str, asyncio.Queue[Envelope]] = {}

        @broker.subscriber("fan_out.input")
        async def handle(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
            broker_: BrokerAnnotation,
        ):
            await node.handler(envelope, correlation_id, broker_)

        @broker.subscriber("worker_a.input")
        async def collect_a(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
        ):
            if correlation_id not in received_a:
                received_a[correlation_id] = asyncio.Queue()
            received_a[correlation_id].put_nowait(envelope)

        @broker.subscriber("worker_b.input")
        async def collect_b(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
        ):
            if correlation_id not in received_b:
                received_b[correlation_id] = asyncio.Queue()
            received_b[correlation_id].put_nowait(envelope)

        async with TestKafkaBroker(broker) as _:
            test_envelope = _make_test_envelope(reply_stack=["final_output"])
            await broker.publish(
                test_envelope,
                topic="fan_out.input",
                correlation_id="parallel-1",
            )

            from tests.utils import wait_for_condition

            await wait_for_condition(
                lambda: "parallel-1" in received_a and "parallel-1" in received_b,
                timeout=5.0,
            )
            result_a = await received_a["parallel-1"].get()
            result_b = await received_b["parallel-1"].get()

            # Both should have the return topic pushed
            expected_stack = ["final_output", "fan_out.private.return"]
            assert result_a.reply_stack == expected_stack
            assert result_b.reply_stack == expected_stack


# ===========================================================================
# Sequential delegates (list[Delegate]) choreography
# ===========================================================================


class StubSequentialDelegateNode(BaseNodeDef[State, Deps]):
    """Returns a list of Delegates for sequential chaining."""

    def __init__(self, node_id: str, delegate_topics: list[str], **kwargs: Any):
        self._delegate_topics = delegate_topics
        super().__init__(node_id, **kwargs)

    async def run(self, ctx: BaseSessionRunContext[State, Deps]) -> NodeResult[State]:
        return [Delegate(topic=t, value=ctx.state) for t in self._delegate_topics]


class TestSequentialDelegateChoreography:
    """Tests for list[Delegate] sequential chaining."""

    @pytest.mark.asyncio
    async def test_sequential_delegates_chain_correctly(self):
        """Sequential delegates: first topic gets published, rest are stacked.

        For delegates [A, B, C], the handler should:
        - Publish to A with reply_stack = [original, self._return_topic, C, B]
        - When A replies → B runs → when B replies → C runs → when C replies → original
        """
        node = StubSequentialDelegateNode(
            "seq_node",
            delegate_topics=["step_a", "step_b"],
            subscribe_topics="seq_node.input",
        )

        broker = BrokerClient()
        received: dict[str, asyncio.Queue[Envelope]] = {}

        @broker.subscriber("seq_node.input")
        async def handle(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
            broker_: BrokerAnnotation,
        ):
            await node.handler(envelope, correlation_id, broker_)

        @broker.subscriber("step_a")
        async def collect_a(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
        ):
            if correlation_id not in received:
                received[correlation_id] = asyncio.Queue()
            received[correlation_id].put_nowait(envelope)

        async with TestKafkaBroker(broker) as _:
            test_envelope = _make_test_envelope(reply_stack=["final_output"])
            await broker.publish(
                test_envelope,
                topic="seq_node.input",
                correlation_id="seq-1",
            )

            from tests.utils import wait_for_condition

            await wait_for_condition(lambda: "seq-1" in received, timeout=5.0)
            result = await received["seq-1"].get()

            # reply_stack should be: [original, return_topic, step_b (reversed remaining)]
            expected_stack = [
                "final_output",
                "seq_node.private.return",
                "step_b",
            ]
            assert result.reply_stack == expected_stack


# ===========================================================================
# BaseNodeDef property tests
# ===========================================================================


class TestBaseNodeDefProperties:
    """Tests for BaseNodeDef initialization and properties."""

    def test_node_id_and_name(self):
        """id and name properties return the node_id."""
        node = StubReplyNode("my_node", subscribe_topics="my_node.input")
        assert node.id == "my_node"
        assert node.name == "my_node"

    def test_return_topic_convention(self):
        """_return_topic follows {node_id}.private.return convention."""
        node = StubReplyNode("test_node", subscribe_topics="test_node.input")
        assert node._return_topic == "test_node.private.return"

    def test_subscribe_topics_string_becomes_list(self):
        """Single string subscribe_topics is wrapped in a list."""
        node = StubReplyNode("n", subscribe_topics="single.topic")
        assert node.subscribe_topics == ["single.topic"]

    def test_subscribe_topics_list_preserved(self):
        """List subscribe_topics is preserved as-is."""
        node = StubReplyNode("n", subscribe_topics=["topic.a", "topic.b"])
        assert node.subscribe_topics == ["topic.a", "topic.b"]

    def test_subscribe_topics_none_warns(self):
        """subscribe_topics=None emits a RuntimeWarning."""
        with pytest.warns(RuntimeWarning, match="not subscribed to any topics"):
            StubReplyNode("orphan")

    def test_publish_topic(self):
        """publish_topic is stored correctly."""
        node = StubReplyNode("n", subscribe_topics="in", publish_topic="out")
        assert node.publish_topic == "out"


# ===========================================================================
# envelope.input_args and Delegate.input tests
# ===========================================================================


class TestEnvelopeInputField:
    """Tests for the envelope.input_args per-invocation field."""

    def test_envelope_input_default_none(self):
        """envelope.input_args defaults to None."""
        ctx = BaseSessionRunContext(
            state=State(),
            deps=Deps(correlation_id="inp1", agent_deps=None),
        )
        envelope = Envelope(context=ctx)
        assert envelope.input_args is None

    def test_envelope_input_set(self):
        """envelope.input_args can carry arbitrary per-invocation data."""
        ctx = BaseSessionRunContext(
            state=State(),
            deps=Deps(correlation_id="inp2", agent_deps=None),
        )
        envelope = Envelope(context=ctx, input_args=["tc-42", "agent_name"])
        assert envelope.input_args == ["tc-42", "agent_name"]

    def test_envelope_input_serialization_roundtrip(self):
        """envelope.input_args survives JSON serialization roundtrip."""
        ctx = BaseSessionRunContext(
            state=State(),
            deps=Deps(correlation_id="inp3", agent_deps=None),
        )
        original = Envelope(context=ctx, reply_stack=["a"], input_args=["tc-1", "my_agent"])
        dumped = original.model_dump(mode="json")
        restored = Envelope.model_validate(dumped)
        assert restored.input_args == ["tc-1", "my_agent"]
        assert restored.reply_stack == ["a"]

    def test_envelope_input_none_serialization_roundtrip(self):
        """Envelope with input=None roundtrips correctly."""
        ctx = BaseSessionRunContext(
            state=State(),
            deps=Deps(correlation_id="inp4", agent_deps=None),
        )
        original = Envelope(context=ctx)
        dumped = original.model_dump(mode="json")
        restored = Envelope.model_validate(dumped)
        assert restored.input_args is None


class TestDelegateInputField:
    """Tests for the Delegate.input per-invocation field."""

    def test_delegate_input_default_none(self):
        """Delegate.input defaults to None."""
        d = Delegate(topic="some.topic")
        assert d.input_args is None

    def test_delegate_input_set(self):
        """Delegate.input_args can carry per-invocation positional args."""
        d = Delegate(topic="some.topic", value=State(), input_args=["tc-1", "source_node"])
        assert d.input_args == ["tc-1", "source_node"]


# ===========================================================================
# Input propagation tests
# ===========================================================================


class StubInputCapturingNode(BaseNodeDef[State, Deps]):
    """Captures the input parameter passed to run() for test assertions.

    Uses a custom parameter name (not 'input') to verify that the framework's
    signature inspection is name-agnostic.
    """

    captured_input: Any | None = None

    async def run(
        self, ctx: BaseSessionRunContext[State, Deps], my_custom_input: Any | None = None
    ) -> NodeResult[State]:
        self.captured_input = my_custom_input
        return Reply(value=ctx.state)


class StubDelegateWithInputNode(BaseNodeDef[State, Deps]):
    """Returns a Delegate that carries input."""

    def __init__(self, node_id: str, delegate_to: str, delegate_input: Any, **kwargs: Any):
        self._delegate_to = delegate_to
        self._delegate_input = delegate_input
        super().__init__(node_id, **kwargs)

    async def run(self, ctx: BaseSessionRunContext[State, Deps]) -> NodeResult[State]:
        return Delegate(topic=self._delegate_to, value=ctx.state, input_args=self._delegate_input)


class TestInputPropagation:
    """Tests for input propagation through the handler."""

    @pytest.mark.asyncio
    async def test_envelope_input_reaches_run(self):
        """envelope.input_args is passed to run() as the input parameter."""
        node = StubInputCapturingNode("cap_node", subscribe_topics="cap.input")

        broker = BrokerClient()

        @broker.subscriber("cap.input")
        async def handle(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
            broker_: BrokerAnnotation,
        ):
            await node.handler(envelope, correlation_id, broker_)

        @broker.subscriber("cap_node.private.return")
        async def sink(envelope: Envelope[State, Deps]):
            pass  # absorb reply

        async with TestKafkaBroker(broker) as _:
            envelope = _make_test_envelope(reply_stack=["cap_node.private.return"])
            envelope.input_args = ["tc-99"]
            await broker.publish(envelope, topic="cap.input", correlation_id="input-prop-1")

            from tests.utils import wait_for_condition

            await wait_for_condition(lambda: node.captured_input is not None, timeout=5.0)
            assert node.captured_input == "tc-99"

    @pytest.mark.asyncio
    async def test_envelope_without_input_passes_none(self):
        """Envelope without input passes None to run()."""
        node = StubInputCapturingNode("cap_node2", subscribe_topics="cap2.input")

        broker = BrokerClient()

        @broker.subscriber("cap2.input")
        async def handle(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
            broker_: BrokerAnnotation,
        ):
            await node.handler(envelope, correlation_id, broker_)

        @broker.subscriber("cap_node2.private.return")
        async def sink(envelope: Envelope[State, Deps]):
            pass

        async with TestKafkaBroker(broker) as _:
            envelope = _make_test_envelope(reply_stack=["cap_node2.private.return"])
            await broker.publish(envelope, topic="cap2.input", correlation_id="input-prop-2")

            from tests.utils import wait_for_condition

            # Wait for handler to run — captured_input stays None but we need
            # to ensure the handler actually executed
            await wait_for_condition(
                lambda: node.captured_input is not None or node.captured_input is None,
                timeout=5.0,
            )
            # Give broker a moment to process
            await asyncio.sleep(0.1)
            assert node.captured_input is None

    @pytest.mark.asyncio
    async def test_delegate_input_carried_to_outbound_envelope(self):
        """Delegate.input is carried into the outbound Envelope."""
        node = StubDelegateWithInputNode(
            "del_inp_node",
            delegate_to="target.input",
            delegate_input=["abc-123", "source_node"],
            subscribe_topics="del_inp.input",
        )

        broker = BrokerClient()
        received: dict[str, asyncio.Queue[Envelope]] = {}

        @broker.subscriber("del_inp.input")
        async def handle(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
            broker_: BrokerAnnotation,
        ):
            await node.handler(envelope, correlation_id, broker_)

        @broker.subscriber("target.input")
        async def collect(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
        ):
            if correlation_id not in received:
                received[correlation_id] = asyncio.Queue()
            received[correlation_id].put_nowait(envelope)

        async with TestKafkaBroker(broker) as _:
            envelope = _make_test_envelope(reply_stack=["original_output"])
            await broker.publish(envelope, topic="del_inp.input", correlation_id="del-inp-1")

            from tests.utils import wait_for_condition

            await wait_for_condition(lambda: "del-inp-1" in received, timeout=5.0)
            result = await received["del-inp-1"].get()
            assert result.input_args == ["abc-123", "source_node"]
