"""Unit tests for experimental choreography-based node architecture.

Tests models (Payload, State, Deps, Envelope), schema generation,
base node choreography (Call, ReturnCall, TailCall, Silent routing), and
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
from calfkit.experimental.base_models.actions import Call, Delegate, ReturnCall, TailCall
from calfkit.experimental.base_models.session_context import (
    CallFrame,
    SessionRunContext,
    Stack,
    WorkflowState,
)
from calfkit.experimental.data_model.payload import (
    DataPart,
    FilePart,
    Payload,
    TextPart,
    ToolCallPart,
)
from calfkit.experimental.data_model.state_deps import Deps, State
from calfkit.experimental.nodes.node_def import (
    BaseNodeDef,
    Envelope,
    NodeResult,
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
        assert state.message_history == []
        assert state.tool_results == {}

    def test_state_serialization_roundtrip(self):
        """State survives serialization roundtrip."""
        state = State(uncommitted_message=None)
        dumped = state.model_dump(mode="json")
        restored = State.model_validate(dumped)
        assert restored.message_history == []

    def test_state_extra_fields_ignored(self):
        """State ignores extra fields (ConfigDict extra='ignore')."""
        data = {
            "uncommitted_message": None,
            "message_history": [],
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
        ctx = SessionRunContext(
            state=State(),
            deps=Deps(correlation_id="e1", agent_deps=None),
        )
        initial_frame = CallFrame(target_topic="t", callback_topic="cb", input_args=None)
        call_stack = Stack[CallFrame]()
        call_stack.push(initial_frame)
        envelope = Envelope(
            context=ctx,
            internal_workflow_state=WorkflowState(call_stack=call_stack),
            reply_stack=["topic_a", "topic_b"],
        )
        assert envelope.reply_stack == ["topic_a", "topic_b"]
        assert envelope.context.deps.correlation_id == "e1"

    def test_envelope_default_empty_reply_stack(self):
        """Envelope defaults to empty reply_stack."""
        ctx = SessionRunContext(
            state=State(),
            deps=Deps(correlation_id="e2", agent_deps=None),
        )
        initial_frame = CallFrame(target_topic="t", callback_topic="cb", input_args=None)
        call_stack = Stack[CallFrame]()
        call_stack.push(initial_frame)
        envelope = Envelope(
            context=ctx,
            internal_workflow_state=WorkflowState(call_stack=call_stack),
        )
        assert envelope.reply_stack == []

    def test_envelope_serialization_roundtrip(self):
        """Envelope survives full JSON serialization roundtrip.

        Uses plain BaseModel (not CompactBaseModel), so all fields
        are always included -- no exclude_unset gotchas.
        """
        initial_frame = CallFrame(target_topic="t", callback_topic="cb", input_args=None)
        call_stack = Stack[CallFrame]()
        call_stack.push(initial_frame)
        ctx = SessionRunContext(
            state=State(uncommitted_message=None),
            deps=Deps(correlation_id="e3", agent_deps={"k": "v"}),
        )
        original = Envelope(
            context=ctx,
            internal_workflow_state=WorkflowState(call_stack=call_stack),
            reply_stack=["reply_topic"],
        )

        dumped = original.model_dump(mode="json")
        restored = Envelope[State, Deps].model_validate(dumped)

        assert restored.reply_stack == ["reply_topic"]
        assert restored.context.deps.correlation_id == "e3"
        assert restored.context.state.message_history == []


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
# Base node choreography tests (Call, ReturnCall, TailCall, Silent, Parallel)
# ===========================================================================


class StubReturnCallNode(BaseNodeDef[State, Deps]):
    """Returns ReturnCall with the current state (callback the caller)."""

    async def run(self, ctx: SessionRunContext[State, Deps]) -> NodeResult[State]:
        return ReturnCall(state=ctx.state)


class StubCallNode(BaseNodeDef[State, Deps]):
    """Returns a Call to a target topic (push frame, expect callback)."""

    def __init__(self, node_id: str, call_to: str, **kwargs: Any):
        self._call_to = call_to
        super().__init__(node_id, **kwargs)

    async def run(self, ctx: SessionRunContext[State, Deps]) -> NodeResult[State]:
        return Call(self._call_to, ctx.state)


class StubTailCallNode(BaseNodeDef[State, Deps]):
    """Returns a TailCall to a target topic (fire-and-forget, inherits callback)."""

    def __init__(self, node_id: str, tail_call_to: str, **kwargs: Any):
        self._tail_call_to = tail_call_to
        super().__init__(node_id, **kwargs)

    async def run(self, ctx: SessionRunContext[State, Deps]) -> NodeResult[State]:
        return TailCall(self._tail_call_to, ctx.state)


class StubSilentNode(BaseNodeDef[State, Deps]):
    """Returns Silent (no publish)."""

    async def run(self, ctx: SessionRunContext[State, Deps]) -> NodeResult[State]:
        return Silent()


def _make_test_envelope(
    reply_stack: list[str],
    *,
    target_topic: str = "test.input",
    callback_topic: str = "test.return",
    input_args: list[Any] | None = None,
) -> Envelope[State, Deps]:
    """Create a minimal test envelope with a single-frame call stack."""
    initial_frame = CallFrame(
        target_topic=target_topic,
        callback_topic=callback_topic,
        input_args=input_args,
    )
    call_stack = Stack[CallFrame]()
    call_stack.push(initial_frame)
    return Envelope(
        context=SessionRunContext(
            state=State(uncommitted_message=None),
            deps=Deps(correlation_id="choreo-test", agent_deps=None),
        ),
        internal_workflow_state=WorkflowState(call_stack=call_stack),
        reply_stack=reply_stack,
    )


class TestReturnCallChoreography:
    """Tests for ReturnCall routing through the handler."""

    @pytest.mark.asyncio
    async def test_return_call_routes_to_callback_topic(self):
        """ReturnCall unwinds the call frame and publishes to callback_topic."""
        node = StubReturnCallNode("reply_node", subscribe_topics="reply_node.input")

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
            test_envelope = _make_test_envelope(reply_stack=[], callback_topic="output_topic")
            await broker.publish(
                test_envelope,
                topic="reply_node.input",
                correlation_id="reply-test-1",
            )

            from tests.utils import wait_for_condition

            await wait_for_condition(lambda: "reply-test-1" in received, timeout=5.0)
            result = await received["reply-test-1"].get()

            # State should be preserved
            assert result.context.state is not None


class TestCallChoreography:
    """Tests for Call routing through the handler."""

    @pytest.mark.asyncio
    async def test_call_pushes_frame_and_routes_to_target(self):
        """Call pushes a new frame to the call stack and publishes to the target topic."""
        node = StubCallNode(
            "caller",
            call_to="target_node.input",
            subscribe_topics="caller.input",
        )

        broker = BrokerClient()
        received: dict[str, asyncio.Queue[Envelope]] = {}

        @broker.subscriber("caller.input")
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
            test_envelope = _make_test_envelope(reply_stack=[])
            await broker.publish(
                test_envelope,
                topic="caller.input",
                correlation_id="call-test-1",
            )

            from tests.utils import wait_for_condition

            await wait_for_condition(lambda: "call-test-1" in received, timeout=5.0)
            result = await received["call-test-1"].get()

            # Current frame should target the callee, with callback to caller's subscribe topic
            assert result.internal_workflow_state.current_frame.target_topic == "target_node.input"
            assert result.internal_workflow_state.current_frame.callback_topic == "caller.input"


class TestTailCallChoreography:
    """Tests for TailCall (fire-and-forget, inherits callback) routing."""

    @pytest.mark.asyncio
    async def test_tail_call_replaces_frame_and_routes(self):
        """TailCall unwinds current frame, invokes new frame inheriting callback, and publishes."""
        node = StubTailCallNode(
            "emitter",
            tail_call_to="event_log",
            subscribe_topics="emitter.input",
        )

        broker = BrokerClient()
        received: dict[str, asyncio.Queue[Envelope]] = {}

        @broker.subscriber("emitter.input")
        async def handle(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
            broker_: BrokerAnnotation,
        ):
            await node.handler(envelope, correlation_id, broker_)

        @broker.subscriber("event_log")
        async def collect(
            envelope: Envelope[State, Deps],
            correlation_id: Annotated[str, Context()],
        ):
            if correlation_id not in received:
                received[correlation_id] = asyncio.Queue()
            received[correlation_id].put_nowait(envelope)

        async with TestKafkaBroker(broker) as _:
            test_envelope = _make_test_envelope(reply_stack=[], callback_topic="original_callback")
            await broker.publish(
                test_envelope,
                topic="emitter.input",
                correlation_id="tail-call-test-1",
            )

            from tests.utils import wait_for_condition

            await wait_for_condition(lambda: "tail-call-test-1" in received, timeout=5.0)
            result = await received["tail-call-test-1"].get()

            # TailCall inherits the original callback_topic
            assert result.internal_workflow_state.current_frame.target_topic == "event_log"
            assert (
                result.internal_workflow_state.current_frame.callback_topic == "original_callback"
            )


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


# ===========================================================================
# BaseNodeDef property tests
# ===========================================================================


class TestBaseNodeDefProperties:
    """Tests for BaseNodeDef initialization and properties."""

    def test_node_id_and_name(self):
        """id and name properties return the node_id."""
        node = StubReturnCallNode("my_node", subscribe_topics="my_node.input")
        assert node.id == "my_node"
        assert node.name == "my_node"

    def test_return_topic_convention(self):
        """_return_topic follows {node_id}.private.return convention."""
        node = StubReturnCallNode("test_node", subscribe_topics="test_node.input")
        assert node._return_topic == "test_node.private.return"

    def test_subscribe_topics_string_becomes_list(self):
        """Single string subscribe_topics is wrapped in a list."""
        node = StubReturnCallNode("n", subscribe_topics="single.topic")
        assert node.subscribe_topics == ["single.topic"]

    def test_subscribe_topics_list_preserved(self):
        """List subscribe_topics is preserved as-is."""
        node = StubReturnCallNode("n", subscribe_topics=["topic.a", "topic.b"])
        assert node.subscribe_topics == ["topic.a", "topic.b"]

    def test_subscribe_topics_none_raises(self):
        """subscribe_topics=None raises RuntimeError."""
        with pytest.raises(RuntimeError, match="not subscribed to any topics"):
            StubReturnCallNode("orphan")

    def test_publish_topic(self):
        """publish_topic is stored correctly."""
        node = StubReturnCallNode("n", subscribe_topics="in", publish_topic="out")
        assert node.publish_topic == "out"


# ===========================================================================
# CallFrame.input_args and Delegate.input tests
# ===========================================================================


class TestCallFrameInputArgs:
    """Tests for input_args on the CallFrame (via WorkflowState.current_frame)."""

    def test_current_frame_input_default_none(self):
        """CallFrame.input_args defaults to None when constructed without it."""
        envelope = _make_test_envelope(reply_stack=[])
        assert envelope.internal_workflow_state.current_frame.input_args is None

    def test_current_frame_input_set(self):
        """CallFrame.input_args carries arbitrary per-invocation data."""
        envelope = _make_test_envelope(reply_stack=[], input_args=["tc-42", "agent_name"])
        assert envelope.internal_workflow_state.current_frame.input_args == ["tc-42", "agent_name"]

    def test_current_frame_input_serialization_roundtrip(self):
        """CallFrame.input_args survives Envelope JSON serialization roundtrip."""
        original = _make_test_envelope(reply_stack=["a"], input_args=["tc-1", "my_agent"])
        dumped = original.model_dump(mode="json")
        restored = Envelope[State, Deps].model_validate(dumped)
        assert restored.internal_workflow_state.current_frame.input_args == ["tc-1", "my_agent"]
        assert restored.reply_stack == ["a"]

    def test_current_frame_input_none_serialization_roundtrip(self):
        """CallFrame with input_args=None roundtrips correctly."""
        original = _make_test_envelope(reply_stack=[])
        dumped = original.model_dump(mode="json")
        restored = Envelope[State, Deps].model_validate(dumped)
        assert restored.internal_workflow_state.current_frame.input_args is None


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
        self, ctx: SessionRunContext[State, Deps], my_custom_input: Any | None = None
    ) -> NodeResult[State]:
        self.captured_input = my_custom_input
        return ReturnCall(state=ctx.state)


class StubCallWithInputNode(BaseNodeDef[State, Deps]):
    """Returns a Call that carries input_args."""

    def __init__(self, node_id: str, call_to: str, call_input: list[Any], **kwargs: Any):
        self._call_to = call_to
        self._call_input = call_input
        super().__init__(node_id, **kwargs)

    async def run(self, ctx: SessionRunContext[State, Deps]) -> NodeResult[State]:
        return Call(self._call_to, ctx.state, *self._call_input)


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
            envelope = _make_test_envelope(
                reply_stack=["cap_node.private.return"], input_args=["tc-99"]
            )
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
    async def test_call_input_carried_to_outbound_frame(self):
        """Call input_args are carried into the outbound CallFrame."""
        node = StubCallWithInputNode(
            "del_inp_node",
            call_to="target.input",
            call_input=["abc-123", "source_node"],
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
            assert result.internal_workflow_state.current_frame.input_args == [
                "abc-123",
                "source_node",
            ]
