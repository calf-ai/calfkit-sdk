"""Tests for structured input/output support in agents.

Uses FunctionModel + TestKafkaBroker for synchronous, deterministic testing
without real LLM calls.
"""

import asyncio
import json
from typing import Annotated

import pytest
from faststream import Context
from faststream.kafka import TestKafkaBroker
from pydantic import BaseModel

from calfkit._vendor.pydantic_ai import (
    ModelMessage,
    ModelResponse,
    TextPart,
    ToolCallPart,
    models,
)
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.broker.broker import BrokerClient
from calfkit.models.event_envelope import EventEnvelope
from calfkit.models.payloads import RouterPayload
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.base_tool_node import agent_tool
from calfkit.nodes.chat_node import ChatNode
from calfkit.runners.service import NodesService
from calfkit.runners.service_client import RouterServiceClient
from calfkit.stores.in_memory import InMemoryMessageHistoryStore
from tests.utils import wait_for_condition


@pytest.fixture(autouse=True)
def block_model_requests():
    """Block actual model requests during unit tests."""
    original_value = models.ALLOW_MODEL_REQUESTS
    models.ALLOW_MODEL_REQUESTS = False
    yield
    models.ALLOW_MODEL_REQUESTS = original_value


# --- Shared test models ---


class OrderResult(BaseModel):
    item: str
    quantity: int


class OrderInput(BaseModel):
    customer_name: str
    order_id: str


# --- Tests ---


@pytest.mark.asyncio
async def test_structured_output_basic():
    """ChatNode with output_type returns structured payload on the envelope."""

    def structured_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        # Return a structured output via the final_result tool
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name="final_result",
                    args=json.dumps({"item": "widget", "quantity": 5}),
                    tool_call_id="out-1",
                )
            ]
        )

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = FunctionModel(structured_model)
    chat_node = ChatNode(model_client, output_type=OrderResult)
    service.register_node(chat_node)

    router_node = AgentRouterNode(
        chat_node=ChatNode(model_client, output_type=OrderResult),
        name="structured_agent",
    )
    service.register_node(router_node)

    response_store: dict[str, asyncio.Queue[EventEnvelope]] = {}

    @broker.subscriber(router_node.publish_to_topic or "default_collect")
    def collect_response(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(event_envelope)

    async with TestKafkaBroker(broker) as _:
        trace_id = await router_node.invoke(
            user_prompt="Order 5 widgets",
            broker=broker,
            final_response_topic="final_response",
            correlation_id="test-structured-1",
        )

        await wait_for_condition(lambda: trace_id in response_store, timeout=5.0)
        queue = response_store[trace_id]
        result = await queue.get()

        # Verify structured payload is set
        assert result.payload is not None
        assert result.payload["item"] == "widget"
        assert result.payload["quantity"] == 5
        assert result.state.is_end_of_turn


@pytest.mark.asyncio
async def test_structured_output_with_function_tools():
    """Function tools are routed first, then structured output is returned."""
    call_count = 0

    @agent_tool
    def lookup_price(item: str) -> str:
        """Look up the price of an item.

        Args:
            item: The item name.
        """
        return f"${item}: $9.99"

    def model_with_tools(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            # First call: use a tool
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name="lookup_price",
                        args={"item": "gadget"},
                        tool_call_id="tool-1",
                    )
                ]
            )
        else:
            # Second call: return structured output
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name="final_result",
                        args=json.dumps({"item": "gadget", "quantity": 3}),
                        tool_call_id="out-1",
                    )
                ]
            )

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = FunctionModel(model_with_tools)
    chat_node = ChatNode(model_client, output_type=OrderResult)
    service.register_node(chat_node)

    router_node = AgentRouterNode(
        chat_node=ChatNode(model_client, output_type=OrderResult),
        tool_nodes=[lookup_price],
        name="tools_structured_agent",
    )
    service.register_node(router_node)
    service.register_node(lookup_price)

    response_store: dict[str, asyncio.Queue[EventEnvelope]] = {}

    @broker.subscriber(router_node.publish_to_topic or "default_collect")
    def collect_response(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(event_envelope)

    async with TestKafkaBroker(broker) as _:
        trace_id = await router_node.invoke(
            user_prompt="Order 3 gadgets",
            broker=broker,
            final_response_topic="final_response",
            correlation_id="test-tools-structured-1",
        )

        await wait_for_condition(lambda: trace_id in response_store, timeout=5.0)
        queue = response_store[trace_id]
        result = await queue.get()

        # Verify tool was called, then structured output returned
        assert call_count == 2
        assert result.payload is not None
        assert result.payload["item"] == "gadget"
        assert result.payload["quantity"] == 3


@pytest.mark.asyncio
async def test_structured_input_validation():
    """Payload is validated against deps_type and passed as deps."""

    def input_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart("Processed order")])

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = FunctionModel(input_model)
    chat_node = ChatNode(model_client)
    service.register_node(chat_node)

    router_node = AgentRouterNode(
        chat_node=ChatNode(model_client),
        name="input_agent",
        deps_type=OrderInput,
    )
    service.register_node(router_node)

    response_store: dict[str, asyncio.Queue[EventEnvelope]] = {}

    @broker.subscriber(router_node.publish_to_topic or "default_collect")
    def collect_response(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(event_envelope)

    async with TestKafkaBroker(broker) as _:
        trace_id = await router_node.invoke(
            user_prompt="Process this order",
            broker=broker,
            final_response_topic="final_response",
            correlation_id="test-input-1",
            deps={"customer_name": "Alice", "order_id": "ORD-123"},
        )

        await wait_for_condition(lambda: trace_id in response_store, timeout=5.0)
        queue = response_store[trace_id]
        result = await queue.get()

        assert isinstance(result.state.latest_message_in_history, ModelResponse)
        # Input payload was consumed as deps — output payload is the text response
        assert isinstance(result.payload, str)


@pytest.mark.asyncio
async def test_payload_consumed_as_input_not_forwarded():
    """When payload is sent as structured input, it is consumed by on_request
    (set as deps, cleared) and does NOT appear as payload in the final response.
    This verifies the input payload is not mistaken for structured output."""

    def deps_capture_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        # Model just returns text — no structured output
        return ModelResponse(parts=[TextPart("Processed")])

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = FunctionModel(deps_capture_model)
    chat_node = ChatNode(model_client)
    service.register_node(chat_node)

    router_node = AgentRouterNode(
        chat_node=ChatNode(model_client),
        name="payload_consume_agent",
        deps_type=OrderInput,
    )
    service.register_node(router_node)

    response_store: dict[str, asyncio.Queue[EventEnvelope]] = {}

    @broker.subscriber(router_node.publish_to_topic or "default_collect")
    def collect_response(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(event_envelope)

    async with TestKafkaBroker(broker) as _:
        trace_id = await router_node.invoke(
            user_prompt="Process this",
            broker=broker,
            final_response_topic="final_response",
            correlation_id="test-payload-consume-1",
            deps={"customer_name": "Bob", "order_id": "ORD-456"},
        )

        await wait_for_condition(lambda: trace_id in response_store, timeout=5.0)
        queue = response_store[trace_id]
        result = await queue.get()

        # Input payload was consumed as deps — output payload is the text response
        assert isinstance(result.payload, str)
        assert "Processed" in result.payload
        assert isinstance(result.state.latest_message_in_history, ModelResponse)


@pytest.mark.asyncio
async def test_backward_compat_text_output():
    """Agent without output_type works exactly as before:
    text response, no structured output payload."""

    def text_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart("Hello, world!")])

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = FunctionModel(text_model)
    chat_node = ChatNode(model_client)
    service.register_node(chat_node)

    router_node = AgentRouterNode(
        chat_node=ChatNode(model_client),
        name="compat_agent",
    )
    service.register_node(router_node)

    response_store: dict[str, asyncio.Queue[EventEnvelope]] = {}

    @broker.subscriber(router_node.publish_to_topic or "default_collect")
    def collect_response(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(event_envelope)

    async with TestKafkaBroker(broker) as _:
        trace_id = await router_node.invoke(
            user_prompt="Hello",
            broker=broker,
            final_response_topic="final_response",
            correlation_id="test-compat-1",
        )

        await wait_for_condition(lambda: trace_id in response_store, timeout=5.0)
        queue = response_store[trace_id]
        result = await queue.get()

        assert isinstance(result.state.latest_message_in_history, ModelResponse)
        # Text output goes on payload — str is an output type
        assert isinstance(result.payload, str)
        assert "Hello, world!" in result.payload


@pytest.mark.asyncio
async def test_service_client_payload():
    """RouterServiceClient.request(deps=...) flows correctly."""

    def payload_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart("Processed")])

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = FunctionModel(payload_model)
    chat_node = ChatNode(model_client)
    service.register_node(chat_node)

    router_node = AgentRouterNode(
        chat_node=ChatNode(model_client),
        name="client_payload_agent",
        deps_type=OrderInput,
    )
    service.register_node(router_node)

    async with TestKafkaBroker(broker) as _:
        client = RouterServiceClient(broker, router_node)
        response = await client.request(
            user_prompt="Process order",
            deps={"customer_name": "Charlie", "order_id": "ORD-789"},
        )

        final_msg = await asyncio.wait_for(response.get_final_response(), timeout=5.0)
        assert isinstance(final_msg, ModelResponse)


@pytest.mark.asyncio
async def test_invoke_response_get_output():
    """InvokeResponse.get_output() returns structured payload."""

    def structured_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name="final_result",
                    args=json.dumps({"item": "gizmo", "quantity": 10}),
                    tool_call_id="out-1",
                )
            ]
        )

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = FunctionModel(structured_model)
    chat_node = ChatNode(model_client, output_type=OrderResult)
    service.register_node(chat_node)

    router_node = AgentRouterNode(
        chat_node=ChatNode(model_client, output_type=OrderResult),
        name="get_output_agent",
    )
    service.register_node(router_node)

    async with TestKafkaBroker(broker) as _:
        client = RouterServiceClient(broker, router_node)
        response = await client.request(user_prompt="Order 10 gizmos")

        output = await asyncio.wait_for(response.get_output(), timeout=5.0)
        assert output is not None
        assert output["item"] == "gizmo"
        assert output["quantity"] == 10


def test_envelope_generic_serialization():
    """EventEnvelope[OrderResult] roundtrip serialization works."""
    envelope: EventEnvelope[OrderResult] = EventEnvelope(
        trace_id="test-123",
        payload=OrderResult(item="widget", quantity=5),
    )

    # Serialize
    dumped = envelope.model_dump()
    assert dumped["payload"]["item"] == "widget"
    assert dumped["payload"]["quantity"] == 5

    # Deserialize
    restored = EventEnvelope.model_validate(dumped)
    assert restored.payload == {"item": "widget", "quantity": 5}
    assert restored.trace_id == "test-123"


def test_envelope_payload_none_excluded():
    """Payload=None is excluded from serialization (CompactBaseModel behavior)."""
    envelope = EventEnvelope(trace_id="test-456")
    dumped = envelope.model_dump()
    assert "payload" not in dumped


def test_envelope_state_always_serialized():
    """State is always included in serialization even when created via default_factory."""
    envelope = EventEnvelope(trace_id="test-789")
    dumped = envelope.model_dump()
    assert "state" in dumped


@pytest.mark.asyncio
async def test_on_request_handler():
    """Verify on_request handles initial requests correctly."""

    def echo_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart("echo")])

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = FunctionModel(echo_model)
    chat_node = ChatNode(model_client, name="echo_chat")
    service.register_node(chat_node)

    router_node = AgentRouterNode(
        chat_node=chat_node,
        name="request_handler_agent",
    )
    service.register_node(router_node)

    response_store: dict[str, asyncio.Queue[EventEnvelope]] = {}

    @broker.subscriber(router_node.publish_to_topic or "default_collect")
    def collect_response(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(event_envelope)

    async with TestKafkaBroker(broker) as _:
        # Publish directly to the entrypoint topic
        env = EventEnvelope(
            trace_id="on-request-1",
            payload=RouterPayload(user_prompt="hello"),
            final_response_topic="final_response",
        )
        env.state.mark_as_start_of_turn()
        assert router_node.entrypoint_topic is not None
        await broker.publish(
            env,
            topic=router_node.entrypoint_topic,
            correlation_id="on-request-1",
        )
        await wait_for_condition(lambda: "on-request-1" in response_store, timeout=5.0)
        result = await response_store["on-request-1"].get()
        assert isinstance(result.state.latest_message_in_history, ModelResponse)
        assert "echo" in str(result.state.latest_message_in_history.parts[0])


@pytest.mark.asyncio
async def test_on_return_handler():
    """Verify on_return handles ChatNode responses correctly with structured output."""

    def structured_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name="final_result",
                    args=json.dumps({"item": "bolt", "quantity": 100}),
                    tool_call_id="out-1",
                )
            ]
        )

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = FunctionModel(structured_model)
    chat_node = ChatNode(model_client, name="return_chat", output_type=OrderResult)
    service.register_node(chat_node)

    router_node = AgentRouterNode(
        chat_node=chat_node,
        name="return_handler_agent",
    )
    service.register_node(router_node)

    response_store: dict[str, asyncio.Queue[EventEnvelope]] = {}

    @broker.subscriber(router_node.publish_to_topic or "default_collect")
    def collect_response(event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]):
        if correlation_id not in response_store:
            response_store[correlation_id] = asyncio.Queue()
        response_store[correlation_id].put_nowait(event_envelope)

    async with TestKafkaBroker(broker) as _:
        env = EventEnvelope(
            trace_id="on-return-1",
            payload=RouterPayload(user_prompt="order bolts"),
            final_response_topic="final_response",
        )
        env.state.mark_as_start_of_turn()
        assert router_node.entrypoint_topic is not None
        await broker.publish(
            env,
            topic=router_node.entrypoint_topic,
            correlation_id="on-return-1",
        )
        await wait_for_condition(lambda: "on-return-1" in response_store, timeout=5.0)
        result = await response_store["on-return-1"].get()

        # Structured output should be on the payload
        assert result.payload is not None
        assert result.payload["item"] == "bolt"
        assert result.payload["quantity"] == 100
        assert result.state.is_end_of_turn


@pytest.mark.asyncio
async def test_structured_output_full_loop_with_memory():
    """Full router flow with memory store — structured output, no infinite loop."""

    def structured_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name="final_result",
                    args=json.dumps({"item": "screw", "quantity": 50}),
                    tool_call_id="out-1",
                )
            ]
        )

    broker = BrokerClient()
    service = NodesService(broker)

    model_client = FunctionModel(structured_model)
    chat_node = ChatNode(model_client, output_type=OrderResult)
    service.register_node(chat_node)

    memory_store = InMemoryMessageHistoryStore()
    router_node = AgentRouterNode(
        chat_node=ChatNode(model_client, output_type=OrderResult),
        name="full_loop_agent",
        message_history_store=memory_store,
    )
    service.register_node(router_node)

    async with TestKafkaBroker(broker) as _:
        client = RouterServiceClient(broker, router_node)
        response = await client.request(
            user_prompt="Order 50 screws",
            thread_id="structured-thread-1",
        )

        output = await asyncio.wait_for(response.get_output(), timeout=5.0)
        assert output is not None
        assert output["item"] == "screw"
        assert output["quantity"] == 50

        # Verify messages were persisted
        stored = await memory_store.get("structured-thread-1", scope="full_loop_agent")
        assert len(stored) >= 2  # At least request + response
