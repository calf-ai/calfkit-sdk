import os
import random
from collections.abc import Callable, Sequence
from typing import Any

import pytest
import uuid_utils
from dishka import make_container
from dotenv import load_dotenv
from faker import Faker

from calfkit._types import OutputT
from calfkit._vendor.pydantic_ai.exceptions import ModelRetry
from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, RetryPromptPart, TextPart, ToolCallPart, ToolReturn
from calfkit._vendor.pydantic_ai.models.function import FunctionModel
from calfkit._vendor.pydantic_ai.tools import DeferredToolCallResult as ToolCallResult
from calfkit.models.envelope import Envelope
from calfkit.models.node_schema import BaseToolNodeSchema
from calfkit.models.session_context import CallFrame, CallFrameStack, Deps, SessionRunContext, WorkflowState
from calfkit.models.state import OverridesState, State
from calfkit.nodes import Agent, ToolNodeDef
from calfkit.providers.pydantic_ai.openai import OpenAIModelClient, OpenAIResponsesModelClient
from calfkit.worker import Worker
from tests.providers import (
    INSTRUCTIONS_TEST_SYSTEM_PROMPT,
    AgentProvider,
    ClientProvider,
    ContextualTool,
    NoArgTool,
    Response,
    SimpleAgent,
    StructuredAgent,
    WorkerProvider,
    agent_name,
    echo_instructions,
)

load_dotenv()

fake = Faker()


@pytest.fixture
def container():
    c = make_container(WorkerProvider(), ClientProvider(), AgentProvider())
    yield c
    c.close()


@pytest.fixture(params=["parallel", "sequential"])
def agent_constructor_args_sequential_modes(request) -> dict[str, bool]:
    mode: str = request.param
    return {"sequential_only_mode": mode == "sequential"}


@pytest.fixture(params=["openai_responses", "openai_chat"])
def agent_constructor_args_model_client(request) -> dict[str, Any]:
    model_type: str = request.param
    if model_type == "openai_chat":
        model_client = OpenAIModelClient(os.environ["TEST_LLM_MODEL_NAME"], reasoning_effort=os.getenv("TEST_REASONING_EFFORT"))
        return {"model_client": model_client}
    elif model_type == "openai_responses":
        model_client = OpenAIResponsesModelClient(os.environ["TEST_LLM_MODEL_NAME"], reasoning_effort=os.getenv("TEST_REASONING_EFFORT"))
        return {"model_client": model_client}
    else:
        raise RuntimeError(f"Invalid model client: {model_type}")


@pytest.fixture
def deploy_agent(agent_constructor_args_model_client, container) -> SimpleAgent:
    worker = container.get(Worker)
    agent = SimpleAgent(
        "test_simple_agent",
        system_prompt=f"You are a helpful AI assistant. Your name is {agent_name}. Help the user with their questions as much as possible.",
        subscribe_topics="test_agent.input",
        publish_topic="test_agent.output",
        **agent_constructor_args_model_client,
    )
    worker.add_nodes(agent)
    return agent


@pytest.fixture
def deploy_function_agent(agent_constructor_args_sequential_modes, container) -> Agent:
    worker = container.get(Worker)
    model = container.get(FunctionModel)
    agent = Agent(
        "test_function_agent",
        system_prompt="You are a helpful AI assistant.",
        subscribe_topics="test_function_agent.input",
        publish_topic="test_function_agent.output",
        model_client=model,
        **agent_constructor_args_sequential_modes,
    )
    print(f"\nDeployed agent with sequential_only_mode={agent_constructor_args_sequential_modes}")
    worker.add_nodes(agent)
    return agent


@pytest.fixture
def deploy_structured_agent(agent_constructor_args_model_client, container) -> StructuredAgent:
    worker = container.get(Worker)
    agent = StructuredAgent(
        "test_structured_agent",
        system_prompt=f"You are a helpful AI assistant. Your name is {agent_name}. Help the user with their questions as much as possible.",
        subscribe_topics="test_agent.input",
        publish_topic="test_agent.output",
        final_output_type=Response,
        **agent_constructor_args_model_client,
    )
    worker.add_nodes(agent)
    return agent


@pytest.fixture
def deploy_structured_agent_factory(agent_constructor_args_model_client, container) -> Callable[..., Agent[OutputT]]:
    worker = container.get(Worker)

    def agent_factory(output_type: type[OutputT]) -> Agent[OutputT]:
        agent = Agent[output_type](
            "test_custom_structured_agent",
            system_prompt=f"You are a helpful AI assistant. Your name is {agent_name}. Help the user with their questions as much as possible.",
            subscribe_topics="test_agent.input",
            publish_topic="test_agent.output",
            final_output_type=output_type,
            **agent_constructor_args_model_client,
        )
        worker.add_nodes(agent)
        return agent

    return agent_factory


@pytest.fixture
def deploy_multiple_agent_tools(container) -> list[ToolNodeDef]:
    tools = container.get(list[ToolNodeDef])
    worker = container.get(Worker)
    worker.add_nodes(*tools)
    return tools


@pytest.fixture
def deploy_no_arg_tools(container) -> list[NoArgTool]:
    tools = container.get(list[NoArgTool])
    worker = container.get(Worker)
    worker.add_nodes(*tools)
    return tools


@pytest.fixture
def deploy_multiple_contextual_tools(container) -> list[ToolNodeDef]:
    tools = container.get(list[ContextualTool])
    worker = container.get(Worker)
    worker.add_nodes(*tools)
    return tools


@pytest.fixture
def deploy_caller_id_agent_tool(container) -> ToolNodeDef:
    tool = container.get(ToolNodeDef)
    worker = container.get(Worker)
    worker.add_nodes(tool)
    return tool


@pytest.fixture
def deploy_instructions_agent(container) -> Agent:
    worker = container.get(Worker)
    model = FunctionModel(echo_instructions)
    agent = Agent(
        "test_instructions_agent",
        system_prompt=INSTRUCTIONS_TEST_SYSTEM_PROMPT,
        subscribe_topics="test_instructions_agent.input",
        publish_topic="test_instructions_agent.output",
        model_client=model,  # pyright: ignore[reportArgumentType]
    )
    worker.add_nodes(agent)
    return agent


@pytest.fixture(params=["tool-override", "tool-override-none", "tool-override-empty", None])
def make_overrides_state_factory(request, container) -> Callable[..., OverridesState | None]:
    mode = request.param

    if mode is None:
        return lambda: None

    elif mode == "tool-override":
        tools = container.get(list[ToolNodeDef])
        return lambda: OverridesState(
            override_agent_tools=[
                BaseToolNodeSchema(
                    node_id=t.node_id,
                    subscribe_topics=t.subscribe_topics,
                    publish_topic=t.publish_topic,
                    tool_schema=t.tool_schema,
                )
                for t in tools
            ]
        )

    elif mode == "tool-override-none":
        return lambda: OverridesState(override_agent_tools=None)

    elif mode == "tool-override-empty":
        return lambda: OverridesState(override_agent_tools=list())

    raise ValueError(f"Unknown mode={mode}")


@pytest.fixture(params=["input-args", None])
def make_input_args_factory(request) -> Callable[..., Sequence[Any] | None]:
    mode = request.param

    if mode is None:
        return lambda: None

    elif mode == "input-args":
        return lambda: fake.pylist(allowed_types=[str, int, float, bool])

    raise ValueError(f"Unknown mode={mode}")


@pytest.fixture(params=["empty-stack", "stack"])
def make_internal_workflow_state(request, make_overrides_state_factory, make_input_args_factory) -> WorkflowState:
    mode = request.param
    stack = CallFrameStack()
    if mode == "empty-stack":
        pass
    elif mode == "stack":
        stack_size = fake.pyint(min_value=1, max_value=20)
        for _ in range(stack_size):
            stack.push(
                CallFrame(
                    target_topic=fake.pystr(min_chars=2),
                    callback_topic=fake.pystr(min_chars=2),
                    overrides=make_overrides_state_factory(),
                    input_args=make_input_args_factory(),
                )
            )
    return WorkflowState(call_stack=stack)


@pytest.fixture(params=[dict, str, None])
def make_tool_call_factory(request) -> Callable[..., ToolCallPart]:
    mode = request.param

    def make_tool_call() -> ToolCallPart:
        args = None
        if mode is dict:
            args = fake.pydict(allowed_types=[str, int, float, bool])
        elif mode is str:
            args = fake.pystr(min_chars=1)

        tool_call = ToolCallPart(tool_name=fake.pystr(min_chars=1), args=args)
        return tool_call

    return make_tool_call


@pytest.fixture(params=[ToolReturn, ModelRetry, RetryPromptPart])
def make_tool_result_factory(request) -> Callable[..., ToolCallResult]:
    mode = request.param

    def make_tool_result() -> ToolCallResult:
        tool_result: ToolCallResult
        if mode is ToolReturn:
            tool_result = ToolReturn(return_value=fake.pyobject(random.choice([bool, str, float, int])))
        elif mode is ModelRetry:
            tool_result = ModelRetry(message=fake.sentence())
        elif mode is RetryPromptPart:
            tool_result = RetryPromptPart(tool_name=fake.pystr(min_chars=1), content=fake.sentence())
        else:
            raise ValueError(f"Unknown mode={mode}")
        return tool_result

    return make_tool_result


@pytest.fixture(params=["tool-calls", "no-tool-calls"])
def make_tool_calls(request, make_tool_call_factory) -> dict[str, ToolCallPart]:
    mode = request.param

    if mode == "tool-calls":
        tool_calls_dict = dict()
        tool_calls_size = fake.pyint(min_value=1, max_value=10)
        for _ in range(tool_calls_size):
            tool_call = make_tool_call_factory()
            tool_calls_dict[tool_call.tool_call_id] = tool_call
        return tool_calls_dict
    elif mode == "no-tool-calls":
        return dict()

    raise ValueError(f"Unknown mode={mode}")


@pytest.fixture(params=["tool-results", "no-tool-results"])
def make_tool_results(request, make_tool_result_factory) -> dict[str, ToolCallResult]:
    mode = request.param

    if mode == "tool-results":
        tool_results_dict = dict()
        tool_results_size = fake.pyint(min_value=1, max_value=10)
        for _ in range(tool_results_size):
            tool_result = make_tool_result_factory()
            tool_results_dict[uuid_utils.uuid4().hex] = tool_result
        return tool_results_dict
    elif mode == "no-tool-results":
        return dict()

    raise ValueError(f"Unknown mode={mode}")


@pytest.fixture(params=[fake.text, None])
def make_uncommitted_message(request) -> ModelMessage | None:
    mode = request.param

    if mode is None:
        return None
    elif mode is fake.text:
        return ModelRequest.user_text_prompt(fake.text())


@pytest.fixture
def make_message_history(make_tool_calls) -> list[ModelMessage]:
    message_history = list()
    history_size = fake.pyint(min_value=1, max_value=20, step=2)
    for i in range(history_size):
        if i % 2 == 0:
            message_history.append(ModelRequest.user_text_prompt(fake.text()))
        else:
            message_history.append(ModelResponse(parts=[TextPart(content=fake.text())]))

    message_history.append(ModelResponse(parts=[part for part in make_tool_calls.values()]))
    return message_history


@pytest.fixture(params=[fake.text, None])
def make_temp_instructions(request) -> str | None:
    mode = request.param

    if mode is None:
        return None
    elif mode is fake.text:
        return fake.text()


@pytest.fixture
def make_envelope_state(
    make_overrides_state_factory, make_tool_calls, make_tool_results, make_uncommitted_message, make_message_history, make_temp_instructions
) -> State:
    return State(
        overrides=make_overrides_state_factory(),
        tool_calls=make_tool_calls,
        tool_results=make_tool_results,
        uncommitted_message=make_uncommitted_message,
        message_history=make_message_history,
        temp_instructions=make_temp_instructions,
    )


@pytest.fixture
def make_envelope_deps() -> Deps:
    return Deps(correlation_id=uuid_utils.uuid4().hex, provided_deps=fake.pydict(allowed_types=[str, int, float, bool]))


@pytest.fixture
def make_run_context(make_envelope_state, make_envelope_deps) -> SessionRunContext:
    return SessionRunContext(state=make_envelope_state, deps=make_envelope_deps)


@pytest.fixture
def make_envelope(make_run_context, make_internal_workflow_state) -> Envelope:
    return Envelope(context=make_run_context, internal_workflow_state=make_internal_workflow_state)
