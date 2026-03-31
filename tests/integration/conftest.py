from collections.abc import Callable

import pytest
from dishka import make_container

from calfkit._types import OutputT
from calfkit.nodes import Agent, ToolNodeDef
from calfkit.worker import Worker
from tests.integration.providers import (
    AgentProvider,
    ClientProvider,
    SimpleAgent,
    StructuredAgent,
    WorkerProvider,
)


@pytest.fixture
def container():
    c = make_container(WorkerProvider(), ClientProvider(), AgentProvider())
    yield c
    c.close()


@pytest.fixture
def deploy_agent(container) -> SimpleAgent:
    worker = container.get(Worker)
    agent = container.get(SimpleAgent)
    worker.add_nodes(agent)
    return agent


@pytest.fixture
def deploy_structured_agent(container) -> StructuredAgent:
    worker = container.get(Worker)
    agent = container.get(StructuredAgent)
    worker.add_nodes(agent)
    return agent


@pytest.fixture
def deploy_structured_agent_factory(container) -> Callable[..., Agent[OutputT]]:
    factory = container.get(Callable)
    return factory


@pytest.fixture
def deploy_multiple_agent_tools(container) -> list[ToolNodeDef]:
    tools = container.get(list[ToolNodeDef])
    worker = container.get(Worker)
    worker.add_nodes(*tools)
    return tools


@pytest.fixture
def deploy_caller_id_agent_tool(container) -> ToolNodeDef:
    tool = container.get(ToolNodeDef)
    worker = container.get(Worker)
    worker.add_nodes(tool)
    return tool
