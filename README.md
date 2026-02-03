# Calfkit SDK

Build AI agents that scale. Calfkit lets you compose agents from independent services—chat, tools, routing—that communicate through events, not API calls. Add capabilities without coordination. Scale each component independently. Stream agent outputs to any downstream system.

## Why Event-Driven Agents?

Building agents like traditional web applications—with tight coupling and direct API calls—creates the same scalability problems that plagued early microservices.

When agents connect through APIs and RPC:
- **Tight coupling** — Changing one tool breaks dependent agents
- **Scaling bottlenecks** — Everything must scale together
- **Siloed outputs** — Agent responses stay trapped in your AI layer

Event-driven architecture provides the solution. Instead of direct API calls between components, agents interact through asynchronous event streams. Each component runs independently, scales horizontally, and outputs can flow anywhere—CRMs, data warehouses, analytics platforms, other agents.

## Why use Calfkit?

Calfkit is a Python SDK that makes event-driven agents simple. You get the benefits of a distributed system—loose coupling, horizontal scalability, durability—without the complexity of managing Kafka infrastructure yourself.

- **Distributed agents out of the box** — Build event-driven, multi-service agents without writing orchestration code or managing infrastructure
- **Add tools without touching existing code** — Deploy new capabilities as independent services that other agents discover automatically
- **Scale what you need, when you need it** — Chat handling, tool execution, and routing each scale independently based on demand
- **Nothing gets lost** — Event persistence ensures reliable message delivery—even during failures or restarts
- **Real-time responses** — Low-latency event processing enables agents to react instantly to incoming data
- **Team independence** — Different teams can develop and deploy chat, tools, and routing concurrently without coordination overhead
- **Universal data flow** — Decoupling enables data to flow freely in both directions. Downstream, agent outputs integrate with any system (CRMs, CDPs, warehouses). Upstream, tools wrap data sources and deploy independently—no coordination needed.

## Quick Start

### Prerequisites

```bash
# Kafka (single command via Docker)
docker run -d -p 9092:9092 apache/kafka:latest

# Python 3.10+
python --version

# OpenAI API key
export OPENAI_API_KEY=sk-...
```

### Install

```bash
pip install git+https://github.com/calf-ai/calf-sdk.git
```

### Deploy the Tool Node

`tool_service.py` — Define and deploy a tool as an independent service.

```python
import asyncio
from calfkit.nodes import agent_tool
from calfkit.broker import Broker
from calfkit.runners import ToolRunner

@agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location"""
    return f"It's sunny in {location}"

async def main():
    broker = Broker(bootstrap_servers="localhost:9092")
    ToolRunner(get_weather).register_on(broker)
    await broker.run_app()

asyncio.run(main())
```

### Deploy the Chat Node

`chat_service.py` — Deploy the LLM chat handler as its own service.

```python
import asyncio
from calfkit.nodes import ChatNode
from calfkit.providers import OpenAIModelClient
from calfkit.broker import Broker
from calfkit.runners import ChatRunner

async def main():
    broker = Broker(bootstrap_servers="localhost:9092")
    model_client = OpenAIModelClient(model_name="gpt-5-nano")
    chat_node = ChatNode(model_client)
    ChatRunner(chat_node).register_on(broker)
    await broker.run_app()

asyncio.run(main())
```

### Deploy the Agent Router Node

`router_service.py` — Deploy the router that orchestrates chat and tools.

```python
import asyncio
from calfkit.nodes import agent_tool, AgentRouterNode, ChatNode
from calfkit.providers import OpenAIModelClient
from calfkit.stores import InMemoryMessageHistoryStore
from calfkit.broker import Broker
from calfkit.runners import AgentRouterRunner

@agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location"""
    return f"It's sunny in {location}"

async def main():
    broker = Broker(bootstrap_servers="localhost:9092")
    model_client = OpenAIModelClient(model_name="gpt-4o")
    router_node = AgentRouterNode(
        chat_node=ChatNode(model_client),
        tool_nodes=[get_weather],
        system_prompt="You are a helpful assistant",
        message_history_store=InMemoryMessageHistoryStore(),
    )
    AgentRouterRunner(router_node).register_on(broker)
    await broker.run_app()

asyncio.run(main())
```

### Invoke the Agent

`client.py` — Send a request and receive the response.

```python
import asyncio
from calfkit.nodes import agent_tool, AgentRouterNode, ChatNode
from calfkit.providers import OpenAIModelClient
from calfkit.stores import InMemoryMessageHistoryStore
from calfkit.broker import Broker

@agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location"""
    return f"It's sunny in {location}"

async def main():
    broker = Broker(bootstrap_servers="localhost:9092")
    model_client = OpenAIModelClient(model_name="gpt-5-nano")

    router_node = AgentRouterNode(
        chat_node=ChatNode(model_client),
        tool_nodes=[get_weather],
        system_prompt="You are a helpful assistant",
        message_history_store=InMemoryMessageHistoryStore(),
    )

    correlation_id = await router_node.invoke(
        user_prompt="What's the weather in Tokyo?",
        broker=broker,
        final_response_topic="final_response",
    )
    print(f"Request started: {correlation_id}")

asyncio.run(main())
```

## License

Apache-2.0
