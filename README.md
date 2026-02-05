# Calfkit SDK

The SDK to build AI agents that scale. Calfkit lets you compose agents from independent services—chat, tools, routing—that communicate through events, not API calls. Add capabilities without coordination. Scale each component independently. Stream agent outputs to any downstream system.

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

Define and deploy a tool as an independent service.

```python
import asyncio
from calfkit.nodes import agent_tool
from calfkit.broker import BrokerClient
from calfkit.runners import NodesService

@agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location"""
    return f"It's sunny in {location}"

async def main():
    broker = BrokerClient(bootstrap_servers="localhost:9092")
    service = NodesService(broker)
    service.register_node(get_weather)
    await service.run()

asyncio.run(main())
```

### Deploy the Chat Node

Deploy the LLM chat handler as its own service.

```python
import asyncio
from calfkit.nodes import ChatNode
from calfkit.providers import OpenAIModelClient
from calfkit.broker import BrokerClient
from calfkit.runners import NodesService

async def main():
    broker = BrokerClient(bootstrap_servers="localhost:9092")
    model_client = OpenAIModelClient(model_name="gpt-5-nano")
    chat_node = ChatNode(model_client)
    service = NodesService(broker)
    service.register_node(chat_node)
    await service.run()

asyncio.run(main())
```

### Deploy the Agent Router Node

Deploy the router that orchestrates chat and tools.

```python
import asyncio
from calfkit.nodes import agent_tool, AgentRouterNode, ChatNode
from calfkit.stores import InMemoryMessageHistoryStore
from calfkit.broker import BrokerClient
from calfkit.runners import NodesService

@agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location"""
    return f"It's sunny in {location}"

async def main():
    broker = BrokerClient(bootstrap_servers="localhost:9092")
    router_node = AgentRouterNode(
        chat_node=ChatNode(),
        tool_nodes=[get_weather],
        system_prompt="You are a helpful assistant",
        message_history_store=InMemoryMessageHistoryStore(),
    )
    service = NodesService(broker)
    service.register_node(router_node)
    await service.run()

asyncio.run(main())
```

### Invoke the Agent

Send a request and receive the response.

When invoking an already-deployed agent, use the `RouterServiceClient`. The node is just a configuration object—you don't need to redefine the deployment parameters.

```python
import asyncio
from calfkit.nodes import AgentRouterNode
from calfkit.broker import BrokerClient
from calfkit.runners import RouterServiceClient

async def main():
    broker = BrokerClient(bootstrap_servers="localhost:9092")

    # Thin client - no deployment parameters needed
    router_node = AgentRouterNode()
    client = RouterServiceClient(broker, router_node)

    # Invoke and wait for response
    response = await client.invoke(user_prompt="What's the weather in Tokyo?")
    final_msg = await response.get_final_response()
    print(f"Assistant: {final_msg.text}")

asyncio.run(main())
```

The `RouterServiceClient` handles ephemeral Kafka subscriptions and cleanup automatically. You can also stream intermediate messages:

```python
response = await client.invoke(user_prompt="What's the weather in Tokyo?")

# Stream all messages (tool calls, intermediate responses, etc.)
async for message in response.messages_stream():
    print(message)
```

## License

Apache-2.0
