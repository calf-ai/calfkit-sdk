# 🐮 Calfkit SDK

[![PyPI version](https://img.shields.io/pypi/v/calfkit)](https://pypi.org/project/calfkit/)
[![License](https://img.shields.io/github/license/calf-ai/calfkit-sdk)](LICENSE)

The SDK to build AI agents that scale. Calfkit lets you compose agents with independent services—chat, tools, routing—that communicate through events, not API calls. Add capabilities without coordination. Scale each component independently. Stream agent outputs to any downstream system.

## Why Event-Driven?

Building agents like traditional web applications, with tight coupling and synchronous API calls, creates the same scalability problems that plagued early microservices.

When agents connect through APIs and RPC:
- **Tight coupling** — Changing one tool breaks dependent agents
- **Scaling bottlenecks** — Everything must scale together
- **Siloed outputs** — Agent responses stay trapped in your AI layer

Event-driven architecture provides the solution. Instead of direct API calls between components, agents interact through asynchronous streams. Each component runs independently, scales horizontally, and outputs can flow anywhere—CRMs, data warehouses, analytics platforms, other agents, or even more tools.

## Why Use Calfkit?

Calfkit is a Python SDK that makes event-driven agents simple. You get all the benefits of a asynchronous, distributed system--loose coupling, horizontal scalability, durability--without the complexity of managing Kafka infrastructure and orchestration yourself.

- **Distributed agents out of the box** — Build event-driven, multi-service agents without writing orchestration code or managing infrastructure
- **Add agent capabilities without touching existing code** — Deploy new tool capabilities as independent services that agents can dynamically discover, no need to touch your agent code
- **Scale what you need, when you need it** — Chat handling, tool execution, and routing each scale independently based on demand
- **Nothing gets lost** — Event persistence ensures reliable message delivery and traceability, even during service failures or restarts
- **High throughput under pressure** — Asynchronous communication decouples requests from processing, so Calfkit agents work through bursty traffic reliably, maximizing throughput
- **Real-time responses** — Low-latency event processing enables agents to react instantly to incoming data
- **Team independence** — Different teams can develop and deploy chat, tools, and routing concurrently without cross-team coordination overhead
- **Universal data flow** — Decoupling enables data to flow freely in both directions. 
    - Downstream, agent outputs can be streamed to any system (CRMs, customer data platforms, warehouses, or even another AI workflow).
    - Upstream, tools can wrap any data sources and deploy independently, no coordination needed.

## Quick Start

### Prerequisites
Calfkit requires **Python 3.10 or later**
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
pip install calfkit
```

### Deploy the Tool Node

Define and deploy a tool as an independent service.

```python
# weather_tool.py
import asyncio
from calfkit.nodes import agent_tool
from calfkit.broker import BrokerClient
from calfkit.runners import NodesService

@agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location"""
    return f"It's sunny in {location}"

async def main():
    broker_client = BrokerClient(bootstrap_servers="localhost:9092") # Connect to Kafka broker
    service = NodesService(broker_client) # Initialize a service instance
    service.register_node(get_weather) # Register the tool node in the service
    await service.run() # (Blocking call) Deploy the service to start serving traffic

if __name__ == "__main__":
    asyncio.run(main())
```

### Deploy the Chat Node

Deploy the LLM chat node as its own service.

```python
# chat_service.py
import asyncio
from calfkit.nodes import ChatNode
from calfkit.providers import OpenAIModelClient
from calfkit.broker import BrokerClient
from calfkit.runners import NodesService

async def main():
    broker_client = BrokerClient(bootstrap_servers="localhost:9092") # Connect to Kafka broker
    model_client = OpenAIModelClient(model_name="gpt-5-nano")
    chat_node = ChatNode(model_client) # Inject a model client into the chat node definition so the chat deployment can perform LLM calls
    service = NodesService(broker_client) # Initialize a service instance
    service.register_node(chat_node) # Register the chat node in the service
    await service.run() # (Blocking call) Deploy the service to start serving traffic

if __name__ == "__main__":
    asyncio.run(main())
```

### Deploy the Agent Router Node

Deploy the agent router that orchestrates chat, tools, and conversation-level memory.

```python
# router_service.py
import asyncio
from calfkit.nodes import agent_tool, AgentRouterNode, ChatNode
from calfkit.stores import InMemoryMessageHistoryStore
from calfkit.broker import BrokerClient
from calfkit.runners import NodesService
from weather_tool import get_weather # Import the tool, the tool definition is reusable

async def main():
    broker_client = BrokerClient(bootstrap_servers="localhost:9092") # Connect to Kafka broker
    router_node = AgentRouterNode(
        chat_node=ChatNode(), # Provide the chat node definition for the router to orchestrate the nodes
        tool_nodes=[get_weather],
        system_prompt="You are a helpful assistant",
        message_history_store=InMemoryMessageHistoryStore(), # Stores messages in-memory in the deployment runtime
    )
    service = NodesService(broker_client) # Initialize a service instance
    service.register_node(router_node) # Register the router node in the service
    await service.run() # (Blocking call) Deploy the service to start serving traffic

if __name__ == "__main__":
    asyncio.run(main())
```

### Invoke the Agent

Send a request and receive the response.

When invoking an already-deployed agent, use the `RouterServiceClient`. The node is just a configuration object, so you don't need to redefine the deployment parameters.

```python
# client.py
import asyncio
from calfkit.nodes import AgentRouterNode
from calfkit.broker import BrokerClient
from calfkit.runners import RouterServiceClient

async def main():
    broker_client = BrokerClient(bootstrap_servers="localhost:9092") # Connect to Kafka broker

    # Thin client - no deployment parameters needed
    router_node = AgentRouterNode()
    client = RouterServiceClient(broker_client, router_node)

    # Invoke and wait for response
    response = await client.invoke(user_prompt="What's the weather in Tokyo?")
    final_msg = await response.get_final_response()
    print(f"Assistant: {final_msg.text}")

if __name__ == "__main__":
    asyncio.run(main())
```

The `RouterServiceClient` handles ephemeral Kafka communication and cleanup automatically. You can also stream intermediate messages:

```python
response = await client.invoke(user_prompt="What's the weather in Tokyo?")

# Stream all messages (tool calls, intermediate responses, etc.)
async for message in response.messages_stream():
    print(message)
```

## License

Apache-2.0
