# Calf SDK

Event-driven AI agents that scale like microservices. Build loosely-coupled, distributed agent systems where tools, chats, and workflows each run as independent services—communicating through events.

## Why Event-Driven Agents?

AI agents are the new apps. But building them like traditional web applications—with tight coupling and direct API calls—creates the same scalability problems that plagued early microservices.

When agents connect through APIs and RPC:
- **Tight coupling** — Changing one tool breaks dependent agents
- **Scaling bottlenecks** — Everything must scale together
- **Siloed outputs** — Agent responses stay trapped in your AI layer

**Event-driven architecture solves this.** Agents communicate through events, not direct calls. Each component runs independently, scales horizontally, and outputs can flow anywhere—CRMs, data warehouses, analytics platforms, other agents.

## What Calf Gives You

Calf is a Python SDK that makes event-driven agents simple. You get the benefits of a distributed system—loose coupling, horizontal scalability, durability—without the complexity of managing Kafka infrastructure yourself.

| Benefit | What It Means for You |
|---------|----------------------|
| **Add tools without touching existing code** | Deploy new capabilities as independent services that other agents discover automatically |
| **Scale what you need, when you need it** | Chat handling, tool execution, and routing each scale independently based on demand |
| **Nothing gets lost** | Event persistence ensures reliable message delivery—even during failures or restarts |
| **Real-time responses** | Low-latency event processing enables agents to react instantly to incoming data |
| **Team independence** | Different teams can develop and deploy chat, tools, and routing concurrently without coordination overhead |
| **Outputs go everywhere** | Agent results flow seamlessly to CRMs, CDPs, data warehouses—not just back to your application |

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

### Create and Run an Agent

```python
import asyncio
from calf.nodes import agent_tool, AgentRouterNode, ChatNode
from calf.providers import OpenAIModelClient
from calf.stores import InMemoryMessageHistoryStore
from calf.broker import Broker
from calf.runners import ChatRunner, ToolRunner, AgentRouterRunner

# 1. Define a tool
@agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location"""
    return f"It's sunny in {location}"

# 2. Setup broker and nodes
async def main():
    broker = Broker(bootstrap_servers="localhost:9092")
    model_client = OpenAIModelClient(model_name="gpt-4o")

    # Deploy chat node
    chat_node = ChatNode(model_client)
    ChatRunner(chat_node).register_on(broker)

    # Deploy tool node
    ToolRunner(get_weather).register_on(broker)

    # Deploy router node
    router_node = AgentRouterNode(
        chat_node=ChatNode(model_client),
        tool_nodes=[get_weather],
        system_prompt="You are a helpful assistant",
        message_history_store=InMemoryMessageHistoryStore(),
    )
    AgentRouterRunner(router_node).register_on(broker)

    # Run broker and invoke
    await broker.run_app()
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
