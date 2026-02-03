# Calf SDK

Build event-driven and distributed AI agents with loosely coupled tools and chats that can be deployed as separate runtimes, without the hassle of setting up Kafka.

## Overview

Calf SDK is a Python library for building event-driven AI agents and workflows on Kafka, but without the hassle of setting up on Kafka. Each component (chat, tools, router, etc.) is capable of running as its own service, communicating through Kafka.

## Features

- **Decoupled nodes** – Chat, tools, and router each run as modular, independently deployable Kafka workers
- **Deploy and scale anywhere** – Develop, run, and scale nodes independently
- **Connect to anything** – Since event-driven systems are inherently distributed, deploying and connecting new tool capabilities is trivial
- **A cleaner development lifecycle** – Since event-driven systems are inherently loosely coupled, teams can continously and concurrently develop and deploy on different parts of their backend

## Quick Start

### Prerequisites

```bash
# Kafka (e.g., using Docker)
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
