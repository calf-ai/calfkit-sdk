# 🐮 Calfkit SDK

[![PyPI version](https://img.shields.io/pypi/v/calfkit)](https://pypi.org/project/calfkit/)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/calfkit?period=total&units=INTERNATIONAL_SYSTEM&left_color=GRAY&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/calfkit)
[![Python versions](https://img.shields.io/pypi/pyversions/calfkit)](https://pypi.org/project/calfkit/)
[![License](https://img.shields.io/github/license/calf-ai/calfkit-sdk)](LICENSE)

The SDK to build event-stream, distributed AI agents.

Calfkit lets you compose agents with independent services—chat, tools, routing—that communicate asynchronously. Add agent capabilities without coordination. Scale each component independently. Stream agent outputs to any downstream system. 

Agents should work like real teams, with independent, distinct roles, async communication, and the ability to onboard new teammates or tools without restructuring the whole org. Build AI employees that integrate.

```bash
pip install calfkit
```

<br>

## Why Calfkit?

### The problem

Building agents like traditional web applications—tight coupling and synchronous API calls—creates the same scalability problems that plagued early microservices:

- **Tight coupling:** Changing one tool or agent breaks dependent agents and tools
- **Scaling bottlenecks:** All agents and tools live on one runtime, so everything must scale together
- **Siloed:** Agent communication models are difficult to wire into existing upstream and downstream systems
- **Non-streaming:** Agents do not naturally follow a livestreaming pattern, making data stream consumption difficult to manage

### What Calfkit gives you

Calfkit is a Python SDK that builds event-stream agents out-the-box. You get the benefits of an asynchronous, distributed system without managing the infrastructure yourself.

- **Distributed to the core:** Agents aren't monoliths that just sit on top of the transportation layer. Agents are decomposed into independent services — the agent itself is a deeply distributed system.
- **Independent scaling:** Each service can scale on its own based on demand.
- **Livestream agents by default:** Agents already listen on event streams, so consuming data streams — realtime market feeds, IoT sensors, user activity event streams — is the native pattern, not a bolted-on integration.
- **Compose agents without coupling:** Compose multi-agent teams and workflows by deploying agents on communication channels that are already tapped into the messaging stream. No extra wiring, and no editing existing code — agents don't even need to know about each other.
- **Universal data flow:** Agents plug into any stream — integrate and consume from any upstream data sources and publish to downstream systems like CRMs, warehouses, or even other agents.

<br>

## Quick Start

### Prerequisites

- Python 3.10 or later
- Docker installed and running (for local testing with a Calfkit broker)
- OpenAI API key (or another OpenAI API compliant LLM provider)

### Install

```bash
pip install calfkit
```

### ☁️ Calfkit Cloud (Coming Soon)

Skip the infrastructure. Calfkit Cloud is a fully-managed Kafka service built for Calfkit AI agents and multi-agent teams. No server infrastructure to self-host or maintain, with built-in observability and agent-event tracing.

Coming soon. [Fill out the interest form →](https://forms.gle/Rk61GmHyJzequEPm8)

### Start Local Calfkit Server (Requires Docker)

Calfkit uses Kafka as the event broker. Run the following command to clone the [calfkit-broker](https://github.com/calf-ai/calfkit-broker) repo and start a local Kafka broker container:

```shell
git clone https://github.com/calf-ai/calfkit-broker && cd calfkit-broker && make dev-up
```

Once the broker is ready, open a new terminal tab to continue with the quickstart.

### Define and Deploy the Tool Node

Define and deploy a tool as an independent service. Tools are not owned by or coupled to any specific agent—once deployed, any agent in your system can discover and invoke the tool. Deploy once, use everywhere.

```python
# weather_tool.py
import asyncio
from calfkit.nodes import agent_tool
from calfkit.broker import BrokerClient
from calfkit.runners import NodesService

# Example tool definition
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

Run the file to deploy the tool service:

```shell
python weather_tool.py
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

Set your OpenAI API key:

```shell
export OPENAI_API_KEY=sk-...
```

Run the file to deploy the chat service:

```shell
python chat_service.py
```

### Deploy the Agent Router Node

Deploy the agent router that orchestrates chat, tools, and conversation-level memory.

```python
# agent_router_service.py
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

Run the file to deploy the agent router service:

```shell
python agent_router_service.py
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

    # Request and wait for response
    response = await client.request(user_prompt="What's the weather in Tokyo?")
    final_msg = await response.get_final_response()
    print(f"Assistant: {final_msg.text}")

if __name__ == "__main__":
    asyncio.run(main())
```

Run the file to invoke the agent:

```shell
python client.py
```

The `RouterServiceClient` handles ephemeral Kafka communication and cleanup automatically. You can also stream intermediate messages:

```python
response = await client.request(user_prompt="What's the weather in Tokyo?")

# Stream all messages (tool calls, intermediate responses, etc.)
async for message in response.messages_stream():
    print(message)
```

### Runtime Configuration (Optional)

Clients can override the system prompt and restrict available tools at invocation time without redeploying:

```python
from weather_tool import get_weather

# Client with runtime patches
router_node = AgentRouterNode(
    system_prompt="You are an assistant with no tools :(",  # Override the deployed system prompt
    tool_nodes=[],  # Patch in any subset of the deployed agent's set of tools
)
client = RouterServiceClient(broker_client, router_node)
response = await client.request(user_prompt="Weather in Tokyo?")
```

This lets different clients customize agent behavior per-request. Tool patching is currently limited to subsets of tools configured in the deployed router.

<br>

## Documentation

Full documentation is coming soon. In the meantime, this README serves as the primary reference for getting started with Calfkit.

<br>

## Contact

[![X](https://img.shields.io/badge/Follow-black?logo=x)](https://x.com/ryanyuhater)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-blue?logo=linkedin)](https://www.linkedin.com/company/calfkit)

<br>

## Support

If you found this project interesting or useful, please consider:
- ⭐ Starring the repository — it helps others discover it!
- 🐛 [Reporting issues](https://github.com/calf-ai/calfkit-sdk/issues)
- 🔀 Submitting PRs

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
