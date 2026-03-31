# 🐮 Calfkit SDK

[![PyPI version](https://img.shields.io/pypi/v/calfkit)](https://pypi.org/project/calfkit/)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/calfkit?period=total&units=INTERNATIONAL_SYSTEM&left_color=GRAY&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/calfkit)
[![Python versions](https://img.shields.io/pypi/pyversions/calfkit)](https://pypi.org/project/calfkit/)
[![License](https://img.shields.io/github/license/calf-ai/calfkit-sdk)](LICENSE)

The SDK to build AI agents as composable, orchestratable microservices.

Calfkit lets you compose agents with independent services—agents, tools, workflows—that communicate asynchronously. Add agent capabilities without coordination. Scale each component independently. Stream agent outputs to any downstream listener. 

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

### What Calfkit provides

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
- Docker installed and running (for testing with a local Calfkit broker)
- LLM Provider API key

<br>

### 1. Install

```bash
pip install calfkit
```

<br>

### 2. Start a Calfkit Broker

<details>
<summary><b>Option A: Local Broker (Requires Docker)</b></summary>
<br>

Calfkit uses Kafka as the event broker. Run the following command to clone the [calfkit-broker](https://github.com/calf-ai/calfkit-broker) repo and start a local Kafka broker container:

```shell
git clone https://github.com/calf-ai/calfkit-broker && cd calfkit-broker && make dev-up
```

Once the broker is ready, open a new terminal tab to continue with the quickstart.

</details>

<details>
<summary><b>Option B: ☁️ Calfkit Cloud (In Beta)</b></summary>
<br>

Skip the infrastructure. Calfkit Cloud is a fully-managed broker service built for Calfkit AI agents and multi-agent teams. No server infrastructure to self-host or maintain, with built-in observability and agent-event tracing. 

You will be provided a Calfkit broker API to deploy your agents instead of setting up and maintaining a broker locally.

[Sign up for access →](https://forms.gle/Rk61GmHyJzequEPm8)

</details>

<br>

### 3. Define and Deploy the Tool Node

Define and deploy a tool as an independent service. Tools are not owned by or coupled to any specific agent—once deployed, any agent in your system can discover and invoke the tool. Deploy once, use everywhere.

```python
# weather_tool.py
import asyncio
from calfkit.nodes import agent_tool
from calfkit.client import Client
from calfkit.worker import Worker

# Define a tool — the @agent_tool decorator turns any function into a deployable tool node
@agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location"""
    return f"It's sunny in {location}"

async def main():
    client = Client.connect("localhost:9092")  # Connect to Kafka broker
    worker = Worker(client, nodes=[get_weather])  # Initialize a worker with the tool node
    await worker.run()  # (Blocking call) Deploy the service to start serving traffic

if __name__ == "__main__":
    asyncio.run(main())
```

Run the file to deploy the tool service:

```shell
python weather_tool.py
```

<br>

### 4. Deploy the Agent Node

Deploy the agent as its own service. The `Agent` handles LLM chat, tool orchestration, and conversation management in a single node. Import the tool definition to register it with the agent—the tool definition is reusable and does not couple the agent to the tool's deployment.

```python
# agent_service.py
import asyncio
from calfkit.nodes import Agent
from calfkit.providers import OpenAIModelClient
from calfkit.client import Client
from calfkit.worker import Worker
from weather_tool import get_weather  # Import the tool definition (reusable)

agent = Agent(
    "weather_agent",
    system_prompt="You are a helpful assistant.",
    subscribe_topics="agent.input",
    publish_topic="agent.output",
    model_client=OpenAIModelClient(model_name="gpt-5-nano"),
    tools=[get_weather],  # Register tool definitions with the agent
)

async def main():
    client = Client.connect("localhost:9092")  # Connect to Kafka broker
    worker = Worker(client, nodes=[agent])  # Initialize a worker with the agent node
    await worker.run()  # (Blocking call) Deploy the service to start serving traffic

if __name__ == "__main__":
    asyncio.run(main())
```

Set your OpenAI API key:

```shell
export OPENAI_API_KEY=sk-...
```

Run the file to deploy the agent service:

```shell
python agent_service.py
```

<br>

### 5. Invoke the Agent

Send a request and receive the response. The `Client` handles broker communication and request correlation automatically.

```python
# invoke.py
import asyncio
from calfkit.client import Client

async def main():
    client = Client.connect("localhost:9092")  # Connect to Kafka broker

    # Send a request and await the response
    result = await client.execute_node(
        "What's the weather in Tokyo?",
        "agent.input",  # The topic the agent subscribes to
    )
    print(f"Assistant: {result.output}")

if __name__ == "__main__":
    asyncio.run(main())
```

Run the file to invoke the agent:

```shell
python invoke.py
```

<br>

### Structured Outputs (Optional)

Agents can be deployed with a `final_output_type` to enforce structured output from the LLM. The output is type-safe and deserialized automatically on the client side.

```python
from dataclasses import dataclass
from calfkit.nodes import Agent
from calfkit.providers import OpenAIModelClient

@dataclass
class WeatherReport:
    location: str
    summary: str

agent = Agent(
    "weather_agent",
    system_prompt="You are a helpful assistant.",
    subscribe_topics="agent.input",
    publish_topic="agent.output",
    model_client=OpenAIModelClient(model_name="gpt-5-nano"),
    final_output_type=WeatherReport,  # Enforce structured output
)
```

When invoking, pass the matching `output_type` to deserialize the response:

```python
result = await client.execute_node(
    "What's the weather in Tokyo?",
    "agent.input",
    output_type=WeatherReport,
)
print(result.output.location)  # "Tokyo"
print(result.output.summary)   # "It's sunny in Tokyo"
```

<br>

### Client-Side Features (Optional)

The `Client` supports multi-turn conversations, runtime dependency injection, and temporary instruction overrides—all without redeploying the agent.

**Multi-turn conversations** — pass the message history from a previous result to maintain context:

```python
result = await client.execute_node("What's the weather in Tokyo?", "agent.input")

# Continue the conversation with full context
result = await client.execute_node(
    "How about in Osaka?",
    "agent.input",
    message_history=result.message_history,
)
```

**Runtime dependency injection** — pass runtime data to tools via the `deps` parameter:

```python
result = await client.execute_node(
    "What's my phone number?",
    "agent.input",
    deps={"user_id": "usr_123"},  # Available to tools via ctx.deps.provided_deps
)
```

**Temporary instructions** — temporarily add system-level instructions scoped per request:

```python
result = await client.execute_node(
    "What's the weather in Tokyo?",
    "agent.input",
    temp_instructions="Always respond in Japanese.",
)
```

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
