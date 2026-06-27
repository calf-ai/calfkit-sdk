# 🐮 Calfkit SDK

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/calf-ai/calfkit-sdk)
[![PyPI version](https://img.shields.io/pypi/v/calfkit)](https://pypi.org/project/calfkit/)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/calfkit?period=total&units=INTERNATIONAL_SYSTEM&left_color=GRAY&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/calfkit)
[![Python versions](https://img.shields.io/pypi/pyversions/calfkit)](https://pypi.org/project/calfkit/)
[![codecov](https://codecov.io/gh/calf-ai/calfkit-sdk/graph/badge.svg?token=ZUP383PSK7)](https://codecov.io/gh/calf-ai/calfkit-sdk)
[![License](https://img.shields.io/github/license/calf-ai/calfkit-sdk)](LICENSE)

Build AI agents as decentralized, event-driven microservices.

Agents, tools, and consumers are independently deployable nodes that dynamically route messages to each other and communicate asynchronously. Build and compose AI workflows as a distributed network of interconnected AI agents.

```console
$ pip install calfkit
```

> **Status: Alpha (pre-1.0).** Calfkit is under active development and the public API can change between versions. Pin a version and check the [CHANGELOG](CHANGELOG.md) before big version bumps.

<br>

## Table of Contents

- [Why Calfkit?](#why-calfkit)
- [Install](#install)
  - [Prerequisites](#prerequisites)
  - [Start a Calfkit broker](#start-a-calfkit-broker)
- [Usage](#usage)
  - [1. Define and deploy the tool node](#1-define-and-deploy-the-tool-node)
  - [2. Deploy the agent node](#2-deploy-the-agent-node)
  - [3. Invoke the agent](#3-invoke-the-agent)
  - [Next steps](#next-steps)
    - [Structured outputs](#structured-outputs)
    - [Deploying to production](#deploying-to-production)
- [Documentation](#documentation)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [Contact](#contact)
- [License](#license)

<br>

## Why Calfkit?

Calfkit shifts the paradigm of agent development from orchestration (centralized control) to choreography (decoupled interactions). Building agent teams with traditional agent frameworks rely on tightly coupled, hard-coded interactions. Calfkit allows you to build agents into an interconnected mesh right out of the box.

- **Distributed by default:** Every agent, tool, and consumer is its own node in a connected network — so agents and tools are a distributed system you deploy and scale piece by piece, not a monolith.

- **Stream-native:** Agents communicate on event streams, so consuming live data — market feeds, IoT sensors, user activity — is the default, not a bolted-on integration. Plug into any upstream source and publish to any downstream system: CRMs, warehouses, even other agents.

- **Composable without coupling:** Build multi-agent teams simply by dropping in agents on shared topics — no extra wiring, no edits to existing code.

<br>

## Install

```console
$ pip install calfkit
```

### Prerequisites

- Python 3.10 or later
- Docker installed and running (for running with a local Calfkit broker)
- An LLM provider API key

### Start a Calfkit broker

Every Calfkit deployment needs a running broker. 

<details>
<summary><b>Option A: Local Broker (Requires Docker)</b></summary>
<br>

Clone the [calfkit-broker](https://github.com/calf-ai/calfkit-broker) repo and start a local broker:

```console
$ git clone https://github.com/calf-ai/calfkit-broker && cd calfkit-broker && make dev-up
```

Once it's ready, open a new terminal to continue.

</details>

<details>
<summary><b>Option B: ☁️ Calfkit Cloud (In Beta)</b></summary>
<br>

Skip the infrastructure. Calfkit Cloud is a fully-managed broker for Calfkit agents — nothing to self-host, with built-in observability and agent-event tracing. You deploy against a hosted broker endpoint instead of running one locally.

[Sign up for access →](https://forms.gle/Rk61GmHyJzequEPm8)

</details>

<br>

## Usage

A complete, runnable version of this walkthrough lives in [`examples/quickstart/`](examples/quickstart/).

### 1. Define and deploy the tool node

A tool is an independent service, not owned by any agent. Define one with `@agent_tool`; once deployed, any agent can invoke it. Deploy once, use everywhere.

```python
# weather_tool.py
from calfkit.nodes import agent_tool

# Define a tool — the @agent_tool decorator turns any function into a deployable tool node
@agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location"""
    return f"It's sunny in {location}"
```

`ck run` points at a `module:attr` target and starts the tool for you — no extra wiring required.

```console
$ ck run weather_tool:get_weather
```

### 2. Deploy the agent node

Provide the `Agent` with a reference to the tool using the same tool definition. This agent listens to the `weather_agent.input` topic.

```python
# agent_service.py
from calfkit.nodes import Agent
from calfkit.providers import OpenAIResponsesModelClient
from weather_tool import get_weather  # Import the tool definition (reusable)

agent = Agent(
    "weather_agent",
    system_prompt="You are a helpful assistant.",
    subscribe_topics="weather_agent.input",
    publish_topic="weather_agent.output",  # Stream every agent output here. Other agents and consumers can listen into this stream
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-nano"),
    tools=[get_weather],  # Register tool definitions with the agent
)
```

Set your OpenAI API key:

```console
$ export OPENAI_API_KEY=sk-...
```

Start the agent:

```console
$ ck run agent_service:agent
```

### 3. Invoke the agent

Send a message to the agent.

```python
# execute.py
import asyncio
from calfkit.client import Client

async def main():
    client = Client.connect("localhost:9092")  # Connect to the broker

    # Send a request and await the response
    result = await client.execute(
        "What's the weather in Tokyo?",
        "weather_agent.input",  # The topic the agent is listening to
    )
    print(f"Assistant: {result.output}")

if __name__ == "__main__":
    asyncio.run(main())
```

Run the file to invoke the agent:

```console
$ python execute.py
```

### Next steps

You've completed the quickstart — a tool and an agent deployed as separate services and invoked over Kafka. The rest of this section covers common things to do next.

#### Structured outputs

Set `final_output_type` to enforce structured output from the LLM — it's deserialized into your type automatically on the client side.

```python
from dataclasses import dataclass
from calfkit.nodes import Agent
from calfkit.providers import OpenAIResponsesModelClient

@dataclass
class WeatherReport:
    location: str
    summary: str

agent = Agent(
    "weather_agent",
    system_prompt="You are a helpful assistant.",
    subscribe_topics="weather_agent.input",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-nano"),
    final_output_type=WeatherReport,  # Enforce structured output
)
```

When invoking, pass the matching `output_type` to deserialize the response:

```python
result = await client.execute(
    "What's the weather in Tokyo?",
    "weather_agent.input",
    output_type=WeatherReport,
)
print(result.output.location)  # "Tokyo"
print(result.output.summary)   # "It's sunny in Tokyo"
```

#### Deploying to production

For production, you can deploy each node with an explicit `Worker`, keeping startup, scaling, and lifecycle management under your control:

```python
# serve_tool.py — deploy the tool as its own service
import asyncio
from calfkit.client import Client
from calfkit.worker import Worker
from weather_tool import get_weather

async def main():
    client = Client.connect("localhost:9092")  # Connect to Kafka broker
    worker = Worker(client, nodes=[get_weather])  # One service per node
    await worker.run()  # (Blocking) serve until stopped

if __name__ == "__main__":
    asyncio.run(main())
```

```console
$ python serve_tool.py
```

<br>

## Documentation

In-repo documentation lives under [`docs/`](docs/).

**New to building a system of agents?** Start with the tutorial **[Build a multi-agent support desk](docs/multi-agent-support-desk.md)** — build and run three agents that discover each other and collaborate by messaging and handoff.

**How-to guides** — goal-oriented walkthroughs:

- **[How to call nodes from a client](docs/client-features.md)** — the three invocation patterns (`execute` / `start` / `send`), multi-turn conversations, runtime dependency injection (`deps`), temporary instructions, fire-and-forget, and bounding reply memory with `reply_ttl`.
- **[How to tap a topic with a consumer node](docs/consumer-nodes.md)** — terminal sinks that run arbitrary Python against every event on a topic; tap an agent's `publish_topic` to log, persist, or fan out.
- **[How to guard and transform node invocations](docs/policy-seams.md)** — guard an invocation with `before_node` (transform the input, short-circuit the body, or raise to block), and validate or reshape its output with `after_node`.
- **[How to handle errors and faults](docs/error-handling.md)** — recover from a failed node or callee with `on_node_error` / `on_callee_error`, mint typed faults with `NodeFaultError`, and inspect an `ErrorReport`.
- **[How to let agents discover and use tools at runtime](docs/tool-discovery.md)** — reference deployed function tool nodes by name (or every live one with `discover=True`) with `Tools`; agents discover their schemas at runtime, so an agent's deployment never imports the tool's code.
- **[How to give agents MCP tools](docs/mcp-tool-discovery.md)** — deploy an `MCPToolboxNode` fronting an MCP server and pass it to agents like a tool node; tools are discovered and kept fresh across processes automatically.
- **[How to let agents find and reach each other at runtime](docs/agent-peers.md)** — agents discover each other by name (no hardcoded addresses) and collaborate two ways: consult a peer and keep control (`Messaging`), or transfer control to a specialist (`Handoff`).
- **[Worker lifecycle & embedding](docs/worker-lifecycle.md)** — open long-lived resources at startup and close them on shutdown, publish presence events, and run with `run()`, the embeddable `start()`/`stop()`, or `async with worker:`.

**Reference:**

- **[API reference](docs/api.md)** — the public surface re-exported from the top-level `calfkit` package, with the key entry-point signatures.
- **[CLI reference](docs/cli.md)** — the `ck run` and `ck topics` commands.
- **[Topic provisioning](docs/topic-provisioning.md)** — the experimental, opt-in topic-creation helper for dev/CI.

**Design & background:**

- **[Design documents](docs/designs/)** — accepted and proposed designs.

<br>

## Roadmap

See [ROADMAP.md](ROADMAP.md) for what's under consideration — listing there isn't a commitment.

<br>

## Contributing

Issues and pull requests are welcome. Please [open an issue](https://github.com/calf-ai/calfkit-sdk/issues) to discuss substantial changes before sending a PR.

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, the quality gates (`make fix` / `make check` / `make test`), PR conventions, and how to write and run tests — including the real-broker integration lane.

<br>

## Contact

[![X](https://img.shields.io/badge/Follow-black?logo=x)](https://x.com/ryanyuhater)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-blue?logo=linkedin)](https://www.linkedin.com/company/calfkit)

If you found this project interesting or useful, please consider:
- ⭐ Starring the repository — it helps others discover it!
- 🐛 [Reporting issues](https://github.com/calf-ai/calfkit-sdk/issues)
- 🔀 Submitting PRs

<br>

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
