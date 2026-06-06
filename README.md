# 🐮 Calfkit SDK

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/calf-ai/calfkit-sdk)
[![PyPI version](https://img.shields.io/pypi/v/calfkit)](https://pypi.org/project/calfkit/)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/calfkit?period=total&units=INTERNATIONAL_SYSTEM&left_color=GRAY&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/calfkit)
[![Python versions](https://img.shields.io/pypi/pyversions/calfkit)](https://pypi.org/project/calfkit/)
[![codecov](https://codecov.io/gh/calf-ai/calfkit-sdk/graph/badge.svg?token=ZUP383PSK7)](https://codecov.io/gh/calf-ai/calfkit-sdk)
[![License](https://img.shields.io/github/license/calf-ai/calfkit-sdk)](LICENSE)

The SDK to build AI agents as orchestratable, event-driven microservices.

Calfkit lets you compose agents as decoupled microservices–agents, tools, workflows–that communicate asynchronously. Add agents to teams without hardcoding any orchestration logic, scale each component independently, and stream agent outputs to any downstream listener. 

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

> **A note on Kafka topics.** This quickstart "just works" because the local
> calfkit-broker has broker-side topic auto-creation enabled — node inboxes are
> created on first use. Most hardened/managed brokers have that **disabled**, in
> which case producers and consumers silently stall on a missing topic. Calfkit
> ships an **EXPERIMENTAL, opt-in** topic provisioner (off by default) for the
> dev/CI case: `Client.connect("localhost:9092", provisioning=ProvisioningConfig(enabled=True))`.
> It is a development convenience (`replication_factor=1`, no ACLs) — **review it
> before production**, where topic creation is typically ops-governed. See
> [`docs/topic-provisioning.md`](docs/topic-provisioning.md).

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
from calfkit.providers import OpenAIResponsesModelClient
from calfkit.client import Client
from calfkit.worker import Worker
from weather_tool import get_weather  # Import the tool definition (reusable)

agent = Agent(
    "weather_agent",
    system_prompt="You are a helpful assistant.",
    subscribe_topics="weather_agent.input",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-nano"),
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
result = await client.execute_node(
    "What's the weather in Tokyo?",
    "weather_agent.input",
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
    deps={"user_id": "usr_123"},  # Available to tools via ctx.deps["user_id"]
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

**Fire-and-forget** — dispatch work to a node without waiting for (or producing) a reply via `emit_to_node`:

```python
correlation_id = await client.emit_to_node(
    "Re-index the catalog.",
    "indexer.input",
)
# Returns the correlation_id immediately; no reply is produced and no
# client-side reply future is allocated.
```

`emit_to_node` takes the same input-shaping arguments as `invoke_node` (`deps`, `temp_instructions`, `message_history`, `run_args`, `model_settings`, `tool_overrides`, `correlation_id`) — but no `reply_topic` or `output_type`, since there is nothing to route back or deserialize.

Because there's no reply, **traceability comes from the target node's `publish_topic` broadcast stream**, not a point-to-point callback. Set a `publish_topic` on the node you emit to and tap it with a [consumer node](#consumer-nodes-optional) to observe terminals (`result.output` is populated exactly as it is for `execute_node`). A node with no `publish_topic` produces no observable record for a fire-and-forget send — there is neither a reply nor a broadcast.

Use `emit_to_node` for true one-way sends, `invoke_node` for async dispatch with a handle to await later, and `execute_node` for synchronous request/reply.

> **Bounding `invoke_node` memory** — each pending `invoke_node` handle holds a reply future until it resolves. If a reply is lost or a handle is abandoned, that future leaks. Pass an opt-in TTL to bound it:
>
> ```python
> client = Client.connect("localhost:9092", reply_ttl=30.0)
> ```
>
> When set, an unanswered handle is evicted after `reply_ttl` seconds and `handle.result()` raises `ReplyExpiredError`. The default (`None`) waits indefinitely. `emit_to_node` allocates no future, so the TTL does not apply to it.

<br>

### Lifecycle Hooks & Resources (Optional)

Nodes and workers can open long-lived **resources** (database pools, HTTP clients, caches) at startup and close them on shutdown, publish **presence/departure** events, and run under `run()`, the embeddable `start()`/`stop()`, or `async with worker:`.

See **[Worker Lifecycle & Embedding](docs/worker-lifecycle.md)** for the full walkthrough — the `@resource` and callback hook patterns, worker-scoped resources, `resources` vs `deps`, presence events, and the three run surfaces with their guarantees.

<br>

### Gating Node Invocations (Optional)

When multiple agents share an input topic (each with its own consumer group), every agent receives every message published to that topic. A **gate stack** lets a node decide whether to handle an inbound event *before* `run()` runs — avoiding wasted LLM tokens on messages addressed elsewhere.

Gates are predicates: `Callable[[SessionRunContext], bool | Awaitable[bool]]`. They stack with **AND semantics** in registration order and short-circuit on the first `False`, exception, or non-bool return. When any gate rejects, `run()` is skipped and the envelope is returned unchanged — the Kafka offset still commits.

**Constructor form** — good for shared, cross-cutting predicates passed in as values:

```python
def is_scheduler_target(ctx) -> bool:
    discord = ctx.deps.get("discord", {})
    return discord.get("slash_target") == "scheduler"

scheduler = Agent(
    "scheduler",
    subscribe_topics="discord.thread.123",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-nano"),
    gates=[is_scheduler_target],
)
```

**Decorator form** — good for node-specific gates defined inline:

```python
scheduler = Agent("scheduler", subscribe_topics="discord.thread.123", model_client=...)

@scheduler.gate
def is_scheduler_target(ctx) -> bool:
    discord = ctx.deps.get("discord", {})
    return discord.get("slash_target") == "scheduler"
```

Constructor and decorator forms can be combined; constructor gates run first.

**Idempotency requirement**: Kafka may redeliver an event before its offset commits, so gates may run more than once for the same logical message. Keep gate functions deterministic and side-effect-free.

**Failure behavior**: If a gate raises or returns a non-bool, the framework logs the failure and rejects the message (fail-safe). Place cheap fast-reject gates first to maximize short-circuit efficiency.

For tool-node gating, pass `gates=[...]` to `ToolNodeDef.create_tool_node(...)` directly; the `@agent_tool` decorator doesn't expose `gates=` because tool topics are typically 1:1.

<br>

### Consumer Nodes (Optional)

A **consumer node** is a terminal sink — it subscribes to one or more topics and runs arbitrary Python logic against every event flowing through. Consumers receive the same `NodeResult` that `Client.execute_node()` returns, including the full session state (`tool_calls`, `tool_results`, `message_history`, `metadata`) and the inbound producer `deps` via `result.deps["key"]` — the same data tools read as `ctx.deps["key"]`.

Deploy a consumer as its own service. Wire it to an agent's `publish_topic` (or any topic carrying calfkit envelopes) to observe outputs from agents, tools, and intermediate hops:

```python
# weather_sink.py
import asyncio
from calfkit.client import Client, NodeResult
from calfkit.nodes import consumer
from calfkit.worker import Worker

@consumer(subscribe_topics="weather_agent.output")
async def log_weather(result: NodeResult) -> None:
    if result.output is None:
        return  # intermediate hop — no final output yet
    print(f"[{result.correlation_id[:8]}] {result.output}")

async def main():
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[log_weather])  # Deploy the consumer node
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
```

Run alongside the agent service:

```shell
python weather_sink.py
```

An agent's `publish_topic` emits on **every** state transition — intermediate hops, tool completions, and terminals — so `result.output` is `None` on hops without final output parts. Filter via a gate if you only want agent terminals:

```python
@consumer(
    subscribe_topics="weather_agent.output",
    gates=[lambda ctx: bool(ctx.state.final_output_parts)],
)
async def save_final(result: NodeResult) -> None:
    await db.save(result.output)  # always populated here
```

**Upstream requirement**: the upstream agent or tool must have a `publish_topic` set for consumers to tap (e.g. add `publish_topic="weather_agent.output"` to the agent in step 4).

**Error policy**: exceptions from the consumer function are logged and swallowed by default so a single bad event can't poison-pill the Kafka offset. Pass `re_raise=True` to fail loud during development.

<br>

### MCP Adaptor (Optional)

Expose any [Model Context Protocol](https://modelcontextprotocol.io/) server's tools (Gmail, GitHub, Postgres, filesystems, browsers, and hundreds of others) as native calfkit tools that any `Agent` can call over standard Kafka envelopes — no per-tool glue code.

See [`docs/mcp-overview.md`](docs/mcp-overview.md) for the quickstart, deployment topologies, `mcp.json` interop, multi-tenancy, observability, and CI drift detection.

<br>

## Documentation

Full documentation is coming soon. In the meantime, this README serves as the primary reference for getting started with Calfkit.

Deep-dive guides:

- [Worker Lifecycle & Embedding](docs/worker-lifecycle.md) — running a worker with `run()` vs the embeddable `start()`/`stop()` and `async with` surfaces, composing it with other long-running services, and the lifecycle guarantees.

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
