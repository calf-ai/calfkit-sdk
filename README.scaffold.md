<!--
README REWRITE — SCAFFOLD (legacy-copy sections filled in; new-writing sections still noted)

Locked decisions:
- Value prop: "team of coworkers" framing — agents discover each other and
  collaborate like a real team. Decentralized agent swarms = the technical descriptor.
- No teaser snippet. Jump straight into a progressive Quickstart (code-first).
- Broker/infra is IMPLIED in the run section (LiveKit-style), never a "Prerequisites" gate.
- ck run accepts multiple targets in one worker → run section is a 2-terminal story.
- Removed: Roadmap, Contact, ⭐-star CTA, social links.
- Guiding examples: LiveKit (infra implied + CLI run progression) + ADK (brevity, fast time-to-example).

STILL TO WRITE: (optional) Quickstart intro line. All sections drafted.
-->

<h1 align="center">🐮 Calfkit</h1>

<h3 align="center">
  Build powerful teams of AI agents that freely discover each other and collaborate on work, automatically.
</h3>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/github/license/calf-ai/calfkit-sdk" alt="License"></a>
  <a href="https://pypi.org/project/calfkit/"><img src="https://img.shields.io/pypi/v/calfkit" alt="PyPI version"></a>
  <a href="https://pepy.tech/project/calfkit"><img src="https://static.pepy.tech/badge/calfkit/month" alt="PyPI downloads"></a>
  <a href="https://pypi.org/project/calfkit/"><img src="https://img.shields.io/pypi/pyversions/calfkit" alt="Python versions"></a>
  <a href="https://codecov.io/gh/calf-ai/calfkit-sdk"><img src="https://codecov.io/gh/calf-ai/calfkit-sdk/graph/badge.svg?token=ZUP383PSK7" alt="codecov"></a>
  <a href="https://deepwiki.com/calf-ai/calfkit-sdk"><img src="https://deepwiki.com/badge.svg" alt="Ask DeepWiki"></a>
</p>

Calfkit agents dynamically find each other at runtime and choreograph work like a real team. No hard-coded orchestrator or extra wiring. The framework for building free-flowing and powerful multi-agent teams.

<br>

## Installation

```bash
pip install calfkit
```

## Quickstart

### An agent (that can discover other agents)

```python
from calfkit import Agent, Messaging, Tools, OpenAIResponsesModelClient

general = Agent(
    name="general",
    description="Answers simple questions and routes requests to whoever can handle it.",
    system_prompt="You are a general assistant. Defer technical questions to other agents.",
    subscribe_topics="general.input",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-mini"),
    peers=[Messaging(discover=True)],   # discover and collaborate w/ any agent at runtime
)
```

### Runtime discoverability allows you to add new agents and tools to the team at any time

```python
from calfkit import Agent, agent_tool, Tools, ToolContext, OpenAIResponsesModelClient

finance = Agent(
    name="finance",
    description="Answers the user's personal finance questions.",
    system_prompt="You are the personal finance specialist. Use tools to look up user data.",
    subscribe_topics="finance.input",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-mini"),
    tools=[Tools(discover=True)],   # discover and use any tool at runtime
)

@agent_tool
def lookup_account_balance(ctx: ToolContext) -> str:
    """Look up the user's current account balance in USD."""
    return f"Account balance: ${ctx.deps.get('balance', '0.00')}"
```

## Running your agents

Start the general assistant independently. Assuming it's saved in `general_help.py`.

```bash
# using the ck CLI
ck run general_help:general
```

Separately, start the `finance` agent and the `lookup_account_balance` tool node. Assuming it's saved in `finance_help.py`.

```bash
ck run finance_help:finance finance_help:lookup_account_balance
```

Ask the general assistant a question. Notice it's able to dynamically discover and consult the `finance` agent for help without any hard-coded configuration of `finance` agent's existence.

```python
import asyncio
from calfkit import Client

async def main():
    client = Client.connect("<calfkit_agent_mesh>")
    result = await client.execute("Do I have enough money to afford a new car?", "general.input")
    
    print(result.output)
    # LOL nah twin

if __name__ == "__main__":
     asyncio.run(main())
```

```bash
python ask.py
```

Calfkit agents discover and communicate over an agent mesh, provided by either Calfkit Cloud (in alpha) or your own self-hosted version. 

Start one locally with Docker:
```bash
git clone https://github.com/calf-ai/calfkit-broker && cd calfkit-broker && make dev-up
```

Or skip the setup, and enable international, cross-host agents with [Calfkit Cloud](https://forms.gle/Rk61GmHyJzequEPm8), a fully-managed agent mesh.

## Why Calfkit?

Calfkit teams are alive: agents find each other, call in specialists, and grow more capable on their own.

- **A team that grows itself.** Drop a new specialist or tool onto the network and every agent can use it instantly — discovered at runtime, no redeploys, no rewiring. Your team gets more capable every time you ship.
- **A decentralized swarm with no central bottleneck.** No orchestrator to choke, no single brain to rewrite — every agent, tool, and consumer is its own microservice, scaling on its own and collaborating peer-to-peer. The team grows without ever getting tangled.
- **Agents that live on real-time data.** They run on event streams — reacting to market feeds, sensors, or user activity the moment it lands, and piping results anywhere downstream. Always-on and connected, not one-shot prompts.

## Examples

See [`examples/`](examples/) for more examples.

- **[Agent, tool & consumer](examples/quickstart/)** — a weather agent and its `get_weather` tool deployed as separate services and invoked over the broker, with a consumer node tapping the agent's output stream.
- **[Multi-agent panel](examples/multi_agent_panel/)** — three persona agents (`optimist`, `skeptic`, `pragmatist`) debate over one shared transcript, each automatically seeing the thread from its own point of view.
- **[MCP toolbox](examples/quickstart_mcp/)** — give an agent a live web-`fetch` tool from an MCP server, deployed as its own node and referenced by name — the agent's code never imports it.

## Documentation

In-repo documentation lives under [`docs/`](docs/).

**New to building agent teams?** Start with the tutorial **[Build a multi-agent support desk](docs/multi-agent-support-desk.md)** — build and run three agents that discover each other and collaborate by messaging and handoff.

**How-to guides** — goal-oriented walkthroughs:

- **[How to call agents from a client](docs/client-features.md)** — the three invocation patterns (`execute` / `start` / `send`), multi-turn conversations, runtime dependency injection (`deps`), temporary instructions, fire-and-forget, and bounding reply memory with `reply_ttl`.
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

## Contributing

Issues and pull requests are welcome. Please [open an issue](https://github.com/calf-ai/calfkit-sdk/issues) to discuss substantial changes before sending a PR.

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, the quality gates (`make fix` / `make check` / `make test`), PR conventions, and how to write and run tests — including the real-broker integration tests.

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
