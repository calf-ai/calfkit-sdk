<h1 align="center">🐮 Calfkit</h1>

<h3 align="center">
  Build powerful AI agents with automatic, open inter-agent discovery and communication.
</h3>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/github/license/calf-ai/calfkit-sdk" alt="License"></a>
  <a href="https://pypi.org/project/calfkit/"><img src="https://img.shields.io/pypi/v/calfkit" alt="PyPI version"></a>
  <a href="https://pepy.tech/project/calfkit"><img src="https://static.pepy.tech/badge/calfkit/month" alt="PyPI downloads"></a>
  <a href="https://pypi.org/project/calfkit/"><img src="https://img.shields.io/pypi/pyversions/calfkit" alt="Python versions"></a>
  <a href="https://codecov.io/gh/calf-ai/calfkit-sdk"><img src="https://codecov.io/gh/calf-ai/calfkit-sdk/graph/badge.svg?token=ZUP383PSK7" alt="codecov"></a>
  <a href="https://deepwiki.com/calf-ai/calfkit-sdk"><img src="https://deepwiki.com/badge.svg" alt="Ask DeepWiki"></a>
</p>

Calfkit agents dynamically find each other at runtime and choreograph work, with no hard-coded orchestrator or wiring. Build free-flowing and flexible multi-agent workflows.

<br>

## Why Calfkit?

- **Dynamic agent-to-agent discovery and collaboration.** Agents find each other at runtime and work together — messaging each other and handing off tasks — so you build multi-agent systems without complex wiring or orchestration, and extend team capabilities at any time.
- **No bottleneck, no single point of failure.** Every agent runs and scales as an independent microservice, so your agent teams are resilient and scalable from day one.
- **Act on live data in realtime.** Agents are event-driven so they act on realtime data streams, sending live results wherever they're needed — build agents that work like continuous workflows, not one-off requests.

## Installation

```bash
pip install calfkit

# or, with a zero-setup local dev broker included:
pip install 'calfkit[mesh]'
```

## Quickstart

Agents run on a mesh. With the `[mesh]` extra installed, `ck dev` starts a local
one for you — no setup, no `CALFKIT_MESH_URL` needed. (To use your own broker
instead, set `CALFKIT_MESH_URL` and use plain `ck run` / `ck chat`.)

### Agent

```python
from calfkit import Agent, Handoff, Messaging, Tools, OpenAIResponsesModelClient

general = Agent(
    name="general",
    description="Answers simple questions and routes requests to whoever can handle it.",
    system_prompt="You are a general assistant. Defer technical questions to other agents.",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-mini"),
    peers=[
        Messaging(discover=True),  # discover and delegate to any agent at runtime
        Handoff(discover=True),  # discover and hand off to any agent at runtime
    ],
)
```

### Run locally

You can add more agents to the team as you keep this agent's process running in the background.

```bash
# Start the agent process (general_help.py), spawning a local mesh if needed:
# ck dev run file_name:agent_name
ck dev run general_help:general

# Interactive agent chat CLI (second terminal — reuses the same mesh)
ck dev chat
```

### Add another agent to the team

```python
from calfkit import Agent, agent_tool, Tools, ToolContext, OpenAIResponsesModelClient

finance = Agent(
    name="finance",
    description="Answers the user's personal finance questions.",
    system_prompt="You are the personal finance specialist. Answer finance-related questions.",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-mini"),
)
```

### Run new agent locally

```bash
ck dev run finance_help:finance

ck dev chat
```

## Running an agent mesh

Calfkit agents discover and communicate over an agent mesh (`CALFKIT_MESH_URL`), which you can run locally yourself.

The easiest local mesh is the bundled dev broker — in-memory, zero setup
(see [How to run a local mesh with `ck dev`](docs/local-dev-mesh.md)):

```bash
pip install 'calfkit[mesh]'
ck dev broker start
```

Or start a persistent one with Docker:

```bash
git clone https://github.com/calf-ai/calfkit-mesh && cd calfkit-mesh && make dev-up
```

If you might be interested in a fully-managed mesh server your agents can join from anywhere, [let me know](https://forms.gle/Rk61GmHyJzequEPm8).

## Documentation

* **Getting started**: See [`docs/`](docs/).
* **Examples**: See [`examples/`](examples/) multi-agent team and general framework API examples.

## Contributing

Issues and pull requests are welcome. Please [open an issue](https://github.com/calf-ai/calfkit-sdk/issues) to discuss substantial changes before sending a PR.

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, the quality gates (`make fix` / `make check` / `make test`), PR conventions, and how to write and run tests — including integration tests.

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
