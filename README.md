<h1 align="center">🐮 Calfkit</h1>

<h3 align="center">
  Build decentralized multi-agent systems. Agents discover each other at runtime, choreograph work, and scale as independent, event-driven services on Kafka.
</h3>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/github/license/calf-ai/calfkit-sdk" alt="License"></a>
  <a href="https://pypi.org/project/calfkit/"><img src="https://img.shields.io/pypi/v/calfkit" alt="PyPI version"></a>
  <a href="https://pepy.tech/project/calfkit"><img src="https://static.pepy.tech/badge/calfkit/month" alt="PyPI downloads"></a>
  <a href="https://pypi.org/project/calfkit/"><img src="https://img.shields.io/pypi/pyversions/calfkit" alt="Python versions"></a>
  <a href="https://codecov.io/gh/calf-ai/calfkit-sdk"><img src="https://codecov.io/gh/calf-ai/calfkit-sdk/graph/badge.svg?token=ZUP383PSK7" alt="codecov"></a>
  <a href="https://deepwiki.com/calf-ai/calfkit-sdk"><img src="https://deepwiki.com/badge.svg" alt="Ask DeepWiki"></a>
</p>

Calfkit agents find each other and choreograph work over a **mesh** — a highly-connected data streaming network they auto-discover and communicate on. Each agent runs as an independent, event-driven service, so you can build free-flowing multi-agent workflows that collaborate and react to live data streams.

<br>

## Why Calfkit?

- **Dynamic agent-to-agent discovery and collaboration.** Agents find each other at runtime and work together — messaging each other and handing off tasks — so you build multi-agent systems without complex wiring or orchestration, and extend team capabilities at any time.
- **Scalable by default.** Every agent runs and scales as an independent microservice, so your agent teams are resilient and scalable from day one.
- **Agentic action on realtime data streams.** Agents are event-driven, so they react to realtime data streams — live market feeds, log streams, support-ticket queues — and send results wherever they're needed. Build agents that work like continuously streaming workflows, not one-off requests.

## Installation

```bash
# Recommended for getting started, includes a zero-setup in-memory dev mesh:
pip install 'calfkit[mesh]'
```

## Quickstart

With the `[mesh]` extra, `ck dev` spins up a local in-memory mesh for you — no Docker, no `CALFKIT_MESH_URL` required.

### Agent

Save as `general.py`:

```python
from calfkit import Agent, Handoff, Messaging, OpenAIResponsesModelClient

general = Agent(
    name="general",
    description="Answers simple questions and routes requests to whoever can handle it.",
    system_prompt="You are a general assistant. Defer technical questions to other agents.",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4"),
    peers=[
        Messaging(discover=True),  # discover and delegate to any agent at runtime
        Handoff(discover=True),    # discover and hand off to any agent at runtime
    ],
)
```

### Run it and chat

```bash
# Starts the agent (and a local mesh if one isn't running):
#   ck dev run <file>:<agent>
ck dev run general:general

# In a second terminal, chat with the agent:
ck dev chat
```

### Add another agent — and watch them discover each other

Save as `finance.py`:

```python
from calfkit import Agent, OpenAIResponsesModelClient

finance = Agent(
    name="finance",
    description="Answers the user's personal finance questions.",
    system_prompt="You are the personal finance specialist. Answer finance-related questions.",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4"),
)
```

```bash
ck dev run finance:finance
```

Now ask a finance question in `ck dev chat` — `general` discovers `finance` at runtime and hands off automatically. No wiring, no orchestrator.

## Running an agent mesh

Calfkit agents discover and communicate over a mesh.

**For local dev**, the bundled in-memory broker (via `[mesh]` extra) is zero-setup — see [How to run a local mesh with `ck dev`](docs/local-dev-mesh.md).

**In production**, any Kafka-API-compatible mesh can be used so you can drop your agent swarms into streaming infrastructure you already use.

Want a fully-managed mesh your agents can join from anywhere? [Join the beta](https://forms.gle/Rk61GmHyJzequEPm8)

## Documentation

* **Getting started**: See [`docs/`](docs/).
* **Examples**: See [`examples/`](examples/) — multi-agent team and general framework API examples.

## Contributing

Issues and pull requests are welcome. Please [open an issue](https://github.com/calf-ai/calfkit-sdk/issues) to discuss substantial changes before sending a PR.

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
