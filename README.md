# Calf SDK

Python SDK for building event-driven AI agents and workflows.

## Overview

Calf SDK provides a simple, unopinionated interface for building event-driven AI workflows. It wraps Kafka complexity and deployment, and provides an easy-to-use pattern for connecting AI components in a distributed architecture.

## Requirements

- Python >= 3.10
- [uv](https://docs.astral.sh/uv/getting-started/installation/) package manager (recommended)

## Installation

```bash
uv add calf-sdk
```

Or with pip:

```bash
pip install calf-sdk
```

## Quick Start

Create a workflow file:

```python
# my_workflow.py
from calf import Calf, Message

calf = Calf()

@calf.on("documents.uploaded")
async def process_document(msg: Message) -> None:
    """Process uploaded documents."""
    print(f"Processing: {msg.data}")

    # Your AI logic here
    summary = f"Summary of {msg.data.get('id')}"

    # Emit result to next stage
    await calf.emit(
        "documents.processed",
        {"id": msg.data.get("id"), "summary": summary},
        metadata=msg.metadata,
    )

@calf.on("documents.processed")
async def handle_result(msg: Message) -> None:
    """Handle processed documents."""
    print(f"Completed: {msg.data}")

if __name__ == "__main__":
    calf.run_app()
```

Run the workflow:

```bash
# Local development (in-memory broker, no Kafka required)
uv run my_workflow.py local

# Production (connects to Kafka)
uv run my_workflow.py start --broker localhost:9092
```

## API Reference

### Calf

The main client for building workflows.

```python
from calf import Calf

calf = Calf()
```

#### `emit(channel, data, metadata=None)`

Send a message to a channel.

```python
await calf.emit(
    "events.user.created",
    {"user_id": "123", "email": "user@example.com"},
    metadata={"trace_id": "abc"},
)
```

#### `listen(channel)`

Receive messages from a channel as an async iterator.

```python
async for msg in calf.listen("events.user.created"):
    print(msg.data)
```

#### `on(channel)`

Decorator to register a handler for a channel.

```python
@calf.on("tasks.process")
async def handler(msg: Message) -> None:
    # Process message
    pass
```

#### `run_app()`

Start the workflow with CLI argument parsing.

```python
if __name__ == "__main__":
    calf.run_app()
```

### Message

A message that flows through channels.

```python
from calf import Message

msg = Message(
    data={"key": "value"},      # The payload
    channel="my.channel",        # Channel name
    metadata={"trace": "123"},   # Optional metadata
)
```

#### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `data` | `dict` | The message payload |
| `channel` | `str` | Source channel name |
| `id` | `str` | Unique message ID (auto-generated) |
| `timestamp` | `datetime` | When the message was created |
| `metadata` | `dict` | User-defined metadata |

## CLI Commands

### `local`

Run with in-memory broker for local development. No Kafka required.

```bash
uv run my_workflow.py local
```

### `start`

Run with Kafka broker for production.

```bash
uv run my_workflow.py start --broker localhost:9092
```

## Development

### Setup

```bash
git clone <repo-url>
cd calf-sdk
uv sync
```

### Run Tests

```bash
uv run pytest tests/ -v
```

### Linting and Type Checking

```bash
uv run ruff check .
uv run mypy calf/
```

## Roadmap

### Higher-Level API (In Progress)

A higher-level, opinionated API is in development at `calf.agents`. This will provide out-of-the-box abstractions for common AI workflow patterns:

**Planned Features:**

- **Agents** - Stateful, named entities with capabilities and lifecycle management
- **Agent Teams** - Coordinated groups of agents working together on complex tasks
- **Agent Tools** - Reusable tools that agents can invoke, with support for event-driven tool execution (e.g., reusable tools wrapping downstream services that communicate asynchronously)
- **Adapters** - Optional integrations with popular AI libraries:
  - LangChain / LangGraph
  - LlamaIndex
  - CrewAI
  - OpenAI / Anthropic clients

**Planned Usage:**

```python
from calf.agents import Agent, Team, task

class ResearcherAgent(Agent):
    name = "researcher"

    @task
    async def research(self, topic: str) -> dict:
        # Agent logic here
        return {"findings": [...]}

class WriterAgent(Agent):
    name = "writer"

    @task
    async def write(self, findings: dict) -> str:
        # Agent logic here
        return "Article content..."

team = Team(
    name="content-team",
    agents=[ResearcherAgent(), WriterAgent()],
)

await team.start()
```

## Project Structure

```
calf-sdk/
├── calf/
│   ├── __init__.py        # Public exports (Calf, Message)
│   ├── client.py          # Main Calf client
│   ├── message.py         # Message model
│   ├── cli.py             # CLI commands
│   └── broker/
│       ├── base.py        # Abstract broker interface
│       ├── memory.py      # In-memory broker (dev)
│       └── kafka.py       # Kafka broker (production)
├── examples/
│   └── simple_workflow.py
├── tests/
│   └── test_workflow.py
└── pyproject.toml
```
