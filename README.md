# Calf SDK

Python SDK for building event-driven AI agents and workflows.

## Motivation

https://seanfalconer.medium.com/the-future-of-ai-agents-is-event-driven-9e25124060d6

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

A higher-level, opinionated API is in development. This will provide out-of-the-box abstractions for common AI agent patterns while maintaining Calf's event-driven architecture.

**Planned Features:**

- **Agents** - Stateful, named entities with LLM models, tools, and memory
- **GroupChat** - Multi-agent coordination with different process patterns (sequential, hierarchical, swarm)
- **Agent Tools** - Event-driven tools that agents can invoke via `@calf.tool` decorator
- **State Management** - External state stores (Redis, PostgreSQL) for distributed deployments
- **Model Providers** - OpenAI-compatible and other LLM providers

**Planned Usage:**

```python
from calf import Agent, GroupChat, Calf, tool, RunContext
from calf import MemoryStateStore, OpenAIClient

# Setup
calf = Calf()
state_store = MemoryStateStore()
model_client = OpenAIClient()

# Define tools
@calf.tool
async def search_web(ctx: RunContext, query: str) -> str:
    """Search the web for information."""
    return await search_api(query)

# Create agents
researcher = Agent(
    name="researcher",
    model="gpt-4o",
    tools=[search_web],
    system_prompt="You are a research assistant.",
)

writer = Agent(
    name="writer",
    model="gpt-4o",
    system_prompt="You are a technical writer.",
)

# Register agents
calf.register(researcher, state_store=state_store, model_client=model_client)
calf.register(writer, state_store=state_store, model_client=model_client)

# Create multi-agent group
chat = GroupChat(
    name="content-team",
    agents=[researcher, writer],
)

# Run the group chat
async for msg in chat.run_stream("Write an article about AI agents"):
    print(f"{msg.agent}: {msg.content}")
```

## Project Structure

```
calf-sdk/
├── calf/
│   ├── __init__.py        # Public exports (Calf, Message, Agent, tool, etc.)
│   ├── client.py          # Main Calf client
│   ├── message.py         # Message model
│   ├── cli.py             # CLI commands
│   ├── broker/            # Broker implementations
│   │   ├── base.py        # Abstract broker interface
│   │   ├── memory.py      # In-memory broker (dev)
│   │   └── kafka.py       # Kafka broker (production)
│   ├── agents/            # NEW: Agent system (planned)
│   ├── tools/             # NEW: Tool system (planned)
│   ├── group_chat/        # NEW: Multi-agent coordination (planned)
│   ├── state/             # NEW: State stores (planned)
│   └── providers/         # NEW: LLM providers (planned)
├── examples/
│   ├── simple_workflow.py
│   ├── chat_agent.py      # NEW: Agent examples (planned)
│   ├── group_chat.py      # NEW: Multi-agent examples (planned)
│   └── location_agent.py  # NEW: Tool examples (planned)
├── docs/
│   └── design/
│       └── HIGH_LEVEL_API.md  # Design documentation
├── tests/
│   └── test_workflow.py
└── pyproject.toml
```
