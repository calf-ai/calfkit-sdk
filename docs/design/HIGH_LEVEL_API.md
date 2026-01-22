# Calf SDK: High-Level Agent API Design Document

> **Status:** Draft (Reviewed)
> **Created:** 2026-01-10
> **Last Updated:** 2026-01-13
> **Authors:** Ryan, Claude
> **Branch:** `feat/high-level-api`

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Confirmed Decisions](#confirmed-decisions)
3. [Goals and Non-Goals](#goals-and-non-goals)
4. [Background and Motivation](#background-and-motivation)
5. [Design Principles](#design-principles)
6. [Architecture Overview](#architecture-overview)
7. [Core Abstractions](#core-abstractions)
8. [Event-Driven Agent Model](#event-driven-agent-model)
9. [Multi-Agent Coordination](#multi-agent-coordination)
10. [State Management](#state-management)
11. [API Reference](#api-reference)
12. [Implementation Plan](#implementation-plan)
13. [Alternatives Considered](#alternatives-considered)
14. [Open Questions](#open-questions)

---

## Executive Summary

This document describes the design of a high-level Agent API for the Calf SDK. The API enables developers to build AI agents and multi-agent systems using an **event-driven-native architecture** where all agent operations—including inference loops, tool execution, and inter-agent communication—flow through the existing Calf pub-sub broker system.

Unlike traditional AI agent frameworks that implement agents as explicit loops, Calf agents are reactive event handlers where the "agentic loop" emerges from event topology rather than imperative control flow. This design provides superior observability, scalability, and composability while maintaining an ergonomic developer experience comparable to leading frameworks like Pydantic AI and AutoGen.

---

## Confirmed Decisions

The following design decisions have been reviewed and confirmed:

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Agent definition style** | Functional with constructor tools | Universal pattern across AI frameworks; `Agent(tools=[...])` + `@calf.tool` |
| **Channel naming** | `calf.agent.{name}.{event}` | Namespaced with `calf.` prefix for clarity |
| **GroupChat API** | `GroupChat` + `Process` enum | Clear pattern matching CrewAI; deferred to future iteration |
| **Workflow API** | Simpler builder pattern (TBD) | Deferred to future iteration |
| **Memory system** | Interface (ABC) only | User implements their own backends |
| **StateStore interface** | ABC with agent-specific methods | Explicit inheritance, clear contract |
| **StateStore implementation** | In-memory only (initial) | Production backends (Redis) deferred |
| **Handoff mechanism** | `handoff()` return type | Deferred to future iteration |
| **Initial scope** | Agent + Tools only | GroupChat, Workflows, Handoffs deferred |

---

## Goals and Non-Goals

### Goals

1. **Event-Driven Native**: All agent operations flow through the Calf broker—no hidden synchronous loops
2. **Maximum Type Safety**: Generic type parameters (`Agent[Deps, Output]`) with compile-time checking
3. **Great Developer Experience**: Minimal boilerplate for simple cases, full control for complex cases
4. **Production Ready**: Built for distributed systems with Kafka from day one
5. **LLM Agnostic**: Start with OpenAI-compatible API (covers most providers)

### Initial Iteration Scope

The first iteration focuses on the **core Agent and Tool system** only:

- `Agent` class with type parameters and constructor-based tool attachment
- `@tool` decorator for tool definition
- `@agent.tool` decorator for agent-specific tools
- In-memory state store (production backends deferred)
- OpenAI-compatible model client

**Deferred to future iterations:**
- Multi-agent GroupChat with Process types (sequential, hierarchical, swarm)
- Graph-based Workflows
- Handoff mechanism between agents
- Production state stores (Redis, PostgreSQL)
- Memory system implementations (vector stores, RAG)

### Non-Goals

1. **Replacing the Low-Level API**: The existing `Calf`, `Message`, and `Broker` APIs remain unchanged
2. **Built-in RAG Pipeline**: Memory interfaces are provided, but RAG implementation is user responsibility
3. **UI/Visualization**: No built-in agent visualization or debugging UI (integrate with external tools)
4. **Multi-Language Support**: Python only for initial release

---

## Background and Motivation

### Current State

The Calf SDK provides a minimal, event-driven pub-sub system with:

- **Message model**: Pydantic-based with auto-generated IDs and timestamps
- **Calf client**: Async event emission and handler registration via decorators
- **Pluggable brokers**: In-memory (development) and Kafka (production)
- **CLI**: Local and distributed execution modes

This foundation is solid but lacks higher-level abstractions for building AI agents.

### Industry Landscape

We analyzed leading AI agent frameworks to inform our design:

| Framework | Key Pattern | Strength | Limitation |
|-----------|-------------|----------|------------|
| **AutoGen** | Actor model, layered architecture | Teams, event-driven core | Complex API surface |
| **LangGraph** | Graph-based state machines | Explicit control flow, checkpointing | Steep learning curve |
| **CrewAI** | Role/goal/backstory pattern | Intuitive metaphor | Less flexible |
| **Pydantic AI** | Type-safe DI, decorators | "FastAPI feeling", model-agnostic | Less multi-agent focus |
| **OpenAI Swarm** | Function-return handoffs | Minimal, easy to understand | Manual state management |

### Our Differentiation

**Calf is event-driven native.** While other frameworks use events as an afterthought (for observability or distribution), Calf makes events the fundamental execution model. Every inference call, tool execution, and agent interaction is an event flowing through the broker.

---

## Design Principles

### 1. Events Are First-Class Citizens

All agent operations produce and consume events. There are no hidden synchronous calls or internal loops that bypass the broker.

```
Traditional:                          Calf:
┌─────────────────────┐              ┌─────────────────────┐
│ while not done:     │              │ on input event:     │
│   response = llm()  │              │   emit inference    │
│   if tools:         │              │                     │
│     execute_tools() │              │ on tool_result:     │
│   else:             │              │   emit inference    │
│     return response │              │                     │
└─────────────────────┘              │ on final:           │
      (hidden loop)                  │   emit output       │
                                     └─────────────────────┘
                                          (reactive)
```

**Rationale**: Event-driven architecture provides natural observability, horizontal scalability, replay/debugging capabilities, and clean separation of concerns.

### 2. Type Safety Over Convenience

We use Python's generic type system extensively. `Agent[Deps, Output]` enforces that dependencies and outputs are properly typed, catching errors at development time rather than runtime.

```python
# Compile-time error: researcher expects ResearchResult, not str
result: str = await researcher.run("query")  # Type error!

# Correct: output is typed as ResearchResult
result: AgentResult[ResearchResult] = await researcher.run("query")
print(result.output.summary)  # IDE autocomplete works!
```

**Rationale**: Following Pydantic AI's approach, type safety dramatically improves developer experience through IDE support and early error detection.

### 3. Simple Things Simple, Complex Things Possible

The functional API uses `Agent()` with **constructor-based tool attachment** as the primary pattern:

```python
from calf import (
    Agent,
    Calf,
    MemoryStateStore,
    OpenAIClient,
    RunContext,
    tool,
)

# Setup Calf runtime
calf = Calf()
state_store = MemoryStateStore()
model_client = OpenAIClient()

# Define reusable tools with @calf.tool decorator
@calf.tool
async def search_web(ctx: RunContext, query: str) -> str:
    """Search the web for information."""
    return await do_search(query)

@calf.tool
async def fetch_url(ctx: RunContext, url: str) -> str:
    """Fetch content from a URL."""
    return await http_get(url)

# Create agent with tools in constructor
agent = Agent(
    name="researcher",
    model="gpt-4o",
    tools=[search_web, fetch_url],  # Tools passed at construction
    system_prompt="You are a research assistant.",
)

# Register agent with Calf runtime
calf.register(agent, state_store=state_store, model_client=model_client)

# Additional tools can be added via decorator on Calf
@calf.tool
async def save_result(ctx: RunContext, data: str) -> str:
    """Save result - available to all agents."""
    return await save_to_db(data)

# Or added later to specific agent
agent.add_tool(another_tool)
```

**Rationale**: Constructor-based tools is the universal pattern across AI frameworks (AutoGen, LlamaIndex, OpenAI Swarm, Agents SDK). The `@calf.tool` decorator provides flexibility for registering tools with the broker.

### 4. Kafka-Native Patterns

The design follows Kafka best practices:

- **Stateless handlers**: All state in external store (Redis)
- **Fire-and-forget events**: No blocking request-reply at broker level
- **Idempotent processing**: Handle at-least-once delivery gracefully
- **Partitioning-friendly**: Agent ID as partition key for ordering

**Rationale**: Calf is built for production distributed systems. Fighting Kafka's model would create friction at scale.

---

## Architecture Overview

### Layered Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        User Application                         │
├─────────────────────────────────────────────────────────────────┤
│  GroupChat / Workflows                                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ Sequential  │  │Hierarchical │  │   Graph     │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
├─────────────────────────────────────────────────────────────────┤
│  Agent[Deps, Output]                                            │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐           │
│  │ Tools   │  │ Memory  │  │ Model   │  │ State   │           │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘           │
├─────────────────────────────────────────────────────────────────┤
│  Calf Core (Existing Low-Level API)                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────────────────────┐         │
│  │ Message │  │  Calf   │  │  Broker (Memory/Kafka)  │         │
│  └─────────┘  └─────────┘  └─────────────────────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

### Module Structure

```
calf/
├── __init__.py              # Public API exports
├── client.py                # Existing Calf client (unchanged)
├── message.py               # Existing Message model (unchanged)
├── broker/                  # Existing broker implementations (unchanged)
│
├── agents/                  # NEW: Agent system
│   ├── __init__.py          # Agent, BaseAgent, RunContext, AgentOutput
│   ├── base.py              # BaseAgent abstract class
│   ├── agent.py             # Agent factory function
│   ├── context.py           # RunContext for dependency injection
│   ├── types.py             # AgentState, AgentOutput, InferenceResult
│   └── handlers.py          # Event handlers for inference and tools
│
├── tools/                   # NEW: Tool system
│   ├── __init__.py          # tool, handoff, ToolDefinition
│   ├── decorator.py         # @calf.tool implementation
│   ├── schema.py            # OpenAI-compatible schema generation
│   └── registry.py          # Tool registration and lookup
│
├── group_chat/              # NEW: Multi-agent coordination
│   ├── __init__.py          # GroupChat, Process
│   ├── chat.py              # GroupChat orchestration
│   ├── process.py           # Sequential, Hierarchical, Swarm
│   └── handoff.py           # Handoff type and helpers
│
├── workflows/               # NEW: Graph-based workflows
│   ├── __init__.py          # Workflow, State
│   ├── graph.py             # Workflow graph builder
│   ├── state.py             # Typed state container
│   └── executor.py          # Graph execution engine
│
├── memory/                  # NEW: Memory systems
│   ├── __init__.py          # Memory, ConversationMemory, VectorMemory
│   ├── base.py              # Memory protocol
│   ├── conversation.py      # Chat history memory
│   └── vector.py            # Vector store integration
│
├── state/                   # NEW: External state store
│   ├── __init__.py          # StateStore, RedisStateStore
│   ├── base.py              # Abstract StateStore protocol
│   ├── redis.py             # Redis implementation
│   └── memory.py            # In-memory for testing
│
└── providers/               # NEW: LLM providers
    ├── __init__.py          # ModelClient, OpenAIClient
    ├── base.py              # Abstract ModelClient protocol
    └── openai.py            # OpenAI-compatible implementation
```

---

## Core Abstractions

### Agent

The primary abstraction for AI entities. Agents are typed containers that encapsulate:

- **Identity**: Name, description
- **Model configuration**: LLM provider, model name, temperature, etc.
- **System prompt**: Static or dynamic instructions
- **Tools**: Functions the agent can call
- **Memory**: Optional conversation history and retrieval
- **Output type**: Structured output schema

```python
from calf import (
    Agent,
    Calf,
    MemoryStateStore,
    OpenAIClient,
    RunContext,
    tool,
)
from pydantic import BaseModel

# Setup Calf runtime
calf = Calf()
state_store = MemoryStateStore()
model_client = OpenAIClient()

class SearchDeps(BaseModel):
    api_key: str

class SearchResult(BaseModel):
    answer: str
    sources: list[str]

researcher = Agent[SearchDeps, SearchResult](
    name="researcher",
    model="gpt-4o",
    system_prompt="You are a research assistant. Always cite sources.",
    output_type=SearchResult,
)

# Register agent with Calf runtime
calf.register(researcher, state_store=state_store, model_client=model_client)

@calf.tool
async def search_web(ctx: RunContext[SearchDeps], query: str) -> str:
    """Search the web for information."""
    # ctx.deps.api_key is typed!
    return await search_api(query, ctx.deps.api_key)
```

#### Type Parameters

- `Deps`: The dependency container type (injected via `RunContext`)
- `Output`: The structured output type (validated by Pydantic)

Both are optional:
- `Agent()` = `Agent[None, str]` (no deps, string output)
- `Agent[MyDeps, str]` (custom deps, string output)
- `Agent[None, MyOutput]` (no deps, structured output)

### RunContext

Dependency injection container passed to tools. Provides:

- **deps**: Typed access to dependencies
- **agent_name**: Current agent's name
- **tool_call_id**: ID of the current tool invocation
- **emit()**: Emit events to the broker (for advanced use)

```python
@calf.tool
async def save_to_db(ctx: RunContext[MyDeps], data: str) -> str:
    """Save data to database."""
    # Typed access to dependencies
    await ctx.deps.database.save(data)

    # Access to context
    logger.info(f"Tool called by {ctx.agent_name}")

    return "Saved successfully"
```

### ToolDefinition

Schema representation for tools, compatible with OpenAI's function calling format:

```python
@dataclass
class ToolDefinition:
    name: str                    # Function name
    description: str             # From docstring
    parameters: dict             # JSON Schema
    handler: Callable            # The actual function

    def to_openai_format(self) -> dict:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            }
        }
```

### AgentOutput

Result of agent execution:

```python
@dataclass
class AgentOutput[T]:
    output: T                           # The typed output
    messages: list[Message]             # Conversation history
    tool_calls: list[ToolCallRecord]    # Tools that were called
    usage: UsageStats                   # Token usage
    duration_ms: int                    # Total execution time
```

---

## Event-Driven Agent Model

### The Fundamental Insight

Traditional agent frameworks implement agents as explicit loops:

```python
# Traditional approach (NOT what we do)
async def run(self, task: str) -> str:
    messages = [system_prompt, user_task]
    while True:
        response = await llm.complete(messages)
        if response.tool_calls:
            for tool in response.tool_calls:
                result = await self.execute(tool)  # Blocking!
                messages.append(result)
        else:
            return response.content
```

This approach has problems:
- **Hidden complexity**: The loop is invisible to external systems
- **Hard to observe**: Must instrument the loop for visibility
- **Hard to scale**: Loops don't distribute naturally
- **Hard to interrupt**: No clean pause/resume semantics

### Calf's Event-Driven Model

In Calf, agents are **reactive event handlers**. There is no explicit loop—the "loop" emerges from event subscriptions:

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Event Topology                               │
│                                                                      │
│  calf.agent.{name}.input ─────────────┐                                  │
│                                  ▼                                  │
│                           ┌──────────────┐                          │
│                           │  Inference   │                          │
│                           │  Handler     │                          │
│                           └──────┬───────┘                          │
│                                  │                                  │
│              ┌───────────────────┼───────────────────┐              │
│              ▼                   ▼                   ▼              │
│     tool_call.search      tool_call.fetch         output           │
│              │                   │                   │              │
│              ▼                   ▼                   │              │
│     ┌──────────────┐    ┌──────────────┐            │              │
│     │Tool Handler  │    │Tool Handler  │            ▼              │
│     └──────┬───────┘    └──────┬───────┘       [Complete]          │
│            │                   │                                    │
│            ▼                   ▼                                    │
│     tool_result.search   tool_result.fetch                         │
│            │                   │                                    │
│            └─────────┬─────────┘                                    │
│                      ▼                                              │
│              context_updated ────┐                                  │
│                                  │                                  │
│              (triggers next      │                                  │
│               inference)◀────────┘                                  │
└─────────────────────────────────────────────────────────────────────┘
```

### Event Channels

Each agent has a dedicated namespace of channels:

| Channel | Purpose | Producer | Consumer |
|---------|---------|----------|----------|
| `calf.agent.{name}.input` | New tasks | External / other agents | Inference handler |
| `calf.agent.{name}.inference` | Inference triggers | Internal handlers | Inference handler |
| `calf.agent.{name}.tool_call.{tool}` | Tool invocations | Inference handler | Tool handler |
| `calf.agent.{name}.tool_result` | Tool results | Tool handlers | State updater |
| `calf.agent.{name}.output` | Final outputs | Inference handler | External / other agents |

### Inference Handler

The inference handler is the core of an agent. It:

1. Loads current state from the state store
2. Builds the messages array (system prompt + history + pending tool results)
3. Calls the LLM
4. Emits either tool call events or the final output

```python
async def inference_handler(self, msg: Message) -> None:
    """Handle inference trigger events."""
    agent_id = self.name

    # 1. Load state
    state = await self.state_store.get_agent_state(agent_id)

    # 2. Build messages
    messages = [
        {"role": "system", "content": self.system_prompt},
        *state.messages,
    ]

    # 3. Call LLM
    response = await self.model_client.complete(
        messages=messages,
        tools=[t.to_openai_format() for t in self.tools],
    )

    # 4. Handle response
    if response.tool_calls:
        # Update state with assistant message
        await self.state_store.add_message(agent_id, {
            "role": "assistant",
            "content": response.content,
            "tool_calls": response.tool_calls,
        })

        # Emit tool call events
        for tool_call in response.tool_calls:
            await self.calf.emit(
                f"calf.agent.{agent_id}.tool_call.{tool_call.function.name}",
                {
                    "tool_call_id": tool_call.id,
                    "arguments": json.loads(tool_call.function.arguments),
                },
                metadata=msg.metadata,  # Preserve correlation ID
            )
    else:
        # Final output - emit to output channel
        await self.calf.emit(
            f"calf.agent.{agent_id}.output",
            {
                "content": response.content,
                "status": "complete",
            },
            metadata=msg.metadata,
        )
```

### Tool Handlers

Tools are automatically converted to event handlers:

```python
# User writes:
@calf.tool
async def search_web(ctx: RunContext[MyDeps], query: str) -> str:
    """Search the web."""
    return await do_search(query)

# Framework generates:
async def search_web_handler(msg: Message) -> None:
    """Auto-generated handler for calf.agent.{name}.tool_call.search_web"""
    ctx = RunContext(
        deps=agent._deps,
        agent_name=agent.name,
        tool_call_id=msg.data["tool_call_id"],
    )

    try:
        result = await search_web(ctx, **msg.data["arguments"])
        await calf.emit(
            f"calf.agent.{agent.name}.tool_result",
            {
                "tool_call_id": msg.data["tool_call_id"],
                "tool_name": "search_web",
                "result": str(result),
                "status": "success",
            },
            metadata=msg.metadata,
        )
    except Exception as e:
        await calf.emit(
            f"calf.agent.{agent.name}.tool_result",
            {
                "tool_call_id": msg.data["tool_call_id"],
                "tool_name": "search_web",
                "error": str(e),
                "status": "error",
            },
            metadata=msg.metadata,
        )
```

### Request-Reply Pattern

For users who want synchronous-style `await agent.run()`, we implement correlation ID matching:

```python
class Agent:
    def __init__(self):
        self._pending: dict[str, asyncio.Future] = {}

    async def run(self, task: str, deps: Deps) -> AgentOutput[Output]:
        """Synchronous-style API using events under the hood."""
        correlation_id = str(uuid4())
        future: asyncio.Future = asyncio.Future()
        self._pending[correlation_id] = future

        # Emit input event
        await self.calf.emit(
            f"calf.agent.{self.name}.input",
            {"content": task},
            metadata={"correlation_id": correlation_id},
        )

        # Wait for matching output
        try:
            return await asyncio.wait_for(future, timeout=self.timeout)
        finally:
            del self._pending[correlation_id]

    async def _output_handler(self, msg: Message) -> None:
        """Resolve pending futures when output arrives."""
        correlation_id = msg.metadata.get("correlation_id")
        if correlation_id in self._pending:
            self._pending[correlation_id].set_result(
                AgentOutput(output=self._parse_output(msg.data))
            )
```

**Key insight**: The client maintains correlation state, not Kafka. This is the Kafka-native pattern for request-reply.

---

## Multi-Agent Coordination

### Handoffs

Handoffs transfer control from one agent to another. They're implemented as a special tool return type:

```python
from calf import Agent, Calf, handoff, RunContext

calf = Calf()

triage = Agent(name="triage", ...)
sales = Agent(name="sales", ...)
support = Agent(name="support", ...)

@calf.tool
async def transfer_to_sales(ctx: RunContext) -> handoff:
    """Transfer the conversation to the sales team."""
    return handoff(sales)

@calf.tool
async def transfer_to_support(ctx: RunContext, priority: str) -> handoff:
    """Transfer to technical support."""
    return handoff(support, context={"priority": priority})
```

Under the hood, handoffs emit a special event and update routing:

```python
# When handoff is returned from a tool:
await calf.emit(
    f"calf.agent.{target_agent.name}.input",
    {
        "content": current_conversation,
        "handoff_from": source_agent.name,
        "handoff_context": handoff.context,
    },
)

await calf.emit(
    f"calf.agent.{source_agent.name}.output",
    {
        "status": "handed_off",
        "handed_off_to": target_agent.name,
    },
)
```

### GroupChat

GroupChat coordinates multiple agents for complex tasks:

```python
from calf import Agent, GroupChat, Process

researcher = Agent(name="researcher", ...)
writer = Agent(name="writer", ...)
editor = Agent(name="editor", ...)

# Sequential: Each agent runs in order
sequential_chat = GroupChat(
    name="content_team",
    agents=[researcher, writer, editor],
    process=Process.sequential,
)

# Hierarchical: Manager delegates and synthesizes
hierarchical_chat = GroupChat(
    name="research_team",
    agents=[researcher, writer, editor],
    process=Process.hierarchical,
    manager=Agent(name="manager", model="gpt-4o"),
)

# Swarm: Agents hand off based on their own judgment
swarm_chat = GroupChat(
    name="support_team",
    agents=[triage, sales, support],
    process=Process.swarm,
    entry_point=triage,
)
```

#### Process Types

| Process | Description | Event Flow |
|---------|-------------|------------|
| `sequential` | Agents run in defined order | A → B → C → output |
| `hierarchical` | Manager decomposes, delegates, synthesizes | Manager → [A, B, C] → Manager → output |
| `swarm` | Agents hand off based on tool calls | Entry → (handoffs) → output |

### Workflows

For complex control flow, use graph-based workflows:

```python
from calf import Workflow, State
from typing import Literal

class ReviewState(State):
    draft: str
    feedback: str | None = None
    approved: bool = False
    iterations: int = 0

workflow = Workflow[ReviewState](name="review_cycle")

@workflow.node
async def write(state: ReviewState) -> ReviewState:
    result = await writer.run(f"Write: {state.draft}\nFeedback: {state.feedback}")
    return state.model_copy(update={"draft": result.output})

@workflow.node
async def review(state: ReviewState) -> ReviewState:
    result = await reviewer.run(f"Review: {state.draft}")
    approved = "APPROVED" in result.output.upper()
    return state.model_copy(update={
        "feedback": result.output,
        "approved": approved,
        "iterations": state.iterations + 1,
    })

@workflow.node
async def should_continue(state: ReviewState) -> Literal["write", "end"]:
    if state.approved or state.iterations >= 3:
        return "end"
    return "write"

# Define graph
workflow.add_edge("start", "write")
workflow.add_edge("write", "review")
workflow.add_conditional_edge("review", "should_continue", {
    "write": "write",
    "end": "end",
})

# Execute
final = await workflow.run(ReviewState(draft="Write about AI"))
```

---

## State Management

### Design Decision: External State Store

**Decision**: Agent state is stored in an external store, not in Kafka.

**Rationale**:

| Concern | Kafka-Only | External Store |
|---------|------------|----------------|
| Read latency | Must consume topic (~seconds) | Random access (~1ms) |
| Write pattern | Append-only | Upsert/update |
| Concurrent access | Consumer groups compete | Concurrent reads OK |
| Query capability | Very limited | Full query support |

The LLM inference latency (500ms–30s) dominates, so state store latency is not a bottleneck.

### Initial Iteration: In-Memory Store

For the first iteration, we provide only an **in-memory state store** for development and testing. Production backends (Redis, PostgreSQL) will be added in future iterations.

### State Schema

```python
from pydantic import BaseModel
from datetime import datetime

class AgentState(BaseModel):
    """Persisted agent state."""
    agent_id: str

    # Conversation
    messages: list[dict]
    system_prompt: str

    # Execution state
    status: Literal["idle", "inferring", "awaiting_tools", "complete"]
    pending_tool_calls: set[str]
    current_correlation_id: str | None

    # Metrics
    created_at: datetime
    updated_at: datetime
    total_tokens: int
    inference_count: int
```

### StateStore Interface (ABC)

The state store uses an **Abstract Base Class** to define the required interface. This ensures explicit inheritance and clear error messages when methods are not implemented.

```python
from abc import ABC, abstractmethod

class StateStore(ABC):
    """Abstract base class for agent state storage."""

    @abstractmethod
    async def get_agent_state(self, agent_id: str) -> AgentState | None:
        """Retrieve the current state for an agent."""
        ...

    @abstractmethod
    async def save_agent_state(self, state: AgentState) -> None:
        """Persist agent state."""
        ...

    @abstractmethod
    async def add_message(self, agent_id: str, message: dict) -> None:
        """Add a message to the agent's conversation history."""
        ...

    @abstractmethod
    async def add_tool_result(self, agent_id: str, tool_call_id: str, result: str) -> None:
        """Record a tool execution result."""
        ...

    @abstractmethod
    async def clear_agent_state(self, agent_id: str) -> None:
        """Clear all state for an agent."""
        ...
```

### In-Memory Implementation (Initial Iteration)

```python
class MemoryStateStore(StateStore):
    """In-memory state store for development and testing."""

    def __init__(self) -> None:
        self._states: dict[str, AgentState] = {}
        self._messages: dict[str, list[dict]] = {}

    async def get_agent_state(self, agent_id: str) -> AgentState | None:
        return self._states.get(agent_id)

    async def save_agent_state(self, state: AgentState) -> None:
        self._states[state.agent_id] = state

    async def add_message(self, agent_id: str, message: dict) -> None:
        if agent_id not in self._messages:
            self._messages[agent_id] = []
        self._messages[agent_id].append(message)

    async def add_tool_result(self, agent_id: str, tool_call_id: str, result: str) -> None:
        await self.add_message(agent_id, {
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": result,
        })

    async def clear_agent_state(self, agent_id: str) -> None:
        self._states.pop(agent_id, None)
        self._messages.pop(agent_id, None)
```

### Future: Redis Implementation

Production Redis implementation will be added in a future iteration:

```python
class RedisStateStore(StateStore):
    """Production state store using Redis."""

    def __init__(self, url: str) -> None:
        self.redis = aioredis.from_url(url)
    # ... implementation deferred
```

### Idempotency

Kafka provides at-least-once delivery. We handle duplicates with idempotency keys:

```python
async def inference_handler(self, msg: Message) -> None:
    # Create idempotency key from message ID + state version
    idempotency_key = f"{msg.id}:{state.updated_at.isoformat()}"

    # Check if already processed
    if await self.state_store.has_processed(idempotency_key):
        return  # Skip duplicate

    # Process...

    # Mark as processed atomically
    await self.state_store.mark_processed(idempotency_key)
```

---

## API Reference

### Public Exports (Initial Iteration)

```python
# calf/__init__.py

# Existing (unchanged)
from calf.client import Calf
from calf.message import Message

# Agents (Initial Iteration)
from calf.agents import Agent, RunContext, AgentOutput

# Tools (Initial Iteration)
from calf.tools import tool, ToolDefinition

# State (Initial Iteration)
from calf.state import StateStore, MemoryStateStore

# Providers (Initial Iteration)
from calf.providers import ModelClient, OpenAIClient
```

### Future Exports (Deferred)

```python
# GroupChat (Future)
from calf.group_chat import GroupChat, Process

# Workflows (Future)
from calf.workflows import Workflow, State

# Handoffs (Future)
from calf.tools import handoff

# Memory (Future - interface only for now)
from calf.memory import Memory

# Production State Stores (Future)
from calf.state import RedisStateStore
```

### Agent API

```python
class Agent[Deps, Output]:
    def __init__(
        self,
        name: str,
        model: str,
        tools: list[ToolDefinition] | None = None,  # Constructor-based tools
        system_prompt: str | Callable[[Deps], str] = "",
        output_type: type[Output] = str,
        temperature: float = 0.7,
        timeout: float = 300.0,
    ) -> None: ...

    # Decorator for agent-specific tools
    def tool(self, func: Callable[P, R]) -> Callable[P, R]: ...

    # Add tools after construction
    def add_tool(self, tool: ToolDefinition) -> None: ...

    # Execute agent (request-reply pattern)
    async def run(
        self,
        task: str,
        deps: Deps | None = None,
    ) -> AgentOutput[Output]: ...

    # Streaming execution
    async def run_stream(
        self,
        task: str,
        deps: Deps | None = None,
    ) -> AsyncIterator[AgentEvent]: ...

    # State persistence
    async def save_state(self) -> dict: ...
    async def load_state(self, state: dict) -> None: ...
```

### GroupChat API (Future)

> **Note:** GroupChat API is deferred to a future iteration.

```python
class GroupChat:
    def __init__(
        self,
        name: str,
        agents: list[Agent],
        process: Process = Process.sequential,
        manager: Agent | None = None,
        entry_point: Agent | None = None,
    ) -> None: ...

    async def run(self, task: str) -> GroupChatOutput: ...
    async def run_stream(self, task: str) -> AsyncIterator[GroupChatEvent]: ...

class Process(Enum):
    sequential = "sequential"      # Agents run in order
    hierarchical = "hierarchical"  # Manager delegates and synthesizes
    swarm = "swarm"                # Agents hand off based on judgment
```

### Workflow API (Future)

> **Note:** Workflow API is deferred to a future iteration. A simpler builder pattern will be designed when this is prioritized.

```python
# API to be designed - will use a simpler builder pattern
# instead of decorator-based node definition
```

---

## Implementation Plan

### Iteration 1: Core Agent System (Current Focus)

**Scope**: Basic agent with tools, event-driven execution, in-memory state

**Deliverables**:
- `Agent` class with generic type parameters `Agent[Deps, Output]`
- Constructor-based tool attachment (`tools=[]` parameter)
- `@calf.tool` decorator for tool definition
- `RunContext` for dependency injection in tools
- `StateStore` ABC with `MemoryStateStore` implementation
- `OpenAIClient` model provider
- `calf.register(agent, state_store, model_client)` for agent registration
- Event handlers for inference and tools
- Request-reply via correlation IDs
- Basic tests and documentation

### Iteration 2: Multi-Agent Coordination (Future)

**Scope**: Handoffs and basic group chat patterns

**Deliverables**:
- `handoff` return type for agent-to-agent transfers
- `GroupChat` class with `Process.sequential` and `Process.swarm`
- Inter-agent event routing
- GroupChat-level state management

### Iteration 3: Advanced Coordination (Future)

**Scope**: Hierarchical teams and production infrastructure

**Deliverables**:
- `Process.hierarchical` with manager agent
- Production state stores (Redis, PostgreSQL)
- Memory interface implementations
- Structured logging and tracing

### Iteration 4: Workflows (Future)

**Scope**: Graph-based workflows with simpler API

**Deliverables**:
- `Workflow` class with builder pattern (design TBD)
- Conditional edges and loops
- Workflow checkpointing
- Comprehensive documentation and examples

---

## Alternatives Considered

### Alternative 1: Traditional Loop-Based Agents

**Approach**: Implement agents as explicit async loops like Pydantic AI.

**Rejected because**:
- Loses the key differentiator (event-driven native)
- Harder to distribute and scale
- Less observable without additional instrumentation
- Would duplicate patterns available in other frameworks

### Alternative 2: Actors Without Broker

**Approach**: Use Python actors (e.g., Ray) without the Kafka broker.

**Rejected because**:
- Loses persistence and replay capability
- Harder to integrate with existing Calf infrastructure
- Less portable to other message brokers

### Alternative 3: State in Kafka (Event Sourcing)

**Approach**: Store all state as Kafka events, reconstruct via replay.

**Rejected because**:
- High latency for state reads (must consume topic)
- Complex recovery logic
- Overkill for typical agent state (which is small)

We chose a hybrid: events flow through Kafka, state lives in Redis.

### Alternative 4: Class-Only API

**Approach**: Only provide `BaseAgent` subclassing, no functional API.

**Rejected because**:
- Too much boilerplate for simple agents
- Less accessible to developers new to the framework
- Other frameworks (Pydantic AI) proved functional API is popular

We chose to support both patterns.

---

## Open Questions

### Q1: Should tool execution be parallel by default?

**Current thinking**: Yes, when multiple tools are called in a single inference response, emit all tool_call events simultaneously. Tool handlers are independent and can execute in parallel.

**Consideration**: Some tools may have dependencies (e.g., fetch URL then parse). May need explicit sequencing option.

### Q2: How should streaming work with structured outputs?

**Current thinking**: Streaming emits partial events (text chunks, tool calls). Final structured output is validated only at the end.

**Alternative**: Progressive validation of partial structured output (like Pydantic AI's `run_stream`).

### Q3: Should we support tool approval (human-in-the-loop)?

**Current thinking**: Not in Phase 1. Can be added later as a special tool handler that waits for human input event.

### Q4: What's the memory interface for RAG?

**Current thinking**: `VectorMemory` has `add()`, `query()`, and `update_context()` methods. The agent automatically calls `update_context()` before inference to inject relevant documents.

**Open**: Should this be synchronous (block inference) or async (best-effort context)?

### Q5: How do we handle agent versioning?

**Current thinking**: Agent name includes version (e.g., `researcher-v2`). Old and new versions can coexist on different channels.

**Alternative**: Version in metadata, single channel with routing logic.

---

## Appendix: Event Schema Reference

### Input Event

```json
{
  "channel": "calf.agent.researcher.input",
  "data": {
    "content": "Research the latest AI trends",
    "history": [],  // Optional: previous messages
    "context": {}   // Optional: additional context
  },
  "metadata": {
    "correlation_id": "req-123",
    "trace_id": "trace-456"
  }
}
```

### Tool Call Event

```json
{
  "channel": "calf.agent.researcher.tool_call.search_web",
  "data": {
    "tool_call_id": "call_abc123",
    "arguments": {
      "query": "AI trends 2024"
    }
  },
  "metadata": {
    "correlation_id": "req-123"
  }
}
```

### Tool Result Event

```json
{
  "channel": "calf.agent.researcher.tool_result",
  "data": {
    "tool_call_id": "call_abc123",
    "tool_name": "search_web",
    "result": "Found 10 results about AI trends...",
    "status": "success"
  },
  "metadata": {
    "correlation_id": "req-123"
  }
}
```

### Output Event

```json
{
  "channel": "calf.agent.researcher.output",
  "data": {
    "content": {
      "summary": "AI trends in 2024 include...",
      "sources": ["https://..."],
      "confidence": 0.92
    },
    "status": "complete"
  },
  "metadata": {
    "correlation_id": "req-123"
  }
}
```

---

## Changelog

| Date | Author | Change |
|------|--------|--------|
| 2026-01-10 | Ryan, Claude | Initial draft |
| 2026-01-13 | Ryan, Claude | Reviewed and confirmed decisions. Reduced initial scope to Agent + Tools only. Changed channel naming to `calf.agent.{name}.{event}`. Changed StateStore to ABC with in-memory implementation. Deferred Teams, Workflows, and production state stores to future iterations. |
