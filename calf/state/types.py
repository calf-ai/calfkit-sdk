"""State types for the Calf state store system."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field


@dataclass(frozen=True)
class StateKey:
    """Composite key for state lookup.

    Combines agent_id and thread_id to support:
    - Single agent, single conversation (use default thread_id)
    - Single agent, multiple conversations (different thread_ids)
    - Multiple agents, shared conversation (same thread_id)
    """

    agent_id: str
    thread_id: str = "default"


class AgentState(BaseModel):
    """Persisted agent state."""

    agent_id: str
    thread_id: str = "default"

    # Conversation
    messages: list[dict[str, Any]] = Field(default_factory=list)
    system_prompt: str = ""

    # Execution state
    status: Literal["idle", "running", "awaiting_tool", "complete", "error"] = "idle"
    pending_tool_calls: list[str] = Field(default_factory=list)

    # Metadata
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Metrics (optional, for observability)
    total_tokens: int = 0
    inference_count: int = 0
