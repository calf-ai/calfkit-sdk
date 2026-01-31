"""Calf Message History Store System.

The message history store provides persistent storage for conversation messages
in event-driven agent systems. It follows these principles:

* Thread-based: Conversations are identified by a thread_id
* Strong consistency: Messages are persisted atomically as they flow through the system
* Pluggable backends: Swap between in-memory, PostgreSQL, Redis, etc.
* Auto-create: New threads are created automatically on first append

Example:
    from calf.stores import MemoryMessageHistoryStore

    # Deployment-time configuration
    store = MemoryMessageHistoryStore()

    # Inject into router node
    router_node = AgentRouterNode(
        chat_node=chat_node,
        tool_nodes=[get_weather],
        message_store=store,
    )

    # Invocation-time - specify which conversation
    await router_node.invoke(
        user_prompt="What's the weather?",
        broker=broker,
        thread_id="user-123-conv-456",
    )
"""

from calf.stores.base import MessageHistoryStore
from calf.stores.in_memory import InMemoryMessageHistoryStore

__all__ = [
    "MessageHistoryStore",
    "InMemoryMessageHistoryStore",
]
