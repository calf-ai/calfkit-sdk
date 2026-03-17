"""Test class-level registry keyed by method name instead of function object."""

from typing import Any
from collections.abc import Callable


def publish_to(topic_name: str) -> Callable[[Any], Any]:
    def decorator(fn: Any) -> Any:
        fn._publish_to_topic_name = topic_name
        return fn
    return decorator


class Base:
    # Registry keyed by method NAME, not function object
    _handler_registry: dict[str, dict[str, str]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Inherit parent's registry, then overlay with new decorated methods
        cls._handler_registry = {}
        for klass in reversed(cls.__mro__):
            if hasattr(klass, "_handler_registry"):
                cls._handler_registry.update(klass._handler_registry)

        # Scan THIS class's own methods for new/overridden decorators
        for name, attr in cls.__dict__.items():
            topic = getattr(attr, "_publish_to_topic_name", None)
            if topic:
                cls._handler_registry[name] = {"publish_topic": topic}

    def get_bound_handlers(self):
        """Resolve method names to actual bound methods via MRO."""
        return {
            name: (getattr(self, name), topics)
            for name, topics in self._handler_registry.items()
        }


# --- Scenario 1: Base subclass with decorated run() ---
class AgentA(Base):
    @publish_to("my-topic")
    async def run(self, msg):
        return f"AgentA: {msg}"


# --- Scenario 2: Override WITHOUT decorator ---
class AgentB(AgentA):
    async def run(self, msg):
        return f"AgentB: {msg}"


# --- Scenario 3: Override WITH new decorator ---
class AgentC(AgentA):
    @publish_to("different-topic")
    async def run(self, msg):
        return f"AgentC: {msg}"


# --- Scenario 4: No override ---
class AgentD(AgentA):
    pass


import asyncio

for label, cls in [
    ("AgentA (base, decorated)", AgentA),
    ("AgentB (override, no decorator)", AgentB),
    ("AgentC (override, new decorator)", AgentC),
    ("AgentD (no override)", AgentD),
]:
    print(f"\n=== {label} ===")
    print(f"  _handler_registry: {cls._handler_registry}")
    instance = cls()
    handlers = instance.get_bound_handlers()
    for name, (method, topics) in handlers.items():
        result = asyncio.run(method("hello"))
        print(f"  handler '{name}': topics={topics}, result={result}")
