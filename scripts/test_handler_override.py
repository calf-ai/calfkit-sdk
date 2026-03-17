"""Test whether overriding a decorated handler in a BaseNode subclass preserves registration."""

import asyncio
import warnings

# Suppress the deprecation warning from BaseNode
warnings.filterwarnings("ignore", category=DeprecationWarning)

from calfkit.nodes.base_node import BaseNode, publish_to, subscribe_to


# --- Scenario 1: Base subclass with decorated run() ---
class AgentA(BaseNode):
    @subscribe_to("input-topic")
    @publish_to("output-topic")
    async def run(self, msg):
        return f"AgentA processed: {msg}"


# --- Scenario 2: Override run() WITH decorators ---
class AgentB(AgentA):
    @subscribe_to("input-topic")
    @publish_to("output-topic")
    async def run(self, msg):
        return f"AgentB processed: {msg}"


# --- Scenario 3: Override run() WITHOUT decorators ---
class AgentC(AgentA):
    async def run(self, msg):
        return f"AgentC processed: {msg}"


# --- Scenario 4: Don't override run() at all ---
class AgentD(AgentA):
    pass


for label, cls in [
    ("AgentA (base, decorated)", AgentA),
    ("AgentB (override WITH decorators)", AgentB),
    ("AgentC (override WITHOUT decorators)", AgentC),
    ("AgentD (no override)", AgentD),
]:
    print(f"\n=== {label} ===")
    print(f"  _handler_registry keys: {list(cls._handler_registry.keys())}")
    print(f"  _handler_registry: {cls._handler_registry}")

    instance = cls(name="test")
    print(f"  bound_registry keys: {list(instance.bound_registry.keys())}")
    print(f"  bound_registry: {instance.bound_registry}")

    # Check if run() is the one that would actually be called
    if instance.bound_registry:
        handler = list(instance.bound_registry.keys())[0]
        result = asyncio.run(handler("hello"))
        print(f"  handler('hello') => {result}")
    else:
        print(f"  NO HANDLERS REGISTERED")
