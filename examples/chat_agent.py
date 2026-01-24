"""
Conversational chat agent with multi-turn memory.

Demonstrates:
- Agent creation with minimal configuration
- Multi-turn conversation with automatic state management
- Interactive REPL-style interface

Run:
    uv run examples/chat_agent.py
"""

import asyncio

from calf import Agent, Calf, InMemoryStateStore
from calf.providers.openai.client import OpenAIClient


# ============== Calf SETUP ==============

calf = Calf()
state_store = InMemoryStateStore()
model_client = OpenAIClient('gpt-5-nano', create_kwargs={'reasoning_effort': 'minimal'})


# ============== AGENT ==============

agent = Agent(
    name="LeAgent",
    model="gpt-5-nano",
    system_prompt="You are a helpful, friendly assistant named LeAgent.",
)

# Register agent with Calf runtime
calf.register(agent, state_store=state_store, model_client=model_client)


async def main() -> None:
    print("=" * 50)
    print("Conversational Chat Agent")
    print("Type 'quit' to exit")
    print("=" * 50)

    while True:
        try:
            user_input = input("\nYou: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n\nGoodbye!")
            break

        if user_input.lower() in ("quit", "exit", "q"):
            print("\nGoodbye!")
            break

        if not user_input:
            continue

        result = await agent.run(user_input)
        print(f"\nAssistant: {result.output}")


if __name__ == "__main__":
    asyncio.run(main())
