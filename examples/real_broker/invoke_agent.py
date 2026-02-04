import asyncio
import traceback
from typing import Annotated

from faststream import Context

from calfkit.broker.broker import Broker
from calfkit.models.event_envelope import EventEnvelope
from calfkit.nodes.agent_router_node import AgentRouterNode

# Import tools from tool_nodes for schema/routing info
from tests.utils import wait_for_condition

# Invoke Agent - Sends requests to the agent and collects responses.

# This is the client that interacts with the deployed agent system.

# Usage:
#     uv run python examples/real_broker/invoke_agent.py

# Prerequisites:
#     - Kafka broker running at localhost:9092
#     - All nodes deployed (tool_nodes.py, chat_node.py, router_node.py)

# Store for collecting final responses
final_responses: dict[str, asyncio.Queue[EventEnvelope]] = {}


async def main():
    print("=" * 50)
    print("Agent Invocation Client")
    print("=" * 50)

    # Connect to the real Kafka broker
    print("\nConnecting to Kafka broker at localhost:9092...")
    broker = Broker(bootstrap_servers="localhost:9092")

    # Create router node client for invoking.
    # No patch for tools and system prompts
    router_node = AgentRouterNode()

    # Set up a subscriber to collect final responses
    @broker.subscriber("final_response")
    async def collect_response(
        event_envelope: EventEnvelope, correlation_id: Annotated[str, Context()]
    ):
        if correlation_id not in final_responses:
            final_responses[correlation_id] = asyncio.Queue()
        await final_responses[correlation_id].put(event_envelope)

    print("Ready to invoke agent.\n")

    # Test queries
    test_queries = [
        "What's the current weather in Tokyo?",
        "What's the stock price of Apple?",
        "What's the exchange rate from USD to JPY?",
    ]

    print("=" * 50)
    print("Running Test Queries")
    print("=" * 50)

    for i, query in enumerate(test_queries, 1):
        print(f"\n[Test {i}] User: {query}")

        try:
            # Invoke the agent
            correlation_id = await router_node.invoke(
                user_prompt=query,
                broker=broker,
                final_response_topic="final_response",
            )
            print(f"  Sent with correlation_id: {correlation_id[:8]}...")

            # Wait for the response
            timeout = 30.0
            await wait_for_condition(
                lambda: correlation_id in final_responses,
            )

            # Get the response
            response_queue = final_responses[correlation_id]
            try:
                response = await asyncio.wait_for(response_queue.get(), timeout=timeout)

                if response.final_response and response.latest_message_in_history:
                    text = getattr(response.latest_message_in_history, "text", None)
                    if text:
                        print(f"  Assistant: {text}")
                    else:
                        print(f"  Response: {response.latest_message_in_history}")
                else:
                    print(f"  Intermediate response: {response.kind}")
            except asyncio.TimeoutError:
                print("  ERROR: Timeout waiting for response")

        except Exception as e:
            print(f"  ERROR: {e}")

            traceback.print_exc()

    # Cleanup
    print("\n" + "=" * 50)
    print("Test completed!")
    print("=" * 50)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInvocation cancelled.")
