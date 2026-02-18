import asyncio
import traceback

from calfkit.broker.broker import BrokerClient
from calfkit.models.event_envelope import EventEnvelope
from calfkit.nodes.agent_router_node import AgentRouterNode

# Import tools from tool_nodes for schema/routing info
from calfkit.runners.service_client import RouterServiceClient

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
    print("\nConnecting to Kafka broker at localhost:9092...")
    broker = BrokerClient(bootstrap_servers="localhost:9092")
    try:
        print("=" * 50)
        print("Agent Invocation Client")
        print("=" * 50)

        # Connect to the real Kafka broker

        # Create router node client for invoking.
        # No patch for tools and system prompts
        router_node = AgentRouterNode()

        print("Ready to invoke agent.\n")

        # Test queries
        test_queries = [
            "What's the current weather in Tokyo and Cupertino?",
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
                router_client = RouterServiceClient(broker, router_node)
                resp = await router_client.request(user_prompt=query)
                print(f"  Sent with correlation_id: {resp.correlation_id[:8]}...")

                # Wait for the response
                timeout = 30.0
                final_msg = None
                async with asyncio.timeout(timeout):
                    final_msg = await resp.get_final_response()
                text = getattr(final_msg, "text", None)
                if text:
                    print(f"  Assistant: {text}")
                else:
                    print(f"  Response: {final_msg}")

            except Exception as e:
                print(f"  ERROR: {e}")

                traceback.print_exc()

        # Cleanup
        print("\n" + "=" * 50)
        print("Test completed!")
        print("=" * 50)

    finally:
        await broker.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInvocation cancelled.")
