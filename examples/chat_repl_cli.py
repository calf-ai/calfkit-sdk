import asyncio
import logging
import time
import uuid
from typing import Annotated

import uuid_utils
from faststream import Context

from calfkit.broker.broker import BrokerClient
from calfkit.models.event_envelope import EventEnvelope
from calfkit.nodes.agent_router_node import AgentRouterNode
from tests.utils import wait_for_condition

logging.disable(logging.CRITICAL)


# CHAT REPL CLI - Interactive chat interface for the agent.

# Usage:
#     uv run python examples/real_broker/repl_cli.py

# Prerequisites:
#     - Kafka broker running at localhost:9092
#     - All nodes deployed (tool_nodes.py, chat_node.py, router_node.py)


class ChatReplCli:
    """Interactive REPL CLI for chatting with the agent."""

    def __init__(self, broker: BrokerClient, router_node: AgentRouterNode):
        self.broker = broker
        self.router_node = router_node
        self.thread_id = str(uuid.uuid4())
        self.pending_responses: dict[str, asyncio.Queue[EventEnvelope]] = {}
        self.response_timeout = 60.0

    async def _response_collector(
        self,
        event_envelope: EventEnvelope,
        correlation_id: Annotated[str, Context()],
    ):
        """Collect responses from the agent."""
        if correlation_id not in self.pending_responses:
            self.pending_responses[correlation_id] = asyncio.Queue()
        await self.pending_responses[correlation_id].put(event_envelope)

    def clear_thread(self):
        """Generate a new thread ID, starting a fresh conversation."""
        self.thread_id = str(uuid.uuid4())
        print(f"[Thread cleared. New thread_id: {self.thread_id[:8]}...]")

    async def send_message(self, user_message: str) -> tuple[str | None, float]:
        """Send a message to the agent and wait for the response.

        Returns:
            Tuple of (response_text, elapsed_time_seconds)
        """
        start_time = time.monotonic()
        correlation_id = uuid_utils.uuid7().hex
        correlation_id = await self.router_node.invoke(
            user_prompt=user_message,
            broker=self.broker,
            final_response_topic="final_response",
            thread_id=self.thread_id,
            correlation_id=correlation_id,
        )

        # Wait for response
        try:
            await wait_for_condition(
                lambda: correlation_id in self.pending_responses,
                timeout=self.response_timeout,
            )
            response_queue = self.pending_responses[correlation_id]
            response = await asyncio.wait_for(response_queue.get(), timeout=5.0)

            elapsed = time.monotonic() - start_time
            if response.is_end_of_turn and response.latest_message_in_history:
                text = getattr(response.latest_message_in_history, "text", None)
                if text:
                    return text, elapsed
            return None, elapsed
        except asyncio.TimeoutError:
            elapsed = time.monotonic() - start_time
            return "[Error: Timeout waiting for response]", elapsed

    async def run(self):
        """Run the REPL loop."""
        print("=" * 50)
        print("Agent REPL CLI")
        print("=" * 50)
        print(f"Thread ID: {self.thread_id[:8]}...")
        print("Type '/clear' to start a new conversation thread")
        print("Type '/quit' or Ctrl+D to exit")
        print("=" * 50)

        while True:
            try:
                # Get user input
                user_input = await asyncio.to_thread(input, "\n> ")

                # Handle empty input
                if not user_input.strip():
                    continue

                # Handle commands
                if user_input.strip() == "/clear":
                    self.clear_thread()
                    continue

                if user_input.strip() in ("/quit", "/exit", "/q"):
                    print("\nGoodbye!")
                    break

                # Send message and display response
                print()
                response, elapsed = await self.send_message(user_input)
                if response:
                    print("-" * 50)
                    print(response)
                    print("-" * 50)
                    print(f"Response time: {elapsed:.2f}s")

            except EOFError:
                print("\n\nGoodbye!")
                break
            except KeyboardInterrupt:
                print("\n\nGoodbye!")
                break
            except Exception as e:
                print(f"\n[Error: {e}]")


async def main():
    # Connect to the real Kafka broker
    broker = BrokerClient(bootstrap_servers="localhost:9092")

    # Check if broker is reachable by trying to start it
    print("Checking Kafka broker connection...")
    try:
        await broker.start()
        is_alive = await broker.ping(timeout=5.0)
        if not is_alive:
            raise Exception("BrokerClient ping failed")
        print("BrokerClient connection successful!")
    except Exception as e:
        print("Error: Unable to connect to Kafka broker at localhost:9092")
        print(f"Details: {e}")
        print("Please ensure the broker is running before starting the REPL CLI.")
        print("  Run: cd /Users/ryan/Projects/calf-broker && make dev-up")
        return

    # Create router node client
    router_node = AgentRouterNode()

    # Create REPL CLI
    cli = ChatReplCli(broker, router_node)

    # Set up response collector
    @broker.subscriber("final_response")
    async def collect_response(
        event_envelope: EventEnvelope,
        correlation_id: Annotated[str, Context()],
    ):
        await cli._response_collector(event_envelope, correlation_id)

    try:
        await cli.run()
    finally:
        await broker.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nREPL terminated.")
