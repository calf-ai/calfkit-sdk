"""Deploy a single named ChatNode backed by an OpenAI-compatible model.

Run one instance per model. The node listens on its private topic
``ai_prompted.<name>`` so that agent routers can target it by name.

Example:
    uv run python examples/daytrading_agents_arena/deploy_chat_node.py \
        --name gpt5-nano --model-id gpt-5-nano --reasoning-effort low

    uv run python examples/daytrading_agents_arena/deploy_chat_node.py \
        --name deepseek --model-id deepseek-chat \
        --base-url https://api.deepseek.com/v1 --api-key $DEEPSEEK_API_KEY
"""

import argparse
import asyncio
import os
import sys

from dotenv import load_dotenv

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.chat_node import ChatNode
from calfkit.providers.pydantic_ai.openai import OpenAIModelClient
from calfkit.runners.service import NodesService

load_dotenv()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deploy a named ChatNode for per-model inference.",
    )
    parser.add_argument(
        "--name",
        required=True,
        help="ChatNode name (becomes private topic ai_prompted.<name>)",
    )
    parser.add_argument(
        "--model-id",
        required=True,
        help="Model ID passed to OpenAIModelClient (e.g. gpt-5-nano, deepseek-chat)",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="Base URL for OpenAI-compatible providers (default: OpenAI)",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="API key for the provider (default: $OPENAI_API_KEY)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Kafka bootstrap servers address",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="Concurrent inference workers (default: 1)",
    )
    parser.add_argument(
        "--reasoning-effort",
        default=None,
        help='Reasoning effort for reasoning models (e.g. "low")',
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    # Resolve API key: explicit flag > env var
    api_key = args.api_key or os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: No API key provided.")
        print("Pass --api-key or set OPENAI_API_KEY.")
        sys.exit(1)

    print("=" * 50)
    print(f"ChatNode Deployment: {args.name}")
    print("=" * 50)

    print(f"\nConnecting to Kafka broker at {args.bootstrap_servers}...")
    broker = BrokerClient(bootstrap_servers=args.bootstrap_servers)

    print(f"Configuring model client: {args.model_id}")
    model_client = OpenAIModelClient(
        model_name=args.model_id,
        base_url=args.base_url,
        api_key=api_key,
        reasoning_effort=args.reasoning_effort,
    )

    chat_node = ChatNode(model_client, name=args.name)
    service = NodesService(broker)
    service.register_node(chat_node, max_workers=args.max_workers)

    print(f"  - Name:  {args.name}")
    print(f"  - Model: {args.model_id}")
    print(f"  - Topic: {chat_node.entrypoint_topic}")
    print(f"  - Workers: {args.max_workers}")
    if args.base_url:
        print(f"  - Base URL: {args.base_url}")
    if args.reasoning_effort:
        print(f"  - Reasoning effort: {args.reasoning_effort}")

    print("\nChat node ready. Waiting for requests...")
    await service.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nChat node stopped.")
