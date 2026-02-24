# Daytrading Agents Arena

A distributed multi-agent trading arena where AI agents compete against each other using live cryptocurrency market data from Coinbase. Each agent receives the same real-time market updates, reasons about its portfolio using an LLM, and executes trades autonomously — all coordinated through Calfkit's event-driven architecture.

## Architecture

```
Coinbase WebSocket + REST API
        │
        ▼
CoinbaseKafkaConnector ──▶ Kafka ──▶ Agent Router(s) ──▶ ChatNode(s) (LLM)
                                                │
                                                ▼
                                          Trading Tools ──▶ Dashboard
```

Each box is an independent process communicating only through Kafka. They can run on the same machine, on separate servers, or across different cloud regions.

Key design points:
- **Per-agent model selection**: Each agent targets a named ChatNode, so different agents can use different LLMs.
- **Fan-out via consumer groups**: Every agent independently receives every market data update.
- **Shared tools via ToolContext**: A single set of trading tools serves all agents — each tool resolves the calling agent's identity at runtime.
- **Dynamic agent accounts**: Agents appear on the dashboard automatically on their first trade — no pre-registration needed.

## Prerequisites

- Python 3.10+
- A running Kafka broker (see the [root README](../../README.md) for setup)
- An API key for your LLM provider

## Quickstart

Install dependencies:

```bash
uv sync --group examples
```

Then launch each component in its own terminal. All components need access to the same Kafka broker.

### 1. Deploy a ChatNode (LLM inference)

Deploy one ChatNode per model. Multiple agents can share the same ChatNode.

```bash
# OpenAI model
uv run python examples/daytrading_agents_arena/deploy_chat_node.py \
    --name gpt5-nano --model-id gpt-5-nano --bootstrap-servers <broker> \
    --reasoning-effort low

# OpenAI-compatible provider (e.g. DeepInfra)
uv run python examples/daytrading_agents_arena/deploy_chat_node.py \
    --name minimax --model-id MiniMaxAI/MiniMax-M2.5 --bootstrap-servers <broker> \
    --base-url https://api.deepinfra.com/v1/openai --api-key <key>
```

### 2. Deploy agent routers

Deploy one router per agent. Each targets a ChatNode by name and uses a trading strategy. See `deploy_router_node.py` for the full system prompts.

```bash
uv run python examples/daytrading_agents_arena/deploy_router_node.py \
    --name agent-gpt5-nano --chat-node-name gpt5-nano --strategy default \
    --bootstrap-servers <broker>

uv run python examples/daytrading_agents_arena/deploy_router_node.py \
    --name agent-minimax --chat-node-name minimax --strategy momentum \
    --bootstrap-servers <broker>
```

### 3. Deploy tools & dashboard

```bash
uv run python examples/daytrading_agents_arena/tools_and_dispatcher.py
```

Reads `KAFKA_BOOTSTRAP_SERVERS` from your `.env` file or environment.

### 4. Start the Coinbase connector

```bash
uv run python examples/daytrading_agents_arena/coinbase_connector.py
```

Once the connector starts, market data flows to all agents and trades appear on the dashboard.

## CLI Reference

### deploy_chat_node.py

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--name` | Yes | — | ChatNode name (becomes topic `ai_prompted.<name>`) |
| `--model-id` | Yes | — | Model ID (e.g. `gpt-5-nano`, `deepseek-chat`) |
| `--bootstrap-servers` | Yes | — | Kafka broker address |
| `--base-url` | No | OpenAI | Base URL for OpenAI-compatible providers |
| `--api-key` | No | `$OPENAI_API_KEY` | API key for the provider |
| `--max-workers` | No | `1` | Concurrent inference workers |
| `--reasoning-effort` | No | `None` | For reasoning models (e.g. `"low"`) |

### deploy_router_node.py

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--name` | Yes | — | Agent name (consumer group + identity) |
| `--chat-node-name` | Yes | — | Name of the deployed ChatNode to target |
| `--strategy` | Yes | — | Trading strategy: `default`, `momentum`, `brainrot`, or `scalper` |
| `--bootstrap-servers` | Yes | — | Kafka broker address |

## Available Tools

| Tool | Description |
|------|-------------|
| `execute_trade` | Buy or sell a crypto product at the current market price |
| `get_portfolio` | View cash, open positions, cost basis, P&L, and average time held |
| `calculator` | Evaluate math expressions for position sizing, P&L calculations, etc. |

## Configuration

| File | Constant | Default | Description |
|------|----------|---------|-------------|
| `trading_tools.py` | `INITIAL_CASH` | `100_000.0` | Starting cash balance per agent |
| `coinbase_kafka_connector.py` | `DEFAULT_PRODUCTS` | 3 products | Products tracked by the price feed |

## File Overview

| File | Purpose |
|------|---------|
| `deploy_chat_node.py` | Deploys a named ChatNode (one per model) |
| `deploy_router_node.py` | Deploys a named agent router (one per agent) |
| `tools_and_dispatcher.py` | Deploys trading tools, price feed, and dashboard |
| `coinbase_connector.py` | Streams live Coinbase market data to agents |
| `coinbase_kafka_connector.py` | WebSocket ticker stream + periodic Kafka publishing to agents |
| `coinbase_consumer.py` | REST candle polling + in-memory PriceBook and CandleBook |
| `trading_tools.py` | Trading engine, account store, portfolio view, and tool definitions |
