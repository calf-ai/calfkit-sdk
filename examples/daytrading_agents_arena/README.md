# Daytrading Agents Arena

A distributed multi-agent trading arena where AI agents with different trading philosophies compete against each other using live cryptocurrency market data from Coinbase.

Each agent receives the same real-time ticker updates and multi-timeframe candlestick data, reasons about its portfolio using an LLM, and executes trades autonomously — all coordinated through Calfkit's event-driven architecture. The goal is to see which trading strategy wins.

## What This Demonstrates

This example shows how event-driven agents built with Calfkit can consume real-time data streams and act on them independently:

- **Strategy-vs-strategy competition**: Three agents with distinct trading philosophies receive identical market data and tools — the only difference is their system prompt.
- **Real-time data ingestion**: A WebSocket connector streams live Coinbase ticker data and polls REST candle endpoints, feeding multi-timeframe OHLCV data into Kafka.
- **Fan-out via consumer groups**: All three agents subscribe to the same Kafka topic with separate consumer group IDs. Each agent independently receives and processes every market data update without blocking the others.
- **Truly distributed deployment**: Each component (ChatNode, routers, tools, connector) is a standalone process that communicates only through Kafka. They can run on the same machine, on separate servers, in different containers, or across different cloud regions — as long as they share a Kafka broker.
- **Independent scaling**: Any component can be scaled, restarted, or redeployed without affecting the rest of the system.
- **Shared tools via ToolContext**: A single deployed set of trading tools serves all agents. Each tool automatically receives the calling agent's identity at runtime via `ToolContext` injection, so adding a new agent requires zero changes to the tools deployment.
- **Live dashboard**: A Rich terminal dashboard renders portfolio state, positions, and a trade log in real time.

## Architecture

```
Coinbase WebSocket (live ticker_batch)
   + REST API (1m / 5m / 15m candles)
        │
        ▼
CoinbaseKafkaConnector ── publishes every 60s ──▶ Kafka
        │
        ├─ consumer group: "momentum"           ──▶ Agent A (Momentum Trader)
        ├─ consumer group: "brainrot-daytrader"  ──▶ Agent B (Brainrot Trader)
        └─ consumer group: "scalper"             ──▶ Agent C (Scalper)
                                                          │
                                                          ▼
                                                    ChatNode(s) (LLM)
                                                          │
                                                          ▼
                                                    Trading Tools ──▶ AccountStore ──▶ Dashboard
```

Each agent has its own system prompt and conversation history. They share a single set of trading tools (which resolve the calling agent via ToolContext) and the AccountStore for trade execution.

The arena supports two deployment modes:
- **Single-model** (quickstart): All agents share one ChatNode and the same LLM.
- **Multi-model**: Each agent targets a named ChatNode backed by a different model (e.g. GPT-5-nano vs DeepSeek). See [Multi-Model Arena](#multi-model-arena) below.

Every box in this diagram is an independent process. They don't import each other or share memory — they only communicate through Kafka topics. This means each component can live on a completely different machine.

## The Three Agents

| Agent | Strategy | Philosophy |
|-------|----------|------------|
| **Momentum** | Follow trends, buy on uptrends, sell on reversals | "The trend is your friend. Let winners run. Cut losers fast." |
| **Brainrot** | Aggressive, max-out positions, never sell at a loss | "YOLO everything. Go big or go home. Vibes-based trading." |
| **Scalper** | Many small, quick trades on tiny price movements | "Trade frequently. Take profits quickly. Minimize hold time." |

Each agent starts with **$100,000** in simulated cash.

## Market Data

Each agent receives every 60 seconds:

- **Live ticker**: current price, best bid, best ask, and 24h volume for each product
- **Multi-timeframe candlesticks (OHLCV)**:
  - 15-min candles (4h ago → 90min ago) — broader trend context
  - 5-min candles (90min ago → 20min ago) — short-term trend
  - 1-min candles (last 20 minutes) — recent price action

## Available Tools

| Tool | Description |
|------|-------------|
| `execute_trade` | Buy or sell a crypto product at the current market price |
| `get_portfolio` | View cash, open positions, cost basis, P&L, and average time held |
| `calculator` | Evaluate math expressions for position sizing, P&L calculations, etc. |

## Prerequisites

- Python 3.10+
- Docker (for the local Kafka broker)
- An OpenAI API key

## Quickstart

### 1. Start the Kafka broker

See the [root README's quickstart](../../README.md) for instructions on setting up a Kafka broker.

### 2. Install example dependencies

```bash
uv sync --group examples
```

### 3. Set environment variables

```bash
export OPENAI_API_KEY="sk-..."
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"  # point this at your Kafka broker
```

> **Note:** `KAFKA_BOOTSTRAP_SERVERS` must be set on every deployment (each process needs to reach the same broker). If you're running everything locally with the default broker setup, `localhost:9092` is the default.

### 4. Launch each component

The system is composed of four independent deployments. Each one only needs network access to the Kafka broker — they can run on the same machine for local development, or on entirely separate hosts in production.

Start them in this order:

**Deployment 1 — Chat Node** (LLM inference service)

```bash
uv run python examples/daytrading_agents_arena/chat_node.py
```

**Deployment 2 — Router Nodes** (three agent routers)

```bash
uv run python examples/daytrading_agents_arena/router_nodes.py
```

**Deployment 3 — Tools & Dashboard** (trading tools + live portfolio UI)

```bash
uv run python examples/daytrading_agents_arena/tools_and_dispatcher.py
```

**Deployment 4 — Coinbase Connector** (live market data stream)

```bash
uv run python examples/daytrading_agents_arena/coinbase_connector.py
```

Once the connector starts, market data flows to all three agents and trades begin appearing on the dashboard.

## Multi-Model Arena

The per-agent deployment scripts let you run each agent against a different LLM. Each ChatNode is deployed with a `--name` that creates a private Kafka topic (`ai_prompted.<name>`), and each router targets a specific ChatNode by name.

### 1. Deploy ChatNodes (one per model)

```bash
# Terminal 1: ChatNode for gpt-5-nano
uv run python examples/daytrading_agents_arena/deploy_chat_node.py \
    --name gpt5-nano --model-id gpt-5-nano --reasoning-effort low

# Terminal 2: ChatNode for DeepSeek
uv run python examples/daytrading_agents_arena/deploy_chat_node.py \
    --name deepseek --model-id deepseek-chat \
    --base-url https://api.deepseek.com/v1 --api-key $DEEPSEEK_API_KEY
```

### 2. Deploy router nodes (one per agent)

```bash
# Terminal 3: Momentum agent → gpt5-nano
uv run python examples/daytrading_agents_arena/deploy_router_node.py \
    --name momentum --chat-node-name gpt5-nano --strategy momentum

# Terminal 4: Brainrot agent → deepseek
uv run python examples/daytrading_agents_arena/deploy_router_node.py \
    --name brainrot-daytrader --chat-node-name deepseek --strategy brainrot

# Terminal 5: Scalper agent → gpt5-nano
uv run python examples/daytrading_agents_arena/deploy_router_node.py \
    --name scalper --chat-node-name gpt5-nano --strategy scalper
```

### 3. Deploy tools, dashboard, and connector

These are the same as the single-model quickstart:

```bash
# Terminal 6: Tools & dashboard
uv run python examples/daytrading_agents_arena/tools_and_dispatcher.py

# Terminal 7: Coinbase connector
uv run python examples/daytrading_agents_arena/coinbase_connector.py
```

### deploy_chat_node.py CLI reference

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--name` | Yes | — | ChatNode name (becomes topic `ai_prompted.<name>`) |
| `--model-id` | Yes | — | Model ID passed to `OpenAIModelClient` |
| `--base-url` | No | `None` (OpenAI default) | Base URL for OpenAI-compatible providers |
| `--api-key` | No | `$OPENAI_API_KEY` | API key for the provider |
| `--bootstrap-servers` | No | `$KAFKA_BOOTSTRAP_SERVERS` / `localhost:9092` | Kafka broker address |
| `--max-workers` | No | `10` | Concurrent inference workers |
| `--reasoning-effort` | No | `None` | Reasoning effort for reasoning models (e.g. `"low"`) |

### deploy_router_node.py CLI reference

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--name` | Yes | — | Agent name (consumer group + identity) |
| `--chat-node-name` | Yes | — | Name of the deployed ChatNode to target |
| `--strategy` | Yes | — | Trading strategy: `momentum`, `brainrot`, or `scalper` |
| `--bootstrap-servers` | No | `$KAFKA_BOOTSTRAP_SERVERS` / `localhost:9092` | Kafka broker address |

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address. Set this on every deployment to point at your shared broker. |
| `OPENAI_API_KEY` | *(required)* | OpenAI API key. Only needed on the Chat Node deployment. |

### Coinbase Connector Options

The connector (`coinbase_kafka_connector.py`) accepts CLI arguments when run directly:

| Flag | Default | Description |
|------|---------|-------------|
| `--bootstrap-servers` | `localhost:9092` | Kafka broker address |
| `--products` | `BTC-USD FARTCOIN-USD SOL-USD` | Coinbase product IDs to subscribe to |
| `--min-interval` | `0` | Minimum seconds between publishes per product (buffers tickers and publishes only the latest) |
| `--log-level` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

> Note: When run via `coinbase_connector.py` (the entry point used in the quickstart), `MIN_PUBLISH_INTERVAL` is set to `60.0` seconds.

### In-Code Constants

These values can be adjusted directly in source:

| File | Constant | Default | Description |
|------|----------|---------|-------------|
| `trading_tools.py` | `INITIAL_CASH` | `100_000.0` | Starting cash balance per agent |
| `trading_tools.py` | `SUBSCRIBED_PRODUCTS` | 3 products | Products tracked by the price feed and dashboard |
| `chat_node.py` | `model_name` | `gpt-5-nano` | LLM model used for agent reasoning |
| `chat_node.py` | `reasoning_effort` | `low` | OpenAI reasoning effort parameter |
| `chat_node.py` | `max_workers` | `10` | Concurrent LLM request capacity |
| `router_nodes.py` | `SYSTEM_PROMPT_*` | *(see source)* | Trading strategy prompt for each agent |

## File Overview

| File | Purpose |
|------|---------|
| `chat_node.py` | Deploys a single shared ChatNode (all agents use one model) |
| `router_nodes.py` | Deploys all three agent routers in one process (single-model mode) |
| `deploy_chat_node.py` | Deploys a single **named** ChatNode via CLI (multi-model mode) |
| `deploy_router_node.py` | Deploys a single **named** agent router via CLI (multi-model mode) |
| `coinbase_connector.py` | Entry point that streams Coinbase data to the deployed agents |
| `coinbase_kafka_connector.py` | Core connector class bridging Coinbase WebSocket + REST to Kafka |
| `coinbase_consumer.py` | Coinbase WebSocket/REST consumer with multi-timeframe CandleBook |
| `tools_and_dispatcher.py` | Deploys trading tools, price feed, and dashboard |
| `trading_tools.py` | Trading engine, account store, portfolio view, calculator, and shared tool definitions (using ToolContext) |
