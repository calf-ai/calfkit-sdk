# Auto Trading Bots

A distributed multi-agent trading system where three AI agents with different trading strategies compete against each other using live cryptocurrency market data from Coinbase.

Each agent receives real-time ticker updates, reasons about its portfolio using an LLM, and executes trades autonomously — all coordinated through Calfkit's event-driven architecture.

## What This Demonstrates

This example shows how event-driven agents built with Calfkit can consume real-time data streams and act on them independently:

- **Real-time data ingestion**: A WebSocket connector streams live Coinbase ticker data into Kafka, which the agents consume asynchronously.
- **Fan-out via consumer groups**: All three agents subscribe to the same Kafka topic with separate consumer group IDs. Each agent independently receives and processes every market data update without blocking the others.
- **Truly distributed deployment**: Each component (ChatNode, routers, tools, connector) is a standalone process that communicates only through Kafka. They can run on the same machine, on separate servers, in different containers, or across different cloud regions — as long as they share a Kafka broker.
- **Independent scaling**: Any component can be scaled, restarted, or redeployed without affecting the rest of the system.
- **Dynamic tool registration**: An `AgentDispatcher` supports onboarding new agents at runtime without redeploying existing services.
- **Live dashboard**: A Rich terminal dashboard renders portfolio state, positions, and a trade log in real time.

## Architecture

```
Coinbase WebSocket (live ticker_batch)
        │
        ▼
CoinbaseKafkaConnector ── buffers ticks, publishes every 15s ──▶ Kafka
        │
        ├─ consumer group: "momentum"           ──▶ Agent A (Momentum Trader)
        ├─ consumer group: "brainrot-daytrader"  ──▶ Agent B (Brainrot Trader)
        └─ consumer group: "scalper"             ──▶ Agent C (Scalper)
                                                          │
                                                          ▼
                                                       ChatNode (LLM)
                                                          │
                                                          ▼
                                                    Trading Tools ──▶ AccountStore ──▶ Dashboard
```

Each agent has its own system prompt, conversation history, and isolated set of trading tools. They share the ChatNode for LLM inference and the AccountStore for trade execution.

Every box in this diagram is an independent process. They don't import each other or share memory — they only communicate through Kafka topics. This means each component can live on a completely different machine.

## The Three Agents

| Agent | Strategy | Philosophy |
|-------|----------|------------|
| **Momentum** | Follow trends, buy on uptrends, sell on reversals | "The trend is your friend. Let winners run. Cut losers fast." |
| **Brainrot** | Aggressive, max-out positions, never sell at a loss | "YOLO everything. Go big or go home. Vibes-based trading." |
| **Scalper** | Many small, quick trades on tiny price movements | "Trade frequently. Take profits quickly. Minimize hold time." |

Each agent starts with **$100,000** in simulated cash.

## Prerequisites

- Python 3.10+
- Docker (for the local Kafka broker)
- An OpenAI API key

## Quickstart

### 1. Start the Kafka broker

```bash
git clone https://github.com/calf-ai/calfkit-broker && cd calfkit-broker && make dev-up
```

### 2. Set environment variables

```bash
export OPENAI_API_KEY="sk-..."
# If your Kafka broker is not on localhost:
# export KAFKA_BOOTSTRAP_SERVERS="your-broker-host:9092"
```

### 3. Launch each component

The system is composed of four independent deployments. Each one only needs network access to the Kafka broker — they can run on the same machine for local development, or on entirely separate hosts in production.

Start them in this order:

**Deployment 1 — Chat Node** (LLM inference service)

```bash
uv run python examples/auto_trading_bots/chat_node.py
```

**Deployment 2 — Router Nodes** (three agent routers)

```bash
uv run python examples/auto_trading_bots/router_nodes.py
```

**Deployment 3 — Tools, Dispatcher & Dashboard** (trading tools + live portfolio UI)

```bash
uv run python examples/auto_trading_bots/tools_and_dispatcher.py
```

**Deployment 4 — Coinbase Connector** (live market data stream)

```bash
uv run python examples/auto_trading_bots/coinbase_connector.py
```

Once the connector starts, market data flows to all three agents and trades begin appearing on the dashboard.

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
| `--products` | `BTC-USD ETH-USD SOL-USD XRP-USD DOGE-USD FARTCOIN-USD` | Coinbase product IDs to subscribe to |
| `--min-interval` | `0` | Minimum seconds between publishes per product (buffers tickers and publishes only the latest) |
| `--log-level` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

> Note: When run via `coinbase_connector.py` (the entry point used in the quickstart), `MIN_PUBLISH_INTERVAL` is set to `15.0` seconds.

### In-Code Constants

These values can be adjusted directly in source:

| File | Constant | Default | Description |
|------|----------|---------|-------------|
| `trading_tools.py` | `INITIAL_CASH` | `100_000.0` | Starting cash balance per agent |
| `trading_tools.py` | `SUBSCRIBED_PRODUCTS` | 8 products | Products tracked by the price feed and dashboard |
| `chat_node.py` | `model_name` | `gpt-5-nano` | LLM model used for agent reasoning |
| `chat_node.py` | `reasoning_effort` | `low` | OpenAI reasoning effort parameter |
| `chat_node.py` | `max_workers` | `10` | Concurrent LLM request capacity |
| `router_nodes.py` | `SYSTEM_PROMPT_*` | *(see source)* | Trading strategy prompt for each agent |

## File Overview

| File | Purpose |
|------|---------|
| `chat_node.py` | Deploys the LLM inference service (ChatNode) |
| `router_nodes.py` | Deploys three agent routers with distinct trading strategies |
| `coinbase_connector.py` | Entry point that streams Coinbase data to the deployed agents |
| `coinbase_kafka_connector.py` | Core connector class bridging Coinbase WebSocket to Kafka |
| `coinbase_consumer.py` | Standalone Coinbase WebSocket consumer (reference/utility) |
| `tools_and_dispatcher.py` | Deploys trading tools, agent dispatcher, price feed, and dashboard |
| `trading_tools.py` | Trading engine, account store, portfolio view, and tool definitions |
