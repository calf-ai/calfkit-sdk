# MCP Adaptor Quickstart

Minimal end-to-end example showing the MCP adaptor pattern: codegen-generated
schemas + shared module + split agent / bridge workers.

This example uses [`@modelcontextprotocol/server-everything`](https://github.com/modelcontextprotocol/servers/tree/main/src/everything),
the reference MCP server that exposes a variety of tools for testing.

## Prerequisites

- Python 3.10+
- Node.js (for the npx-installable MCP server)
- A running Kafka broker (use the [calfkit-broker](https://github.com/calf-ai/calfkit-broker) for local dev)
- `OPENAI_API_KEY` exported
- `pip install calfkit[mcp-codegen]` (the `mcp-codegen` extra installs typer for the CLI)

## Layout

```
quickstart_mcp/
├── README.md             # this file
├── everything_schemas.py # generated; committed
├── shared.py             # shared McpServer declaration
├── agent_service.py      # the agent worker
├── tools_service.py      # the bridge worker (hosts MCP subprocess)
└── invoke.py             # one-shot client that triggers the agent
```

## Setup

### 1. Generate the schemas (one-time, committed)

```bash
calfkit mcp codegen everything \
    --command "npx -y @modelcontextprotocol/server-everything" \
    --output everything_schemas.py
```

The CLI spawns the MCP server briefly, runs `tools/list`, and writes the
`McpToolDef` constants into `everything_schemas.py`. Commit this file.

### 2. Start the Kafka broker

```bash
# In another terminal — clone+start the calfkit-broker container
git clone https://github.com/calf-ai/calfkit-broker && cd calfkit-broker && make dev-up
```

### 3. Start the bridge worker

The bridge spawns the MCP subprocess and serves tool calls over Kafka.

```bash
export OPENAI_API_KEY=sk-...    # (not needed by the bridge but consistent across workers)
python tools_service.py
```

### 4. Start the agent worker

```bash
export OPENAI_API_KEY=sk-...
python agent_service.py
```

### 5. Invoke

```bash
python invoke.py
```

## How it works

- `shared.py` declares one `mcp(...)` instance with the codegen-supplied tools.
- The bridge worker hosts the MCP subprocess and dispatches tool calls.
- The agent worker only imports the schemas — it never spawns the subprocess.
- They communicate over Kafka topics named `mcp.everything.<tool>.input` / `.output`.

If `@modelcontextprotocol/server-everything` is upgraded with new tools, run:

```bash
calfkit mcp codegen everything --command "npx -y @modelcontextprotocol/server-everything" --output everything_schemas.py --check
```

`--check` exits non-zero on drift without overwriting the file (CI-friendly).
Re-run without `--check` to refresh and commit the diff.
