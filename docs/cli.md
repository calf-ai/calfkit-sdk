# Calfkit CLI reference

The `ck` command bundles the SDK's command-line tooling.

Commands:

| Command | Purpose |
| --- | --- |
| [`ck run`](#ck-run) | Run node(s) as a worker for local development (no `Worker` boilerplate). |
| [`ck chat`](#ck-chat) | Chat with an online agent from an interactive terminal REPL. |
| [`ck dev`](#ck-dev) | Run against a zero-setup local mesh: connect to (or spawn) a managed dev broker. |
| [`ck topics`](#ck-topics) | Best-effort create the Kafka topics a set of nodes reference. |

---

## `ck run`

Run one or more nodes as a worker without writing the
`Client.connect(...)` → `Worker(...)` → `worker.run()` boilerplate — point it at
a node and it serves, in the spirit of `fastapi dev`.

```text
ck run TARGET [TARGET ...] [OPTIONS]
```

> **Development only.** `ck run` is a convenience for running nodes locally.
> Production deployments should use an explicit `Worker` so startup, scaling, and
> topic governance stay under operator control — see
> [Production deployment](#production-deployment) below.

### Targets

Each `TARGET` is a dotted **`module:attr`** import path (like `uvicorn main:app`):

- `attr` may be a **single node** or an **iterable of nodes** — iterables are
  expanded, so `mypkg.workers:all_nodes` (a list) works.
- Pass **multiple targets** to run them in one worker:
  `ck run app.agents:planner app.tools:search`.
- Targets de-duplicate by `node_id`, so listing the same node twice is harmless.

Targets are resolved with Python's import machinery, so the module must be
**importable** from where you run the command. By default the current directory
is placed on the import path (see `--app-dir`), so run from your project root:

```console
$ # project root contains weather_tool.py
$ ck run weather_tool:get_weather

$ # nested package (dots, not slashes; no .py suffix)
$ ck run app.tools.weather:get_weather
```

Nested directories work as packages, including
[PEP&nbsp;420 namespace packages](https://peps.python.org/pep-0420/) (no
`__init__.py` required). If a target module uses **relative imports**
(`from . import ...`), make it a regular package (add `__init__.py`).

### Node files need no boilerplate

Because `ck run` imports your module and runs the node, the file only needs
the node definition at module scope — no `main()`, no `asyncio.run(...)`, no
`Worker`:

```python
# weather_tool.py — the whole file
from calfkit.nodes import agent_tool

@agent_tool
def get_weather(location: str) -> str:
    """Get the current weather at a location"""
    return f"It's sunny in {location}"
```

If a file *does* keep its own runnable entrypoint, guard it with
`if __name__ == "__main__": asyncio.run(...)` — the guard does not fire on
import, so `ck run` ignores it. (A `Worker.run()` left at module top level
would block the import; keep it under the guard.)

### Options

| Flag | Default | Description |
| --- | --- | --- |
| `--host`, `-H` | `$CALFKIT_MESH_URL` → `localhost` | Kafka bootstrap server(s), comma-separated. Precedence: this flag **>** `$CALFKIT_MESH_URL` **>** `localhost`. |
| `--provision` | off | Opt-in dev topic auto-creation (**experimental**; `replication_factor=1`, no ACLs). See [Topic provisioning](topic-provisioning.md). |
| `--enable-idempotence` | off | Turn on idempotent producers across every producer the worker runs. Off by default so the worker runs against brokers without producer-id support; needs a broker that implements the producer-id / transaction coordinator to enable. |
| `--reload` | off | Watch source files and restart the worker on change (see [Reload](#reload)). |
| `--reload-dir` | current dir | Directory to watch with `--reload`. Repeatable. |
| `--app-dir` | `.` (current dir) | Directory inserted on `sys.path` for resolving `module:attr` targets. |
| `--group-id` | each node's id | Kafka consumer-group override applied to every node. |
| `--env-file` | `./.env` if present | dotenv file to load before starting. A *missing explicit* `--env-file` warns (it is not silently ignored). |

### Reload

`--reload` runs the worker under a [watchfiles](https://github.com/samuelcolvin/watchfiles)
supervisor, the same mechanism `uvicorn`/`fastapi dev` use: a lightweight parent
process watches `.py` files and re-spawns a fresh worker process (clean
re-import) on every change.

- **Config errors fail fast.** Before starting the supervisor, the targets are
  pre-flighted (resolved + validated) in the parent, so a bad `module:attr`, an
  import error, a non-node object, or zero resolved nodes exits `2` immediately
  rather than leaving an idle watcher.
- **Runtime failures restart on edit.** A failure that only appears once the
  worker is live (e.g. the broker is unreachable) is reported in the child
  process and the supervisor keeps watching — fix the code and save to retry.
  This is the standard dev-server reload contract.

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Clean shutdown (Ctrl-C, or the worker stopped on its own). |
| `2` | Configuration error — bad `module:attr` spec, import failure, non-node object, or zero nodes resolved (surfaced before the worker starts, including under `--reload`). |

### Examples

```console
$ # One tool node
$ ck run weather_tool:get_weather

$ # An agent and its tool in one worker, against a specific broker
$ ck run agent_service:agent weather_tool:get_weather --host localhost:9092

$ # Auto-restart on edits, auto-create dev topics
$ ck run agent_service:agent --reload --provision

$ # Resolve targets relative to ./src, load a custom env file
$ ck run workers:all_nodes --app-dir src --env-file .env.local
```

### Production deployment

`ck run` is for development. In production, deploy each node with an
explicit `Worker`:

```python
# serve_tool.py
import asyncio
from calfkit.client import Client
from calfkit.worker import Worker
from weather_tool import get_weather

async def main():
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[get_weather])
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
```

```console
$ python serve_tool.py
```

---

## `ck chat`

Chat with an agent running on your mesh from an interactive REPL — discover the
agents currently online, pick one (or name it directly), then hold a multi-turn
conversation. Each turn streams the agent's intermediate work — its messages, tool
calls and results, and any handoffs — live, followed by its answer.

```text
ck chat [NAME] [OPTIONS]
```

With no `NAME`, `ck chat` lists the agents online and prompts you to choose;
`ck chat researcher` skips the picker and connects to that agent directly (and
exits `2` if it isn't online). There is no "back" — leave and rerun to switch
agents.

### A session

```console
$ ck chat
Discovering agents...
Online agents

  1  researcher   Deep web research with citations
  2  support-bot  Handles customer support tickets and refunds

Select an agent [1-2, q to quit]: 2

Chatting with support-bot. Type /exit or press Ctrl-D to leave.
----------------------------------------------------------------

you > my order #4471 never arrived

support-bot
  [message]      Let me pull up that order for you.
  [tool call]    lookup_order(id='4471')
  [tool result]  shipped 2026-06-24, in transit

support-bot > Order #4471 shipped on the 24th and is in transit.
Estimated delivery is tomorrow. Want me to start a trace?

you >
```

Each turn renders in three tiers: **your input** (`you >`), the agent's live
**work log** (indented, one line per step, tagged `[message]` / `[tool call]` /
`[tool result]` / `[tool error]` / `[handoff]`), and the agent's **answer**
(`‹agent› >`). The work log is headed by the agent's name, so it is always clear
which agent did the work; a mid-turn handoff prints the new agent's name as its
own header.

**Handoffs stick.** A handoff transfers control, so when a turn's answer comes from
a different agent than you addressed, `ck chat` re-binds to that agent — your next
message goes to it, not back to the one you started with — and prints a
`(now chatting with ‹agent›)` note. A consult (`message_agent`) keeps control, so it
does not move you. `message_history` is threaded across the change, so the agent that
takes over has the full conversation.

### Leaving

`/exit`, `/quit`, or Ctrl-D end the session; Ctrl-C exits at any time, including
mid-turn. Any other input is sent to the agent verbatim.

### Options

| Flag | Default | Description |
| --- | --- | --- |
| `--host`, `-H` | `$CALFKIT_MESH_URL` → `localhost` | Kafka bootstrap server(s), comma-separated. Precedence: this flag **>** `$CALFKIT_MESH_URL` **>** `localhost`. |
| `--provision` | off | Opt-in creation of this client's reply inbox topic (**experimental**). Needed on brokers that don't auto-create topics (e.g. Tansu). The agent's own topics are provisioned by *its* worker (`ck run --provision`), not here. See [Topic provisioning](topic-provisioning.md). |
| `--env-file` | `./.env` if present | dotenv file to load before connecting. A *missing explicit* `--env-file` warns (it is not silently ignored). |
| `--timeout` | wait indefinitely | Per-turn patience, in seconds. A turn that exceeds it prints a notice and the session continues. (Ctrl-C exits the whole session — see [Leaving](#leaving).) |

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Clean exit (`/exit`, `/quit`, Ctrl-D, or Ctrl-C), or when no agents are online. |
| `2` | A named agent that isn't online, or an unusable mesh directory (`MeshUnavailableError`: still catching up, the directory topic absent, or the broker unreachable). A bad `--env-file` warns and continues. |

> **Replies are rendered as text.** `ck chat` connects with a string output type,
> so an agent whose output is structured data shows that data as JSON on the answer
> line. It's a conversational tool for trying agents out and watching their work —
> for typed structured output, call the agent from a client (see
> [How to call nodes from a client](client-features.md)).

---

## `ck dev`

Run a calfkit project against a **local mesh** with zero broker setup. Before
delegating to the equivalent top-level command, every `ck dev` command
**ensures a broker** at the target address: if one is reachable it is reused;
otherwise a bundled [Tansu](https://tansu.io) broker (in-memory, Kafka-compatible)
is spawned as a detached background daemon that persists until an explicit
`ck dev broker stop`/`restart` or a reboot.

```text
ck dev run TARGET [TARGET ...] [OPTIONS]
ck dev chat [NAME] [OPTIONS]
ck dev broker (start | stop | status | restart) [OPTIONS]
```

The bundled broker requires the **`[mesh]` extra** (`pip install 'calfkit[mesh]'`),
which ships the binary (Linux x86_64/aarch64 glibc+musl, macOS arm64/x86_64 — no
Windows; the broker is Unix-only) and `psutil` (the ownership scan).
`CALF_TANSU_BIN` overrides the bundled binary with your own, with or without the
extra installed (resolution order: `CALF_TANSU_BIN` → bundled → `tansu` on
`PATH`). Without the extra, `ck dev run`/`ck dev chat` still work as pure
clients against an already-reachable broker; the `ck dev broker` management
commands need the extra's process scan. See
[How to run a local mesh with `ck dev`](local-dev-mesh.md).

### Spawn rules

- The address resolves with the usual precedence (`--host` > `$CALFKIT_MESH_URL`
  > `localhost`) and is normalized: `localhost` → `127.0.0.1`, missing port →
  `9092`. The delegated worker connects to the normalized address.
- A broker is spawned **only** for a single **loopback** address
  (`127.0.0.0/8` / `::1`) with nothing reachable there. `0.0.0.0`, hostnames,
  non-local IPs, and multi-address lists are connect-only: reachable → reused,
  unreachable → exit `2` (never a spawn).
- The spawned broker's data is **in-memory only** — a stop, restart, or reboot
  clears every topic and message.
- Spawn logs: `~/.calfkit/logs/tansu-<address>.log`, overwritten on each spawn.

### `ck dev run` and `ck dev chat`

Identical to [`ck run`](#ck-run) / [`ck chat`](#ck-chat) — same targets,
arguments, and exit codes — plus the broker-ensure above and a **local-dev
preset**:

| Preset | `ck dev run` | `ck dev chat` | Why |
| --- | --- | --- | --- |
| Provisioning | **on** (`--no-provision` to disable) | **on** (`--no-provision` to disable) | The in-memory broker starts empty and does not auto-create topics. |
| Reload | **on** (`--no-reload` to disable) | — | The inner-loop default. |
| Idempotence | off (`--enable-idempotence` to enable) | — | The bundled broker has no producer-id support. |

The broker is ensured once, in the parent — under `--reload` the restarted
workers only reconnect — and each command first prints whether the broker is
**managed** (`ck dev: managed broker at 127.0.0.1:9092 (pid 51234)`) or
**reused** (`… — not managed by calfkit`).

### `ck dev broker`

Direct control of the dev broker daemon, decoupled from any app run. Every
subcommand takes `--host`/`-H` (same precedence as above; default
`127.0.0.1:9092`) and loads `./.env` first, like `ck dev run`/`ck dev chat` —
so a `.env`-set `CALFKIT_MESH_URL` targets the same address across every
`ck dev` command.

| Command | Behavior |
| --- | --- |
| `start` | Connect-or-spawn and return (the daemon keeps running). Idempotent. |
| `stop` | Stop the dev broker at the target address. `--all` stops every running dev broker (ignores `--host`). A no-op with a message when none matches. |
| `status` | List the running dev broker(s) (address, pid, start time) and probe the target address — a reachable broker that isn't a dev broker reports as *reachable, not managed by calfkit*. |
| `restart` | `stop` then `start` — the clean slate (all in-memory data is lost). |

`ck dev` manages **dev brokers only**: a process is recognized by its command
line (an in-memory-engine `tansu` bound to the target address), whoever started
it. A durable Tansu, your own Kafka/Redpanda, or any other process on the port
is never touched; no state is persisted anywhere to track this.

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Clean exit (including a `stop`/`status` that found nothing to act on). |
| `2` | Configuration or broker-ensure error — invalid address, unreachable non-loopback address, missing binary/`[mesh]` extra, spawn or readiness failure — plus everything that exits `2` in the delegated command. |

---

## `ck topics`

```text
ck topics provision --nodes module:attr [--nodes module:attr ...] [OPTIONS]
```

Resolve the Kafka topics a set of nodes reference (subscribe inboxes, framework
return inboxes, publish topics, and agent tool inputs) and best-effort create
them — a development convenience for shaping a local/CI broker.

| Flag | Default | Description |
| --- | --- | --- |
| `--nodes` | — (required) | Node source as `module:attr` (a node or an iterable). Repeatable. |
| `--bootstrap-servers` | `localhost` | Kafka bootstrap server URL(s), comma-separated. |
| `--partitions` | `1` | Partition count for newly created topics. |
| `--replication-factor` | `1` | Replication factor for newly created topics (`1` is **not** durable). |
| `--timeout-ms` | `30000` | Budget for the provisioning operation. |
| `--dry-run` | off | Resolve and print the topic set without contacting Kafka. |

Exit codes: `0` success / dry-run · `2` error.

> **Experimental / opt-in.** This is a dev convenience (`rf=1`, no ACLs), **not**
> a production provisioning story. See
> **[docs/topic-provisioning.md](topic-provisioning.md)**.
