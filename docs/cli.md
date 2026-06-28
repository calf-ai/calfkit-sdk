# Calfkit CLI reference

The `ck` command bundles the SDK's command-line tooling.

Commands:

| Command | Purpose |
| --- | --- |
| [`ck run`](#ck-run) | Run node(s) as a worker for local development (no `Worker` boilerplate). |
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
