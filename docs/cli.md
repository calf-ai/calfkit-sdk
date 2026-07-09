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
`ck dev mesh stop`/`restart` or a reboot.

```text
ck dev run  TARGET [TARGET ...] [--detach/-d] [OPTIONS]
ck dev chat [NAME | TARGET ...] [OPTIONS]
ck dev status [OPTIONS]
ck dev stop (NAME [NAME ...] | --all) [OPTIONS]
ck dev down [OPTIONS]
ck dev mesh (start [--detach/-d] | stop | status | restart) [OPTIONS]
```

The bundled broker requires the **`[mesh]` extra** (`pip install 'calfkit[mesh]'`),
which ships the binary (Linux x86_64/aarch64 glibc+musl, macOS arm64/x86_64 — no
Windows; the broker is Unix-only) and `psutil` (the ownership scan).
`CALF_TANSU_BIN` overrides the bundled binary with your own, with or without the
extra installed (resolution order: `CALF_TANSU_BIN` → bundled → `tansu` on
`PATH`). Without the extra, foreground `ck dev run` and the `ck dev chat`
attach forms still work as pure clients against an already-reachable broker —
but everything that manages agent daemons needs the extra's process scan.
`ck dev status`, `ck dev stop`, `ck dev down`, `ck dev mesh stop`, and
`ck dev mesh status` exit `2` with the install hint without it (one edge:
`ck dev mesh stop` against a *multi-address* target is a messaged no-op —
multi-address is never a spawn target, so there is nothing to scan);
`ck dev run -d` and `ck dev chat TARGET…` need it to launch (they still
succeed on a core install when every target is already online — pure reuse
never scans); `ck dev mesh start -d`/`restart` degrade gracefully (reuse/borrow
still works; only the managed-vs-reused classification is lost). Bare foreground
`ck dev mesh start` never reuses — it errors if a broker is already reachable —
so it only needs the binary, not the scan. See
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

### `ck dev run --detach/-d`

Everything `ck dev run` is, with the attachment cut (the `docker compose up
-d` gesture): the worker tree — the reload supervisor plus the worker it
restarts on edits — is spawned as a **detached daemon** and the command
returns only when its agents/tools are **online on the mesh** (bounded at
15 s; a failure reports the daemon's log tail). Lifetime: until
`ck dev stop <name>`, `ck dev down`, or a reboot.

- Per launched name it prints the **supervisor pid** (the process `stop`
  signals), the lifetime statement, and the log path
  (`~/.calfkit/logs/agents-<address>-<targets>.log`, overwritten per launch).
- If every name of a target is already online, the target is **reused**
  (`ck dev: reusing agent '<name>' (online, last seen 3s ago)`) and nothing is
  spawned. Reuse matches names **on the mesh**, never code identity.
- If a daemon for a name exists but its agents are offline (broken code or
  mid-restart), a relaunch is an **error** naming the pid, the logs, and the
  `stop` command — never a second daemon over a broken first.
- If a target is **partially online** (some of its names online, others not),
  launching it is an error naming the collision — a worker hosts all of a
  target's nodes together, so it can neither be reused nor launched. Applies
  to `chat TARGET…` too.
- All targets of one invocation co-host in **one** worker (the `ck run`
  rule); they still discover and communicate over the mesh, never in-process.
- A target must resolve to at least one agent or tool (a plain consumer node
  has no mesh presence to manage — run those in the foreground).
- Ctrl-C during the readiness wait leaves the daemon running (recoverable —
  `ck dev status`); exit `130`.

Ownership is **stateless**: a daemon is recognized by an internal
`--dev-daemon=<names>` marker in its command line — matched only in that
exact emitted form, and only on a `run` command line, so the flag merely
*appearing as data* in some other process's argv (say, a grep of it) does not
match (both anchors are required) — with its `--host` scoping it to an
address, exactly like the broker's process scan; no registry file anywhere. The flag is internal
`ck dev` plumbing; it is accepted (and ignored) by `ck run` purely so it
lands in the daemon's argv.

### `ck dev chat` targets — session workers

An argument containing `:` is a `module:attr` **target**; a bare word is an
agent **name** (mixing them, passing two names, or duplicate node names across
targets: exit `2`). With targets, the chat runs them **inside its own
process** — a *session worker* on the chat's own client — waits for them to be
online, then opens the normal picker:

- The session worker dies **with the chat process**, on any exit — there is no
  child process to orphan. Its logs share the chat terminal.
- **No reload in a chat session** (an in-process worker cannot re-import your
  code): the edit loop is save → `/exit` → rerun, or use
  `ck dev run -d … && ck dev chat` for live reload while chatting.
- Targets already online are **reused**, not launched; if every target was
  reused nothing is started at all. The exit narration says what was owned:
  `✦ stopped '<names>' (ran in this session)` /
  `✦ still running: <names> — 'ck dev chat' to rejoin, 'ck dev down' to stop
  everything`.
- The launched+reused set must contain at least one **agent** (tools alone are
  nobody to chat with): exit `2`.

### `ck dev status`

One table, unfiltered, host-scoped (`--host` > `$CALFKIT_MESH_URL` >
`localhost`): the broker, every managed daemon's names joined with a live mesh
snapshot, and **every other online node** annotated `not a ck dev daemon (stop
it where it runs)`. Heartbeat ages are always shown ("online" can lag a crash
by up to the ~15 s staleness window — the age is the honesty device). A
daemon-owned name with no mesh record reads `unknown (see logs)` (crashed
edit, mid-restart, or still booting — indistinguishable from outside). A down
broker never errors: its line reads `no broker reachable`, presence columns
degrade to `unknown (mesh unreachable)`, daemon rows still render. Exit `0`
whenever it can answer — a down broker or unreadable mesh never errors; only a
missing `[mesh]` extra or an invalid address exits `2`, as everywhere in the
family.

Heartbeat cadence behind the ages: every worker `ck dev` launches (foreground
runs, `-d` daemons, chat-session workers) heartbeats every **5 s**, so its
crash-staleness window is ~15 s. A worker launched by plain `ck run` keeps the
30 s production default — its "online" can lag a crash by up to ~90 s, ages
shown all the same.

### `ck dev stop` and `ck dev down`

| Command | Behavior |
| --- | --- |
| `stop NAME…` | Stop the daemon(s) owning those names — **whole-daemon**: co-hosted names go down together and every one is narrated (`stopped daemon pid 51288 (agents: general, finance)`). SIGTERM → 8 s grace → SIGKILL, delivered to the daemon's whole **process group**. |
| `stop --all` | Every `ck dev` agent daemon on **every** address (ignores `--host`). |
| `down` | `stop --all`, then `mesh stop` at the resolved address. |

Names resolve **within the target address** while `--all`/`down` sweep
globally — that is the one name-vs-address asymmetry in the family. An unknown
name exits `2` listing what *is* running at the address (and hints at
`--host`); a name that is online but not a daemon (a session worker, a
foreground run, anything external) exits `2` with *stop it where it runs*; two
same-named daemons at one address exit `2` listing both pids — never a guess.
Foreground runs and chat-session workers **survive `down`** by design (they
carry no marker) and will error against the stopped broker — stop them where
they run. Finding nothing to stop is a messaged no-op, exit `0`.

### `ck dev mesh`

Direct control of the local dev mesh — the broker your agents run on —
decoupled from any app run. Every
subcommand takes `--host`/`-H` (same precedence as above; default
`127.0.0.1:9092`) and loads `./.env` first, like `ck dev run`/`ck dev chat` —
so a `.env`-set `CALFKIT_MESH_URL` targets the same address across every
`ck dev` command.

| Command | Behavior |
| --- | --- |
| `start` | Run the broker in the **foreground** (its output streams to your terminal; Ctrl-C stops it). Errors if a broker is already running — foreground can't attach, so stop it or use `-d`. |
| `start -d`/`--detach` | Connect-or-spawn a **detached** daemon and return (it keeps running). Idempotent — the shape `ck dev run`/`ck dev chat` share when they ensure a broker. |
| `stop` | Stop the dev broker at the target address. `--all` stops every running dev broker (ignores `--host`). A no-op with a message when none matches. |
| `status` | List the running dev broker(s) (address, pid, start time) and probe the target address — a reachable broker that isn't a dev broker reports as *reachable, not managed by calfkit*. |
| `restart` | `stop` then re-spawn a detached daemon — the clean slate (all in-memory data is lost). |

`ck dev` manages **dev brokers only**: a process is recognized by its command
line (an in-memory-engine `tansu` bound to the target address), whoever started
it. A durable Tansu, your own Kafka/Redpanda, or any other process on the port
is never touched; no state is persisted anywhere to track this.

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Clean exit (including a `stop`/`status`/`down` that found nothing to act on). |
| `2` | Configuration or supervise error — invalid address, unreachable non-loopback address, missing binary/`[mesh]` extra, broker or agent spawn/readiness failure, a bad `chat` grammar, an unknown/unmanaged `stop` name — plus everything that exits `2` in the delegated command. |
| `130` | Ctrl-C during a `run -d` readiness wait (the daemon is left running — `ck dev status`). |

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
