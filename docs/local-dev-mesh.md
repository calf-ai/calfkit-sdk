# How to run a local mesh with `ck dev`

This guide shows you how to run and iterate on your agents with **zero broker
setup**: `ck dev` starts (or reuses) a local, in-memory, Kafka-compatible broker
— [Tansu](https://tansu.io) — and runs your nodes against it. It assumes you
already have a runnable node (see the [CLI reference](cli.md#ck-run) for how
`module:attr` targets work).

## Install the dev broker

The broker binary is opt-in — it ships in the `[mesh]` extra so a plain
`pip install calfkit` stays pure-Python:

```bash
pip install 'calfkit[mesh]'
```

Wheels cover Linux (x86_64/aarch64, glibc and musl) and macOS (Apple Silicon
and Intel). There is no Windows wheel — the broker itself is Unix-only, so on
Windows use WSL2. On a Unix platform without a bundled wheel, point
`CALF_TANSU_BIN` at your own build (see
[Using your own broker binary](#using-your-own-broker-binary)).

## Run your agents

```console
$ ck dev run app:agent
ck dev: managed broker at 127.0.0.1:9092 (pid 51234)
```

One command does the whole local-dev posture: it ensures a broker is running at
the target address (spawning one only if nothing answers), **provisions topics
automatically** (the in-memory broker starts empty and does not auto-create
topics), and watches your source files for **reload**. Override any preset with
`--no-provision` or `--no-reload`.

The broker is a background daemon that **outlives the command** — Ctrl-C stops
your worker, not the broker, so restarts reconnect instantly and other commands
share the same mesh.

## Launch agents in the background with `--detach`

```console
$ ck dev run -d general:general
ck dev: managed broker at 127.0.0.1:9092 (pid 51234)
ck dev: launched agent 'general' (pid 51288) — runs until 'ck dev stop general' — logs: /Users/you/.calfkit/logs/agents-127.0.0.1_9092-general_general.log
```

`--detach/-d` (the `docker compose up -d` gesture) runs the same reload
supervisor as a **managed background daemon** and returns only once the agent
is **online on the mesh** — so `ck dev run -d … && ck dev chat` always lands
on a mesh where the agent already exists. The daemon keeps reloading on your
edits until an explicit `ck dev stop <name>` (or `ck dev down`, or a reboot).

Launching the same target again while it is online **reuses** it instead
(`ck dev: reusing agent 'general' (online, last seen 3s ago)`) — reuse matches
the **name on the mesh**, so check `ck dev status` if you're unsure what is
actually running. If a daemon for the name exists but its agents are offline
(usually a broken edit), a relaunch errors and points at the logs and the
`stop` command instead of stacking a second daemon.

## Chat with them from a second terminal

```console
$ ck dev chat
```

`ck dev chat` connects to the same broker (reusing it — nothing new is spawned)
and provisions its own reply inbox. This two-terminal loop — `ck dev run` in
one, `ck dev chat` in the other — is the working local setup; see
[How to chat with an agent from the terminal](chat-with-agents.md) for the chat
session itself.

`ck dev chat` can also **launch agents for the session**: give it
`module:attr` target(s) instead of a name and it runs them *inside the chat
process itself*, then opens the picker:

```console
$ ck dev chat general:general
```

A session worker lives and dies with the chat — close the session (`/exit`,
Ctrl-D, Ctrl-C, or even killing the terminal) and it is gone with it, by
construction. Because it runs in-process there is **no reload** in a chat
session: the edit loop is save → `/exit` → rerun (the broker and any daemons
persist, so this is seconds). Use `ck dev run -d … && ck dev chat` when you
want live reload while chatting. On exit the session narrates what it owned:

```text
✦ stopped 'general' (ran in this session)
✦ still running: finance — 'ck dev chat' to rejoin, 'ck dev down' to stop everything
```

## The team loop: watch two agents find each other

The core lesson of the mesh — *agents are independent processes that discover
each other at runtime* — is something you can watch happen:

1. Terminal 1: `ck dev run -d general:general && ck dev chat` — launch
   the generalist, start chatting.
2. Terminal 2: `ck dev run -d finance:finance` — add a specialist to the
   **live** team. The first agent's process is untouched.
3. Back in the chat, ask a finance question — `general` discovers `finance`
   over the mesh and hands off to it, mid-conversation.

Separate commands, separate processes, discovery over the mesh — no wiring,
no orchestrator, no restarts.

## See and stop what you launched

```console
$ ck dev status
KIND    NAME            STATE                      PID    SINCE                      TARGET           LOGS
broker  127.0.0.1:9092  running                    51234  2026-07-02T14:00:12+00:00  —                —
agent   general         online (last seen 3s ago)  51288  2026-07-02T14:02:40+00:00  general:general  /Users/you/.calfkit/logs/agents-….log
```

`ck dev status` shows everything, unfiltered: the broker, every managed
daemon, and **every other node online on the mesh** (a chat-session worker, a
foreground run, a teammate's worker) annotated
`not a ck dev daemon (stop it where it runs)`. A daemon whose names have no
live mesh record shows `unknown (see logs)` — usually a broken edit mid-reload;
its log path is right there in the row. When the broker itself is down, the
presence columns degrade to `unknown (mesh unreachable)` and the daemon rows
still render. Heartbeat ages are always shown: "online" can lag a crash — by
up to ~15s for `ck dev`-launched workers (they heartbeat every 5s), and up to
~90s for a plain `ck run` worker on the 30s production default — and the age
is the honesty device.

- `ck dev stop NAME…` — stop the daemon(s) owning those names. A stop is
  **whole-daemon**: co-hosted agents launched by one command go down together,
  and every name is narrated (`stopped daemon pid 51288 (agents: general)`).
  Stopping resolves names **at the target address** (`--host` for other
  meshes); `--all` sweeps every daemon on every address.
- `ck dev down` — stop **all** agent daemons, then the broker at the target
  address. Foreground runs and chat-session workers survive `down` by design
  (they are not daemons) — they will error against the stopped broker; stop
  them where they run.

Daemon logs live at `~/.calfkit/logs/agents-<address>-<targets>.log`,
overwritten on each launch — consult them *before* relaunching over a broken
daemon.

## Control the mesh directly

```console
$ ck dev mesh status
127.0.0.1:9092: pid 51234, running, started 2026-07-01T22:38:13+00:00
```

- `ck dev mesh start` — run the broker in the **foreground** (its output streams
  to your terminal; Ctrl-C stops it). Add `-d`/`--detach` to run it as a
  detached daemon and return instead — idempotent connect-or-spawn.
- `ck dev mesh stop` — stop the dev broker at the target address
  (`--all` stops every running one).
- `ck dev mesh restart` — stop then re-spawn a detached daemon. **The data is
  in-memory**, so this is the clean slate: all topics and messages are gone.
  (A reboot resets it the same way.)

Target a non-default address with `--host/-H` on any of these; each address is
its own independent single-node mesh.

What `stop` will and won't touch: it stops the **memory-engine Tansu** bound to
the target address — whoever started it — and nothing else. A durable Tansu,
your own Kafka/Redpanda, or any other process on the port is never signalled.

## Point at a broker you already run

If a broker is already reachable at the target address, `ck dev` **reuses** it
and spawns nothing:

```console
$ ck dev run app:agent --host broker.staging.internal:9092
ck dev: reusing broker at broker.staging.internal:9092 — not managed by calfkit
```

- A reachable address (local or remote) is used as-is — `ck dev` is then a pure
  client, and the `[mesh]` extra isn't needed.
- An **unreachable remote** address is an error (exit `2`), never a spawn:
  spawning is reserved for a single loopback address (`127.x.x.x` / `::1`);
  multi-address lists never spawn.
- A SASL/TLS-secured broker fails the plaintext reachability probe, so it reads
  as absent — `ck dev` does not transparently reuse secured brokers.

`--host` follows the same precedence as the rest of the CLI: flag >
`$CALFKIT_MESH_URL` > `localhost`.

## Using your own broker binary

Set `CALF_TANSU_BIN` to an executable to bypass the bundled binary entirely —
for a Unix platform without a bundled wheel, a local `cargo` build, or a pinned
version. It works with or without the `[mesh]` extra installed:

```bash
CALF_TANSU_BIN=~/src/tansu/target/release/tansu ck dev run app:agent
```

The binary resolves in order: `CALF_TANSU_BIN` → the `[mesh]`-bundled binary →
a `tansu` on `PATH`.

## Troubleshooting

- **"the bundled dev broker is not installed"** — install the extra
  (`pip install 'calfkit[mesh]'`) or set `CALF_TANSU_BIN`.
- **The broker won't start** — the error includes the tail of its log; the full
  log is at `~/.calfkit/logs/tansu-<address>.log` (overwritten on each spawn).
  A non-Kafka process already holding the port surfaces here as a bind failure.
- **State disappeared** — expected: the dev broker is memory-only. Anything you
  need to survive a restart belongs on a real broker.

See also: the [`ck dev` CLI reference](cli.md#ck-dev) for every flag and exit
code, and [Topic provisioning](topic-provisioning.md) for what the provisioning
preset creates.
