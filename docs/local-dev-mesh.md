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

## Chat with them from a second terminal

```console
$ ck dev chat
```

`ck dev chat` connects to the same broker (reusing it — nothing new is spawned)
and provisions its own reply inbox. This two-terminal loop — `ck dev run` in
one, `ck dev chat` in the other — is the working local setup; see
[How to chat with an agent from the terminal](chat-with-agents.md) for the chat
session itself.

## Control the broker directly

```console
$ ck dev broker status
127.0.0.1:9092: pid 51234, running, started 2026-07-01T22:38:13+00:00
```

- `ck dev broker start` — connect-or-spawn and return. Idempotent.
- `ck dev broker stop` — stop the dev broker at the target address
  (`--all` stops every running one).
- `ck dev broker restart` — stop then start. **The data is in-memory**, so this
  is the clean slate: all topics and messages are gone. (A reboot resets it the
  same way.)

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
