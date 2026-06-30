# How to chat with an agent from the terminal

`ck chat` opens an interactive REPL for talking to an agent running on your mesh —
for trying an agent out, watching how it uses its tools, or a quick manual check
without writing any client code.

## Before you start

You need a running mesh and at least one agent online (advertising itself on the
control plane). Running locally, start your agent worker first — for example with
[`ck run`](cli.md#ck-run):

```console
$ ck run support_desk:support_bot
```

`ck chat` connects to `$CALFKIT_MESH_URL` (or `localhost`) by default; point it at
a different broker with `--host`.

## Pick an agent and start chatting

Run `ck chat` with no arguments to see who's online and choose one:

```console
$ ck chat
Discovering agents...
Online agents

  1  researcher   Deep web research with citations
  2  support-bot  Handles customer support tickets and refunds

Select an agent [1-2, q to quit]: 2

Chatting with support-bot. Type /exit or press Ctrl-D to leave.
```

Type a message at the `you >` prompt and press Enter. The reply streams in two
parts — first the agent's **work log** (one indented, tagged line per step), then
its **answer**:

```text
you > my order #4471 never arrived

support-bot
  [message]      Let me pull up that order for you.
  [tool call]    lookup_order(id='4471')
  [tool result]  shipped 2026-06-24, in transit

support-bot > Order #4471 shipped on the 24th and is in transit.
```

The conversation is multi-turn — keep typing and the agent remembers the earlier
turns. If it hands off to another agent mid-turn, that agent's name appears as its
own header, so you can follow who did what.

## Skip the picker

If you already know the agent's name, pass it as an argument to go straight in:

```console
$ ck chat support-bot
```

This connects directly to `support-bot` (and exits with an error if it isn't
online).

## Leave the session

End with `/exit`, `/quit`, or Ctrl-D. Ctrl-C also exits at any time, including
mid-turn. To talk to a different agent, leave and run `ck chat` again — there is
no in-session "back".

## Cap how long a turn may take

By default `ck chat` waits as long as the agent needs. To bound each turn, pass
`--timeout` in seconds:

```console
$ ck chat researcher --timeout 120
```

A turn that exceeds the limit prints `(no response within the timeout)` and the
session continues; your next message starts a fresh turn. (Pressing Ctrl-C instead
exits the whole session — set `--timeout` if you want a stuck turn to abort while
the session keeps going.)

## Related

- [CLI reference: `ck chat`](cli.md#ck-chat) — every option and exit code.
- [How to call nodes from a client](client-features.md) — the programmatic way to
  invoke agents, including typed structured output and streaming a run's steps.
