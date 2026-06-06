# Worker Lifecycle & Embedding

A `Worker` hosts your nodes (agents, tools, consumers) against a Kafka broker: it
registers each node's subscribers/publishers, joins their consumer groups, opens
any lifecycle resources, and later drains and closes everything cleanly.

There are three ways to run a worker. They all bring up and tear down the *same*
things — they differ only in whether the worker installs OS signal handlers and
whether the call blocks the caller.

> For the lifecycle **hooks** themselves (`@resource`, `on_startup`,
> `after_startup`, `on_shutdown`, `after_shutdown`, presence events, and the
> `resources` vs `deps` distinction), see
> [Lifecycle Hooks & Resources](../README.md#lifecycle-hooks--resources-optional)
> in the README. This guide is about *running* a worker and *embedding* it
> alongside other services.

## Choosing a surface

| Surface | Installs signal handlers | Blocks the caller | Use it for |
|---|---|---|---|
| `await worker.run()` | yes | yes (until signalled) | running the worker as a standalone service — the batteries-included default |
| `await worker.start()` / `await worker.stop()` | no | no | embedding the worker beside another long-running service; explicit control over startup ordering |
| `async with worker:` | no | no (scoped to the block) | tests and short-lived embedding |

What **start** does, in order, on every surface:

1. register each node's FastStream subscribers + publishers (and expand MCP
   servers into per-tool bridge subscribers);
2. open `@resource` / `on_startup` brackets and MCP sessions;
3. start the broker — consumers subscribe and join their consumer groups;
4. run `after_startup` hooks (the broker/producer is live here).

**stop** reverses it: `on_shutdown` hooks → drain the broker → `after_shutdown`
and `@resource` teardown.

---

## `run()` — standalone service (default)

The batteries-included way to deploy a worker as its own process. It installs
SIGINT/SIGTERM handlers and blocks until the process is signalled, then shuts
down gracefully.

```python
import asyncio
from calfkit import Worker
from calfkit.client import Client

async def main():
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[my_agent, my_tool])
    await worker.run()  # blocks until SIGINT/SIGTERM, then drains + closes

if __name__ == "__main__":
    asyncio.run(main())
```

Use `run()` when the worker *is* the application. If you need to run anything
else in the same process, use `start()`/`stop()` instead.

---

## `start()` / `stop()` — embeddable, non-blocking

`start()` returns as soon as the worker is fully up. It does **not** install
signal handlers and does **not** block, so the worker's consumers run as one
managed component **alongside another long-running service** in the same event
loop — your application owns the foreground and signals.

`stop()` drains in-flight consumers and closes resources/MCP sessions. It is a
no-op if the worker was never started, so a defensive `try/finally` is always
safe.

### Basic

```python
import asyncio
from calfkit import Worker
from calfkit.client import Client

async def main():
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[my_agent, my_tool])

    await worker.start()      # non-blocking: returns once consumers + resources are up
    try:
        await run_my_app()    # whatever else your process does
    finally:
        await worker.stop()   # drain consumers + close resources

if __name__ == "__main__":
    asyncio.run(main())
```

### Bring the worker up *before* you accept external events

`start()` completes only after the consumers have subscribed and joined their
groups, so you can guarantee they are live before your app starts accepting
traffic — replies that arrive immediately after you go live aren't lost in a
register→serve gap.

```python
async def main():
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[reply_consumer])

    # 1) Bring the worker fully up: handlers registered, consumer groups joined,
    #    resources/MCP opened.
    await worker.start()
    try:
        # 2) Only now begin accepting external events — the consumers are already
        #    subscribed, so the first replies they trigger won't be dropped.
        await discord_gateway.start()   # the other long-running service (blocks)
    finally:
        # 3) Drain + close on shutdown.
        await worker.stop()
```

### Run beside other services under your own supervisor

Because `start()` is non-blocking, you can bring the worker up and then run
several services concurrently under your own task group. Your run loop owns the
foreground and signal handling; the worker is just one component it supervises.

```python
async def main():
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[my_agent])

    await worker.start()
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(discord_gateway.serve())
            tg.create_task(metrics_server.serve())
            # ... other long-running services
    finally:
        await worker.stop()
```

---

## `async with worker:` — scoped lifecycle

Entering the context calls `start()`; exiting calls `stop()` — even if the block
raises. Ideal for tests and short-lived embedding.

```python
async with worker:
    # the worker's consumers + resources are live in this block
    await shutdown_signal.wait()   # or run a short task
# stopped + drained automatically on exit
```

This is how the test suite drives a worker end-to-end:

```python
from faststream.kafka import TestKafkaBroker

async with TestKafkaBroker(worker._client.broker):
    await worker.start()
    # publish an envelope, assert the handler saw ctx.resources[...], etc.
    await worker.stop()
```

---

## Resources and lifecycle hooks compose with every surface

Anything you open with `@resource`, `@worker.resource`, or `on_startup` comes up
during **start** and is torn down during **stop** — on all three surfaces, with
no extra wiring. A worker-scoped resource is shared with every node's handler as
`ctx.resources["key"]`.

```python
worker = Worker(client, nodes=[my_tool])

@worker.resource("pool")
async def pool(ctx):
    p = await create_pool()
    try:
        yield p                 # reachable as ctx.resources["pool"] in every node handler
    finally:
        await p.close()         # closed on worker.stop()

await worker.start()            # pool opened here, before consumers serve
try:
    await run_my_app()
finally:
    await worker.stop()         # pool closed here
```

See [Lifecycle Hooks & Resources](../README.md#lifecycle-hooks--resources-optional)
for the full hook API (the `@resource` context-manager pattern, the
`on_startup`/`after_shutdown` callback pattern, one-pattern-per-owner, presence
events, and `resources` vs `deps`).

---

## Behavior & guarantees

- **Single-use.** A worker can be started once. A second `start()` / `run()` (or
  re-entering `async with`) raises
  `RuntimeError("Worker is single-use; create a new Worker to restart")`. Build a
  new `Worker` to restart.
- **`stop()` is always safe.** It is a no-op if the worker never started, so
  `try/finally: await worker.stop()` never raises on the "not started" path.
- **A failed boot never leaks.** If `start()` fails partway (for example the
  broker can't reach Kafka), it tears down anything it already opened — resource
  brackets and MCP sessions — before re-raising. This holds for `start()`,
  `run()`, and `async with` (which recovers even though Python skips `__aexit__`
  when `__aenter__` raises). A failure *during* that cleanup is logged, never
  raised, so it can't mask the original boot error.
- **Resilient shutdown.** `stop()` releases the resource brackets in a `finally`,
  so a flaky/cancelled broker disconnect can't strand pools, clients, or MCP
  sessions. Resource teardown still runs *after* the drain attempt, preserving
  "drain before close" order. `CancelledError` still propagates after the
  best-effort teardown.
- **Signals are opt-in.** Only `run()` installs SIGINT/SIGTERM handlers. The
  embeddable surfaces (`start()`/`stop()`, `async with`) install none, so the
  host application keeps full control of signal handling.
