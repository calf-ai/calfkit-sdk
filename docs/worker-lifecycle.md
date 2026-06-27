# Worker Lifecycle & Embedding

A `Worker` hosts your nodes (agents, tools, consumers) against a Kafka broker: it
registers each node's subscribers/publishers, joins their consumer groups, opens
any lifecycle resources, and later drains and closes everything cleanly.

This guide covers both halves of that:

- **[Resources & lifecycle hooks](#resources--lifecycle-hooks)** — open long-lived
  resources at startup and close them on shutdown, and publish presence/departure
  events.
- **[Running & embedding the worker](#running--embedding-the-worker)** — `run()`
  as a standalone service, or the non-blocking `start()`/`stop()` and
  `async with` surfaces to embed the worker beside other services.

---

## Resources & lifecycle hooks

Nodes and workers can open long-lived **resources** — database pools, HTTP
clients, caches — when the service boots and close them cleanly on shutdown. A
resource is a live, server-side object that handlers read via
`ctx.resources["key"]`.

The decorator surface — `resource`, `on_startup`, `after_startup`, `on_shutdown`,
`after_shutdown` — is available on every node *and* on the `Worker`. Each hook is
a `fn(ctx)` callable (sync or async; its return value is ignored) and is
repeatable — register as many per phase as you like.

> These lifecycle hooks run **once per process**, at startup and shutdown. Don't
> confuse them with a node's **policy seams** (`before_node` / `after_node` /
> `on_node_error` / `on_callee_error`) — structurally identical `@node.<name>`
> decorators that instead run **once per message**, to guard and shape each
> invocation. See [How to guard and transform node invocations](policy-seams.md).

### Resources vs deps

Keep these distinct:

- **`resources`** are live, server-side objects (pools, clients) opened by
  lifecycle hooks, scoped to a worker or node, and **never serialized**. They
  never travel on the wire. Read them as `ctx.resources["key"]` (consumers:
  `result.resources["key"]`). The read side is typed as a read-only `Mapping`, so
  `ctx.resources[...] = ...` is a type error.
- **`deps`** are per-request, JSON-serializable values the caller passes at invoke
  time (`client.agent(name).execute(..., deps={...})`). They ride along on the Kafka
  envelope and are read by tools as `ctx.deps["key"]`.

Use `resources` for connections you open once and reuse; use `deps` for
request-specific data supplied by the caller.

### The `@resource` pattern (recommended)

Decorate a single-yield async generator. Whatever you `yield` is the resource
value; code after the `yield` runs at teardown. A tool reads it from
`ctx.resources`:

```python
# weather_tool.py
import asyncio
import httpx
from calfkit.nodes import agent_tool
from calfkit.client import Client
from calfkit.worker import Worker
from calfkit import ToolContext  # type for ctx, optional

@agent_tool
async def get_weather(ctx: ToolContext, location: str) -> str:
    """Get the current weather at a location"""
    http: httpx.AsyncClient = ctx.resources["http"]  # the live, shared client
    resp = await http.get(f"https://api.example.com/weather?q={location}")
    return resp.text

# Open one httpx client at startup, close it at shutdown.
@get_weather.resource("http")
async def http_client(ctx):              # ctx: ResourceSetupContext
    async with httpx.AsyncClient() as client:
        yield client                     # value lands in ctx.resources["http"]
    # exiting the `async with` closes the client on teardown

async def main():
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[get_weather])
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
```

### The callback pattern (alternative)

`@node.on_startup` writes into the bag and `@node.after_shutdown` cleans up. Use
this when a context manager doesn't fit:

```python
@get_weather.on_startup
async def open_pool(ctx):                 # ctx: LifecycleContext (resources writable)
    ctx.resources["pool"] = await create_pool()

@get_weather.after_shutdown
async def close_pool(ctx):
    await ctx.resources["pool"].close()
```

### One pattern per owner

Pick `@resource` *or* the `on_startup`/`after_shutdown` callbacks on a given
owner, not both. If you mix them, the `@resource` brackets win and the callbacks
are ignored (with a warning) — this mirrors FastAPI's lifespan-vs-`on_event`
rule. Registering two `@resource` brackets under the same name on one owner raises
`LifecycleConfigError` immediately.

### Worker-scoped resources

Declare a resource on the `Worker` to open **one shared object visible to every
node's handler** as `ctx.resources["key"]`. This is the canonical "one shared
pool for all nodes" pattern:

```python
async def main():
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[get_weather])

    @worker.resource("pool")
    async def shared_pool(ctx):
        pool = await create_pool()
        try:
            yield pool                    # reachable as ctx.resources["pool"] in every node
        finally:
            await pool.close()

    await worker.run()
```

Worker resources are merged under each node's own resources; if a node declares a
resource with the same key, **the node's value wins** for that node's handlers.

### Presence & departure events

`@worker.after_startup` and `@worker.on_shutdown` receive a `ServingContext`
while the broker is live (`ctx.broker`) and expose the worker as `ctx.owner`, so
you can announce a worker's arrival and departure to the fleet:

```python
from calfkit.worker import Worker, ServingContext

@worker.after_startup
async def announce_up(ctx: ServingContext[Worker]) -> None:
    await ctx.broker.publish({"id": ctx.owner.id, "status": "up"}, topic="fleet.presence")

@worker.on_shutdown
async def announce_down(ctx: ServingContext[Worker]) -> None:
    await ctx.broker.publish({"id": ctx.owner.id, "status": "down"}, topic="fleet.presence")
```

Typing the hook as `ServingContext[Worker]` makes `ctx.owner.id` autocomplete.
The context types are importable from the top level too:
`from calfkit import ServingContext, LifecycleContext, ResourceSetupContext, LifecycleConfigError`.

**At-least-once caveat**: presence publishes are *not* exactly-once. Under
consumer-group rebalances an `after_startup` hook may fire more than once, and a
hard crash skips the `on_shutdown` "down" entirely. Pair presence events with a
consumer-side TTL / liveness check rather than treating them as a perfectly
balanced up/down ledger.

---

## Running & embedding the worker

There are three ways to run a worker. They all bring up and tear down the *same*
things — they differ only in whether the worker installs OS signal handlers and
whether the call blocks the caller.

### Choosing a surface

| Surface | Installs signal handlers | Blocks the caller | Use it for |
|---|---|---|---|
| `await worker.run()` | yes | yes (until signalled) | running the worker as a standalone service — the batteries-included default |
| `await worker.start()` / `await worker.stop()` | no | no | embedding the worker beside another long-running service; explicit control over startup ordering |
| `async with worker:` | no | no (scoped to the block) | tests and short-lived embedding |

What **start** does, in order, on every surface:

1. register each node's FastStream subscribers + publishers;
2. open `@resource` / `on_startup` brackets;
3. start the broker — consumers subscribe and join their consumer groups;
4. run `after_startup` hooks (the broker/producer is live here).

**stop** reverses it: `on_shutdown` hooks → drain the broker → `after_shutdown`
and `@resource` teardown.

### `run()` — standalone service (default)

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

### `start()` / `stop()` — embeddable, non-blocking

`start()` returns as soon as the worker is fully up. It does **not** install
signal handlers and does **not** block, so the worker's consumers run as one
managed component **alongside another long-running service** in the same event
loop — your application owns the foreground and signals.

`stop()` drains in-flight consumers and closes resources. It is a
no-op if the worker was never started, so a defensive `try/finally` is always
safe.

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

**Bring the worker up *before* you accept external events.** `start()` completes
only after the consumers have subscribed and joined their groups, so you can
guarantee they are live before your app starts accepting traffic — replies that
arrive immediately after you go live aren't lost in a register→serve gap.

```python
async def main():
    client = Client.connect("localhost:9092")
    worker = Worker(client, nodes=[reply_consumer])

    # 1) Bring the worker fully up: handlers registered, consumer groups joined,
    #    resources opened.
    await worker.start()
    try:
        # 2) Only now begin accepting external events — the consumers are already
        #    subscribed, so the first replies they trigger won't be dropped.
        await discord_gateway.start()   # the other long-running service (blocks)
    finally:
        # 3) Drain + close on shutdown.
        await worker.stop()
```

**Run beside other services under your own supervisor.** Because `start()` is
non-blocking, you can bring the worker up and then run several services
concurrently under your own task group. Your run loop owns the foreground and
signal handling; the worker is just one component it supervises.

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

### `async with worker:` — scoped lifecycle

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

## Behavior & guarantees

- **Single-use.** A worker can be started once. A second `start()` / `run()` (or
  re-entering `async with`) raises
  `RuntimeError("Worker is single-use; create a new Worker to restart")`. Build a
  new `Worker` to restart.
- **`stop()` is always safe.** It is a no-op if the worker never started, so
  `try/finally: await worker.stop()` never raises on the "not started" path.
- **A failed boot never leaks.** If `start()` fails partway (for example the
  broker can't reach Kafka), it tears down anything it already opened — the
  resource brackets — before re-raising. This holds for `start()`,
  `run()`, and `async with` (which recovers even though Python skips `__aexit__`
  when `__aenter__` raises). A failure *during* that cleanup is logged, never
  raised, so it can't mask the original boot error.
- **Resilient shutdown.** `stop()` releases the resource brackets in a `finally`,
  so a flaky/cancelled broker disconnect can't strand pools or clients. Resource
  teardown still runs *after* the drain attempt, preserving
  "drain before close" order. `CancelledError` still propagates after the
  best-effort teardown.
- **Signals are opt-in.** Only `run()` installs SIGINT/SIGTERM handlers. The
  embeddable surfaces (`start()`/`stop()`, `async with`) install none, so the
  host application keeps full control of signal handling.
