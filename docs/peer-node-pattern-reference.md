# Reference: Node / NodeRef API

This page is the technical reference for a canonical implementation of the
[peer node pattern](peer-node-pattern.md): a node identified by an **identity**
that can both host work and be invoked through a send-only reference.

The reference is **self-contained and project-independent**. It documents the
implementation reproduced in full below; that code is the source of truth for
every statement on this page. Copy it into a single module to run any example
verbatim.

For *why* the API is shaped this way — why the reference is separate from the
servant, why calls are asynchronous — see the
[explanation](peer-node-pattern.md). This page only describes *what* the API is.

---

## The canonical implementation

```python
from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol


@dataclass(frozen=True)
class Message:
    """One unit transferred over a transport."""

    payload: Any
    reply_to: "NodeRef | None" = None


class Transport(Protocol):
    """Routes messages between identities."""

    async def send(self, identity: str, message: Message) -> None: ...

    def inbox(self, identity: str) -> "asyncio.Queue[Message]": ...


class InMemoryTransport:
    """Single-process Transport backed by per-identity asyncio queues."""

    def __init__(self) -> None:
        self._inboxes: dict[str, asyncio.Queue[Message]] = {}

    def inbox(self, identity: str) -> asyncio.Queue[Message]:
        return self._inboxes.setdefault(identity, asyncio.Queue())

    async def send(self, identity: str, message: Message) -> None:
        self.inbox(identity).put_nowait(message)


@dataclass(frozen=True)
class NodeRef:
    """A send-only handle to a node, addressed by identity."""

    identity: str
    transport: Transport

    async def tell(self, payload: Any) -> None:
        await self.transport.send(self.identity, Message(payload=payload))

    async def ask(self, payload: Any, *, timeout: float | None = None) -> Any:
        reply_id = f"reply:{uuid.uuid4()}"
        reply_ref = NodeRef(reply_id, self.transport)
        await self.transport.send(
            self.identity, Message(payload=payload, reply_to=reply_ref)
        )
        inbox = self.transport.inbox(reply_id)
        reply = await asyncio.wait_for(inbox.get(), timeout)
        return reply.payload


Handler = Callable[[Any], Awaitable[Any]]


class Node:
    """A servant that hosts a receive loop for one identity."""

    def __init__(
        self, identity: str, transport: Transport, handler: Handler
    ) -> None:
        self._identity = identity
        self._transport = transport
        self._handler = handler
        self._running = False

    @property
    def identity(self) -> str:
        return self._identity

    @property
    def ref(self) -> NodeRef:
        return NodeRef(self._identity, self._transport)

    @staticmethod
    def ref_to(identity: str, transport: Transport) -> NodeRef:
        return NodeRef(identity, transport)

    async def run(self) -> None:
        if self._running:
            raise RuntimeError(f"Node {self._identity!r} is already running")
        self._running = True
        inbox = self._transport.inbox(self._identity)
        try:
            while True:
                message = await inbox.get()
                result = await self._handler(message.payload)
                if message.reply_to is not None:
                    await message.reply_to.tell(result)
        finally:
            self._running = False
```

---

## `Message`

A frozen dataclass holding one unit of data on a transport.

### Fields

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `payload` | `Any` | — | The application data carried by the message. |
| `reply_to` | `NodeRef \| None` | `None` | A reference the recipient may use to send a reply. `None` for fire-and-forget messages. |

---

## `Transport`

A `Protocol` describing how messages are routed between identities. Any object
providing the two members below satisfies it. `InMemoryTransport` is the
reference single-process implementation.

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `send` | `async send(identity: str, message: Message) -> None` | Deliver `message` to the inbox of `identity`. |
| `inbox` | `inbox(identity: str) -> asyncio.Queue[Message]` | Return the inbox queue for `identity`, creating it if absent. |

---

## `InMemoryTransport`

A single-process `Transport` backed by one `asyncio.Queue` per identity. Inboxes
are created lazily on first access and shared by identity string.

### `InMemoryTransport()`

Takes no arguments. Constructs an empty transport with no inboxes.

### Methods

| Method | Signature | Returns | Description |
|--------|-----------|---------|-------------|
| `inbox` | `inbox(identity: str) -> asyncio.Queue[Message]` | the queue for `identity` | Returns the existing inbox for `identity`, or creates and stores a new unbounded `asyncio.Queue` if none exists. |
| `send` | `async send(identity: str, message: Message) -> None` | `None` | Enqueues `message` on the inbox for `identity` via `put_nowait`. Does not block. |

---

## `NodeRef`

A frozen, hashable, send-only handle to a node. Addresses a node by `identity`
over a `transport`. A `NodeRef` cannot host a node and cannot stop one; it
exposes only the two messaging methods below.

### `NodeRef(identity, transport)`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `identity` | `str` | yes | The address of the target node. |
| `transport` | `Transport` | yes | The transport over which messages are sent. |

### Attributes

| Name | Type | Description |
|------|------|-------------|
| `identity` | `str` | The target node's address. |
| `transport` | `Transport` | The transport used for sends. |

### `tell`

```
async tell(payload: Any) -> None
```

Sends `payload` to the target identity as a fire-and-forget message
(`reply_to` is `None`). Returns once the message is handed to the transport;
does not wait for the recipient to process it.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `payload` | `Any` | yes | The data to send. |

**Returns:** `None`.

### `ask`

```
async ask(payload: Any, *, timeout: float | None = None) -> Any
```

Sends `payload` to the target identity with a fresh single-use reply reference,
then waits for one reply and returns its payload. The reply reference addresses
an ephemeral inbox named `reply:<uuid4>`.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `payload` | `Any` | yes | — | The data to send. |
| `timeout` | `float \| None` | no | `None` | Seconds to wait for a reply. `None` waits indefinitely. |

**Returns:** the `payload` of the reply message (`Any`).

**Raises:** `asyncio.TimeoutError` if no reply arrives within `timeout`.

---

## `Handler`

A type alias for the callable a `Node` invokes per message.

```
Handler = Callable[[Any], Awaitable[Any]]
```

A handler receives a message `payload` and returns an awaitable resolving to the
result. When the originating message carried a `reply_to`, the returned result
is sent back through it.

---

## `Node`

A servant that owns the receive loop for one identity. Constructed with the
handler that processes each message. Hands out `NodeRef` handles to itself but is
never itself a handle.

### `Node(identity, transport, handler)`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `identity` | `str` | yes | The address this node serves. |
| `transport` | `Transport` | yes | The transport whose inbox for `identity` this node consumes. |
| `handler` | `Handler` | yes | The coroutine function invoked once per received message. |

### Attributes and properties

| Name | Type | Description |
|------|------|-------------|
| `identity` | `str` | Read-only property returning the served address. |
| `ref` | `NodeRef` | Read-only property returning a `NodeRef` to this node over the same transport. |

### `ref_to`

```
@staticmethod
ref_to(identity: str, transport: Transport) -> NodeRef
```

Constructs a `NodeRef` for an arbitrary `identity` and `transport` without
constructing a `Node`. Used to obtain a handle to a node hosted elsewhere.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `identity` | `str` | yes | The target address. |
| `transport` | `Transport` | yes | The transport over which the handle sends. |

**Returns:** a `NodeRef`.

### `run`

```
async run() -> None
```

Consumes the transport inbox for this node's identity in a loop. For each
message: awaits `handler(message.payload)`; if `message.reply_to` is not `None`,
sends the result through it via `tell`.

The loop runs until the awaiting task is cancelled. On exit (including
cancellation), the running flag is cleared, after which `run` may be called
again.

**Returns:** `None` (the coroutine does not return normally; it runs until
cancelled).

**Raises:**

| Error | Condition |
|-------|-----------|
| `RuntimeError` | `run` is called while this node instance is already running. |
| `asyncio.CancelledError` | The task awaiting `run` is cancelled. Propagates after the running flag is cleared. |

---

## Examples

### Request/response

```python
async def main() -> None:
    transport = InMemoryTransport()

    async def handler(payload):
        return f"handled {payload!r}"

    server = Node("agent-42", transport, handler)
    task = asyncio.create_task(server.run())

    client = Node.ref_to("agent-42", transport)
    print(await client.ask("do-work"))        # handled 'do-work'

    task.cancel()


asyncio.run(main())
```

### Fire-and-forget

```python
ref = Node.ref_to("agent-42", transport)
await ref.tell("event")                       # returns immediately, no reply
```

### Local handle from a hosted node

```python
server = Node("agent-42", transport, handler)
ref = server.ref                              # NodeRef to server, same transport
await ref.ask("ping")
```

### Bounded wait

```python
try:
    await ref.ask("slow-task", timeout=2.0)
except asyncio.TimeoutError:
    ...                                       # no reply within 2 seconds
```

---

See also: [About the Peer Node Pattern](peer-node-pattern.md) for the concepts,
trade-offs, and the reference-vs-servant rationale behind this API.
