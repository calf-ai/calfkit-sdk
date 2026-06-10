# About the Peer Node Pattern

A recurring question in this project is: *we have a `Node` that is deployed to
serve calls, but it also carries its own identity — so can't the same object be
used on the client side to invoke the server? Is one object that is both client
and server a good idea, or an anti-pattern?*

This document is the knowledge base for that question. It is **understanding-oriented**:
it explains where the pattern comes from, what the established frameworks call it,
why they all converge on the same shape, and where the genuine trade-offs lie. It
is not a step-by-step guide and not an API reference — it exists to be read while
reflecting on a design, not while mid-task.

The short version, which the rest of the document unpacks:

> A node keyed by an **identity** that can both *host* work and *invoke* work is a
> legitimate, well-established pattern — it is the **actor model with
> identity-as-address**. The mistake is not having both roles; it is collapsing
> the *running host* and the *callable handle* into a single type. Every mature
> framework keeps them separate.

---

## The pattern, and what to call it

There is no single industry term for "one class that is both client and server."
What you are reaching for already has a name from a different angle: the **actor
model**, where each actor is reachable through an opaque **address** and is, by
construction, *both a sender and a receiver of messages*. The unification you
sensed is real — but it lives in the *behaviour* (an actor sends and receives),
not in a single do-everything class.

The supporting ideas that orbit this are worth naming, because they recur across
the literature:

- **Identity-as-address.** The node's identity *is* its routing address. In
  Erlang/OTP a process can be registered under a name, and any process anywhere
  sends to it by that name. In Akka the actor's *path* encodes its location and
  routing. In this project, a node's identity maps to a Kafka topic — same idea,
  different transport.
- **Location transparency.** A handle to a node works the same way whether the
  node is in-process or across the network. This is a *goal*, not a free lunch —
  see [The limits of location transparency](#the-limits-of-location-transparency).
- **Reference vs. servant.** The thing you *call through* is distinct from the
  thing that *does the work*. This is the crux of the whole topic, and the next
  section is about it.
- **"Peer."** In peer-to-peer systems a "peer" is a node that is simultaneously
  client and server. That is a statement about *network topology* (no central
  server), which is a different claim from "one type plays both roles." The two
  often get conflated; keeping them apart avoids a lot of confusion.

So the honest answer to "is this a pattern or an anti-pattern?" is: *the pattern
is sound; one specific way of structuring it is the anti-pattern.*

---

## Why every framework separates the reference from the servant

The single most consistent finding across the established systems is that they
all split the **handle** (what a caller holds) from the **servant** (what runs
the work) — and none of them endorse a one-type client+server object. This is not
a coincidence; the same forces produce the same answer each time.

| System | The callable handle | The running servant |
|---|---|---|
| Akka (Typed) | `ActorRef` — immutable, serializable, **send-only**; it cannot stop the actor | the actor and its receive behaviour |
| Erlang/OTP | a registered **name** (or pid) used via thin `call`/`cast` wrappers | the `gen_server` process and its callbacks |
| CORBA / RMI | the **stub** — a local proxy | the **servant** that implements the interface |
| Pyro5 (Python) | `Proxy` — a client-side stand-in | the registered object on the server |

The recurring shape is: a **lightweight, send-only reference** that is cheap to
pass around and safe to hand to untrusted-ish code, plus a **heavier servant**
that owns the receive loop and the resources. Akka states the contract most
sharply — an `ActorRef` *cannot* terminate the actor it points at. The reference
deliberately exposes *less* than the servant.

### The two forces that drive the split

**1. A handle should not be able to host.** If the only type you have is the full
node, then any code holding one can call its `run()`/serve method. In a
message-bus system that is not a harmless mistake: starting a second host on the
same identity means a second consumer bound to the same topic. Depending on
consumer-group configuration you get either rebalancing that fights the real
host, or *two processes handling every message* — duplicated side effects, and a
bug that is silent until it corrupts something. Removing the host method from the
handle makes that mistake impossible *by construction* rather than by discipline.

**2. A handle should not pay the servant's construction cost.** Constructing a
real servant typically opens resources — a database pool, the topic binding, the
receive buffer. A caller only needs to *send*; it should not be paying to
allocate server-side machinery it will never use. A pure, identity-only reference
sidesteps that entirely.

The cleanest expression of the whole idea that the research surfaced is **Akka
Typed's reply-to reference**: the caller puts *its own* reference inside the
request message, and the receiver replies through it. No global registry lookup,
fully composable across hops:

```scala
// Akka Typed — the canonical shape
final case class Greet(whom: String, replyTo: ActorRef[Greeted])
//                                    ^^^^^^^ the caller's own send-only handle
```

Translated into a self-contained Python sketch that embodies the same
disciplines — reference ≠ servant, reply-to carried in the message, identity as
address:

```python
import asyncio
from dataclasses import dataclass

# A stand-in "bus": identity -> inbox. In this project the transport is Kafka,
# and the identity maps to a topic; the shape of the pattern is identical.
_BUS: dict[str, asyncio.Queue] = {}


def _inbox(identity: str) -> asyncio.Queue:
    return _BUS.setdefault(identity, asyncio.Queue())


@dataclass(frozen=True)
class NodeRef:
    """The handle. Immutable, serializable, send-only.
    It deliberately has no run()/serve method — a caller cannot host."""

    identity: str

    async def call(self, payload, reply_to: "NodeRef | None" = None):
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        _inbox(self.identity).put_nowait((payload, reply_to, future))
        return await future


class Node:
    """The servant. Owns the receive loop and (in real life) the resources.
    It mints references to itself; it is never handed out wholesale."""

    def __init__(self, identity: str) -> None:
        self._identity = identity

    @property
    def ref(self) -> NodeRef:                 # local handle to a node I hold
        return NodeRef(self._identity)

    @staticmethod
    def ref_to(identity: str) -> NodeRef:     # remote handle from a bare identity
        return NodeRef(identity)

    async def run(self) -> None:
        inbox = _inbox(self._identity)
        while True:
            payload, reply_to, future = await inbox.get()
            result = f"[{self._identity}] handled {payload!r}"
            if reply_to is not None:          # reply through the caller's handle
                await reply_to.call(result)
            future.set_result(result)
```

```python
async def main() -> None:
    # Server deployment: build the servant and serve.
    server = Node("agent-42")
    asyncio.create_task(server.run())

    # Client side: a send-only handle. It cannot accidentally run().
    client = Node.ref_to("agent-42")
    print(await client.call("do-work"))       # [agent-42] handled 'do-work'


asyncio.run(main())
```

---

## "But it's the same codebase" — same class, different instances

A natural objection is: *in a monorepo the `Node` class is importable on both
sides, so why not just construct a `Node` on the client and call it directly?*
This is worth answering carefully, because the answer corrects a subtle mental
slip.

You *can* — nothing forbids `n = Node("agent-42"); n.ref.call(...)`, and that is
exactly what the local `ref` property is for. The slip is in the phrase "the same
`Node` object." Sharing a class is a **compile-time** fact. At **runtime** the
client and the server are two different instances in two different processes; the
client's `Node("agent-42")` is *not* the server's `Node("agent-42")`. They share
nothing in memory — only the identity string, which the transport uses to route.
The client instance's own inbox is never read by anyone.

That is precisely why the host method is dangerous on the client side: it is not
a no-op. `n.run()` in the client process starts a *real, competing* consumer on
the shared identity. The handle/servant split exists so that the thing you hold
on the calling side simply *has no way* to do that.

This also clarifies when the split earns its keep:

- If your servant's constructor is **pure** (it only stores the identity), then
  building a `Node` on the client to get a reference is harmless, and a collapsed
  design is defensible.
- The moment hosting means **owning resources or a consumer slot** — which, for a
  Kafka-backed node, it almost always does — the split stops being stylistic. A
  reference that cannot host and cannot allocate server state is the difference
  between a safe handle and a loaded foot-gun.

---

## Choices and alternatives

Reasonable people draw the line in different places. The trade-offs:

**Collapsed (one type, both `run()` and `call()`).** Maximally DRY and symmetric;
the simplest thing that can work. Acceptable when the servant constructor is pure
and hosting carries no cost — for example a pure in-process actor used only for
testing or a single-process demo. Its cost is that every holder of the object can
host and can pay construction cost, and intent ("am I a host or a caller?") is
ambiguous at the call site.

**Separated reference + servant (recommended here).** A send-only `NodeRef` and a
hosting `Node`, both minted from the same identity. Costs one extra small type.
Buys: hosting is impossible from a handle, callers don't pay server construction,
and the type at a call site *tells you* the role. This is the unanimous choice of
Akka, Erlang/OTP, CORBA/RMI and Pyro5, and it is the right default for
calfkit-style nodes over Kafka.

**Generated stub + skeleton (gRPC/Thrift-style).** A code generator emits the
client stub and the server skeleton from a shared interface definition. This is
the separated model taken to its logical end, with the two halves produced
mechanically. It buys strong cross-language contracts at the cost of a build step
and generated code — worthwhile when clients are polyglot, overkill when caller
and host share a language and a repo.

A note on terminology drift: it is tempting to call the separated design a
"peer." It can be deployed in a peer-to-peer *topology*, but the separation
itself is orthogonal to topology. Keep the two ideas distinct when discussing a
design, or reviews go in circles.

---

## The limits of location transparency

Location transparency — "a handle works the same whether the node is local or
remote" — is the property that makes the whole pattern feel clean. It is a
worthwhile *goal*, but treating it as *complete* is the classic distributed-objects
error.

The verification pass behind this document specifically **refuted** the strong
claim that actor references make local and remote communication truly identical,
and it **refuted** the idea that remote-object systems are inherently symmetric
peers (objects switch roles *sequentially*; they are not magically both at once).
Both refutations point at the same underlying truth, articulated decades ago in
Waldo et al.'s *A Note on Distributed Computing* and in Martin Fowler's *First Law
of Distributed Object Design — "don't distribute your objects"*: a remote call has
latency, partial failure, and concurrency semantics that a local call does not,
and an abstraction that hides that boundary too well produces fragile systems.

The practical lesson for this project is not "avoid the pattern." It is: keep the
remote boundary *honest*. The reference should expose **send/ask** semantics that
make asynchrony and failure visible (the `await ...call()` in the sketch above is
explicit for exactly this reason), rather than masquerading as an ordinary local
method that silently crosses the network.

---

## Where this lands for calfkit

The first shipped instance of this pattern is MCP tooling: `MCPToolbox` is
the servant (hosts the MCP session, deploys via `add_nodes`), and
`MCPToolboxRef` is the reference (identity + optional tool scoping, zero
deployment knowledge — a passive resolution token rather than an RPC proxy) that agents hold in `tools=[...]` — see
[mcp-tool-discovery](mcp-tool-discovery.md).

The MCP bridge node — a node that both exposes tools and calls other nodes — is a
genuine and natural instance of this pattern. The guidance that falls out of the
above:

- Treat the node's **identity as its address** (it already maps to a Kafka topic).
- Expose a **send-only reference** for the call side rather than handing callers
  the full hosting node, so that hosting and resource ownership cannot leak onto
  the calling path.
- Keep the call surface **explicitly asynchronous** so the network boundary stays
  visible.

This is the same conclusion Akka and Erlang reached independently, applied to a
Kafka transport.

See also: [Reference: Node / NodeRef API](peer-node-pattern-reference.md) for a
complete, runnable canonical implementation of the types discussed here.

---

*The implementation in the source tree is always the source of truth; the Python
above is an illustrative sketch of the pattern, not a description of a shipped
API. If you find a calfkit node whose structure contradicts this document, trust
the code and flag the doc for reconciliation.*
