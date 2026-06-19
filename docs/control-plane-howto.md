# How to add a control plane to your nodes

This guide shows you how to make a fleet of nodes publish a record onto a
control-plane topic and read it back as a live, collapsed view of which nodes are
online and what they offer. This is the pattern behind capability discovery,
agent discovery, and access-policy planes.

It assumes you already work with calfkit nodes and workers. For *why* the
substrate is shaped this way — instance-keying, advisory staleness, the
worker/node split — read [About the control-plane substrate](control-plane-substrate.md)
instead.

> The records and nodes below are **illustrative**. The substrate ships as
> machinery; concrete planes land in their own changes. Use these as a template,
> not as an import.

## 1. Define the record

Subclass `ControlPlaneRecord` and add your plane's content. The one hard rule:
**set a concrete `schema_version`** — the base omits it on purpose so a forgotten
version fails loudly when you open a view, not silently at read time.

```python
from calfkit import ControlPlaneRecord

class ServiceCard(ControlPlaneRecord):
    schema_version: int = 1      # required: every record type pins its own version
    dispatch_topic: str          # your plane's content...
    role: str
```

Do not add `node_id`/`worker_id` fields — identity is the wire key, not record
content. Inherited from the base you already get `started_at`,
`last_heartbeat_at`, and `heartbeat_interval`; the publisher fills those.

## 2. Advertise it from a node

Decorate a method with `@advertises(topic=..., record=...)` on any node (the
hook lives on every `BaseNodeDef` subclass — agents, tool nodes, consumers). The
method is a *content factory*: it receives a bare `ControlPlaneStamp` and returns
a fully-formed record.

```python
from calfkit import ControlPlaneStamp, advertises
from calfkit.nodes import BaseNodeDef

class WeatherTool(BaseNodeDef):

    @advertises(topic="calf.services", record=ServiceCard)
    def _service_card(self, stamp: ControlPlaneStamp) -> ServiceCard:
        return ServiceCard(
            **stamp.model_dump(),                 # boot + liveness + cadence
            dispatch_topic=self.subscribe_topics[0],
            role="weather",
        )
```

Two things to get right, both because the publisher re-runs this factory on every
heartbeat tick:

- **Splat the stamp, never a full record.** `**stamp.model_dump()` spreads the
  three worker-stamped fields. (A `ControlPlaneRecord` is itself a stamp, so
  splatting one would smuggle in duplicate keys — a `TypeError` the type checker
  cannot catch.)
- **If your record carries a "last changed" timestamp,** return a value the node
  tracks and advances only on a real change — never `now()` computed in the
  factory, which would move every tick and mask content staleness.

Declaring two adverts for the same topic on one class fails at class-definition
time with `RegistryConfigError`.

## 3. Host the node — publishing is automatic

Host the node on a `Worker`. When any hosted node declares an advert, the worker
auto-wires the publisher and one writer per advert topic; there is nothing else
to register.

```python
from calfkit.client.client import Client
from calfkit.worker.worker import Worker

client = Client.connect("kafka:9092")
worker = Worker(client, nodes=[WeatherTool(node_id="weather-1", subscribe_topics=["weather-1.in"])])
await worker.run()   # after_startup publishes once + starts the heartbeat; on_shutdown tombstones
```

To tune the publisher, pass a config (every field has a working default, so this
is optional):

```python
from calfkit import ControlPlaneConfig

worker = Worker(client, nodes=[...], control_plane=ControlPlaneConfig(heartbeat_interval=15.0))
```

## 4. Read the plane

Open a `ControlPlaneView` over the same topic with the same record type, start
it, and wait for it to catch up before the first read. The view presents a live,
collapsed `node_id → record` map.

```python
from calfkit import ControlPlaneView

view: ControlPlaneView[ServiceCard] = ControlPlaneView.open(
    bootstrap_servers="kafka:9092",
    topic="calf.services",
    record_type=ServiceCard,
)
await view.start()
try:
    await view.barrier()                 # wait until the compacted log has replayed
    card = view.get("weather-1")         # the one live record, or None
    online = view.online_nodes()         # {node_id, ...} with a live instance
    everyone = view.snapshot()           # {node_id: record} for all online nodes
finally:
    await view.stop()
```

The reader needs no agreement with the writer on cadence — `heartbeat_interval`
arrives on each record, so staleness is judged correctly even from a different
process or a different Kafka cluster (point `bootstrap_servers` wherever the
plane lives).

To tie the view's lifecycle to a worker instead of managing it by hand, open it
inside a `@resource` (open + `start()` on setup, `stop()` on teardown); see
[worker lifecycle](worker-lifecycle.md).

## Adapt it

- **Tune staleness on the read side.** By default a node drops out of the view
  after `3 × heartbeat_interval` with no fresh record. Override with
  `ControlPlaneView.open(..., stale_after=45.0)` to set an explicit threshold.

- **Create the topic.** With provisioning enabled (dev/CI) the worker's writer
  creates the compacted topic idempotently — bring up either side first. In
  production, create it out-of-band like any governed topic
  (`cleanup.policy=compact`, RF ≥ 3); see [topic provisioning](topic-provisioning.md).
  A view does not create topics unless you pass `ensure_topic=True`.

- **Evolve the schema.** Adding a field is safe: records are tolerant readers and
  ignore unknown fields, so old and new writers interoperate. For a *breaking*
  change, bump `schema_version`. A view hides records whose `schema_version`
  exceeds its own and logs a one-time warning per instance, so a node on a newer
  schema is invisible (not misread) to an old reader until that reader upgrades.

- **Expect crash vs. clean-shutdown to differ.** A node that *crashes* keeps its
  last record until it ages out (stale-after); a node that shuts down *cleanly*
  tombstones itself and disappears at once. Both are intended — see the
  [explanation](control-plane-substrate.md#liveness-is-advisory-and-it-travels-on-the-record).

See also: [About the control-plane substrate](control-plane-substrate.md) for the
reasoning, and the [design spec](designs/control-plane-substrate-spec.md) for the
exact contract.
