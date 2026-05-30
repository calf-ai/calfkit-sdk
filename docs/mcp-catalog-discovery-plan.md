# MCP Catalog Discovery — Implementation Sub-Plan (Archived Alternative)

**Status:** ⚠️ **Archived — not selected for v1.** Kept as a record of the design alternative considered. v1 ships with codegen-generated schema declarations (see [`docs/mcp-adaptor-implementation-plan.md`](./mcp-adaptor-implementation-plan.md)). A future revision may revisit runtime discovery via the MCP-over-Kafka RPC pattern documented in [`docs/mcp-discovery-rpc-design.md`](./mcp-discovery-rpc-design.md).
**Document version:** 1.0
**Last updated:** 2026-05-30
**Originally companion to:** [`docs/mcp-adaptor-design.md`](./mcp-adaptor-design.md), [`docs/mcp-adaptor-implementation-plan.md`](./mcp-adaptor-implementation-plan.md)
**Scope (historical):** The Kafka compacted-topic catalog mechanism — how bridge workers would publish their tool schemas, how agent workers would read them, and the lifecycle plumbing that would make it race-free.

## Why archived

This sub-plan proposed runtime schema discovery via a Kafka compacted topic. v1 chose a different approach (codegen-generated declarations) for these reasons:

1. **Aligns with the dominant event-driven schema pattern** — protobuf/Avro/Confluent Schema Registry all favour schemas-as-artifacts over runtime introspection.
2. **Eliminates FastStream-internals access** — the compacted-topic approach required reaching into `broker.config.*` (admin client + consumer builder), which this plan documented as a deliberate but uncomfortable trade-off.
3. **Removes the bootstrap-ordering coupling** between bridge and agent workers at startup.
4. **Reviewable schema changes** via PR diffs on generated files.
5. **Smaller v1 implementation surface** — ~80 LOC of catalog mechanics + ~50 LOC of FastStream-internals shim deleted.

The analysis below remains valuable as historical context for why the codegen path was selected and what implementation cost the runtime-discovery alternative would have carried.

---

## 1. Executive summary

This document plans the catalog discovery feature: a Kafka compacted topic per MCP server that holds the bridge worker's last-published tool catalog, which agent workers read once at startup to populate their `tools_registry`.

The plan leans on FastStream natively where possible. Two places require reaching past FastStream into raw aiokafka primitives:

1. **Topic creation with `cleanup.policy=compact`** — FastStream has no abstraction here; we use `broker.config.admin_client` directly.
2. **One-shot read-to-end-of-partition** at agent boot — FastStream subscribers are long-lived pollers, not bounded readers; we use `broker.config.builder` to spawn a bounded `AIOKafkaConsumer` outside the subscriber lifecycle.

Both deviations are isolated in a single internal module (`calfkit/mcp/_fs_internals.py`) so a future FastStream version bump that renames these attributes only touches one file. Total hand-rolled code: **~70 LOC** including error handling and tests.

**Pin:** `faststream[kafka]>=0.6.6,<0.7`. The 0.6.x line is stable; 0.7.0 may rename `broker.config.*`.

---

## 2. Architecture

```
                       ┌─────────────────────────────────┐
                       │  Bridge worker (process A)       │
                       │  Worker(nodes=[gmail])           │
                       │                                  │
                       │  Lifecycle:                      │
                       │   1. on_startup:                 │
                       │      • broker.connect()          │
                       │      • _ensure_topic(            │
                       │           mcp.gmail.catalog,     │
                       │           compact)               │
                       │      • McpServer._discover()     │
                       │      • broker.publish(           │
                       │           catalog,               │
                       │           topic=mcp.gmail.       │
                       │              catalog,            │
                       │           key=b"gmail")          │
                       │   2. broker.start():             │
                       │      • McpBridge subscribers     │
                       │        for each tool topic       │
                       │                                  │
                       └─────────┬───────────────────────┘
                                 │
                                 │ Publishes once at startup
                                 ▼
                       ┌─────────────────────────────────┐
                       │  Kafka topic                    │
                       │  mcp.gmail.catalog              │
                       │  cleanup.policy=compact         │
                       │  key=b"gmail" → JSON value      │
                       │  (latest message retained       │
                       │   forever via compaction)       │
                       └─────────┬───────────────────────┘
                                 │
                                 │ Read once at startup
                                 ▼
                       ┌─────────────────────────────────┐
                       │  Agent worker (process B)        │
                       │  Worker(nodes=[scribe])          │
                       │  (scribe declares tools=[gmail]) │
                       │                                  │
                       │  Lifecycle:                      │
                       │   1. on_startup:                 │
                       │      • broker.connect()          │
                       │      • _read_catalog_once(       │
                       │           mcp.gmail.catalog,     │
                       │           key=b"gmail",          │
                       │           timeout=60s)           │
                       │      • populate                  │
                       │        gmail._discovered         │
                       │   2. broker.start():             │
                       │      • scribe subscribes to its  │
                       │        own input topic           │
                       │                                  │
                       └─────────────────────────────────┘
```

Key invariants:

- **Bridge worker is the only writer.** Multiple bridge replicas all publish the same content; Kafka compaction keeps the latest.
- **Agent workers are read-only consumers.** They read once at boot using a unique consumer group (so they always re-read from the start).
- **The catalog topic is the single source of truth across processes.** No file system dependency, no service discovery, no HTTP.

---

## 3. FastStream functionality used

The following FastStream APIs are leveraged. Every usage is checked against `faststream[kafka]==0.6.6` source.

### 3.1 `KafkaBroker.publish(message, topic=..., key=..., ...)` — bridge publication

`faststream/kafka/broker/broker.py:526–540`. Sends a message with a Kafka record key (for compaction). Fully sufficient; we pass `key=server._name.encode("utf-8")`.

```python
# In Worker._on_startup, bridge side:
await broker.publish(
    json.dumps(catalog_payload).encode("utf-8"),
    topic=f"mcp.{server._name}.catalog",
    key=server._name.encode("utf-8"),
    headers={"content-type": "application/json"},
)
```

**Notes:** the `key` kwarg accepts `bytes | Any | None`; we always pass `bytes` to be explicit. FastStream's `correlation_id` is auto-generated and unused for this publish.

### 3.2 `broker.connect()` — explicit producer initialization

`faststream/_internal/broker/broker.py` (inherited from `BrokerUsecase`). Idempotent against the producer state. Required because publishing in `@app.on_startup` happens *before* `broker.start()`, at which point the producer is in `EmptyProducerState` and would raise `IncorrectState`.

```python
async def _on_startup(self) -> None:
    await self._client._connection.connect()  # ← required before any publish
    # ... topic creation, catalog publication ...
```

**Notes:** safe to call before `broker.start()` runs; the broker tracks connect state and `start()` will short-circuit the producer re-init. Calfkit already does this in `client/base.py:152` against the same broker class.

### 3.3 `FastStream(broker, on_startup=[...], on_shutdown=[...])` — lifecycle hooks

`faststream/_internal/application.py`. The `on_startup` hook fires **before** `broker.start()` (which is when subscribers begin polling), making it the correct seam for catalog publication and reading.

Lifecycle ordering (source-verified):

```
1. on_startup hooks      ← publish catalog here (bridge) or read it (agent)
2. broker.start()        ← connects + starts subscribers
3. after_startup hooks   ← too late: subscribers already consuming
```

```python
# Worker.run() patch
app = FastStream(
    self._client._connection,
    on_startup=[self._on_startup],
    on_shutdown=[self._on_shutdown],
)
await app.run(**extra_run_args)
```

### 3.4 `AckPolicy.MANUAL` — not used in v1

We do not subscribe to the catalog topic via `@broker.subscriber` because that creates a long-lived poller. v2 may add hot-reload via a long-lived subscriber with `ack_policy=AckPolicy.MANUAL` and `auto_offset_reset="earliest"`. Out of v1 scope.

### 3.5 Native AsyncAPI emission

FastStream auto-generates AsyncAPI specs from registered subscribers and publishers. Because our catalog publish goes through `broker.publish` (not through `@broker.publisher`-decorated handlers), the catalog topic will **not** appear in AsyncAPI by default. We accept this — the catalog is calfkit-internal infrastructure, not a public surface. Adding it to AsyncAPI is a v1.1 polish if operators ask.

---

## 4. Hand-rolled components (and what they undermine)

Two components reach past FastStream into raw aiokafka. Each is justified, isolated, and bounded.

### 4.1 `_ensure_compacted_topic(broker, name)` — ~15 LOC

**What it does:** creates the catalog topic with `cleanup.policy=compact` if it doesn't already exist.

**Why FastStream doesn't cover it:** FastStream has no first-class topic-management API. The `allow_auto_create_topics` knob that existed in 0.5.x is gone in 0.6.x, and even when present, it relied on broker-side defaults that don't include compaction.

**What we use instead:** `broker.config.admin_client`, which is a fully-started `aiokafka.admin.AIOKafkaAdminClient` that FastStream wires up during `broker.connect()` for its own internal `ping()` method. We borrow it.

```python
# calfkit/mcp/_catalog_admin.py

from aiokafka.admin import NewTopic
from aiokafka.errors import TopicAlreadyExistsError


async def ensure_compacted_topic(
    broker: "KafkaBroker",
    name: str,
    partitions: int = 1,
    replication: int = 1,
) -> None:
    """Create the catalog topic with cleanup.policy=compact if missing.

    Reaches into broker.config.admin_client (FastStream-internal) because
    FastStream offers no first-class topic-management abstraction at v0.6.x.
    """
    await broker.connect()  # ensure admin_client exists
    admin = broker.config.admin_client
    topic = NewTopic(
        name=name,
        num_partitions=partitions,
        replication_factor=replication,
        topic_configs={
            "cleanup.policy": "compact",
            "min.cleanable.dirty.ratio": "0.01",   # cleaner runs aggressively
            "segment.ms": "60000",                  # roll segments fast → compaction can run
            "delete.retention.ms": "100",
        },
    )
    try:
        await admin.create_topics([topic])
    except TopicAlreadyExistsError:
        pass  # idempotent; we don't assert config on existing topics in v1
```

**What this undermines:**

- We bypass FastStream's lifecycle for topic management. If the cluster auto-creates topics on first publish (default cluster config), our `ensure_compacted_topic` runs first; if it doesn't (strict clusters), this is the only path that gets compacted topics created.
- The topic does not appear in FastStream's AsyncAPI as a managed topic. Acceptable: it's infrastructure, not a public publish/subscribe contract.
- We have no test-time equivalent — `TestKafkaBroker` does not simulate compaction. Tests of the topic-creation logic require a real Kafka instance (Docker compose, CI lane).

**Risk mitigation:**

- Isolated to one module (`_catalog_admin.py`). A future FastStream API for topic mgmt = one-file refactor.
- `TopicAlreadyExistsError` is the only expected error; everything else is logged and re-raised.
- v1.1 may add an `assert_config` mode that calls `admin.describe_configs` and warns on drift.

### 4.2 `_read_catalog_once(broker, topic, key, timeout)` — ~40 LOC

**What it does:** reads all messages currently in a compacted topic until end-of-partition, returns the latest message matching the given key.

**Why FastStream doesn't cover it:** FastStream's `@broker.subscriber` is a long-lived poller. There is no "read to current high-water-mark then stop" helper. The closest fits — `ack_policy=MANUAL`, `auto_offset_reset="earliest"` — give us the right *configuration* but not the *bounded lifecycle*.

**What we use instead:** `broker.config.builder`, which is a `functools.partial(aiokafka.AIOKafkaConsumer, ...)` with FastStream's connection params (bootstrap servers, security, client_id) pre-baked. We add the per-consumer kwargs (`group_id`, `auto_offset_reset`, `enable_auto_commit`) and own the consumer's lifecycle.

```python
# calfkit/mcp/_catalog_reader.py

import asyncio
import json
import logging
import time
import uuid
from typing import Any

logger = logging.getLogger(__name__)


class CatalogNotPublishedError(RuntimeError):
    """Raised when the catalog topic exists but holds no message for the requested key."""


async def read_catalog_once(
    broker: "KafkaBroker",
    topic: str,
    key: bytes,
    *,
    timeout_s: float = 60.0,
) -> dict[str, Any]:
    """Read the latest catalog for `key` from a compacted topic.

    Uses a unique consumer group per invocation (UUID) so that on each
    invocation we read from the partition's beginning. On a compacted
    topic, "beginning" is effectively "current state" — Kafka has already
    discarded prior versions of the same key.

    Reaches into broker.config.builder (FastStream-internal) to obtain a
    pre-configured AIOKafkaConsumer factory.
    """
    await broker.connect()
    consumer = broker.config.builder(
        topic,
        group_id=f"calfkit-mcp-catalog-{uuid.uuid4()}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        # Force partition assignment, then seek to beginning.
        # The first getmany triggers group join + assignment.
        await consumer.getmany(timeout_ms=500)
        partitions = consumer.assignment()
        if not partitions:
            raise CatalogNotPublishedError(
                f"No partitions assigned for topic {topic!r}. "
                f"Does the topic exist?"
            )
        await consumer.seek_to_beginning()
        end_offsets = await consumer.end_offsets(list(partitions))

        # Empty topic → no catalog has been published yet.
        if all(off == 0 for off in end_offsets.values()):
            raise CatalogNotPublishedError(
                f"Catalog topic {topic!r} is empty. Has the bridge "
                f"worker published yet? It must boot at least once "
                f"before agent workers."
            )

        latest_value: bytes | None = None
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            batch = await consumer.getmany(timeout_ms=1000, max_records=500)
            for tp, records in batch.items():
                for record in records:
                    if record.key == key:
                        latest_value = record.value
            # Stop when each partition has been consumed up to its
            # high-water-mark — i.e. we've seen everything currently
            # in the topic.
            if all(consumer.position(tp) >= end_offsets[tp] for tp in partitions):
                break

        if latest_value is None:
            raise CatalogNotPublishedError(
                f"Topic {topic!r} has messages but none with key={key!r}."
            )

        return json.loads(latest_value)
    finally:
        await consumer.stop()
```

**What this undermines:**

- **AsyncAPI invisibility.** This consumer doesn't appear in FastStream's AsyncAPI export. The catalog topic is documented separately (see §13 of the impl plan).
- **No `TestKafkaBroker` coverage.** FastStream's test broker simulates message routing but not the full aiokafka consumer API (`seek_to_beginning`, `end_offsets`, `position`). Tests of this function require a real Kafka — included as part of the integration test lane.
- **No FastStream middleware.** Calfkit's broker doesn't yet use middleware, but if it ever did, this consumer would bypass it. Acceptable in v1; an `apply_middleware=True` flag could be added later if needed.
- **Manual offset bookkeeping.** We don't commit offsets (we don't need to — group ID is throwaway). This is what `enable_auto_commit=False` does, which FastStream wouldn't have offered cleanly without `AckPolicy.MANUAL` + a long-lived subscriber.

**Risk mitigation:**

- Isolated to one module (`_catalog_reader.py`). A future FastStream API for bounded reads = one-file refactor.
- `CatalogNotPublishedError` has actionable error message identifying the bridge dependency.
- aiokafka version compatibility: positional `topic` arg to `builder(topic, ...)` may require explicit `subscribe()` in some versions. Verified at integration test time; if needed, switched to explicit `TopicPartition` assignment.

### 4.3 `_fs_internals.py` — the containment shim

A 20-line module that wraps every `broker.config.*` access so the rest of the code never imports FastStream internals directly. This is the "blast radius" containment.

```python
# calfkit/mcp/_fs_internals.py

"""Internal shim around FastStream's broker.config.* accessors.

FastStream <0.7 exposes admin_client and builder via broker.config.
These attributes are not part of FastStream's documented stable API.
Containing all such accesses here means a future FastStream rename
touches one file.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer
    from aiokafka.admin import AIOKafkaAdminClient
    from faststream.kafka import KafkaBroker


def admin_client(broker: "KafkaBroker") -> "AIOKafkaAdminClient":
    """Return the FastStream-managed Kafka admin client.

    Available after broker.connect() / broker.start(). Raises AttributeError
    on FastStream versions that have moved this attribute.
    """
    return broker.config.admin_client


def consumer_builder(broker: "KafkaBroker"):
    """Return the FastStream-managed AIOKafkaConsumer factory.

    A functools.partial with bootstrap_servers, security, and client_id
    pre-baked. Callers add group_id, auto_offset_reset, etc.
    """
    return broker.config.builder


def producer_is_connected(broker: "KafkaBroker") -> bool:
    """Whether broker.connect() has been called and the producer is live."""
    return bool(broker.config.producer)
```

Every other module in `calfkit/mcp/` imports from this shim, never from `broker.config.*` directly. CI lint rule (optional): `grep "broker.config\." -- calfkit/mcp/` must return only `_fs_internals.py`.

---

## 5. Module layout

New files under `calfkit/mcp/`:

```
calfkit/mcp/
├── _catalog_admin.py        ← ensure_compacted_topic       (~25 LOC inc. docstrings)
├── _catalog_reader.py       ← read_catalog_once            (~60 LOC inc. docstrings)
├── _catalog_writer.py       ← publish_catalog              (~40 LOC)
├── _catalog_message.py      ← CatalogMessage dataclass     (~50 LOC)
└── _fs_internals.py         ← FastStream-internal shim     (~30 LOC)
```

Existing files modified:

```
calfkit/worker/worker.py     ← _on_startup uses the catalog helpers
```

Total new code: **~205 LOC** across 5 small files, of which **~75 LOC** is the actual catalog mechanism and the rest is types/docstrings/validation. The "undermining FastStream" footprint is **~80 LOC** (admin + reader + shim).

---

## 6. Catalog message protocol

### 6.1 Wire format (JSON, UTF-8 encoded)

```json
{
  "schema_version": 1,
  "server": "gmail",
  "published_at": "2026-05-29T12:34:56.789Z",
  "published_by": {
    "calfkit_version": "0.3.5",
    "bridge_node_id": "mcp_bridge_gmail",
    "hostname": "tools-worker-0",
    "pid": 4421
  },
  "mcp_server_info": {
    "name": "gmail-mcp-server",
    "version": "1.2.0",
    "protocol_version": "2025-11-25"
  },
  "tools": [
    {
      "name": "search",
      "description": "Search emails",
      "input_schema": { "type": "object", "properties": {...} },
      "output_schema": null,
      "annotations": {
        "read_only_hint": true,
        "destructive_hint": false,
        "idempotent_hint": true,
        "open_world_hint": true
      },
      "meta": null
    }
  ]
}
```

### 6.2 Kafka record envelope

| Field | Value |
|---|---|
| Topic | `mcp.<server>.catalog` (e.g. `mcp.gmail.catalog`) |
| Key | `<server>` as UTF-8 bytes (`b"gmail"`) |
| Value | JSON document above, UTF-8 encoded |
| Headers | `content-type: application/json`, `schema-version: 1` |
| Partition | 0 (single partition per server) |

### 6.3 `CatalogMessage` dataclass

```python
# calfkit/mcp/_catalog_message.py

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any
import json


CATALOG_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class PublisherInfo:
    calfkit_version: str
    bridge_node_id: str
    hostname: str
    pid: int


@dataclass(frozen=True)
class McpServerInfo:
    name: str
    version: str
    protocol_version: str


@dataclass(frozen=True)
class CatalogToolSpec:
    name: str
    description: str | None
    input_schema: dict[str, Any]
    output_schema: dict[str, Any] | None
    annotations: dict[str, Any] | None
    meta: dict[str, Any] | None


@dataclass(frozen=True)
class CatalogMessage:
    server: str
    published_at: str            # ISO 8601, UTC
    published_by: PublisherInfo
    mcp_server_info: McpServerInfo
    tools: list[CatalogToolSpec]
    schema_version: int = CATALOG_SCHEMA_VERSION

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> "CatalogMessage":
        data = json.loads(raw)
        version = data.get("schema_version")
        if version != CATALOG_SCHEMA_VERSION:
            raise CatalogSchemaVersionError(
                f"Expected schema_version={CATALOG_SCHEMA_VERSION}, got {version!r}. "
                f"Upgrade calfkit, or check that bridge and agent are on compatible versions."
            )
        return cls(
            server=data["server"],
            published_at=data["published_at"],
            published_by=PublisherInfo(**data["published_by"]),
            mcp_server_info=McpServerInfo(**data["mcp_server_info"]),
            tools=[CatalogToolSpec(**t) for t in data["tools"]],
        )
```

### 6.4 Schema evolution

The `schema_version` field on the catalog message enables forward-compatible evolution. v2 message changes that add fields are read by v1 with `_unknown_fields` ignored; v2 additions that change semantics bump the version and v1 readers fail loud (`CatalogSchemaVersionError`).

---

## 7. Bridge worker startup flow

### 7.1 Sequence

```
Worker.run() called
  └─ FastStream(broker, on_startup=[self._on_startup], on_shutdown=[...]).run()
       └─ on_startup hooks fire:
            ├─ broker.connect()  ← producer + admin_client now live
            ├─ for each McpServer in self._mcp_servers:
            │    ├─ ensure_compacted_topic(broker, f"mcp.{server._name}.catalog")
            │    ├─ await server._discover(ephemeral=False)
            │    │      └─ opens MCP session, initialize, tools/list
            │    │         keeps session alive for runtime call_tool
            │    ├─ catalog = build_catalog_message(server)
            │    ├─ await broker.publish(catalog.to_bytes(),
            │    │                       topic=f"mcp.{server._name}.catalog",
            │    │                       key=server._name.encode("utf-8"),
            │    │                       headers={"content-type": "application/json"})
            │    └─ register McpBridge instances for each tool
            └─ broker.start()  ← subscribers go live
```

### 7.2 Failure modes during startup

| Failure | Outcome |
|---|---|
| `broker.connect()` fails | Worker fails to start; aiokafka error propagates |
| `ensure_compacted_topic` fails (cluster rejects compaction config) | Worker fails to start; user must check Kafka cluster ACLs |
| `server._discover()` fails (subprocess crash, OAuth expired) | Worker fails to start; clear error message identifies the MCP server |
| `broker.publish()` fails | Worker fails to start; catalog never reached Kafka, no agent should boot against a partially-deployed bridge |
| Catalog publication races multi-replica bridges | Last-write-wins under compaction; eventual consistency reached within one segment roll (~60s, configurable via `segment.ms`) |

### 7.3 `_publish_catalog` helper

```python
# calfkit/mcp/_catalog_writer.py

import os
import socket
from datetime import datetime, timezone
from calfkit import __version__ as calfkit_version
from ._catalog_message import (
    CatalogMessage, CatalogToolSpec, McpServerInfo, PublisherInfo,
)


async def publish_catalog(
    broker: "KafkaBroker",
    server: "McpServer",
) -> None:
    """Publish a server's discovered catalog to its compacted Kafka topic.

    Must be called after server._discover() has populated server._discovered
    and server._mcp_server_info, and after broker.connect() has been called
    (which on_startup ensures).
    """
    if server._discovered is None:
        raise RuntimeError(
            f"publish_catalog called on {server._name} before discovery completed"
        )

    message = CatalogMessage(
        server=server._name,
        published_at=datetime.now(timezone.utc).isoformat(),
        published_by=PublisherInfo(
            calfkit_version=calfkit_version,
            bridge_node_id=f"mcp_bridge_{server._name}",
            hostname=socket.gethostname(),
            pid=os.getpid(),
        ),
        mcp_server_info=McpServerInfo(
            name=server._initialize_result.server_info.name,
            version=server._initialize_result.server_info.version,
            protocol_version=server._initialize_result.protocol_version,
        ),
        tools=[
            CatalogToolSpec(
                name=m.name,
                description=m.description,
                input_schema=m.input_schema,
                output_schema=m.output_schema,
                annotations=m.annotations,
                meta=m.meta,
            )
            for m in server._discovered
        ],
    )

    await broker.publish(
        message.to_bytes(),
        topic=f"mcp.{server._name}.catalog",
        key=server._name.encode("utf-8"),
        headers={
            "content-type": "application/json",
            "schema-version": str(CATALOG_SCHEMA_VERSION),
        },
    )
```

---

## 8. Agent worker startup flow

### 8.1 Sequence

```
Worker.run() called
  └─ FastStream(broker, on_startup=[self._on_startup], on_shutdown=[...]).run()
       └─ on_startup hooks fire:
            ├─ broker.connect()  ← consumer builder + admin_client now live
            ├─ for each McpServer referenced by Agent.tools but NOT in self._mcp_servers:
            │    ├─ catalog = await read_catalog_once(
            │    │                broker,
            │    │                topic=f"mcp.{server._name}.catalog",
            │    │                key=server._name.encode("utf-8"),
            │    │                timeout_s=worker.catalog_read_timeout_s,
            │    │             )
            │    └─ server._apply_catalog(catalog)
            │         └─ populates server._discovered for subsequent __iter__
            ├─ register handlers for all nodes (agent + any bridge nodes if mixed)
            └─ broker.start()  ← subscribers go live
```

### 8.2 `_apply_catalog` on `McpServer`

```python
# In McpServer (added):

def _apply_catalog(self, catalog: CatalogMessage) -> None:
    """Populate self._discovered from a deserialized catalog message.

    Called only on agent-worker side (read-only). Does NOT open an MCP
    session — the agent worker never talks to MCP directly.
    """
    self._discovered = [
        McpToolMeta(
            name=t.name,
            description=t.description,
            input_schema=t.input_schema,
            output_schema=t.output_schema,
            annotations=t.annotations,
            meta=t.meta,
        )
        for t in catalog.tools
    ]
```

### 8.3 Failure modes during startup

| Failure | Outcome | Recovery |
|---|---|---|
| Catalog topic doesn't exist | `KafkaError` on consumer start | Operator must run bridge worker first to create topic |
| Catalog topic exists but empty (`end_offset == 0`) | `CatalogNotPublishedError` with actionable message | Operator must boot bridge worker at least once |
| Catalog topic has messages but none with our key | `CatalogNotPublishedError` | Operator must check `McpServer.name` matches the published catalog |
| Catalog message fails to deserialize (schema version mismatch) | `CatalogSchemaVersionError` | Upgrade calfkit on whichever side is older |
| Timeout (60s default) waiting for catalog | `CatalogNotPublishedError` (timeout variant) | Operator must verify Kafka connectivity + bridge availability |
| Network partition during read | aiokafka retries within its own backoff; if outlasts the catalog-read timeout, surfaces as `CatalogNotPublishedError` | Orchestrator restart loop |

All errors include the topic name and server name in their messages so operators have direct breadcrumbs.

### 8.4 Bootstrap timeout — defaults and tunability

Default `catalog_read_timeout_s=60`. Rationale: 60 seconds is long enough for a Kafka cluster to recover from transient broker reassignment but short enough that a misconfiguration is loud within a deployment window.

Configurable per `McpServer` (`mcp(..., catalog_read_timeout_s=120)`) and at the `Worker` level for a default.

---

## 9. Single-process worker behavior

When `Worker(nodes=[gmail, agent])` runs everything in one process:

- The bridge does live discovery and populates `gmail._discovered`.
- The bridge publishes the catalog to Kafka (so *other* workers can read it).
- The agent reads `gmail._discovered` directly in memory — **no catalog-topic read.**
- The publish is still wasted-ish but useful for any future workers.

Implementation: `_collect_agent_only_servers()` (already in the main impl plan §6.1) returns servers referenced by `Agent.tools` that are NOT also in `self._mcp_servers`. Only those servers get a catalog read.

```python
async def _on_startup(self) -> None:
    await self._client._connection.connect()

    # Bridge-mode: discover, publish, register subscribers
    for server in self._mcp_servers:
        await ensure_compacted_topic(
            self._client._connection, f"mcp.{server._name}.catalog",
        )
        await server._discover(ephemeral=False)
        await publish_catalog(self._client._connection, server)
        for meta in server._apply_filters(server._discovered):
            self._mcp_bridges.append(McpBridge(server, meta))

    # Agent-only-mode: read catalog from Kafka
    for server in self._collect_agent_only_servers():
        catalog = await read_catalog_once(
            self._client._connection,
            topic=f"mcp.{server._name}.catalog",
            key=server._name.encode("utf-8"),
            timeout_s=self._catalog_read_timeout_s,
        )
        server._apply_catalog(catalog)

    self._register_handlers_now()
```

---

## 10. Worker patch

**File:** `calfkit/worker/worker.py`. Changes from main impl plan §6 plus the catalog-specific additions:

```python
import asyncio
import logging
from typing import Any

from faststream import FastStream

from calfkit.client import Client
from calfkit.nodes import BaseNodeDef
from calfkit.mcp._server import McpServer
from calfkit.mcp._bridge import McpBridge
from calfkit.mcp._catalog_admin import ensure_compacted_topic
from calfkit.mcp._catalog_writer import publish_catalog
from calfkit.mcp._catalog_reader import read_catalog_once

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        client: Client,
        nodes: list[BaseNodeDef] | None = None,
        max_workers: int = 1,
        group_id: str | None = None,
        extra_publish_kwargs: dict[str, Any] = {},
        extra_subscribe_kwargs: dict[str, Any] = {},
        catalog_read_timeout_s: float = 60.0,
    ):
        self._client = client
        self._max_workers = max_workers
        self._group_id = group_id
        self._extra_publish_kwargs = extra_publish_kwargs
        self._extra_subscribe_kwargs = extra_subscribe_kwargs
        self._catalog_read_timeout_s = catalog_read_timeout_s
        self._prepared = False

        self._mcp_servers: list[McpServer] = []
        self._nodes: list[BaseNodeDef] = []
        for node in (nodes or []):
            if isinstance(node, McpServer):
                self._mcp_servers.append(node)
            else:
                self._nodes.append(node)
        self._mcp_bridges: list[McpBridge] = []

    async def _on_startup(self) -> None:
        broker = self._client._connection
        await broker.connect()

        spawned: list[McpServer] = []
        try:
            # Bridge-mode servers: discover, publish catalog, register bridges
            for server in self._mcp_servers:
                await ensure_compacted_topic(
                    broker, f"mcp.{server._name}.catalog",
                )
                await server._discover(ephemeral=False)
                spawned.append(server)
                await publish_catalog(broker, server)
                for meta in server._apply_filters(server._discovered):
                    self._mcp_bridges.append(McpBridge(server, meta))

            # Agent-only-mode servers: read catalog from Kafka
            for server in self._collect_agent_only_servers():
                catalog = await read_catalog_once(
                    broker,
                    topic=f"mcp.{server._name}.catalog",
                    key=server._name.encode("utf-8"),
                    timeout_s=self._catalog_read_timeout_s,
                )
                server._apply_catalog(catalog)

            self._register_handlers_now()
        except BaseException:
            # SIGTERM during boot, exception, anything — clean up MCP sessions
            for s in spawned:
                if s._session is not None:
                    try:
                        await s._session.aclose()
                    except Exception:
                        pass
                    s._session = None
            raise

    async def _on_shutdown(self) -> None:
        for server in self._mcp_servers:
            if server._session is not None:
                await server._session.aclose()
                server._session = None

    async def run(self, **extra_run_args: Any) -> None:
        logger.info(
            "worker starting: %d nodes, %d MCP servers",
            len(self._nodes), len(self._mcp_servers),
        )
        app = FastStream(
            self._client._connection,
            on_startup=[self._on_startup],
            on_shutdown=[self._on_shutdown],
        )
        await app.run(**extra_run_args)

    # ... existing register_handlers_now, _collect_agent_only_servers, etc.
```

Total Worker patch: **~50 LOC net** added to existing `worker.py`.

---

## 11. Topic configuration deep dive

### 11.1 Compaction config rationale

| Config | Value | Why |
|---|---|---|
| `cleanup.policy` | `compact` | Retain only the latest message per key. Catalog is "current state," not "history." |
| `min.cleanable.dirty.ratio` | `0.01` | Run the log cleaner aggressively. Catalog topic has very few messages and many overwrites; default 0.5 would let stale entries linger. |
| `segment.ms` | `60000` (60s) | Roll segments quickly so the log cleaner can compact frequently. Default is 7 days, which would mean stale catalogs persist for a week before compaction. |
| `delete.retention.ms` | `100` | Tombstone retention; if a server is decommissioned and a tombstone is published (v2), it disappears fast. |
| `partitions` | `1` | One catalog per server. Multiple partitions add no value (single key) and complicate end-of-partition detection. |
| `replication.factor` | inherited from cluster default | Matches other calfkit topics. |

### 11.2 Operational considerations

- The catalog topic is created on the bridge's first boot. Agent workers cannot create it (they don't have admin permissions in some clusters).
- Operators should ensure their Kafka cluster permits `auto.create.topics.enable=false` deployments by granting `Create` ACL on `mcp.*.catalog` topic prefix to bridge workers' service accounts.
- For Confluent Cloud / managed Kafka, `cleanup.policy=compact` is universally supported; some niche clusters may not allow it (audit at integration test time).

### 11.3 Migration / upgrade

For v1.1+ schema changes:
- v1.1 bridge publishes both `schema_version=1` (key=server) AND `schema_version=2` (key=`<server>.v2`) catalogs during a deprecation window.
- v1.1 agents read `<server>.v2` first, fall back to `<server>`.
- v1.2 drops v1 publication once all agents are upgraded.

For v1 → v1.1 schema changes only — out of v1 implementation scope but documented as the migration path.

---

## 12. Testing strategy

### 12.1 Unit tests

| Test | Module | Tool |
|---|---|---|
| `CatalogMessage` serialization / deserialization round-trip | `_catalog_message` | pytest |
| `CatalogMessage` schema-version mismatch raises | `_catalog_message` | pytest |
| `publish_catalog` constructs the correct dataclass | `_catalog_writer` | pytest with mock broker |
| `read_catalog_once` handles timeout correctly | `_catalog_reader` | pytest with mock aiokafka consumer |
| `read_catalog_once` returns latest message for key | `_catalog_reader` | pytest with mock aiokafka consumer |
| `read_catalog_once` raises on empty topic | `_catalog_reader` | pytest with mock aiokafka consumer |
| `_fs_internals` accessors work against FastStream stub | `_fs_internals` | pytest |

### 12.2 Integration tests (TestKafkaBroker)

| Test | What it covers |
|---|---|
| Bridge worker publishes catalog on startup | Confirms `publish_catalog` runs in `on_startup` order |
| Agent worker reads published catalog | End-to-end through `TestKafkaBroker` (which DOES support `publish` + `subscriber` interactions) |
| Single-process worker bypasses catalog read | Confirms `_collect_agent_only_servers` returns empty for in-process bridge |
| `BaseException` during startup cleans up MCP sessions | Cancellation safety |

### 12.3 Integration tests (real Kafka, separate CI lane)

| Test | What it covers |
|---|---|
| `ensure_compacted_topic` creates topic with correct config | `admin.describe_configs` returns expected `cleanup.policy=compact` |
| `read_catalog_once` works with real `seek_to_beginning` / `end_offsets` | TestKafkaBroker doesn't simulate these; real Kafka required |
| Catalog topic compaction actually compacts | Publish 100 catalogs in succession, wait for compaction window, verify only 1 remains via `admin.describe_log_dirs` |
| Multi-replica bridge → single catalog after compaction | Spin up 3 bridges, all publish, verify last-write-wins |
| Agent worker boots before bridge → fails with actionable error | Lifecycle ordering |
| Agent worker boots after bridge → succeeds | Happy path |
| Bridge crashes mid-publish → next boot republishes successfully | Idempotency |
| Schema version mismatch surfaces clearly | Cross-version compat |

### 12.4 Why we need a real-Kafka lane

`TestKafkaBroker` in FastStream simulates message routing but not:
- Log compaction
- `seek_to_beginning` / `end_offsets` / `position` semantics
- Admin client operations (`create_topics`, `describe_configs`)
- Aiokafka-specific consumer assignment behavior

A real-Kafka Docker compose fixture (Confluent's `cp-kafka` or Redpanda) is required for the `_catalog_admin.py` and `_catalog_reader.py` integration tests. CI lane: a separate `kafka-integration` job that runs in ~2 minutes against a single-broker container.

---

## 13. Risk register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| FastStream `broker.config.*` rename in 0.7 | Medium | Medium | All access via `_fs_internals.py`; one-file refactor on upgrade. Pin `<0.7`. |
| `broker.connect()` idempotency regression | Low | High | Test with double-connect at integration time. Mirror calfkit's existing pattern from `client/base.py:152`. |
| Kafka cluster forbids `cleanup.policy=compact` | Low | High | Document the required ACL / cluster config in operator docs. Detect at startup and produce a clear error. |
| `broker.config.builder(topic, ...)` auto-subscribe varies by aiokafka version | Medium | Medium | Detected at integration test time. Fallback path uses explicit `TopicPartition` assignment instead. |
| Bridge publishes before its subscribers go live; agent reads, bridge crashes immediately | Low | Low | Catalog is durable in Kafka. Agent boots; bridge restarts. Self-healing. |
| Multi-replica bridges produce different catalogs (e.g. MCP server returns slightly different tool lists at different times) | Low | Medium | Last-write-wins under compaction. Operationally: document that bridges should be deployed against a single MCP server version. Lint: `CatalogMessage` hash logged on each publish so operators can detect drift. |
| First-deploy bootstrap timeout in tight Kubernetes deployment | Medium | Low | Default 60s timeout is conservative. Configurable. Orchestrator restart loop handles longer initialization. |
| Catalog message size exceeds Kafka `max.message.bytes` (default 1MB) | Low | Medium | A 100-tool catalog with rich JSON schemas is ~100KB. 10x headroom against default. Document max as 500 tools per server. |

---

## 14. Phased implementation

### Phase 1 — Catalog message and admin (1 day)
- `_catalog_message.py`: `CatalogMessage`, `CatalogToolSpec`, `PublisherInfo`, `McpServerInfo`. Pure data + serde.
- `_catalog_admin.py`: `ensure_compacted_topic`. Real-Kafka integration test.
- `_fs_internals.py`: containment shim.

### Phase 2 — Writer (1 day)
- `_catalog_writer.py`: `publish_catalog`. Unit test with mock broker.

### Phase 3 — Reader (2 days)
- `_catalog_reader.py`: `read_catalog_once`. Unit tests with mocked aiokafka; real-Kafka integration test for end-of-partition semantics.
- Handle aiokafka version compatibility for positional `topic` arg.

### Phase 4 — Worker integration (1 day)
- Patch `Worker.__init__`, add `_on_startup` / `_on_shutdown`, swap to `FastStream(..., on_startup=[...], on_shutdown=[...])`.
- Cancellation safety with `BaseException` cleanup.
- `TestKafkaBroker` end-to-end test.

### Phase 5 — Documentation (1 day)
- Operator docs: required Kafka ACLs, compaction config, topic prefix conventions.
- Troubleshooting: "`CatalogNotPublishedError` — what to check."
- Update design doc §7.3 to reference this sub-plan.

**Total: ~6 working days** for the catalog mechanism alone, isolated from the rest of the MCP adaptor work.

---

## 15. Open implementation questions

1. **Catalog topic auto-create permissions.** If the Kafka cluster forbids broker-side auto-creation but also forbids client-side topic creation for non-admin service accounts, the bridge can't create the catalog topic. Should `ensure_compacted_topic` retry-with-warn or hard-fail? Proposed: hard-fail at startup with a clear error pointing at the ACL config.

2. **`min.cleanable.dirty.ratio=0.01` aggressiveness.** This causes the log cleaner to run nearly every segment roll. For low-volume catalog topics this is fine, but on shared Kafka clusters with many catalog topics, this could marginally increase cleaner load. Acceptable? Alternative: `0.1` is the standard "aggressive" default.

3. **Catalog topic creation in `ensure_compacted_topic` — verify or assume existing config?** v1 silently passes on `TopicAlreadyExistsError`. Should we also `describe_configs` and warn if the existing topic isn't actually compacted (e.g. someone created it manually with `delete` policy)?

4. **Bootstrap timeout default — 60s vs longer?** 60s is conservative for healthy clusters but tight for first-deploy ordering issues. Kubernetes `initialDelaySeconds` / `failureThreshold` would absorb longer waits; whatever we pick, document the orchestrator-side tuning.

5. **Multi-replica bridge concurrent publish — assert content equality?** When 3 replicas publish identical catalogs (assumed-identical), is there value in adding a content hash header and warning if two replicas publish different content? Or accept "last write wins" silently?

6. **AsyncAPI inclusion of catalog topic.** Should the catalog topic appear in calfkit's AsyncAPI export, even though it doesn't go through `@broker.publisher`? Would require a custom AsyncAPI entry; v1 says no.

---

## 16. References

### Internal
- [`docs/mcp-adaptor-design.md`](./mcp-adaptor-design.md) §7 (wire model)
- [`docs/mcp-adaptor-implementation-plan.md`](./mcp-adaptor-implementation-plan.md) §6 (Worker lifecycle integration)
- `calfkit/worker/worker.py` (target of the patch)
- `calfkit/client/base.py:152` (existing pattern for broker-state poking)

### External
- [FastStream Kafka broker source](https://github.com/ag2ai/faststream/blob/main/faststream/kafka/broker/broker.py) (v0.6.6 tag)
- [FastStream lifecycle hooks](https://faststream.airt.ai/latest/getting-started/lifespan/)
- [aiokafka admin client](https://aiokafka.readthedocs.io/en/stable/admin_client.html)
- [Kafka log compaction concept](https://kafka.apache.org/documentation/#compaction)
