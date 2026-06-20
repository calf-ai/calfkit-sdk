# Topic Provisioning (EXPERIMENTAL, opt-in)

> **Status: EXPERIMENTAL / opt-in.** Topic provisioning is **off by default**.
> Everything below is a *development convenience* for getting a local or CI
> broker into the right shape. It is **not** a production provisioning story —
> see [Dev-safe / review-for-prod](#dev-safe--review-for-prod) before relying on
> it outside local/CI use.

> **Runnable example:** [`examples/topic_provisioning.py`](../examples/topic_provisioning.py)
> exercises this surface end-to-end — `ProvisioningConfig` knobs,
> `topics_for_nodes` inspection, programmatic `TopicProvisioner` + `ProvisionReport`,
> and idempotency.

## 1. Problem

A calfkit deployment is a mesh of nodes that publish to and subscribe from named
Kafka topics: agent inboxes (`subscribe_topics`), framework-private return
inboxes (`*.private.return`, issue #141), agent `publish_topic`s, and per-tool
input topics. For any of this traffic to flow, the topics have to **exist**.

Today that "just works" locally only because the
[calfkit-broker](https://github.com/calf-ai/calfkit-broker) dev image ships with
broker-side `auto.create.topics.enable=true`. That is a property of the *broker*,
not of calfkit — a hardened broker (and most managed/production clusters) has it
**disabled**, at which point producers and consumers silently stall on a missing
topic with no obvious error.

We want an explicit, opt-in way for calfkit itself to create the topics a set of
nodes reference, so a developer can stand up a broker that does *not* auto-create
topics without hand-rolling `kafka-topics.sh` invocations — while making the
production caveats impossible to miss.

## 2. Non-goals

- **Not** a replacement for ops-governed provisioning (Terraform, GitOps, a
  platform team) with naming policy, retention review, and partition planning.
- **Not** durable by default (`replication_factor=1`).
- **Not** an ACL manager. No authorization is created; if the broker enforces
  authz, created topics may still be unusable by the right principals.
- **Not** on by default, and never silently enabled.

## 3. Public surface

The full surface lives in `calfkit.provisioning`. Only `ProvisioningConfig` is
re-exported at the top level (`from calfkit import ProvisioningConfig`); the rest
is reached via `from calfkit.provisioning import ...`.

```python
from calfkit.provisioning import (
    ProvisioningConfig,        # opt-in config (the only thing most users touch)
    TopicProvisioner,          # the best-effort admin-client creator
    ProvisionReport,           # outcome: created / existing / unauthorized
    TopicProvisioningError,    # non-retriable failure or timeout
    topics_for_nodes,          # resolve the topic set a list of nodes references
)
```

### 3.1 `ProvisioningConfig`

```python
@dataclass
class ProvisioningConfig:
    enabled: bool = False                            # master switch (default: off)
    num_partitions: int = 1
    replication_factor: int = 1                      # NOT durable; see caveats
    topic_configs: dict[str, str] = {}               # data topics only
    create_timeout_ms: int = 30000
```

- `enabled` — the master switch. When `False` (or when no config is supplied at
  all), provisioning is a **pure no-op**: no admin client is ever constructed and
  the `aiokafka.admin` dependency is never imported.
- `num_partitions` / `replication_factor` — applied to every newly-created
  topic. `replication_factor=1` is the default and is **not durable**.
- `topic_configs` — per-topic `config` overrides (e.g.
  `{"retention.ms": "604800000"}`) applied to **data topics only**. Framework
  topics (reply topics, `*.private.return` inboxes) are deliberately excluded:
  overrides like `cleanup.policy=compact` or short retention are semantically
  wrong for correlation-keyed request/reply traffic.
- `create_timeout_ms` — overall budget for the whole provisioning operation
  (connect + create + retry + inspect), enforced by a single `asyncio.wait_for`.
  On expiry a `TopicProvisioningError` is raised naming the still-pending topics.

### 3.2 `topics_for_nodes(nodes) -> list[str]`

Computes the de-duplicated, first-seen-ordered set of topics a list of nodes
references. For each node:

- every entry of `subscribe_topics` (the node's public inbox(es)),
- the node's framework-private `_return_topic` (tool `ReturnCall` / built-in
  `TailCall` inbox — issue #141), and
- `publish_topic` when set.

**Agent nodes** additionally contribute *all* of each tool's `subscribe_topics`
(every declared inbox is provisioned). At runtime the agent publishes tool
`Call` envelopes onto `tools_registry[name].subscribe_topics[0]`, but the full
`subscribe_topics` list for each tool is created so no declared inbox is left
missing. Agent nodes are detected *structurally* (they expose a `tools`
collection) so `calfkit.provisioning` stays decoupled from `calfkit.nodes`.

### 3.3 `ProvisionReport`

Returned by a successful provisioning pass; the basis for logging / the CLI
summary.

| Field          | Meaning                                                         | Kafka code |
| -------------- | --------------------------------------------------------------- | ---------- |
| `created`      | Did not exist; created.                                         | 0          |
| `existing`     | Already existed; idempotent no-op.                              | 36         |
| `unauthorized` | Not authorized to create; **skipped with a loud warning**.      | 29         |

Unauthorized topics are *not* created and *not* fatal: the producers/consumers on
them will silently stall unless they are pre-created out-of-band, so the warning
is the only signal.

### 3.4 `TopicProvisioningError`

Raised on a **non-retriable** per-topic error, or when `create_timeout_ms`
elapses with topics still pending. Carries `.topic` and `.code` (the Kafka error
code, or `None` for a timeout) so callers can log the precise cause.

## 4. How provisioning is triggered

Provisioning is wired into three entry points. All three are gated on
`ProvisioningConfig.enabled`; none does any work when disabled.

### 4.1 Worker startup (eager, the common path)

`ProvisioningConfig` rides on the **client**:

```python
from calfkit import Client, Worker, ProvisioningConfig

client = Client.connect(
    "localhost:9092",
    provisioning=ProvisioningConfig(enabled=True),  # opt in
)
worker = Worker(client, nodes=[agent, tool])
await worker.run()
```

When enabled, the worker's FastStream `on_startup` hook runs `provision_topics()`
**after** `register_handlers()` and **before** `broker.start()` (so every
subscriber's inbox exists before consumption begins). The topic set is `topics_for_nodes()` over exactly the nodes
that were registered; each node's `_return_topic` is passed as a framework topic
so `topic_configs` is never applied to it. A provisioning failure aborts startup.

For a manual `register_handlers()`-without-`run()` deployment (which bypasses the
worker's startup hook), run an explicit provisioning pass out-of-band — the CLI
(`calfkit topics provision`, §4.3) or the `calfkit.provisioning.provision_topics()`
module function (§3) — both idempotent.

### 4.2 Client reply topic (lazy, once)

A `Client` provisions **its own reply topic** the first time the client publishes — any of `send`/`start`/`execute` (just
before the broker connects), and at most once for the lifetime of the client. The
reply topic is a framework inbox, so it is passed in `framework_topics` to keep
user `topic_configs` off it. This is gated on the same
`client.provisioning.enabled`.

The client also exposes read-only properties the worker reads to reach the same
broker: `client.provisioning` and `client.server_urls`. (The broker credentials
captured at `Client.connect(...)` are forwarded to the provisioner internally —
see §5.3.)

### 4.3 CLI: `calfkit topics provision`

A static, out-of-band path for CI/setup scripts. It does **not** require a running
worker — it imports node definitions, resolves their topics, and creates them.

```shell
# Resolve + create every topic the nodes reference
calfkit topics provision \
    --nodes myapp.workers:all_nodes \
    --bootstrap-servers localhost:9092

# Multiple node sources, custom partition/replication
calfkit topics provision \
    --nodes myapp.workers:agent \
    --nodes myapp.workers:tool \
    --partitions 3 --replication-factor 3

# Resolve + print the topic set, create nothing
calfkit topics provision --nodes myapp.workers:all_nodes --dry-run
```

- `--nodes module:attr` (repeatable) — each `attr` may be a single node or an
  iterable of nodes; results concatenate in flag order.
- `--bootstrap-servers` (default `localhost`), `--partitions` (default `1`),
  `--replication-factor` (default `1`, **not durable**), `--timeout-ms`
  (default `30000`), `--dry-run`.
- Exit codes: `0` success / dry-run; `2` error (node resolution failed, or a
  topic could not be provisioned).

The CLI requires the `cli` optional extra (typer): `pip install calfkit[cli]`.

## 5. Behavior details

### 5.1 Error classification

`create_topics` returns a per-topic `topic_errors` list of `(topic, code, ...)`.
The provisioner classifies each code:

- `0` (NoError) -> **created**.
- `36` (TopicAlreadyExists) -> **existing** (idempotent success).
- `29` (TopicAuthorizationFailed) -> **unauthorized** (warn + continue).
- Any other code -> consult `aiokafka.errors.for_code(code)`: if the error is
  `retriable`, the topic is re-issued (bounded `0.5s` backoff, all within the
  overall `create_timeout_ms` budget); otherwise a `TopicProvisioningError` is
  raised naming the topic and code.

### 5.2 Timeout

The whole flow — admin `start()`, `create_topics`, retriable re-issues, response
inspection — is wrapped in one `asyncio.wait_for(create_timeout_ms)`. On expiry,
the still-pending topics are surfaced in the `TopicProvisioningError` message.

### 5.3 Security / credentials

The provisioner builds its own `AIOKafkaAdminClient`. It accepts a FastStream
`security=` object (parsed via `faststream.kafka.security.parse_security`) plus
raw aiokafka kwargs (`sasl_*`, `ssl_context`, `security_protocol`). The
`security=` object wins on overlapping keys, **except** that a conflicting
`security_protocol` supplied in both places raises rather than silently picking
one. The client captures these from `Client.connect(...)` and forwards them so
provisioning hits the same broker with the same credentials.

### 5.4 Idempotency / no-op guarantees

- Disabled config -> no admin client constructed, `aiokafka.admin` never imported.
- Empty topic set -> returns an empty `ProvisionReport` without contacting Kafka.
- The client reply topic is provisioned at most once per client.
- An explicit `calfkit.provisioning.provision_topics()` pass (or the CLI) is safe to repeat and is idempotent.

## 6. Dev-safe / review-for-prod

**This feature is a development convenience. Review every field before relying on
it outside local/CI use.**

- **`replication_factor=1` is the default and is NOT durable.** A single broker
  failure loses the topic and its data. Real deployments need
  `replication_factor >= 3`.
- **No ACLs are created.** If the broker enforces authorization, topics created
  here may not be readable/writable by the right principals unless ACLs are
  provisioned out-of-band. Authorization failures are warned-and-skipped, not
  fatal — the affected traffic silently stalls.
- **In production, topic creation is almost always ops-governed** (Terraform,
  GitOps, a platform team) with naming policy, retention, and partition-count
  review. Enabling provisioning in production **bypasses that governance.**
- **Off by default, always.** `ProvisioningConfig(enabled=True)` (or the CLI,
  which sets `enabled=True` for you) is the only way to turn it on.

The recommendation is: enable it for local development and CI against a broker you
control; leave it off in production and let your platform's ops-governed pipeline
own topic creation.

## 7. Rejected alternatives

- **`Worker(auto_create_topics=True)`** (the shape floated in the v1 design's
  open questions). Rejected: it puts the switch on the worker, but provisioning
  needs the broker URL and credentials that already live on the `Client`, and the
  client's own reply topic needs the same switch. Putting one
  `ProvisioningConfig` on `Client.connect(...)` keeps the credentials and the
  switch in one place and lets both the worker and the client read it. The actual
  API is `Client.connect(provisioning=ProvisioningConfig(...))`, defaulting
  **off**.
- **On-by-default / relying on broker `auto.create.topics.enable`.** Rejected:
  it is a broker property calfkit can't guarantee, it masks typos as silently
  stalled traffic, and it produces single-partition, broker-default-replicated
  topics with no review — the worst of both worlds in production.
