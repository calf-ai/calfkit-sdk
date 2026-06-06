# Startup topic provisioning — design & implementation plan

**Issue:** [#180](https://github.com/calf-ai/calfkit-sdk/issues/180) (with context from the closed [#174](https://github.com/calf-ai/calfkit-sdk/issues/174))
**Status:** Proposed — design converged, pending implementation (TDD)
**Branch:** `fix/client-reply-topic-provisioning` (worktree off `origin/main` @ 0.5.3)

---

## 1. TL;DR

`Client.connect()` registers a reply‑dispatcher **subscriber** on the client's reply
topic at connect time, but that topic is only created **lazily on the first
`invoke`**. Any path that starts the broker without invoking first —
`await client.broker.start()`, `Worker.run()/start()/async with`, `calfkit run` —
activates the reply consumer against a topic that does not exist. On a broker that
does **not** auto‑create topics (Tansu, hardened Kafka/Redpanda), the consumer loops
`Topic … not found in cluster metadata` forever and `broker.start()` **never returns**
— a silent infinite hang, even with `ProvisioningConfig(enabled=True)`.

The fix moves "create the topics this process uses" out of the request path and into
the **one place every start path funnels through** — the broker's `start()`, in the
window after `connect()` (admin client available) and before subscribers consume.
calfkit keeps owning the *policy + topic computation*; it reuses **FastStream's
already‑managed admin client** for execution instead of building a second one.

**Scope is deliberately narrow.** Provisioning only does work when it is **enabled**
(`ProvisioningConfig(enabled=True)`). The default (disabled) path is **untouched** —
on Kafka/Redpanda, broker‑side `auto.create.topics.enable` handles topic creation
exactly as today. We do **not** add an always‑on "verify topic exists" preflight to
the default path (it would be redundant on auto‑creating brokers and would change
default behaviour). The only fail‑fast is in the **enabled** path: if provisioning was
requested but a topic could not be created (e.g. ACL‑denied), we raise a clear error
from the create report instead of warning and then stalling.

---

## 2. Problem & root cause

### Root cause (precise)

1. `Client.connect()` unconditionally registers a FastStream subscriber on the reply
   topic — `client/base.py` → `reply_dispatcher.py` (`@broker.subscriber(reply_topic, …)`).
2. The reply topic is provisioned only inside `_publish_call`'s connect‑guard
   (`client/base.py`), reachable **only** via `_invoke`/`_emit`.
3. `KafkaBroker.start()` = `await self.connect()` then `await super().start()`;
   `super().start()` starts **every** registered subscriber — including the reply
   consumer. On a no‑auto‑create broker that consumer's subscription to a missing
   topic loops metadata fetches forever, so `start()` never returns.
4. The existing provisioning never closes the gap before `start()`:
   - lazy first‑invoke provisioning doesn't run before a direct `start()`, and **never**
     runs for a "pure target" client that never invokes;
   - `Worker.provision_topics()` provisions only `topics_for_nodes(...)`, which by
     construction **excludes** the client reply topic.

**The violated invariant:** *a subscriber's topic must exist before the broker starts
consuming it.* The reply consumer is the one subscriber whose topic creation is tied
to "first invoke" rather than to startup.

### Confirmed: it is not only the bare‑broker path

Empirically, `Worker.run()` **also hangs** (the worker provisions its node topics, those
consumers get assigned, then the client's reply consumer — on the same broker — loops
`not found`). The original issue's belief that `Worker.run()` was immune is incorrect;
the reporter most likely tested the worker against an auto‑creating broker. The fix
must therefore cover **all** start surfaces, not just bare `broker.start()`.

---

## 3. What we verified

### 3.1 Reproduction (live, against Tansu 0.6.0 in‑memory, no auto‑create)

| Scenario | calfkit today (**aiokafka**) | FastStream **confluent** auto‑create |
|---|---|---|
| **#180** — subscribe to reply topic (missing) | ❌ hangs forever | ✅ admin‑creates → starts clean |
| **#174‑subscribe** — Worker subscribes to missing node topic | ❌ hangs | ✅ admin‑creates → starts clean |
| **#174‑publish** — publish to a dynamic missing topic | ❌ hangs (`not found in cluster metadata`) | ❌ fails (`unknown topic or partition`) |

Control: pre‑creating the reply topic via the admin `CreateTopics` API before
`broker.start()` lets the consumer join its group and `start()` returns cleanly — so
the gap is specifically *topic creation timing*, not protocol compatibility.

### 3.2 Why we do **not** delete calfkit provisioning in favour of FastStream's

FastStream *does* do framework‑owned topic creation — but only on the **confluent**
broker (`allow_auto_create_topics` → `admin_client.create_topics(topics_to_create)`
pre‑subscribe, `confluent/helpers/client.py`). calfkit runs on the **aiokafka**
`KafkaBroker`, which has **no** auto‑create. Adopting FastStream's mechanism is a
non‑starter for three independent reasons, each empirically grounded above:

1. **Wrong transport.** It would require migrating the whole SDK aiokafka →
   confluent‑kafka (new C dependency, different consumer/producer semantics — confluent
   doesn't even hang on a missing topic, it silently proceeds).
2. **Subscriber‑only.** `topics_to_create = self.topics` — consumer subscriptions only.
   It misses publisher topics (`node.publish_topic`) and agent tool‑input topics, and
   empirically fails the **#174 publish** path. calfkit would still need its own
   provisioning.
3. **No policy.** Hardcoded `num_partitions=1, replication_factor=1`, no framework‑inbox
   tagging for `topic_configs`.

What the confluent code *does* settle: the **pattern** (admin `create_topics`
pre‑subscribe, best‑effort, warn‑on‑failure, off the framework's admin client) is
correct and framework‑blessed. calfkit follows that pattern, **centralized** at broker
start over a richer node‑graph‑derived topic set, rather than per‑consumer over
subscriber topics only.

### 3.3 Load‑bearing library facts (verified against installed FastStream 0.7.1)

- **FastStream's `KafkaBroker` already manages an `AIOKafkaAdminClient`**: built on
  `connect()` from the broker's own connection kwargs (inherits bootstrap + SASL/SSL),
  exposed as `broker.config.broker_config.admin_client` (valid **after** `connect()`),
  closed on `disconnect()`, skipped under `consumer_only=True`.
  (`faststream/kafka/configs/broker.py`)
- **`KafkaBroker.start()` = `connect()` then `super().start()`**; `super().start()`
  iterates `self.subscribers` and starts them (the hang point). There is a clean window
  between connect and subscriber‑start where the admin client exists and the cluster is
  reachable. There is **no** broker‑level startup hook in 0.7.1 (hooks are app‑level
  only, and a bare `broker.start()` has no app).
- **`TestKafkaBroker` patches `broker.start`** with a fake, so the start‑hook override is
  bypassed under test → **zero blast radius** on the existing `TestKafkaBroker` suite;
  the hook is unit‑tested by calling it directly with a faked admin client.

---

## 4. Why fail‑fast is scoped to the enabled path (the design call)

The provisioning flag and the broker type are **correlated**, which is why an always‑on
preflight is the wrong default:

- **Provisioning disabled** (the default) → you are almost certainly on Kafka/Redpanda
  with `auto.create.topics.enable=true` → the broker creates topics on subscribe →
  **no hang**. An always‑on `describe`/preflight here is *redundant* (on Redpanda the
  describe would merely trigger the same auto‑create the subscribe would have) and would
  add an admin round‑trip + a describe‑permission expectation to a path that works today.
- **Provisioning enabled** → you turned it on *because* you are on a no‑auto‑create
  broker → provisioning creates the topics → **no hang**.

The only quadrant where a missing topic still bites is **disabled provisioning + a
no‑auto‑create broker** — a misconfiguration. We deliberately **do not** add machinery
to the default path to catch it; the guidance is "enable provisioning for
non‑auto‑creating brokers." (Hardened production Kafka that disables auto‑create is the
notable case here; such deployments should enable provisioning or pre‑create calfkit's
framework topics. Documented as a known limitation, not silently handled.)

The one fail‑fast we keep is **free and opt‑in by construction**: when provisioning is
**enabled** but the `create_topics` report shows a declared topic was **not** created
(e.g. `TOPIC_AUTHORIZATION_FAILED`), we raise instead of today's warn‑then‑stall. It
reads the report we already have — no extra round‑trip — and only fires when
provisioning was explicitly requested.

---

## 5. Ownership model (the converged pattern)

Two independent consults (event‑driven‑architecture + Python‑SDK‑DX) plus the empirical
work converged on this split:

| Concern | Owner | Why |
|---|---|---|
| Provisioning **policy** (`ProvisioningConfig`: enable, partitions, rf, configs, timeouts) | **calfkit `Client`/`Worker` surface** | An application/deployment decision, configured where the user already configures connection + nodes. |
| **Topic‑set computation** (`topics_for_nodes`, framework‑inbox tagging) | **calfkit `provisioning`** | Needs node‑graph structure (subscribe/publish/return/tool topics) that transport layers can't derive. |
| **Execution mechanism** (the admin connection) | **Reused from FastStream** (`broker.config.broker_config.admin_client`) | FastStream already owns an admin‑client lifecycle (security inherited). A second one is accidental complexity. |
| **Start‑time trigger** (run before consume, all paths) | **A thin, generic broker hook** | The only universal choke point; carries **zero** provisioning knowledge. |

**Non‑negotiables that fell out of the discussion:**
- The broker must **not** own provisioning policy. The subclass is a generic lifecycle
  seam only.
- We do **not** build a second admin client or re‑plumb security.
- `client.broker.start()` (the issue's literal desired outcome) must keep working → a
  start‑time hook is required.
- The **default (disabled) path is not modified.**

---

## 6. Target architecture

```
                 ┌──────────────────────────────────────────────────────────┐
   declares      │  StartupTopicEnsurer  (calfkit/provisioning/ensurer.py)   │
   reply topic   │   • holds: ProvisioningConfig                             │
 Client.connect ─┼─▶  • registry: {topic -> is_framework}                    │
                 │   • run(broker):                                          │
   declares node │       if not config.enabled: return        # default path: no-op
   topics in     │       admin = broker.config.broker_config.admin_client    │
 Worker.on_start ┼─▶    report = create declared set (one pass)              │
                 │       if any declared topic uncreated: raise MissingTopicsError
                 └───────────────────────▲──────────────────────────────────┘
                                         │ pre_start(broker)
   ┌─────────────────────────────────────┴───────────────┐
   │ _PreStartHookBroker(KafkaBroker)   (client/_broker.py)│   generic; no provisioning knowledge
   │   start(): connect() → pre_start(self) → super().start()
   │   stop():  super().stop() → reset one-shot
   └───────────────────────────────────────────────────────┘
                                         │ uses (enabled only)
   ┌─────────────────────────────────────┴───────────────┐
   │ stateless executor  (calfkit/provisioning/provisioner.py)
   │   provision_topics(admin, topics, *, framework_topics, config) -> ProvisionReport
   └───────────────────────────────────────────────────────┘
```

**Call flow (every start path):**

```
client.broker.start()  ─┐
Worker.run/start()/with ─┼─▶ _PreStartHookBroker.start()
calfkit run ────────────┘        ├─ await self.connect()         # FastStream builds its admin client
                                 ├─ await pre_start(self)         # ensurer.run(self)
                                 │     ├─ if disabled: return     # DEFAULT PATH — no-op, no admin call
                                 │     ├─ admin = self.config.broker_config.admin_client
                                 │     ├─ provision declared set  (reuse admin client)
                                 │     └─ raise MissingTopicsError if any declared topic uncreated
                                 └─ await super().start()         # subscribers consume — topics now exist
```

---

## 7. Mechanism in detail

### 7.1 The generic broker hook — `_PreStartHookBroker`

```python
# calfkit/client/_broker.py  (new, ~25 lines)
class _PreStartHookBroker(KafkaBroker):
    """KafkaBroker that runs a one-shot async hook after connect() and before
    subscribers consume. Generic — knows nothing about provisioning."""
    def __init__(self, *args, pre_start=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._pre_start = pre_start            # Callable[[KafkaBroker], Awaitable[None]] | None
        self._pre_start_done = False

    async def start(self) -> None:
        await self.connect()                   # idempotent; ensures the admin client exists
        if self._pre_start is not None and not self._pre_start_done:
            self._pre_start_done = True        # set before await: one-shot
            await self._pre_start(self)
        await super().start()                  # KafkaBroker.start: connect (no-op) + subscribers

    async def stop(self, *exc: object) -> None:
        await super().stop(*exc)
        self._pre_start_done = False           # re-run on a fresh start (topic may have been deleted)
```

The hook is generic and always wired, but it is *cheap* on the default path: when
provisioning is disabled the callback returns immediately (no admin call, no behaviour
change). The only added cost on the disabled path is a single idempotent `connect()`
that `KafkaBroker.start()` already performs.

### 7.2 The orchestrator — `StartupTopicEnsurer`

Owns the *policy + declared‑topic registry*; executes against the broker's admin client,
**only when enabled**.

```python
# calfkit/provisioning/ensurer.py  (new)
class StartupTopicEnsurer:
    def __init__(self, *, config: ProvisioningConfig):
        self._config = config
        self._declared: dict[str, bool] = {}              # topic -> is_framework

    def declare(self, topics: Iterable[str], *, framework: bool = False) -> None:
        for t in topics:
            self._declared[t] = self._declared.get(t, False) or framework

    async def run(self, broker: KafkaBroker) -> None:
        if not self._config.enabled or not self._declared:
            return                                        # DEFAULT PATH: untouched
        admin = _admin_client_or_none(broker)             # None under consumer_only / not connected
        if admin is None:
            logger.info("provisioning enabled but no admin client (consumer_only?); skipping")
            return
        framework = {t for t, fw in self._declared.items() if fw}
        report = await provision_topics(admin, list(self._declared),
                                        framework_topics=framework, config=self._config)
        _log_report(report)                               # created / existing / unauthorized summary
        existing = set(report.created) | set(report.existing)
        uncreated = [t for t in self._declared if t not in existing]
        if uncreated:
            raise MissingTopicsError(uncreated)           # enabled but couldn't create -> fail loud
```

Notes:
- **No `server_urls` / `security_kwargs`** — the admin client already carries them.
- **No `describe`/verify** on the default path. The `uncreated` check is derived from the
  `create_topics` report we already have (zero extra round‑trips) and only runs when
  provisioning is enabled.
- **`consumer_only` / unavailable admin** → skip gracefully (calfkit doesn't set
  `consumer_only` today, but keeps the read‑only‑ACL story honest).
- Declarations merge: `topics_for_nodes` lists return inboxes as data; the framework
  declaration upgrades them to `framework=True` so `topic_configs` are never applied to
  correlation‑keyed inboxes.

### 7.3 The stateless executor — `provisioner.py`

`TopicProvisioner` is demoted from "owns an admin client + security merge" to a pure
function that receives an already‑started admin client:

```python
async def provision_topics(admin, topics, *, framework_topics, config) -> ProvisionReport
```

Keeps the existing classify logic (code 0 created / 36 existing / 29 unauthorized /
retriable → retry / else raise), `NewTopic` shaping from `config`, the `framework_topics`
carve‑out, and `ProvisionReport`. It no longer constructs or closes a client (FastStream
owns that lifecycle), so `_make_admin_client`, `_merge_security_kwargs`, `from_connection`,
and `_normalize_bootstrap` are removed. `describe_missing` is **not** added (no always‑on
verify).

### 7.4 The fail‑fast exception

```python
# calfkit/exceptions.py
class MissingTopicsError(RuntimeError):
    """Raised when topic provisioning was enabled but one or more required topics could
    not be created (e.g. authorization denied), so the consumers would stall. Names the
    topics and the remedies (grant CreateTopics ACL / pre-create out-of-band)."""
    def __init__(self, topics: list[str]): ...
    topics: list[str]
```

### 7.5 Wiring in `connect()` and `Worker`

```python
# Client.connect(...)  — no new kwargs
provisioning = provisioning or ProvisioningConfig()
ensurer = StartupTopicEnsurer(config=provisioning)
ensurer.declare([reply_topic], framework=True)
broker = _PreStartHookBroker(server_urls, middlewares=[ContextInjectionMiddleware],
                             pre_start=ensurer.run, **broker_forwarded)
dispatcher.register(broker, reply_topic, group_id)
client = cls(broker, reply_topic, dispatcher, ...)
client._startup_ensurer = ensurer      # so the Worker can declare node topics
return client
```

```python
# Worker._on_startup(): after register_handlers(), instead of provision_topics():
self._client._startup_ensurer.declare(topics_for_nodes(self._registered_nodes))
self._client._startup_ensurer.declare({n._return_topic for n in self._registered_nodes},
                                      framework=True)
```

The worker no longer *executes* provisioning; it only *declares*. The single provision
pass runs in `broker.start()` (after the on‑startup hooks, before subscribers consume),
reusing FastStream's admin client — and only when enabled.

---

## 8. Concrete changes (file‑by‑file)

| File | Change |
|---|---|
| `calfkit/client/_broker.py` | **New.** `_PreStartHookBroker(KafkaBroker)` — generic one‑shot pre‑start hook. |
| `calfkit/provisioning/ensurer.py` | **New.** `StartupTopicEnsurer` — policy + declared‑topic registry + `run(broker)` (no‑op when disabled). |
| `calfkit/provisioning/provisioner.py` | Demote `TopicProvisioner` to stateless `provision_topics(admin, …)`. Remove `_make_admin_client`, `_merge_security_kwargs`, `from_connection`, `_normalize_bootstrap`. Keep `ProvisionReport`, `topics_for_nodes`, classify logic, error types. |
| `calfkit/client/base.py` | `connect()` builds `_PreStartHookBroker` + `StartupTopicEnsurer`, declares the reply topic, wires `pre_start`. **Delete** the lazy provisioning block in `_publish_call`, the `_reply_topic_provisioned` flag, and `_provision_reply_topic`. **Remove** the `server_urls`/`security_kwargs` capture/strip that existed only for the second admin client (keep `provisioning` property; drop `server_urls`/`security_kwargs` properties if unused elsewhere). |
| `calfkit/worker/worker.py` | `_on_startup` declares node topics into the ensurer instead of calling `provision_topics()`. **Delete** `Worker.provision_topics()` (hard break — no‑start provisioning path removed). |
| `calfkit/exceptions.py` | **New** `MissingTopicsError`. |
| `calfkit/provisioning/__init__.py` | Export `StartupTopicEnsurer`, `provision_topics`; drop removed symbols. |
| `docs/topic-provisioning.md` | Update to the new ownership model. |

---

## 9. Public API & breaking changes (pre‑1.0 — clean breaks, no shims)

- **New:** `MissingTopicsError`, raised only when provisioning is **enabled** but a
  declared topic could not be created. Previously this case warned and then stalled.
- **No new `connect()` kwargs.** (The earlier `verify_topics` / `topic_verify_timeout`
  idea is dropped — no always‑on verify.)
- **Removed:** `Worker.provision_topics()` (provisioning now happens at broker start).
- **Removed:** `TopicProvisioner.from_connection()` / its self‑owned admin client;
  `Client.server_urls` / `Client.security_kwargs` properties if unused elsewhere.
- **Unchanged:** the default (provisioning‑disabled) behaviour, `ProvisioningConfig`,
  `Client.provisioning`, `topics_for_nodes`, the request path, the worker's three run
  surfaces, `reply_topic` behaviour.

---

## 10. Lifecycle coverage (verified against FastStream source)

All start surfaces funnel through `_PreStartHookBroker.start()`:

| Surface | Path to `broker.start()` |
|---|---|
| `await client.broker.start()` | direct |
| `await worker.run()` | `app.run()` → `_startup` → `app.start()` → `_start_broker()` → `broker.start()` |
| `await worker.start()` / `async with worker:` | `app.start()` → `_start_broker()` → `broker.start()` |
| `calfkit run` | builds `Client`+`Worker` → `worker.run()` (as above) |
| auto‑start on first `invoke` | `_publish_call` → `broker.start()` |

- **Ordering:** FastStream runs `on_startup` hooks *before* `_start_broker()`, so the
  worker has declared its node topics by the time `ensurer.run(broker)` executes inside
  `broker.start()`.
- **Failure teardown:** if `ensurer.run()` raises (`MissingTopicsError` or a provisioning
  error), `broker.start()` raises → `app.start()/run()` raises → the worker's
  `start()`/`run()` wrapper runs `_cleanup_after_failed_start()` (closes MCP sessions +
  resource brackets). `async with` is covered because `worker.start()` cleans up
  internally before re‑raising. Same path the worker already uses for "broker can't
  reach Kafka."

---

## 11. Behaviour matrix (target)

| Provisioning | Broker | Result |
|---|---|---|
| **enabled** | no auto‑create (Tansu/hardened) | reply + node topics created before consume → start clean **(the #180 fix)** |
| **enabled** | no auto‑create, create denied (ACL) | report shows uncreated → **raise** `MissingTopicsError` (was: warn + stall) |
| **enabled** | auto‑create (Redpanda) | create returns existing/created → start clean |
| **disabled** (default) | auto‑create (Kafka/Redpanda) | **unchanged** — auto‑created on subscribe |
| **disabled** (default) | no auto‑create | **unchanged** — still hangs (enable provisioning for such brokers; documented limitation) |
| any | `consumer_only` / no admin client | provisioning skipped with a clear log |

---

## 12. Test plan (TDD, red‑first)

**Unit — `_PreStartHookBroker`** (call `start()` directly; no live Kafka):
1. `pre_start` runs once, after `connect()`, before `super().start()`; a second `start()`
   does not re‑run it (one‑shot).
2. `pre_start=None` → behaves like `KafkaBroker.start()`.
3. `pre_start` raising propagates and `super().start()` is not reached.
4. `stop()` resets the one‑shot.

**Unit — `provisioner` / `ensurer`** (inject a fake `AIOKafkaAdminClient`):
5. `provision_topics(admin, …)` classifies created/existing/unauthorized/retry/raise and
   honours `framework_topics` (no `topic_configs` on framework inboxes).
6. `StartupTopicEnsurer.run`: **disabled → returns immediately, admin never touched**;
   enabled → creates the declared set; enabled + uncreated topic → `MissingTopicsError`;
   enabled + no admin client → skip with log.
7. `Client.connect` wires the ensurer with the reply topic declared `framework=True` and
   `pre_start=ensurer.run`; the deleted lazy‑guard tests are rewritten to drive
   `ensurer.run` / `_PreStartHookBroker.start` directly.

**Unit — worker:** `_on_startup` declares `topics_for_nodes(...)` + return inboxes into the
ensurer (incl. MCP bridge topics, post‑`register_handlers`); no standalone provisioning pass.

**Integration — live Tansu (no auto‑create):**
8. **#180 regression** — bare `await client.broker.start()` with `enabled=True` returns
   within timeout (was: hang).
9. **#180 regression (worker)** — `Worker.run()` reaches "serving" without hanging.
10. **Default path unchanged** — `Client.connect()` (disabled) → `broker.start()` does
    **not** call the admin client / does not provision (assert no create), confirming the
    default path is untouched.
11. **Happy path** — full `execute_node` round‑trip still works (deleted lazy guard didn't
    regress the request path).

`TestKafkaBroker`‑based tests are unaffected (it patches `broker.start`).
Run `/pytest-coverage` on the new/changed modules to 100%, then `make fix && make check`.

---

## 13. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Reusing a FastStream internal (`config.broker_config.admin_client`) | Single guarded accessor (`_admin_client_or_none`) with a clear error; FastStream version is already pinned. One place to update if it ever moves. |
| `consumer_only` / no admin client | Detected → skip provisioning with a clear log (honest behaviour, not a regression). |
| disabled provisioning + no‑auto‑create broker still hangs (e.g. hardened prod Kafka; the reply topic is dynamically named and can't be pre‑created) | **Accepted + documented** limitation. Guidance: enable provisioning for non‑auto‑creating brokers, or pass a static `reply_topic` and pre‑create it. Not silently worked around on the default path. |
| Test churn in existing provisioning suite (demoting `TopicProvisioner`) | Phase the change (see §14); `provision_topics` is still seam‑testable with a fake admin. |

---

## 14. Phased rollout

- **Phase 1 — close #180 (the bug):** `_PreStartHookBroker` + `StartupTopicEnsurer` +
  `MissingTopicsError`; wire the reply topic; delete the lazy guard. Ships the
  user‑visible fix. (Reply topic provisioned on the reused admin client when enabled.)
- **Phase 2 — unify worker provisioning + simplify executor:** move the worker's node
  provisioning to `ensurer.declare`, delete `Worker.provision_topics()`, demote
  `TopicProvisioner` to the stateless function, and remove the second admin client +
  security re‑plumbing + `server_urls`/`security_kwargs` capture.

Both phases land behind the same architecture; Phase 1 alone resolves the issue, Phase 2
removes the duplication. They may be one PR or two depending on review appetite.

---

## 15. Appendix — verified source references

- `faststream/kafka/configs/broker.py` — broker‑managed admin client lifecycle (`connect`/`disconnect`), `consumer_only` skip.
- `faststream/kafka/broker/broker.py` — `start()` = `connect()` + `super().start()`.
- `faststream/_internal/broker/broker.py` — base `start()` iterates `self.subscribers`; `connect()` idempotent (`if self._connection is None`).
- `faststream/_internal/application.py` — `run()`→`_startup`→`start()`→`_start_broker()`→`broker.start()`; on‑startup hooks run before the broker starts.
- `faststream/_internal/testing/broker.py` — `TestKafkaBroker` patches `broker.start`.
- `faststream/confluent/helpers/client.py`, `confluent/helpers/admin.py` — the framework‑owned auto‑create precedent (subscriber topics only, rf=1/parts=1).
