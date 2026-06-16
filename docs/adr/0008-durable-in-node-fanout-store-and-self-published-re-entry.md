# Durable in-node fan-out: node-scoped compacted tables + a self-published re-entry close

A fan-out agent issues N parallel tool calls and must aggregate the N returns before resuming its
turn. The original implementation held that batch in-process (`agent._pending_batches`, a dict
keyed by the invocation's `frame_id`): correct on one steady process, but the batch is **lost on a
graceful rebalance or restart and unsafe under multiple replicas** — a partition moving mid-batch
strands the workflow with no record of what was owed.

We make the batch **durable in two node-scoped compacted ktables**: `calf.fanout.{node_id}.state`
(a `FanoutState` — the registration plus the accumulating per-slot outcomes, re-written once per
fold, last-write-wins) and `calf.fanout.{node_id}.basestate` (a write-once `EnvelopeSnapshot` — the
conversation state, call stack, and deps as of fan-out, the sole restore source at close). The fold
is **framework-final in the node** (`BaseNodeDef._aggregate`), not a separate worker-level
aggregator loop over a `calf.fanout.events` topic (the rejected v1 design) — the aggregator added a
topic, a process, and a hop for machinery that belongs in the one place that already holds the
batch's context.

The close is a **self-published re-entry** (`x-calf-kind: return`, `in_reply_to == fanout_id`), not
an in-process resume: on the completing fold the owner publishes a frame-preserving return
addressed to its own inbox, and on consuming it rebuilds context from the durable basestate and
resumes the body. This keeps the resume on the normal delivery path (the durable analog of a tool
return), so it survives a rebalance between completion and resume rather than living in volatile
call-stack state.

The design is **single-writer-safe from transport, not from the tables**: compacted topics are LWW
with no compare-and-swap, so correctness rests on exactly one folder per batch — every sibling
reply is correlation-keyed (one partition), all replicas share `group_id = node.name`, and
caller-capable nodes are pinned to `max_workers=1`. The tables provide *availability* (a new owner
reads the durable state and continues); transport provides *serialization*. Reads of the mutable
`state` barrier first for read-your-own-writes (ktables `barrier()`); the write-once `basestate`
barriers only on a miss.

Consequences: +1 Kafka hop per fan-out closure (the re-entry); two compacted topics per fan-out
node, **self-provisioned by ktables' `ensure_topic`** because calfkit's provisioner cannot set
`cleanup.policy=compact`; each fan-out agent owns the store as a node `@resource` (offline tests,
where the `@resource` never runs under `TestKafkaBroker`, inject a fake into the bag instead). This
PR is **return-only**: a failed sibling folds as a `FailedToolCall` and still strands the caller at
close — today's at-most-once behavior, minus the lost batch. Typed-fault escalation is the
fault-rail PR's job, which re-homes this fold's `FanoutOutcome.result` field to the parts/fault
carriage and adds the seam arms onto the staged pipeline this PR builds. Decided 2026-06-15.
