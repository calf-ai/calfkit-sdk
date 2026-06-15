# Tests with external dependencies opt in via orthogonal markers; brokers come from testcontainers

Almost the whole suite runs offline and deterministically — a scripted
`FunctionModel` stands in for the LLM and FastStream's in-memory
`TestKafkaBroker` stands in for Kafka — so `make test` must never need Docker, a
network, or credentials. But a few tests genuinely need a *real* broker (proving
produce/consume, compaction, capability discovery over the wire), and a separate
few need a *real* LLM API (proving our integration with the live provider holds).
These are two independent external dependencies, and the existing
`tests/integration/` directory conflated them: it keyed off the LLM axis (its
files were gated by `skip_if_no_openai_key`) while a file talking to a real
broker sat alongside them, gated by an ad-hoc runtime connect-probe that
hard-coded `localhost:9092`.

We rejected a single coarse `integration` directory or marker as the gate. A
test can need *neither, one, the other, or both* dependencies, and one bucket
cannot express four states — markers compose, directories do not (a both-axes
test has no single home). We also rejected deciding skips by probing the network
at collection time: it is slow, flaky, and turns a cold or absent broker into a
*collection error* rather than a clean skip. The inherited mcp-toolbox test was
exactly that anti-pattern and was migrated off it.

The decision is **orthogonal, composable pytest markers, one per external
dependency**, deselected by default. `kafka` (a real broker) is registered now;
`llm` (a real model API) is the planned second axis for the existing real-API
tests, which for now keep their `skip_if_no_openai_key` gate. Markers are
registered under `--strict-markers` (an unregistered marker is an error, not a
silent no-op), and `addopts = ["--strict-markers", "-m", "not kafka"]` makes the
broker lane opt-in: a bare `pytest` / `make test` deselects it and needs no
Docker, while `-m kafka` / `make test-kafka` overrides the default selection. A
both-axes test would simply carry both markers. The broker itself is a
session-scoped single-node Redpanda started and torn down by **testcontainers**
from inside the test session, so neither developers nor CI hand-run one;
`CALF_TEST_KAFKA_BOOTSTRAP` is an escape hatch to reuse an external broker
instead. Missing testcontainers or an unreachable Docker daemon **skips
cleanly** (the lane degrades on a machine without Docker), but a broker that *is*
reachable and fails for any other reason propagates as a real error — no silent
green.

Consequences worth remembering: the topic-provisioning lane
(`kafka-integration.yml`, env-gated by `CALF_TEST_KAFKA`) is deliberately **not**
folded onto testcontainers and keeps its own hand-run broker, because it asserts
the "no silent topic create" contract and therefore needs `auto_create_topics`
DISABLED — which testcontainers' `RedpandaContainer` (it runs `redpanda start
--mode dev-container`, auto-create ON) cannot give as a first-class knob. The
testcontainers lane is consequently far simpler than the provisioning one: no
manual `docker run`, health-wait, or config steps. The Redpanda image tag is
pinned and bumped deliberately, because the library's default
(`RedpandaContainer`'s built-in `v23.1.13`) is years stale. And the `llm` axis
is reserved but unwired: when it lands it should also close the latent gap where
`skip_if_no_openai_key` checks only `OPENAI_API_KEY` while the model fixture also
hard-requires `TEST_LLM_MODEL_NAME`. Decided 2026-06-14.
