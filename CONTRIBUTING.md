# Contributing to calfkit

Issues and pull requests are welcome. Please
[open an issue](https://github.com/calf-ai/calfkit-sdk/issues) to discuss
substantial changes before sending a PR.

## Development setup

calfkit uses [uv](https://docs.astral.sh/uv/) for dependency management and
requires Python 3.10+.

```console
$ uv sync --group dev        # install the project and dev dependencies
$ uv run pytest tests/       # run anything inside the project environment
```

Run any project command through `uv run` so it uses the locked environment.

## Quality gates

Before opening a PR, make sure these pass — CI runs the same checks:

```console
$ make fix     # auto-fix lint + formatting (ruff)
$ make check   # lint, format, and type checks (ruff + mypy)
$ make test    # run the test suite (real-broker tests excluded)
```

`make help` lists every target. New features and fixes should come with tests
(see [Testing](#testing)).

## Opening a pull request

- **Title follows [Conventional Commits](https://www.conventionalcommits.org/).**
  It is enforced by CI and parsed by release-please to generate the changelog.
  Allowed types: `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `build`,
  `ci`, `chore`, `revert`.
- **No parentheses in the title subject.** release-please's parser rejects them
  and silently skips the release. Put any scope in a `(scope)` *after the type* —
  e.g. `feat(cli): add run command`, not `feat: add run command (cli)`.
- PRs are **squash-merged**, so the PR title becomes the commit subject.
- Merging requires **two approving reviews** and **code-owner review** (see
  [`.github/CODEOWNERS`](.github/CODEOWNERS)); review threads must be resolved.

## Testing

calfkit's tests are split by the external dependency they require. The default
suite runs fully offline; tests that need a real broker or a real LLM are
opt-in, so `make test` stays fast and needs no Docker, network, or credentials.

### Test taxonomy

| Kind | Needs | Gate | In `make test`? |
|---|---|---|---|
| Unit / component | nothing — `FunctionModel` + in-memory `TestKafkaBroker` | — | yes |
| Real broker | a Kafka-protocol broker (Redpanda) | `@pytest.mark.kafka` | no — use `make test-kafka` |
| Topic provisioning | a broker with topic auto-create **disabled** | `CALF_TEST_KAFKA` env var | no — separate lane |
| Real LLM | a live model API | `OPENAI_API_KEY` (+ `TEST_LLM_MODEL_NAME`) | skips unless the key is set |

Default to **offline** tests: a scripted `FunctionModel` stands in for the LLM
and FastStream's `TestKafkaBroker` stands in for Kafka, so the tests are
deterministic, free, and need no infrastructure. Reach for a real broker only to
prove behavior a fake cannot — produce/consume round-trips, capability discovery
over the wire, topic provisioning.

### Writing a real-broker test

Real-broker tests live in `tests/integration/`, carry the `kafka` marker, and
take the `kafka_bootstrap` fixture (plus `topic_namespace` for isolation):

```python
import pytest

from calfkit.client import Client

pytestmark = pytest.mark.kafka  # opt the whole module into the real-broker lane


async def test_round_trip(kafka_bootstrap: str, topic_namespace: str) -> None:
    client = Client.connect(kafka_bootstrap)
    topic = f"{topic_namespace}.input"
    ...
```

- **`kafka_bootstrap`** — the `host:port` of a live broker. By default a
  single-node Redpanda started by [testcontainers](https://testcontainers.com/)
  for the test session; set `CALF_TEST_KAFKA_BOOTSTRAP` to point at an external
  broker instead.
- **`topic_namespace`** — a unique per-test prefix. Derive every topic and
  consumer-group name from it so tests sharing the session broker don't collide.

For working examples, see `tests/integration/test_real_broker_smoke.py` (the
minimal marker-and-fixtures wiring, using a raw `aiokafka` produce/consume
round-trip) and `tests/integration/test_mcp_toolbox_capability.py` (a richer test
that drives calfkit's `Client` against a live broker).

### Running the real-broker lane

```console
$ make test-kafka     # runs `-m kafka`; testcontainers starts/stops Redpanda
```

This requires a running **Docker** daemon (the `integration` group, which `make
test-kafka` installs, provides testcontainers). Without Docker the lane skips
cleanly rather than failing. To run against an already-running broker instead —
no Docker or `integration` group needed, since testcontainers is never imported:

```console
$ CALF_TEST_KAFKA_BOOTSTRAP=localhost:9092 uv run pytest -m kafka
```

### The topic-provisioning lane

Tests that assert the "no silent topic create" contract need a broker with topic
auto-create **disabled**, which the testcontainers broker (run in dev-container
mode, auto-create on) does not provide. Those tests live in
`tests/integration/test_topic_provisioning.py`, are gated by the
`CALF_TEST_KAFKA` env var, and run in their own CI lane against a
purpose-configured broker — keep new real-broker tests in the `kafka` lane
unless they specifically need auto-create off.

For the rationale behind this opt-in, marker-gated structure, see
[ADR 0007](docs/adr/0007-external-dependency-tests-opt-in-via-orthogonal-markers.md).
