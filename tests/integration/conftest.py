"""Shared fixtures for the real-broker (``kafka``) integration lane.

Tests in this lane are marked ``@pytest.mark.kafka`` and exercise a REAL
Kafka-protocol broker (Redpanda). They are OPT-IN: ``addopts`` in
``pyproject.toml`` deselects ``kafka`` by default, so a bare ``pytest`` /
``make test`` never needs Docker. Run them with ``-m kafka`` / ``make
test-kafka``.

Broker source, in priority order:

1. ``CALF_TEST_KAFKA_BOOTSTRAP`` — if set, reuse that already-running broker
   (a CI services container, a local ``rpk``/Redpanda, etc.). No container is
   started and the suite owns no lifecycle. This mirrors the env-var contract
   the topic-provisioning lane already uses, so one address works everywhere.
2. Otherwise a single-node Redpanda is started for the test session via
   testcontainers and torn down at session end.

When testcontainers is not installed, or Docker is not reachable, the lane
SKIPS cleanly rather than erroring — ``-m kafka`` degrades gracefully on a
machine without Docker. A broker that *is* reachable but fails for any other
reason propagates as a real error (no silent green).
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest

# A current, pinned Redpanda image. testcontainers' RedpandaContainer otherwise
# defaults to a stale 2023 tag (v23.1.13); pin a recent release for parity with
# the broker operators actually run. Bump this deliberately.
_REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v26.1.10"


@pytest.fixture(scope="session")
def kafka_bootstrap() -> Iterator[str]:
    """Bootstrap address of a live broker, shared across the test session.

    See the module docstring for the broker-source resolution order. Yields the
    ``host:port`` string callers pass to ``Client.connect`` / admin clients.
    """
    external = os.getenv("CALF_TEST_KAFKA_BOOTSTRAP")
    if external:
        yield external
        return

    # testcontainers ships RedpandaContainer under its ``kafka`` module (NOT
    # ``testcontainers.redpanda``). importorskip turns a missing optional
    # dependency into a clean skip instead of a collection error.
    kafka_module = pytest.importorskip(
        "testcontainers.kafka",
        reason="testcontainers not installed; run `uv sync --group integration`",
    )
    # docker-py is a hard dependency of testcontainers, so it is importable here.
    from docker.errors import DockerException

    try:
        container = kafka_module.RedpandaContainer(_REDPANDA_IMAGE)
        container.start()
    except DockerException as exc:
        # Docker daemon absent / unreachable: skip rather than fail the lane.
        pytest.skip(f"Docker not available for Redpanda testcontainer: {exc}")

    try:
        yield container.get_bootstrap_server()
    finally:
        container.stop()


@pytest.fixture
def topic_namespace() -> str:
    """A unique, collision-free prefix for one test's topics and consumer groups.

    Tests in this lane share one session broker (and reruns hit the same broker
    when ``CALF_TEST_KAFKA_BOOTSTRAP`` points at a persistent one), so derive
    every topic and consumer-group name from this prefix to avoid cross-test
    bleed and leftover-state flakes.
    """
    return f"calf-test-{uuid.uuid4().hex[:8]}"
