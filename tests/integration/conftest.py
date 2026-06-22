"""Shared fixtures for the real-broker (``kafka``) integration lane.

Tests in this lane are marked ``@pytest.mark.kafka`` and exercise a REAL Kafka-protocol broker
(Redpanda). They are OPT-IN: ``addopts`` in ``pyproject.toml`` deselects ``kafka`` by default,
so a bare ``pytest`` / ``make test`` never needs Docker. Run them with ``make test-kafka``
(parallel by default; ``make test-kafka KAFKA_XDIST=0`` for serial).

Broker source, in priority order:

1. ``CALF_TEST_KAFKA_BOOTSTRAP`` — if set, reuse that already-running broker (a CI services
   container, a local ``rpk``/Redpanda, etc.). No container is started and the suite owns no
   lifecycle.
2. Under ``pytest-xdist`` (``-n`` > 0): the xdist **controller** starts ONE Redpanda for the
   whole run (:func:`pytest_configure_node`) and hands its bootstrap address to every worker via
   ``workerinput``, so all workers share one broker. It is torn down once, in the controller, by
   :func:`pytest_unconfigure`. The per-test ``topic_namespace`` keeps concurrent tests isolated.
   NOTE: :func:`pytest_configure_node` is a controller-only hook, so the run must be scoped to
   this directory (``pytest tests/integration …`` / ``make test-kafka``) for the controller to
   load this conftest — a bare ``pytest -m kafka`` from the repo root would not register it.
3. Otherwise (plain ``pytest`` / ``-n0``): a single Redpanda is started for the session and torn
   down at session end (the original path).

Broker config is env-overridable (all optional): ``CALF_TEST_REDPANDA_IMAGE`` /
``CALF_TEST_REDPANDA_SMP`` / ``CALF_TEST_REDPANDA_MEMORY``.

When testcontainers is not installed, or Docker is not reachable, the lane SKIPS cleanly rather
than erroring. A broker that *is* reachable but fails for any other reason propagates as a real
error (no silent green).
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from textwrap import dedent
from typing import Any

import pytest

# A current, pinned Redpanda image (testcontainers otherwise defaults to a stale 2023 tag).
# Overridable via CALF_TEST_REDPANDA_IMAGE; bump this deliberately.
_DEFAULT_REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v26.1.10"
# Redpanda dev-container resources. Defaults bumped above testcontainers' --smp 1 --memory 1G for
# the parallel load; overridable per environment (laptop vs CI) via the env vars below.
_DEFAULT_SMP = "2"
_DEFAULT_MEMORY = "2G"

# Distinguishes "the controller never injected an address" (wrong invocation — see below) from
# "address is None" (Docker/testcontainers absent).
_UNSET = object()


def _make_tuned_container() -> Any:
    """Build (unstarted) a Redpanda testcontainer with env-overridable image/smp/memory.

    Upstream ``RedpandaContainer.tc_start`` hardcodes ``--smp 1 --memory 1G``; subclass to
    substitute env values into the otherwise-identical start command. testcontainers is imported
    lazily so a machine without the ``integration`` extra skips rather than erroring at import.
    """
    from testcontainers.kafka import RedpandaContainer

    image = os.getenv("CALF_TEST_REDPANDA_IMAGE", _DEFAULT_REDPANDA_IMAGE)
    smp = os.getenv("CALF_TEST_REDPANDA_SMP", _DEFAULT_SMP)
    memory = os.getenv("CALF_TEST_REDPANDA_MEMORY", _DEFAULT_MEMORY)

    class _TunedRedpanda(RedpandaContainer):
        # Mirrors upstream RedpandaContainer.tc_start, parametrizing --smp/--memory.
        def tc_start(self) -> None:
            host = self.get_container_host_ip()
            port = self.get_exposed_port(self.redpanda_port)
            data = (
                dedent(
                    f"""
                    #!/bin/bash
                    /usr/bin/rpk redpanda start --mode dev-container --smp {smp} --memory {memory} \
                    --kafka-addr PLAINTEXT://0.0.0.0:29092,OUTSIDE://0.0.0.0:9092  \
                    --advertise-kafka-addr PLAINTEXT://127.0.0.1:29092,OUTSIDE://{host}:{port}
                    """
                )
                .strip()
                .encode("utf-8")
            )
            self.create_file(data, RedpandaContainer.TC_START_SCRIPT)

    return _TunedRedpanda(image)


def _ensure_shared_broker(config: pytest.Config) -> str | None:
    """Start ONE shared Redpanda (idempotent; cached on ``config``); ``None`` if unavailable.

    Returns the bootstrap address, or ``None`` when testcontainers/Docker is absent (so workers
    skip). A start failure for any *other* reason propagates (no silent green).
    """
    existing = getattr(config, "_calf_shared_redpanda", None)
    if existing is not None:
        return existing.get_bootstrap_server()  # type: ignore[no-any-return]
    try:
        from docker.errors import DockerException

        container = _make_tuned_container()
    except ImportError:
        return None  # the `integration` extra is not installed
    try:
        container.start()
    except DockerException:
        return None  # Docker daemon absent / unreachable
    config._calf_shared_redpanda = container  # type: ignore[attr-defined]
    return container.get_bootstrap_server()  # type: ignore[no-any-return]


@pytest.hookimpl(optionalhook=True)
def pytest_configure_node(node: Any) -> None:
    """xdist controller hook: start one shared broker and inject its address into each worker.

    ``optionalhook=True`` so a plain (xdist-less) run does not trip pytest's unknown-hook
    validation. Runs once per worker node in the controller; the container is started on the
    first call and cached, so every worker receives the same broker.
    """
    if os.getenv("CALF_TEST_KAFKA_BOOTSTRAP"):
        return  # external broker; nothing to start
    markexpr = node.config.getoption("markexpr", default="") or ""
    if "kafka" not in markexpr or "not kafka" in markexpr:
        return  # the kafka lane isn't selected — don't start a broker for an unrelated parallel run
    node.workerinput["calf_kafka_bootstrap"] = _ensure_shared_broker(node.config)


def pytest_unconfigure(config: pytest.Config) -> None:
    """Stop the shared container iff THIS process (the controller) started one."""
    container = getattr(config, "_calf_shared_redpanda", None)
    if container is not None:
        container.stop()
        config._calf_shared_redpanda = None  # type: ignore[attr-defined]


@pytest.fixture(scope="session")
def kafka_bootstrap(request: pytest.FixtureRequest) -> Iterator[str]:
    """Bootstrap address of a live broker. See the module docstring for the resolution order."""
    external = os.getenv("CALF_TEST_KAFKA_BOOTSTRAP")
    if external:
        yield external
        return

    workerinput = getattr(request.config, "workerinput", None)
    if workerinput is not None:
        # Under xdist: the controller started one shared broker (pytest_configure_node) and
        # injected its address here. A MISSING key means the controller never loaded THIS conftest
        # — i.e. the lane was run without its directory path, so the controller-only hook was never
        # registered. Surface that as an actionable message rather than a confusing "no broker".
        addr = workerinput.get("calf_kafka_bootstrap", _UNSET)
        if addr is _UNSET:
            pytest.skip(
                "shared broker not started under xdist — run the lane scoped to its directory so the "
                "controller registers the broker hook: `make test-kafka` (or "
                "`pytest tests/integration -m kafka -n auto`)"
            )
        if addr is None:
            pytest.skip("Docker/testcontainers not available for the shared Redpanda testcontainer")
        yield addr
        return

    # Plain pytest / -n0: own a single container for this session (the original path).
    pytest.importorskip(
        "testcontainers.kafka",
        reason="testcontainers not installed; run `uv sync --group integration`",
    )
    from docker.errors import DockerException

    try:
        container = _make_tuned_container()
        container.start()
    except DockerException as exc:
        pytest.skip(f"Docker not available for Redpanda testcontainer: {exc}")
    try:
        yield container.get_bootstrap_server()
    finally:
        container.stop()


@pytest.fixture
def topic_namespace() -> str:
    """A unique, collision-free prefix for one test's topics and consumer groups.

    Tests in this lane share one broker, so derive every topic and consumer-group name from this
    prefix to avoid cross-test bleed and leftover-state flakes — and to stay isolated when the
    lane runs in parallel under ``pytest-xdist``.
    """
    return f"calf-test-{uuid.uuid4().hex[:8]}"
