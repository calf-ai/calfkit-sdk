"""Real-broker tests for the ``ck dev`` agent lifecycle (kafka lane, ADR-0007).

Four slices from the impl plan's CG-C, each end to end with real processes:

- the ``run -d`` lifecycle against the lane's broker: spawn → online → reuse-with-age →
  ``status`` → ``stop`` (group-reap: no survivor from the daemon tree);
- **the kill-9 liveness proof (Ryan's empirical gate, spec §5.6)**: a dev-preset (5s heartbeat)
  daemon never flaps offline while alive, and flips offline within the ~15–20s staleness window
  after a SIGKILL crash (no tombstone);
- the ``chat TARGET`` in-process session against the real broker (shared-client co-location);
- the fresh-broker first-run e2e (bundled Tansu): one command from nothing to an online agent —
  the readiness gate absorbs the missing-presence-topic window — then ``ck dev down``.

Daemon targets are generated per test as uniquely-named modules under ``tmp_path`` (resolved via
``--app-dir``), so parallel tests on the shared broker never collide on names or import caches.
The scan slices need ``psutil`` (the ``[mesh]`` extra) and skip cleanly without it — the shipped
``test_dev_broker_kafka`` posture.
"""

from __future__ import annotations

import asyncio
import os
import re
import signal
import socket
import time
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from calfkit.cli._dev_broker import normalize
from calfkit.cli._dev_probe import is_reachable
from calfkit.client import Client, MeshViewConfig
from calfkit.tuning import KTableReaderTuning

pytestmark = pytest.mark.kafka

_FAST_MESH = MeshViewConfig(reader_tuning=KTableReaderTuning(poll_timeout_ms=20, fetch_max_wait_ms=10))

_NODES_TEMPLATE = '''"""Generated ck dev daemon target (unique per test)."""

from calfkit.nodes.agent import Agent
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient


class _FakeModel(PydanticModelClient):
    """Advertise-only: the agent is hosted for presence, never to run a turn."""

    @property
    def model_name(self) -> str:
        return "fake"

    @property
    def system(self) -> str:
        return "fake"

    async def request(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError


agent = Agent("{name}", subscribe_topics="{name}.in", model_client=_FakeModel())
'''


def _write_target(tmp_path: Path, agent_name: str) -> str:
    """A uniquely-named importable module hosting one advertise-only agent; returns its spec."""
    module = f"dev_nodes_{uuid.uuid4().hex[:10]}"
    (tmp_path / f"{module}.py").write_text(_NODES_TEMPLATE.format(name=agent_name))
    return f"{module}:agent"


def _invoke(args: list[str]) -> Any:
    from calfkit.cli import _build_app

    return CliRunner().invoke(_build_app(), args)


def _pid_of(launch_line_output: str) -> int:
    """The DAEMON supervisor pid from the §3.1 launched line — never the managed-broker line's
    pid, which precedes it when the broker was spawned too (the fresh-Tansu path)."""
    match = re.search(r"launched (?:agent|tool) '[^']+' \(pid (\d+)\)", launch_line_output)
    assert match, f"no launched-line pid in: {launch_line_output!r}"
    return int(match.group(1))


def _gone_or_zombie(psutil: Any, pid: int) -> bool:
    try:
        return bool(psutil.Process(pid).status() == psutil.STATUS_ZOMBIE)
    except psutil.NoSuchProcess:
        return True


def _reap_tree(pid: int) -> None:
    """Best-effort cleanup so a failed test never leaks a daemon tree."""
    with suppress(ProcessLookupError, PermissionError):
        os.killpg(pid, signal.SIGKILL)


@pytest.fixture
def isolated_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Locks + logs under tmp (inherited by spawned daemons); no ambient mesh URL."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("CALFKIT_MESH_URL", raising=False)
    return tmp_path


def test_run_detach_lifecycle_spawn_online_reuse_status_stop(
    kafka_bootstrap: str, topic_namespace: str, isolated_home: Path, tmp_path: Path
) -> None:
    """spawn → online → reuse-with-age → status → stop; the stop group-reaps the WHOLE daemon
    tree (supervisor + reload worker + multiprocessing helpers — no survivors)."""
    psutil = pytest.importorskip("psutil", reason="the [mesh] extra is not installed")
    agent_name = f"{topic_namespace}-general"
    spec = _write_target(tmp_path, agent_name)
    base = ["--host", kafka_bootstrap, "--app-dir", str(tmp_path)]

    result = _invoke(["dev", "run", "-d", spec, *base])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert f"launched agent '{agent_name}'" in result.stdout
    pid = _pid_of(result.stdout)
    try:
        # Reuse: a second -d finds the names online and spawns nothing (the age is the honesty device).
        again = _invoke(["dev", "run", "-d", spec, *base])
        assert again.exit_code == 0, again.stdout + str(again.exception)
        assert f"reusing agent '{agent_name}' (online, last seen" in again.stdout

        status = _invoke(["dev", "status", "--host", kafka_bootstrap])
        assert status.exit_code == 0, status.stdout + str(status.exception)
        row = next(line for line in status.stdout.splitlines() if agent_name in line)
        assert "online (last seen" in row
        assert str(pid) in row
        assert "not a ck dev daemon" not in row  # it IS managed

        tree = [pid] + [child.pid for child in psutil.Process(pid).children(recursive=True)]
        assert len(tree) >= 2, "the reload supervisor should have a worker child"

        stop = _invoke(["dev", "stop", agent_name, "--host", kafka_bootstrap])
        assert stop.exit_code == 0, stop.stdout + str(stop.exception)
        assert f"stopped daemon pid {pid} (agents: {agent_name})" in stop.stdout

        deadline = time.monotonic() + 15.0
        while not all(_gone_or_zombie(psutil, member) for member in tree):
            assert time.monotonic() < deadline, f"survivors from the daemon tree: {tree}"
            time.sleep(0.2)
    finally:
        _reap_tree(pid)


def test_kill9_staleness_flip_and_no_flap_liveness_proof(
    kafka_bootstrap: str, topic_namespace: str, isolated_home: Path, tmp_path: Path
) -> None:
    """Ryan's empirical gate (spec §5.6): a 5s-heartbeat daemon (a) never flaps offline while
    alive across many samples, and (b) after kill -9 (a crash: no tombstone) reads offline within
    the ~15–20s staleness window (3 × 5s from its last heartbeat)."""
    pytest.importorskip("psutil", reason="the [mesh] extra is not installed")
    agent_name = f"{topic_namespace}-general"
    spec = _write_target(tmp_path, agent_name)

    result = _invoke(["dev", "run", "-d", spec, "--host", kafka_bootstrap, "--app-dir", str(tmp_path)])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    pid = _pid_of(result.stdout)

    async def _observe() -> float:
        client = Client.connect(kafka_bootstrap, mesh_config=_FAST_MESH)
        try:
            # (a) no flap: ~12s of continuous sampling (> 2 heartbeat gaps) — online in EVERY sample.
            for _ in range(24):
                agents = await client.mesh.get_agents()
                assert agent_name in agents, "a live dev-preset agent flapped offline between heartbeats"
                await asyncio.sleep(0.5)
            # (b) crash: SIGKILL the whole tree — no graceful shutdown, no tombstone.
            os.killpg(pid, signal.SIGKILL)
            killed_at = time.monotonic()
            while agent_name in (await client.mesh.get_agents()):
                assert time.monotonic() - killed_at < 25.0, "the crashed agent never went stale"
                await asyncio.sleep(0.5)
            return time.monotonic() - killed_at
        finally:
            await client.aclose()

    try:
        flip_seconds = asyncio.run(_observe())
        # Staleness = 3 x 5s from the LAST heartbeat, which was at most ~5s before the kill:
        # the flip lands in ~10-15s; ~20s is the honest upper bound (spec: "within ~15-20s").
        assert flip_seconds <= 21.0, f"staleness flip took {flip_seconds:.1f}s"
    finally:
        _reap_tree(pid)


async def test_chat_target_runs_an_in_process_session_worker(
    kafka_bootstrap: str, topic_namespace: str, isolated_home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The §3.2 in-process session against a real broker: preflight → launch on the SHARED chat
    client → readiness → picker (the launched agent is listed) → exit narration; the worker is
    gone with the session (its tombstone lands — the agent reads offline right after)."""
    pytest.importorskip("psutil", reason="the [mesh] extra is not installed")
    from calfkit.cli._dev_agents import preflight

    agent_name = f"{topic_namespace}-general"
    spec = _write_target(tmp_path, agent_name)
    plan = preflight([spec], app_dir=str(tmp_path))

    async def _quit_reader(_prompt: str) -> str:
        return "q"

    monkeypatch.setattr("calfkit.cli._chat.make_reader", lambda _loop: _quit_reader)
    from calfkit.cli._chat import run_chat_session

    await run_chat_session(
        None,
        kafka_bootstrap,
        None,
        provision=True,
        session_plan=plan,
        session_host_key=normalize([kafka_bootstrap]).key,
    )
    out = capsys.readouterr().out
    assert agent_name in out  # the launched agent reached the picker
    assert f"✦ stopped '{agent_name}' (ran in this session)" in out

    # Atomic lifetime: the worker stopped with the session — its graceful tombstone makes the
    # agent read offline immediately (no staleness wait).
    client = Client.connect(kafka_bootstrap, mesh_config=_FAST_MESH)
    try:
        deadline = time.monotonic() + 20.0
        while agent_name in (await client.mesh.get_agents()):
            assert time.monotonic() < deadline, "the session worker's tombstone never landed"
            await asyncio.sleep(0.2)
    finally:
        await client.aclose()


def test_fresh_broker_first_run_end_to_end(isolated_home: Path, tmp_path: Path) -> None:
    """G1 from nothing (spec §1): one command spawns the bundled Tansu AND the agent daemon —
    the readiness gate absorbs the missing-presence-topic window (open_failed = not ready,
    provisioning creates the plane) — then the address-scoped teardown clears daemon and broker.

    Teardown uses ``stop NAME`` + ``broker stop`` (both scoped to this test's address) rather
    than ``ck dev down``: the down sweep is deliberately global across addresses (spec §3.4) and
    would reap daemons belonging to parallel tests — its composition is covered offline.
    """
    pytest.importorskip("psutil", reason="the [mesh] extra is not installed")
    pytest.importorskip("calfkit_mesh", reason="the [mesh] extra is not installed")

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    host = f"127.0.0.1:{port}"
    agent_name = f"fresh-{uuid.uuid4().hex[:8]}-general"
    spec = _write_target(tmp_path, agent_name)

    result = _invoke(["dev", "run", "-d", spec, "--host", host, "--app-dir", str(tmp_path)])
    pid: int | None = None
    try:
        assert result.exit_code == 0, result.stdout + str(result.exception)
        assert "managed broker" in result.stdout  # Tansu spawned first
        assert f"launched agent '{agent_name}'" in result.stdout
        pid = _pid_of(result.stdout)

        stop = _invoke(["dev", "stop", agent_name, "--host", host])
        assert stop.exit_code == 0, stop.stdout + str(stop.exception)
        assert f"stopped daemon pid {pid}" in stop.stdout

        broker_stop = _invoke(["dev", "broker", "stop", "--host", host])
        assert broker_stop.exit_code == 0, broker_stop.stdout + str(broker_stop.exception)
        assert f"stopped {host}" in broker_stop.stdout
        assert is_reachable(host, timeout=2.0) is False
    finally:
        if pid is not None:
            _reap_tree(pid)
        _invoke(["dev", "broker", "stop", "--host", host])  # belt-and-braces if the assert path bailed early
