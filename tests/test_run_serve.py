"""Tests for the ``calfkit run`` serve entrypoint (``calfkit.cli._run.serve``).

``serve`` is the picklable function the reload supervisor spawns and the
no-reload path calls directly. These tests patch the ``Client``/``Worker``
seams so no live Kafka broker is required, and assert the wiring: node
resolution, host precedence, provisioning, and group-id passthrough.
"""

from __future__ import annotations

import pytest
import typer

_FIXTURE = "tests.provisioning_cli_nodes"


@pytest.fixture
def captured(monkeypatch: pytest.MonkeyPatch) -> dict:
    """Patch the Client/Worker seams and capture what ``serve`` wires up."""
    cap: dict = {}

    class FakeClient:
        @classmethod
        def connect(cls, server_urls: object = None, **kwargs: object) -> FakeClient:
            cap["server_urls"] = server_urls
            cap["provisioning"] = kwargs.get("provisioning")
            return cls()

    class FakeWorker:
        def __init__(self, client: object, nodes: object = None, group_id: object = None, **kwargs: object) -> None:
            cap["nodes"] = nodes
            cap["group_id"] = group_id

        async def run(self, **kwargs: object) -> None:
            cap["ran"] = True

    monkeypatch.setattr("calfkit.client.Client", FakeClient)
    monkeypatch.setattr("calfkit.worker.Worker", FakeWorker)
    return cap


def test_serve_resolves_nodes_and_runs_worker(captured: dict) -> None:
    """serve resolves the targets, builds a Worker with them, and runs it."""
    from calfkit.cli._run import serve

    serve([f"{_FIXTURE}:nodes"], host=None, provision=False, group_id=None, env_file=None, app_dir=".")

    assert [n.node_id for n in captured["nodes"]] == ["tool_alpha", "tool_beta"]
    assert captured["ran"] is True


def test_serve_no_host_delegates_to_client_default(captured: dict) -> None:
    """With no --host, serve passes server_urls=None so Client.connect applies
    its CALF_HOST_URL -> localhost fallback."""
    from calfkit.cli._run import serve

    serve([f"{_FIXTURE}:single"], host=None, provision=False, group_id=None, env_file=None, app_dir=".")

    assert captured["server_urls"] is None


def test_serve_host_flag_maps_to_server_urls(captured: dict) -> None:
    """A single --host value is forwarded to Client.connect verbatim."""
    from calfkit.cli._run import serve

    serve([f"{_FIXTURE}:single"], host="myhost:9092", provision=False, group_id=None, env_file=None, app_dir=".")

    assert captured["server_urls"] == "myhost:9092"


def test_serve_host_comma_splits_to_list(captured: dict) -> None:
    """A comma-separated --host becomes a list of bootstrap servers."""
    from calfkit.cli._run import serve

    serve([f"{_FIXTURE}:single"], host="a:9092, b:9092", provision=False, group_id=None, env_file=None, app_dir=".")

    assert captured["server_urls"] == ["a:9092", "b:9092"]


def test_serve_provision_flag_enables_provisioning(captured: dict) -> None:
    """--provision passes an enabled ProvisioningConfig; default passes None."""
    from calfkit.cli._run import serve

    serve([f"{_FIXTURE}:single"], host=None, provision=True, group_id=None, env_file=None, app_dir=".")
    assert captured["provisioning"] is not None
    assert captured["provisioning"].enabled is True


def test_serve_no_provision_passes_none(captured: dict) -> None:
    """Without --provision, no provisioning config is passed."""
    from calfkit.cli._run import serve

    serve([f"{_FIXTURE}:single"], host=None, provision=False, group_id=None, env_file=None, app_dir=".")
    assert captured["provisioning"] is None


def test_serve_group_id_passthrough(captured: dict) -> None:
    """--group-id is forwarded to the Worker."""
    from calfkit.cli._run import serve

    serve([f"{_FIXTURE}:single"], host=None, provision=False, group_id="mygroup", env_file=None, app_dir=".")
    assert captured["group_id"] == "mygroup"


def test_serve_empty_resolution_exits_2(captured: dict) -> None:
    """A target that resolves to zero nodes errors with exit 2."""
    from calfkit.cli._run import serve

    with pytest.raises(typer.Exit) as exc:
        serve([f"{_FIXTURE}:empty_list"], host=None, provision=False, group_id=None, env_file=None, app_dir=".")
    assert exc.value.exit_code == 2


def test_serve_prints_banner_with_node_id(captured: dict, capsys: pytest.CaptureFixture[str]) -> None:
    """serve prints a startup banner naming the resolved node(s)."""
    from calfkit.cli._run import serve

    serve([f"{_FIXTURE}:single"], host=None, provision=False, group_id=None, env_file=None, app_dir=".")
    out = capsys.readouterr().out
    assert "tool_echo" in out
