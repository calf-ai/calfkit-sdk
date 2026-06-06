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


def test_serve_prints_banner_with_node_details(captured: dict, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    """The banner names the node, its kind, its topics, provisioning state, and
    the effective broker (localhost when --host and CALF_HOST_URL are unset)."""
    from calfkit.cli._run import serve

    monkeypatch.delenv("CALF_HOST_URL", raising=False)
    serve([f"{_FIXTURE}:single"], host=None, provision=False, group_id=None, env_file=None, app_dir=".")
    out = capsys.readouterr().out
    assert "tool_echo" in out
    assert "[tool]" in out
    assert "echo.in" in out  # subscribe topic
    assert "echo.out" in out  # publish topic
    assert "provisioning: off" in out
    assert "localhost" in out  # effective broker fallback


def test_serve_banner_shows_mcp_server(captured: dict, capsys: pytest.CaptureFixture[str]) -> None:
    """A directly-served McpServer renders in the banner with the [mcp] kind."""
    from calfkit.cli._run import serve

    serve([f"{_FIXTURE}:solo_mcp"], host=None, provision=False, group_id=None, env_file=None, app_dir=".")
    out = capsys.readouterr().out
    assert "solo_mcp" in out
    assert "[mcp]" in out


def test_serve_banner_shows_env_host_when_no_flag(captured: dict, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    """With no --host, the banner reflects CALF_HOST_URL (the effective broker)."""
    from calfkit.cli._run import serve

    monkeypatch.setenv("CALF_HOST_URL", "envhost:9092")
    serve([f"{_FIXTURE}:single"], host=None, provision=False, group_id=None, env_file=None, app_dir=".")
    out = capsys.readouterr().out
    assert "envhost:9092" in out


# ---------------------------------------------------------------------------
# _parse_host
# ---------------------------------------------------------------------------


def test_parse_host_none_returns_none() -> None:
    from calfkit.cli._run import _parse_host

    assert _parse_host(None) is None


def test_parse_host_empty_returns_none() -> None:
    from calfkit.cli._run import _parse_host

    assert _parse_host("") is None


def test_parse_host_separators_only_returns_none() -> None:
    """A non-empty value that strips to nothing falls back to None (so env/
    localhost still win), not an empty/garbage server list."""
    from calfkit.cli._run import _parse_host

    assert _parse_host(" , ") is None


def test_parse_host_single_returns_str() -> None:
    from calfkit.cli._run import _parse_host

    assert _parse_host("h:9092") == "h:9092"


def test_parse_host_comma_returns_list() -> None:
    from calfkit.cli._run import _parse_host

    assert _parse_host("a:9092, b:9092") == ["a:9092", "b:9092"]


# ---------------------------------------------------------------------------
# _load_env
# ---------------------------------------------------------------------------


def test_load_env_explicit_file_is_loaded(monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> None:
    """An explicit --env-file that exists is passed to load_dotenv."""
    import dotenv

    import calfkit.cli._run as run_mod

    env_path = tmp_path / "custom.env"  # type: ignore[operator]
    env_path.write_text("FOO=bar\n")
    calls: list[str] = []
    monkeypatch.setattr(dotenv, "load_dotenv", lambda p: calls.append(p) or True)

    run_mod._load_env(str(env_path))
    assert calls == [str(env_path)]


def test_load_env_missing_explicit_file_warns_and_skips(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """A missing explicit --env-file warns (not silent) and does not load."""
    import dotenv

    import calfkit.cli._run as run_mod

    calls: list[str] = []
    monkeypatch.setattr(dotenv, "load_dotenv", lambda p: calls.append(p) or True)

    run_mod._load_env("/no/such/file.env")
    assert calls == []
    assert "not found" in capsys.readouterr().err


def test_load_env_autoloads_dotenv_when_present(monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> None:
    """With no explicit file, ./.env is auto-loaded when it exists."""
    import dotenv

    import calfkit.cli._run as run_mod

    monkeypatch.chdir(tmp_path)  # type: ignore[arg-type]
    (tmp_path / ".env").write_text("FOO=bar\n")  # type: ignore[operator]
    calls: list[str] = []
    monkeypatch.setattr(dotenv, "load_dotenv", lambda p: calls.append(p) or True)

    run_mod._load_env(None)
    assert calls == [".env"]


def test_load_env_no_file_no_autoload(monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> None:
    """With no explicit file and no ./.env, load_dotenv is not called."""
    import dotenv

    import calfkit.cli._run as run_mod

    monkeypatch.chdir(tmp_path)  # type: ignore[arg-type]
    calls: list[str] = []
    monkeypatch.setattr(dotenv, "load_dotenv", lambda p: calls.append(p) or True)

    run_mod._load_env(None)
    assert calls == []
