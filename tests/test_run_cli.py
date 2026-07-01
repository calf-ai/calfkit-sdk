"""Tests for the ``ck run`` typer command and its top-level mounting.

The ``serve`` engine is patched out (it would block on a real broker), so
these tests assert the command's dispatch: no-reload calls ``serve`` directly,
``--reload`` hands ``serve`` to ``watchfiles.run_process``, bad specs exit 2,
and Ctrl-C exits cleanly.
"""

from __future__ import annotations

import os
import re

import pytest
from typer.testing import CliRunner

_ANSI_SGR = re.compile(r"\x1b\[[0-9;]*m")


def _plain(s: str) -> str:
    """Strip ANSI SGR escape codes so substring assertions survive styling."""
    return _ANSI_SGR.sub("", s)


def test_run_mounted_on_top_level_cli() -> None:
    """The top-level ``calfkit`` app mounts the ``run`` command with its flags."""
    from calfkit.cli import _build_app

    result = CliRunner().invoke(_build_app(), ["run", "--help"])
    assert result.exit_code == 0, result.stdout
    out = _plain(result.stdout)
    assert "--reload" in out
    assert "--host" in out
    assert "--provision" in out
    assert "--enable-idempotence" in out


def test_run_no_reload_calls_serve(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without --reload, the command calls serve() directly with parsed args."""
    import calfkit.cli.run as run_mod
    from calfkit.cli import _build_app

    cap: dict = {}

    def fake_serve(
        targets: list[str],
        host: str | None,
        provision: bool,
        group_id: str | None,
        env_file: str | None,
        app_dir: str,
        enable_idempotence: bool = False,
    ) -> None:
        cap.update(targets=targets, host=host, provision=provision, group_id=group_id, env_file=env_file, app_dir=app_dir)

    monkeypatch.setattr(run_mod, "serve", fake_serve)

    result = CliRunner().invoke(_build_app(), ["run", "mymod:agent"])
    assert result.exit_code == 0, result.stdout + result.stderr
    assert cap["targets"] == ["mymod:agent"]
    assert cap["host"] is None
    assert cap["provision"] is False
    assert cap["group_id"] is None
    assert os.path.isabs(cap["app_dir"])


def test_run_forwards_flags_to_serve(monkeypatch: pytest.MonkeyPatch) -> None:
    """--host / --provision / --group-id reach serve()."""
    import calfkit.cli.run as run_mod
    from calfkit.cli import _build_app

    cap: dict = {}

    def fake_serve(
        targets: list[str],
        host: str | None,
        provision: bool,
        group_id: str | None,
        env_file: str | None,
        app_dir: str,
        enable_idempotence: bool = False,
    ) -> None:
        cap.update(host=host, provision=provision, group_id=group_id)

    monkeypatch.setattr(run_mod, "serve", fake_serve)

    result = CliRunner().invoke(
        _build_app(),
        ["run", "mymod:agent", "--host", "h:9092", "--provision", "--group-id", "g"],
    )
    assert result.exit_code == 0, result.stdout + result.stderr
    assert cap["host"] == "h:9092"
    assert cap["provision"] is True
    assert cap["group_id"] == "g"


def test_run_reload_hands_serve_to_watchfiles(monkeypatch: pytest.MonkeyPatch) -> None:
    """--reload runs serve via watchfiles.run_process (the supervisor), not inline."""
    import watchfiles

    import calfkit.cli._run as run_engine
    from calfkit.cli import _build_app

    cap: dict = {}

    def fake_run_process(*paths: str, target: object = None, args: object = None, watch_filter: object = None, **kwargs: object) -> None:
        cap.update(paths=paths, target=target, args=args, watch_filter=watch_filter)

    monkeypatch.setattr(watchfiles, "run_process", fake_run_process)

    # A real, resolvable target: the parent pre-flights (imports + validates) it
    # before handing off to the supervisor.
    result = CliRunner().invoke(_build_app(), ["run", "tests.provisioning_cli_nodes:single", "--reload"])
    assert result.exit_code == 0, result.stdout + result.stderr
    # The supervised target is the real serve engine; targets ride in args[0].
    assert cap["target"] is run_engine.serve
    assert cap["args"][0] == ["tests.provisioning_cli_nodes:single"]
    # Default watch dir is the cwd.
    assert os.getcwd() in cap["paths"]


def test_run_reload_watch_dir_override(monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> None:
    """--reload-dir overrides the watched directory."""
    import watchfiles

    from calfkit.cli import _build_app

    cap: dict = {}

    def fake_run_process(*paths: str, **kwargs: object) -> None:
        cap["paths"] = paths

    monkeypatch.setattr(watchfiles, "run_process", fake_run_process)

    watched = str(tmp_path)  # type: ignore[arg-type]
    result = CliRunner().invoke(_build_app(), ["run", "tests.provisioning_cli_nodes:single", "--reload", "--reload-dir", watched])
    assert result.exit_code == 0, result.stdout + result.stderr
    assert cap["paths"] == (watched,)


def test_run_reload_bad_target_exits_2_before_supervisor(monkeypatch: pytest.MonkeyPatch) -> None:
    """Under --reload a broken target fails fast (exit 2) and the supervisor
    (watchfiles.run_process) is never started — no idle watcher around a child
    that never came up."""
    import watchfiles

    from calfkit.cli import _build_app

    cap = {"called": False}

    def fake_run_process(*paths: str, **kwargs: object) -> None:
        cap["called"] = True

    monkeypatch.setattr(watchfiles, "run_process", fake_run_process)

    result = CliRunner().invoke(_build_app(), ["run", "no_colon_here", "--reload"])
    assert result.exit_code == 2
    assert "module:attr" in _plain(result.stderr)
    assert cap["called"] is False


def test_run_multiple_targets_forwarded_to_serve(monkeypatch: pytest.MonkeyPatch) -> None:
    """The variadic positional collects every target and forwards the full list."""
    import calfkit.cli.run as run_mod
    from calfkit.cli import _build_app

    cap: dict = {}

    def fake_serve(
        targets: list[str],
        host: str | None,
        provision: bool,
        group_id: str | None,
        env_file: str | None,
        app_dir: str,
        enable_idempotence: bool = False,
    ) -> None:
        cap["targets"] = targets

    monkeypatch.setattr(run_mod, "serve", fake_serve)

    result = CliRunner().invoke(_build_app(), ["run", "a:x", "b:y", "c:z"])
    assert result.exit_code == 0, result.stdout + result.stderr
    assert cap["targets"] == ["a:x", "b:y", "c:z"]


def test_run_custom_app_dir_forwarded_to_serve(monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> None:
    """--app-dir is absolutised and forwarded to serve."""
    import calfkit.cli.run as run_mod
    from calfkit.cli import _build_app

    cap: dict = {}

    def fake_serve(
        targets: list[str],
        host: str | None,
        provision: bool,
        group_id: str | None,
        env_file: str | None,
        app_dir: str,
        enable_idempotence: bool = False,
    ) -> None:
        cap["app_dir"] = app_dir

    monkeypatch.setattr(run_mod, "serve", fake_serve)

    target_dir = str(tmp_path)  # type: ignore[arg-type]
    result = CliRunner().invoke(_build_app(), ["run", "m:a", "--app-dir", target_dir])
    assert result.exit_code == 0, result.stdout + result.stderr
    assert cap["app_dir"] == os.path.abspath(target_dir)


def test_run_enable_idempotence_flag_forwarded_to_serve(monkeypatch: pytest.MonkeyPatch) -> None:
    """--enable-idempotence reaches serve() as True; absent it is False (opt-in)."""
    import calfkit.cli.run as run_mod
    from calfkit.cli import _build_app

    cap: dict = {}

    def fake_serve(
        targets: list[str],
        host: str | None,
        provision: bool,
        group_id: str | None,
        env_file: str | None,
        app_dir: str,
        enable_idempotence: bool = False,
    ) -> None:
        cap["enable_idempotence"] = enable_idempotence

    monkeypatch.setattr(run_mod, "serve", fake_serve)

    result_on = CliRunner().invoke(_build_app(), ["run", "mymod:agent", "--enable-idempotence"])
    assert result_on.exit_code == 0, result_on.stdout + result_on.stderr
    assert cap["enable_idempotence"] is True

    result_off = CliRunner().invoke(_build_app(), ["run", "mymod:agent"])
    assert result_off.exit_code == 0, result_off.stdout + result_off.stderr
    assert cap["enable_idempotence"] is False


def test_run_bad_spec_exits_2() -> None:
    """A target without 'module:attr' form errors with exit 2 before serving."""
    from calfkit.cli import _build_app

    result = CliRunner().invoke(_build_app(), ["run", "no_colon_here"])
    assert result.exit_code == 2
    assert "module:attr" in _plain(result.stderr)


def test_run_keyboard_interrupt_exits_0(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ctrl-C during a no-reload run shuts down cleanly (exit 0)."""
    import calfkit.cli.run as run_mod
    from calfkit.cli import _build_app

    def boom(*args: object, **kwargs: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(run_mod, "serve", boom)

    result = CliRunner().invoke(_build_app(), ["run", "mymod:agent"])
    assert result.exit_code == 0
