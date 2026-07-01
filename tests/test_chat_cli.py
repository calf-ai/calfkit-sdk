"""CliRunner smoke tests for the ``ck chat`` command (no-reader paths only).

``CliRunner``'s ``BytesIO`` stdin has no usable ``fileno()``, so these exercise
only the paths that never read a line: ``--help`` and the mocked-mesh offline-name
``Exit(2)``. The interactive reader/transcript is covered by the unit/integration
tests in ``test_chat_io`` / ``test_chat_session``.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

import pytest
from typer.testing import CliRunner

from calfkit.client.mesh import AgentInfo, Mesh
from calfkit.exceptions import MeshUnavailableError

_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    return _ANSI.sub("", text)


def test_chat_mounted_with_options() -> None:
    from calfkit.cli import _build_app

    result = CliRunner().invoke(_build_app(), ["chat", "--help"])
    assert result.exit_code == 0, result.stdout
    out = _plain(result.stdout)
    assert "--host" in out
    assert "--timeout" in out
    assert "--env-file" in out
    assert "--provision" in out


def test_chat_provision_flag_forwarded_to_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """--provision reaches run_chat_session as True; absent it is False (opt-in)."""
    from calfkit.cli import _build_app

    cap: dict = {}

    async def fake_session(name: object, server_urls: object, timeout: object, provision: bool = False) -> None:
        cap["provision"] = provision

    monkeypatch.setattr("calfkit.cli._chat.run_chat_session", fake_session)

    result_on = CliRunner().invoke(_build_app(), ["chat", "--provision"])
    assert result_on.exit_code == 0, result_on.stdout + result_on.stderr
    assert cap["provision"] is True

    result_off = CliRunner().invoke(_build_app(), ["chat"])
    assert result_off.exit_code == 0, result_off.stdout + result_off.stderr
    assert cap["provision"] is False


def test_chat_offline_name_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    from calfkit.cli import _build_app

    async def _fake(_self: Mesh) -> dict[str, AgentInfo]:
        return {"helpbot": AgentInfo(name="helpbot", description="x", last_seen=datetime(2026, 1, 1, tzinfo=timezone.utc))}

    monkeypatch.setattr(Mesh, "get_agents", _fake)
    result = CliRunner().invoke(_build_app(), ["chat", "missing"])
    assert result.exit_code == 2, result.stdout + result.stderr
    assert "is not online" in _plain(result.stdout + result.stderr)


def test_chat_mesh_unavailable_exits_2_with_reason_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    from calfkit.cli import _build_app

    async def _raise(_self: Mesh) -> dict[str, AgentInfo]:
        raise MeshUnavailableError("down", reason="open_failed")

    monkeypatch.setattr(Mesh, "get_agents", _raise)
    result = CliRunner().invoke(_build_app(), ["chat"])
    assert result.exit_code == 2, result.stdout + result.stderr
    out = _plain(result.stdout + result.stderr)
    assert "mesh unavailable (open_failed)" in out
    assert "no agents/tools are online" in out  # the reason-aware hint


def test_chat_session_value_error_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    # A ValueError from the running session (e.g. a pydantic ValidationError, which subclasses
    # ValueError) is a real bug — it must NOT be masked as a clean config-error Exit(2).
    from calfkit.cli import _build_app

    async def _raise(*_a: object, **_k: object) -> None:
        raise ValueError("a real bug")

    monkeypatch.setattr("calfkit.cli._chat.run_chat_session", _raise)
    result = CliRunner().invoke(_build_app(), ["chat"])
    assert isinstance(result.exception, ValueError)  # propagated, not swallowed
    assert result.exit_code != 2


def test_chat_config_value_error_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    # A ValueError while parsing config (the defensive --host arm) is a clean Exit(2), no traceback.
    from calfkit.cli import _build_app

    def _raise(_host: object) -> None:
        raise ValueError("bad host value")

    monkeypatch.setattr("calfkit.cli.chat._parse_host", _raise)
    result = CliRunner().invoke(_build_app(), ["chat"])
    assert result.exit_code == 2, result.stdout + result.stderr
    assert "Error: bad host value" in _plain(result.stdout + result.stderr)


def test_chat_keyboard_interrupt_exits_0(monkeypatch: pytest.MonkeyPatch) -> None:
    from calfkit.cli import _build_app

    async def _raise(*_a: object, **_k: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr("calfkit.cli._chat.run_chat_session", _raise)
    result = CliRunner().invoke(_build_app(), ["chat"])
    assert result.exit_code == 0
