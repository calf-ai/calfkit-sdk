"""Tests for ``calfkit.cli.mcp`` typer commands.

Uses typer's ``CliRunner`` + monkey-patching of ``McpSession`` to avoid
spawning a real MCP subprocess. End-to-end tests against a real MCP
server live in the Phase 8 lane.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from typer.testing import CliRunner

from calfkit.cli.mcp import app
from calfkit.mcp._tool_def import McpToolDef

runner = CliRunner()


def _td(name: str = "t") -> McpToolDef:
    return McpToolDef(name=name, input_schema={"type": "object", "properties": {}})


@pytest.fixture
def patched_session(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace ``McpSession`` so the CLI doesn't spawn a real subprocess.

    Returns the mock session; tests configure its ``list_tools`` return value
    as needed.
    """
    mock_session = MagicMock()
    mock_session.initialize = AsyncMock(return_value=MagicMock())
    mock_session.list_tools = AsyncMock(return_value=[_td("search"), _td("send")])
    mock_session.aclose = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    def _mock_constructor(transport: Any, **kwargs: Any) -> MagicMock:
        return mock_session

    monkeypatch.setattr("calfkit.cli.mcp.McpSession", _mock_constructor)
    return mock_session


# ---------------------------------------------------------------------------
# CLI arg validation
# ---------------------------------------------------------------------------


def test_cli_requires_command_or_url() -> None:
    """Neither --command nor --url → error."""
    result = runner.invoke(app, ["codegen", "gmail"])
    assert result.exit_code == 2
    assert "exactly one" in result.stderr


def test_cli_rejects_both_command_and_url() -> None:
    """Both --command and --url → error (XOR)."""
    result = runner.invoke(app, ["codegen", "gmail", "--command", "x", "--url", "https://y"])
    assert result.exit_code == 2
    assert "exactly one" in result.stderr


def test_cli_rejects_empty_command(patched_session: MagicMock) -> None:
    """Empty --command after shell-split → error."""
    result = runner.invoke(app, ["codegen", "gmail", "--command", "   "])
    assert result.exit_code == 2
    assert "empty after" in result.stderr


# ---------------------------------------------------------------------------
# Happy path: stdio command
# ---------------------------------------------------------------------------


def test_cli_writes_file_for_stdio_command(patched_session: MagicMock, tmp_path: Path) -> None:
    output = tmp_path / "gmail_schemas.py"
    result = runner.invoke(
        app,
        ["codegen", "gmail", "--command", "echo 'fake mcp'", "--output", str(output)],
    )
    assert result.exit_code == 0, result.stderr
    assert output.exists()
    contents = output.read_text(encoding="utf-8")
    assert "DO NOT EDIT BY HAND" in contents
    assert "class Gmail:" in contents
    assert "SEARCH" in contents
    assert "SEND" in contents
    assert "Wrote" in result.stdout
    assert "2 tool(s)" in result.stdout


def test_cli_writes_file_for_http_url(patched_session: MagicMock, tmp_path: Path) -> None:
    output = tmp_path / "github_schemas.py"
    result = runner.invoke(
        app,
        ["codegen", "github", "--url", "https://api.github.com/mcp", "--output", str(output)],
    )
    assert result.exit_code == 0, result.stderr
    assert output.exists()
    assert "class Github:" in output.read_text(encoding="utf-8")


def test_cli_passes_token_to_http_transport(patched_session: MagicMock, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """When --token is set, it's threaded into the HttpTransport."""
    captured_transport: list[Any] = []

    def capture_constructor(transport: Any, **kwargs: Any) -> MagicMock:
        captured_transport.append(transport)
        return patched_session

    monkeypatch.setattr("calfkit.cli.mcp.McpSession", capture_constructor)

    output = tmp_path / "out.py"
    result = runner.invoke(
        app,
        ["codegen", "x", "--url", "https://api.x.com/mcp", "--token", "tok-123", "--output", str(output)],
    )
    assert result.exit_code == 0, result.stderr

    assert len(captured_transport) == 1
    transport = captured_transport[0]
    assert transport.url == "https://api.x.com/mcp"
    assert transport.token == "tok-123"


def test_cli_creates_parent_directories(patched_session: MagicMock, tmp_path: Path) -> None:
    """If --output points to a nested path, parent dirs are created."""
    output = tmp_path / "deep" / "nested" / "schemas.py"
    result = runner.invoke(app, ["codegen", "x", "--command", "echo x", "--output", str(output)])
    assert result.exit_code == 0, result.stderr
    assert output.exists()


# ---------------------------------------------------------------------------
# --check mode (drift detection)
# ---------------------------------------------------------------------------


def test_cli_check_passes_when_file_matches(patched_session: MagicMock, tmp_path: Path) -> None:
    """First write, then --check on the same content → exit 0."""
    output = tmp_path / "schemas.py"
    # First write (no --check)
    runner.invoke(app, ["codegen", "x", "--command", "echo x", "--output", str(output)])
    # Then --check — should be a no-op
    result = runner.invoke(app, ["codegen", "x", "--command", "echo x", "--output", str(output), "--check"])
    assert result.exit_code == 0
    assert "up to date" in result.stdout


def test_cli_check_fails_when_file_missing(patched_session: MagicMock, tmp_path: Path) -> None:
    """--check against a non-existent file → exit 1 with drift message."""
    output = tmp_path / "missing.py"
    result = runner.invoke(app, ["codegen", "x", "--command", "echo x", "--output", str(output), "--check"])
    assert result.exit_code == 1
    assert "does not exist" in result.stderr


def test_cli_check_fails_when_content_differs(patched_session: MagicMock, tmp_path: Path) -> None:
    output = tmp_path / "schemas.py"
    output.write_text("# stale outdated content\n", encoding="utf-8")
    result = runner.invoke(app, ["codegen", "x", "--command", "echo x", "--output", str(output), "--check"])
    assert result.exit_code == 1
    assert "Drift detected" in result.stderr
    # Diff lines should be present
    assert "+" in result.stderr or "-" in result.stderr
    # Helpful remediation hint
    assert "calfkit mcp codegen" in result.stderr


def test_cli_check_does_not_write(patched_session: MagicMock, tmp_path: Path) -> None:
    """--check must NOT overwrite the file even when drift is detected."""
    output = tmp_path / "schemas.py"
    stale = "# stale\n"
    output.write_text(stale, encoding="utf-8")
    runner.invoke(app, ["codegen", "x", "--command", "echo x", "--output", str(output), "--check"])
    assert output.read_text(encoding="utf-8") == stale  # unchanged


# ---------------------------------------------------------------------------
# Error handling: MCP session failure
# ---------------------------------------------------------------------------


def test_cli_handles_session_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """If the MCP session fails to open, exit code 2 with a clear stderr message."""
    mock_session = MagicMock()
    mock_session.__aenter__ = AsyncMock(side_effect=RuntimeError("subprocess failed to start"))
    mock_session.__aexit__ = AsyncMock(return_value=None)
    monkeypatch.setattr("calfkit.cli.mcp.McpSession", lambda transport, **kwargs: mock_session)

    output = tmp_path / "schemas.py"
    result = runner.invoke(app, ["codegen", "x", "--command", "echo x", "--output", str(output)])
    assert result.exit_code == 2
    assert "failed to talk to MCP server" in result.stderr
    assert "subprocess failed to start" in result.stderr


def test_cli_handles_initialize_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """If initialize raises after session opens, also exit 2."""
    mock_session = MagicMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)
    mock_session.initialize = AsyncMock(side_effect=RuntimeError("initialize timed out"))
    monkeypatch.setattr("calfkit.cli.mcp.McpSession", lambda transport, **kwargs: mock_session)

    output = tmp_path / "schemas.py"
    result = runner.invoke(app, ["codegen", "x", "--command", "echo x", "--output", str(output)])
    assert result.exit_code == 2
    assert "initialize timed out" in result.stderr


# ---------------------------------------------------------------------------
# Help / discoverability
# ---------------------------------------------------------------------------


def test_cli_help_lists_codegen() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "codegen" in result.stdout


def test_cli_codegen_help_shows_flags() -> None:
    result = runner.invoke(app, ["codegen", "--help"])
    assert result.exit_code == 0
    # Critical flags are documented
    assert "--command" in result.stdout
    assert "--url" in result.stdout
    assert "--token" in result.stdout
    assert "--output" in result.stdout
    assert "--check" in result.stdout
