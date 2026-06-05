"""``calfkit mcp`` typer subcommand.

Exposes two commands: ``codegen`` and ``schema``. ``codegen`` spawns an MCP
server, runs ``initialize`` + ``tools/list``, and emits a generated Python
module with :class:`McpToolDef` constants the user can import. ``schema`` emits
the reference JSON Schema for ``mcp.json`` (no MCP server needed).

Requires the ``cli`` optional extra (typer). If typer is not installed, the
import raises with a clear remediation message rather than silently failing.

Example invocations::

    calfkit mcp codegen gmail \\
        --command "npx -y @modelcontextprotocol/server-gmail" \\
        --output gmail_schemas.py

    calfkit mcp codegen github \\
        --url "https://api.github.com/mcp" \\
        --token "$GITHUB_TOKEN" \\
        --output github_schemas.py

    calfkit mcp codegen gmail \\
        --command "..." \\
        --output gmail_schemas.py \\
        --check        # CI mode: exit non-zero on drift, no write

Exit codes (v1 plan §11 Q17):
    0 — success / no drift
    1 — drift detected (``--check`` mode)
    2 — error (MCP server failed to start, file I/O failed, etc.)
"""

from __future__ import annotations

import asyncio
import difflib
import json
import shlex
from collections.abc import Callable
from pathlib import Path

try:
    import typer
except ImportError as e:  # pragma: no cover -- exercised manually
    raise ImportError("calfkit mcp codegen requires the 'cli' optional extra. Install with: pip install calfkit[cli]") from e

from calfkit.mcp._codegen import diff_modules, render_module
from calfkit.mcp._session import HttpTransport, McpSession, McpTransport, StdioTransport

app = typer.Typer(
    name="mcp",
    help="MCP (Model Context Protocol) adaptor commands.",
    no_args_is_help=True,
)


@app.callback()
def _mcp_callback() -> None:
    """Force typer into multi-command mode so 'mcp codegen' is the invocation
    pattern even when codegen is currently the only subcommand. Future
    subcommands (e.g. 'mcp inspect') will land alongside it.
    """


def _emit_text(
    rendered: str,
    output: Path,
    *,
    check: bool,
    differ: Callable[[str, str], str],
    label: str,
    refresh_cmd: str,
) -> int:
    """Write ``rendered`` to ``output`` (or, in ``check`` mode, compare without writing).

    Shared write/``--check`` tail for ``codegen`` and ``schema``. The diff is
    caller-supplied — ``codegen`` passes its AST-aware ``diff_modules`` while
    ``schema`` passes a plain ``difflib`` text diff. ``differ(rendered, existing)``
    must return an empty string when the two are equivalent.

    Returns the exit code (0 ok · 1 drift/missing · 2 io-error).
    """
    if check:
        if not output.exists():
            typer.echo(f"Drift: {output} does not exist (would create with {label})", err=True)
            return 1
        try:
            existing = output.read_text(encoding="utf-8")
        except OSError as e:
            typer.echo(f"Error: cannot read {output}: {e}", err=True)
            return 2
        diff = differ(rendered, existing)
        if diff:
            typer.echo(f"Drift detected in {output}:", err=True)
            typer.echo(diff, err=True)
            typer.echo(f"Re-run without --check to refresh: {refresh_cmd}", err=True)
            return 1
        typer.echo(f"OK: {output} is up to date ({label}).")
        return 0
    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8")
    except OSError as e:
        typer.echo(f"Error: cannot write {output}: {e}", err=True)
        return 2
    typer.echo(f"Wrote {output} ({label}).")
    return 0


# Public for tests — let test code call codegen logic directly without
# invoking the CLI runner (e.g. when verifying the renderer + the
# subprocess-spawn glue together but skipping typer-arg parsing).
async def _generate_and_write(
    *,
    server_name: str,
    transport: McpTransport,
    output: Path,
    check: bool,
    source_str: str,
) -> int:
    """Open the MCP server, render the module, write or check.

    Returns the appropriate exit code (0/1/2). Used by both the typer
    command and tests.
    """
    # Open the MCP session (spawn subprocess for stdio, open HTTP for http)
    try:
        async with McpSession(transport) as session:
            await session.initialize()
            tools = await session.list_tools()
    except Exception as e:
        typer.echo(f"Error: failed to talk to MCP server: {e}", err=True)
        return 2

    rendered = render_module(server_name=server_name, tools=tools, source=source_str)

    return _emit_text(
        rendered,
        output,
        check=check,
        differ=diff_modules,
        label=f"{len(tools)} tool(s)",
        refresh_cmd=f"calfkit mcp codegen {server_name} ...",
    )


@app.command()
def codegen(
    name: str = typer.Argument(..., help="Logical server name (used as the class name in the generated module)."),
    command: str | None = typer.Option(
        None,
        "--command",
        help="Shell command to spawn the MCP server via stdio. Example: 'npx -y @mcp/server-gmail'.",
    ),
    url: str | None = typer.Option(
        None,
        "--url",
        help="Streamable HTTP URL of the MCP server. Mutually exclusive with --command.",
    ),
    token: str | None = typer.Option(
        None,
        "--token",
        help="HTTP bearer token (only with --url). Sugar for Authorization: Bearer <token>.",
    ),
    output: Path = typer.Option(
        Path("schemas.py"),
        "--output",
        "-o",
        help="Path to write the generated module. Parent directories are created.",
    ),
    check: bool = typer.Option(
        False,
        "--check",
        help="Exit non-zero if the generated module would differ from the existing file. Does not write.",
    ),
) -> None:
    """Generate ``McpToolDef`` schemas from a running MCP server.

    Either ``--command`` (stdio) or ``--url`` (HTTP) must be supplied.
    """
    if (command is None) == (url is None):
        typer.echo("Error: exactly one of --command or --url is required.", err=True)
        raise typer.Exit(2)

    transport: McpTransport
    source_str: str
    if command is not None:
        # shlex split for proper quoting handling
        parts = shlex.split(command)
        if not parts:
            typer.echo("Error: --command is empty after shell-splitting.", err=True)
            raise typer.Exit(2)
        transport = StdioTransport(command=parts[0], args=tuple(parts[1:]))
        source_str = f"stdio: {command}"
    else:
        assert url is not None  # narrowed by the XOR guard
        transport = HttpTransport(url=url, token=token)
        source_str = f"http: {url}"

    exit_code = asyncio.run(
        _generate_and_write(
            server_name=name,
            transport=transport,
            output=output,
            check=check,
            source_str=source_str,
        )
    )
    if exit_code != 0:
        raise typer.Exit(exit_code)


@app.command()
def schema(
    output: Path = typer.Option(
        Path("calfkit/mcp/mcp.schema.json"),
        "--output",
        "-o",
        help="Path to write the reference schema. Default targets the repo checkout (not an installed package).",
    ),
    check: bool = typer.Option(
        False,
        "--check",
        help="Exit non-zero if the file would differ from the existing one. Does not write.",
    ),
) -> None:
    """Emit the reference JSON Schema for mcp.json (no MCP server needed)."""
    from calfkit.mcp import mcp_json_schema

    rendered = json.dumps(mcp_json_schema(), indent=2) + "\n"

    def _text_diff(expected: str, actual: str) -> str:
        return "".join(
            difflib.unified_diff(
                actual.splitlines(keepends=True),
                expected.splitlines(keepends=True),
                fromfile="existing",
                tofile="generated",
            )
        )

    code = _emit_text(
        rendered,
        output,
        check=check,
        differ=_text_diff,
        label="reference schema",
        refresh_cmd="calfkit mcp schema",
    )
    if code != 0:
        raise typer.Exit(code)


def main() -> None:
    """Entry point for direct invocation via ``python -m calfkit.cli.mcp``."""
    app()


if __name__ == "__main__":  # pragma: no cover
    main()


# Re-export the typer app so callers (including the future top-level
# ``calfkit`` script entry in Phase 6) can mount it.
__all__ = ["app", "main", "_generate_and_write"]
