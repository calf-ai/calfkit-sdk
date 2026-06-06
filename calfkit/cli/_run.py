"""Runtime glue for ``calfkit run``.

``serve`` is the single entrypoint that turns resolved ``module:attr`` targets
into a running :class:`~calfkit.worker.Worker`. It is deliberately a
module-level function taking only picklable arguments (strings / bools) so the
``--reload`` supervisor can hand it to ``watchfiles.run_process``, which spawns
it in a fresh process on every file change.

Requires the ``cli`` optional extra (typer). If typer is not installed, the
import raises with a clear remediation message rather than silently failing.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

try:
    import typer
except ImportError as e:  # pragma: no cover -- exercised manually
    raise ImportError("the calfkit CLI requires the 'cli' optional extra. Install with: pip install calfkit[cli]") from e

from calfkit.cli._loader import dedupe_by_node_id, resolve_specs, validate_nodes


def _load_env(env_file: str | None) -> None:
    """Load environment variables from a dotenv file.

    An explicit ``env_file`` is loaded as given; otherwise ``./.env`` is loaded
    if present. A development convenience so ``OPENAI_API_KEY`` and friends are
    available without exporting them by hand.
    """
    from dotenv import load_dotenv

    if env_file:
        load_dotenv(env_file)
    elif os.path.exists(".env"):
        load_dotenv(".env")


def _parse_host(host: str | None) -> str | list[str] | None:
    """Map the ``--host`` flag to a ``server_urls`` value for ``Client.connect``.

    ``None`` (flag omitted) is passed through unchanged so ``Client.connect``
    applies its ``CALF_HOST_URL`` → ``localhost`` fallback — preserving the
    flag > env > localhost precedence. A comma-separated value becomes a list.
    """
    if not host:
        return None
    parts = [s.strip() for s in host.split(",") if s.strip()]
    if not parts:
        return None
    return parts if len(parts) > 1 else parts[0]


def _print_banner(nodes: list[Any], server_urls: str | list[str] | None, provision: bool) -> None:
    """Print a concise startup banner describing what is being served."""
    typer.echo("🐮 Calfkit — starting dev worker")
    typer.echo(f"   broker: {server_urls if server_urls else '$CALF_HOST_URL or localhost'}")
    typer.echo(f"   topic provisioning: {'on' if provision else 'off'}")
    for node in nodes:
        node_id = getattr(node, "node_id", None) or getattr(node, "raw_name", repr(node))
        kind = getattr(node, "_node_kind", "mcp")
        line = f"   • {node_id} [{kind}]"
        subscribe = getattr(node, "subscribe_topics", None)
        if subscribe:
            line += f"  subscribe={list(subscribe)}"
        publish = getattr(node, "publish_topic", None)
        if publish:
            line += f"  publish={publish}"
        typer.echo(line)


def serve(
    targets: list[str],
    host: str | None,
    provision: bool,
    group_id: str | None,
    env_file: str | None,
    app_dir: str,
) -> None:
    """Resolve targets and run them in a single Worker until stopped.

    Args:
        targets: ``module:attr`` specs; each attr is a node or iterable of nodes.
        host: ``--host`` value (see :func:`_parse_host` for precedence).
        provision: Enable experimental dev topic auto-creation.
        group_id: Optional Kafka consumer-group override (defaults per-node).
        env_file: Optional dotenv path (``./.env`` is auto-loaded otherwise).
        app_dir: Directory inserted on ``sys.path`` for import resolution.

    Raises:
        typer.Exit: (code 2) on a bad spec, import failure, non-node object, or
            if the targets resolve to zero nodes.
    """
    from calfkit.client import Client
    from calfkit.provisioning import ProvisioningConfig
    from calfkit.worker import Worker

    _load_env(env_file)

    objs = resolve_specs(list(targets), app_dir=app_dir)
    validate_nodes(objs)
    nodes = dedupe_by_node_id(objs)
    if not nodes:
        typer.echo("Error: no nodes resolved from the given target(s).", err=True)
        raise typer.Exit(2)

    server_urls = _parse_host(host)
    provisioning = ProvisioningConfig(enabled=True) if provision else None
    client = Client.connect(server_urls, provisioning=provisioning)
    worker = Worker(client, nodes=nodes, group_id=group_id)

    _print_banner(nodes, server_urls, provision)
    asyncio.run(worker.run())
