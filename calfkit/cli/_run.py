"""Runtime glue for ``ck run``.

``serve`` is the single entrypoint that turns resolved ``module:attr`` targets
into a running :class:`~calfkit.worker.Worker`. It is deliberately a
module-level function taking only picklable arguments (strings / bools) so the
``--reload`` supervisor can hand it to ``watchfiles.run_process``, which spawns
it in a fresh process on every file change.
"""

from __future__ import annotations

import asyncio
from typing import Any

import typer

from calfkit.cli._common import _load_env, _parse_host
from calfkit.cli._loader import load_nodes
from calfkit.client._mesh_url import resolve_mesh_url


def _print_banner(nodes: list[Any], server_urls: str | list[str] | None, provision: bool, enable_idempotence: bool) -> None:
    """Print a concise startup banner describing what is being served."""
    # Resolve through the same helper Client.connect uses, so the banner reports
    # the exact broker the client connects to (its arg > $CALFKIT_MESH_URL >
    # localhost fallback), never a placeholder.
    broker = ", ".join(resolve_mesh_url(server_urls))
    typer.echo("🐮 Calfkit — starting dev worker")
    typer.echo(f"   broker: {broker}")
    typer.echo(f"   topic provisioning: {'on' if provision else 'off'}")
    typer.echo(f"   producer idempotence: {'on' if enable_idempotence else 'off'}")
    for node in nodes:
        node_id = getattr(node, "node_id", None) or repr(node)
        kind = getattr(node, "_node_kind", "node")
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
    enable_idempotence: bool = False,
) -> None:
    """Resolve targets and run them in a single Worker until stopped.

    Args:
        targets: ``module:attr`` specs; each attr is a node or iterable of nodes.
        host: ``--host`` value (see :func:`_parse_host` for precedence).
        provision: Enable experimental dev topic auto-creation.
        group_id: Optional Kafka consumer-group override (defaults per-node).
        env_file: Optional dotenv path (``./.env`` is auto-loaded otherwise).
        app_dir: Directory inserted on ``sys.path`` for import resolution.
        enable_idempotence: Turn idempotent producers on (``--enable-idempotence``).
            Off by default so the worker runs against brokers without producer-id
            support; when set it flows to ``Client.connect`` as the single knob that
            hardens every producer (broker + control-plane + fan-out writers). Absent,
            calfkit sets nothing (``enable_idempotence=None``).

    Raises:
        typer.Exit: (code 2) on a bad spec, import failure, non-node object, or
            if the targets resolve to zero nodes.
    """
    _load_env(env_file)
    nodes = load_nodes(list(targets), app_dir=app_dir)

    # Import the broker-facing modules only after the targets validate, so a
    # bad spec fails fast without paying these imports.
    from calfkit.client import Client
    from calfkit.provisioning import ProvisioningConfig
    from calfkit.worker import Worker

    server_urls = _parse_host(host)
    provisioning = ProvisioningConfig(enabled=True) if provision else None
    # The CLI flag is opt-in only: absent -> None so calfkit imposes nothing (library defaults);
    # --enable-idempotence -> True threads idempotence to every producer via the client.
    client = Client.connect(server_urls, provisioning=provisioning, enable_idempotence=True if enable_idempotence else None)
    worker = Worker(client, nodes=nodes, group_id=group_id)

    _print_banner(nodes, server_urls, provision, enable_idempotence)
    asyncio.run(worker.run())
