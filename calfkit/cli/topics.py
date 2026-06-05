"""``calfkit topics`` typer subcommand.

Currently exposes one command: ``provision``. Resolves the Kafka topics a set
of nodes reference and best-effort creates them via the admin client — a
**development convenience** for getting a local/CI broker into the right shape
without hand-rolling ``kafka-topics.sh`` invocations.

Requires the ``cli`` optional extra (typer). If typer is not installed, the
import raises with a clear remediation message rather than silently failing.

Example invocations::

    calfkit topics provision \\
        --nodes myapp.workers:all_nodes \\
        --bootstrap-servers localhost:9092

    calfkit topics provision \\
        --nodes myapp.workers:agent \\
        --nodes myapp.workers:tool \\
        --partitions 3 --replication-factor 3

    calfkit topics provision \\
        --nodes myapp.workers:all_nodes \\
        --dry-run          # resolve + print the topic set, create nothing

Exit codes:
    0 — success / dry-run
    2 — error (node resolution failed, or a topic could not be provisioned)
"""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Iterable
from typing import Any

try:
    import typer
except ImportError as e:  # pragma: no cover -- exercised manually
    raise ImportError("calfkit topics provision requires the 'cli' optional extra. Install with: pip install calfkit[cli]") from e

app = typer.Typer(
    name="topics",
    help="Kafka topic management commands.",
    no_args_is_help=True,
)


@app.callback()
def _topics_callback() -> None:
    """Force typer into multi-command mode so 'topics provision' is the
    invocation pattern even when provision is currently the only subcommand.
    Future subcommands will land alongside it.
    """


def _resolve_nodes(specs: list[str]) -> list[Any]:
    """Resolve ``module:attr`` specs into a flat list of node objects.

    Each ``attr`` may be a single node or an iterable of nodes. Results are
    concatenated in the order the ``--nodes`` flags were supplied.

    Raises:
        typer.Exit: (code 2) on a malformed spec or import/attribute failure.
    """
    resolved: list[Any] = []
    for spec in specs:
        if ":" not in spec:
            typer.echo(
                f"Error: --nodes must be in 'module:attr' form, got {spec!r}.",
                err=True,
            )
            raise typer.Exit(2)
        module_path, _, attr = spec.partition(":")
        if not module_path or not attr:
            typer.echo(
                f"Error: --nodes must be in 'module:attr' form, got {spec!r}.",
                err=True,
            )
            raise typer.Exit(2)
        try:
            module = importlib.import_module(module_path)
        except Exception as e:  # noqa: BLE001 -- surface any import failure
            typer.echo(f"Error: cannot import module {module_path!r}: {e}", err=True)
            raise typer.Exit(2) from e
        try:
            obj = getattr(module, attr)
        except AttributeError as e:
            typer.echo(f"Error: module {module_path!r} has no attribute {attr!r}.", err=True)
            raise typer.Exit(2) from e

        # A single node has subscribe_topics (str-keyed) but is not itself a
        # plain iterable of nodes. Strings are iterable too, so treat str/bytes
        # as scalars. Everything else iterable is expanded.
        if isinstance(obj, (str, bytes)) or not isinstance(obj, Iterable):
            resolved.append(obj)
        else:
            resolved.extend(obj)
    return resolved


def _partition_nodes(objs: list[Any]) -> list[Any]:
    """Drop ``McpServer`` entries with a clear note, returning the rest.

    MCP topics are derived from a *live* MCP session (tools are only knowable
    after ``initialize``), so they are provisioned at worker startup — not by
    this static command. Skipping them here, loudly, avoids a confusing crash
    on ``McpServer.subscribe_topics`` (which it does not expose).
    """
    from calfkit.mcp._server import McpServer

    nodes: list[Any] = []
    for obj in objs:
        if isinstance(obj, McpServer):
            typer.echo(
                f"Note: skipping MCP server {obj.raw_name!r}. MCP topics are "
                "provisioned at worker startup (from the live MCP session), "
                "not by 'topics provision'.",
                err=True,
            )
            continue
        nodes.append(obj)
    return nodes


@app.command()
def provision(
    nodes: list[str] = typer.Option(
        ...,
        "--nodes",
        help="Node source as 'module:attr' (a node or an iterable of nodes). Repeatable.",
    ),
    bootstrap_servers: str = typer.Option(
        "localhost",
        "--bootstrap-servers",
        help="Kafka bootstrap server URL(s), comma-separated.",
    ),
    partitions: int = typer.Option(
        1,
        "--partitions",
        help="Partition count for every newly-created data topic.",
    ),
    replication_factor: int = typer.Option(
        1,
        "--replication-factor",
        help="Replication factor for every newly-created topic. rf=1 is NOT durable.",
    ),
    timeout_ms: int = typer.Option(
        30000,
        "--timeout-ms",
        help="Overall budget (ms) for the provisioning operation.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Resolve and print the topic set without contacting Kafka (no admin client).",
    ),
) -> None:
    """Provision every Kafka topic referenced by the given nodes.

    Resolves ``--nodes module:attr`` specs, computes the full topic set
    (subscribe inboxes, framework return inboxes, publish topics, and agent
    tool inputs), then best-effort creates them. ``McpServer`` entries are
    skipped with a note — their topics are provisioned at worker startup.

    This is a **development convenience** (no ACLs; ``--replication-factor 1``
    is the default and is not durable). In production, topics are typically
    ops-governed. See ``calfkit.provisioning.ProvisioningConfig`` for caveats.
    """
    from calfkit.provisioning import (
        ProvisioningConfig,
        TopicProvisioner,
        TopicProvisioningError,
        topics_for_nodes,
    )

    resolved = _resolve_nodes(nodes)
    node_list = _partition_nodes(resolved)

    topics = topics_for_nodes(node_list)
    framework_topics = {node._return_topic for node in node_list}

    if not topics:
        typer.echo("No topics to provision (no nodes resolved).")
        return

    typer.echo(f"Resolved {len(topics)} topic(s):")
    for topic in topics:
        typer.echo(f"  {topic}")

    if dry_run:
        typer.echo("Dry run: no topics created.")
        return

    config = ProvisioningConfig(
        enabled=True,
        num_partitions=partitions,
        replication_factor=replication_factor,
        create_timeout_ms=timeout_ms,
    )
    server_urls = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
    provisioner = TopicProvisioner(
        bootstrap_servers=server_urls if len(server_urls) > 1 else server_urls[0],
        config=config,
    )

    try:
        report = asyncio.run(provisioner.provision(topics, framework_topics=framework_topics))
    except TopicProvisioningError as e:
        typer.echo(f"Error: topic provisioning failed: {e}", err=True)
        raise typer.Exit(2) from e

    typer.echo(f"Provisioned: {len(report.created)} created, {len(report.existing)} already existed, {len(report.unauthorized)} unauthorized.")
    if report.created:
        typer.echo(f"  created: {', '.join(report.created)}")
    if report.existing:
        typer.echo(f"  existing: {', '.join(report.existing)}")
    if report.unauthorized:
        typer.echo(f"  unauthorized (NOT created; producers/consumers will stall): {', '.join(report.unauthorized)}")


def main() -> None:
    """Entry point for direct invocation via ``python -m calfkit.cli.topics``."""
    app()


if __name__ == "__main__":  # pragma: no cover
    main()


# Re-export the typer app so the top-level ``calfkit`` script can mount it.
__all__ = ["app", "main"]
