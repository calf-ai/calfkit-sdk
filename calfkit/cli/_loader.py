"""Shared node loader for the calfkit CLI.

Resolves ``module:attr`` specs into a flat list of node objects. Used by both
``calfkit topics provision`` (``--nodes module:attr``) and ``calfkit run``
(positional ``module:attr`` targets).

Each ``attr`` may resolve to a single node or an iterable of nodes; iterables
are expanded. ``McpServer`` instances are treated as scalars even though they
are iterable (their ``__iter__`` yields tool defs) so a directly-resolved MCP
server is preserved rather than splatted.

Requires the ``cli`` optional extra (typer). If typer is not installed, the
import raises with a clear remediation message rather than silently failing.
"""

from __future__ import annotations

import importlib
import os
import sys
from collections.abc import Iterable
from typing import Any

try:
    import typer
except ImportError as e:  # pragma: no cover -- exercised manually
    raise ImportError("the calfkit CLI requires the 'cli' optional extra. Install with: pip install \"calfkit[cli]\"") from e


def resolve_specs(specs: list[str], *, app_dir: str | None = None, source_label: str = "target") -> list[Any]:
    """Resolve ``module:attr`` specs into a flat list of resolved objects.

    Each ``attr`` may be a single object or an iterable of objects; iterables
    (other than ``str``/``bytes`` and ``McpServer``) are expanded. Results are
    concatenated in the order the specs were supplied.

    Args:
        specs: ``module:attr`` strings.
        app_dir: Optional directory inserted at ``sys.path[0]`` before importing
            so targets are resolvable relative to it (e.g. the project root the
            user runs the command from). The path is absolutised; duplicates are
            not re-inserted.
        source_label: How to name the spec source in error messages (e.g.
            ``"target"`` for ``calfkit run`` or ``"--nodes"`` for
            ``calfkit topics provision``).

    Raises:
        typer.Exit: (code 2) on a malformed spec, an import failure, or a
            missing attribute.
    """
    from calfkit.mcp._server import McpServer

    if app_dir is not None:
        abs_dir = os.path.abspath(app_dir)
        if abs_dir not in sys.path:
            sys.path.insert(0, abs_dir)

    resolved: list[Any] = []
    for spec in specs:
        module_path, sep, attr = spec.partition(":")
        if not sep or not module_path or not attr:
            typer.echo(
                f"Error: {source_label} must be in 'module:attr' form, got {spec!r}.",
                err=True,
            )
            raise typer.Exit(2)
        try:
            module = importlib.import_module(module_path)
        except (ModuleNotFoundError, ImportError) as e:
            # The module (or one of its imports) does not exist / cannot load.
            typer.echo(f"Error: cannot import module {module_path!r}: {e}", err=True)
            raise typer.Exit(2) from e
        except Exception as e:  # noqa: BLE001 -- side-effect code at import time failed
            # The module exists but its top-level code raised when executed.
            typer.echo(
                f"Error: module {module_path!r} raised during import (side-effect code failed): {e}",
                err=True,
            )
            raise typer.Exit(2) from e
        try:
            obj = getattr(module, attr)
        except AttributeError as e:
            typer.echo(f"Error: module {module_path!r} has no attribute {attr!r}.", err=True)
            raise typer.Exit(2) from e

        # A single node has ``subscribe_topics`` but is not itself a plain
        # iterable of nodes. Strings/bytes are iterable too; an McpServer is
        # iterable (its ``__iter__`` yields tool defs) but must be kept whole.
        # Treat all three as scalars; expand everything else iterable.
        if isinstance(obj, (str, bytes, McpServer)) or not isinstance(obj, Iterable):
            resolved.append(obj)
        else:
            resolved.extend(obj)
    return resolved


def validate_nodes(objs: list[Any], *, source_label: str = "target") -> None:
    """Ensure every object is a node or an ``McpServer``.

    A node is duck-typed by the attributes the worker wiring relies on
    (``subscribe_topics`` and ``_return_topic``). ``McpServer`` instances are
    allowed through unconditionally — the worker expands them into per-tool
    bridges at startup.

    Args:
        objs: Resolved objects to validate.
        source_label: How to name the spec source in error messages.

    Raises:
        typer.Exit: (code 2) if any object is neither a node nor an McpServer.
    """
    from calfkit.mcp._server import McpServer

    for obj in objs:
        if isinstance(obj, McpServer):
            continue
        missing = [attr for attr in ("subscribe_topics", "_return_topic") if not hasattr(obj, attr)]
        if missing:
            typer.echo(
                f"Error: resolved object {obj!r} is not a node "
                f"(missing {', '.join(missing)}). {source_label} must point at a "
                "BaseNodeDef or McpServer (or an iterable of them).",
                err=True,
            )
            raise typer.Exit(2)


def load_nodes(targets: list[str], *, app_dir: str | None = None, source_label: str = "target") -> list[Any]:
    """Resolve, validate, and de-duplicate ``targets`` into a non-empty node list.

    The composition used by ``calfkit run`` for both the inline (no-reload) path
    and the parent-side pre-flight before the reload supervisor starts. Raising
    on an empty result means a misconfigured run fails fast and loud (exit 2)
    instead of starting an idle worker.

    Raises:
        typer.Exit: (code 2) on a bad spec, import failure, non-node object, or
            if ``targets`` resolve to zero nodes.
    """
    objs = resolve_specs(targets, app_dir=app_dir, source_label=source_label)
    validate_nodes(objs, source_label=source_label)
    nodes = dedupe_by_node_id(objs)
    if not nodes:
        typer.echo("Error: no nodes resolved from the given target(s).", err=True)
        raise typer.Exit(2)
    return nodes


def dedupe_by_node_id(objs: list[Any]) -> list[Any]:
    """Drop duplicate nodes by ``node_id``, preserving first-seen order.

    Objects without a ``node_id`` (e.g. ``McpServer``) are keyed by identity so
    they are always retained.
    """
    seen: set[Any] = set()
    out: list[Any] = []
    for obj in objs:
        key = getattr(obj, "node_id", None)
        if key is None:
            key = id(obj)
        if key in seen:
            continue
        seen.add(key)
        out.append(obj)
    return out
