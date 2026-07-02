"""Shared helpers for the calfkit CLI commands.

``_load_env`` and ``_parse_host`` are used by more than one command (``ck run``'s
``serve`` and ``ck chat``), so they live here rather than inside any single
command's module.
"""

from __future__ import annotations

import os

import typer


def _load_env(env_file: str | None) -> None:
    """Load environment variables from a dotenv file.

    An explicit ``env_file`` is loaded as given; otherwise ``./.env`` is loaded
    if present. A development convenience so ``OPENAI_API_KEY`` and friends are
    available without exporting them by hand.

    An explicit ``env_file`` that does not exist is surfaced as a warning rather
    than silently ignored (``load_dotenv`` no-ops on a missing path), so a typo'd
    ``--env-file`` doesn't turn into a confusing "missing API key" failure later.
    """
    from dotenv import load_dotenv

    if env_file:
        if not os.path.exists(env_file):
            # Warn once per process: the `ck dev` wrappers load the env before delegating to a
            # command that loads it again (harmlessly — override=False), and a typo'd path
            # should not print the same warning twice.
            if env_file not in _warned_missing_env_files:
                _warned_missing_env_files.add(env_file)
                typer.echo(f"Warning: --env-file {env_file!r} not found; continuing without it.", err=True)
            return
        load_dotenv(env_file)
    elif os.path.exists(".env"):
        load_dotenv(".env")


_warned_missing_env_files: set[str] = set()


def _parse_host(host: str | None) -> str | list[str] | None:
    """Map the ``--host`` flag to a ``server_urls`` value for ``Client.connect``.

    ``None`` (flag omitted) is passed through unchanged so ``Client.connect``
    applies its ``CALFKIT_MESH_URL`` → ``localhost`` fallback — preserving the
    flag > env > localhost precedence. A comma-separated value becomes a list.
    """
    if not host:
        return None
    parts = [s.strip() for s in host.split(",") if s.strip()]
    if not parts:
        return None
    return parts if len(parts) > 1 else parts[0]
