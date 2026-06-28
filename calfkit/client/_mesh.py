"""Resolve the mesh URL — the Kafka bootstrap server(s) the client connects to.

The single source of truth for the ``arg > $CALFKIT_MESH_URL > localhost``
precedence and the normalization to the list form aiokafka expects. Both
:meth:`Client.connect <calfkit.client.caller.Client.connect>` and the ``ck run``
startup banner resolve through :func:`resolve_mesh_url`, so the banner can never
report a different broker than the one the client actually connects to.
"""

from __future__ import annotations

import os
from collections.abc import Iterable

MESH_URL_ENV_VAR = "CALFKIT_MESH_URL"
DEFAULT_MESH_URL = "localhost"


def resolve_mesh_url(mesh_url: str | Iterable[str] | None) -> list[str]:
    """Resolve and normalize the mesh URL to a list of bootstrap servers.

    Precedence: an explicit *mesh_url* wins; otherwise ``$CALFKIT_MESH_URL`` (a
    set-but-empty value is ignored); otherwise :data:`DEFAULT_MESH_URL`.

    The result is always a ``list[str]``: FastStream wraps a bare ``str`` into
    ``[str]`` and aiokafka never comma-splits inside a list element, so a string
    is materialized as a single-element list and any other iterable (including a
    one-shot generator) is materialized once here so the broker receives real
    bootstrap servers rather than an already-consumed iterator.
    """
    if mesh_url is None:
        mesh_url = os.getenv(MESH_URL_ENV_VAR) or DEFAULT_MESH_URL
    return [mesh_url] if isinstance(mesh_url, str) else list(mesh_url)
