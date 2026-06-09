from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

import httpx
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamable_http_client
from mcp.shared._httpx_utils import create_mcp_http_client
from mcp.shared.message import SessionMessage
from pydantic import BaseModel, ConfigDict

__all__ = (
    "StdioServerParameters",
    "StreamableHttpParameters",
    "stdio_client",
    "http_client",
)


class StreamableHttpParameters(BaseModel):
    """Parameters for initializing a ``streamable_http_client``."""

    # httpx.Auth is not a pydantic-native type; allow it as an arbitrary field.
    model_config = ConfigDict(arbitrary_types_allowed=True)

    # The endpoint URL.
    url: str

    # Optional headers to include in requests.
    headers: dict[str, Any] | None = None

    # HTTP timeout for regular operations.
    timeout: timedelta = timedelta(seconds=30)

    # Timeout for SSE read operations.
    sse_read_timeout: timedelta = timedelta(seconds=60 * 5)

    # Close the client session when the transport closes.
    terminate_on_close: bool = True

    auth: httpx.Auth | None = None


# The yielded stream pair both transports produce for a ``ClientSession``.
_Streams = tuple[
    MemoryObjectReceiveStream[SessionMessage | Exception],
    MemoryObjectSendStream[SessionMessage],
]


@asynccontextmanager
async def http_client(server: StreamableHttpParameters) -> AsyncIterator[_Streams]:
    """Connect to a Streamable HTTP MCP server described by ``server``.

    The HTTP analogue of ``mcp.stdio_client``: pass a config object and get back
    the ``(read_stream, write_stream)`` pair to feed a ``ClientSession``. Unlike
    ``stdio_client``, no subprocess is started — the server must already be
    running at ``server.url``.

    ``streamable_http_client`` also yields a session-id callback as a third
    value; it is dropped here so the yielded shape matches ``stdio_client``.
    """
    # streamable_http_client takes a pre-built httpx client rather than loose
    # headers/timeout, so construct one with the MCP-recommended defaults.
    async with create_mcp_http_client(
        headers=server.headers,
        timeout=httpx.Timeout(
            server.timeout.total_seconds(),
            read=server.sse_read_timeout.total_seconds(),
        ),
        auth=server.auth,
    ) as httpx_client:
        async with streamable_http_client(
            server.url,
            http_client=httpx_client,
            terminate_on_close=server.terminate_on_close,
        ) as (read_stream, write_stream, *_):
            yield read_stream, write_stream
