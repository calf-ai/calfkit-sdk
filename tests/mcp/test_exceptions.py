"""Pin the CalfkitMcpError taxonomy.

The base class was renamed from ``McpError`` to ``CalfkitMcpError`` to
avoid collision with the MCP SDK's ``mcp.shared.exceptions.McpError``
(raised by ``ClientSession.call_tool`` on JSON-RPC failures). A user
catching the calfkit base must NOT silently swallow SDK protocol errors.

Without these tests, a future drive-by revert ("rename back, it's
shorter") would re-introduce the collision and pass every other test.
"""

from __future__ import annotations

from mcp.shared.exceptions import ErrorData
from mcp.shared.exceptions import McpError as SdkMcpError

from calfkit.mcp.exceptions import (
    CalfkitMcpError,
    McpConfigError,
    McpProtocolError,
    McpToolDriftError,
    McpTransportError,
)


def test_calfkit_mcp_error_is_the_base_class_for_all_subtypes() -> None:
    """Every concrete calfkit MCP error inherits from CalfkitMcpError."""
    for sub in (McpConfigError, McpTransportError, McpProtocolError, McpToolDriftError):
        assert issubclass(sub, CalfkitMcpError), f"{sub.__name__} must inherit from CalfkitMcpError"


def test_calfkit_mcp_error_does_not_catch_sdk_mcp_error() -> None:
    """``except CalfkitMcpError`` must NOT swallow ``mcp.shared.exceptions.McpError``.

    This is the whole point of the rename. If McpError(SDK) inherited from
    CalfkitMcpError (or shared its name), the bridge boundary would silently
    absorb JSON-RPC errors that the operator needs to see as
    ToolExecutionError.
    """
    sdk_err = SdkMcpError(ErrorData(code=-32603, message="internal", data=None))
    assert not isinstance(sdk_err, CalfkitMcpError)


def test_calfkit_mcp_error_name_is_stable() -> None:
    """The class name is part of the public API (operators import it by name)."""
    assert CalfkitMcpError.__name__ == "CalfkitMcpError"
    assert CalfkitMcpError.__module__ == "calfkit.mcp.exceptions"
