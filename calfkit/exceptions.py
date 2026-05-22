from typing import Any


class DeserializationError(Exception):
    """Raised when client-side output deserialization fails."""


class ToolExecutionError(Exception):
    """The original traceback is not preserved across the Kafka boundary; it is
    logged at the worker that ran the tool. ``exc_type`` and ``exc_message`` are
    the only forensic data available at the agent.
    """

    def __init__(self, *, tool_name: str, tool_call_id: str, exc_type: str, exc_message: str):
        self.tool_name = tool_name
        self.tool_call_id = tool_call_id
        self.exc_type = exc_type
        self.exc_message = exc_message
        super().__init__(f"Tool {tool_name!r} (call_id={tool_call_id}) raised {exc_type}: {exc_message}")

    def __reduce__(self) -> tuple[Any, ...]:
        # Bypass keyword-only ``__init__`` during reconstruction by routing
        # through a module-level helper that uses ``__new__``. A naive
        # ``(self.__class__, (), state)`` reduction would call ``__init__()``
        # with no args and fail because the keyword arguments are required.
        return (
            _reconstruct_tool_execution_error,
            (),
            {
                "tool_name": self.tool_name,
                "tool_call_id": self.tool_call_id,
                "exc_type": self.exc_type,
                "exc_message": self.exc_message,
            },
        )

    def __setstate__(self, state: dict[str, Any] | None) -> None:
        # ``__reduce__`` above always provides a dict, but the supertype
        # signature allows ``None`` (Liskov); treat ``None`` as a no-op so
        # subclasses constructed without state don't crash.
        if state is None:
            return
        self.tool_name = state["tool_name"]
        self.tool_call_id = state["tool_call_id"]
        self.exc_type = state["exc_type"]
        self.exc_message = state["exc_message"]
        Exception.__init__(
            self,
            f"Tool {self.tool_name!r} (call_id={self.tool_call_id}) raised {self.exc_type}: {self.exc_message}",
        )


def _reconstruct_tool_execution_error() -> "ToolExecutionError":
    """Create a bare ``ToolExecutionError`` for reconstruction via ``__setstate__``.

    Bypasses the keyword-only ``__init__`` so state restoration can populate
    fields directly. Must be module-level (not a lambda or local closure) so
    it can be located by fully-qualified name during deserialization.
    """
    return ToolExecutionError.__new__(ToolExecutionError)
