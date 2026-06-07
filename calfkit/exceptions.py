from typing import Any


class DeserializationError(Exception):
    """Raised when client-side output deserialization fails."""


class LifecycleConfigError(Exception):
    """Raised when a node/worker lifecycle configuration is invalid.

    Covers misconfiguration detectable at registration time. Currently: a
    duplicate ``@resource`` name on a single owner (resource names must be
    unique per owner).
    """


class RegistryConfigError(Exception):
    """Raised when a :class:`~calfkit._registry.RegistryMixin` subclass declares
    an invalid handler set.

    Covers misconfiguration detectable at class-definition time. Currently: two
    methods in the same class hierarchy registered under the same ``handler``
    name (handler names must be unique per class).
    """


class ReplyExpiredError(Exception):
    """Raised when an awaited reply does not arrive within the dispatcher's
    ``reply_ttl`` and the pending future is evicted.

    Surfacing this (instead of a bare ``CancelledError``) gives callers an
    actionable signal carrying the offending ``correlation_id`` and the ``ttl``
    (seconds) that elapsed.
    """

    def __init__(self, correlation_id: str, ttl: float):
        self.correlation_id = correlation_id
        self.ttl = ttl
        super().__init__(f"No reply for correlation_id={correlation_id!r} within reply_ttl={ttl}s")

    def __reduce__(self) -> tuple[Any, tuple[str, float]]:
        # The default reduction replays ``self.args`` (the formatted message
        # string) through ``__init__``, which would fail because the constructor
        # requires both positional fields. Reconstruct from the real fields.
        return (self.__class__, (self.correlation_id, self.ttl))


class MissingTopicsError(RuntimeError):
    """Raised at broker start when topic provisioning was **enabled** but one or
    more required topics could not be created (e.g. the principal lacks the
    ``CreateTopics`` ACL). Starting the consumers would otherwise stall forever
    on cluster metadata, so we fail loud instead.

    Carries the offending ``topics`` so the caller can see exactly what is
    missing; the message names them plus the remedies.
    """

    def __init__(self, topics: list[str]):
        self.topics = topics
        names = ", ".join(topics)
        super().__init__(
            f"Topic provisioning was enabled but these topic(s) could not be created: {names}. "
            "Grant the client CreateTopics authorization, or pre-create the topic(s) out-of-band."
        )


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
