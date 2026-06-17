from typing import Any

import pydantic_core

from calfkit.models.error_report import ErrorReport

_CALF_NAMESPACE = "calf."


def safe_exc_message(e: BaseException) -> str:
    """Best-effort string of an exception, robust against a broken ``__str__``.

    A bare ``str(e)`` can itself raise (a broken ``__str__``, or args that don't
    coerce). On the fault path that would propagate out and re-open the silent-drop
    hole the rail closes (e.g. while synthesizing an ``ErrorReport`` from a tool's
    exception). Mirrors stdlib ``traceback._some_str`` with a ``repr`` fallback.
    """
    try:
        return str(e)
    except Exception:
        try:
            return repr(e)
        except Exception:
            return f"<unprintable {type(e).__name__}>"


class NodeFaultError(Exception):
    """A terminal fault — one class, symmetric for minting and receiving (spec §11).

    **Mint** a deliberate typed fault from node/seam code::

        raise NodeFaultError("billing.quota_exceeded", message="...", retryable=False, details={...})

    **Receive** one at the client edge (the reply dispatcher fails the pending
    future with ``NodeFaultError(report)``), then branch on the slotted report::

        try:
            result = await handle.result()
        except NodeFaultError as e:
            if e.report.find(FaultTypes.MODEL_CONTEXT_WINDOW_EXCEEDED):  # find(), not == — groups compose
                ...

    The contract is the :class:`~calfkit.models.error_report.ErrorReport` on
    ``report``, never an exception class — no ``error_type → exception`` registry,
    so every surface (seams, sinks, client) reads the same slotted report.
    """

    report: ErrorReport

    def __init__(
        self,
        error_type_or_report: str | ErrorReport,
        *,
        message: str = "",
        retryable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        if isinstance(error_type_or_report, ErrorReport):
            # RECEIVE/WRAP: carry an existing report verbatim. No namespace guard —
            # it may legitimately be a framework-minted calf.* the client re-raises.
            if message or retryable or details is not None:
                raise ValueError("message/retryable/details are mint-only; NodeFaultError(report) wraps a report verbatim.")
            self.report = error_type_or_report
        else:
            # MINT: build a fresh report from a user-supplied type.
            error_type = error_type_or_report
            if not error_type or error_type.isspace():
                raise ValueError("error_type must be a non-empty code (it is the contract consumers branch on).")
            if error_type.startswith(_CALF_NAMESPACE):
                raise ValueError(
                    f"error_type {error_type!r} is under the reserved {_CALF_NAMESPACE!r} prefix, which is "
                    "framework-only; choose a type outside it so consumers can trust the calf.* namespace."
                )
            if details:
                reserved = sorted(k for k in details if k.startswith(_CALF_NAMESPACE))
                if reserved:
                    raise ValueError(f"details keys under the reserved {_CALF_NAMESPACE!r} prefix are framework-only: {reserved}.")
            # Eagerly check the details are JSON-serializable so an unserializable
            # value fails here, at the keyboard, not later on the error path. (An
            # oversized-but-serializable details is not rejected; build_safe bounds
            # it to 16 KB and records the drop under calf.elided.)
            try:
                pydantic_core.to_json(details or {})
            except Exception as exc:
                raise ValueError(f"NodeFaultError details are not JSON-serializable: {safe_exc_message(exc)}") from exc
            self.report = ErrorReport.build_safe(error_type=error_type, message=message, retryable=retryable, details=details or {})
        # error_type can be empty on a *received* report (decode has no min_length);
        # keep the exception string non-empty so logs/tracebacks never read blank.
        super().__init__(self.report.message or self.report.error_type or "<unspecified fault>")

    def __reduce__(self) -> tuple[Any, tuple[ErrorReport]]:
        # Reconstruct from the report, not by replaying self.args (a message
        # string) through __init__, which would mis-route the string into the
        # mint arm and rebuild a different report (cf. ReplyExpiredError, which
        # carries a custom reduction for the same reason).
        return (self.__class__, (self.report,))


class SeamContractError(Exception):
    """A policy seam violated its contract (spec §6.2 / §6.8).

    Raised when a seam returns a value that can never be a node output — a ``bool``,
    the session ``State`` or the ``SeamContext`` itself, or ``bytes`` (the §6.2
    coercion guards) — or when ``after_node`` returns an action instead of a value
    (§6.8). It faults loudly (P1) so a migration trap corrupts nothing silently; the
    skeleton routes it to ``on_node_error`` like any other node-own raise.
    """


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

    Covers misconfiguration detectable at class-definition time: two handlers
    registered under the same ``route`` (routes must be unique per class), an
    invalid route pattern, an ambiguous catch-all (an explicit ``@handler('*')``
    alongside an overridden ``run()``), or a handler whose payload parameter and
    ``schema=`` disagree.
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


class MCPToolResolutionError(Exception):
    """A strict MCP tool selector could not be satisfied for this turn.

    Raised by the agent BEFORE the model runs when a selector declared with
    ``strict=True`` cannot fully resolve against the Capability View (toolbox
    missing, requested tool not advertised, malformed/newer-schema record, or
    no view resource at all). Non-strict selectors degrade with a warning
    instead.
    """
