from typing import Any, Literal

import pydantic_core

# ``safe_exc_message`` moved to the dependency-free ``calfkit._safe`` leaf so ``error_report`` can use
# it without the import cycle (``exceptions`` imports ``error_report``); explicitly re-exported here
# (the ``as`` form) so existing ``from calfkit.exceptions import safe_exc_message`` callers are
# unchanged, and used on the mint path below.
from calfkit._safe import safe_exc_message as safe_exc_message
from calfkit.models.error_report import ErrorReport

_CALF_NAMESPACE = "calf."


class NodeFaultError(Exception):
    """A terminal fault — one class, symmetric for minting and receiving (spec §11).

    **Mint** a deliberate typed fault from node/seam code::

        raise NodeFaultError("billing.quota_exceeded", message="...", retryable=False, details={...})

    **Receive** one at the client edge (the client hub maps a ``fault`` reply to
    ``NodeFaultError(report)``, raised from ``result()``). Branch on the stable
    ``error_type``; read ``report.exception`` for diagnostics (spec §11.1)::

        try:
            result = await handle.result()
        except NodeFaultError as e:
            # BRANCH on the stable dotted code — find(), not ==, so it traverses fault groups:
            if e.report.find(FaultTypes.MODEL_CONTEXT_WINDOW_EXCEEDED):
                ...
            # DIAGNOSE with the harvested exception slot (first-class for logging / "what failed"):
            if e.report.exception is not None:
                log.error("upstream %s: %s", e.report.exception.type, e.report.exception.attrs)
            # FORENSIC: inspect a specific deeper cause by class name (best-effort stopgap, §11.1):
            inner = next((r for r in e.report.walk()
                          if r.exception and r.exception.type == "BadRequestError"), None)

    The contract is the :class:`~calfkit.models.error_report.ErrorReport` on
    ``report``, never an exception class — no ``error_type → exception`` registry,
    so every surface (seams, sinks, client) reads the same slotted report. The
    ``exception`` slot is **diagnostic-first**: ``error_type`` + ``find()`` is the durable
    branching contract; ``walk()`` + ``exception.type`` is forensic, not a blessed branching
    API (it keys on a class name the LiteLLM migration, #230, will change).
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
        # mint arm and rebuild a different report (cf. ClientTimeoutError /
        # ClientClosedError, which carry custom reductions for the same reason).
        return (self.__class__, (self.report,))


class SeamContractError(Exception):
    """A policy seam violated its contract (spec §6.2 / §6.3 / §6.8).

    Raised when a seam returns a value that can never be a node output:

    - a ``bool``, the session ``State`` or the ``SeamContext`` itself, or
      ``bytes`` — the §6.2 coercion guards;
    - ``Next``, the route-dispatch decline sentinel, which is dispatch
      vocabulary, not a seam output (§6.2);
    - a substitute that fails a *typed* node's declared output-type validation
      — the output-position guard (§6.3 / scenario 44), so a type-breaking
      value faults at the seam rather than downstream;
    - an action returned by ``after_node``, whose contract is values-only
      (§6.8).

    It faults loudly (P1) so a migration trap corrupts nothing silently; the
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


class ClientTimeoutError(Exception):
    """Raised when the client stops waiting for a reply (``result(timeout=)`` /
    ``execute(timeout=)``).

    A typed, run-survives signal (spec §2.5) — never a bare ``asyncio.TimeoutError``. The run
    itself is unaffected; only this client gave up waiting. Carries the offending
    ``correlation_id`` and the ``timeout`` (seconds) that elapsed.
    """

    def __init__(self, correlation_id: str, timeout: float):
        self.correlation_id = correlation_id
        self.timeout = timeout
        super().__init__(f"No reply for correlation_id={correlation_id!r} within timeout={timeout}s")

    def __reduce__(self) -> tuple[Any, tuple[str, float]]:
        # Required positional args break the default reduction (which replays the formatted
        # message string); reconstruct from the real fields.
        return (self.__class__, (self.correlation_id, self.timeout))


class ClientClosedError(Exception):
    """Raised when the client is closed (``aclose()``) with work still pending.

    A typed, run-survives signal — never a bare ``CancelledError``. Two shapes: with a
    ``correlation_id`` it names the run whose ``result()`` was still pending (spec §2.5 / §5.8);
    without one it marks a non-run wait interrupted by close (e.g. a ``client.mesh`` read). The
    run itself is unaffected; the client simply stopped consuming.
    """

    def __init__(self, correlation_id: str | None = None):
        self.correlation_id = correlation_id
        message = f"client closed while awaiting a reply for correlation_id={correlation_id!r}" if correlation_id is not None else "client closed"
        super().__init__(message)

    def __reduce__(self) -> tuple[Any, tuple[str | None]]:
        return (self.__class__, (self.correlation_id,))


_MeshUnavailableReason = Literal["establishing", "reader_dead", "open_failed"]


class MeshUnavailableError(Exception):
    """A ``client.mesh`` read could not return a roster — the per-kind view is not usable.

    Raised by ``client.mesh.get_agents()`` / ``get_tools()`` (spec §6.4) rather than ever
    returning a silently-partial roster. Branch on ``reason`` to route without string-matching
    the message (the discriminate-by-field precedent is ``NodeFaultError.report.retryable``):

    - ``reason="establishing"`` — the view is still catching up (a cold-start latch; no cause
      attached). It self-heals on the **same** client, so retry with a short backoff.
    - ``reason="reader_dead"`` — the reader died on a non-retriable error (cause attached).
      Terminal for the view's lifetime: alert; recovery needs a fresh ``Client``.
    - ``reason="open_failed"`` — the open itself raised (a missing directory topic or an
      unreachable broker; cause attached). The next read re-opens, so retry with backoff.

    A misconfigured client (``server_urls is None``) raises a synchronous ``ValueError`` instead
    — you never retry a programming bug.
    """

    def __init__(self, message: str, *, reason: _MeshUnavailableReason) -> None:
        self.reason: _MeshUnavailableReason = reason
        super().__init__(message)

    def __reduce__(self) -> tuple[Any, tuple[str, _MeshUnavailableReason]]:
        # The keyword-only `reason` breaks the default reduction (which would replay only the
        # message string through __init__ and silently drop `reason`); rebuild from both fields
        # via a module-level reconstructor (cf. ClientTimeoutError / ClientClosedError).
        return (_rebuild_mesh_unavailable, (self.args[0], self.reason))


def _rebuild_mesh_unavailable(message: str, reason: _MeshUnavailableReason) -> MeshUnavailableError:
    return MeshUnavailableError(message, reason=reason)


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
