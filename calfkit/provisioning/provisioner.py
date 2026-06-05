import asyncio
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from calfkit.provisioning.config import ProvisioningConfig

logger = logging.getLogger(__name__)

# Kafka error codes we classify explicitly (see aiokafka.errors.for_code).
_CODE_NONE = 0  # NoError -> created
_CODE_TOPIC_ALREADY_EXISTS = 36  # idempotent success
_CODE_TOPIC_AUTHORIZATION_FAILED = 29  # ACL denied -> warn + continue

# Bounded backoff between retriable create attempts (seconds). The overall
# operation is still capped by ``ProvisioningConfig.create_timeout_ms`` via the
# single ``asyncio.wait_for`` wrapping the whole flow.
_RETRY_BACKOFF_S = 0.5


def topics_for_nodes(nodes: Iterable[Any]) -> list[str]:
    """Compute the full set of Kafka topics referenced by ``nodes``.

    For every node this includes, in encounter order:

    * each entry of ``subscribe_topics`` (the node's public inbox(es)),
    * the node's framework-private ``_return_topic`` (tool ReturnCall /
      built-in TailCall inbox — issue #141), and
    * ``publish_topic`` when set.

    Agent nodes additionally contribute each tool's input topic
    (``agent.tools[*].subscribe_topics``): the agent publishes tool ``Call``
    envelopes onto ``tools_registry[name].subscribe_topics[0]``, so those
    topics must exist for tool dispatch to land. Agent nodes are detected
    structurally by exposing a ``tools`` collection of tool schemas — this
    keeps :mod:`calfkit.provisioning` decoupled from :mod:`calfkit.nodes`.

    The result is de-duplicated while preserving first-seen order.
    """
    seen: dict[str, None] = {}

    def _add(topic: str | None) -> None:
        if topic and topic not in seen:
            seen[topic] = None

    for node in nodes:
        for topic in node.subscribe_topics:
            _add(topic)
        _add(node._return_topic)
        _add(node.publish_topic)

        tools = getattr(node, "tools", None)
        if tools:
            for tool in tools:
                for topic in tool.subscribe_topics:
                    _add(topic)

    return list(seen)


@dataclass
class ProvisionReport:
    """Outcome of a provisioning pass, for logging / a CLI summary.

    Args:
        created: Topics that did not exist and were created (code 0).
        existing: Topics that already existed — idempotent no-op (code 36).
        unauthorized: Topics the client is not authorized to create (code 29).
            Creation was skipped with a loud warning; consumers/producers will
            silently stall on these unless they are pre-created out-of-band.
    """

    created: list[str] = field(default_factory=list)
    existing: list[str] = field(default_factory=list)
    unauthorized: list[str] = field(default_factory=list)


class TopicProvisioningError(Exception):
    """A topic could not be provisioned and the failure is not recoverable.

    Carries the offending ``topic`` and the Kafka ``code`` (error code or, for
    a timeout, ``None``) so callers can log the precise cause.
    """

    def __init__(self, message: str, *, topic: str, code: int | None) -> None:
        self.topic = topic
        self.code = code
        super().__init__(message)


def _normalize_bootstrap(server_urls: str | Iterable[str] | None) -> str | list[str]:
    """Normalize connect server URL(s) to the admin client's accepted shape.

    ``None`` falls back to ``"localhost"``; a ``str`` is passed through as-is;
    any other iterable is materialized to a ``list``.
    """
    if server_urls is None:
        return "localhost"
    if isinstance(server_urls, str):
        return server_urls
    return list(server_urls)


def _make_admin_client(**kwargs: Any) -> Any:
    """Construct an ``AIOKafkaAdminClient``.

    This is the **only** place the admin client is instantiated, and the seam
    that unit tests monkeypatch to inject a fake. ``aiokafka.admin`` is
    imported lazily here so the dependency is only loaded when provisioning is
    actually enabled.
    """
    from aiokafka.admin import AIOKafkaAdminClient  # type: ignore[import-untyped]

    return AIOKafkaAdminClient(**kwargs)


def _merge_security_kwargs(
    security: Any | None,
    raw_kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Merge a FastStream ``security=`` object with raw connection kwargs.

    The ``security`` object (when given) is parsed via
    ``faststream.kafka.security.parse_security`` into admin-client kwargs and
    forms the authoritative base. Raw kwargs (e.g. ``sasl_kerberos_*`` /
    ``sasl_oauth_token_provider`` that ``parse_security`` does not emit, or a
    standalone ``security_protocol``) are merged on top:

    * The ``security=`` object wins on any overlapping key — EXCEPT
    * ``security_protocol``: if both sources specify it AND they disagree, that
      is a genuine configuration conflict and we raise rather than silently
      pick one.
    """
    base: dict[str, Any] = {}
    if security is not None:
        from faststream.kafka.security import parse_security

        base = dict(parse_security(security))

    merged = dict(base)
    for key, value in raw_kwargs.items():
        if key in base:
            if key == "security_protocol" and base[key] != value:
                raise ValueError(
                    "Conflicting security_protocol: security="
                    f"{base[key]!r} but security_protocol={value!r} was also "
                    "passed. Provide it in exactly one place."
                )
            # security= object wins on all other overlaps.
            continue
        merged[key] = value
    return merged


class TopicProvisioner:
    """Best-effort, opt-in creator of Kafka topics via the admin client.

    See :class:`~calfkit.provisioning.ProvisioningConfig` for the dev-safe /
    review-for-prod caveats. Construction performs NO network I/O — the admin
    client is only created (and connected) inside :meth:`provision`.

    A single instance handles one :meth:`provision` call at a time; it is not
    safe to reuse concurrently (``_last_pending`` is per-operation state).
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str | list[str],
        config: ProvisioningConfig,
        security: Any | None = None,
        **security_kwargs: Any,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._config = config
        self._conn_kwargs = _merge_security_kwargs(security, security_kwargs)
        # Topics still unresolved at the latest create attempt; surfaced by the
        # timeout path so the error names what is missing.
        self._last_pending: list[str] = []

    @classmethod
    def from_connection(
        cls,
        *,
        server_urls: str | Iterable[str] | None,
        config: ProvisioningConfig,
        security_kwargs: dict[str, Any] | None = None,
    ) -> "TopicProvisioner":
        """Build a provisioner from connect-style inputs.

        Centralizes the bootstrap normalization (:func:`_normalize_bootstrap`)
        and the ``security=`` object split that the client, worker, and CLI all
        otherwise mirror. ``security_kwargs`` is treated read-only: ``security``
        is popped out of a copy and the remaining raw kwargs are forwarded to
        the admin client.
        """
        kwargs = dict(security_kwargs) if security_kwargs else {}
        security = kwargs.pop("security", None)
        return cls(
            bootstrap_servers=_normalize_bootstrap(server_urls),
            config=config,
            security=security,
            **kwargs,
        )

    async def provision(
        self,
        topics: Iterable[str],
        *,
        framework_topics: set[str],
    ) -> ProvisionReport:
        """Create ``topics`` if missing, returning a :class:`ProvisionReport`.

        The entire operation — admin ``start()``, ``create_topics``, retriable
        re-issues, and response inspection — is bounded by a single
        ``asyncio.wait_for`` using ``config.create_timeout_ms``. ``topic_configs``
        from the config are applied to data topics only, never to
        ``framework_topics`` (reply / ``*.private.return`` inboxes), for which
        overrides like ``cleanup.policy=compact`` would be semantically wrong.

        Raises:
            TopicProvisioningError: On a non-retriable per-topic error, or when
                the timeout elapses with topics still pending.
        """
        # Reset per-operation state so a prior call's pending set can't leak
        # into this one's timeout/error reporting.
        self._last_pending = []
        pending = list(dict.fromkeys(topics))
        if not pending:
            return ProvisionReport()

        self._last_pending = list(pending)
        timeout_s = self._config.create_timeout_ms / 1000
        try:
            return await asyncio.wait_for(
                self._run(pending, framework_topics),
                timeout=timeout_s,
            )
        except asyncio.TimeoutError as exc:
            # ``self._last_pending`` reflects topics not yet resolved when the
            # deadline hit. Name them so the operator knows what is missing.
            still = ", ".join(self._last_pending) if self._last_pending else "<unknown>"
            raise TopicProvisioningError(
                f"Topic provisioning timed out after {self._config.create_timeout_ms}ms; still pending: {still}",
                topic=self._last_pending[0] if self._last_pending else "",
                code=None,
            ) from exc

    async def _run(self, pending: list[str], framework_topics: set[str]) -> ProvisionReport:
        report = ProvisionReport()
        self._last_pending = list(pending)
        admin = _make_admin_client(
            bootstrap_servers=self._bootstrap_servers,
            request_timeout_ms=self._config.create_timeout_ms,
            **self._conn_kwargs,
        )
        try:
            await admin.start()
            while pending:
                self._last_pending = list(pending)
                new_topics = [self._new_topic(t, framework_topics) for t in pending]
                resp = await admin.create_topics(new_topics)
                next_pending: list[str] = []
                accounted: set[str] = set()
                for row in resp.topic_errors:
                    topic, code = self._unpack(row)
                    accounted.add(topic)
                    decision = self._classify(topic, code)
                    if decision == "created":
                        report.created.append(topic)
                    elif decision == "existing":
                        report.existing.append(topic)
                    elif decision == "unauthorized":
                        report.unauthorized.append(topic)
                        logger.warning(
                            "Topic %r authorization failed (code 29): not created. "
                            "Producers/consumers on this topic will silently stall "
                            "unless it is pre-created out-of-band.",
                            topic,
                        )
                    elif decision == "retry":
                        next_pending.append(topic)
                # Defensive: every requested topic must appear in the response.
                # A broker that silently drops a topic from its reply must not be
                # treated as success — name the unaccounted topic(s) and fail.
                unaccounted = [t for t in pending if t not in accounted]
                if unaccounted:
                    raise TopicProvisioningError(
                        f"Topic provisioning response omitted requested topic(s): {', '.join(unaccounted)}.",
                        topic=unaccounted[0],
                        code=None,
                    )
                pending = next_pending
                if pending:
                    await asyncio.sleep(_RETRY_BACKOFF_S)
                    self._last_pending = list(pending)
            return report
        finally:
            # Best-effort idempotent close on every path, including
            # cancellation/timeout. A failure here must not mask an in-flight
            # exception (e.g. a real provisioning error), so swallow + log it.
            try:
                await admin.close()
            except Exception:  # noqa: BLE001
                logger.warning("Failed to close Kafka admin client", exc_info=True)

    def _new_topic(self, topic: str, framework_topics: set[str]) -> Any:
        from aiokafka.admin import NewTopic

        # NewTopic(num_partitions=-1, ...) raises client-side (XOR validator),
        # so always supply concrete positive values.
        num_partitions = self._config.num_partitions
        replication_factor = self._config.replication_factor
        topic_configs = None
        if topic not in framework_topics and self._config.topic_configs:
            topic_configs = dict(self._config.topic_configs)
        return NewTopic(
            name=topic,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            topic_configs=topic_configs,
        )

    @staticmethod
    def _unpack(row: tuple[Any, ...]) -> tuple[str, int]:
        # ``topic_errors`` rows are (topic, code) on response v0 and
        # (topic, code, message) on v1+. We only need topic + code.
        return row[0], row[1]

    @staticmethod
    def _classify(topic: str, code: int) -> str:
        from aiokafka.errors import for_code  # type: ignore[import-untyped]

        if code == _CODE_NONE:
            return "created"
        if code == _CODE_TOPIC_ALREADY_EXISTS:
            return "existing"
        if code == _CODE_TOPIC_AUTHORIZATION_FAILED:
            return "unauthorized"
        err = for_code(code)
        if getattr(err, "retriable", False):
            return "retry"
        raise TopicProvisioningError(
            f"Topic {topic!r} failed provisioning with non-retriable error {err.__name__} (code {code}).",
            topic=topic,
            code=code,
        )


__all__ = [
    "ProvisionReport",
    "TopicProvisioner",
    "TopicProvisioningError",
    "topics_for_nodes",
]
