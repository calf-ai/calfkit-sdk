from dataclasses import dataclass, field


@dataclass(frozen=True)
class ProvisioningConfig:
    """Opt-in configuration for best-effort Kafka topic auto-creation.

    **Experimental** (opt-in; off by default). This API may change or be
    removed in a minor release — calfkit is pre-1.0. Feedback welcome.

    .. warning::

        This is a **development convenience**, not a production provisioning
        story. Review every field before relying on it outside local/CI use:

        * ``replication_factor=1`` is the default and is **NOT durable** — a
          single broker failure loses the topic and its data. Real deployments
          need ``replication_factor >= 3``.
        * No ACLs are created. If the broker enforces authorization, topics
          created here will not be readable/writable by the right principals
          unless ACLs are provisioned out-of-band.
        * In production, topic creation is almost always an **ops-governed**
          operation (Terraform, GitOps, a platform team) with naming policy,
          retention, and partition-count review. Enabling this in production
          bypasses that governance.

    Args:
        enabled: Master switch. When ``True``, the worker/client provisions the
            referenced topics on startup; when ``False`` (the default) it is a
            no-op.
        num_partitions: Partition count for every newly-created data topic.
            Always supplied as a concrete positive value (never broker-default
            ``-1``).
        replication_factor: Replication factor for every newly-created topic.
            See the durability warning above.
        topic_configs: Per-topic ``config`` overrides (e.g.
            ``{"retention.ms": "604800000"}``) applied to **data topics only**.
            Framework topics (reply topics, ``*.private.return`` inboxes) are
            deliberately excluded — settings like ``cleanup.policy=compact`` or
            short retention are semantically wrong for correlation-keyed
            request/reply traffic.
        create_timeout_ms: Budget for topic creation. The create/classify/retry
            loop is bounded by it (the ``asyncio.wait_for`` inside
            :func:`~calfkit.provisioning.provision_topics`); the standalone
            :class:`~calfkit.provisioning.TopicProvisioner` additionally bounds
            the admin ``start()`` (connect) under the same budget. Exceeding it
            raises :class:`~calfkit.provisioning.TopicProvisioningError` (with
            ``code=None``) naming the still-pending topics. Also reused as the
            admin client's ``request_timeout_ms``.

    Raises:
        ValueError: If ``num_partitions < 1``, ``replication_factor < 1``, or
            ``create_timeout_ms <= 0``.
    """

    enabled: bool = False
    num_partitions: int = 1
    replication_factor: int = 1
    topic_configs: dict[str, str] = field(default_factory=dict)
    create_timeout_ms: int = 30000

    def __post_init__(self) -> None:
        if self.num_partitions < 1:
            raise ValueError(f"num_partitions must be >= 1, got {self.num_partitions!r}")
        if self.replication_factor < 1:
            raise ValueError(f"replication_factor must be >= 1, got {self.replication_factor!r}")
        if self.create_timeout_ms <= 0:
            raise ValueError(f"create_timeout_ms must be > 0, got {self.create_timeout_ms!r}")
