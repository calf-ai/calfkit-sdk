from dataclasses import dataclass, field


@dataclass
class ProvisioningConfig:
    """Opt-in configuration for best-effort Kafka topic auto-creation.

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
            referenced topics on startup; when ``False`` it is a no-op.
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
        create_timeout_ms: Overall budget for the provisioning operation
            (connect + create + retry + inspect). Exceeding it raises
            :class:`~calfkit.provisioning.TopicProvisioningError`-adjacent
            timeout behavior naming the still-pending topics.
    """

    enabled: bool
    num_partitions: int = 1
    replication_factor: int = 1
    topic_configs: dict[str, str] = field(default_factory=dict)
    create_timeout_ms: int = 30000
