from collections.abc import Sequence
from dataclasses import dataclass

from calfkit.nodes import BaseNodeDef


@dataclass(frozen=True)
class MCPDiscoveryConfig:
    """Optional tuning for MCP capability discovery (spec §8.3).

    Entirely optional — the control plane is zero-config by design: the
    capability view and toolbox publishers derive the broker URL from the
    client the user already connected. Every field here is an escape hatch.

    Args:
        topic: The cluster-wide compacted capability topic.
        catchup_timeout: Bound on the worker boot gate awaiting view catch-up;
            on expiry the worker serves degraded (loudly logged).
        heartbeat_interval: Seconds between a toolbox's record re-publishes.
        bootstrap_servers: Override for the control-plane Kafka cluster.
            ``None`` (default) derives from the connected client — set only
            for the split-cluster case (control plane on different brokers
            than the data plane).
    """

    topic: str = "mcp.capabilities"
    catchup_timeout: float = 30.0
    heartbeat_interval: float = 30.0
    bootstrap_servers: str | None = None

    def __post_init__(self) -> None:
        if self.catchup_timeout <= 0:
            raise ValueError(f"catchup_timeout must be > 0, got {self.catchup_timeout}")
        if self.heartbeat_interval <= 0:
            raise ValueError(f"heartbeat_interval must be > 0, got {self.heartbeat_interval}")
        if not self.topic:
            raise ValueError("topic must be non-empty")


@dataclass
class WorkerConfig:
    node: type[BaseNodeDef]
    # Note: to use the deployment config like temporal's deployment pattern, we can decouple deployment information entirely from node so nodes no longer need topic info in its init. This might be cleaner.  # noqa: E501
    subscribe_topics: Sequence[str]
    publish_topics: str
    max_workers: int
    group_id: str
