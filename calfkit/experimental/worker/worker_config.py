from collections.abc import Sequence
from dataclasses import dataclass

from calfkit.experimental.nodes.base import BaseNodeDef


@dataclass
class WorkerConfig:
    node: type[BaseNodeDef]
    # Note: to use the deployment config like temporal's deployment pattern, we can decouple deployment information entirely from node so nodes no longer need topic info in its init. This might be cleaner.
    subscribe_topics: Sequence[str]
    publish_topics: str
    max_workers: int
    group_id: str
