from dataclasses import KW_ONLY, dataclass


@dataclass
class BaseNodeSchema:
    _: KW_ONLY
    node_id: str
    subscribe_topics: list[str]
    publish_topic: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.subscribe_topics, (list, tuple)):
            self.subscribe_topics = [self.subscribe_topics]
        # Reject empty subscribe_topics for every node kind (Agent, Consumer,
        # Tool, …). Lives here rather than in ``BaseNodeDef.__init__`` because
        # ``@dataclass`` subclasses like ``BaseToolNodeDef`` get an
        # auto-generated ``__init__`` that bypasses ``BaseNodeDef.__init__``
        # entirely; ``__post_init__`` is the one hook every subclass runs.
        # Without this guard, ``Worker.register_handlers`` would still add
        # ``_return_topic`` to the subscriber set (issue #141 fix), so the
        # node would "register" successfully but have no public inbox — a
        # silent zombie consumer.
        if not self.subscribe_topics:
            raise ValueError(f"node {self.node_id!r} requires at least one subscribe_topic; got empty list")
