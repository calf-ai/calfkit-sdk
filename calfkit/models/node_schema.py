from dataclasses import KW_ONLY, dataclass

from calfkit._vendor.pydantic_ai.tools import ToolDefinition


@dataclass
class BaseNodeSchema:
    _: KW_ONLY
    node_id: str
    subscribe_topics: list[str]
    publish_topic: str | None

    def __post_init__(self) -> None:
        # Reject empty / whitespace-only / dot-prefixed ``node_id``. ``node_id``
        # is the prefix for several per-node Kafka topics
        # (``{node_id}.private.return``, ``{node_id}.fanout-state``,
        # ``{node_id}.fanout-returns``); an empty or dot-prefixed value would
        # produce malformed/shared topic names (e.g., ``.private.return``)
        # that every empty-id node would collide on — re-introducing the
        # co-tenant leak shape that issue #141 / PR #142 closed for
        # ``_return_topic``. Same ``__post_init__`` placement rationale as
        # the ``subscribe_topics`` guard below: ``@dataclass`` subclasses
        # like ``BaseToolNodeDef`` bypass ``BaseNodeDef.__init__``.
        if not self.node_id or not self.node_id.strip() or self.node_id.startswith("."):
            raise ValueError(
                f"node_id must be a non-empty identifier without a leading dot; got {self.node_id!r}. "
                f"node_id is the prefix for several per-node Kafka topics "
                f"({{node_id}}.private.return, {{node_id}}.fanout-state, "
                f"{{node_id}}.fanout-returns) — an empty or dot-prefixed value "
                f"would produce malformed/shared topic names and re-introduce the "
                f"co-tenant leak shape (issue #141)."
            )

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


@dataclass
class BaseToolNodeSchema(BaseNodeSchema):
    _: KW_ONLY
    tool_schema: ToolDefinition
