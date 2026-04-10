from dataclasses import KW_ONLY, dataclass

from calfkit._vendor.pydantic_ai.tools import ToolDefinition


@dataclass
class BaseNodeSchema:
    _: KW_ONLY
    node_id: str
    subscribe_topics: list[str]
    publish_topic: str | None

    def __post_init__(self):
        if not isinstance(self.subscribe_topics, (list, tuple)):
            self.subscribe_topics = [self.subscribe_topics]


@dataclass
class BaseToolNodeSchema(BaseNodeSchema):
    _: KW_ONLY
    tool_schema: ToolDefinition
