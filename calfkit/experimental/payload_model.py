import json
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Discriminator, Field

from calfkit.experimental.utils import generate_payload_id


class TextPart(BaseModel):
    kind: Literal["text"] = "text"
    text: str
    metadata: dict[str, Any] | None = None


class FilePart(BaseModel):
    kind: Literal["file"] = "file"
    media_type: str
    uri: str | None = None
    data: str | None = None
    metadata: dict[str, Any] | None = None


class DataPart(BaseModel):
    kind: Literal["data"] = "data"
    data: dict[str, Any]
    schema_: dict[str, Any] | None = Field(default=None, alias="schema")
    metadata: dict[str, Any] | None = None


class ToolCallPart(BaseModel):
    kind: Literal["tool"] = "tool"
    tool_call_id: str
    kwargs: dict[str, Any]
    tool_name: str
    metadata: dict[str, Any] | None = None


ContentPart = Annotated[TextPart | FilePart | DataPart | ToolCallPart, Discriminator("kind")]


class Payload(BaseModel):
    """a single direction-agnostic payload type for inter-agent and intra-agent communication.
    Generally, one payload per node execution. One node run() should only handle one payload at a time.
    In addition, the a payload can generally only have one node author. All parts stored within a payload are all created by one node--the node that created the payload."""  # noqa: E501

    id: str = Field(default_factory=generate_payload_id)  # unique ID (UUID)
    correlation_id: str
    source_node_id: str | None = None  # producing agent identifier (observability, not routing)
    parts: list[ContentPart]  # the content in this payload: text, structured data, or file
    timestamp: float  # unix epoch timestamp
    metadata: dict[str, Any] | None = None

    def text(self, separator: str = "\n\n") -> str:
        """Concatenate all parts into a single string representation."""
        fragments: list[str] = []
        for part in self.parts:
            if isinstance(part, TextPart):
                fragments.append(part.text)
            elif isinstance(part, DataPart):
                fragments.append(json.dumps(part.data))
            elif isinstance(part, FilePart) and part.data is not None:
                fragments.append(part.data)
        return separator.join(fragments)
